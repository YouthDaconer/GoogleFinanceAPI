/**
 * HU 2.6 — Migración de los saldos que existían antes de la épica.
 *
 * Sin esto, quien ya tenía cuentas ve "no disponible" en todas sus tarjetas de
 * saldo hasta que mueva dinero nuevo: la épica sólo serviría para el dinero
 * futuro. Esta migración reconstruye lo que puede del historial y, sólo donde
 * no puede, estima y pide una confirmación única por cuenta (RN-12).
 *
 * Qué hace por cada saldo de cada cuenta:
 *
 * 1. **Replaya su libro mayor** con el mismo módulo que pinta el historial
 *    (`balanceLedger`), así que la base reconstruida y las cifras que el usuario
 *    ve no pueden discrepar (D1).
 * 2. Si el replay **no alcanza el saldo guardado**, escribe UN asiento de
 *    apertura por la diferencia, con la fecha más temprana determinable y la
 *    tasa histórica de esa fecha (D5). Es una escritura por saldo, no por
 *    movimiento.
 * 3. Si no hay tasa para esa fecha, la base queda `unknown`: un dato ausente se
 *    declara ausente, nunca se rellena con un valor por defecto (RN-13).
 * 4. Anota el veredicto de conciliación en la cuenta, para que la página lo lea
 *    sin ninguna lectura nueva (D4).
 *
 * Tres propiedades que lo hacen seguro de ejecutar, copiadas del recálculo de
 * 2.4 que ya está en producción (D13):
 *
 * - **Idempotente**: un saldo con `balanceCostBasis.{CUR}.source` ya escrito se
 *   salta. Una base `user-confirmed` no se toca jamás.
 * - **Acotado**: tope de escrituras y de consultas de tasa nuevas por
 *   invocación. Si se alcanza, devuelve `hasMore` y el cliente encadena.
 * - **No bloquea nada**: el aviso que deja es una marca en la cuenta; el
 *   producto sigue funcionando igual mientras el usuario lo pospone (AC-7).
 *
 * @module services/balanceLedgerMigration
 * @see platform-docs/stories/2.6-libro-mayor-saldo-migracion/refinamiento.md (T9, D5, D13)
 */

const admin = require('./firebaseAdmin');
const db = admin.firestore();

const historicalRateService = require('./historicalRateService');
const {
  getUserReferenceCurrency,
  COST_BASIS_STATUS,
  EMPTY_BALANCE_EPSILON,
} = require('./helpers/balanceCostBasis');
const { projectBalanceLedger, RECONCILIATION_STATUS } = require('./helpers/balanceLedger');
const {
  buildAdjustment,
  ADJUSTMENT_RATE_SOURCES,
  ADJUSTMENT_REASONS,
  MIN_ADJUSTMENT_DELTA,
} = require('./helpers/balanceAdjustment');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

/** Máximo de saldos migrados por invocación */
const MAX_BALANCES_PER_RUN = 60;

/**
 * Máximo de resoluciones de tasa NUEVAS por invocación.
 *
 * Cada una puede salir a Yahoo si la fecha no está en `historicalExchangeRates`.
 * Es el factor que limita el tiempo de la función, no el número de saldos.
 */
const MAX_RATE_LOOKUPS_PER_RUN = 60;

/** Origen de una base de costo escrita por esta migración */
const MIGRATION_SOURCES = {
  /** Reconstruida replayando los movimientos reales del saldo */
  LEDGER_REPLAY: 'ledger-replay',
  /** Estimada con la tasa histórica de la fecha más temprana determinable */
  MIGRATION_ESTIMATED: 'migration-estimated',
};

// ============================================================================
// HELPERS INTERNOS
// ============================================================================

/**
 * Limpia decimales con la convención del resto del backend.
 *
 * @param {number} num - Número a limpiar
 * @param {number} [decimals=8] - Decimales a conservar
 * @returns {number}
 */
function cleanDecimal(num, decimals = 8) {
  return Number(Math.round(Number(num + 'e' + decimals)) / 10 ** decimals);
}

/**
 * Fecha de la apertura: el día del primer movimiento del saldo si lo hay, y si
 * no el día en que se creó la cuenta. Es lo más cercano a "cuándo llegó ese
 * dinero" que el sistema puede saber (RN-12).
 *
 * @param {Array<Object>} rows - Filas del libro mayor, de más reciente a más antigua
 * @param {Object} accountData - Documento de la cuenta
 * @returns {string} Día en formato `YYYY-MM-DD`
 */
function resolveOpeningDate(rows, accountData) {
  // `rows` viene invertido: la última es la más antigua.
  const earliest = rows.length > 0 ? rows[rows.length - 1].date : null;

  if (earliest) return earliest;

  const createdAt = accountData?.createdAt;

  if (createdAt && typeof createdAt.toDate === 'function') {
    return createdAt.toDate().toISOString().substring(0, 10);
  }

  if (typeof createdAt === 'string' && createdAt.length >= 10) {
    return createdAt.substring(0, 10);
  }

  return new Date().toISOString().substring(0, 10);
}

/**
 * ¿Hay que dejar en paz esta base de costo?
 *
 * Se salta la que ya construyó una migración anterior y la que afirmó el
 * usuario. La que construyeron 2.1 y 2.2 movimiento a movimiento (sin `source`
 * y con `status: 'known'`) también se respeta: describe el mismo historial que
 * el replay reconstruiría.
 *
 * @param {Object|undefined} basis - Entrada de `balanceCostBasis[divisa]`
 * @param {string} referenceCurrency - Moneda de referencia vigente
 * @returns {boolean}
 */
function alreadySettled(basis, referenceCurrency) {
  if (!basis) return false;
  if (basis.source) return true;

  return basis.status === COST_BASIS_STATUS.KNOWN
    && basis.referenceCurrency === referenceCurrency;
}

// ============================================================================
// API PÚBLICA
// ============================================================================

/**
 * Migra los saldos de un usuario al libro mayor.
 *
 * @param {string} userId - UID del usuario
 * @param {Object} [options] - Topes de la pasada, para test
 * @param {number} [options.maxBalances] - Saldos por invocación
 * @param {number} [options.maxRateLookups] - Consultas de tasa nuevas por invocación
 * @returns {Promise<{migratedCount: number, estimatedCount: number,
 *   unavailableCount: number, driftCount: number, scannedCount: number,
 *   hasMore: boolean, referenceCurrency: string,
 *   notices: Array<{accountId: string, accountName: string, currency: string,
 *     estimatedRate: number|null, rateDate: string|null}>}>}
 */
async function migrateBalanceLedgerForUser(userId, options = {}) {
  const maxBalances = options.maxBalances || MAX_BALANCES_PER_RUN;
  const maxRateLookups = options.maxRateLookups || MAX_RATE_LOOKUPS_PER_RUN;

  const referenceCurrency = await getUserReferenceCurrency(userId);

  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .get();

  if (accountsSnapshot.empty) {
    return {
      migratedCount: 0,
      estimatedCount: 0,
      unavailableCount: 0,
      driftCount: 0,
      scannedCount: 0,
      hasMore: false,
      referenceCurrency,
      notices: [],
    };
  }

  let migratedCount = 0;
  let estimatedCount = 0;
  let unavailableCount = 0;
  let driftCount = 0;
  let scannedCount = 0;
  let rateLookups = 0;
  let hasMore = false;
  const notices = [];

  for (const accountDoc of accountsSnapshot.docs) {
    if (migratedCount >= maxBalances || rateLookups >= maxRateLookups) {
      hasMore = true;
      break;
    }

    const accountData = { id: accountDoc.id, ...accountDoc.data() };
    const balances = accountData.balances || {};

    if (Object.keys(balances).length === 0) continue;

    // Una sola lectura de transacciones por cuenta, con el índice que ya existe.
    const transactionsSnapshot = await db.collection('transactions')
      .where('portfolioAccountId', '==', accountDoc.id)
      .get();

    const transactions = transactionsSnapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));

    // El estado se acumula entre divisas porque las escrituras de la cuenta se
    // fusionan en un único `update`, igual que hace la conversión de 2.2.
    const accountUpdate = {};
    const openingTransactions = [];
    const workingAccount = {
      balances: { ...balances },
      balanceCostBasis: { ...(accountData.balanceCostBasis || {}) },
    };

    for (const [currency, rawBalance] of Object.entries(balances)) {
      if (migratedCount >= maxBalances || rateLookups >= maxRateLookups) {
        hasMore = true;
        break;
      }

      scannedCount += 1;

      const balance = Number(rawBalance) || 0;
      const currentBasis = accountData.balanceCostBasis?.[currency];

      const projection = projectBalanceLedger({
        transactions,
        currency,
        referenceCurrency,
        balance,
      });

      // El veredicto se anota siempre, incluso en un saldo ya resuelto: es lo
      // que hace visible la deriva al entrar a la página (AC-3, D4).
      accountUpdate[`balanceReconciliation.${currency}`] = {
        ledgerBalance: projection.reconciliation.ledgerBalance,
        difference: projection.reconciliation.difference,
        status: projection.reconciliation.status,
        checkedAt: admin.firestore.FieldValue.serverTimestamp(),
      };

      if (projection.reconciliation.status === RECONCILIATION_STATUS.DRIFT) {
        driftCount += 1;
      }

      // Sin exposición cambiaria no hay base que reconstruir ni aviso que dar
      // (RN-14, AC-9), pero el veredicto de arriba sí se guarda.
      if (currency === referenceCurrency) continue;

      if (alreadySettled(currentBasis, referenceCurrency)) continue;

      const difference = projection.reconciliation.difference;

      // Caso 1: el historial explica el saldo entero. La base sale del replay.
      if (Math.abs(difference) < EMPTY_BALANCE_EPSILON) {
        const replayed = projection.replayedCostBasis;

        if (replayed && replayed.status === COST_BASIS_STATUS.KNOWN) {
          workingAccount.balanceCostBasis[currency] = {
            ...replayed,
            source: MIGRATION_SOURCES.LEDGER_REPLAY,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          };
          accountUpdate[`balanceCostBasis.${currency}`] = workingAccount.balanceCostBasis[currency];
          migratedCount += 1;
        } else {
          // El historial llega pero no dice a qué tasa entró el dinero. Se
          // declara ausente en lugar de inventar una base (RN-13).
          workingAccount.balanceCostBasis[currency] = {
            cost: null,
            referenceCurrency,
            status: COST_BASIS_STATUS.UNKNOWN,
            source: MIGRATION_SOURCES.MIGRATION_ESTIMATED,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          };
          accountUpdate[`balanceCostBasis.${currency}`] = workingAccount.balanceCostBasis[currency];
          unavailableCount += 1;
          migratedCount += 1;

          notices.push({
            accountId: accountDoc.id,
            accountName: accountData.name || '',
            currency,
            estimatedRate: null,
            rateDate: null,
          });
        }

        continue;
      }

      // Caso 2: el historial no alcanza. La diferencia es el saldo de apertura:
      // dinero que ya estaba antes de que el producto llevara su cuenta (D5).
      if (Math.abs(difference) < MIN_ADJUSTMENT_DELTA) continue;

      const openingDate = resolveOpeningDate(projection.rows, accountData);

      let estimatedRate = null;
      let rateDate = null;

      if (rateLookups < maxRateLookups) {
        rateLookups += 1;

        try {
          const resolved = await historicalRateService.getCrossRate(
            currency,
            referenceCurrency,
            openingDate
          );

          if (resolved !== null) {
            estimatedRate = resolved.rate;
            rateDate = resolved.rateDate;
          }
        } catch (error) {
          // Un fallo del proveedor no puede dejar el saldo sin su apertura: se
          // escribe igual, con la base declarada ausente (RN-13).
          console.warn(`[balanceLedgerMigration] Sin tasa para ${currency}/${referenceCurrency} en ${openingDate}:`, error.message);
        }
      }

      const { transactionData, balanceUpdate } = buildAdjustment({
        account: {
          // El asiento de apertura describe lo que faltaba: se construye sobre
          // el estado que dejó el replay, no sobre el saldo guardado.
          balances: { [currency]: projection.reconciliation.ledgerBalance },
          balanceCostBasis: { [currency]: projection.replayedCostBasis },
        },
        accountId: accountDoc.id,
        userId,
        currency,
        delta: cleanDecimal(difference),
        date: `${openingDate}T${new Date().toISOString().substring(11)}`,
        referenceCurrency,
        adjustmentReason: ADJUSTMENT_REASONS.OPENING,
        acquisitionRate: estimatedRate,
        acquisitionRateSource: estimatedRate !== null
          ? ADJUSTMENT_RATE_SOURCES.MARKET_DATE
          : ADJUSTMENT_RATE_SOURCES.UNAVAILABLE,
        dollarPriceToDate: accountData.balanceCostBasis?.[currency]?.dollarPriceToDate || 1,
        estimated: true,
      });

      openingTransactions.push(transactionData);

      const basis = balanceUpdate[`balanceCostBasis.${currency}`];

      workingAccount.balanceCostBasis[currency] = {
        ...(basis || {
          cost: null,
          referenceCurrency,
          status: COST_BASIS_STATUS.UNKNOWN,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        }),
        // La base sale de una estimación, no de movimientos reales: se marca
        // para rotularla y para poder confirmarla una sola vez (RN-12, D14).
        source: MIGRATION_SOURCES.MIGRATION_ESTIMATED,
        estimatedRate,
        estimatedRateDate: rateDate,
      };
      accountUpdate[`balanceCostBasis.${currency}`] = workingAccount.balanceCostBasis[currency];

      // Con la apertura escrita, el saldo pasa a cuadrar con su historial.
      accountUpdate[`balanceReconciliation.${currency}`] = {
        ledgerBalance: cleanDecimal(balance),
        difference: 0,
        status: RECONCILIATION_STATUS.RECONCILED,
        checkedAt: admin.firestore.FieldValue.serverTimestamp(),
      };

      migratedCount += 1;

      if (estimatedRate !== null) {
        estimatedCount += 1;
      } else {
        unavailableCount += 1;
      }

      notices.push({
        accountId: accountDoc.id,
        accountName: accountData.name || '',
        currency,
        estimatedRate,
        rateDate,
      });
    }

    if (Object.keys(accountUpdate).length === 0 && openingTransactions.length === 0) continue;

    const batch = db.batch();

    for (const transactionData of openingTransactions) {
      batch.set(db.collection('transactions').doc(), transactionData);
    }

    if (Object.keys(accountUpdate).length > 0) {
      batch.update(accountDoc.ref, accountUpdate);
    }

    await batch.commit();
  }

  console.log(`[balanceLedgerMigration] userId: ${userId} - migrados: ${migratedCount}, estimados: ${estimatedCount}, sin base: ${unavailableCount}, derivas: ${driftCount}, hasMore: ${hasMore}`);

  return {
    migratedCount,
    estimatedCount,
    unavailableCount,
    driftCount,
    scannedCount,
    hasMore,
    referenceCurrency,
    // Un aviso por saldo migrado; la interfaz los agrupa por cuenta, que es
    // como RN-12 pide preguntar: una sola vez por cuenta, no por movimiento.
    notices,
  };
}

module.exports = {
  migrateBalanceLedgerForUser,
  MIGRATION_SOURCES,
  MAX_BALANCES_PER_RUN,
  MAX_RATE_LOOKUPS_PER_RUN,
  // Exportados para test
  _resolveOpeningDate: resolveOpeningDate,
  _alreadySettled: alreadySettled,
};
