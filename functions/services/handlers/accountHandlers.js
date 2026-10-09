/**
 * Account Handlers - Lógica de negocio para operaciones de portfolioAccounts
 * 
 * SCALE-CF-001: Handlers extraídos de portfolioAccountOperations.js para consolidación
 * de Cloud Functions HTTP.
 * 
 * @module handlers/accountHandlers
 * @see docs/stories/56.story.md
 */

const { HttpsError } = require("firebase-functions/v2/https");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");

// Importar invalidación de cache de distribución
const { invalidateDistributionCache } = require('../portfolioDistributionService');
// GATE-006: Validación de límites por plan de suscripción
const { validateQuantityLimit } = require('../helpers/subscriptionValidator');
// HU 2.1: base de costo de los saldos de efectivo
// HU 2.5: `EMPTY_BALANCE_EPSILON` decide cuándo un saldo cuenta como vacío (RN-11)
const {
  getUserReferenceCurrency,
  EMPTY_BALANCE_EPSILON,
} = require('../helpers/balanceCostBasis');
// HU 2.6: un saldo inicial, una edicion directa y un ajuste manual dejan todos
// su asiento. Ninguna accion mueve un saldo sin registrarlo (RN-06)
const {
  buildAdjustment,
  resolveAdjustmentRate,
  MIN_ADJUSTMENT_DELTA,
  ADJUSTMENT_REASONS,
} = require('../helpers/balanceAdjustment');

const db = getFirestore();

// ============================================================================
// HELPERS
// ============================================================================

/** Límite duro de operaciones por batch en Firestore */
const FIRESTORE_BATCH_LIMIT = 500;

/**
 * Borra una lista de documentos troceando en batches de 500.
 *
 * FIX-DELETE-002: un `db.batch()` admite como máximo 500 operaciones. Superarlo
 * hace que `commit()` lance, y en un borrado en cascada eso deja los datos a
 * medio eliminar: los pasos siguientes no se ejecutan y quedan huérfanos que ya
 * no se pueden alcanzar, porque la cuenta que los referenciaba desaparecio.
 *
 * @param {Array} docs - Documentos a borrar (snapshot.docs)
 * @returns {Promise<number>} Cuántos se borraron
 */
async function deleteDocsInBatches(docs) {
  for (let i = 0; i < docs.length; i += FIRESTORE_BATCH_LIMIT) {
    const batch = db.batch();
    docs.slice(i, i + FIRESTORE_BATCH_LIMIT).forEach(doc => batch.delete(doc.ref));
    await batch.commit();
  }

  return docs.length;
}

/**
 * Busca los assets de una cuenta cubriendo AMBOS nombres de campo.
 *
 * FIX-DELETE-002: los assets se han escrito historicamente con dos nombres para
 * la referencia a la cuenta. FIX-DELETE-001 cambio la consulta de
 * `portfolioAccountId` a `portfolioAccount`, que es el que usan los escritores
 * actuales (assetResolver.js y asset_repository.py), pero con eso dejo
 * inalcanzables los assets antiguos escritos con el nombre viejo: al borrar su
 * cuenta ya no aparecian en la consulta y quedaban huérfanos para siempre.
 *
 * Se consultan los dos campos y se deduplica por id de documento. No se usa un
 * filtro OR porque abarca campos distintos y exigiria un indice compuesto.
 *
 * NO se filtra por userId a proposito. Hay assets escritos sin ese campo, y en
 * Firestore una igualdad sobre un campo ausente no coincide con nada, asi que el
 * filtro los volvia invisibles al borrado igual que el desajuste de nombre de
 * campo. La propiedad ya quedo probada antes de llegar aqui: la cuenta existe y
 * su userId es el de quien llama. La cuenta es la unica autoridad sobre sus
 * assets, que es el mismo criterio de las reglas de Firestore
 * (`belongsToUser(resource.data.portfolioAccount)`).
 *
 * @param {string} accountId - Cuenta ya verificada como propiedad de quien llama
 * @returns {Promise<Array>} Documentos de asset, sin duplicados
 */
async function findAccountAssets(accountId) {
  const byId = new Map();

  for (const field of ['portfolioAccount', 'portfolioAccountId']) {
    const snapshot = await db.collection('assets')
      .where(field, '==', accountId)
      .get();

    snapshot.docs.forEach(doc => byId.set(doc.id, doc));
  }

  return [...byId.values()];
}

// ============================================================================
// HANDLERS
// ============================================================================

/**
 * Crear una nueva cuenta de portafolio
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de la cuenta
 * @returns {Promise<{success: boolean, accountId: string, account: Object}>}
 */
async function addPortfolioAccount(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { name, description, isActive, taxDeductionPercentage, balances } = payload;

  console.log(`[accountHandlers][addPortfolioAccount] userId: ${userId}, name: ${name}`);

  // Validaciones
  if (!name || typeof name !== "string" || name.trim().length === 0) {
    throw new HttpsError("invalid-argument", "El nombre de la cuenta es requerido");
  }

  // GATE-006: Validar límite de cuentas según plan
  await validateQuantityLimit(userId, "maxAccounts", "portfolioAccounts", { isActive: true });

  try {
    const initialBalances = sanitizeInitialBalances(balances);
    const referenceCurrency = await getUserReferenceCurrency(userId);

    // HU 2.6 (RN-2.6-C): crear una cuenta con saldo no es una excepcion al libro
    // mayor. Cada saldo inicial nace como su primer movimiento, con su tipo de
    // cambio, en lugar de existir como un numero sin origen (D8).
    const openingDate = payload.openingDate
      ? String(payload.openingDate).substring(0, 10)
      : new Date().toLocaleDateString("en-CA");

    const openings = await buildOpeningEntries({
      accountData: { balances: {}, balanceCostBasis: {} },
      accountId: null,
      userId,
      balances: initialBalances,
      declaredRates: payload.balanceRates,
      referenceCurrency,
      openingDate,
    });

    const newAccount = {
      userId,
      name: name.trim(),
      description: description?.trim() || "",
      isActive: isActive !== undefined ? isActive : true,
      taxDeductionPercentage: taxDeductionPercentage || 0,
      balances: initialBalances,
      createdAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    };

    if (Object.keys(openings.costBasis).length > 0) {
      newAccount.balanceCostBasis = openings.costBasis;
    }

    const docRef = db.collection("portfolioAccounts").doc();

    const batch = db.batch();
    batch.set(docRef, newAccount);

    for (const transactionData of openings.transactions) {
      batch.set(db.collection("transactions").doc(), {
        ...transactionData,
        portfolioAccountId: docRef.id,
      });
    }

    await batch.commit();

    // Invalidar cache de distribución
    invalidateDistributionCache(userId);

    console.log(`[accountHandlers][addPortfolioAccount] Éxito - accountId: ${docRef.id}, aperturas: ${openings.transactions.length}`);

    return {
      success: true,
      accountId: docRef.id,
      openingEntries: openings.transactions.length,
      account: {
        id: docRef.id,
        ...newAccount,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      },
    };
  } catch (error) {
    console.error("[accountHandlers][addPortfolioAccount] Error:", error);
    throw new HttpsError("internal", `Error al crear la cuenta: ${error.message}`);
  }
}

/**
 * HU 2.5 (RN-11) — Divisas que la actualización pretende eliminar y todavía
 * tienen saldo.
 *
 * Eliminar una divisa con saldo es el camino por el que hoy desaparece dinero
 * del historial sin dejar asiento: se va el saldo y con él la base de costo que
 * dice cuánto costó. Exigir saldo en cero convierte una decisión destructiva en
 * una secuencia trazable — retirar o convertir primero, y esos movimientos sí
 * realizan su diferencia en cambio.
 *
 * El guardarraíl vive aquí, no en la interfaz, porque los dos botones de borrado
 * del cliente —el del diálogo de movimiento y el de la papelera del formulario
 * de cuenta— pasan los dos por este handler.
 *
 * @param {Object|undefined} currentBalances - `balances` actuales de la cuenta
 * @param {Object|undefined} nextBalances - `balances` que trae la actualización
 * @returns {Array<{currency: string, balance: number}>} Divisas bloqueantes
 */
function findRemovedCurrenciesWithBalance(currentBalances, nextBalances) {
  if (!currentBalances || !nextBalances || typeof nextBalances !== "object") return [];

  return Object.keys(currentBalances)
    .filter((currency) => !Object.prototype.hasOwnProperty.call(nextBalances, currency))
    .map((currency) => ({ currency, balance: Number(currentBalances[currency]) || 0 }))
    .filter(({ balance }) => Math.abs(balance) >= EMPTY_BALANCE_EPSILON);
}

/**
 * HU 2.6 — Normaliza el mapa de saldos iniciales que llega del cliente.
 *
 * @param {Object|undefined} balances - Mapa divisa -> monto
 * @returns {Object} Mapa saneado, sin divisas vacías ni montos no numéricos
 */
function sanitizeInitialBalances(balances) {
  if (!balances || typeof balances !== "object") return {};

  const sanitized = {};

  for (const [currency, amount] of Object.entries(balances)) {
    if (!currency || typeof currency !== "string") continue;

    const parsed = Number(amount);
    sanitized[currency] = Number.isFinite(parsed) ? parsed : 0;
  }

  return sanitized;
}

/**
 * HU 2.6 — Fecha de transacción de una apertura.
 *
 * Conserva el día tal cual se eligió y le pega la hora actual, para que dos
 * aperturas del mismo día se ordenen entre sí. Deliberadamente NO usa
 * `combineDateWithCurrentTime`, que compone en hora local y devuelve UTC: de
 * tarde en América el día resultante ya no es el que se pidió (bug encontrado
 * en 2.1).
 *
 * @param {string} dateOnly - Día en formato `YYYY-MM-DD`
 * @returns {string} ISO completo
 */
function toOpeningTimestamp(dateOnly) {
  return `${dateOnly}T${new Date().toISOString().substring(11)}`;
}

/**
 * HU 2.6 (RN-2.6-C) — Convierte saldos iniciales en sus asientos de apertura.
 *
 * Un saldo con el que se estrena una cuenta es dinero que ya existía, no un
 * aporte: se registra como `cash_adjustment/opening` y no como `cash_income`,
 * para no reescribir el rendimiento histórico de nadie (D8).
 *
 * **No bloquea la creación cuando falta la tasa** (D9). RN-05 bloquea el ingreso
 * porque el usuario está declarando dinero nuevo; crear la cuenta es el paso
 * previo a todo lo demás, y dejarlo caído por una caída del proveedor de tasas
 * no protege ningún dato. La base queda declarada ausente (RN-13) y el aviso de
 * migración la recogerá.
 *
 * @param {Object} params
 * @param {Object} params.accountData - Estado de partida de la cuenta
 * @param {string|null} params.accountId - Id de la cuenta, o null si aún no existe
 * @param {string} params.userId - UID del propietario
 * @param {Object} params.balances - Saldos que nacen, mapa divisa -> monto
 * @param {Object} [params.declaredRates] - Tasas declaradas por divisa
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {string} params.openingDate - Día de la apertura, `YYYY-MM-DD`
 * @returns {Promise<{transactions: Array<Object>, costBasis: Object}>}
 */
async function buildOpeningEntries({
  accountData,
  accountId,
  userId,
  balances,
  declaredRates,
  referenceCurrency,
  openingDate,
}) {
  const transactions = [];
  const costBasis = {};
  const date = toOpeningTimestamp(openingDate);

  for (const [currency, amount] of Object.entries(balances || {})) {
    if (Math.abs(Number(amount) || 0) < MIN_ADJUSTMENT_DELTA) continue;

    const resolved = await resolveAdjustmentRate({
      currency,
      referenceCurrency,
      date: openingDate,
      declaredRate: declaredRates ? declaredRates[currency] : null,
    });

    const { transactionData, balanceUpdate } = buildAdjustment({
      account: accountData,
      accountId,
      userId,
      currency,
      delta: Number(amount),
      date,
      referenceCurrency,
      adjustmentReason: ADJUSTMENT_REASONS.OPENING,
      acquisitionRate: resolved.acquisitionRate,
      acquisitionRateSource: resolved.acquisitionRateSource,
      dollarPriceToDate: resolved.dollarPriceToDate,
    });

    transactions.push(accountId ? { ...transactionData, portfolioAccountId: accountId } : transactionData);

    const basis = balanceUpdate[`balanceCostBasis.${currency}`];
    if (basis) costBasis[currency] = basis;
  }

  return { transactions, costBasis };
}

/**
 * HU 2.6 (RN-06) — Divisas cuyo monto cambia en una edición de cuenta.
 *
 * Editar la cuenta puede añadir una divisa o quitar una vacía, pero no mover el
 * dinero de una que ya estaba: para eso está el ajuste, que deja asiento (D10).
 * El guardarraíl vive aquí, junto al de RN-11, porque es donde pasan todos los
 * llamadores.
 *
 * @param {Object|undefined} currentBalances - `balances` actuales
 * @param {Object|undefined} nextBalances - `balances` que trae la actualización
 * @returns {Array<{currency: string, from: number, to: number}>}
 */
function findEditedBalances(currentBalances, nextBalances) {
  if (!currentBalances || !nextBalances || typeof nextBalances !== "object") return [];

  return Object.keys(currentBalances)
    .filter((currency) => Object.prototype.hasOwnProperty.call(nextBalances, currency))
    .map((currency) => ({
      currency,
      from: Number(currentBalances[currency]) || 0,
      to: Number(nextBalances[currency]) || 0,
    }))
    .filter(({ from, to }) => Math.abs(from - to) >= MIN_ADJUSTMENT_DELTA);
}

/**
 * Actualizar una cuenta de portafolio existente
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de actualización
 * @returns {Promise<{success: boolean, accountId: string, updatedFields: string[]}>}
 */
async function updatePortfolioAccount(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { accountId, updates } = payload;

  console.log(`[accountHandlers][updatePortfolioAccount] userId: ${userId}, accountId: ${accountId}`);

  // Validaciones
  if (!accountId) {
    throw new HttpsError("invalid-argument", "El ID de la cuenta es requerido");
  }

  if (!updates || typeof updates !== "object") {
    throw new HttpsError("invalid-argument", "Los datos de actualización son requeridos");
  }

  try {
    const accountRef = db.collection("portfolioAccounts").doc(accountId);
    const accountDoc = await accountRef.get();

    if (!accountDoc.exists) {
      throw new HttpsError("not-found", "La cuenta no existe");
    }

    const accountData = accountDoc.data();
    if (accountData.userId !== userId) {
      throw new HttpsError("permission-denied", "No tienes permiso para actualizar esta cuenta");
    }

    // Campos permitidos para actualizar
    const allowedFields = ["name", "description", "isActive", "taxDeductionPercentage", "balances"];
    const sanitizedUpdates = {};

    for (const field of allowedFields) {
      if (updates[field] !== undefined) {
        sanitizedUpdates[field] = updates[field];
      }
    }

    // HU 2.6 (RN-06): editar una cuenta deja de mover saldos. Se puede anadir una
    // divisa nueva —que nace con su asiento de apertura— y quitar una en cero,
    // pero cambiar el monto de una divisa guardada se hace con un ajuste, que si
    // deja rastro (D10).
    if (sanitizedUpdates.balances !== undefined) {
      const edited = findEditedBalances(accountData.balances, sanitizedUpdates.balances);

      if (edited.length > 0) {
        const detail = edited
          .map(({ currency, from, to }) => `${currency}: ${from.toFixed(2)} -> ${to.toFixed(2)}`)
          .join(", ");

        throw new HttpsError(
          "failed-precondition",
          `No se puede cambiar el monto de un saldo desde la edición de la cuenta (${detail}). Registra un ajuste para que quede en el historial.`
        );
      }
    }

    // HU 2.5 (RN-11): una divisa solo se elimina de una cuenta con saldo en cero.
    if (sanitizedUpdates.balances !== undefined) {
      const blocking = findRemovedCurrenciesWithBalance(accountData.balances, sanitizedUpdates.balances);

      if (blocking.length > 0) {
        const detail = blocking
          .map(({ currency, balance }) => `${balance.toFixed(2)} ${currency}`)
          .join(", ");

        throw new HttpsError(
          "failed-precondition",
          `No se puede eliminar una divisa con saldo: ${detail}. Retira o convierte ese saldo antes de eliminarla.`
        );
      }

      // La divisa se va con el saldo en cero: su base de costo se va con ella.
      // Dejarla convertiría el próximo ingreso en esa divisa en un saldo que
      // arrastra el costo de un dinero que ya no existe.
      for (const currency of Object.keys(accountData.balances || {})) {
        if (!Object.prototype.hasOwnProperty.call(sanitizedUpdates.balances, currency)) {
          sanitizedUpdates[`balanceCostBasis.${currency}`] = FieldValue.delete();
        }
      }
    }

    // Una divisa que no estaba nace con su asiento de apertura, igual que si la
    // cuenta se acabara de crear con ella (RN-2.6-C).
    let openings = { transactions: [], costBasis: {} };

    if (sanitizedUpdates.balances !== undefined) {
      const addedBalances = {};

      for (const [currency, amount] of Object.entries(sanitizedUpdates.balances || {})) {
        if (!Object.prototype.hasOwnProperty.call(accountData.balances || {}, currency)) {
          addedBalances[currency] = Number(amount) || 0;
        }
      }

      if (Object.keys(addedBalances).length > 0) {
        const referenceCurrency = await getUserReferenceCurrency(userId);

        openings = await buildOpeningEntries({
          accountData: { balances: {}, balanceCostBasis: {} },
          accountId,
          userId,
          balances: addedBalances,
          declaredRates: updates.balanceRates,
          referenceCurrency,
          openingDate: new Date().toLocaleDateString("en-CA"),
        });

        for (const [currency, basis] of Object.entries(openings.costBasis)) {
          sanitizedUpdates[`balanceCostBasis.${currency}`] = basis;
        }
      }
    }

    sanitizedUpdates.updatedAt = FieldValue.serverTimestamp();

    const batch = db.batch();
    batch.update(accountRef, sanitizedUpdates);

    for (const transactionData of openings.transactions) {
      batch.set(db.collection("transactions").doc(), transactionData);
    }

    await batch.commit();

    // Invalidar cache de distribución
    invalidateDistributionCache(userId);

    console.log(`[accountHandlers][updatePortfolioAccount] Éxito - accountId: ${accountId}, aperturas: ${openings.transactions.length}`);

    return {
      success: true,
      accountId,
      openingEntries: openings.transactions.length,
      updatedFields: Object.keys(sanitizedUpdates).filter(k => k !== "updatedAt"),
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error("[accountHandlers][updatePortfolioAccount] Error:", error);
    throw new HttpsError("internal", `Error al actualizar la cuenta: ${error.message}`);
  }
}

/**
 * Eliminar una cuenta de portafolio y todos sus datos asociados
 * 
 * REF-SEC-002: Eliminación completa manejada en backend
 * Elimina atómicamente: assets, transacciones, referencia en portfolioDistribution, y la cuenta
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de la cuenta
 * @returns {Promise<{success: boolean, accountId: string, deletedAccountName: string, deletedAssets: number, deletedTransactions: number}>}
 */
async function deletePortfolioAccount(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { accountId } = payload;

  console.log(`[accountHandlers][deletePortfolioAccount] userId: ${userId}, accountId: ${accountId}`);

  if (!accountId) {
    throw new HttpsError("invalid-argument", "El ID de la cuenta es requerido");
  }

  try {
    const accountRef = db.collection("portfolioAccounts").doc(accountId);
    const accountDoc = await accountRef.get();

    if (!accountDoc.exists) {
      throw new HttpsError("not-found", "La cuenta no existe");
    }

    const accountData = accountDoc.data();
    if (accountData.userId !== userId) {
      throw new HttpsError("permission-denied", "No tienes permiso para eliminar esta cuenta");
    }

    // =========================================================================
    // REF-SEC-002: Eliminación completa de datos asociados
    // =========================================================================
    
    let deletedAssetsCount = 0;
    let deletedTransactionsCount = 0;

    // 1. Eliminar todos los assets asociados a esta cuenta
    // FIX-DELETE-002: se cubren los dos nombres de campo historicos y no se
    // filtra por userId, porque ambas cosas dejaban assets fuera de la consulta
    // y por tanto huerfanos. Se trocea en batches de 500: antes se usaba un
    // unico batch, que con mas de 500 assets habria hecho fallar el commit.
    console.log(`[accountHandlers][deletePortfolioAccount] Eliminando assets de la cuenta ${accountId}`);
    const assetDocs = await findAccountAssets(accountId);

    if (assetDocs.length > 0) {
      deletedAssetsCount = await deleteDocsInBatches(assetDocs);
      console.log(`[accountHandlers][deletePortfolioAccount] Eliminados ${deletedAssetsCount} assets`);
    }

    // 2. Eliminar todas las transacciones asociadas a esta cuenta
    console.log(`[accountHandlers][deletePortfolioAccount] Eliminando transacciones de la cuenta ${accountId}`);
    const transactionsSnapshot = await db.collection("transactions")
      .where("portfolioAccountId", "==", accountId)
      .get();

    if (!transactionsSnapshot.empty) {
      deletedTransactionsCount = await deleteDocsInBatches(transactionsSnapshot.docs);
      console.log(`[accountHandlers][deletePortfolioAccount] Eliminadas ${deletedTransactionsCount} transacciones`);
    }

    // 3. Eliminar la referencia de la cuenta en portfolioDistribution
    console.log(`[accountHandlers][deletePortfolioAccount] Limpiando portfolioDistribution`);
    const distributionRef = db.collection("portfolioDistribution").doc(userId);
    const distributionDoc = await distributionRef.get();
    
    if (distributionDoc.exists) {
      const distributionData = distributionDoc.data();
      if (distributionData?.accounts && distributionData.accounts[accountId]) {
        await distributionRef.update({
          [`accounts.${accountId}`]: FieldValue.delete()
        });
        console.log(`[accountHandlers][deletePortfolioAccount] Referencia eliminada de portfolioDistribution`);
      }
    }

    // 4. Eliminar la cuenta
    await accountRef.delete();

    // PERF-SNAP-028: Best-effort cleanup de snapshots de la cuenta eliminada
    try {
      const accountSnapshots = await db.collection('performanceSnapshots')
        .where('userId', '==', userId)
        .where('accountId', '==', accountId)
        .get();

      if (!accountSnapshots.empty) {
        // FIX-DELETE-002: tambien troceado. Los snapshots son diarios, asi que
        // una cuenta con mas de año y medio de historia pasa de 500 documentos.
        const deleted = await deleteDocsInBatches(accountSnapshots.docs);
        console.log(`[accountHandlers][deletePortfolioAccount] Deleted ${deleted} snapshots for account ${accountId}`);
      }
    } catch (cleanupError) {
      console.warn(`[accountHandlers][deletePortfolioAccount] Snapshot cleanup failed for account ${accountId}: ${cleanupError.message}`);
    }

    // 5. Invalidar cache de distribución
    invalidateDistributionCache(userId);

    console.log(`[accountHandlers][deletePortfolioAccount] Éxito - accountId: ${accountId}, assets: ${deletedAssetsCount}, transactions: ${deletedTransactionsCount}`);

    return {
      success: true,
      accountId,
      deletedAccountName: accountData.name,
      deletedAssets: deletedAssetsCount,
      deletedTransactions: deletedTransactionsCount,
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error("[accountHandlers][deletePortfolioAccount] Error:", error);
    throw new HttpsError("internal", `Error al eliminar la cuenta: ${error.message}`);
  }
}

/**
 * Actualizar el balance de una moneda específica en una cuenta
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos del balance
 * @returns {Promise<{success: boolean, accountId: string, currency: string, previousBalance: number, newBalance: number, operation: string}>}
 */
async function updatePortfolioAccountBalance(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { accountId, currency, amount, operation } = payload;

  console.log(`[accountHandlers][updatePortfolioAccountBalance] userId: ${userId}, accountId: ${accountId}, currency: ${currency}`);

  // Validaciones
  if (!accountId) {
    throw new HttpsError("invalid-argument", "El ID de la cuenta es requerido");
  }

  if (!currency || typeof currency !== "string") {
    throw new HttpsError("invalid-argument", "La moneda es requerida");
  }

  if (typeof amount !== "number") {
    throw new HttpsError("invalid-argument", "El monto debe ser un número");
  }

  // operation puede ser 'add' (sumar al balance), 'set' (establecer valor exacto), o 'subtract' (restar)
  const validOperations = ["add", "set", "subtract"];
  const op = operation || "add";
  if (!validOperations.includes(op)) {
    throw new HttpsError("invalid-argument", `Operación inválida. Debe ser: ${validOperations.join(", ")}`);
  }

  try {
    const accountRef = db.collection("portfolioAccounts").doc(accountId);
    const accountDoc = await accountRef.get();

    if (!accountDoc.exists) {
      throw new HttpsError("not-found", "La cuenta no existe");
    }

    const accountData = accountDoc.data();
    if (accountData.userId !== userId) {
      throw new HttpsError("permission-denied", "No tienes permiso para actualizar esta cuenta");
    }

    const currentBalance = accountData.balances?.[currency] || 0;
    let newBalance;

    switch (op) {
      case "add":
        newBalance = currentBalance + amount;
        break;
      case "subtract":
        newBalance = currentBalance - amount;
        break;
      case "set":
        newBalance = amount;
        break;
      default:
        newBalance = currentBalance + amount;
    }

    const delta = newBalance - currentBalance;

    if (Math.abs(delta) < MIN_ADJUSTMENT_DELTA) {
      console.log(`[accountHandlers][updatePortfolioAccountBalance] Sin cambio - ${currency}: ${currentBalance}`);

      return {
        success: true,
        accountId,
        currency,
        previousBalance: currentBalance,
        newBalance: currentBalance,
        operation: op,
        transactionId: null,
      };
    }

    // HU 2.6 (RN-06): esta acción movía el saldo por su cuenta. Ahora delega en
    // el mismo núcleo que el ajuste manual, así que deja su asiento como
    // cualquier otro movimiento. Sigue siendo la misma acción (D11).
    const referenceCurrency = await getUserReferenceCurrency(userId);
    const adjustmentDate = payload.date
      ? String(payload.date).substring(0, 10)
      : new Date().toLocaleDateString("en-CA");

    const resolved = await resolveAdjustmentRate({
      currency,
      referenceCurrency,
      date: adjustmentDate,
      declaredRate: payload.exchangeRate,
    });

    const { transactionData, balanceUpdate } = buildAdjustment({
      account: accountData,
      accountId,
      userId,
      currency,
      delta,
      date: toOpeningTimestamp(adjustmentDate),
      referenceCurrency,
      adjustmentReason: ADJUSTMENT_REASONS.MANUAL,
      description: payload.reason || "",
      acquisitionRate: resolved.acquisitionRate,
      acquisitionRateSource: resolved.acquisitionRateSource,
      dollarPriceToDate: resolved.dollarPriceToDate,
    });

    const batch = db.batch();
    const transactionRef = db.collection("transactions").doc();

    batch.set(transactionRef, transactionData);
    batch.update(accountRef, {
      ...balanceUpdate,
      updatedAt: FieldValue.serverTimestamp(),
    });

    await batch.commit();

    invalidateDistributionCache(userId);

    console.log(`[accountHandlers][updatePortfolioAccountBalance] Éxito - ${currency}: ${currentBalance} -> ${newBalance}`);

    return {
      success: true,
      accountId,
      currency,
      previousBalance: currentBalance,
      newBalance: balanceUpdate[`balances.${currency}`],
      operation: op,
      transactionId: transactionRef.id,
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error("[accountHandlers][updatePortfolioAccountBalance] Error:", error);
    throw new HttpsError("internal", `Error al actualizar el balance: ${error.message}`);
  }
}

/**
 * HU 2.3 — Confirmación única de la base de costo de un saldo (RN-12).
 *
 * Cuando el usuario va a comprar pagando con un saldo cuyo tipo de cambio de
 * adquisición el sistema no pudo determinar, se le pide **una sola vez** que lo
 * confirme. Este handler fija esa base: `costo = saldo × tasa confirmada`.
 *
 * La no-repetición no es un flag de interfaz: al quedar `status: 'known'`, la
 * derivación de `createAsset` encuentra base determinable y no vuelve a
 * preguntar en ninguna compra posterior desde ese saldo. Por eso mismo el
 * handler **rechaza** una segunda confirmación sobre una base ya conocida: si
 * el usuario quiere corregirla, el camino es registrar el movimiento de
 * efectivo que falta (RN-04), no sobrescribir el dato.
 *
 * El asiento contable de esta confirmación pertenece a 2.6 (libro mayor del
 * saldo); aquí solo se fija la base.
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - { accountId, currency, rate }
 * @returns {Promise<{success: boolean, accountId: string, currency: string, rate: number, cost: number, referenceCurrency: string}>}
 */
async function confirmBalanceCostBasis(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { accountId, currency, rate } = payload || {};

  console.log(`[accountHandlers][confirmBalanceCostBasis] userId: ${userId}, accountId: ${accountId}, currency: ${currency}`);

  if (!accountId) {
    throw new HttpsError("invalid-argument", "El ID de la cuenta es requerido");
  }

  if (!currency || typeof currency !== "string") {
    throw new HttpsError("invalid-argument", "La moneda es requerida");
  }

  const confirmedRate = Number(rate);
  if (!Number.isFinite(confirmedRate) || confirmedRate <= 0) {
    throw new HttpsError("invalid-argument", "El tipo de cambio debe ser mayor que cero");
  }

  try {
    const accountRef = db.collection("portfolioAccounts").doc(accountId);
    const accountDoc = await accountRef.get();

    if (!accountDoc.exists) {
      throw new HttpsError("not-found", "La cuenta no existe");
    }

    const accountData = accountDoc.data();
    if (accountData.userId !== userId) {
      throw new HttpsError("permission-denied", "No tienes permiso para actualizar esta cuenta");
    }

    const referenceCurrency = await getUserReferenceCurrency(userId);

    // Sin exposición cambiaria no hay base que confirmar (RN-14).
    if (currency === referenceCurrency) {
      throw new HttpsError(
        "failed-precondition",
        "Un saldo en la moneda de referencia no tiene tipo de cambio de adquisición"
      );
    }

    const currentBasis = accountData.balanceCostBasis?.[currency];

    // La confirmación es única: una base ya conocida no se sobrescribe (RN-12).
    //
    // HU 2.6 (D14): salvo que sea la que **estimó la migración**, que es
    // exactamente lo que el aviso de AC-6 pide confirmar o corregir. Una base
    // construida de movimientos reales, o ya confirmada por el usuario, sigue
    // siendo intocable: para corregir esa, el camino es registrar el movimiento
    // que falta (RN-04).
    const isMigrationEstimate = currentBasis?.source === 'migration-estimated';

    if (currentBasis
      && currentBasis.status === 'known'
      && currentBasis.referenceCurrency === referenceCurrency
      && !isMigrationEstimate) {
      throw new HttpsError(
        "failed-precondition",
        "Este saldo ya tiene su tipo de cambio de adquisición registrado"
      );
    }

    const balance = accountData.balances?.[currency] || 0;
    const cost = Math.round(balance * confirmedRate * 100) / 100;

    await accountRef.update({
      [`balanceCostBasis.${currency}`]: {
        cost,
        referenceCurrency,
        status: 'known',
        // Deja constancia de que esta base la afirmó el usuario, no la derivó
        // el sistema: 2.6 la presentará como estimada allí donde corresponda.
        source: 'user-confirmed',
        confirmedRate,
        updatedAt: FieldValue.serverTimestamp(),
      },
    });

    console.log(`[accountHandlers][confirmBalanceCostBasis] Éxito - ${currency}: ${balance} @ ${confirmedRate} = ${cost} ${referenceCurrency}`);

    return {
      success: true,
      accountId,
      currency,
      rate: confirmedRate,
      cost,
      referenceCurrency,
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error("[accountHandlers][confirmBalanceCostBasis] Error:", error);
    throw new HttpsError("internal", `Error al confirmar la base de costo del saldo: ${error.message}`);
  }
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  addPortfolioAccount,
  updatePortfolioAccount,
  deletePortfolioAccount,
  updatePortfolioAccountBalance,
  confirmBalanceCostBasis,
  // Exportado para test (HU 2.5, RN-11)
  _findRemovedCurrenciesWithBalance: findRemovedCurrenciesWithBalance,
  // Exportados para test (HU 2.6, RN-06 y RN-2.6-C)
  _findEditedBalances: findEditedBalances,
  _buildOpeningEntries: buildOpeningEntries,
  _sanitizeInitialBalances: sanitizeInitialBalances,
};
