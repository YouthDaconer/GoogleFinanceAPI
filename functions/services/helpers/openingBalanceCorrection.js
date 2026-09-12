/**
 * HU 2.7 — Corregir el saldo con el que empezó la cuenta.
 *
 * 2.6 dejó una sola vía para corregir un saldo: registrar un ajuste fechado.
 * Funciona cuando faltaban movimientos, y miente cuando lo que estaba mal era
 * el primer número — dice que hoy apareció dinero que llevaba años ahí. Esta
 * historia abre la segunda vía **por la puerta correcta**: no tocando el saldo,
 * sino corrigiendo el movimiento de apertura del que ese saldo nace. El saldo
 * sigue siendo la proyección de su historial (RN-06); lo que cambia es cuánto
 * vale su primer movimiento.
 *
 * Tres propiedades que hacen que esto no pueda romper lo que 2.6 construyó:
 *
 * - **El saldo resultante sale del replay, no de una aritmética de deltas**
 *   (D2). Al cambiar el monto del primer movimiento cambia la tasa promedio
 *   vigente con la que cada salida posterior consumió base, así que la base de
 *   costo final NO es la actual más un delta: sólo se obtiene replayando. Se
 *   replaya con el mismo `projectBalanceLedger` que pinta el historial, así que
 *   el saldo que se persiste y las cifras que el usuario lee no pueden
 *   divergir.
 * - **El objetivo se despeja contra el saldo del historial, no contra el
 *   guardado** (D3). El libro mayor es lineal en el monto de la apertura, así
 *   que la diferencia a aplicar es `objetivo − ledgerBalance`. Despejarla
 *   contra el saldo guardado dejaría a una cuenta con deriva en un saldo
 *   distinto del que el usuario escribió.
 * - **El recorrido en negativo se valida replayando, no con una fórmula** (D4).
 *   Hacerlo por fórmula exigiría asumir que la apertura es el primer
 *   movimiento del saldo, y en datos anteriores a 2.6 puede no serlo.
 *
 * Es puro: no lee ni escribe en Firestore. Quien llama resuelve antes la tasa
 * y decide si el conflicto bloquea la operación.
 *
 * @module services/helpers/openingBalanceCorrection
 * @see platform-docs/stories/2.7-ajuste-saldo-registro-o-apertura/refinamiento.md (T1, D2, D3, D4, D8)
 */

const {
  projectBalanceLedger,
  MOVEMENT_KINDS,
  RECONCILIATION_STATUS,
  ADJUSTMENT_REASONS,
  _sortKey: sortKey,
} = require('./balanceLedger');
const { buildAdjustment, ADJUSTMENT_TYPE } = require('./balanceAdjustment');
const { COST_BASIS_STATUS, EMPTY_BALANCE_EPSILON } = require('./balanceCostBasis');

// ============================================================================
// CONSTANTES
// ============================================================================

/**
 * Origen de la base de costo que escribe esta corrección.
 *
 * Es el mismo valor que usa la migración de 2.6 cuando reconstruye una base
 * replayando movimientos reales, y por la misma razón: aquí la base también
 * sale del replay, no de una estimación (D2).
 */
const CORRECTION_COST_BASIS_SOURCE = 'ledger-replay';

/** Por debajo de esto la corrección no cambia el saldo y no merece escritura */
const MIN_CORRECTION_SHIFT = 0.005;

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
 * Día en formato `YYYY-MM-DD` de un campo `date` o de un `createdAt`.
 *
 * @param {*} value - Valor a interpretar
 * @returns {string|null}
 */
function toDateOnly(value) {
  if (!value) return null;

  if (typeof value.toDate === 'function') {
    return value.toDate().toISOString().substring(0, 10);
  }

  const text = String(value);
  return text.length >= 10 ? text.substring(0, 10) : null;
}

/**
 * El día anterior a una fecha `YYYY-MM-DD`.
 *
 * RN-2.7-F pide que la apertura que falta se cree **justo antes** del
 * movimiento más antiguo. La migración de 2.6 la fechaba el mismo día; un día
 * antes es lo que hace que el punto de partida sea explícito en lugar de
 * competir en orden con el movimiento que ya estaba (D8).
 *
 * @param {string} date - Día en formato `YYYY-MM-DD`
 * @returns {string} El día anterior, en el mismo formato
 */
function previousDay(date) {
  const parsed = new Date(`${date}T00:00:00Z`);
  parsed.setUTCDate(parsed.getUTCDate() - 1);
  return parsed.toISOString().substring(0, 10);
}

/**
 * El asiento de apertura de un saldo, o `null` si nunca tuvo uno.
 *
 * Si hubiera más de uno —posible en datos que pasaron por la creación de
 * cuenta y por la migración— manda el más antiguo: es el punto de partida de
 * verdad, y el resto son movimientos posteriores que esta corrección no toca
 * (RN-2.7-C).
 *
 * @param {Array<Object>} transactions - Transacciones de la cuenta, con `id`
 * @param {string} currency - Divisa del saldo
 * @returns {Object|null}
 */
function findOpeningTransaction(transactions, currency) {
  const openings = (transactions || []).filter((transaction) => transaction
    && transaction.type === ADJUSTMENT_TYPE
    && transaction.adjustmentReason === ADJUSTMENT_REASONS.OPENING
    && transaction.currency === currency);

  if (openings.length === 0) return null;

  return openings.sort((a, b) => (sortKey(a) < sortKey(b) ? -1 : 1))[0];
}

/**
 * Las filas del recorrido que se desplazan al corregir la apertura.
 *
 * Sólo se desplazan las que están en la apertura o después de ella. Una fila
 * anterior —posible cuando un dato antiguo tiene la apertura fechada después
 * de otro movimiento— conserva su saldo, así que un negativo suyo es
 * preexistente y no puede bloquear esta corrección. Cuando no hay apertura, la
 * que se va a crear es anterior a todo, así que se desplaza el recorrido
 * entero.
 *
 * @param {Array<Object>} rows - Filas del libro mayor, de más reciente a más antigua
 * @param {string|null} openingId - Id del asiento de apertura
 * @returns {Array<Object>} Filas afectadas, en orden cronológico ascendente
 */
function shiftableRows(rows, openingId) {
  const chronological = [...(rows || [])].reverse();

  if (!openingId) return chronological;

  const openingIndex = chronological.findIndex((row) => row.id === openingId);

  return openingIndex >= 0 ? chronological.slice(openingIndex) : chronological;
}

/**
 * El punto más bajo del recorrido, con la fecha en que se alcanza.
 *
 * @param {Array<Object>} rows - Filas en orden cronológico
 * @returns {{balance: number, date: string|null}}
 */
function lowestPoint(rows) {
  if (!rows || rows.length === 0) return { balance: 0, date: null };

  return rows.reduce((lowest, row) => (
    row.balanceAfter < lowest.balance
      ? { balance: row.balanceAfter, date: row.date }
      : lowest
  ), { balance: rows[0].balanceAfter, date: rows[0].date });
}

// ============================================================================
// API PÚBLICA
// ============================================================================

/**
 * Todo lo que hace falta saber de un saldo antes de corregir su apertura.
 *
 * El diálogo lo pide una sola vez al abrirse y deriva de aquí, con dos restas,
 * el monto nuevo de la apertura y el mínimo admisible — sin una llamada por
 * tecla y sin que el usuario calcule nada (RN-2.7-B, D5). No puede salir de
 * `getBalanceLedger`, que recorta filas y por tanto no ve el mínimo real del
 * recorrido.
 *
 * @param {Object} params
 * @param {Array<Object>} params.transactions - Transacciones de la cuenta, con `id`
 * @param {string} params.currency - Divisa del saldo
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {number} [params.storedBalance=0] - Saldo guardado hoy
 * @param {*} [params.accountCreatedAt] - `createdAt` de la cuenta, para el saldo sin movimientos
 * @returns {{hasOpening: boolean, openingId: string|null, openingDate: string|null,
 *   openingAmount: number, openingRate: number|null, openingRateEstimated: boolean,
 *   openingRateMissing: boolean, ledgerBalance: number, storedBalance: number,
 *   difference: number, minBalanceAfter: number, minBalanceDate: string|null,
 *   movementCountAfter: number, newOpeningDate: string|null,
 *   earliestMovementDate: string|null, hasFxExposure: boolean,
 *   reconciliationStatus: string}}
 */
function planOpeningCorrection({
  transactions,
  currency,
  referenceCurrency,
  storedBalance = 0,
  accountCreatedAt = null,
}) {
  const projection = projectBalanceLedger({
    transactions,
    currency,
    referenceCurrency,
    balance: storedBalance,
  });

  const opening = findOpeningTransaction(transactions, currency);
  const openingId = opening ? (opening.id || null) : null;

  // `rows` viene de más reciente a más antigua: la última es la más antigua.
  const earliestMovementDate = projection.rows.length > 0
    ? projection.rows[projection.rows.length - 1].date
    : null;

  const affected = shiftableRows(projection.rows, openingId);
  const lowest = lowestPoint(affected);

  const openingRate = opening && typeof opening.acquisitionRate === 'number'
    && Number.isFinite(opening.acquisitionRate) && opening.acquisitionRate > 0
    ? opening.acquisitionRate
    : null;
  const openingRateEstimated = opening ? opening.costBasisEstimated === true : false;
  const hasFxExposure = currency !== referenceCurrency;

  const openingAmount = opening
    ? cleanDecimal(Number(opening.adjustmentDelta) || 0)
    : 0;

  return {
    hasOpening: Boolean(opening),
    openingId,
    openingDate: opening ? toDateOnly(opening.date) : null,
    openingAmount,
    openingRate,
    openingRateEstimated,
    // La tasa de la apertura se reutiliza tal cual, porque es el mismo día
    // (RN-2.7-E). Sólo se pide cuando no la hay o cuando la que hay la estimó
    // la migración — y nunca sin exposición cambiaria (RN-14, D7, D12).
    openingRateMissing: hasFxExposure && (openingRate === null || openingRateEstimated),
    ledgerBalance: projection.reconciliation.ledgerBalance,
    storedBalance: projection.reconciliation.storedBalance,
    difference: projection.reconciliation.difference,
    // El punto más bajo del tramo que se desplaza. El cliente valida con una
    // resta: `minBalanceAfter + diferencia >= 0` (AC-3).
    minBalanceAfter: cleanDecimal(lowest.balance),
    minBalanceDate: lowest.date,
    movementCountAfter: Math.max(affected.length - (opening ? 1 : 0), 0),
    // Fecha de la apertura que se crearía si no hay ninguna (RN-2.7-F, D8).
    newOpeningDate: opening
      ? null
      : (earliestMovementDate
        ? previousDay(earliestMovementDate)
        : (toDateOnly(accountCreatedAt) || new Date().toISOString().substring(0, 10))),
    earliestMovementDate,
    hasFxExposure,
    reconciliationStatus: projection.reconciliation.status,
  };
}

/**
 * Corrige la apertura y recalcula el recorrido entero.
 *
 * Devuelve el parche del asiento (o el documento nuevo, si no había apertura),
 * el fragmento de cuenta con saldo, base y veredicto, y el conflicto de
 * recorrido negativo si lo hay. **No escribe**: quien llama decide si el
 * conflicto bloquea y mete todo en un solo batch (D9).
 *
 * @param {Object} params
 * @param {Object} params.plan - Resultado de `planOpeningCorrection`
 * @param {Array<Object>} params.transactions - Transacciones de la cuenta, con `id`
 * @param {string} params.currency - Divisa del saldo
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {number} params.targetBalance - Saldo que el usuario ve hoy en su bróker
 * @param {string} params.accountId - Id de la cuenta
 * @param {string} params.userId - UID del propietario
 * @param {string} params.openingId - Id del asiento: el existente o el reservado para el nuevo
 * @param {number|null} [params.acquisitionRate] - Tasa de la apertura, la que ya tenía o la declarada
 * @param {string} params.acquisitionRateSource - Origen de esa tasa
 * @param {number} [params.dollarPriceToDate] - Referencia por 1 USD en la fecha de la apertura
 * @param {boolean} [params.estimated=false] - La tasa sigue siendo una estimación
 * @param {string} [params.openingTime] - Hora ISO para la apertura que se crea
 * @returns {{shift: number, previousOpeningAmount: number, newOpeningAmount: number,
 *   cost: number|null, creates: boolean, transactionPatch: Object|null,
 *   transactionData: Object|null, accountUpdate: Object, newBalance: number,
 *   conflict: {date: string|null, balance: number, minimumBalance: number}|null}}
 */
function applyOpeningCorrection({
  plan,
  transactions,
  currency,
  referenceCurrency,
  targetBalance,
  accountId,
  userId,
  openingId,
  acquisitionRate = null,
  acquisitionRateSource,
  dollarPriceToDate = 1,
  estimated = false,
  openingTime = null,
}) {
  // D3: el libro mayor es lineal en el monto de la apertura, así que la
  // diferencia se despeja contra el saldo del historial. Corregir la apertura
  // cierra la deriva en la misma operación (AC-10).
  const shift = cleanDecimal(Number(targetBalance) - plan.ledgerBalance);
  const previousOpeningAmount = plan.openingAmount;
  const newOpeningAmount = cleanDecimal(previousOpeningAmount + shift);

  // La fecha de la apertura no se toca: corregir cuánto había no cambia desde
  // cuándo lo había (RN-2.7-E, AC-9). Sólo la que se crea recibe fecha.
  const openingDate = plan.hasOpening
    ? null
    : `${plan.newOpeningDate}T${openingTime || new Date().toISOString().substring(11)}`;

  const { transactionData } = buildAdjustment({
    // El saldo y la base que devolvería `buildAdjustment` se descartan: los
    // produce el replay (D2). Sólo se usa el documento que construye, para no
    // duplicar la forma del asiento de apertura que fijó 2.6.
    account: { balances: {}, balanceCostBasis: {} },
    accountId,
    userId,
    currency,
    delta: newOpeningAmount,
    date: openingDate || `${plan.openingDate}T00:00:00.000Z`,
    referenceCurrency,
    adjustmentReason: ADJUSTMENT_REASONS.OPENING,
    acquisitionRate,
    acquisitionRateSource,
    dollarPriceToDate,
    estimated,
  });

  const cost = transactionData.acquisitionCost;

  // Campos que cambian del asiento. `date`, `createdAt` y `description` quedan
  // fuera a propósito: la fecha es del usuario (AC-9) y el instante de creación
  // es lo que da orden estable al recorrido.
  const correctedFields = {
    adjustmentDelta: newOpeningAmount,
    amount: Math.abs(newOpeningAmount),
    acquisitionRate: transactionData.acquisitionRate,
    acquisitionRateSource: transactionData.acquisitionRateSource,
    acquisitionCost: cost,
    dollarPriceToDate: transactionData.dollarPriceToDate,
    costBasisEstimated: transactionData.costBasisEstimated,
  };

  // El replay se hace sobre el asiento ya corregido, en memoria. Ni el parche
  // con sus sentinelas de servidor ni la escritura entran aquí.
  const opening = plan.hasOpening
    ? (transactions || []).find((transaction) => transaction.id === openingId)
    : null;

  const replayTransactions = plan.hasOpening
    ? (transactions || []).map((transaction) => (transaction.id === openingId
      ? { ...transaction, ...correctedFields }
      : transaction))
    : [...(transactions || []), { ...transactionData, id: openingId }];

  const projection = projectBalanceLedger({
    transactions: replayTransactions,
    currency,
    referenceCurrency,
    balance: targetBalance,
  });

  // D4: el conflicto se busca en el recorrido ya replayado, no en una fórmula.
  const affected = shiftableRows(projection.rows, openingId);
  const lowest = lowestPoint(affected);

  const conflict = lowest.balance < -EMPTY_BALANCE_EPSILON
    ? {
      date: lowest.date,
      balance: cleanDecimal(lowest.balance, 2),
      // El saldo mínimo que el usuario puede escribir para que el recorrido no
      // baje de cero en ningún punto (AC-3).
      minimumBalance: cleanDecimal(Number(targetBalance) - lowest.balance, 2),
    }
    : null;

  // D2: saldo y base salen del replay. Sin exposición cambiaria no hay base que
  // escribir, igual que decide `buildBalanceUpdate` (RN-14).
  const accountUpdate = {
    [`balances.${currency}`]: projection.reconciliation.ledgerBalance,
    [`balanceReconciliation.${currency}`]: {
      ledgerBalance: projection.reconciliation.ledgerBalance,
      difference: projection.reconciliation.difference,
      status: projection.reconciliation.status,
    },
  };

  if (currency !== referenceCurrency) {
    const replayed = projection.replayedCostBasis;

    accountUpdate[`balanceCostBasis.${currency}`] = replayed
      && replayed.status === COST_BASIS_STATUS.KNOWN
      ? { ...replayed, source: CORRECTION_COST_BASIS_SOURCE }
      // El replay no determina el costo: se declara ausente en lugar de
      // conservar la base anterior, que ya no describe este saldo (RN-13).
      : {
        cost: null,
        referenceCurrency,
        status: COST_BASIS_STATUS.UNKNOWN,
        source: CORRECTION_COST_BASIS_SOURCE,
      };
  }

  return {
    shift,
    previousOpeningAmount,
    newOpeningAmount,
    cost,
    creates: !plan.hasOpening,
    // RN-2.7-G: la apertura corregida declara desde qué valor se corrigió. La
    // que se **crea** no lleva marca: no se modificó nada, se registró el
    // movimiento que faltaba (AC-6).
    transactionPatch: plan.hasOpening
      ? { ...correctedFields, openingCorrectedFrom: previousOpeningAmount }
      : null,
    transactionData: plan.hasOpening ? null : { ...transactionData },
    previousOpeningDate: opening ? toDateOnly(opening.date) : null,
    openingDate: plan.hasOpening ? plan.openingDate : plan.newOpeningDate,
    accountUpdate,
    newBalance: projection.reconciliation.ledgerBalance,
    conflict,
  };
}

module.exports = {
  planOpeningCorrection,
  applyOpeningCorrection,
  CORRECTION_COST_BASIS_SOURCE,
  MIN_CORRECTION_SHIFT,
  // Exportados para test
  _findOpeningTransaction: findOpeningTransaction,
  _previousDay: previousDay,
  _shiftableRows: shiftableRows,
  _lowestPoint: lowestPoint,
  // Reexportado por comodidad de los handlers
  MOVEMENT_KINDS,
  RECONCILIATION_STATUS,
};
