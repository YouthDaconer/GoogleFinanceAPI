/**
 * HU 2.6 — El libro mayor de un saldo.
 *
 * Un saldo deja de ser un número que el producto mantiene y pasa a ser la
 * **proyección de sus movimientos** (RN-06). Este módulo es esa proyección:
 * dado el conjunto de transacciones de una cuenta, reconstruye el recorrido de
 * una de sus divisas movimiento a movimiento, y dice a qué saldo y a qué tasa
 * promedio se llega después de cada uno.
 *
 * Tres propiedades que lo hacen utilizable por sus tres consumidores —el
 * historial que se despliega, la reconciliación y el replay de la migración—
 * sin que ninguno pueda contradecir a los otros (D1):
 *
 * - **El historial de un saldo NO es su historial de efectivo** (D2). Lo que
 *   más mueve un saldo es la compra de un activo, y la pestaña que existía
 *   filtraba por `assetType == 'cash'`, así que no la mostraba. Aquí entran los
 *   siete tipos que mueven caja.
 * - **La aritmética del costo no se reimplementa**: cada movimiento se replaya
 *   por el MISMO `buildBalanceUpdate` que escribe en producción (D3). Si algún
 *   día cambia la regla del promedio ponderado, cambia en un solo sitio y el
 *   historial la sigue sin enterarse.
 * - **Nada se persiste desde aquí**. El saldo acumulado y la tasa promedio se
 *   derivan (RN-02); quien quiera guardar el veredicto de conciliación lo hace
 *   con el resultado, no dentro de este módulo.
 *
 * @module services/helpers/balanceLedger
 * @see platform-docs/stories/2.6-libro-mayor-saldo-migracion/refinamiento.md (T1, D1, D2, D3)
 */

const {
  buildBalanceUpdate,
  deriveAverageRate,
  COST_BASIS_STATUS,
  EMPTY_BALANCE_EPSILON,
} = require('./balanceCostBasis');

// ============================================================================
// CONSTANTES
// ============================================================================

/**
 * Clase de movimiento tal como se lee **desde el saldo que se está mirando**.
 *
 * No es el `type` del documento: una conversión es un solo documento que se lee
 * como salida desde una divisa y como entrada desde la otra (RN-2.2-A). La
 * interfaz traduce estas claves a lenguaje llano; aquí no hay textos.
 */
const MOVEMENT_KINDS = {
  /** Saldo de apertura: de dónde venía el dinero que ya estaba */
  OPENING: 'opening',
  /** Corrección manual del saldo */
  ADJUSTMENT: 'adjustment',
  INCOME: 'income',
  EXPENSE: 'expense',
  /** Conversión vista desde la divisa que salió */
  CONVERSION_OUT: 'conversion-out',
  /** Conversión vista desde la divisa que entró */
  CONVERSION_IN: 'conversion-in',
  /** Compra de un activo: el efectivo sale de este saldo */
  BUY: 'buy',
  /** Venta de un activo: el producto entra a este saldo */
  SELL: 'sell',
  /** Pago de dividendo, neto de retención */
  DIVIDEND: 'dividend',
};

/** Veredicto de la comparación entre el saldo guardado y su libro mayor */
const RECONCILIATION_STATUS = {
  /** El saldo coincide con la suma de sus movimientos */
  RECONCILED: 'reconciled',
  /** No coincide: hay una diferencia sin explicar (AC-3) */
  DRIFT: 'drift',
};

/**
 * Motivo por el que un ajuste registró un movimiento.
 *
 * Una apertura y una corrección manual son el mismo hecho contable con distinto
 * origen, así que comparten el tipo `cash_adjustment` y se distinguen aquí (D6).
 */
const ADJUSTMENT_REASONS = {
  OPENING: 'opening',
  MANUAL: 'manual',
};

// ============================================================================
// HELPERS INTERNOS
// ============================================================================

/**
 * Limpia decimales con la misma convención que el resto del backend.
 *
 * @param {number} num - Número a limpiar
 * @param {number} [decimals=8] - Decimales a conservar
 * @returns {number}
 */
function cleanDecimal(num, decimals = 8) {
  return Number(Math.round(Number(num + 'e' + decimals)) / 10 ** decimals);
}

/**
 * Convierte a número, o `null` si no lo es. Un `0` legítimo sobrevive.
 *
 * La ausencia se comprueba ANTES de convertir: `Number(null)` es `0`, y tomar
 * ese cero por un costo conocido haría crecer el saldo sin que crezca su costo
 * — exactamente la mentira que RN-13 prohíbe.
 *
 * @param {*} value - Valor a interpretar
 * @returns {number|null}
 */
function toNumber(value) {
  if (value === null || value === undefined || value === '') return null;

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Una tasa sirve si es un número positivo. Cero y negativo no son tasas.
 *
 * @param {*} value - Valor a interpretar
 * @returns {number|null}
 */
function toRate(value) {
  const parsed = toNumber(value);
  return parsed !== null && parsed > 0 ? parsed : null;
}

/**
 * Día del movimiento en formato `YYYY-MM-DD`.
 *
 * @param {*} date - Campo `date` del documento (ISO o `YYYY-MM-DD`)
 * @returns {string} Día, o cadena vacía si el documento no lo trae
 */
function toDateOnly(date) {
  if (!date) return '';
  return String(date).substring(0, 10);
}

/**
 * Clave de orden cronológico estable.
 *
 * Dos movimientos del mismo día tienen que ordenarse igual en cada ejecución,
 * porque el promedio ponderado de una entrada y la salida proporcional que le
 * sigue no dan lo mismo invertidos. Se desempata por el instante de creación y,
 * a falta de él, por el índice de lote y el identificador.
 *
 * @param {Object} transaction - Documento de la transacción
 * @returns {string} Clave comparable lexicográficamente
 */
function sortKey(transaction) {
  const date = String(transaction.date || '');
  const createdAt = transaction.createdAt;

  let createdMillis = 0;
  if (createdAt && typeof createdAt.toMillis === 'function') {
    createdMillis = createdAt.toMillis();
  } else if (createdAt && typeof createdAt._seconds === 'number') {
    createdMillis = createdAt._seconds * 1000;
  } else if (createdAt instanceof Date) {
    createdMillis = createdAt.getTime();
  }

  const lot = typeof transaction.lotIndex === 'number' ? transaction.lotIndex : 0;

  return [
    date.padEnd(30, '0'),
    String(createdMillis).padStart(16, '0'),
    String(lot).padStart(6, '0'),
    String(transaction.id || ''),
  ].join('|');
}

/**
 * Aplica al estado en memoria el fragmento que `buildBalanceUpdate` habría
 * escrito en Firestore, para poder encadenar el siguiente movimiento sobre él.
 *
 * @param {Object} state - Estado con `balances` y `balanceCostBasis`
 * @param {Object} update - Fragmento devuelto por `buildBalanceUpdate`
 * @param {string} currency - Divisa que se está proyectando
 * @returns {Object} Estado nuevo
 */
function applyUpdate(state, update, currency) {
  const next = {
    balances: { ...state.balances },
    balanceCostBasis: { ...state.balanceCostBasis },
  };

  const balancePath = `balances.${currency}`;
  const basisPath = `balanceCostBasis.${currency}`;

  if (Object.prototype.hasOwnProperty.call(update, balancePath)) {
    next.balances[currency] = update[balancePath];
  }

  if (Object.prototype.hasOwnProperty.call(update, basisPath)) {
    next.balanceCostBasis[currency] = update[basisPath];
  }

  return next;
}

// ============================================================================
// API PÚBLICA
// ============================================================================

/**
 * Traduce un documento de transacción al movimiento que produjo sobre un saldo.
 *
 * Es la tabla de D2 hecha código: cada tipo dice cuánto movió la caja de esa
 * divisa y a qué tasa entró o salió ese dinero. Devuelve `null` cuando el
 * documento no toca ese saldo, que es el caso mayoritario.
 *
 * Convenio de la tasa: **unidades de la moneda de referencia por 1 unidad de la
 * divisa del saldo**, el mismo que usan 2.1, 2.2 y 2.3.
 *
 * @param {Object} transaction - Documento de `transactions`
 * @param {string} currency - Divisa del saldo desde el que se mira
 * @returns {{kind: string, amount: number, appliedRate: number|null,
 *   costDelta: number|null, counterpartCurrency: string|null,
 *   assetName: string|null, adjustmentReason: string|null,
 *   estimated: boolean}|null}
 */
function resolveCashImpact(transaction, currency) {
  if (!transaction || !currency) return null;

  const type = transaction.type;
  const amount = toNumber(transaction.amount) || 0;
  const price = toNumber(transaction.price);
  const commission = toNumber(transaction.commission) || 0;
  const acquisitionCost = toNumber(transaction.acquisitionCost);
  const acquisitionRate = toRate(transaction.acquisitionRate);
  const realizationRate = toRate(transaction.realizationRate);

  const base = {
    counterpartCurrency: null,
    assetName: transaction.assetName || transaction.symbol || null,
    adjustmentReason: null,
    estimated: false,
  };

  // --- Conversión: un documento, dos saldos (RN-2.2-A) --------------------
  if (type === 'cash_conversion') {
    const toCurrency = transaction.toCurrency;
    const toAmount = toNumber(transaction.toAmount) || 0;

    if (transaction.currency === currency) {
      // Sale: el costo que se libera lo retira `buildBalanceUpdate` a la tasa
      // promedio vigente. La tasa que se muestra es la que ese retiro implica.
      const releasedCost = toNumber(transaction.releasedCost);

      return {
        ...base,
        kind: MOVEMENT_KINDS.CONVERSION_OUT,
        amount: -amount,
        appliedRate: releasedCost !== null && amount > 0
          ? cleanDecimal(releasedCost / amount, 6)
          : null,
        costDelta: null,
        counterpartCurrency: toCurrency || null,
      };
    }

    if (toCurrency === currency) {
      // Entra: la tasa es la de valoración de lo que se entregó (2.2, D3).
      return {
        ...base,
        kind: MOVEMENT_KINDS.CONVERSION_IN,
        amount: toAmount,
        appliedRate: acquisitionRate !== null
          ? acquisitionRate
          : (acquisitionCost !== null && toAmount > 0
            ? cleanDecimal(acquisitionCost / toAmount, 6)
            : null),
        costDelta: acquisitionCost,
        counterpartCurrency: transaction.currency || null,
      };
    }

    return null;
  }

  // El resto de los tipos sólo tocan su propia divisa.
  if (transaction.currency !== currency) return null;

  switch (type) {
    case 'cash_income':
      return {
        ...base,
        kind: MOVEMENT_KINDS.INCOME,
        amount,
        appliedRate: acquisitionRate,
        costDelta: acquisitionCost,
      };

    case 'cash_expense':
      return {
        ...base,
        kind: MOVEMENT_KINDS.EXPENSE,
        amount: -amount,
        // 2.5 le dio nombre propio a la tasa de salida; antes de 2.5 el retiro
        // sólo tenía `acquisitionRate`, que describe el mismo número.
        appliedRate: realizationRate !== null ? realizationRate : acquisitionRate,
        costDelta: null,
      };

    case 'cash_adjustment': {
      const delta = toNumber(transaction.adjustmentDelta);
      const isOpening = transaction.adjustmentReason === ADJUSTMENT_REASONS.OPENING;

      return {
        ...base,
        kind: isOpening ? MOVEMENT_KINDS.OPENING : MOVEMENT_KINDS.ADJUSTMENT,
        // El delta lleva su propio signo; `amount` guarda su valor absoluto para
        // los lectores que ya interpretan ese campo.
        amount: delta !== null ? delta : amount,
        appliedRate: acquisitionRate,
        costDelta: acquisitionCost,
        adjustmentReason: transaction.adjustmentReason || ADJUSTMENT_REASONS.MANUAL,
        estimated: transaction.costBasisEstimated === true,
      };
    }

    case 'buy':
      // El efectivo que sale es el valor de la compra más su comisión.
      return {
        ...base,
        kind: MOVEMENT_KINDS.BUY,
        amount: -cleanDecimal(amount * (price !== null ? price : 0) + commission),
        appliedRate: acquisitionRate,
        costDelta: null,
      };

    case 'sell':
      // Entra el producto neto de comisión, con el tipo de cambio del día de la
      // venta (2.4). En una venta multi-lote cada documento trae su parte.
      return {
        ...base,
        kind: MOVEMENT_KINDS.SELL,
        amount: cleanDecimal(amount * (price !== null ? price : 0) - commission),
        appliedRate: realizationRate,
        costDelta: acquisitionCost,
      };

    case 'dividendPay':
      // `price` ya es el neto por unidad tras la retención fiscal.
      return {
        ...base,
        kind: MOVEMENT_KINDS.DIVIDEND,
        amount: cleanDecimal(amount * (price !== null ? price : 0)),
        appliedRate: realizationRate,
        costDelta: acquisitionCost,
      };

    default:
      // Un tipo que no mueve caja (o uno futuro que aún no se ha declarado) no
      // entra en el libro mayor en lugar de entrar con delta cero: una fila de
      // cero afirmaría que el movimiento existió y no movió nada.
      return null;
  }
}

/**
 * Reconstruye el libro mayor de un saldo y lo compara con el saldo guardado.
 *
 * El recorrido va hacia adelante —de la apertura a hoy— porque el promedio
 * ponderado no es reversible; las filas se devuelven **de más reciente a más
 * antigua**, que es como las pide AC-1.
 *
 * Sin exposición cambiaria (la divisa del saldo es la de referencia) las tasas
 * salen en `null` y la interfaz no pinta esas dos columnas (RN-14, D15).
 *
 * @param {Object} params
 * @param {Array<Object>} params.transactions - Transacciones de la cuenta, con `id`
 * @param {string} params.currency - Divisa del saldo a proyectar
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {number} [params.balance=0] - Saldo guardado hoy, para el veredicto
 * @returns {{rows: Array<Object>, reconciliation: Object, hasFxExposure: boolean}}
 */
function projectBalanceLedger({ transactions, currency, referenceCurrency, balance = 0 }) {
  const hasFxExposure = currency !== referenceCurrency;

  const movements = (transactions || [])
    .map((transaction) => {
      const impact = resolveCashImpact(transaction, currency);
      return impact ? { transaction, impact } : null;
    })
    .filter(Boolean)
    .sort((a, b) => (sortKey(a.transaction) < sortKey(b.transaction) ? -1 : 1));

  let state = { balances: { [currency]: 0 }, balanceCostBasis: {} };
  const rows = [];

  for (const { transaction, impact } of movements) {
    const update = buildBalanceUpdate({
      account: state,
      currency,
      amountDelta: impact.amount,
      costDelta: impact.amount > 0 ? impact.costDelta : null,
      referenceCurrency,
    });

    state = applyUpdate(state, update, currency);

    const basis = state.balanceCostBasis[currency];
    const balanceAfter = state.balances[currency];

    rows.push({
      id: transaction.id || null,
      date: toDateOnly(transaction.date),
      type: transaction.type,
      kind: impact.kind,
      amount: cleanDecimal(impact.amount),
      appliedRate: hasFxExposure ? impact.appliedRate : null,
      balanceAfter: cleanDecimal(balanceAfter),
      averageRateAfter: hasFxExposure
        ? deriveAverageRate(basis, balanceAfter, referenceCurrency)
        : null,
      costStatus: basis ? basis.status : COST_BASIS_STATUS.KNOWN,
      counterpartCurrency: impact.counterpartCurrency,
      assetName: impact.assetName,
      adjustmentReason: impact.adjustmentReason,
      estimated: impact.estimated,
      description: transaction.description || null,
    });
  }

  const ledgerBalance = cleanDecimal(state.balances[currency] || 0);
  const storedBalance = cleanDecimal(toNumber(balance) || 0);
  const difference = cleanDecimal(storedBalance - ledgerBalance, 2);

  return {
    // De más reciente a más antiguo (AC-1). El recorrido fue al revés porque el
    // promedio ponderado sólo se puede acumular hacia adelante.
    rows: rows.reverse(),
    hasFxExposure,
    reconciliation: {
      ledgerBalance,
      storedBalance,
      difference,
      status: Math.abs(difference) < EMPTY_BALANCE_EPSILON
        ? RECONCILIATION_STATUS.RECONCILED
        : RECONCILIATION_STATUS.DRIFT,
      movementCount: rows.length,
    },
    // Costo que el replay del historial es capaz de justificar. La migración lo
    // usa para reconstruir la base; `null` cuando el historial no alcanza a
    // determinarlo, que es distinto de que valga cero (RN-13).
    replayedCostBasis: state.balanceCostBasis[currency] || null,
  };
}

/**
 * ¿Este documento mueve la caja de alguna divisa de la cuenta?
 *
 * Sirve para acotar lo que hay que leer y para saber, al borrar un activo, qué
 * efectivo hay que devolver (D12).
 *
 * @param {Object} transaction - Documento de `transactions`
 * @returns {boolean}
 */
function movesCash(transaction) {
  if (!transaction || !transaction.type) return false;

  return [
    'cash_income',
    'cash_expense',
    'cash_conversion',
    'cash_adjustment',
    'buy',
    'sell',
    'dividendPay',
  ].includes(transaction.type);
}

module.exports = {
  projectBalanceLedger,
  resolveCashImpact,
  movesCash,
  MOVEMENT_KINDS,
  RECONCILIATION_STATUS,
  ADJUSTMENT_REASONS,
  // Exportados para test
  _sortKey: sortKey,
  _toDateOnly: toDateOnly,
};
