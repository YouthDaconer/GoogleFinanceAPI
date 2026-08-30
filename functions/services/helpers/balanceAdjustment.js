/**
 * HU 2.6 — El asiento de todo saldo que se mueve sin operación detrás.
 *
 * Quedaban tres caminos por los que un saldo cambiaba sin dejar rastro: crear
 * una cuenta con saldos iniciales, editar el monto de un saldo guardado y la
 * acción directa de mover el balance. Los tres pasan ahora por aquí, y aquí
 * siempre se escribe una transacción (RN-06).
 *
 * Un ajuste y una apertura son el mismo hecho contable con distinto origen, así
 * que comparten el tipo `cash_adjustment` y se distinguen por
 * `adjustmentReason` (D6). Ninguno de los dos es flujo de caja del portafolio:
 * la apertura describe dinero que ya estaba y el ajuste corrige un error de
 * registro. Contarlos como aportes movería el rendimiento de todos.
 *
 * **Un ajuste no realiza diferencia en cambio** (D7). RN-2.6-B dice que a la
 * baja consume base "según el promedio vigente", y salir exactamente al
 * promedio da cero por la identidad de 2.5. No es una omisión: un ajuste no es
 * una operación en el bróker, y meterlo en la línea de resultado por diferencia
 * en cambio inventaría un resultado que nadie obtuvo.
 *
 * @module services/helpers/balanceAdjustment
 * @see platform-docs/stories/2.6-libro-mayor-saldo-migracion/refinamiento.md (T3, D6, D7)
 */

const admin = require('../firebaseAdmin');
const historicalRateService = require('../historicalRateService');
const { buildBalanceUpdate } = require('./balanceCostBasis');
const { ADJUSTMENT_REASONS } = require('./balanceLedger');

// ============================================================================
// CONSTANTES
// ============================================================================

/** Tipo de transacción de un ajuste y de una apertura */
const ADJUSTMENT_TYPE = 'cash_adjustment';

/** Origen del tipo de cambio con el que entró el dinero de un ajuste */
const ADJUSTMENT_RATE_SOURCES = {
  /** Lo declaró el usuario */
  USER: 'user',
  /** Tasa de mercado de la fecha del ajuste */
  MARKET_DATE: 'market-date',
  /** La divisa del saldo es la de referencia: no hay tasa que resolver */
  IDENTITY: 'identity',
  /** No se pudo determinar (RN-13) */
  UNAVAILABLE: 'unavailable',
};

/** Por debajo de esto un ajuste no mueve nada y no merece asiento */
const MIN_ADJUSTMENT_DELTA = 0.005;

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

// ============================================================================
// API PÚBLICA
// ============================================================================

/**
 * ¿Este ajuste necesita que alguien diga a qué tipo de cambio entró el dinero?
 *
 * Sólo lo necesita cuando **aumenta** un saldo en divisa distinta de la de
 * referencia, por la misma razón que un ingreso: introduce dinero cuyo costo
 * hay que conocer (RN-2.6-B). Un ajuste a la baja consume base al promedio
 * vigente y no pregunta nada.
 *
 * @param {Object} params
 * @param {number} params.delta - Variación del saldo, con signo
 * @param {string} params.currency - Divisa del saldo
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @returns {boolean}
 */
function requiresExchangeRate({ delta, currency, referenceCurrency }) {
  return delta > 0 && currency !== referenceCurrency;
}

/**
 * Resuelve a qué tipo de cambio entró el dinero de un ajuste.
 *
 * La tasa que declara quien registra el ajuste manda sobre la de mercado: puede
 * saber a qué cambio le entró ese dinero mejor que el proveedor de tasas. Si no
 * la declara, se busca la de la fecha del ajuste. Si tampoco hay, se devuelve
 * `null` y quien llama decide si eso bloquea la operación (RN-05 sí lo hace en
 * un ingreso; crear una cuenta no, D9).
 *
 * @param {Object} params
 * @param {string} params.currency - Divisa del saldo
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {string} params.date - Fecha del ajuste, `YYYY-MM-DD`
 * @param {number|null} [params.declaredRate] - Tasa que declaró el usuario
 * @returns {Promise<{acquisitionRate: number|null, acquisitionRateSource: string,
 *   dollarPriceToDate: number}>}
 */
async function resolveAdjustmentRate({ currency, referenceCurrency, date, declaredRate }) {
  if (currency === referenceCurrency) {
    // Sin exposición cambiaria no hay tasa que resolver (RN-14).
    return {
      acquisitionRate: null,
      acquisitionRateSource: ADJUSTMENT_RATE_SOURCES.IDENTITY,
      dollarPriceToDate: await resolveDollarPriceToDate({
        currency,
        referenceCurrency,
        date,
        acquisitionRate: null,
      }),
    };
  }

  const declared = Number(declaredRate);
  let acquisitionRate = Number.isFinite(declared) && declared > 0 ? declared : null;
  let acquisitionRateSource = acquisitionRate !== null
    ? ADJUSTMENT_RATE_SOURCES.USER
    : ADJUSTMENT_RATE_SOURCES.UNAVAILABLE;

  if (acquisitionRate === null) {
    const resolved = await historicalRateService.getCrossRate(currency, referenceCurrency, date);

    if (resolved !== null) {
      acquisitionRate = resolved.rate;
      acquisitionRateSource = ADJUSTMENT_RATE_SOURCES.MARKET_DATE;
    }
  }

  return {
    acquisitionRate,
    acquisitionRateSource,
    dollarPriceToDate: await resolveDollarPriceToDate({
      currency,
      referenceCurrency,
      date,
      acquisitionRate,
    }),
  };
}

/**
 * `dollarPriceToDate` en la semántica que `convertCurrency` exige en el cliente:
 * unidades de la moneda de referencia por 1 USD. Misma resolución que
 * `addCashTransaction` desde 2.1 (D6 de aquella historia).
 *
 * @param {Object} params
 * @param {string} params.currency - Divisa del movimiento
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {string} params.date - Fecha del movimiento, `YYYY-MM-DD`
 * @param {number|null} params.acquisitionRate - Tasa ya resuelta de la divisa
 * @returns {Promise<number>}
 */
async function resolveDollarPriceToDate({ currency, referenceCurrency, date, acquisitionRate }) {
  if (referenceCurrency === 'USD') return 1;

  if (currency === 'USD' && acquisitionRate !== null) return acquisitionRate;

  const referenceRate = await historicalRateService.getRateForDate(referenceCurrency, date);

  return referenceRate !== null ? referenceRate.rate : 1;
}

/**
 * Construye el asiento de un ajuste y el fragmento que mueve el saldo, para que
 * los dos entren en el mismo batch.
 *
 * Es puro: no lee ni escribe nada. Quien llama resuelve antes la tasa
 * (`resolveAdjustmentRate`) y decide si su ausencia bloquea la operación.
 *
 * @param {Object} params
 * @param {Object} params.account - Documento actual de la cuenta
 * @param {string} params.accountId - Id de la cuenta
 * @param {string} params.userId - UID del propietario
 * @param {string} params.currency - Divisa del saldo
 * @param {number} params.delta - Variación del saldo, con signo
 * @param {string} params.date - Fecha del ajuste en ISO (con hora)
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {string} [params.adjustmentReason='manual'] - `opening` o `manual`
 * @param {string} [params.description] - Motivo que escribió el usuario
 * @param {number|null} [params.acquisitionRate] - Tasa a la que entró el dinero
 * @param {string} [params.acquisitionRateSource] - Origen de esa tasa
 * @param {number} [params.dollarPriceToDate=1] - Referencia por 1 USD en la fecha
 * @param {boolean} [params.estimated=false] - La tasa la estimó la migración
 * @returns {{transactionData: Object, balanceUpdate: Object, newBalance: number,
 *   cost: number|null}}
 */
function buildAdjustment({
  account,
  accountId,
  userId,
  currency,
  delta,
  date,
  referenceCurrency,
  adjustmentReason = ADJUSTMENT_REASONS.MANUAL,
  description = '',
  acquisitionRate = null,
  acquisitionRateSource = ADJUSTMENT_RATE_SOURCES.UNAVAILABLE,
  dollarPriceToDate = 1,
  estimated = false,
}) {
  const signedDelta = cleanDecimal(delta);
  const isInflow = signedDelta > 0;
  const hasFxExposure = currency !== referenceCurrency;

  // El costo sólo se conoce en una entrada con tasa. En una salida lo retira
  // `buildBalanceUpdate` a la tasa promedio vigente, que es lo que dice RN-2.6-B.
  const cost = isInflow && acquisitionRate !== null
    ? cleanDecimal(Math.abs(signedDelta) * acquisitionRate, 2)
    : null;

  const isOpening = adjustmentReason === ADJUSTMENT_REASONS.OPENING;

  const transactionData = {
    assetName: isOpening
      ? `Saldo de apertura de ${currency}`
      : `Ajuste de saldo de ${currency}`,
    type: ADJUSTMENT_TYPE,
    // Distingue la apertura de la corrección manual sin multiplicar tipos (D6).
    adjustmentReason,
    // El delta lleva el signo; `amount` mantiene el valor absoluto que todos los
    // lectores actuales de transacciones de efectivo esperan encontrar.
    adjustmentDelta: signedDelta,
    amount: Math.abs(signedDelta),
    price: 1,
    currency,
    date,
    portfolioAccountId: accountId,
    commission: 0,
    assetType: 'cash',
    dollarPriceToDate: cleanDecimal(dollarPriceToDate),
    defaultCurrencyForAdquisitionDollar: referenceCurrency,
    acquisitionRate: acquisitionRate !== null ? cleanDecimal(acquisitionRate) : null,
    acquisitionRateSource,
    acquisitionCost: cost,
    referenceCurrency,
    // Marca la base que salió de una estimación de la migración, para que la
    // interfaz la rotule como tal allí donde se muestre (RN-12, AC-7).
    costBasisEstimated: estimated,
    description: description || '',
    userId,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  // Un ajuste no realiza diferencia en cambio (D7). Sin exposición cambiaria no
  // se escribe ni el campo, igual que hace 2.5 con el retiro (RN-14).
  if (hasFxExposure) {
    transactionData.realizedFxAmount = null;
    transactionData.realizedFxCurrency = referenceCurrency;
    transactionData.realizedFxAvailability = 'not-applicable';
    transactionData.realizedFxUnavailableReason = null;
  }

  const balanceUpdate = buildBalanceUpdate({
    account,
    currency,
    amountDelta: signedDelta,
    costDelta: isInflow ? cost : null,
    referenceCurrency,
  });

  return {
    transactionData,
    balanceUpdate,
    newBalance: balanceUpdate[`balances.${currency}`],
    cost,
  };
}

module.exports = {
  buildAdjustment,
  resolveAdjustmentRate,
  requiresExchangeRate,
  ADJUSTMENT_TYPE,
  ADJUSTMENT_RATE_SOURCES,
  MIN_ADJUSTMENT_DELTA,
  // Reexportado por comodidad de los handlers
  ADJUSTMENT_REASONS,
  // Exportado para test
  _resolveDollarPriceToDate: resolveDollarPriceToDate,
};
