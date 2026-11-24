/**
 * HU 2.8 — La tasa que necesita la base de un saldo cuando nadie la declaró.
 *
 * `buildBalanceUpdate` es puro: decide **si** un movimiento necesita una tasa
 * de mercado para mantener la base (`basisValuationNeed`), pero no la consulta.
 * Este módulo es el paso asíncrono que va antes. Lo usan todos los puntos que
 * mueven un saldo y el script de corrección, para que la regla viva en un solo
 * sitio.
 *
 * Se pide tasa **solo** para lo que nunca tuvo una declarada en la moneda de la
 * base (RN-2.8-C):
 * - el dinero que entra mientras el usuario usa otra moneda de referencia
 *   (RN-2.8-D);
 * - el dinero que ya estaba en un saldo cuya base se reubica (RN-2.8-E).
 *
 * La tasa es el cierre de mercado del día del movimiento, con el margen de días
 * no hábiles del canal de tasas: la misma consulta que valora ventas y
 * dividendos desde la HU 2.4 (`resolveRealizationRate`). El resultado se guarda
 * en el propio movimiento (`basisValuation`, RN-3-B), así el libro mayor lo
 * reproduce sin volver a preguntar y el pasado no cambia.
 *
 * @module services/helpers/basisValuation
 * @see platform-docs/stories/2.8-base-costo-cambio-referencia/refinamiento.md (T2)
 */

const { basisValuationNeed, isUsableRate } = require('./balanceCostBasis');
const { resolveRealizationRate } = require('./realizedFxDecomposition');

/** De dónde salió la tasa de una valoración */
const BASIS_VALUATION_SOURCES = {
  /** Cierre de mercado del día del movimiento (o el hábil anterior) */
  MARKET_DATE: 'market-date',
  /** No había tasa: la base se declara desconocida (RN-2.8-F) */
  UNAVAILABLE: 'unavailable',
};

/**
 * Resuelve la valoración que pide la base para este movimiento.
 *
 * @param {Object} params
 * @param {Object} params.account - Documento actual de la cuenta
 * @param {string} params.currency - Divisa del saldo que se mueve
 * @param {number} params.amountDelta - Variación del saldo
 * @param {number|null} [params.costDelta] - Costo declarado de la entrada, en la referencia vigente
 * @param {string} params.referenceCurrency - Moneda de referencia vigente
 * @param {string} params.date - Fecha del movimiento, ISO con o sin hora
 * @returns {Promise<{referenceCurrency: string, rate: number|null, rateDate: string|null,
 *   source: string}|null>} `null` si el movimiento no necesita tasa
 */
async function resolveBasisValuation({
  account,
  currency,
  amountDelta,
  costDelta = null,
  referenceCurrency,
  date,
}) {
  const target = basisValuationNeed({ account, currency, amountDelta, costDelta, referenceCurrency });
  if (!target) return null;

  let resolved = null;
  try {
    resolved = await resolveRealizationRate(currency, target, date);
  } catch (error) {
    // La ausencia se declara (RN-2.8-F): el movimiento se registra igual.
    console.warn(`[basisValuation] Sin tasa ${currency}→${target} para ${date}:`, error.message);
  }

  const rate = resolved?.realizationRate;

  if (!isUsableRate(rate)) {
    return {
      referenceCurrency: target,
      rate: null,
      rateDate: null,
      source: BASIS_VALUATION_SOURCES.UNAVAILABLE,
    };
  }

  return {
    referenceCurrency: target,
    rate,
    rateDate: resolved.realizationRateDate || null,
    source: BASIS_VALUATION_SOURCES.MARKET_DATE,
  };
}

/**
 * Fragmento del documento del movimiento con la valoración usada. Vacío cuando
 * no hubo valoración, para que quien nunca cambia de moneda de referencia siga
 * escribiendo exactamente el mismo documento (RN-2.8-G).
 *
 * @param {Object|null} valuation - Resultado de `resolveBasisValuation`
 * @returns {Object}
 */
function basisValuationFields(valuation) {
  return valuation ? { basisValuation: valuation } : {};
}

module.exports = {
  resolveBasisValuation,
  basisValuationFields,
  BASIS_VALUATION_SOURCES,
};
