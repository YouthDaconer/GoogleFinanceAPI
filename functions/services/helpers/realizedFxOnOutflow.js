/**
 * HU 2.5 — Sacar dinero realiza la diferencia en cambio.
 *
 * Mientras la divisa está en la cuenta, lo que el tipo de cambio le hace al
 * saldo es una revaluación: sube y baja, pero no es un resultado. En el momento
 * en que la divisa **sale** —por un retiro o por una conversión— esa revaluación
 * deja de ser potencial y se convierte en un hecho (RN-07).
 *
 * Este módulo es el único sitio donde se calcula esa cifra. La identidad es una
 * resta, no un reparto:
 *
 *     diferencia en cambio = monto × (r_salida − r_promedio)
 *                          = valor de salida − costo liberado
 *
 * Con `r_promedio` = tasa promedio de adquisición del saldo (`cost / balance`,
 * RN-02) y `r_salida` = tasa a la que sale la divisa el día de la operación.
 *
 * **Quién escribe el saldo**: nadie de aquí. `buildBalanceUpdate` retira
 * `monto × r_promedio` de la base de costo desde 2.1, de modo que la tasa
 * promedio del remanente no cambia (RN-08). Este módulo replica esa lectura
 * **sólo para informar** la cifra; la escritura sigue siendo suya y de nadie
 * más — el mismo reparto que 2.2 estableció.
 *
 * @module services/helpers/realizedFxOnOutflow
 * @see platform-docs/stories/2.5-diferencia-cambio-salidas-efectivo/refinamiento.md (D1, D2, D3, D4, D11)
 */

const { deriveAverageRate } = require('./balanceCostBasis');

// ============================================================================
// CONSTANTES
// ============================================================================

/** Disponibilidad de la diferencia en cambio de una salida */
const OUTFLOW_FX_AVAILABILITY = {
  /** Se conocen las dos tasas: la cifra es exacta */
  AVAILABLE: 'available',
  /** Falta un dato para calcularla — se declara, nunca se rellena con cero */
  UNAVAILABLE: 'unavailable',
  /** La divisa que sale ES la de referencia: no hay diferencia en cambio que realizar */
  NOT_APPLICABLE: 'not-applicable',
};

/** Motivo por el que la diferencia en cambio de una salida no está disponible */
const OUTFLOW_UNAVAILABLE_REASONS = {
  /** No se pudo determinar el tipo de cambio del día de la salida */
  MISSING_REALIZATION_RATE: 'missing-realization-rate',
  /** El saldo no tiene una base de costo utilizable: no hay tasa promedio contra la que comparar */
  UNKNOWN_COST_BASIS: 'unknown-cost-basis',
};

/** Origen de la tasa a la que sale la divisa */
const OUTFLOW_RATE_SOURCES = {
  /** La declaró el usuario en el formulario */
  USER: 'user',
  /** Tasa de mercado de la fecha */
  MARKET_DATE: 'market-date',
  /** La divisa que sale es la de referencia */
  IDENTITY: 'identity',
  /** No se pudo determinar */
  UNAVAILABLE: 'unavailable',
};

// ============================================================================
// HELPERS INTERNOS
// ============================================================================

/**
 * Redondea a la precisión monetaria del producto, con la misma convención que
 * `cleanDecimal` de assetHandlers y `round2` de balanceCostBasis.
 *
 * @param {number} value - Valor a redondear
 * @returns {number}
 */
function round2(value) {
  return Number(Math.round(Number(value + 'e2')) / 100);
}

/**
 * Comprueba que un valor sirve como tasa: número finito y estrictamente positivo.
 *
 * @param {*} value - Valor a comprobar
 * @returns {boolean}
 */
function isUsableRate(value) {
  return typeof value === 'number' && Number.isFinite(value) && value > 0;
}

// ============================================================================
// API PÚBLICA
// ============================================================================

/**
 * Calcula la diferencia en cambio que realiza una salida de divisa de la cuenta.
 *
 * Casos:
 * - **Sin exposición cambiaria** (`currency === referenceCurrency`): no hay nada
 *   que realizar. Devuelve `not-applicable` con `realizedFxAmount: null`. No se
 *   devuelve cero, porque un cero afirma "la divisa no aportó nada" y aquí no
 *   hay divisa de la que hablar (RN-14, AC-4).
 * - **Sin tasa del día**: `unavailable` con `missing-realization-rate`. La
 *   operación se registra igual — un retiro es un hecho consumado en el bróker
 *   (D4), la misma asimetría que 2.4 aplica a las ventas.
 * - **Sin base de costo utilizable**: `unavailable` con `unknown-cost-basis`.
 *   Sin tasa promedio no hay contra qué comparar la del día.
 *
 * @param {Object} params
 * @param {Object} params.account - Documento actual de la cuenta
 * @param {string} params.currency - Divisa que sale
 * @param {number} params.amount - Monto que sale, en unidades de `currency` (positivo)
 * @param {number|null} params.outflowRate - Unidades de la moneda de referencia por 1 de `currency`, el día de la salida
 * @param {string} params.referenceCurrency - Moneda de referencia vigente del usuario
 * @returns {{averageRate: number|null, releasedCost: number|null, outflowValue: number|null,
 *   realizedFxAmount: number|null, availability: string, unavailableReason: string|null}}
 */
function computeOutflowRealizedFx({ account, currency, amount, outflowRate, referenceCurrency }) {
  // `releasedCost` se informa siempre que se conozca, aunque falte la tasa del
  // día: lo que ese dinero costó es un dato que sí tenemos, y perderlo obligaría
  // a los lectores del documento a reconstruirlo.
  const unavailable = (reason, averageRate = null, releasedCost = null) => ({
    averageRate,
    releasedCost,
    outflowValue: null,
    realizedFxAmount: null,
    availability: OUTFLOW_FX_AVAILABILITY.UNAVAILABLE,
    unavailableReason: reason,
  });

  // RN-14: el efectivo en la propia moneda de referencia no realiza nada.
  if (currency === referenceCurrency) {
    return {
      averageRate: null,
      releasedCost: null,
      outflowValue: null,
      realizedFxAmount: null,
      availability: OUTFLOW_FX_AVAILABILITY.NOT_APPLICABLE,
      unavailableReason: null,
    };
  }

  const currentBalance = account?.balances?.[currency] || 0;
  const averageRate = deriveAverageRate(
    account?.balanceCostBasis?.[currency],
    currentBalance,
    referenceCurrency
  );

  if (averageRate === null) {
    return unavailable(OUTFLOW_UNAVAILABLE_REASONS.UNKNOWN_COST_BASIS);
  }

  const movedAmount = Math.abs(Number(amount) || 0);

  if (!isUsableRate(outflowRate)) {
    return unavailable(
      OUTFLOW_UNAVAILABLE_REASONS.MISSING_REALIZATION_RATE,
      averageRate,
      round2(movedAmount * averageRate)
    );
  }

  // La resta se hace sobre los dos valores SIN redondear y se redondea una sola
  // vez al final, que es como 2.2 la venía calculando. Redondear las dos puntas
  // por separado movería la cifra que esa historia ya persistió.
  const outflowValue = movedAmount * outflowRate;
  const releasedCost = movedAmount * averageRate;

  return {
    averageRate,
    releasedCost: round2(releasedCost),
    outflowValue: round2(outflowValue),
    realizedFxAmount: round2(outflowValue - releasedCost),
    availability: OUTFLOW_FX_AVAILABILITY.AVAILABLE,
    unavailableReason: null,
  };
}

/**
 * Construye los campos de trazabilidad que se persisten en el documento de una
 * salida de efectivo (`cash_expense`) o de una conversión (`cash_conversion`).
 *
 * Los campos son aditivos: ningún lector actual del documento cambia.
 *
 * **Sin exposición cambiaria no se escribe ni un campo** (D11): el documento de
 * un retiro en la propia moneda de referencia queda exactamente como el de hoy,
 * que es lo que AC-4 exige.
 *
 * @param {Object} params
 * @param {Object} params.outcome - Resultado de `computeOutflowRealizedFx`
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {number|null} params.outflowRate - Tasa del día de la salida
 * @param {string|null} params.outflowRateSource - Origen de esa tasa
 * @returns {Object} Fragmento de campos para el documento de la transacción
 */
function buildOutflowFxFields({ outcome, referenceCurrency, outflowRate, outflowRateSource }) {
  if (outcome.availability === OUTFLOW_FX_AVAILABILITY.NOT_APPLICABLE) {
    return {};
  }

  return {
    referenceCurrency,
    // Seis decimales: suficiente para cualquier par de divisas y sin arrastrar
    // la basura del flotante al documento. (2.4 escribe `round2(r * 1e6) / 1e6`,
    // que por la escala deja ocho; aquí se redondea a los seis que dice hacer.)
    realizationRate: isUsableRate(outflowRate) ? Math.round(outflowRate * 1e6) / 1e6 : null,
    realizationRateSource: outflowRateSource || OUTFLOW_RATE_SOURCES.UNAVAILABLE,
    releasedCost: outcome.releasedCost,
    realizedFxAmount: outcome.realizedFxAmount,
    realizedFxCurrency: referenceCurrency,
    realizedFxAvailability: outcome.availability,
    realizedFxUnavailableReason: outcome.unavailableReason,
  };
}

module.exports = {
  computeOutflowRealizedFx,
  buildOutflowFxFields,
  OUTFLOW_FX_AVAILABILITY,
  OUTFLOW_UNAVAILABLE_REASONS,
  OUTFLOW_RATE_SOURCES,
  // Exportados para test
  _round2: round2,
};
