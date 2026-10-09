/**
 * HU 2.4 — El dinero vuelve con la tasa de su propio día.
 *
 * Cuando el usuario vende, el sistema registraba la operación con el tipo de
 * cambio de la **compra**, de modo que la ganancia cambiaria realizada no
 * existía en ninguna parte: lo invertido y lo recibido se convertían con la
 * misma tasa y el resultado en moneda local era, literalmente, el resultado en
 * dólares multiplicado por la tasa de compra.
 *
 * Este módulo es el único sitio donde se decide la tasa de cada punta de la
 * operación y donde se descompone el resultado realizado.
 *
 * **La descomposición es una identidad algebraica, no un reparto** (RN-10). Con
 * `p` = producto de la venta, `i` = inversión, `rc` = tasa de compra y
 * `rv` = tasa de venta:
 *
 *     resultado total = p·rv − i·rc = (p − i)·rc  +  p·(rv − rc)
 *                                    └── mérito ──┘  └─ divisa ─┘
 *
 * Suma exacta siempre, sin residuos y sin término cruzado que repartir — a
 * diferencia de la descomposición geométrica de `FEAT-FX-IMPACT`, que trabaja
 * sobre series de rendimiento y sí necesita aproximar.
 *
 * @module services/helpers/realizedFxDecomposition
 * @see platform-docs/stories/2.4-venta-dividendo-tasa-del-dia/refinamiento.md (D1, D2, D4, D5)
 */

const historicalRateService = require('../historicalRateService');

// ============================================================================
// CONSTANTES
// ============================================================================

/** Origen de la tasa de compra del lote vendido (D4) */
const ACQUISITION_RATE_SOURCES = {
  /** La divisa del activo ES la de referencia: no hay exposición cambiaria */
  IDENTITY: 'identity',
  /** El activo trae la tasa derivada que persistió 2.3 */
  ASSET: 'asset',
  /** `acquisitionDollarValue` de un activo en USD anclado a la moneda de referencia */
  LEGACY: 'legacy-acquisition',
  /** Tasa de mercado de la fecha de adquisición */
  MARKET_DATE: 'market-date',
  /** No determinable — la descomposición se declara ausente (RN-13) */
  UNAVAILABLE: 'unavailable',
};

/** Origen de la tasa del día de la venta o del pago */
const REALIZATION_RATE_SOURCES = {
  IDENTITY: 'identity',
  MARKET_DATE: 'market-date',
  UNAVAILABLE: 'unavailable',
};

/** Disponibilidad de la descomposición */
const DECOMPOSITION_AVAILABILITY = {
  AVAILABLE: 'available',
  UNAVAILABLE: 'unavailable',
};

/** Motivo por el que una descomposición no está disponible */
const UNAVAILABLE_REASONS = {
  /** No se pudo determinar a qué tipo de cambio se compró el lote */
  MISSING_ACQUISITION_RATE: 'missing-acquisition-rate',
  /** No se pudo determinar el tipo de cambio del día de la venta */
  MISSING_REALIZATION_RATE: 'missing-realization-rate',
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

/**
 * Recorta una fecha ISO —con o sin hora— a `YYYY-MM-DD`.
 *
 * Se toma siempre la fecha tal como se guardó, no la derivada del instante UTC:
 * `combineDateWithCurrentTime` compone en hora local y devuelve UTC, así que de
 * tarde en América el instante cae en el día siguiente al que eligió el usuario
 * (bug 1 del dev-record de 2.1).
 *
 * @param {string} value - Fecha ISO
 * @returns {string|null} Fecha `YYYY-MM-DD`, o null si no es utilizable
 */
function toDateOnly(value) {
  if (typeof value !== 'string' || value.length < 10) return null;
  const dateOnly = value.substring(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(dateOnly) ? dateOnly : null;
}

/**
 * Tasa cruzada de una fecha sin propagar los fallos del proveedor.
 *
 * Una venta es un hecho consumado en el bróker: si el servicio de tasas falla,
 * la operación se registra igual y la ausencia se declara (D5).
 *
 * @param {string} fromCurrency - Divisa del activo
 * @param {string} toCurrency - Moneda de referencia
 * @param {string} date - Fecha `YYYY-MM-DD`
 * @returns {Promise<{rate: number, rateDate: string, source: string}|null>}
 */
async function getCrossRateSafe(fromCurrency, toCurrency, date) {
  try {
    return await historicalRateService.getCrossRate(fromCurrency, toCurrency, date);
  } catch (error) {
    console.warn(
      `[realizedFxDecomposition] Sin tasa ${fromCurrency}→${toCurrency} en ${date}: ${error.message}`
    );
    return null;
  }
}

// ============================================================================
// API PÚBLICA
// ============================================================================

/**
 * Tasa a la que se compró el lote que se está vendiendo: cuántas unidades de la
 * moneda de referencia costó cada unidad de la divisa del activo.
 *
 * **Precedencia** (D4). No hay un solo sitio donde esté la tasa, porque los
 * activos anteriores a 2.3 no la persistieron:
 *
 * | # | Condición                                                        | Fuente               |
 * |---|------------------------------------------------------------------|----------------------|
 * | 1 | La divisa del activo es la de referencia                         | `identity`           |
 * | 2 | El activo trae `acquisitionRate` con la misma moneda de referencia| `asset`              |
 * | 3 | Activo en USD con `acquisitionDollarValue` anclado a la referencia| `legacy-acquisition` |
 * | 4 | Tasa de mercado de la fecha de adquisición                        | `market-date`        |
 * | 5 | Ninguna de las anteriores                                         | `unavailable`        |
 *
 * La rama 3 no es una heurística: para un activo en USD cuyo
 * `defaultCurrencyForAdquisitionDollar` es la moneda de referencia, la
 * semántica histórica del campo —"unidades de la moneda ancla por 1 USD"— **es
 * ya** la tasa que se busca. Es el mismo número, leído del sitio en el que
 * estaba antes de que 2.3 le diera nombre propio.
 *
 * @param {Object} asset - Documento del activo vendido
 * @param {string} referenceCurrency - Moneda de referencia vigente del usuario
 * @returns {Promise<{acquisitionRate: number|null, acquisitionRateSource: string}>}
 */
async function resolveLotAcquisitionRate(asset, referenceCurrency) {
  const assetCurrency = asset?.currency || 'USD';

  // 1. Sin exposición cambiaria no hay nada que derivar (RN-14).
  if (assetCurrency === referenceCurrency) {
    return { acquisitionRate: 1, acquisitionRateSource: ACQUISITION_RATE_SOURCES.IDENTITY };
  }

  // 2. La tasa que 2.3 derivó del saldo en el momento de la compra.
  const assetRate = Number(asset?.acquisitionRate);
  if (isUsableRate(assetRate) && asset?.referenceCurrency === referenceCurrency) {
    return { acquisitionRate: assetRate, acquisitionRateSource: ACQUISITION_RATE_SOURCES.ASSET };
  }

  // 3. Activo en USD anclado a la moneda de referencia: el campo antiguo YA es
  //    la tasa buscada, con su semántica intacta.
  const legacyRate = Number(asset?.acquisitionDollarValue);
  if (
    assetCurrency === 'USD'
    && isUsableRate(legacyRate)
    && asset?.defaultCurrencyForAdquisitionDollar === referenceCurrency
  ) {
    return { acquisitionRate: legacyRate, acquisitionRateSource: ACQUISITION_RATE_SOURCES.LEGACY };
  }

  // 4. Tasa de mercado del día en que se adquirió el lote.
  const acquisitionDate = toDateOnly(asset?.acquisitionDate);
  if (acquisitionDate) {
    const resolved = await getCrossRateSafe(assetCurrency, referenceCurrency, acquisitionDate);
    if (resolved && isUsableRate(resolved.rate)) {
      return {
        acquisitionRate: resolved.rate,
        acquisitionRateSource: ACQUISITION_RATE_SOURCES.MARKET_DATE,
      };
    }
  }

  // 5. Un dato ausente se declara ausente (RN-13).
  return { acquisitionRate: null, acquisitionRateSource: ACQUISITION_RATE_SOURCES.UNAVAILABLE };
}

/**
 * Tasa del día en que el dinero nace dentro de la cuenta: el día de la venta o
 * el del pago del dividendo (RN-03).
 *
 * @param {string} currency - Divisa en la que se recibe el dinero
 * @param {string} referenceCurrency - Moneda de referencia del usuario
 * @param {string} date - Fecha del movimiento, ISO con o sin hora
 * @returns {Promise<{realizationRate: number|null, realizationRateSource: string,
 *   realizationRateDate: string|null}>}
 */
async function resolveRealizationRate(currency, referenceCurrency, date) {
  if (currency === referenceCurrency) {
    return {
      realizationRate: 1,
      realizationRateSource: REALIZATION_RATE_SOURCES.IDENTITY,
      realizationRateDate: toDateOnly(date),
    };
  }

  const dateOnly = toDateOnly(date);
  if (!dateOnly) {
    return {
      realizationRate: null,
      realizationRateSource: REALIZATION_RATE_SOURCES.UNAVAILABLE,
      realizationRateDate: null,
    };
  }

  const resolved = await getCrossRateSafe(currency, referenceCurrency, dateOnly);
  if (resolved && isUsableRate(resolved.rate)) {
    return {
      realizationRate: resolved.rate,
      realizationRateSource: REALIZATION_RATE_SOURCES.MARKET_DATE,
      realizationRateDate: resolved.rateDate || dateOnly,
    };
  }

  return {
    realizationRate: null,
    realizationRateSource: REALIZATION_RATE_SOURCES.UNAVAILABLE,
    realizationRateDate: null,
  };
}

/**
 * Descompone el resultado realizado de una venta en mérito del activo y efecto
 * divisa (RN-10).
 *
 * El cálculo se hace sobre el producto **bruto** de la venta, de modo que
 * `assetMeritAmount` sea exactamente `valuePnL × acquisitionRate` — la misma
 * cifra bruta que la tabla de posiciones cerradas ya llama "resultado". La
 * comisión se valora aparte, a la tasa del día en que se pagó.
 *
 * Si falta cualquiera de las dos tasas, la descomposición se declara ausente en
 * lugar de reportar cero efecto divisa (RN-13, AC-9).
 *
 * @param {Object} params
 * @param {number} params.grossProceeds - Producto bruto de la venta, en la divisa del activo
 * @param {number} params.invested - Costo de las unidades vendidas, en la divisa del activo
 * @param {number|null} params.acquisitionRate - Tasa de compra del lote
 * @param {number|null} params.realizationRate - Tasa del día de la venta
 * @returns {{assetMeritAmount: number|null, realizedFxAmount: number|null,
 *   realizedTotalAmount: number|null, availability: string, unavailableReason: string|null}}
 */
function decomposeRealizedResult({
  grossProceeds,
  invested,
  acquisitionRate,
  realizationRate,
  fundedConvertedFraction = 0,
}) {
  const unavailable = (reason) => ({
    assetMeritAmount: null,
    realizedFxAmount: null,
    realizedTotalAmount: null,
    fxIsRealGainLoss: false,
    fundedConvertedFraction: null,
    availability: DECOMPOSITION_AVAILABILITY.UNAVAILABLE,
    unavailableReason: reason,
  });

  if (!isUsableRate(acquisitionRate)) {
    return unavailable(UNAVAILABLE_REASONS.MISSING_ACQUISITION_RATE);
  }

  if (!isUsableRate(realizationRate)) {
    return unavailable(UNAVAILABLE_REASONS.MISSING_REALIZATION_RATE);
  }

  const proceeds = Number(grossProceeds) || 0;
  const cost = Number(invested) || 0;

  const assetMeritAmount = round2((proceeds - cost) * acquisitionRate);
  const realizedFxAmount = round2(proceeds * (realizationRate - acquisitionRate));

  // ¿Es el componente cambiario una ganancia de verdad?
  //
  // La aritmética NO cambia: `mérito + divisa = total` sigue cuadrando al
  // céntimo (AC-2), y las cifras ya escritas no se mueven. Lo que se añade es
  // **cómo se puede nombrar** el componente cambiario.
  //
  // Sólo lo es si la divisa con la que se pagó la posición se había comprado
  // entregando moneda de referencia. Cuando el activo se compró con divisa que
  // el usuario ya tenía —un colombiano comprando acciones colombianas con sus
  // pesos—, el movimiento del cambio no es dinero que ganara: es el mismo
  // resultado medido con otra regla. Llamarlo ganancia realizada lo mete en su
  // rendimiento y le atribuye un acierto que nadie tuvo.
  //
  // Se exige la fracción **entera**: un lote pagado a medias con divisa
  // comprada no se presenta como ganancia. Quedarse corto es el error seguro.
  const fraction = Number(fundedConvertedFraction);
  const fxIsRealGainLoss = Number.isFinite(fraction) && fraction >= 1;

  return {
    assetMeritAmount,
    realizedFxAmount,
    // El total se toma de las dos componentes ya redondeadas, no del cálculo
    // directo: así la suma que el usuario ve cuadra al céntimo, sin residuos
    // que tendría que creerse (AC-2).
    realizedTotalAmount: round2(assetMeritAmount + realizedFxAmount),
    /** true si el componente cambiario puede presentarse como ganancia/pérdida */
    fxIsRealGainLoss,
    /** Fracción de la posición pagada con divisa comprada (0..1) */
    fundedConvertedFraction: Number.isFinite(fraction) ? fraction : 0,
    availability: DECOMPOSITION_AVAILABILITY.AVAILABLE,
    unavailableReason: null,
  };
}

/**
 * Construye los campos de trazabilidad cambiaria que se persisten en una
 * transacción de salida de activo (`sell`) o de dividendo (`dividendPay`).
 *
 * Los campos son aditivos: ningún lector actual de la transacción cambia, y
 * `dollarPriceToDate` conserva su semántica intacta (D3).
 *
 * @param {Object} params
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {number|null} params.acquisitionRate - Tasa de compra del lote
 * @param {string} params.acquisitionRateSource - Origen de esa tasa
 * @param {number|null} params.realizationRate - Tasa del día del movimiento
 * @param {string} params.realizationRateSource - Origen de esa tasa
 * @param {Object} params.decomposition - Resultado de `decomposeRealizedResult`
 * @param {number|null} params.acquisitionCost - Costo con el que el dinero entra al saldo
 * @returns {Object} Fragmento de campos para el documento de la transacción
 */
function buildRealizedFxFields({
  referenceCurrency,
  acquisitionRate,
  acquisitionRateSource,
  realizationRate,
  realizationRateSource,
  decomposition,
  acquisitionCost,
}) {
  return {
    referenceCurrency,
    acquisitionRate: acquisitionRate === null ? null : round2(acquisitionRate * 1e6) / 1e6,
    acquisitionRateSource,
    realizationRate: realizationRate === null ? null : round2(realizationRate * 1e6) / 1e6,
    realizationRateSource,
    assetMeritAmount: decomposition.assetMeritAmount,
    realizedFxAmount: decomposition.realizedFxAmount,
    realizedTotalAmount: decomposition.realizedTotalAmount,
    realizedFxAvailability: decomposition.availability,
    realizedFxUnavailableReason: decomposition.unavailableReason,
    // Origen del dinero que pagó la posición. Se persiste con la operación
    // porque describe un hecho del pasado: cambiar de moneda de referencia o
    // mover el saldo después no debe reescribir si aquello fue una ganancia.
    fxIsRealGainLoss: decomposition.fxIsRealGainLoss === true,
    fundedConvertedFraction: decomposition.fundedConvertedFraction ?? null,
    acquisitionCost: acquisitionCost === null || acquisitionCost === undefined
      ? null
      : round2(acquisitionCost),
  };
}

module.exports = {
  resolveLotAcquisitionRate,
  resolveRealizationRate,
  decomposeRealizedResult,
  buildRealizedFxFields,
  ACQUISITION_RATE_SOURCES,
  REALIZATION_RATE_SOURCES,
  DECOMPOSITION_AVAILABILITY,
  UNAVAILABLE_REASONS,
  // Exportados para test
  _round2: round2,
  _toDateOnly: toDateOnly,
};
