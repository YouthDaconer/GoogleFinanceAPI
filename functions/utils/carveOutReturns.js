/**
 * FEAT-EXCLUDE-001: Rendimiento de un sub-portafolio (carve-out) por tenencias.
 *
 * Responde "¿qué rendimiento tuvo el resto de mi portafolio?" excluyendo uno o
 * varios tickers (ej: VUAA.L, VOO). NO es un contrafactual: no se simula qué
 * habría pasado con el capital del activo excluido; ese activo simplemente no
 * entra ni al numerador ni al denominador, así que no hay supuestos sobre el
 * "efectivo liberado".
 *
 * Método: TWR por tenencias (holdings-based).
 *
 *   w_a(t)  = V_a(t-1) / Σ_{b∉E} V_b(t-1)
 *   r_a(t)  = retorno de mercado del ticker entre las fechas de los docs
 *             (precio de cierre, convertido a la moneda del reporte)
 *             + dividendos registrados del día / V_a(t-1)
 *   r_ex(t) = Σ_{a∉E} w_a(t) · r_a(t)
 *
 * De portfolioPerformance sólo se toma QUÉ se tenía y CUÁNTO pesaba
 * (assetPerformance[*].totalValue del día anterior). El retorno NO se toma de
 * assetPerformance[*].adjustedDailyChangePercentage: medido contra cierres
 * reales sobre un portafolio de 2 años y 39 activos, agregar ese campo sesgaba
 * el resultado +12 a +16 pp al excluir un activo, mientras el método por
 * tenencias reprodujo el TWR oficial dentro de la banda explicada por los
 * dividendos. Por construcción, los cashflows, las ventas parciales y los
 * registros retroactivos no afectan al resultado: cambian los pesos del día
 * siguiente, nunca el retorno del día.
 *
 * Exactitud: el cálculo es estricto. Si a un activo con peso le falta precio
 * de mercado en alguna de las dos fechas, el día se marca como no calculable
 * en lugar de re-ponderar en silencio, y la cobertura se devuelve para que el
 * llamador pueda advertirlo.
 *
 * Diferencia conocida frente al TWR oficial: una compra o venta a un precio
 * distinto del cierre genera un P&L intradía que el TWR oficial captura y el
 * de tenencias no (las tenencias se valoran a cierre). Es la convención
 * estándar de atribución por tenencias diarias.
 *
 * Este módulo es puro: no hace I/O. Las series de precios, tasas y dividendos
 * las trae services/carveOutReturnsService.js.
 *
 * @module utils/carveOutReturns
 */

/** Máximo de tickers excluibles por consulta (acota cardinalidad del cache). */
const MAX_EXCLUDED_TICKERS = 20;

/**
 * Antigüedad máxima (días calendario) de un cierre para usarlo como precio de
 * una fecha. Cubre fines de semana largos y feriados; más allá, el activo se
 * considera sin precio (deslistado, símbolo sin datos).
 */
const MAX_PRICE_STALE_DAYS = 7;

const CARVE_OUT_STATUS = {
  OK: 'ok',
  /** Primer día del sub-portafolio: no hay base, el factor TWR no se mueve. */
  START: 'start',
  /** El doc del día no trae assetPerformance. */
  NO_ASSET_DATA: 'no-asset-data',
  /** El doc del día anterior no traía assetPerformance: no hay pesos. */
  NO_PRIOR_ASSET_DATA: 'no-prior-asset-data',
  /** Algún activo con peso no tiene precio de mercado en las dos fechas. */
  INCOMPLETE: 'incomplete',
};

// ============================================================================
// Tickers y exclusión
// ============================================================================

/**
 * Normaliza un ticker para comparar (trim + mayúsculas).
 *
 * @param {string} ticker
 * @returns {string}
 */
function normalizeTicker(ticker) {
  return String(ticker).trim().toUpperCase();
}

/**
 * Extrae el ticker de una llave de assetPerformance.
 *
 * Las llaves son `${name}_${assetType}`, pero el histórico tiene llaves como
 * `VUAA.L_undefined` (backfills anteriores al default de assetType). Por eso se
 * compara por ticker —todo lo anterior al ÚLTIMO "_"— y nunca por llave exacta.
 *
 * @param {string} assetKey
 * @returns {string}
 */
function tickerFromAssetKey(assetKey) {
  const idx = assetKey.lastIndexOf('_');
  return normalizeTicker(idx > 0 ? assetKey.slice(0, idx) : assetKey);
}

/**
 * Valida y normaliza la lista de tickers a excluir.
 *
 * @param {unknown} excludeTickers
 * @returns {Set<string>}
 * @throws {Error} Si no es un arreglo de strings no vacíos o excede el máximo
 */
function buildExcludeSet(excludeTickers) {
  if (excludeTickers === undefined || excludeTickers === null) return new Set();
  if (!Array.isArray(excludeTickers)) {
    throw new Error('excludeTickers debe ser un arreglo de tickers');
  }
  const set = new Set();
  for (const t of excludeTickers) {
    if (typeof t !== 'string' || t.trim() === '') {
      throw new Error('excludeTickers solo admite tickers no vacíos');
    }
    set.add(normalizeTicker(t));
  }
  if (set.size > MAX_EXCLUDED_TICKERS) {
    throw new Error(`excludeTickers admite como máximo ${MAX_EXCLUDED_TICKERS} tickers`);
  }
  return set;
}

/**
 * @param {string} assetKey
 * @param {Set<string>} excludeSet
 * @returns {boolean}
 */
function isExcluded(assetKey, excludeSet) {
  return excludeSet.size > 0 && excludeSet.has(tickerFromAssetKey(assetKey));
}

/**
 * Entradas válidas de assetPerformance.
 *
 * Descarta las entradas anidadas que deja un "." en la ruta de un update
 * (`VUAA: { L_etf: {...} }`): siempre conviven con la llave plana canónica
 * (`VUAA.L_etf`), a veces con valores distintos, y sumarlas duplicaría el peso.
 *
 * @param {Object<string, Object>|undefined|null} assetPerformance
 * @returns {Array<[string, Object]>}
 */
function assetEntries(assetPerformance) {
  if (!assetPerformance || typeof assetPerformance !== 'object') return [];
  return Object.entries(assetPerformance).filter(
    ([, data]) => data && typeof data === 'object' && Number.isFinite(Number(data.totalValue))
  );
}

/**
 * Valores de cierre por activo, para usarlos como pesos del día siguiente.
 *
 * @param {Object<string, Object>|undefined|null} assetPerformance
 * @returns {Object<string, number>|null} null si el doc no trae datos por activo
 */
function extractAssetValues(assetPerformance) {
  const entries = assetEntries(assetPerformance);
  if (entries.length === 0) return null;
  const values = {};
  for (const [key, data] of entries) {
    values[key] = Number(data.totalValue) || 0;
  }
  return values;
}

/**
 * Tickers (normalizados) que tuvieron valor > 0 en algún día y no están
 * excluidos: son los que necesitan serie de precios.
 *
 * @param {Array<{date: string, currencyData: Object|undefined}>} days
 * @param {Set<string>} excludeSet
 * @returns {string[]}
 */
function collectHeldTickers(days, excludeSet) {
  const tickers = new Set();
  for (const { currencyData } of days) {
    for (const [key, data] of assetEntries(currencyData?.assetPerformance)) {
      if (Number(data.totalValue) > 0 && !isExcluded(key, excludeSet)) {
        tickers.add(tickerFromAssetKey(key));
      }
    }
  }
  return [...tickers].sort();
}

/**
 * Tickers de la exclusión que no aparecen en ningún día del historial (ej: se
 * pidió "VUAA" y el activo es "VUAA.L"). Se reportan para no excluir en vano
 * sin avisar.
 *
 * @param {Array<{date: string, currencyData: Object|undefined}>} days
 * @param {Set<string>} excludeSet
 * @returns {string[]}
 */
function findUnmatchedExclusions(days, excludeSet) {
  const seen = new Set();
  for (const { currencyData } of days) {
    for (const [key] of assetEntries(currencyData?.assetPerformance)) {
      seen.add(tickerFromAssetKey(key));
    }
  }
  return [...excludeSet].filter((t) => !seen.has(t)).sort();
}

// ============================================================================
// Series de precios y tasas de cambio
// ============================================================================

/**
 * Convierte la respuesta de `/historical` ({ 'YYYY-MM-DD': { close, ... } })
 * en una serie ordenada. Descarta cierres no positivos o no numéricos.
 *
 * @param {Object<string, {close: number}>|null|undefined} historical
 * @returns {{dates: string[], closes: number[]}}
 */
function toPriceSeries(historical) {
  const points = [];
  for (const [date, ohlcv] of Object.entries(historical || {})) {
    const close = Number(ohlcv?.close);
    if (Number.isFinite(close) && close > 0) points.push([date.slice(0, 10), close]);
  }
  points.sort((a, b) => a[0].localeCompare(b[0]));
  return { dates: points.map((p) => p[0]), closes: points.map((p) => p[1]) };
}

/**
 * Diferencia en días calendario entre dos fechas ISO (b - a).
 * @param {string} a
 * @param {string} b
 * @returns {number}
 */
function daysBetween(a, b) {
  return Math.round((Date.parse(`${b}T00:00:00Z`) - Date.parse(`${a}T00:00:00Z`)) / 86400000);
}

/**
 * Último cierre con fecha <= date, si no es más viejo que MAX_PRICE_STALE_DAYS.
 *
 * @param {{dates: string[], closes: number[]}|undefined} series
 * @param {string} date
 * @returns {number|null}
 */
function closeAsOf(series, date) {
  if (!series || series.dates.length === 0) return null;
  let lo = 0;
  let hi = series.dates.length - 1;
  let idx = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (series.dates[mid] <= date) {
      idx = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  if (idx < 0) return null;
  if (daysBetween(series.dates[idx], date) > MAX_PRICE_STALE_DAYS) return null;
  return series.closes[idx];
}

/**
 * Normaliza el código de moneda de una cotización. Las acciones de Londres
 * cotizan en peniques (GBp/GBX): para retornos y conversiones basta tratarlas
 * como GBP, porque el factor 100 se cancela al dividir dos precios.
 *
 * @param {string|null|undefined} code
 * @returns {string}
 */
function normalizeCurrency(code) {
  if (!code) return 'USD';
  if (code === 'GBp' || code.toUpperCase() === 'GBX') return 'GBP';
  return code.toUpperCase();
}

/**
 * Símbolo de la API para la tasa "unidades de `currency` por 1 USD"
 * (convención de Yahoo: EUR=X, COP=X, GBP=X). USD no necesita serie.
 *
 * @param {string} currency
 * @returns {string|null}
 */
function fxSymbol(currency) {
  const c = normalizeCurrency(currency);
  return c === 'USD' ? null : `${c}=X`;
}

/**
 * Valor en USD de 1 unidad de `currency` en una fecha.
 *
 * @param {Object<string, {dates: string[], closes: number[]}>} fxSeriesByCurrency
 *   Series "unidades por USD" indexadas por código de moneda
 * @param {string} currency
 * @param {string} date
 * @returns {number|null}
 */
function usdPerUnit(fxSeriesByCurrency, currency, date) {
  const c = normalizeCurrency(currency);
  if (c === 'USD') return 1;
  const unitsPerUsd = closeAsOf(fxSeriesByCurrency[c], date);
  return unitsPerUsd ? 1 / unitsPerUsd : null;
}

/**
 * Convierte un monto entre monedas a la tasa de una fecha.
 *
 * @returns {number|null}
 */
function convertAmount(amount, from, to, fxSeriesByCurrency, date) {
  const f = usdPerUnit(fxSeriesByCurrency, from, date);
  const t = usdPerUnit(fxSeriesByCurrency, to, date);
  if (f === null || t === null) return null;
  return (amount * f) / t;
}

/**
 * Retorno de mercado (%) de un ticker entre dos fechas, expresado en la moneda
 * del reporte: (p1/p0) × (tasa1/tasa0) − 1.
 *
 * @param {Object} params
 * @param {{dates: string[], closes: number[]}|undefined} params.priceSeries
 * @param {string} params.quoteCurrency - Moneda en que cotiza el ticker
 * @param {string} params.reportCurrency - Moneda del reporte (USD, COP, ...)
 * @param {Object<string, {dates: string[], closes: number[]}>} params.fxSeriesByCurrency
 * @param {string} d0
 * @param {string} d1
 * @returns {number|null}
 */
function marketReturnPct({ priceSeries, quoteCurrency, reportCurrency, fxSeriesByCurrency }, d0, d1) {
  const p0 = closeAsOf(priceSeries, d0);
  const p1 = closeAsOf(priceSeries, d1);
  if (p0 === null || p1 === null) return null;
  const rate0 = convertAmount(1, quoteCurrency, reportCurrency, fxSeriesByCurrency, d0);
  const rate1 = convertAmount(1, quoteCurrency, reportCurrency, fxSeriesByCurrency, d1);
  if (rate0 === null || rate1 === null || rate0 <= 0) return null;
  return ((p1 / p0) * (rate1 / rate0) - 1) * 100;
}

// ============================================================================
// Dividendos registrados
// ============================================================================

/**
 * Efectivo neto cobrado por un dividendPay.
 *
 * En dividendPay, `amount` son UNIDADES y `price` es el dividendo por unidad
 * neto de retención (processDividendPayments: `amount: totalUnits,
 * price: netAmount / totalUnits`). Misma fórmula que useDividends en el
 * frontend, para que ambos cuenten lo mismo.
 *
 * @param {Object} tx
 * @returns {number} NaN si no se puede determinar
 */
function dividendNetCash(tx) {
  const gross = Number.isFinite(tx?.grossAmount) && tx.grossAmount > 0
    ? tx.grossAmount
    : Number(tx?.price) * Number(tx?.amount);
  return gross - (Number(tx?.taxDeductionAmount) || 0);
}

/**
 * Agrupa los dividendos registrados (transacciones dividendPay) por ticker,
 * convertidos a la moneda del reporte en su fecha.
 *
 * Se asocian por `symbol` y no por `assetId`: los dividendPay no traen
 * assetId. El monto es el efectivo neto (dividendNetCash).
 *
 * @param {Object[]} transactions - dividendPay del usuario
 * @param {Object} params
 * @param {string} params.reportCurrency
 * @param {Object<string, {dates: string[], closes: number[]}>} params.fxSeriesByCurrency
 * @returns {{
 *   byTicker: Map<string, Array<{date: string, amount: number}>>,
 *   unconverted: number
 * }}
 */
function groupDividends(transactions, { reportCurrency, fxSeriesByCurrency }) {
  const byTicker = new Map();
  let unconverted = 0;
  for (const tx of transactions || []) {
    const symbol = tx?.symbol;
    const date = typeof tx?.date === 'string' ? tx.date.slice(0, 10) : null;
    const raw = dividendNetCash(tx);
    if (!symbol || !date || !Number.isFinite(raw) || raw <= 0) continue;
    const amount = convertAmount(raw, tx.currency || 'USD', reportCurrency, fxSeriesByCurrency, date);
    if (amount === null) {
      unconverted++;
      continue;
    }
    const ticker = normalizeTicker(symbol);
    if (!byTicker.has(ticker)) byTicker.set(ticker, []);
    byTicker.get(ticker).push({ date, amount });
  }
  return { byTicker, unconverted };
}

/**
 * Suma de dividendos de un ticker con fecha en (d0, d1].
 *
 * @param {Map<string, Array<{date: string, amount: number}>>} byTicker
 * @param {string} ticker - normalizado
 * @param {string} d0
 * @param {string} d1
 * @returns {number}
 */
function dividendsBetween(byTicker, ticker, d0, d1) {
  let total = 0;
  for (const { date, amount } of byTicker.get(ticker) || []) {
    if (date > d0 && date <= d1) total += amount;
  }
  return total;
}

// ============================================================================
// Cálculo diario y serie
// ============================================================================

/**
 * Calcula un día del sub-portafolio.
 *
 * Devuelve la misma forma que consume calculateHistoricalReturns
 * (adjustedDailyChangePercentage, totalValue, totalCashFlow, ...), más el
 * status del día.
 *
 * @param {Object<string, number>|null} prevAssetValues - Valores de cierre del
 *   día anterior por llave; null si ese doc no traía assetPerformance
 * @param {Object<string, Object>|undefined|null} assetPerformance - Bloque del día
 * @param {Set<string>} excludeSet - Tickers normalizados a excluir
 * @param {Object} sources
 * @param {(ticker: string) => number|null} sources.marketReturn - % del día
 * @param {(ticker: string) => number} [sources.dividendCash] - Dividendos del día
 *   en la moneda del reporte
 */
function computeCarveOutDay(prevAssetValues, assetPerformance, excludeSet, sources) {
  const result = {
    status: CARVE_OUT_STATUS.OK,
    adjustedDailyChangePercentage: null,
    totalValue: 0,
    totalInvestment: 0,
    totalCashFlow: 0,
    doneProfitAndLoss: 0,
    unrealizedProfitAndLoss: 0,
    dividends: 0,
    weightedValue: 0,
    missingValue: 0,
    missingTickers: [],
  };

  const entries = assetEntries(assetPerformance);
  if (entries.length === 0) {
    result.status = CARVE_OUT_STATUS.NO_ASSET_DATA;
    return result;
  }

  for (const [key, data] of entries) {
    if (isExcluded(key, excludeSet)) continue;
    result.totalValue += Number(data.totalValue) || 0;
    result.totalInvestment += Number(data.totalInvestment) || 0;
    result.totalCashFlow += Number(data.totalCashFlow) || 0;
    result.doneProfitAndLoss += Number(data.doneProfitAndLoss) || 0;
    result.unrealizedProfitAndLoss += Number(data.unrealizedProfitAndLoss) || 0;
  }

  if (prevAssetValues === null || prevAssetValues === undefined) {
    result.status = CARVE_OUT_STATUS.NO_PRIOR_ASSET_DATA;
    return result;
  }

  // Varias llaves pueden ser el mismo ticker (VUAA.L_etf y VUAA.L_undefined):
  // se agrupan para pedir el precio una vez y sumar el peso completo.
  const weightByTicker = new Map();
  for (const [key, mvb] of Object.entries(prevAssetValues)) {
    if (mvb <= 0 || isExcluded(key, excludeSet)) continue;
    const ticker = tickerFromAssetKey(key);
    weightByTicker.set(ticker, (weightByTicker.get(ticker) || 0) + mvb);
  }

  let weightedSum = 0;
  for (const [ticker, mvb] of weightByTicker) {
    const change = sources.marketReturn(ticker);
    if (typeof change !== 'number' || !Number.isFinite(change)) {
      result.missingValue += mvb;
      result.missingTickers.push(ticker);
      continue;
    }
    const cash = sources.dividendCash ? sources.dividendCash(ticker) : 0;
    result.dividends += cash;
    weightedSum += mvb * change + cash * 100;
    result.weightedValue += mvb;
  }

  if (result.missingValue > 0) {
    result.status = CARVE_OUT_STATUS.INCOMPLETE;
    return result;
  }

  if (result.weightedValue <= 0) {
    // El sub-portafolio no tenía valor ayer: arranca hoy, el factor no se mueve
    result.status = CARVE_OUT_STATUS.START;
    result.adjustedDailyChangePercentage = 0;
    return result;
  }

  result.adjustedDailyChangePercentage = weightedSum / result.weightedValue;
  return result;
}

/**
 * Recorre los docs diarios (ordenados por fecha) y produce la serie del
 * sub-portafolio con su cobertura.
 *
 * @param {Array<{date: string, currencyData: Object|undefined}>} days
 * @param {Set<string>} excludeSet
 * @param {Object} sources
 * @param {(ticker: string, d0: string, d1: string) => number|null} sources.marketReturn
 * @param {(ticker: string, d0: string, d1: string) => number} [sources.dividendCash]
 */
function buildCarveOutSeries(days, excludeSet, sources) {
  const out = [];
  const unusableByStatus = {};
  const missingTickers = new Set();
  let usableDays = 0;
  let firstUsableDate = null;
  let lastUsableDate = null;
  let dividends = 0;
  // El primer doc no tiene día anterior: es un arranque (START), no un hueco.
  // A partir de ahí, null significa "el doc anterior no traía datos por activo".
  let prevAssetValues = {};
  let prevDate = null;

  for (const { date, currencyData } of days) {
    const assetPerformance = currencyData?.assetPerformance;
    const d0 = prevDate;
    const day = computeCarveOutDay(prevAssetValues, assetPerformance, excludeSet, {
      marketReturn: (ticker) => (d0 ? sources.marketReturn(ticker, d0, date) : null),
      dividendCash: sources.dividendCash && d0
        ? (ticker) => sources.dividendCash(ticker, d0, date)
        : undefined,
    });
    out.push({ date, day });

    if (day.adjustedDailyChangePercentage !== null) {
      usableDays++;
      dividends += day.dividends;
      if (!firstUsableDate) firstUsableDate = date;
      lastUsableDate = date;
    } else {
      unusableByStatus[day.status] = (unusableByStatus[day.status] || 0) + 1;
      day.missingTickers.forEach((t) => missingTickers.add(t));
    }

    prevAssetValues = extractAssetValues(assetPerformance);
    prevDate = date;
  }

  return {
    days: out,
    coverage: {
      totalDays: days.length,
      usableDays,
      firstUsableDate,
      lastUsableDate,
      unusableByStatus,
      missingPriceTickers: [...missingTickers].sort(),
      dividendsIncluded: dividends,
    },
  };
}

/**
 * Convierte la serie en docs con la forma de portfolioPerformance para
 * alimentar calculateHistoricalReturns sin tocarlo. Los días no calculables se
 * omiten (no se inventa un 0%): su ausencia queda reflejada en la cobertura.
 *
 * dailyChangePercentage se iguala al ajustado: los gráficos encadenan ese
 * campo, y el cambio bruto de valor del sub-portafolio incluiría los flujos.
 *
 * @param {ReturnType<typeof buildCarveOutSeries>} series
 * @param {string} currency
 * @returns {Object[]}
 */
function toPerformanceDocs(series, currency) {
  return series.days
    .filter(({ day }) => day.adjustedDailyChangePercentage !== null)
    .map(({ date, day }) => ({
      date,
      [currency]: {
        adjustedDailyChangePercentage: day.adjustedDailyChangePercentage,
        dailyChangePercentage: day.adjustedDailyChangePercentage,
        totalValue: day.totalValue,
        totalInvestment: day.totalInvestment,
        totalCashFlow: day.totalCashFlow,
        doneProfitAndLoss: day.doneProfitAndLoss,
        unrealizedProfitAndLoss: day.unrealizedProfitAndLoss,
      },
    }));
}

module.exports = {
  MAX_EXCLUDED_TICKERS,
  MAX_PRICE_STALE_DAYS,
  CARVE_OUT_STATUS,
  normalizeTicker,
  tickerFromAssetKey,
  buildExcludeSet,
  isExcluded,
  extractAssetValues,
  collectHeldTickers,
  findUnmatchedExclusions,
  toPriceSeries,
  closeAsOf,
  normalizeCurrency,
  fxSymbol,
  usdPerUnit,
  convertAmount,
  marketReturnPct,
  dividendNetCash,
  groupDividends,
  dividendsBetween,
  computeCarveOutDay,
  buildCarveOutSeries,
  toPerformanceDocs,
};
