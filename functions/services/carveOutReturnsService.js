/**
 * FEAT-EXCLUDE-001: Rendimientos históricos de un sub-portafolio que excluye
 * tickers (ej: VUAA.L), para compararlo contra un benchmark o un activo.
 *
 * Orquesta el I/O alrededor de utils/carveOutReturns.js (puro):
 *  1. Docs diarios de portfolioPerformance (qué se tenía y cuánto pesaba).
 *  2. Moneda de cotización de cada ticker (quotes de la API).
 *  3. Cierres diarios por ticker y tasas de cambio, ambos vía
 *     `${FINANCE_QUERY_API_URL}/historical?symbol=...&range=...` con una sola
 *     llamada de rango amplio por símbolo.
 *  4. Dividendos registrados (transacciones dividendPay).
 * y entrega el resultado con la misma forma que getHistoricalReturns, para que
 * el frontend lo consuma sin cambios, más un bloque `carveOut` con la
 * cobertura.
 *
 * Los campos MWR (*PersonalReturn) se omiten: dependen de los cashflows por
 * activo, que no son confiables en el histórico, y no están validados.
 *
 * @module services/carveOutReturnsService
 */

const { calculateHistoricalReturns } = require('./historicalReturnsService');
const { getHistoricalPrices } = require('./financeQuery');
const { getPricesFromApi } = require('./marketDataHelper');
const carve = require('../utils/carveOutReturns');

/** Llamadas simultáneas a /historical. */
const FETCH_CONCURRENCY = 6;

/** Las series de cierre cambian una vez al día; se comparten entre usuarios. */
const HISTORY_CACHE_TTL_MS = 6 * 60 * 60 * 1000;

/** Días extra al elegir el rango, para tener el cierre previo al primer doc. */
const RANGE_MARGIN_DAYS = 10;

const historyCache = new Map();

/**
 * Rango diario más corto de la API que cubre desde firstDate hasta hoy.
 *
 * @param {string} firstDate - YYYY-MM-DD
 * @param {Date} [now]
 * @returns {'1y'|'2y'|'5y'|'10y'}
 */
function chooseHistoryRange(firstDate, now = new Date()) {
  const spanDays = (now.getTime() - Date.parse(`${firstDate}T00:00:00Z`)) / 86400000 + RANGE_MARGIN_DAYS;
  if (spanDays <= 365) return '1y';
  if (spanDays <= 730) return '2y';
  if (spanDays <= 1826) return '5y';
  return '10y';
}

/**
 * Serie de cierres de un símbolo, con cache en memoria de la instancia.
 * Un fallo devuelve null (el ticker quedará sin precio y sus días se marcan
 * como no calculables); no se cachea para reintentar en la próxima consulta.
 */
async function fetchSeries(symbol, range, deps) {
  const key = `${symbol}|${range}`;
  const hit = historyCache.get(key);
  if (hit && Date.now() - hit.at < HISTORY_CACHE_TTL_MS) return hit.series;
  try {
    const series = carve.toPriceSeries(await deps.getHistoricalPrices(symbol, range));
    if (series.dates.length === 0) return null;
    historyCache.set(key, { series, at: Date.now() });
    return series;
  } catch (error) {
    console.warn(`[carveOut] Sin historico para ${symbol} (${range}): ${error.message}`);
    return null;
  }
}

/** Ejecuta fn sobre items con un máximo de `limit` promesas en vuelo. */
async function mapLimit(items, limit, fn) {
  const results = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const i = next++;
      results[i] = await fn(items[i]);
    }
  });
  await Promise.all(workers);
  return results;
}

async function loadDays(db, userId, accountId, currency) {
  const basePath = accountId === 'overall'
    ? `portfolioPerformance/${userId}/dates`
    : `portfolioPerformance/${userId}/accounts/${accountId}/dates`;
  const snapshot = await db.collection(basePath).orderBy('date', 'asc').get();
  return snapshot.docs.map((doc) => {
    const data = doc.data() || {};
    return { date: data.date || doc.id, currencyData: data[currency] };
  });
}

/**
 * dividendPay de las cuentas del usuario (o de la cuenta pedida). Se parte de
 * portfolioAccounts para garantizar que la cuenta pertenece al usuario.
 */
async function loadDividendTransactions(db, userId, accountId, fromDate) {
  const accounts = await db.collection('portfolioAccounts').where('userId', '==', userId).get();
  const ids = accounts.docs
    .map((doc) => doc.id)
    .filter((id) => accountId === 'overall' || id === accountId);
  const txs = [];
  for (let i = 0; i < ids.length; i += 10) {
    const snapshot = await db.collection('transactions')
      .where('portfolioAccountId', 'in', ids.slice(i, i + 10))
      .where('type', '==', 'dividendPay')
      .get();
    snapshot.docs.forEach((doc) => {
      const tx = doc.data();
      if (typeof tx.date === 'string' && tx.date.slice(0, 10) >= fromDate) txs.push(tx);
    });
  }
  return txs;
}

function stripPersonalReturns(returns) {
  return Object.fromEntries(
    Object.entries(returns || {}).filter(([key]) => !/PersonalReturn$|PersonalData$/.test(key))
  );
}

/**
 * @param {Object} params
 * @param {FirebaseFirestore.Firestore} params.db
 * @param {string} params.userId
 * @param {string} params.currency - Moneda del reporte
 * @param {string} params.accountId - 'overall' o id de cuenta
 * @param {Set<string>} params.excludeSet - De carve.buildExcludeSet (no vacío)
 * @param {Object} [deps] - Inyectables para tests
 * @returns {Promise<Object>} Forma de getHistoricalReturns + bloque carveOut
 */
async function getCarveOutHistoricalReturns(
  { db, userId, currency, accountId, excludeSet },
  deps = { getHistoricalPrices, getPricesFromApi }
) {
  const days = await loadDays(db, userId, accountId, currency);
  const excludedTickers = [...excludeSet].sort();

  const meta = {
    method: 'holdings-twr',
    dividends: 'registered-net',
    excludedTickers,
    excludedTickersNotFound: carve.findUnmatchedExclusions(days, excludeSet),
  };

  if (days.length === 0) {
    return {
      ...calculateHistoricalReturns([], currency, null, null),
      carveOut: { ...meta, coverage: { totalDays: 0, usableDays: 0 } },
    };
  }

  const firstDate = days[0].date;
  const range = chooseHistoryRange(firstDate);
  const tickers = carve.collectHeldTickers(days, excludeSet);

  // Moneda de cotización: sin ella no se puede convertir el retorno, así que
  // un ticker ausente en los quotes queda sin precio (día no calculable).
  const quotes = tickers.length > 0 ? await deps.getPricesFromApi(tickers) : [];
  const quoteCurrency = new Map();
  for (const quote of quotes || []) {
    if (quote?.symbol) quoteCurrency.set(carve.normalizeTicker(quote.symbol), carve.normalizeCurrency(quote.currency));
  }

  const dividendTxs = await loadDividendTransactions(db, userId, accountId, firstDate);

  const currencies = new Set([carve.normalizeCurrency(currency)]);
  quoteCurrency.forEach((c) => currencies.add(c));
  dividendTxs.forEach((tx) => currencies.add(carve.normalizeCurrency(tx.currency)));
  currencies.delete('USD');

  const pricedTickers = tickers.filter((t) => quoteCurrency.has(t));
  const fxList = [...currencies];
  const [priceList, fxSeriesList] = await Promise.all([
    mapLimit(pricedTickers, FETCH_CONCURRENCY, (t) => fetchSeries(t, range, deps)),
    mapLimit(fxList, FETCH_CONCURRENCY, (c) => fetchSeries(carve.fxSymbol(c), range, deps)),
  ]);

  const prices = new Map();
  pricedTickers.forEach((t, i) => { if (priceList[i]) prices.set(t, priceList[i]); });
  const fxSeriesByCurrency = {};
  fxList.forEach((c, i) => { if (fxSeriesList[i]) fxSeriesByCurrency[c] = fxSeriesList[i]; });

  const dividends = carve.groupDividends(dividendTxs, { reportCurrency: currency, fxSeriesByCurrency });

  const series = carve.buildCarveOutSeries(days, excludeSet, {
    marketReturn: (ticker, d0, d1) => carve.marketReturnPct({
      priceSeries: prices.get(ticker),
      quoteCurrency: quoteCurrency.get(ticker),
      reportCurrency: currency,
      fxSeriesByCurrency,
    }, d0, d1),
    dividendCash: (ticker, d0, d1) => carve.dividendsBetween(dividends.byTicker, ticker, d0, d1),
  });

  const result = calculateHistoricalReturns(carve.toPerformanceDocs(series, currency), currency, null, null);

  console.log(`[carveOut] user=${userId} account=${accountId} ${currency} excl=${excludedTickers.join(',')} `
    + `usable=${series.coverage.usableDays}/${series.coverage.totalDays} tickers=${tickers.length} range=${range}`);

  return {
    ...result,
    returns: stripPersonalReturns(result.returns),
    carveOut: {
      ...meta,
      historyRange: range,
      coverage: {
        ...series.coverage,
        tickersWithoutQuote: tickers.filter((t) => !quoteCurrency.has(t)),
        currenciesWithoutFx: fxList.filter((c) => !fxSeriesByCurrency[c]),
        dividendsUnconverted: dividends.unconverted,
      },
    },
  };
}

module.exports = {
  getCarveOutHistoricalReturns,
  chooseHistoryRange,
  // Exportados para test
  _historyCache: historyCache,
};
