/**
 * FIX-ATTR-HOLDINGS-001: Contribuciones al rendimiento por tenencias diarias.
 *
 * Orquesta el I/O alrededor de utils/holdingsAttribution.js (puro):
 *  1. Docs diarios de portfolioPerformance del período (+ el cierre previo como
 *     base), sólo los campos necesarios. Varias cuentas se suman por fecha.
 *  2. Cierres de mercado y tasas de cambio (misma cache que el carve-out).
 *  3. Dividendos registrados y ventas del período.
 *  4. Qué activos siguen vivos (colección assets).
 * y devuelve la misma forma que calculateContributions (legacy), para que
 * attributionService, el resumen y el frontend la consuman sin cambios de
 * contrato.
 *
 * Qué cambia frente al cálculo anterior:
 *  - contribution (pp) = Σ peso(t-1) × retorno(t), encadenado. Suma el TWR por
 *    tenencias del período; no hace falta repartir residuos.
 *  - weightAverage = peso promedio mientras se tuvo el activo en el período
 *    (antes: costo acumulado de todas las ventas / valor al inicio).
 *  - weightStart = peso real al cierre previo al período.
 *  - returnPercent = retorno del activo en el período mientras se tuvo
 *    (antes mezclaba ROI desde la compra y retorno del período según el caso).
 *  - contributionAbsolute / valueChange = ganancia en $ por precio y
 *    dividendos en el período.
 *  - realizedPnL = P&L realizada de las ventas del período (campo nuevo).
 *
 * Devuelve null cuando no hay al menos dos días con datos por activo: el
 * llamador cae al cálculo legacy.
 *
 * @module services/attribution/holdingsContributions
 */

const carve = require('../../utils/carveOutReturns');
const holdings = require('../../utils/holdingsAttribution');
const { getPeriodStartDate } = require('./types');

/** Tiempo máximo para traer cierres y tasas; si se agota, se usa el precio implícito. */
const PRICE_BUDGET_MS = 15000;

function performancePath(userId, seriesId) {
  return seriesId === 'overall'
    ? `portfolioPerformance/${userId}/dates`
    : `portfolioPerformance/${userId}/accounts/${seriesId}/dates`;
}

function toDateStr(date) {
  return date.toISOString().split('T')[0];
}

/** Resuelve null si la promesa no termina en `ms` (no la cancela). */
function withTimeout(promise, ms) {
  let timer;
  return Promise.race([
    promise,
    new Promise((resolve) => { timer = setTimeout(() => resolve(null), ms); }),
  ]).finally(() => clearTimeout(timer));
}

/**
 * Cuentas a usar. Si la selección son todas las cuentas activas se usa la
 * serie 'overall' (una lectura por día en vez de una por cuenta y día).
 */
async function resolveAccounts(db, userId, accountIds) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  const active = snapshot.docs.map((doc) => doc.id);
  const wantsOverall = !accountIds || accountIds.length === 0 || accountIds.includes('overall');
  if (wantsOverall) return { seriesIds: ['overall'], accountIds: active, useOverall: true };

  const own = new Set(active);
  const selected = [...new Set(accountIds)].filter((id) => own.has(id));
  if (selected.length > 0 && selected.length === active.length) {
    return { seriesIds: ['overall'], accountIds: active, useOverall: true };
  }
  return { seriesIds: selected, accountIds: selected, useOverall: false };
}

/**
 * Serie diaria compacta por (usuario, serie, moneda), compartida entre
 * períodos: cambiar de "YTD" a "1 Año" no vuelve a leer los mismos docs. Cada
 * consulta relee sólo el último día (el EOD puede reescribirlo) y lo nuevo.
 * El TTL acota cuánto puede tardar en verse un backfill de días pasados.
 */
const SERIES_CACHE_TTL_MS = 30 * 60 * 1000;
const SERIES_CACHE_MAX_ENTRIES = 40;
const seriesCache = new Map();

/** De cada doc sólo se guarda valor y unidades por llave: es lo único que usa el cálculo. */
function compactDay(doc, currency) {
  const data = doc.data() || {};
  const currencyData = data[currency] || {};
  const raw = currencyData.assetPerformance;
  // Bloque ausente (doc de backfill sólo con totales) ≠ bloque vacío (no tenía nada)
  let assetPerformance;
  if (raw && typeof raw === 'object') {
    assetPerformance = {};
    for (const [key, value] of Object.entries(raw)) {
      if (value && typeof value === 'object' && Number.isFinite(Number(value.totalValue))) {
        assetPerformance[key] = { totalValue: Number(value.totalValue) || 0, units: Number(value.units) || 0 };
      }
    }
  }
  return {
    date: data.date || doc.id,
    currencyData: { totalValue: Number(currencyData.totalValue) || 0, assetPerformance },
  };
}

/**
 * Docs con fromStr <= date < untilStr (sin tope si untilStr es null) y el
 * último doc anterior a fromStr. Sólo fecha, valor total y assetPerformance.
 */
async function loadSeriesRange(db, userId, seriesId, currency, fromStr, untilStr) {
  const col = db.collection(performancePath(userId, seriesId));
  const fields = ['date', `${currency}.totalValue`, `${currency}.assetPerformance`];
  let rangeQuery = col.where('date', '>=', fromStr);
  if (untilStr) rangeQuery = rangeQuery.where('date', '<', untilStr);
  const [baseSnap, rangeSnap] = await Promise.all([
    col.where('date', '<', fromStr).orderBy('date', 'desc').limit(1).select(...fields).get(),
    rangeQuery.orderBy('date', 'asc').select(...fields).get(),
  ]);
  return {
    base: baseSnap.docs.length > 0 ? compactDay(baseSnap.docs[0], currency) : null,
    days: rangeSnap.docs.map((doc) => compactDay(doc, currency)),
  };
}

/**
 * Docs de una serie para el período: el último cierre anterior a startStr
 * (base) y todos los de [startStr, endStr].
 */
async function getSeriesDays(db, userId, seriesId, currency, startStr, endStr) {
  const key = `${userId}|${seriesId}|${currency}`;
  let entry = seriesCache.get(key);
  if (entry && Date.now() - entry.at > SERIES_CACHE_TTL_MS) entry = null;

  if (!entry) {
    // entry.days = docs con date >= entry.from; entry.base = último doc anterior (null: no hay más historia)
    const { base, days } = await loadSeriesRange(db, userId, seriesId, currency, startStr, null);
    entry = { at: Date.now(), from: startStr, base, days };
  } else {
    if (startStr < entry.from && entry.base) {
      const older = await loadSeriesRange(db, userId, seriesId, currency, startStr, entry.from);
      entry.days = [...older.days, ...entry.days];
      entry.base = older.base;
      entry.from = startStr;
    }
    const lastDate = entry.days.length > 0 ? entry.days[entry.days.length - 1].date : entry.from;
    const tail = await loadSeriesRange(db, userId, seriesId, currency, lastDate, null);
    entry.days = [...entry.days.filter((d) => d.date < lastDate), ...tail.days];
  }
  seriesCache.delete(key);
  seriesCache.set(key, entry);
  while (seriesCache.size > SERIES_CACHE_MAX_ENTRIES) {
    seriesCache.delete(seriesCache.keys().next().value);
  }

  const inRange = entry.days.filter((d) => d.date >= startStr && (!endStr || d.date <= endStr));
  const before = entry.days.filter((d) => d.date < startStr);
  const base = before.length > 0
    ? before[before.length - 1]
    : (startStr >= entry.from ? entry.base : null);
  return base ? [base, ...inRange] : inRange;
}

async function loadActiveTickers(db, accountIds) {
  const tickers = new Set();
  const snapshots = await Promise.all(accountIds.map((accId) =>
    db.collection('assets')
      .where('portfolioAccount', '==', accId)
      .where('isActive', '==', true)
      .get()
  ));
  for (const snapshot of snapshots) {
    for (const doc of snapshot.docs) {
      const asset = doc.data() || {};
      if ((Number(asset.units) || 0) > 0 && asset.name) tickers.add(carve.normalizeTicker(asset.name));
    }
  }
  return tickers;
}

/** dividendPay de las cuentas con fecha posterior a la base. */
async function loadDividendTransactions(db, accountIds, afterDate) {
  const snapshots = await Promise.all(accountIds.map((accId) =>
    db.collection('transactions')
      .where('portfolioAccountId', '==', accId)
      .where('type', '==', 'dividendPay')
      .get()
  ));
  const txs = [];
  for (const snapshot of snapshots) {
    for (const doc of snapshot.docs) {
      const tx = doc.data() || {};
      if (typeof tx.date === 'string' && tx.date.slice(0, 10) > afterDate) txs.push(tx);
    }
  }
  return txs;
}

/**
 * Moneda de cotización por ticker (casi nunca cambia): evita volver a pedir
 * cotizaciones en cada consulta sólo para saber en qué moneda cotiza.
 */
const QUOTE_CURRENCY_TTL_MS = 24 * 60 * 60 * 1000;
const quoteCurrencyCache = new Map();

/**
 * Rango de históricos a pedir. Siempre al menos '5y': así todos los períodos
 * (y el carve-out, que pide desde el primer doc) comparten la misma entrada
 * de cache por símbolo en vez de bajar '1y', '2y' y '5y' por separado.
 */
function historyRangeFor(firstDate, deps) {
  const range = deps.chooseHistoryRange(firstDate);
  return range === '1y' || range === '2y' ? '5y' : range;
}

/**
 * Moneda de cotización, cierres y tasas de cambio de los tickers tenidos.
 * Si se agota el presupuesto devuelve LO QUE YA LLEGÓ: esos tickers usan
 * precio de mercado y el resto el precio implícito de los docs. Lo que siga
 * llegando queda en cache para la próxima consulta.
 */
async function loadMarketData({ tickers, currency, dividendTxs, firstDate, deps }) {
  const state = { prices: new Map(), quoteCurrency: new Map(), fxSeriesByCurrency: {}, fxNeeded: [], timedOut: false };
  if (tickers.length === 0) return state;

  const now = Date.now();
  const unknown = [];
  for (const ticker of tickers) {
    const hit = quoteCurrencyCache.get(ticker);
    if (hit && now - hit.at < QUOTE_CURRENCY_TTL_MS) state.quoteCurrency.set(ticker, hit.currency);
    else unknown.push(ticker);
  }

  const work = (async () => {
    if (unknown.length > 0) {
      const quotes = await deps.getPricesFromApi(unknown);
      for (const quote of quotes || []) {
        if (!quote?.symbol) continue;
        const ticker = carve.normalizeTicker(quote.symbol);
        const quoteCurrency = carve.normalizeCurrency(quote.currency);
        state.quoteCurrency.set(ticker, quoteCurrency);
        quoteCurrencyCache.set(ticker, { currency: quoteCurrency, at: Date.now() });
      }
    }
    const currencies = new Set([carve.normalizeCurrency(currency)]);
    state.quoteCurrency.forEach((c) => currencies.add(c));
    dividendTxs.forEach((tx) => currencies.add(carve.normalizeCurrency(tx.currency)));
    currencies.delete('USD');
    state.fxNeeded = [...currencies];

    const range = historyRangeFor(firstDate, deps);
    const priced = tickers.filter((t) => state.quoteCurrency.has(t));
    await Promise.all([
      deps.mapLimit(priced, deps.fetchConcurrency, async (t) => {
        const series = await deps.fetchSeries(t, range, deps);
        if (series) state.prices.set(t, series);
      }),
      deps.mapLimit(state.fxNeeded, deps.fetchConcurrency, async (c) => {
        const series = await deps.fetchSeries(carve.fxSymbol(c), range, deps);
        if (series) state.fxSeriesByCurrency[c] = series;
      }),
    ]);
    return true;
  })().catch((error) => {
    console.warn(`[AttributionHoldings] Sin precios de mercado: ${error.message}`);
    return true;
  });

  const finished = await withTimeout(work, deps.priceBudgetMs);
  if (!finished) {
    state.timedOut = true;
    console.warn(`[AttributionHoldings] Presupuesto de precios agotado (${deps.priceBudgetMs} ms): `
      + `${state.prices.size}/${tickers.length} tickers con precio de mercado, el resto con precio implicito`);
  }
  // Copia: lo que llegue después no debe cambiar el cálculo en curso
  return {
    prices: new Map(state.prices),
    quoteCurrency: new Map(state.quoteCurrency),
    fxSeriesByCurrency: { ...state.fxSeriesByCurrency },
    fxNeeded: [...state.fxNeeded],
    timedOut: state.timedOut,
  };
}

function defaultDeps() {
  const admin = require('../firebaseAdmin');
  const { getHistoricalPrices } = require('../financeQuery');
  const { getPricesFromApi } = require('../marketDataHelper');
  const carveService = require('../carveOutReturnsService');
  const { getSellTransactionsInPeriod } = require('./contributionCalculator');
  return {
    db: admin.firestore(),
    getHistoricalPrices,
    getPricesFromApi,
    fetchSeries: carveService.fetchSeries,
    mapLimit: carveService.mapLimit,
    chooseHistoryRange: carveService.chooseHistoryRange,
    fetchConcurrency: carveService.FETCH_CONCURRENCY,
    getSellTransactionsInPeriod,
    priceBudgetMs: PRICE_BUDGET_MS,
  };
}

/**
 * @param {Object} params
 * @param {string} params.userId
 * @param {string} params.period - '1M' | ... | 'ALL'
 * @param {string} [params.currency='USD']
 * @param {string[]} [params.accountIds=['overall']]
 * @param {{startDate: Date, endDate: Date}} [params.dateRange]
 * @param {Object} [deps] - Inyectables para tests
 * @returns {Promise<Object|null>} Forma de calculateContributions + campos del
 *   método; null si no hay datos suficientes
 */
async function calculateHoldingsContributions(params, deps = null) {
  const { userId, period, currency = 'USD', accountIds = ['overall'], dateRange } = params;
  const d = deps || defaultDeps();
  const startedAt = Date.now();

  const periodStartDate = dateRange?.startDate || getPeriodStartDate(period);
  const periodEndDate = dateRange?.endDate || new Date();
  const startStr = toDateStr(periodStartDate);
  const endStr = dateRange?.endDate ? toDateStr(dateRange.endDate) : null;

  const timings = {};
  const lap = (name, since) => { timings[name] = Date.now() - since; return Date.now(); };
  let t = Date.now();

  const accounts = await resolveAccounts(d.db, userId, accountIds);
  if (accounts.seriesIds.length === 0) return null;
  t = lap('accounts', t);

  const seriesList = await Promise.all(
    accounts.seriesIds.map((id) => getSeriesDays(d.db, userId, id, currency, startStr, endStr))
  );
  t = lap('series', t);
  const days = holdings.mergeAccountDays(seriesList);
  if (days.length < 2) return null;

  const baseDate = days[0].date;
  const tickers = carve.collectHeldTickers(days, new Set());

  const [activeTickers, dividendTxs, sellsByAsset] = await Promise.all([
    loadActiveTickers(d.db, accounts.accountIds),
    loadDividendTransactions(d.db, accounts.accountIds, baseDate),
    d.getSellTransactionsInPeriod(userId, periodStartDate, periodEndDate, accounts.accountIds, currency),
  ]);
  t = lap('transactions', t);

  const market = await loadMarketData({ tickers, currency, dividendTxs, firstDate: baseDate, deps: d });
  t = lap('market', t);
  const dividends = carve.groupDividends(dividendTxs, {
    reportCurrency: currency,
    fxSeriesByCurrency: market.fxSeriesByCurrency,
  });

  const result = holdings.computeHoldingsAttribution(days, {
    marketReturn: (ticker, d0, d1) => {
      if (!market.prices.has(ticker)) return null;
      return carve.marketReturnPct({
        priceSeries: market.prices.get(ticker),
        quoteCurrency: market.quoteCurrency.get(ticker),
        reportCurrency: currency,
        fxSeriesByCurrency: market.fxSeriesByCurrency,
      }, d0, d1);
    },
    dividendCash: (ticker, d0, d1) => carve.dividendsBetween(dividends.byTicker, ticker, d0, d1),
  });
  if (!result || result.coverage.steps === 0) return null;
  lap('compute', t);

  // Ventas del período agrupadas por ticker (las llaves de transacciones son name_assetType)
  const sellsByTicker = new Map();
  let totalRealizedPnL = 0;
  for (const sell of Object.values(sellsByAsset || {})) {
    const ticker = carve.normalizeTicker(sell.ticker);
    const agg = sellsByTicker.get(ticker) || { realizedPnL: 0, count: 0 };
    agg.realizedPnL += sell.totalRealizedPnL || 0;
    agg.count += sell.transactions?.length || 0;
    sellsByTicker.set(ticker, agg);
    totalRealizedPnL += sell.totalRealizedPnL || 0;
  }

  const attributions = [];
  for (const a of result.assets) {
    const isActive = a.valueEnd > 0 && activeTickers.has(a.normalizedTicker);
    // Nunca tuvo valor al cierre de un día del período: no hay nada que atribuir
    if (!isActive && a.heldSteps === 0) continue;
    const sells = sellsByTicker.get(a.normalizedTicker);
    const salesCount = sells?.count || 0;
    const realizedPnL = sells?.realizedPnL || 0;
    attributions.push({
      assetKey: a.assetKey || `${a.ticker}_${a.type}`,
      ticker: a.ticker,
      name: a.ticker,
      sector: 'Unknown',
      logo: null,
      type: a.type,
      status: isActive ? 'active' : 'sold',
      weightStart: a.weightStart,
      weightEnd: isActive ? a.weightEnd : 0,
      weightAverage: a.avgWeight,
      returnPercent: a.returnPct,
      contribution: a.contribution,
      contributionAbsolute: a.dollarChange,
      valueStart: a.valueStart,
      valueEnd: isActive ? a.valueEnd : 0,
      valueChange: a.dollarChange,
      realizedPnL,
      isNewAsset: a.valueStart === 0,
      hasPartialSales: isActive && salesCount > 0,
      partialSalesPnL: isActive && salesCount > 0 ? realizedPnL : undefined,
      partialSalesCount: salesCount > 0 ? salesCount : undefined,
      daysHeld: a.heldSteps,
      heldFrom: a.firstHeldDate,
      heldTo: a.lastHeldDate,
      _source: { method: 'holdings', priceSources: a.sources },
    });
  }
  attributions.sort((x, y) => y.contribution - x.contribution);

  const sumOfContributions = attributions.reduce((sum, x) => sum + x.contribution, 0);
  const firstPeriodDay = days.find((day) => day.date >= startStr);
  const tickersWithoutQuote = tickers.filter((t) => !market.quoteCurrency.has(t));

  console.log(`[AttributionHoldings] user=${userId} series=${accounts.seriesIds.join(',')} ${currency} ${startStr}..${endStr || 'hoy'} `
    + `dias=${days.length} pasos=${result.coverage.steps} TWR=${result.twr.toFixed(2)}% activos=${attributions.length} `
    + `precios mercado/implicito/sin=${result.coverage.marketReturns}/${result.coverage.impliedReturns}/${result.coverage.missingReturns} `
    + `${market.timedOut ? 'PRESUPUESTO-AGOTADO ' : ''}${Date.now() - startedAt}ms ${JSON.stringify(timings)}`);

  return {
    method: 'holdings',
    attributions,
    totalPortfolioValue: result.endValue,
    totalPortfolioInvestment: 0,
    portfolioReturn: result.twr,
    holdingsTWR: result.twr,
    growthFactor: result.factor,
    latestDate: result.endDate,
    periodStartDate: firstPeriodDay ? firstPeriodDay.date : baseDate,
    baseDate,
    startTotalValue: result.startValue,
    sumOfContributions,
    discrepancy: 0,
    normalized: false,
    latestPerformanceData: { date: result.endDate, [currency]: result.latest },
    coverage: {
      ...result.coverage,
      days: days.length,
      seriesIds: accounts.seriesIds,
      tickers: tickers.length,
      tickersWithoutQuote,
      tickersWithMarketPrice: market.prices.size,
      currenciesWithoutFx: market.fxNeeded.filter((c) => !market.fxSeriesByCurrency[c]),
      priceBudgetExhausted: market.timedOut,
      dividendsUnconverted: dividends.unconverted,
      timingsMs: timings,
    },
    realizedPnL: {
      total: totalRealizedPnL,
      assetsWithSales: sellsByTicker.size,
      closedPositions: attributions.filter((x) => x.status === 'sold').length,
    },
  };
}

module.exports = {
  calculateHoldingsContributions,
  PRICE_BUDGET_MS,
  // Exportados para test
  _resolveAccounts: resolveAccounts,
  _withTimeout: withTimeout,
  _getSeriesDays: getSeriesDays,
  _seriesCache: seriesCache,
  _quoteCurrencyCache: quoteCurrencyCache,
};
