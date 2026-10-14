/**
 * HU 5.2 — "Consultando analistas y noticias de tus principales activos".
 *
 * Port de `services/copilotPayloadEnricher.ts`, que corría en el navegador y
 * que esta historia **elimina** de allí: el análisis tiene que sobrevivir a que
 * el usuario cierre la pestaña (RN-5.2-N), así que ninguna de sus etapas puede
 * depender de él.
 *
 * Lo único que cambia respecto al original es el plazo de espera: en el
 * navegador eran 3 s porque el usuario estaba delante; aquí no hay nadie
 * esperando y bajar la calidad del análisis por tres segundos no tiene sentido.
 * La degradación aislada por sección se conserva intacta — si analistas y
 * noticias no responden, el análisis se completa igual y esas secciones
 * simplemente no aparecen (QA-19).
 *
 * @module services/copilot/copilotEnricher
 * @see platform-docs/stories/5.2-copilot-segundo-plano-progreso/refinamiento.md (T5)
 */

const { DateTime } = require('luxon');
const { FINANCE_QUERY_API_URL, getServiceHeaders } = require('../config');

const MAX_TOP_HOLDINGS = 5;
const MAX_NEWS_TICKERS = 3;
const MAX_NEWS_PER_TICKER = 3;
const MAX_TOTAL_NEWS = 10;
const MAX_ACCOUNTS = 5;
const MAX_UPCOMING_DIVIDENDS = 10;

/** Ver la cabecera del módulo: en el navegador eran 3 s. */
const FETCH_TIMEOUT_MS = 10000;

/**
 * Resuelve a `null` si la promesa tarda demasiado, en vez de propagar el fallo.
 *
 * @param {Promise<*>} promesa
 * @param {number} ms
 * @returns {Promise<*|null>}
 */
function withTimeout(promesa, ms) {
  let temporizador;
  const limite = new Promise((resolve) => {
    temporizador = setTimeout(() => resolve(null), ms);
  });
  return Promise.race([promesa, limite]).finally(() => clearTimeout(temporizador));
}

/**
 * Normaliza la recomendación del proveedor al vocabulario del prompt.
 *
 * @param {string} key
 * @returns {string}
 */
function mapRecommendationKey(key) {
  const mapping = {
    strong_buy: 'strongBuy',
    strongbuy: 'strongBuy',
    buy: 'buy',
    hold: 'hold',
    sell: 'sell',
    strong_sell: 'strongSell',
    strongsell: 'strongSell',
  };
  return mapping[String(key || '').toLowerCase()] || 'hold';
}

/**
 * "Q3 2026" a partir de la fecha de cierre del trimestre.
 *
 * @param {string} [endDate]
 * @returns {string}
 */
function formatEarningsPeriod(endDate) {
  if (!endDate) return '';
  const fecha = DateTime.fromISO(String(endDate), { zone: 'America/New_York' });
  if (!fecha.isValid) return '';
  return `Q${fecha.quarter} ${fecha.year}`;
}

/**
 * Antigüedad del titular, respetando lo que ya venga formateado.
 *
 * @param {string} timeStr
 * @param {Date} [now]
 * @returns {string}
 */
function formatTimeAgo(timeStr, now = new Date()) {
  if (!timeStr) return '';
  if (timeStr.includes('ago') || timeStr.includes('hace')) return timeStr;

  const fecha = DateTime.fromISO(String(timeStr), { zone: 'America/New_York' });
  if (!fecha.isValid) return timeStr;

  const diffH = Math.floor(
    DateTime.fromJSDate(now, { zone: 'America/New_York' }).diff(fecha, 'hours').hours,
  );
  if (diffH < 1) return '<1h ago';
  if (diffH < 24) return `${diffH}h ago`;
  return `${Math.floor(diffH / 24)}d ago`;
}

/**
 * Los activos del usuario, normalizados a USD y ordenados por valor de mercado.
 *
 * @param {object[]} assets - Documentos de `assets`.
 * @param {Map<string, object>} priceMap
 * @param {Map<string, number>} rates
 * @returns {object[]}
 */
function rankAssetsByValue(assets, priceMap, rates) {
  const grouped = new Map();

  for (const a of assets) {
    if (!a.units || a.units <= 0) continue;
    const ticker = a.name || '';
    if (!ticker) continue;

    const priceInfo = priceMap.get(ticker);
    const assetCurrency = (priceInfo && priceInfo.currency) || a.currency || 'USD';
    const rate = rates.get(assetCurrency) || 1;
    const rawPrice = priceInfo && priceInfo.price !== undefined ? priceInfo.price : a.unitValue || 0;
    const priceUSD = rate > 0 ? rawPrice / rate : rawPrice;
    const costUSD = rate > 0 ? (a.unitValue || 0) / rate : a.unitValue || 0;
    if (priceUSD <= 0) continue;

    // El nombre que ve el analista: el de la API, luego el que escribió el
    // usuario, y sólo al final el ticker. Nunca el nombre de la cuenta.
    const displayName =
      (priceInfo && priceInfo.name && priceInfo.name.length > 1 && priceInfo.name) ||
      (a.company && a.company.length > 1 && a.company) ||
      ticker;

    const existing = grouped.get(ticker);
    if (existing) {
      existing.totalCostUSD += costUSD * a.units;
      existing.totalShares += a.units;
    } else {
      grouped.set(ticker, {
        totalShares: a.units,
        totalCostUSD: costUSD * a.units,
        price: priceUSD,
        name: displayName,
        sector: (priceInfo && priceInfo.sector) || a.sector || 'Other',
        currency: assetCurrency,
        portfolioAccount: a.portfolioAccount || '',
      });
    }
  }

  let totalValue = 0;
  const enriched = [];

  for (const [ticker, g] of grouped) {
    const marketValue = g.price * g.totalShares;
    totalValue += marketValue;
    const avgCost = g.totalShares > 0 ? g.totalCostUSD / g.totalShares : 0;
    const roi = avgCost > 0 ? ((g.price - avgCost) / avgCost) * 100 : 0;

    enriched.push({
      ticker,
      name: g.name,
      sector: g.sector,
      shares: g.totalShares,
      averageCost: avgCost,
      currentPrice: g.price,
      portfolioAccount: g.portfolioAccount,
      currency: g.currency,
      marketValue,
      weight: 0,
      roi: Math.round(roi * 10) / 10,
      unrealizedPnL: Math.round((marketValue - g.totalCostUSD) * 100) / 100,
    });
  }

  for (const item of enriched) {
    item.weight = totalValue > 0 ? Math.round((item.marketValue / totalValue) * 1000) / 10 : 0;
  }

  return enriched.sort((a, b) => b.marketValue - a.marketValue);
}

/**
 * Consenso de analistas y próximos resultados de las mayores posiciones.
 *
 * @param {object[]} rankedAssets
 * @param {{fetchAnalystConsensus: Function}} fetchers
 * @returns {Promise<object[]>}
 */
async function enrichTopHoldings(rankedAssets, fetchers) {
  const top = rankedAssets.slice(0, MAX_TOP_HOLDINGS);
  if (top.length === 0) return [];

  const results = await Promise.allSettled(
    top.map((a) => withTimeout(fetchers.fetchAnalystConsensus(a.ticker), FETCH_TIMEOUT_MS)),
  );

  return top.map((asset, i) => {
    const result = results[i];
    const apiData = result.status === 'fulfilled' ? result.value : null;

    const holding = {
      ticker: asset.ticker,
      name: asset.name,
      sector: asset.sector,
      weight: asset.weight,
      roi: asset.roi,
      unrealizedPnL: asset.unrealizedPnL,
    };

    if (apiData && apiData.targetPrice && apiData.recommendationKey) {
      const { targetPrice } = apiData;
      holding.analystConsensus = {
        recommendation: mapRecommendationKey(apiData.recommendationKey),
        targetPriceMean: targetPrice.mean,
        targetPriceLow: targetPrice.low,
        targetPriceHigh: targetPrice.high,
        currentPrice: targetPrice.current,
        upsidePercent:
          targetPrice.current > 0
            ? Math.round(((targetPrice.mean - targetPrice.current) / targetPrice.current) * 1000) / 10
            : 0,
        numberOfAnalysts: targetPrice.numberOfAnalysts,
      };
    }

    if (apiData && apiData.earningsEstimates && apiData.earningsEstimates.length > 0) {
      const next = apiData.earningsEstimates[0];
      holding.nextEarnings = {
        period: formatEarningsPeriod(next.endDate) || next.period,
        estimateAvg: next.avg,
        estimateLow: next.low,
        estimateHigh: next.high,
      };
    }

    return holding;
  });
}

/**
 * Titulares recientes de los tickers principales.
 *
 * @param {object[]} rankedAssets
 * @param {{fetchNews: Function}} fetchers
 * @returns {Promise<object[]>}
 */
async function enrichRecentNews(rankedAssets, fetchers) {
  const tickers = rankedAssets.slice(0, MAX_NEWS_TICKERS).map((a) => a.ticker);
  if (tickers.length === 0) return [];

  const results = await Promise.allSettled(
    tickers.map((t) => withTimeout(fetchers.fetchNews(t), FETCH_TIMEOUT_MS)),
  );

  const allNews = [];
  for (let i = 0; i < tickers.length; i++) {
    const result = results[i];
    const articles = result.status === 'fulfilled' ? result.value : null;
    if (!Array.isArray(articles)) continue;

    for (const article of articles.slice(0, MAX_NEWS_PER_TICKER)) {
      if (allNews.length >= MAX_TOTAL_NEWS) break;
      allNews.push({
        ticker: tickers[i],
        title: article.title,
        source: article.source,
        timeAgo: formatTimeAgo(article.time),
      });
    }
  }

  return allNews;
}

/**
 * Comparativo por cuenta: valor, ROI, activo dominante y sector dominante.
 *
 * @param {object[]} rankedAssetsPorCuenta - Activos normalizados a USD.
 * @param {object[]} accounts
 * @returns {object[]}
 */
function buildAccountsSummary(rankedAssetsPorCuenta, accounts) {
  if (accounts.length === 0) return [];

  const accountMap = new Map(accounts.map((a) => [a.id, a.name || a.id]));
  const byAccount = new Map();

  for (const asset of rankedAssetsPorCuenta) {
    if (asset.shares <= 0 || asset.currentPrice <= 0) continue;
    const acctId = asset.portfolioAccount;
    if (!acctId || !accountMap.has(acctId)) continue;
    const list = byAccount.get(acctId) || [];
    list.push(asset);
    byAccount.set(acctId, list);
  }

  const summaries = [];

  for (const [acctId, assetsDeLaCuenta] of byAccount) {
    const tickerMap = new Map();

    for (const a of assetsDeLaCuenta) {
      const value = a.currentPrice * a.shares;
      const cost = a.averageCost * a.shares;
      const existing = tickerMap.get(a.ticker);
      if (existing) {
        existing.shares += a.shares;
        existing.cost += cost;
        existing.value += value;
      } else {
        tickerMap.set(a.ticker, {
          shares: a.shares,
          cost,
          value,
          sector: a.sector || 'Other',
          currency: a.currency || 'USD',
        });
      }
    }

    let totalValue = 0;
    let totalCost = 0;
    for (const t of tickerMap.values()) {
      totalValue += t.value;
      totalCost += t.cost;
    }

    const accountROI =
      totalCost > 0 ? Math.round(((totalValue - totalCost) / totalCost) * 1000) / 10 : 0;

    let topTicker = '';
    let topValue = 0;
    for (const [ticker, t] of tickerMap) {
      if (t.value > topValue) {
        topTicker = ticker;
        topValue = t.value;
      }
    }

    const sectorMap = new Map();
    for (const t of tickerMap.values()) {
      sectorMap.set(t.sector, (sectorMap.get(t.sector) || 0) + t.value);
    }

    let dominantSector = 'Other';
    let dominantSectorValue = 0;
    for (const [sec, val] of sectorMap) {
      if (val > dominantSectorValue) {
        dominantSector = sec;
        dominantSectorValue = val;
      }
    }

    const monedas = new Set([...tickerMap.values()].map((t) => t.currency).filter(Boolean));

    summaries.push({
      accountName: accountMap.get(acctId) || acctId,
      totalValue: Math.round(totalValue * 100) / 100,
      assetCount: tickerMap.size,
      accountROI,
      topAsset: topTicker,
      topAssetWeight: totalValue > 0 ? Math.round((topValue / totalValue) * 1000) / 10 : 0,
      dominantSector,
      dominantSectorWeight:
        totalValue > 0 ? Math.round((dominantSectorValue / totalValue) * 1000) / 10 : 0,
      originalCurrency: monedas.size === 1 ? [...monedas][0] : undefined,
    });
  }

  return summaries.sort((a, b) => b.totalValue - a.totalValue).slice(0, MAX_ACCOUNTS);
}

/**
 * Renta por dividendos, a partir de datos ya calculados.
 *
 * Igual que en el navegador, estos datos sólo llegan si alguien los precalcula;
 * cuando no están, la sección no aparece. Es el comportamiento vigente y
 * RN-5.2-S pide conservarlo: la HU 5.2 no cambia el contenido del análisis.
 *
 * @param {object} state
 * @param {Date} [now]
 * @returns {object|undefined}
 */
function buildDividendMetrics(state, now = new Date()) {
  const metrics = state.dividendMetrics;
  if (!metrics || metrics.totalAssets === 0) return undefined;

  const ahora = DateTime.fromJSDate(now, { zone: 'America/New_York' });
  const fechaDePago = (d) =>
    d.paymentDate instanceof Date
      ? DateTime.fromJSDate(d.paymentDate, { zone: 'America/New_York' })
      : DateTime.fromISO(String(d.paymentDate), { zone: 'America/New_York' });

  const upcoming = (state.upcomingDividends || [])
    .filter((d) => {
      const pago = fechaDePago(d);
      if (!pago.isValid) return false;
      const diasHasta = pago.diff(ahora, 'days').days;
      return diasHasta >= 0 && diasHasta <= 30;
    })
    .slice(0, MAX_UPCOMING_DIVIDENDS)
    .map((d) => ({
      ticker: d.symbol,
      estimatedAmount: Math.round(d.estimatedAmount * 100) / 100,
      paymentDate: fechaDePago(d).toISODate(),
    }));

  return {
    portfolioYield: Math.round(metrics.portfolioYield * 100) / 100,
    annualProjection: Math.round(metrics.annualProjection * 100) / 100,
    totalReceivedYTD: Math.round(metrics.totalReceivedYTD * 100) / 100,
    dividendPayingAssets: metrics.dividendPayingAssets,
    totalAssets: metrics.totalAssets,
    upcomingPayments: upcoming,
  };
}

/**
 * Una consulta a finance-query por el canal autenticado de servicio a servicio
 * que ya usan las demás Cloud Functions.
 *
 * Nunca lanza: un endpoint que no responde devuelve `null`, y quien pregunta
 * decide qué hacer sin esa sección. Es la degradación aislada que QA-19 (HU
 * 5.2) exige y de la que depende también RN-5.6-D.
 *
 * Es de módulo, y no una closure dentro de `createEnrichmentFetchers`, porque
 * `copilotFundamentals` (HU 5.6) necesita exactamente este canal: tenerlo
 * escondido obligaba a copiarlo.
 *
 * @param {string} endpoint - Ruta relativa a `FINANCE_QUERY_API_URL`.
 * @param {string} [quien] - Prefijo del log, para saber quién preguntaba.
 * @returns {Promise<object|null>}
 */
async function pedirAFinanceQuery(endpoint, quien = 'copilotEnricher') {
  try {
    const response = await fetch(`${FINANCE_QUERY_API_URL}${endpoint}`, {
      headers: getServiceHeaders({ Accept: 'application/json' }),
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
    if (!response.ok) return null;
    return await response.json();
  } catch (error) {
    console.error(`[${quien}] Error consultando ${endpoint}:`, error);
    return null;
  }
}

/**
 * Los fetchers reales contra finance-query.
 *
 * @returns {{fetchAnalystConsensus: Function, fetchNews: Function}}
 */
function createEnrichmentFetchers() {
  const pedir = (endpoint) => pedirAFinanceQuery(endpoint);

  return {
    fetchAnalystConsensus: (symbol) => pedir(`/analysis/${symbol}`),
    fetchNews: (symbol) => pedir(`/news?symbol=${encodeURIComponent(symbol)}`),
  };
}

/**
 * Enriquece el payload. Cada sección falla por su cuenta: lo que no llega,
 * no aparece, y el análisis se completa igual (QA-19).
 *
 * @param {{assets: object[], priceMap: Map, rates: Map, accounts: object[]}} estado
 * @param {object} [fetchers]
 * @returns {Promise<object>} Las secciones que sí tienen datos.
 */
async function enrichCopilotPayload(estado, fetchers = createEnrichmentFetchers()) {
  const rankedAssets = rankAssetsByValue(estado.assets, estado.priceMap, estado.rates);
  const sections = {};

  const [holdingsResult, newsResult] = await Promise.allSettled([
    enrichTopHoldings(rankedAssets, fetchers),
    enrichRecentNews(rankedAssets, fetchers),
  ]);

  if (holdingsResult.status === 'fulfilled' && holdingsResult.value.length > 0) {
    sections.topHoldings = holdingsResult.value;
  } else if (holdingsResult.status === 'rejected') {
    console.error('[copilotEnricher] Sin datos de analistas:', holdingsResult.reason);
  }

  if (newsResult.status === 'fulfilled' && newsResult.value.length > 0) {
    sections.recentNews = newsResult.value;
  } else if (newsResult.status === 'rejected') {
    console.error('[copilotEnricher] Sin noticias:', newsResult.reason);
  }

  try {
    const accountsSummary = buildAccountsSummary(rankedAssets, estado.accounts);
    if (accountsSummary.length > 0) sections.accountsSummary = accountsSummary;
  } catch (error) {
    console.error('[copilotEnricher] Sin comparativo de cuentas:', error);
  }

  try {
    const dividendMetrics = buildDividendMetrics(estado);
    if (dividendMetrics) sections.dividendMetrics = dividendMetrics;
  } catch (error) {
    console.error('[copilotEnricher] Sin métricas de dividendos:', error);
  }

  return sections;
}

module.exports = {
  MAX_TOP_HOLDINGS,
  MAX_NEWS_TICKERS,
  MAX_NEWS_PER_TICKER,
  MAX_TOTAL_NEWS,
  MAX_ACCOUNTS,
  FETCH_TIMEOUT_MS,
  withTimeout,
  mapRecommendationKey,
  formatEarningsPeriod,
  formatTimeAgo,
  rankAssetsByValue,
  enrichTopHoldings,
  enrichRecentNews,
  buildAccountsSummary,
  buildDividendMetrics,
  pedirAFinanceQuery,
  createEnrichmentFetchers,
  enrichCopilotPayload,
};
