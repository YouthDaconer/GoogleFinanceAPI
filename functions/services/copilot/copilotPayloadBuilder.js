/**
 * HU 5.2 — "Preparando los datos de tu portafolio", en el backend.
 *
 * Port de `buildEnrichedBasePayload` (`hooks/useCopilotAnalysis.ts`), que hasta
 * ahora corría en el navegador. Se muda porque QA-22 hace que el usuario cierre
 * la pestaña **durante** esta etapa y el análisis tiene que seguir: una etapa
 * que vive en el navegador muere con él.
 *
 * RN-5.2-R — el retrato es el del momento en que se lanzó: el payload se arma
 * una sola vez, al entrar en la etapa, y los cambios posteriores del portafolio
 * no se incorporan.
 *
 * Lo que el navegador hacía y aquí no: combinar el movimiento intradía. El
 * `todayFactor` sale de un hook de precios en vivo que el backend no tiene, así
 * que los rendimientos son los del último cierre consolidado e `isLive` es
 * `false`. Declararlo `true` sin haber aplicado el factor sería exactamente el
 * progreso cosmético que esta historia viene a quitar.
 *
 * @module services/copilot/copilotPayloadBuilder
 * @see platform-docs/stories/5.2-copilot-segundo-plano-progreso/refinamiento.md (T4)
 */

const admin = require('../firebaseAdmin');
const positions = require('./copilotPositions');
const queryHandlers = require('../handlers/queryHandlers');
const { FINANCE_QUERY_API_URL, getServiceHeaders } = require('../config');

/** Tope de activos que se nombran como mayores subidas o bajadas. */
const MAX_MOVERS = 5;

/** Qué tan lejos se espera a finance-query por las métricas de riesgo. */
const RISK_METRICS_TIMEOUT_MS = 10000;

/**
 * Perfil de riesgo a partir de la beta, con los mismos cortes que usaba el
 * navegador: cambiarlos cambiaría el análisis, y RN-5.2-S lo prohíbe.
 *
 * @param {number|null|undefined} beta
 * @returns {string}
 */
function deriveRiskCategory(beta) {
  const b = beta === null || beta === undefined ? 1.0 : beta;
  if (b < 0.8) return 'conservative';
  if (b > 1.5) return 'very_aggressive';
  if (b > 1.2) return 'aggressive';
  return 'moderate';
}

/**
 * Tasas por divisa (unidades locales por 1 USD), desde el catálogo.
 *
 * @param {object[]} currencies
 * @returns {Map<string, number>}
 */
function buildCurrencyRateMap(currencies) {
  const rates = new Map([['USD', 1]]);
  for (const c of currencies) {
    if (c.code && c.exchangeRate) rates.set(c.code, c.exchangeRate);
  }
  return rates;
}

/**
 * Posiciones del usuario agregadas por ticker, con su peso.
 *
 * Son las posiciones reales, no los constituyentes en que la API de
 * distribución descompone los ETF: las subidas y la concentración se cuentan
 * sobre lo que el usuario tiene, igual que en el navegador.
 *
 * @param {object[]} assets
 * @param {Map<string, object>} priceMap
 * @param {Map<string, number>} rates
 * @returns {Array<{symbol: string, weight: number, sector: string, asset_type: string}>}
 */
function buildUserHoldings(assets, priceMap, rates) {
  const byTicker = new Map();

  for (const a of assets) {
    if (!a.units || a.units <= 0) continue;
    const ticker = a.name || '';
    if (!ticker) continue;

    const priceInfo = priceMap.get(ticker);
    const assetCurrency = (priceInfo && priceInfo.currency) || a.currency || 'USD';
    const rate = rates.get(assetCurrency) || 1;
    const costUSD = rate > 0 ? ((a.unitValue || 0) * a.units) / rate : 0;
    const precio = priceInfo && priceInfo.price !== undefined ? priceInfo.price : a.unitValue;
    const priceUSD = rate > 0 ? (precio || 0) / rate : 0;

    const existing = byTicker.get(ticker);
    if (existing) {
      existing.totalCostUSD += costUSD;
      existing.totalUnits += a.units;
    } else {
      byTicker.set(ticker, {
        totalCostUSD: costUSD,
        totalUnits: a.units,
        priceUSD,
        sector: (priceInfo && priceInfo.sector) || a.sector || 'Other',
        assetType: a.assetType || '',
        currency: assetCurrency,
        // HU 5.3: el prompt de investigación necesita el nombre para
        // desambiguar — "MELI" a secas se confunde con otras cosas.
        name: (priceInfo && priceInfo.name) || a.company || ticker,
      });
    }
  }

  let totalValueUSD = 0;
  for (const [, data] of byTicker) totalValueUSD += data.priceUSD * data.totalUnits;

  const holdings = [];
  for (const [symbol, data] of byTicker) {
    const weight = totalValueUSD > 0 ? (data.priceUSD * data.totalUnits) / totalValueUSD : 0;
    holdings.push({
      symbol,
      weight,
      sector: data.sector,
      asset_type: data.assetType,
      // HU 5.3: el resto del portafolio se agrupa también por divisa
      currency: data.currency,
      name: data.name,
    });
  }

  return holdings.sort((a, b) => b.weight - a.weight);
}

/**
 * Las mayores subidas o bajadas frente al costo promedio, en USD.
 *
 * @param {Array<object>} userHoldings
 * @param {object[]} assets
 * @param {Map<string, object>} priceMap
 * @param {Map<string, number>} rates
 * @param {'gainers'|'losers'} direction
 * @returns {Array<{ticker: string, returnPct: number, weight: number}>}
 */
function extractTopMovers(userHoldings, assets, priceMap, rates, direction) {
  const assetsByTicker = new Map();

  for (const a of assets) {
    if (!a.units || a.units <= 0 || !a.unitValue) continue;
    const ticker = a.name || '';
    if (!ticker) continue;
    const priceInfo = priceMap.get(ticker);
    const assetCurrency = (priceInfo && priceInfo.currency) || a.currency || 'USD';
    const rate = rates.get(assetCurrency) || 1;
    const costUSD = rate > 0 ? (a.unitValue * a.units) / rate : 0;

    const existing = assetsByTicker.get(ticker);
    if (existing) {
      existing.totalCostUSD += costUSD;
      existing.totalUnits += a.units;
    } else {
      assetsByTicker.set(ticker, { totalCostUSD: costUSD, totalUnits: a.units });
    }
  }

  const enriched = [];
  for (const h of userHoldings) {
    const priceInfo = priceMap.get(h.symbol);
    const assetData = assetsByTicker.get(h.symbol);
    if (!priceInfo || !priceInfo.price || !assetData || assetData.totalUnits <= 0) continue;

    const rate = rates.get(priceInfo.currency || 'USD') || 1;
    const avgCostUSD = assetData.totalCostUSD / assetData.totalUnits;
    const currentPriceUSD = rate > 0 ? priceInfo.price / rate : 0;
    const returnPct = avgCostUSD > 0 ? ((currentPriceUSD - avgCostUSD) / avgCostUSD) * 100 : 0;

    enriched.push({
      ticker: h.symbol,
      returnPct: Math.round(returnPct * 10) / 10,
      weight: Math.round(h.weight * 1000) / 10,
    });
  }

  if (direction === 'gainers') {
    return enriched
      .filter((a) => a.returnPct > 0)
      .sort((a, b) => b.returnPct - a.returnPct)
      .slice(0, MAX_MOVERS);
  }

  return enriched
    .filter((a) => a.returnPct < 0)
    .sort((a, b) => a.returnPct - b.returnPct)
    .slice(0, MAX_MOVERS);
}

/**
 * Concentración del portafolio: peso de las tres mayores, número de activos e
 * índice de Herfindahl.
 *
 * @param {Array<{weight: number}>} userHoldings
 * @returns {{top3Weight: number, totalAssets: number, herfindahlIndex: number}}
 */
function buildConcentration(userHoldings) {
  const sorted = [...userHoldings].sort((a, b) => b.weight - a.weight);
  const top3Weight = sorted.slice(0, 3).reduce((s, h) => s + h.weight * 100, 0);
  const hhi = userHoldings.reduce((sum, h) => sum + Math.pow(h.weight, 2), 0);

  return {
    top3Weight: Math.round(top3Weight * 10) / 10,
    totalAssets: userHoldings.length,
    herfindahlIndex: Math.round(hhi * 100) / 100,
  };
}

/**
 * Métricas de riesgo desde finance-query. Si no responde, el análisis sigue con
 * una beta neutra: ninguna fuente auxiliar puede tumbar un análisis (QA-19).
 *
 * @param {string} userId
 * @param {string} currency
 * @returns {Promise<object|null>}
 */
async function fetchRiskMetrics(userId, currency) {
  try {
    const params = new URLSearchParams({ userId, period: '1Y', currency });
    const response = await fetch(`${FINANCE_QUERY_API_URL}/risk-metrics?${params}`, {
      headers: getServiceHeaders(),
      signal: AbortSignal.timeout(RISK_METRICS_TIMEOUT_MS),
    });
    if (!response.ok) return null;
    return await response.json();
  } catch (error) {
    console.error('[copilotPayloadBuilder] Error consultando métricas de riesgo:', error);
    return null;
  }
}

/**
 * Lee de Firestore lo que el navegador tenía en el store de Zustand.
 *
 * @param {string} userId
 * @returns {Promise<{assets: object[], priceMap: Map, currencies: object[], accounts: object[]}>}
 */
async function readPortfolioState(userId) {
  const db = admin.firestore();

  // Los activos y las cuentas salen de `copilotPositions`: un `asset` no tiene
  // `userId` y se llega a él por su cuenta. Filtrarlos por `userId` aquí daba un
  // portafolio vacío sin fallar, que es la peor forma de equivocarse.
  const [posiciones, pricesSnap, currenciesSnap] = await Promise.all([
    positions.leerPosiciones(userId),
    db.collection('currentPrices').get(),
    db.collection('currencies').get(),
  ]);

  const priceMap = new Map();
  for (const doc of pricesSnap.docs) {
    priceMap.set(doc.id, { symbol: doc.id, ...doc.data() });
  }

  return {
    assets: posiciones.assets,
    priceMap,
    currencies: currenciesSnap.docs.map((doc) => doc.data()),
    accounts: posiciones.accounts,
  };
}

/**
 * Arma el retrato del portafolio que recibirá el motor.
 *
 * @param {string} userId
 * @param {{ language: string, currency: string }} run
 * @returns {Promise<{ sinPosiciones: boolean, payload: object|null, estado: object|null }>}
 *   `sinPosiciones` corta el análisis antes de gastar nada (Escenario 7).
 */
async function buildBasePayload(userId, { language, currency }) {
  const estado = await readPortfolioState(userId);
  const rates = buildCurrencyRateMap(estado.currencies);
  const userHoldings = buildUserHoldings(estado.assets, estado.priceMap, rates);

  if (userHoldings.length === 0) {
    return { sinPosiciones: true, payload: null, estado: null };
  }

  const contexto = { auth: { uid: userId } };

  const [distributionResult, returnsResult, riskResult] = await Promise.allSettled([
    queryHandlers.getPortfolioDistribution(contexto, { currency, includeHoldings: true }),
    queryHandlers.getMultiAccountHistoricalReturns(contexto, {
      currency,
      accountIds: ['overall'],
    }),
    fetchRiskMetrics(userId, currency),
  ]);

  const distribution = distributionResult.status === 'fulfilled' ? distributionResult.value : null;
  const returns = returnsResult.status === 'fulfilled' ? returnsResult.value : null;
  const riskMetrics = riskResult.status === 'fulfilled' ? riskResult.value : null;

  if (distributionResult.status === 'rejected') {
    console.error('[copilotPayloadBuilder] Distribución no disponible:', distributionResult.reason);
  }
  if (returnsResult.status === 'rejected') {
    console.error('[copilotPayloadBuilder] Rendimientos no disponibles:', returnsResult.reason);
  }

  const sectors = (distribution && distribution.sectors) || [];
  const returnsData = (returns && returns.returns) || returns || {};
  const beta = riskMetrics && riskMetrics.beta !== undefined ? riskMetrics.beta : 1.0;
  const profitableWeeks =
    riskMetrics && riskMetrics.profitableWeeks !== undefined ? riskMetrics.profitableWeeks : 50;

  const totalPortfolioValue =
    distribution && distribution.totals && distribution.totals.portfolioValue
      ? Math.round(distribution.totals.portfolioValue * 100) / 100
      : undefined;

  const payload = {
    portfolioBeta: Math.round(beta * 100) / 100,
    riskCategory: deriveRiskCategory(beta),
    sectorDistribution: sectors.map((s) => ({
      sector: s.sector,
      weight: Math.round((s.percentage !== undefined ? s.percentage : s.weight * 100) * 10) / 10,
    })),
    topGainers: extractTopMovers(userHoldings, estado.assets, estado.priceMap, rates, 'gainers'),
    topLosers: extractTopMovers(userHoldings, estado.assets, estado.priceMap, rates, 'losers'),
    ytdReturn: Math.round((returnsData.ytdReturn || 0) * 10) / 10,
    profitableWeeksPercentage: Math.round(profitableWeeks * 10) / 10,
    concentration: buildConcentration(userHoldings),
    userCurrency: currency,
    userLanguage: language,
    totalPortfolioValue,
    periodReturns: {
      oneMonth: Math.round((returnsData.oneMonthReturn || 0) * 100) / 100,
      threeMonth: Math.round((returnsData.threeMonthReturn || 0) * 100) / 100,
      sixMonth: Math.round((returnsData.sixMonthReturn || 0) * 100) / 100,
      oneYear: Math.round((returnsData.oneYearReturn || 0) * 100) / 100,
    },
    isLive: false,
  };

  return { sinPosiciones: false, payload, estado: { ...estado, rates, userHoldings } };
}

module.exports = {
  MAX_MOVERS,
  deriveRiskCategory,
  buildCurrencyRateMap,
  buildUserHoldings,
  extractTopMovers,
  buildConcentration,
  readPortfolioState,
  buildBasePayload,
};
