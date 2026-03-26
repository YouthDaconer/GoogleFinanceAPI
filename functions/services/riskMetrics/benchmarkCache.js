/**
 * Benchmark Cache Manager
 * 
 * Maneja el cache de datos de benchmark (S&P 500, sector weights) para
 * evitar queries repetidas a Firestore.
 * 
 * @module services/riskMetrics/benchmarkCache
 * @see docs/stories/36.story.md
 */

const NodeCache = require('node-cache');
const admin = require('../firebaseAdmin');
const { CACHE_KEYS, CACHE_TTL, DEFAULT_BENCHMARKS } = require('./types');

// Lazy-load para evitar dependencia circular con financeQuery
let _getQuotes = null;
function getQuotesLazy() {
  if (!_getQuotes) {
    _getQuotes = require('../financeQuery').getQuotes;
  }
  return _getQuotes;
}

const db = admin.firestore();

const cache = new NodeCache({
  stdTTL: CACHE_TTL,
  checkperiod: 120,
  useClones: false
});

/**
 * Obtiene datos de retornos del S&P 500 para un período
 * @param {string} startDate - Fecha inicio (YYYY-MM-DD)
 * @param {string} endDate - Fecha fin (YYYY-MM-DD)
 * @returns {Promise<Array<{date: string, dailyReturn: number, indexValue: number}>>}
 */
async function getMarketReturns(startDate, endDate) {
  const cacheKey = `${CACHE_KEYS.SP500_YTD}_${startDate}_${endDate}`;
  
  const cached = cache.get(cacheKey);
  if (cached) {
    console.log(`[benchmarkCache] Cache HIT for market returns: ${cacheKey}`);
    return cached;
  }
  
  console.log(`[benchmarkCache] Cache MISS for market returns: ${cacheKey}`);
  
  try {
    const indexRef = db.collection('indexHistories')
      .doc(DEFAULT_BENCHMARKS.BENCHMARK_INDEX)
      .collection('dates');
    
    const snapshot = await indexRef
      .where('date', '>=', startDate)
      .where('date', '<=', endDate)
      .orderBy('date', 'asc')
      .get();
    
    const marketReturns = snapshot.docs.map(doc => ({
      date: doc.id,
      dailyReturn: (doc.data().percentChange || 0) / 100,
      indexValue: doc.data().close || 0
    }));
    
    cache.set(cacheKey, marketReturns);
    console.log(`[benchmarkCache] Cached ${marketReturns.length} market data points`);
    
    return marketReturns;
  } catch (error) {
    console.error('[benchmarkCache] Error fetching market returns:', error);
    return [];
  }
}

/**
 * Obtiene los pesos sectoriales del S&P 500
 * @returns {Promise<Object>} Mapa de sector -> peso (0-1)
 */
async function getSectorWeights() {
  const cached = cache.get(CACHE_KEYS.SECTOR_WEIGHTS);
  if (cached) {
    console.log('[benchmarkCache] Cache HIT for sector weights');
    return cached;
  }
  
  console.log('[benchmarkCache] Cache MISS for sector weights');
  
  const defaultWeights = {
    'Technology': 0.29,
    'Healthcare': 0.13,
    'Financials': 0.12,
    'Consumer Discretionary': 0.11,
    'Communication Services': 0.09,
    'Industrials': 0.08,
    'Consumer Staples': 0.06,
    'Energy': 0.04,
    'Utilities': 0.03,
    'Real Estate': 0.03,
    'Materials': 0.02
  };
  
  try {
    const sectorDoc = await db.collection('benchmarks')
      .doc('sp500_sectors')
      .get();
    
    if (sectorDoc.exists) {
      const weights = sectorDoc.data().weights || defaultWeights;
      cache.set(CACHE_KEYS.SECTOR_WEIGHTS, weights);
      console.log('[benchmarkCache] Cached sector weights from Firestore');
      return weights;
    }
  } catch (error) {
    console.error('[benchmarkCache] Error fetching sector weights:', error);
  }
  
  cache.set(CACHE_KEYS.SECTOR_WEIGHTS, defaultWeights);
  console.log('[benchmarkCache] Using default sector weights');
  return defaultWeights;
}

/**
 * Obtiene la tasa libre de riesgo actual
 * @returns {Promise<number>} Tasa anualizada (0.055 = 5.5%)
 */
async function getRiskFreeRate() {
  const cacheKey = 'risk_free_rate';
  
  const cached = cache.get(cacheKey);
  if (cached !== undefined) {
    return cached;
  }
  
  try {
    const rateDoc = await db.collection('benchmarks')
      .doc('risk_free_rate')
      .get();
    
    if (rateDoc.exists) {
      const rate = rateDoc.data().rate || DEFAULT_BENCHMARKS.RISK_FREE_RATE;
      cache.set(cacheKey, rate);
      return rate;
    }
  } catch (error) {
    console.error('[benchmarkCache] Error fetching risk-free rate:', error);
  }
  
  return DEFAULT_BENCHMARKS.RISK_FREE_RATE;
}

/**
 * Obtiene la tasa libre de riesgo dinámica con fallback en cascada
 * 
 * 1. Cache en memoria (TTL: 6 horas)
 * 2. Firestore: benchmarks/risk_free_rate (si updatedAt < 48h)
 * 3. finance-query API: /quotes?symbols=^IRX (T-Bill 13 semanas)
 * 4. Constante: DEFAULT_BENCHMARKS.RISK_FREE_RATE (último recurso)
 * 
 * @returns {Promise<{rate: number, source: string}>} Tasa como decimal y fuente
 */
async function getDynamicRiskFreeRate() {
  const cacheKey = 'dynamic_risk_free_rate';
  
  // 1. Cache en memoria (6 horas)
  const cached = cache.get(cacheKey);
  if (cached !== undefined) {
    return cached;
  }
  
  // 2. Firestore (si reciente < 48h)
  try {
    const rateDoc = await db.collection('benchmarks').doc('risk_free_rate').get();
    if (rateDoc.exists) {
      const data = rateDoc.data();
      const updatedAt = data.updatedAt || 0;
      const isRecent = (Date.now() - updatedAt) < 48 * 60 * 60 * 1000;
      if (isRecent && data.rate && data.rate > 0) {
        const result = { rate: data.rate, source: data.source || 'firestore' };
        cache.set(cacheKey, result, 21600); // 6h
        console.log(`[benchmarkCache] RFR from Firestore: ${(data.rate * 100).toFixed(2)}%`);
        return result;
      }
    }
  } catch (error) {
    console.warn('[benchmarkCache] Error reading RFR from Firestore:', error.message);
  }
  
  // 3. API en tiempo real (^IRX = 13-Week Treasury Bill)
  try {
    const getQuotesFn = getQuotesLazy();
    const quotes = await getQuotesFn('^IRX');
    if (quotes && quotes.length > 0) {
      const priceStr = String(quotes[0].price || '').replace(/[%,+]/g, '');
      const ratePercent = parseFloat(priceStr);
      if (!isNaN(ratePercent) && ratePercent > 0 && ratePercent < 20) {
        const rate = ratePercent / 100;
        const result = { rate, source: 'IRX_LIVE' };
        cache.set(cacheKey, result, 21600);
        console.log(`[benchmarkCache] RFR from ^IRX: ${ratePercent.toFixed(2)}%`);
        // Persistir para otras instancias
        db.collection('benchmarks').doc('risk_free_rate').set({
          rate, source: 'IRX_LIVE', symbol: '^IRX',
          updatedAt: Date.now(),
          updatedDate: new Date().toISOString().split('T')[0],
          description: '13-Week Treasury Bill Yield'
        }, { merge: true }).catch(e => console.warn('[benchmarkCache] Error persisting RFR:', e.message));
        return result;
      }
    }
  } catch (error) {
    console.warn('[benchmarkCache] Error fetching ^IRX:', error.message);
  }
  
  // 4. Fallback
  console.warn('[benchmarkCache] Using hardcoded RFR as last resort');
  const result = { rate: DEFAULT_BENCHMARKS.RISK_FREE_RATE, source: 'HARDCODED_FALLBACK' };
  cache.set(cacheKey, result, 3600); // 1h (más corto para reintentar pronto)
  return result;
}

/**
 * Invalida una entrada específica del cache
 * @param {string} key - Clave a invalidar
 */
function invalidateCache(key) {
  cache.del(key);
  console.log(`[benchmarkCache] Invalidated cache key: ${key}`);
}

/**
 * Invalida todo el cache
 */
function clearAllCache() {
  cache.flushAll();
  console.log('[benchmarkCache] All cache cleared');
}

/**
 * Obtiene estadísticas del cache
 * @returns {Object} Estadísticas
 */
function getCacheStats() {
  return {
    keys: cache.keys(),
    stats: cache.getStats()
  };
}

module.exports = {
  getMarketReturns,
  getSectorWeights,
  getRiskFreeRate,
  getDynamicRiskFreeRate,
  invalidateCache,
  clearAllCache,
  getCacheStats
};
