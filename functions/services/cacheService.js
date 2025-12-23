/**
 * Cache Service for Circuit Breaker Fallbacks
 * 
 * Provides fallback data from Firestore when external APIs are unavailable.
 * Used by circuit breakers to return cached data instead of failing.
 * 
 * Collections used:
 * - currentPrices: Stock/ETF prices (updated every 2 min)
 * - markets: Market status (updated every 30 min)
 * - currencies: Exchange rates (updated every 2 min)
 * 
 * @see SCALE-BE-003 - Circuit Breaker para APIs Externas
 */

const admin = require('./firebaseAdmin');
const { StructuredLogger } = require('../utils/logger');

const db = admin.firestore();
const logger = new StructuredLogger('CacheService');

const etfMemoryCache = new Map();
const ETF_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours

async function getCachedPrices(symbols) {
  if (!symbols || symbols.length === 0) {
    return [];
  }

  logger.info('Fetching cached prices from Firestore', { 
    symbolCount: symbols.length 
  });

  const pricesRef = db.collection('currentPrices');
  const prices = [];
  const batchSize = 10;

  for (let i = 0; i < symbols.length; i += batchSize) {
    const batch = symbols.slice(i, i + batchSize);
    const promises = batch.map(symbol => pricesRef.doc(symbol).get());
    
    const docs = await Promise.all(promises);
    
    docs.forEach((doc, idx) => {
      if (doc.exists) {
        const data = doc.data();
        prices.push({
          symbol: batch[idx],
          price: data.price,
          name: data.name,
          change: data.change,
          percentChange: data.percentChange,
          currency: data.currency,
          lastUpdated: data.lastUpdated,
          fromCache: true,
          cacheAge: data.lastUpdated ? Date.now() - data.lastUpdated : null,
        });
      }
    });
  }

  logger.info('Returned cached prices', { 
    requested: symbols.length, 
    found: prices.length 
  });

  return prices;
}

async function getCachedMarketStatus() {
  const MARKET_DOC_ID = 'US';
  
  try {
    const doc = await db.collection('markets').doc(MARKET_DOC_ID).get();
    
    if (doc.exists) {
      const data = doc.data();
      logger.info('Returning cached market status', {
        isOpen: data.isOpen,
        session: data.session,
      });
      
      return {
        ...data,
        fromCache: true,
        cacheAge: data.lastUpdated?.toDate 
          ? Date.now() - data.lastUpdated.toDate().getTime() 
          : null,
      };
    }
  } catch (error) {
    logger.warn('Error fetching cached market status', { error: error.message });
  }

  return {
    exchange: 'US',
    isOpen: false,
    session: 'unknown',
    fromCache: true,
    fallbackDefault: true,
  };
}

async function cacheMarketStatus(marketData) {
  const MARKET_DOC_ID = 'US';
  
  try {
    await db.collection('markets').doc(MARKET_DOC_ID).set({
      ...marketData,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    
    logger.debug('Cached market status', { 
      isOpen: marketData.isOpen 
    });
  } catch (error) {
    logger.warn('Error caching market status', { error: error.message });
  }
}

async function getCachedCurrencyRates(currencyCodes) {
  if (!currencyCodes || currencyCodes.length === 0) {
    return {};
  }

  const rates = {};
  const currenciesRef = db.collection('currencies');

  try {
    const snapshot = await currenciesRef
      .where('isActive', '==', true)
      .get();

    snapshot.docs.forEach(doc => {
      const data = doc.data();
      if (currencyCodes.includes(data.code)) {
        rates[data.code] = {
          rate: data.exchangeRate,
          lastUpdated: data.lastUpdated,
          fromCache: true,
        };
      }
    });

    logger.info('Returned cached currency rates', {
      requested: currencyCodes.length,
      found: Object.keys(rates).length,
    });
  } catch (error) {
    logger.warn('Error fetching cached currency rates', { 
      error: error.message 
    });
  }

  return rates;
}

function getCachedEtfData(ticker) {
  const cached = etfMemoryCache.get(ticker);
  
  if (cached && Date.now() - cached.timestamp < ETF_CACHE_TTL) {
    logger.debug('ETF cache hit (memory)', { ticker });
    return cached.data;
  }

  logger.debug('ETF cache miss', { ticker });
  return null;
}

function cacheEtfData(ticker, data) {
  etfMemoryCache.set(ticker, { 
    data, 
    timestamp: Date.now() 
  });
  
  logger.debug('Cached ETF data in memory', { ticker });
}

function clearEtfCache() {
  etfMemoryCache.clear();
  logger.info('ETF memory cache cleared');
}

function getEtfCacheStats() {
  return {
    size: etfMemoryCache.size,
    tickers: Array.from(etfMemoryCache.keys()),
  };
}

module.exports = {
  getCachedPrices,
  getCachedMarketStatus,
  cacheMarketStatus,
  getCachedCurrencyRates,
  getCachedEtfData,
  cacheEtfData,
  clearEtfCache,
  getEtfCacheStats,
};
