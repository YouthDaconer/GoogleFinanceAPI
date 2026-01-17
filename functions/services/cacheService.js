/**
 * Cache Service for Circuit Breaker Fallbacks
 * 
 * OPT-DEMAND-CLEANUP: Refactorizado para arquitectura On-Demand pura.
 * 
 * FUNCIONES DEPRECADAS (2026-01-17):
 * - getCachedPrices(): Ya no lee de Firestore. El frontend tiene polling
 *   que obtiene datos frescos del API Lambda. Cachear precios obsoletos
 *   es peor que mostrar un error y reintentar.
 * - getCachedCurrencyRates(): Las tasas de cambio vienen con los precios
 *   del API Lambda. No se requiere fallback separado.
 * 
 * FUNCIONES ACTIVAS:
 * - getCachedMarketStatus(): Estado del mercado (open/closed) es predecible
 *   y cambia poco, útil como fallback.
 * - getCachedEtfData(): Holdings de ETFs no cambian frecuentemente,
 *   cache de 24h es válido.
 * 
 * @see docs/architecture/OPT-DEMAND-CLEANUP-firestore-fallback-removal.md
 * @see SCALE-BE-003 - Circuit Breaker para APIs Externas
 */

const admin = require('./firebaseAdmin');
const { StructuredLogger } = require('../utils/logger');

const db = admin.firestore();
const logger = new StructuredLogger('CacheService');

const etfMemoryCache = new Map();
const ETF_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours

/**
 * @deprecated OPT-DEMAND-CLEANUP (2026-01-17): Esta función está DEPRECADA.
 * 
 * RAZÓN: El frontend tiene polling implementado que obtiene datos frescos
 * del API Lambda. Cachear precios de Firestore que pueden tener días de
 * antigüedad es peor que mostrar un error con opción de retry.
 * 
 * Los precios de acciones son muy volátiles (pueden moverse 5-20% en un día).
 * Mostrar datos obsoletos puede llevar a decisiones financieras incorrectas.
 * 
 * COMPORTAMIENTO: Retorna array vacío siempre. Los llamadores deben
 * manejar esto como "sin fallback disponible".
 * 
 * @param {string[]} symbols - Símbolos a buscar (ignorado)
 * @returns {Promise<Array>} Array vacío siempre
 */
async function getCachedPrices(symbols) {
  logger.warn('getCachedPrices() is DEPRECATED - returning empty array', {
    symbolCount: symbols?.length || 0,
    reason: 'OPT-DEMAND-CLEANUP: Frontend polling handles fresh data from API Lambda'
  });
  
  // Retornar vacío - el Circuit Breaker debe manejar esto como "sin fallback"
  return [];
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

/**
 * @deprecated OPT-DEMAND-CLEANUP (2026-01-17): Esta función está DEPRECADA.
 * 
 * RAZÓN: Las tasas de cambio ahora vienen incluidas en la respuesta del
 * API Lambda (/market-quotes retorna currencyRates). El frontend tiene
 * polling que obtiene datos frescos regularmente.
 * 
 * Cachear tasas de cambio obsoletas de Firestore es innecesario y
 * potencialmente peligroso para cálculos financieros.
 * 
 * COMPORTAMIENTO: Retorna objeto vacío siempre. Los llamadores deben
 * manejar esto como "sin fallback disponible".
 * 
 * @param {string[]} currencyCodes - Códigos de moneda (ignorado)
 * @returns {Promise<Object>} Objeto vacío siempre
 */
async function getCachedCurrencyRates(currencyCodes) {
  logger.warn('getCachedCurrencyRates() is DEPRECATED - returning empty object', {
    codesCount: currencyCodes?.length || 0,
    reason: 'OPT-DEMAND-CLEANUP: Currency rates come from API Lambda with prices'
  });
  
  // Retornar vacío - el Circuit Breaker debe manejar esto como "sin fallback"
  return {};
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
