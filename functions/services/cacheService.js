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

// ============================================================================
// OPT-DEMAND-CLEANUP (2026-01-17): Funciones de cache de precios DEPRECADAS
// ============================================================================
// getCachedPrices() y getCachedCurrencyRates() retornan arrays vacíos porque:
// - El frontend usa polling al API Lambda para datos frescos
// - Cachear precios obsoletos de Firestore es peor que mostrar error + retry
// - Las tasas de cambio vienen del API Lambda junto con los precios
// 
// NOTA: Estas funciones DEBEN existir porque financeQuery.js las importa
// para usarlas como fallback del circuit breaker. Si no existen, el circuit
// breaker falla con TypeError cuando el API no responde.
// ============================================================================

/**
 * @deprecated OPT-DEMAND-CLEANUP - Retorna array vacío.
 * Mantenida para compatibilidad con circuit breaker en financeQuery.js
 * 
 * @param {string[]} symbols - Lista de símbolos (ignorada)
 * @returns {Promise<Array>} Array vacío siempre
 */
async function getCachedPrices(symbols) {
  logger.warn('getCachedPrices called (deprecated, returning empty array)', {
    symbolCount: symbols?.length || 0,
  });
  return [];
}

/**
 * @deprecated OPT-DEMAND-CLEANUP - Retorna objeto vacío.
 * Mantenida para compatibilidad con imports en otros módulos.
 * 
 * @returns {Promise<Object>} Objeto vacío siempre
 */
async function getCachedCurrencyRates() {
  logger.warn('getCachedCurrencyRates called (deprecated, returning empty object)');
  return {};
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
  // Funciones deprecadas (OPT-DEMAND-CLEANUP) - retornan vacío pero existen para compatibilidad
  getCachedPrices,
  getCachedCurrencyRates,
  // Funciones activas
  getCachedMarketStatus,
  cacheMarketStatus,
  getCachedEtfData,
  cacheEtfData,
  clearEtfCache,
  getEtfCacheStats,
};
