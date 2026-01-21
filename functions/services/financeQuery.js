/**
 * Finance Query Service with Circuit Breaker
 * 
 * Client for the finance-query API via Cloudflare Tunnel.
 * Now protected by circuit breaker pattern for resilience.
 * 
 * SEC-CF-001: Migrated from Lambda URL to Cloudflare Tunnel
 * SEC-TOKEN-004: Includes service token for authentication
 * @see SCALE-BE-003 - Circuit Breaker para APIs Externas
 * @see docs/architecture/SEC-CF-001-cloudflare-tunnel-migration-plan.md
 * @see docs/architecture/SEC-TOKEN-001-api-security-hardening-plan.md
 */

const { getCircuit } = require('../utils/circuitBreaker');
const { getCachedPrices, getCachedCurrencyRates } = require('./cacheService');
const { StructuredLogger } = require('../utils/logger');
const { FINANCE_QUERY_API_URL, getServiceHeaders } = require('./config');

// SEC-CF-001: URL centralizada via Cloudflare Tunnel
const API_BASE_URL = FINANCE_QUERY_API_URL;
const logger = new StructuredLogger('financeQuery');

// Circuit breakers per endpoint type
const quotesCircuit = getCircuit('finance-query-quotes', {
  failureThreshold: 5,
  resetTimeout: 60000,
});

const generalCircuit = getCircuit('finance-query-general', {
  failureThreshold: 5,
  resetTimeout: 60000,
});

let data = null;
let loading = false;
let error = null;

const fetchData = async (endpoint, maxRetries = 3, delay = 1000) => {
  let attempts = 0;
  loading = true;
  error = null;

  while (attempts < maxRetries) {
    try {
      // SEC-TOKEN-004: Incluir headers de autenticación de servicio
      const headers = getServiceHeaders();
      const url = `${API_BASE_URL}${endpoint}`;
      
      // DEBUG: Log request details (solo primeros 3 intentos de la sesión)
      if (attempts === 0) {
        console.log(`[FinanceQuery] Calling: ${url.substring(0, 100)}...`);
        console.log(`[FinanceQuery] Headers: x-service-token=${headers['x-service-token'] ? 'SET' : 'NOT SET'}`);
      }
      
      const response = await fetch(url, { headers });
      
      if (!response.ok) {
        const errorText = await response.text().catch(() => 'Unable to read response body');
        console.error(`[FinanceQuery] HTTP ${response.status}: ${errorText.substring(0, 200)}`);
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      data = await response.json();
      return data;
    } catch (err) {
      attempts++;
      error = err.message || 'Error desconocido';
      console.warn(`Intento ${attempts} fallido: ${error}`);

      if (attempts < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        console.error('Todos los intentos fallaron');
        throw new Error(`API call failed after ${maxRetries} attempts: ${error}`);
      }
    } finally {
      loading = false;
    }
  }
};

// === QUOTES WITH CIRCUIT BREAKER (AC2, AC4) ===

const getQuotesWithFallback = async (symbols) => {
  const symbolsArray = typeof symbols === 'string' ? symbols.split(',') : symbols;
  
  return quotesCircuit.execute(
    async () => {
      const result = await fetchData(`/quotes?symbols=${symbols}`);
      if (!result) {
        throw new Error('No data returned from quotes API');
      }
      return result;
    },
    async () => {
      logger.warn('Using cached prices due to circuit breaker', {
        symbolCount: symbolsArray.length,
      });
      return getCachedPrices(symbolsArray);
    }
  );
};

// === SIMPLE QUOTES WITH CIRCUIT BREAKER ===

const getSimpleQuotesWithFallback = async (symbols) => {
  const symbolsArray = typeof symbols === 'string' ? symbols.split(',') : symbols;
  
  return quotesCircuit.execute(
    async () => {
      const result = await fetchData(`/simple-quotes/?symbols=${symbols}`);
      if (!result) {
        throw new Error('No data returned from simple-quotes API');
      }
      return result;
    },
    async () => {
      logger.warn('Using cached prices for simple quotes', {
        symbolCount: symbolsArray.length,
      });
      return getCachedPrices(symbolsArray);
    }
  );
};

// === GENERAL ENDPOINTS (less critical, graceful degradation) ===

const getWithGracefulDegradation = async (endpoint, fallbackValue = []) => {
  return generalCircuit.execute(
    async () => {
      const result = await fetchData(endpoint);
      if (!result) {
        throw new Error(`No data returned from ${endpoint}`);
      }
      return result;
    },
    async () => {
      logger.warn('Returning fallback due to circuit breaker', {
        endpoint,
        fallbackType: Array.isArray(fallbackValue) ? 'empty array' : typeof fallbackValue,
      });
      return fallbackValue;
    }
  );
};

// === EXPORTED FUNCTIONS ===

const getIndices = () => getWithGracefulDegradation('/indices', []);
const getActives = () => getWithGracefulDegradation('/actives', []);
const getGainers = () => getWithGracefulDegradation('/gainers', []);
const getLosers = () => getWithGracefulDegradation('/losers', []);
const getNews = () => getWithGracefulDegradation('/news', []);
const getNewsFromSymbol = (symbol) => getWithGracefulDegradation(`/news?symbol=${symbol}`, []);
const getSectors = () => getWithGracefulDegradation('/sectors', []);
const search = (query) => getWithGracefulDegradation(`/search?query=${query}`, []);

// Quotes use dedicated circuit with cache fallback
const getQuotes = (symbols) => getQuotesWithFallback(symbols);
const getSimpleQuotes = (symbols) => getSimpleQuotesWithFallback(symbols);

// Market quotes for currencies (COP=X, EUR=X, etc.) and indices
// INTRADAY-001: Endpoint específico para tasas de cambio en tiempo real
const getMarketQuotes = (symbols) => getWithGracefulDegradation(`/market-quotes?symbols=${symbols}`, {});

// These don't have fallback - they throw on circuit open
const getSimilarStocks = (symbol) => fetchData(`/similar-stocks/?symbol=${symbol}`);
const getHistorical = (symbol, time, interval) => fetchData(`/historical/?symbol=${symbol}&time=${time}&interval=${interval}`);
const getIndicators = (func, symbol) => fetchData(`/indicators/?function=${func}&symbol=${symbol}`);
const getAnalysis = (symbol, time, interval) => fetchData(`/analysis/?symbol=${symbol}&time=${time}&interval=${interval}`);

module.exports = {
  getIndices,
  getActives,
  getGainers,
  getLosers,
  getNews,
  getQuotes,
  getSimpleQuotes,
  getMarketQuotes,
  getSimilarStocks,
  getSectors,
  search,
  getHistorical,
  getIndicators,
  getAnalysis,
  getData: () => data,
  isLoading: () => loading,
  getError: () => error,
  getNewsFromSymbol,
  // Export for testing
  _getCircuitStates: () => ({
    quotes: quotesCircuit.getState(),
    general: generalCircuit.getState(),
  }),
};