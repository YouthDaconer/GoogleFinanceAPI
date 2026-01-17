/**
 * Market Data Helper Service
 * 
 * OPT-DEMAND-CLEANUP: Servicio centralizado para obtener precios y tasas de cambio.
 * 
 * IMPORTANTE - Arquitectura de Currencies:
 * - La colección `currencies` de Firestore es la FUENTE DE VERDAD para:
 *   - Cuáles currencies están activas (isActive: true)
 *   - Metadata: nombre, símbolo, bandera, etc.
 * - Las TASAS DE CAMBIO se obtienen del API Lambda (datos frescos)
 * 
 * Este servicio es usado por:
 * - calculateDailyPortfolioPerformance.js
 * - unifiedMarketDataUpdate.js
 * - calculatePortfolioRisk.js
 * - processDividendPayments.js
 * 
 * @see docs/architecture/OPT-DEMAND-CLEANUP-firestore-fallback-removal.md
 * @module services/marketDataHelper
 */

const admin = require('firebase-admin');
const { getQuotes } = require('./financeQuery');
const { StructuredLogger } = require('../utils/logger');
const axios = require('axios');

const logger = new StructuredLogger('marketDataHelper');

// API Lambda URL para tasas de cambio
const API_BASE_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

// Cache en memoria para tasas de cambio (evita llamadas repetidas en la misma ejecución)
let currencyRatesCache = null;
let currencyRatesCacheTimestamp = 0;
const CURRENCY_RATES_CACHE_TTL = 5 * 60 * 1000; // 5 minutos

// NOTA: SUPPORTED_CURRENCY_CODES fue ELIMINADA
// Las currencies activas ahora se leen de Firestore (isActive: true)
// Esto permite al usuario configurar sus propias currencies sin cambios de código

/**
 * Obtiene precios actuales desde el API Lambda
 * 
 * @param {string[]} symbols - Lista de símbolos a consultar
 * @returns {Promise<Object[]>} Array de objetos con precios
 */
async function getPricesFromApi(symbols) {
  if (!symbols || symbols.length === 0) {
    logger.info('No symbols to fetch prices for');
    return [];
  }

  const uniqueSymbols = [...new Set(symbols)];
  
  try {
    logger.info('Fetching prices from API Lambda', {
      symbolCount: uniqueSymbols.length,
      source: 'api-lambda'
    });

    const symbolsString = uniqueSymbols.join(',');
    const apiResponse = await getQuotes(symbolsString);
    
    if (!apiResponse) {
      logger.warn('Empty response from API Lambda');
      return [];
    }

    // Normalizar respuesta (puede ser array u objeto)
    const prices = [];
    
    if (Array.isArray(apiResponse)) {
      apiResponse.forEach(quote => {
        if (quote && quote.symbol) {
          prices.push(normalizeQuote(quote));
        }
      });
    } else if (typeof apiResponse === 'object') {
      Object.entries(apiResponse).forEach(([symbol, quote]) => {
        if (quote) {
          prices.push(normalizeQuote({ ...quote, symbol }));
        }
      });
    }

    logger.info('Prices fetched successfully from API', {
      requested: uniqueSymbols.length,
      received: prices.length,
      source: 'api-lambda'
    });

    return prices;

  } catch (error) {
    logger.error('Error fetching prices from API Lambda', {
      error: error.message,
      symbolCount: uniqueSymbols.length
    });
    throw new Error(`Failed to fetch prices: ${error.message}`);
  }
}

/**
 * Normaliza un quote del API al formato esperado por los servicios
 * 
 * @param {Object} quote - Quote del API
 * @returns {Object} Quote normalizado
 */
function normalizeQuote(quote) {
  const priceValue = parseFloat(quote.price) || 
                     parseFloat(quote.regularMarketPrice) || 0;
  
  return {
    symbol: quote.symbol,
    price: priceValue,
    regularMarketPrice: priceValue,
    name: quote.name || quote.shortName || quote.symbol,
    sector: quote.sector || null,
    industry: quote.industry || null,
    type: quote.type || quote.quoteType || 'stock',
    logo: quote.logo || null,
    currency: quote.currency || 'USD',
    country: quote.country || null,
    exchange: quote.exchange || null,
    change: parseFloat(quote.change) || parseFloat(quote.regularMarketChange) || 0,
    percentChange: parseFloat(String(quote.changePercent || quote.regularMarketChangePercent || '0').replace('%', '')) || 0,
  };
}

/**
 * Obtiene currencies activas con tasas de cambio frescas del API Lambda
 * 
 * FLUJO:
 * 1. Lee currencies con isActive: true de Firestore (configuración del usuario)
 * 2. Obtiene tasas de cambio del API Lambda para esas currencies
 * 3. Combina metadata de Firestore + tasas del API Lambda
 * 
 * La colección `currencies` de Firestore es la FUENTE DE VERDAD para:
 * - Cuáles currencies usar (isActive)
 * - Metadata: id, code, name, symbol, flagCurrency
 * 
 * El API Lambda es la fuente para:
 * - Tasas de cambio actualizadas (exchangeRate)
 * 
 * @returns {Promise<Object[]>} Array de objetos Currency con metadata + exchangeRate fresco
 */
async function getCurrencyRatesFromApi() {
  // Verificar cache
  if (currencyRatesCache && Date.now() - currencyRatesCacheTimestamp < CURRENCY_RATES_CACHE_TTL) {
    logger.info('Using cached currency rates', {
      cacheAge: Date.now() - currencyRatesCacheTimestamp,
      count: currencyRatesCache.length
    });
    return currencyRatesCache;
  }

  try {
    const db = admin.firestore();
    
    // =========================================================================
    // PASO 1: Leer currencies activas de Firestore (fuente de verdad para config)
    // =========================================================================
    logger.info('Fetching active currencies from Firestore');
    
    const currenciesSnapshot = await db.collection('currencies')
      .where('isActive', '==', true)
      .get();
    
    if (currenciesSnapshot.empty) {
      logger.warn('No active currencies found in Firestore, using USD default');
      const defaultCurrency = [{
        id: 'USD',
        code: 'USD',
        exchangeRate: 1,
        isActive: true,
        name: 'US Dollar',
        symbol: '$',
        flagCurrency: 'https://flagcdn.com/us.svg',
      }];
      currencyRatesCache = defaultCurrency;
      currencyRatesCacheTimestamp = Date.now();
      return defaultCurrency;
    }
    
    // Extraer datos de Firestore
    const activeCurrencies = currenciesSnapshot.docs.map(doc => ({
      id: doc.id,
      ...doc.data()
    }));
    
    const currencyCodes = activeCurrencies.map(c => c.code).filter(Boolean);
    
    logger.info('Active currencies from Firestore', {
      count: activeCurrencies.length,
      codes: currencyCodes
    });
    
    // =========================================================================
    // PASO 2: Obtener tasas de cambio del API Lambda
    // =========================================================================
    logger.info('Fetching exchange rates from API Lambda', {
      currencies: currencyCodes
    });
    
    // Construir símbolos de currency para el API (formato: COP=X, EUR=X, etc.)
    // Excluir USD ya que siempre es 1
    const currencySymbols = currencyCodes
      .filter(code => code !== 'USD')
      .map(code => `${code}=X`);
    
    let apiRates = {};
    
    if (currencySymbols.length > 0) {
      try {
        const symbolsParam = currencySymbols.join(',');
        const url = `${API_BASE_URL}/market-quotes?symbols=${symbolsParam}`;
        
        const { data } = await axios.get(url, { timeout: 15000 });
        
        // Extraer tasas de la respuesta
        if (Array.isArray(data)) {
          data.forEach(item => {
            if (item.symbol && item.regularMarketPrice) {
              // Convertir COP=X a COP
              const currencyCode = item.symbol.replace('=X', '');
              apiRates[currencyCode] = parseFloat(item.regularMarketPrice) || 1;
            }
          });
        }
        
        logger.info('Exchange rates received from API', {
          requested: currencySymbols.length,
          received: Object.keys(apiRates).length,
          rates: apiRates
        });
        
      } catch (apiError) {
        logger.warn('Failed to fetch rates from API Lambda, using Firestore rates as fallback', {
          error: apiError.message
        });
        // Continuar con las tasas de Firestore
      }
    }
    
    // USD siempre es 1
    apiRates['USD'] = 1;
    
    // =========================================================================
    // PASO 3: Combinar metadata de Firestore + tasas del API Lambda
    // =========================================================================
    const currencies = activeCurrencies.map(currency => {
      const freshRate = apiRates[currency.code];
      const hasApiRate = freshRate !== undefined && freshRate !== null;
      
      return {
        id: currency.id,
        code: currency.code,
        name: currency.name,
        symbol: currency.symbol,
        flagCurrency: currency.flagCurrency,
        isActive: true,
        // Usar tasa del API si está disponible, sino usar la de Firestore
        exchangeRate: hasApiRate ? freshRate : (currency.exchangeRate || 1),
        // Metadata adicional para debugging
        rateSource: hasApiRate ? 'api-lambda' : 'firestore-fallback',
        lastUpdated: new Date().toISOString(),
      };
    });

    // Guardar en cache
    currencyRatesCache = currencies;
    currencyRatesCacheTimestamp = Date.now();

    logger.info('Currency rates ready', {
      count: currencies.length,
      codes: currencies.map(c => c.code),
      sources: currencies.reduce((acc, c) => {
        acc[c.rateSource] = (acc[c.rateSource] || 0) + 1;
        return acc;
      }, {})
    });

    return currencies;

  } catch (error) {
    logger.error('Error fetching currency rates', {
      error: error.message,
      stack: error.stack
    });
    
    // Retornar default mínimo para no bloquear cálculos
    return [{
      id: 'USD',
      code: 'USD',
      exchangeRate: 1,
      isActive: true,
      name: 'US Dollar',
      symbol: '$',
      flagCurrency: 'https://flagcdn.com/us.svg',
      rateSource: 'default-fallback',
    }];
  }
}

// ============================================================================
// Funciones getCurrencyName y getCurrencySymbol ELIMINADAS
// La metadata de currencies ahora viene de Firestore (nombre, símbolo, bandera)
// ============================================================================

/**
 * Invalida el cache de tasas de cambio
 */
function invalidateCurrencyRatesCache() {
  currencyRatesCache = null;
  currencyRatesCacheTimestamp = 0;
  logger.info('Currency rates cache invalidated');
}

module.exports = {
  getPricesFromApi,
  getCurrencyRatesFromApi,
  normalizeQuote,
  invalidateCurrencyRatesCache,
};
