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
 * @see docs/architecture/SEC-CF-001-cloudflare-tunnel-migration-plan.md
 * @module services/marketDataHelper
 */

const admin = require('firebase-admin');
const { getQuotes } = require('./financeQuery');
const { StructuredLogger } = require('../utils/logger');
const axios = require('axios');
const { FINANCE_QUERY_API_URL, getServiceHeaders } = require('./config');

const logger = new StructuredLogger('marketDataHelper');

// SEC-CF-001: API URL via Cloudflare Tunnel
const API_BASE_URL = FINANCE_QUERY_API_URL;

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
 * FIX-BATCH-001: El endpoint /v1/quotes tiene un límite de 50 símbolos por request
 * (SEC-AUDIT-002: B-MED-06). Cuando hay más de 50 símbolos únicos, se dividen
 * en batches secuenciales para obtener todos los precios.
 * Sin este batching, los símbolos más allá del #50 se descartaban silenciosamente,
 * causando que sus assets tuvieran precio=0 y rendimiento de -100%.
 * 
 * @param {string[]} symbols - Lista de símbolos a consultar
 * @returns {Promise<Object[]>} Array de objetos con precios
 */
const API_QUOTES_BATCH_SIZE = 50; // Matches SEC-AUDIT-002 limit in quotes.py

async function getPricesFromApi(symbols) {
  if (!symbols || symbols.length === 0) {
    logger.info('No symbols to fetch prices for');
    return [];
  }

  const uniqueSymbols = [...new Set(symbols)];
  
  try {
    // FIX-BATCH-001: Split into batches of API_QUOTES_BATCH_SIZE
    const batches = [];
    for (let i = 0; i < uniqueSymbols.length; i += API_QUOTES_BATCH_SIZE) {
      batches.push(uniqueSymbols.slice(i, i + API_QUOTES_BATCH_SIZE));
    }

    logger.info('Fetching prices from API Lambda', {
      symbolCount: uniqueSymbols.length,
      batches: batches.length,
      source: 'api-lambda'
    });

    const allPrices = [];

    for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
      const batch = batches[batchIdx];
      const symbolsString = batch.join(',');
      
      if (batches.length > 1) {
        logger.info(`Fetching batch ${batchIdx + 1}/${batches.length}`, {
          batchSize: batch.length
        });
      }

      const apiResponse = await getQuotes(symbolsString);
      
      if (!apiResponse) {
        logger.warn(`Empty response from API Lambda (batch ${batchIdx + 1})`);
        continue;
      }

      // Normalizar respuesta (puede ser array u objeto)
      if (Array.isArray(apiResponse)) {
        apiResponse.forEach(quote => {
          if (quote && quote.symbol) {
            allPrices.push(normalizeQuote(quote));
          }
        });
      } else if (typeof apiResponse === 'object') {
        Object.entries(apiResponse).forEach(([symbol, quote]) => {
          if (quote) {
            allPrices.push(normalizeQuote({ ...quote, symbol }));
          }
        });
      }
    }

    logger.info('Prices fetched successfully from API', {
      requested: uniqueSymbols.length,
      received: allPrices.length,
      batches: batches.length,
      source: 'api-lambda'
    });

    return allPrices;

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
  // FIX-PRICE-001: El API puede devolver precios con comas como separador de miles
  // Ejemplo: "2,235.00" para ECOPETROL.CL
  // parseFloat("2,235.00") retorna 2, no 2235
  // Debemos limpiar las comas antes de parsear
  const cleanPrice = (val) => {
    if (typeof val === 'number') return val;
    if (typeof val === 'string') return parseFloat(val.replace(/,/g, '')) || 0;
    return 0;
  };
  
  const priceValue = cleanPrice(quote.price) || 
                     cleanPrice(quote.regularMarketPrice) || 0;
  
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
    change: cleanPrice(quote.change) || cleanPrice(quote.regularMarketChange) || 0,
    percentChange: parseFloat(String(quote.changePercent || quote.regularMarketChangePercent || '0').replace(/[%,]/g, '')) || 0,
    // FIX-DIV-001: Incluir campos de dividendos para processDividendPayments
    dividend: quote.dividend || null,
    dividendDate: quote.dividendDate || null,
    exDividend: quote.exDividend || null,
    yield: quote.yield || null,
    lastDividend: quote.lastDividend || null,
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
        
        // SEC-TOKEN-004: Incluir headers de autenticación de servicio
        const { data } = await axios.get(url, { 
          timeout: 15000,
          headers: getServiceHeaders(),
        });
        
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

/**
 * SCALE-005: Normaliza una tasa de cambio a la convención "1 USD = X unidades".
 * 
 * EUR/GBP/AUD/NZD se cotizan inversamente en Yahoo Finance:
 *   EUR=X → ~1.09 significa "1 EUR = 1.09 USD" → invertir a 0.917 = "1 USD = 0.917 EUR"
 * COP/MXN/BRL/CAD se cotizan directamente:
 *   COP=X → ~4285 ya es "1 USD = 4285 COP"
 */
function normalizeToUsdBase(currencyCode, rawRate) {
  if (currencyCode === 'USD') return 1;
  if (!rawRate || rawRate <= 0) return rawRate;

  const invertCurrencies = ['EUR', 'GBP', 'AUD', 'NZD'];
  if (invertCurrencies.includes(currencyCode) && rawRate > 1) {
    return 1 / rawRate;
  }

  return rawRate;
}

module.exports = {
  getPricesFromApi,
  getCurrencyRatesFromApi,
  normalizeQuote,
  invalidateCurrencyRatesCache,
  normalizeToUsdBase,
};
