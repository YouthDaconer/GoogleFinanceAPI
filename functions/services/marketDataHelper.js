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
    
    // HU #3: la divisa se pide por el endpoint de tasas del canal de mercado, que
    // ya devuelve todo en base USD. Así la convención se fija en un solo sitio
    // (RN-3-E, D3).
    const foreignCodes = currencyCodes.filter(code => code !== 'USD');
    
    let apiRates = {};
    
    if (foreignCodes.length > 0) {
      try {
        const url = `${API_BASE_URL}/exchange-rates?currencies=${encodeURIComponent(foreignCodes.join(','))}`;
        
        // SEC-TOKEN-004: Incluir headers de autenticación de servicio
        const { data } = await axios.get(url, { 
          timeout: 15000,
          headers: getServiceHeaders(),
        });
        
        if (data && data.rates) {
          for (const [code, rate] of Object.entries(data.rates)) {
            const parsed = parseFloat(rate);
            if (Number.isFinite(parsed) && parsed > 0) {
              apiRates[code] = parsed;
            }
          }
        }
        
        logger.info('Exchange rates received from API', {
          requested: foreignCodes.length,
          received: Object.keys(apiRates).length,
          unavailable: (data && data.unavailable) || [],
          rates: apiRates
        });
        
      } catch (apiError) {
        // HU #3: no hay tasa de reserva. Una tasa ausente se declara ausente
        // (RN-3-D); valorarla con la última conocida es el error que esta
        // historia viene a cerrar.
        logger.warn('El canal de mercado no devolvió tasas de cambio; se declararán no disponibles', {
          error: apiError.message
        });
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
        // HU #3: sin tasa del canal, la divisa queda sin valorar. Nunca se
        // sustituye por la última conocida (RN-3-D)
        exchangeRate: hasApiRate ? freshRate : null,
        // Metadata adicional para debugging
        rateSource: hasApiRate ? 'api-lambda' : 'unavailable',
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
    
    // HU #3: sólo el USD, cuya tasa es 1 por definición. Ninguna otra divisa se
    // devuelve con un valor inventado: lo que no se pudo consultar se declara
    // ausente y quien lo lea mostrará "no disponible" (RN-3-D).
    return [{
      id: 'USD',
      code: 'USD',
      exchangeRate: 1,
      isActive: true,
      name: 'US Dollar',
      symbol: '$',
      flagCurrency: 'https://flagcdn.com/us.svg',
      rateSource: 'base-currency',
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
