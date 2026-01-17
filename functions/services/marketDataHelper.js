/**
 * Market Data Helper Service
 * 
 * OPT-DEMAND-CLEANUP: Servicio centralizado para obtener precios y tasas de cambio
 * desde el API Lambda, eliminando la dependencia de Firestore.
 * 
 * Este servicio es usado por:
 * - calculateDailyPortfolioPerformance.js
 * - scheduledPortfolioCalculations.js
 * - calculatePortfolioRisk.js
 * - processDividendPayments.js
 * 
 * @see docs/architecture/OPT-DEMAND-CLEANUP-firestore-fallback-removal.md
 * @module services/marketDataHelper
 */

const { getQuotes } = require('./financeQuery');
const { StructuredLogger } = require('../utils/logger');

const logger = new StructuredLogger('marketDataHelper');

// Cache en memoria para tasas de cambio (evita llamadas repetidas en la misma ejecución)
let currencyRatesCache = null;
let currencyRatesCacheTimestamp = 0;
const CURRENCY_RATES_CACHE_TTL = 5 * 60 * 1000; // 5 minutos

/**
 * Códigos de moneda que el API Lambda soporta para tasas de cambio
 * Estas son las monedas más comunes usadas en el portafolio
 */
const SUPPORTED_CURRENCY_CODES = [
  'USD', 'EUR', 'GBP', 'COP', 'MXN', 'BRL', 'ARS', 'CLP', 'PEN',
  'JPY', 'CHF', 'CAD', 'AUD', 'CNY', 'INR', 'KRW'
];

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
 * Obtiene tasas de cambio desde el API Lambda
 * 
 * El API /quotes retorna currencyRates como parte de la respuesta.
 * Esta función hace una llamada mínima para obtener solo las tasas.
 * 
 * @returns {Promise<Object[]>} Array de objetos Currency con code y exchangeRate
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
    logger.info('Fetching currency rates from API Lambda');

    // El API /quotes retorna currencyRates cuando se consultan símbolos
    // Usamos un símbolo común como "SPY" para obtener las tasas
    const symbolsString = 'SPY';
    const apiResponse = await getQuotes(symbolsString);
    
    // Extraer currencyRates de la respuesta
    let currencyRates = {};
    
    if (apiResponse && apiResponse.currencyRates) {
      currencyRates = apiResponse.currencyRates;
    } else if (apiResponse && Array.isArray(apiResponse) && apiResponse[0]?.currencyRates) {
      currencyRates = apiResponse[0].currencyRates;
    }

    // Convertir a formato Currency[]
    const currencies = Object.entries(currencyRates).map(([code, rate]) => ({
      id: code,
      code: code,
      exchangeRate: parseFloat(rate) || 1,
      isActive: true,
      // Metadata estática (se puede extender)
      name: getCurrencyName(code),
      symbol: getCurrencySymbol(code),
    }));

    // Si el API no retorna tasas, agregar al menos USD
    if (currencies.length === 0) {
      logger.warn('No currency rates from API, using defaults');
      currencies.push({
        id: 'USD',
        code: 'USD',
        exchangeRate: 1,
        isActive: true,
        name: 'US Dollar',
        symbol: '$',
      });
    }

    // Guardar en cache
    currencyRatesCache = currencies;
    currencyRatesCacheTimestamp = Date.now();

    logger.info('Currency rates fetched successfully', {
      count: currencies.length,
      codes: currencies.map(c => c.code),
      source: 'api-lambda'
    });

    return currencies;

  } catch (error) {
    logger.error('Error fetching currency rates from API Lambda', {
      error: error.message
    });
    
    // Retornar default mínimo para no bloquear cálculos
    return [{
      id: 'USD',
      code: 'USD',
      exchangeRate: 1,
      isActive: true,
      name: 'US Dollar',
      symbol: '$',
    }];
  }
}

/**
 * Obtiene el nombre de una moneda por su código
 * 
 * @param {string} code - Código ISO de la moneda
 * @returns {string} Nombre de la moneda
 */
function getCurrencyName(code) {
  const names = {
    USD: 'US Dollar',
    EUR: 'Euro',
    GBP: 'British Pound',
    COP: 'Colombian Peso',
    MXN: 'Mexican Peso',
    BRL: 'Brazilian Real',
    ARS: 'Argentine Peso',
    CLP: 'Chilean Peso',
    PEN: 'Peruvian Sol',
    JPY: 'Japanese Yen',
    CHF: 'Swiss Franc',
    CAD: 'Canadian Dollar',
    AUD: 'Australian Dollar',
    CNY: 'Chinese Yuan',
    INR: 'Indian Rupee',
    KRW: 'South Korean Won',
  };
  return names[code] || code;
}

/**
 * Obtiene el símbolo de una moneda por su código
 * 
 * @param {string} code - Código ISO de la moneda
 * @returns {string} Símbolo de la moneda
 */
function getCurrencySymbol(code) {
  const symbols = {
    USD: '$',
    EUR: '€',
    GBP: '£',
    COP: '$',
    MXN: '$',
    BRL: 'R$',
    ARS: '$',
    CLP: '$',
    PEN: 'S/',
    JPY: '¥',
    CHF: 'Fr',
    CAD: 'C$',
    AUD: 'A$',
    CNY: '¥',
    INR: '₹',
    KRW: '₩',
  };
  return symbols[code] || code;
}

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
  SUPPORTED_CURRENCY_CODES,
};
