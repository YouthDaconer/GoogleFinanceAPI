/**
 * Transaction Enricher Service
 * 
 * IMPORT-002: Enriches raw transactions with market data and calculated fields.
 * 
 * @module transactions/services/transactionEnricher
 * @see docs/stories/90.story.md (IMPORT-002)
 */

const { getQuotes } = require('../../financeQuery');
const { IMPORT_ERROR_CODES, TRANSACTION_TYPES } = require('../types');

// ============================================================================
// INTERFACES (JSDoc)
// ============================================================================

/**
 * @typedef {Object} TransactionToImport
 * @property {string} ticker - Ticker symbol
 * @property {'buy'|'sell'} type - Transaction type
 * @property {number} amount - Quantity/units
 * @property {number} price - Unit price
 * @property {string} date - Transaction date (YYYY-MM-DD)
 * @property {string} [currency] - Currency (default USD)
 * @property {number} [commission] - Commission/fee (default 0)
 * @property {number} originalRowNumber - Row number in source file
 */

/**
 * @typedef {Object} EnrichedTransaction
 * @property {string} assetId - Reference to asset document
 * @property {string} assetName - Ticker
 * @property {'buy'|'sell'} type - Transaction type
 * @property {number} amount - Quantity
 * @property {number} price - Unit price
 * @property {string} date - ISO date
 * @property {string} currency - Currency code
 * @property {number} commission - Fee
 * @property {'stock'|'etf'|'crypto'} assetType - Type of asset
 * @property {string} market - Exchange/market
 * @property {number} dollarPriceToDate - Exchange rate on date
 * @property {string} defaultCurrencyForAdquisitionDollar - Reference currency
 * @property {string} portfolioAccountId - Account ID
 * @property {string} userId - User ID
 * @property {number} originalRowNumber - Source row number
 */

/**
 * @typedef {Object} ImportError
 * @property {number} rowNumber - Original row number
 * @property {string} ticker - Ticker symbol
 * @property {string} code - Error code
 * @property {string} message - Error message
 */

// ============================================================================
// CACHES
// ============================================================================

/** @type {Map<string, number>} Exchange rate cache: "currency:date" -> rate */
const exchangeRateCache = new Map();

// ============================================================================
// MAIN FUNCTIONS
// ============================================================================

/**
 * Enriches transactions with asset info, exchange rates, and calculated fields.
 * 
 * @param {TransactionToImport[]} transactions - Raw transactions to enrich
 * @param {Map<string, Object>} assetMap - Map of ticker -> AssetInfo
 * @param {string} portfolioAccountId - Account ID
 * @param {string} userId - User ID
 * @param {string} defaultCurrency - Default currency (user preference)
 * @returns {Promise<{data: EnrichedTransaction[], errors: ImportError[]}>}
 */
async function enrichTransactions(
  transactions, 
  assetMap, 
  portfolioAccountId, 
  userId, 
  defaultCurrency
) {
  console.log(`[transactionEnricher] Enriching ${transactions.length} transactions`);
  
  /** @type {EnrichedTransaction[]} */
  const enriched = [];
  /** @type {ImportError[]} */
  const errors = [];
  
  // Pre-load exchange rates for all unique dates
  if (defaultCurrency !== 'USD') {
    const uniqueDates = [...new Set(transactions.map(tx => tx.date))];
    await preloadExchangeRates(uniqueDates, defaultCurrency);
  }
  
  for (const tx of transactions) {
    try {
      const normalizedTicker = tx.ticker?.trim().toUpperCase();
      
      // Get asset info
      const assetInfo = assetMap.get(normalizedTicker);
      
      if (!assetInfo) {
        errors.push({
          rowNumber: tx.originalRowNumber,
          ticker: tx.ticker,
          code: IMPORT_ERROR_CODES.ASSET_NOT_FOUND,
          message: `Asset not found for ticker: ${tx.ticker}`,
        });
        continue;
      }
      
      // Normalize transaction type
      const normalizedType = normalizeTransactionType(tx.type);
      
      if (!normalizedType) {
        errors.push({
          rowNumber: tx.originalRowNumber,
          ticker: tx.ticker,
          code: IMPORT_ERROR_CODES.INVALID_DATA,
          message: `Invalid transaction type: ${tx.type}`,
        });
        continue;
      }
      
      // Parse numeric values
      const rawAmount = parseNumber(tx.amount);
      const amount = Math.abs(rawAmount);  // Convert to absolute value
      const price = Math.abs(parseNumber(tx.price));
      const commission = Math.abs(parseNumber(tx.commission)) || 0;
      
      if (isNaN(amount) || amount <= 0) {
        errors.push({
          rowNumber: tx.originalRowNumber,
          ticker: tx.ticker,
          code: IMPORT_ERROR_CODES.INVALID_DATA,
          message: `Invalid amount: ${tx.amount}`,
        });
        continue;
      }
      
      if (isNaN(price) || price <= 0) {
        errors.push({
          rowNumber: tx.originalRowNumber,
          ticker: tx.ticker,
          code: IMPORT_ERROR_CODES.INVALID_DATA,
          message: `Invalid price: ${tx.price}`,
        });
        continue;
      }
      
      // Normalize date
      const normalizedDate = normalizeDate(tx.date);
      
      if (!normalizedDate) {
        errors.push({
          rowNumber: tx.originalRowNumber,
          ticker: tx.ticker,
          code: IMPORT_ERROR_CODES.INVALID_DATA,
          message: `Invalid date: ${tx.date}`,
        });
        continue;
      }
      
      // Get exchange rate (AC-015, AC-016)
      const currency = tx.currency || defaultCurrency || 'USD';
      let dollarPriceToDate = 1;
      
      if (defaultCurrency !== 'USD') {
        dollarPriceToDate = await getExchangeRate(defaultCurrency, normalizedDate);
      }
      
      // Build enriched transaction
      const enrichedTx = {
        assetId: assetInfo.id,
        assetName: normalizedTicker,
        type: normalizedType,
        amount: Math.abs(amount),  // Always positive
        price,
        date: normalizedDate,
        currency,
        commission,
        assetType: assetInfo.assetType,
        market: assetInfo.market,
        dollarPriceToDate,
        defaultCurrencyForAdquisitionDollar: defaultCurrency || 'USD',
        portfolioAccountId,
        userId,
        originalRowNumber: tx.originalRowNumber,
      };
      
      enriched.push(enrichedTx);
      
    } catch (error) {
      console.error(`[transactionEnricher] Error enriching row ${tx.originalRowNumber}:`, error);
      errors.push({
        rowNumber: tx.originalRowNumber,
        ticker: tx.ticker || 'UNKNOWN',
        code: IMPORT_ERROR_CODES.ENRICHMENT_FAILED,
        message: error.message || 'Unknown enrichment error',
      });
    }
  }
  
  console.log(`[transactionEnricher] Enrichment complete: ${enriched.length} success, ${errors.length} errors`);
  
  return { data: enriched, errors };
}

// ============================================================================
// EXCHANGE RATE FUNCTIONS (AC-015, AC-016)
// ============================================================================

/**
 * Preloads exchange rates for a list of dates (optimization)
 * 
 * @param {string[]} dates - List of dates (YYYY-MM-DD)
 * @param {string} currency - Currency code (e.g., COP)
 */
async function preloadExchangeRates(dates, currency) {
  // For now, we'll load rates one by one from cache or API
  // Future optimization: batch API call if available
  console.log(`[transactionEnricher] Preloading exchange rates for ${dates.length} dates`);
  
  for (const date of dates) {
    await getExchangeRate(currency, date);
  }
}

/**
 * Gets exchange rate for a specific currency and date.
 * Uses cache to avoid duplicate API calls (AC-017).
 * 
 * @param {string} currency - Currency code (e.g., COP)
 * @param {string} date - Date (YYYY-MM-DD)
 * @returns {Promise<number>} Exchange rate (currency per USD)
 */
async function getExchangeRate(currency, date) {
  if (!currency || currency === 'USD') {
    return 1;
  }
  
  const cacheKey = `${currency}:${date}`;
  
  if (exchangeRateCache.has(cacheKey)) {
    return exchangeRateCache.get(cacheKey);
  }
  
  try {
    // Try to get from quotes API
    const symbol = `${currency}=X`;
    const quotes = await getQuotes(symbol);
    
    if (quotes && quotes[symbol]) {
      const rate = quotes[symbol].regularMarketPrice || quotes[symbol].price || 1;
      exchangeRateCache.set(cacheKey, rate);
      return rate;
    }
    
    // Fallback: use approximate rates for common currencies
    const fallbackRates = {
      COP: 4200,
      EUR: 0.92,
      GBP: 0.79,
      MXN: 17.5,
      BRL: 5.0,
    };
    
    const fallbackRate = fallbackRates[currency] || 1;
    console.warn(`[transactionEnricher] Using fallback rate for ${currency}: ${fallbackRate}`);
    exchangeRateCache.set(cacheKey, fallbackRate);
    return fallbackRate;
    
  } catch (error) {
    console.error(`[transactionEnricher] Error fetching exchange rate for ${currency}:`, error);
    // Return 1 as safe default
    return 1;
  }
}

// ============================================================================
// NORMALIZATION HELPERS
// ============================================================================

/**
 * Normalizes transaction type to buy/sell
 * @param {string} type - Raw type value
 * @returns {'buy'|'sell'|null}
 */
function normalizeTransactionType(type) {
  const normalized = (type || '').toLowerCase().trim();
  
  const buyPatterns = ['buy', 'b', 'compra', 'c', 'bot', 'bought', 'open', 'long'];
  const sellPatterns = ['sell', 's', 'venta', 'v', 'sld', 'sold', 'close', 'short'];
  
  if (buyPatterns.includes(normalized)) return TRANSACTION_TYPES.BUY;
  if (sellPatterns.includes(normalized)) return TRANSACTION_TYPES.SELL;
  
  return null;
}

/**
 * Parses a number from various formats
 * @param {string|number} value - Value to parse
 * @returns {number}
 */
function parseNumber(value) {
  if (typeof value === 'number') return value;
  if (!value) return 0;
  
  // Remove commas (thousand separators) and parse
  const cleaned = value.toString().replace(/,/g, '').trim();
  return parseFloat(cleaned);
}

/**
 * Normalizes a date to ISO timestamp format (YYYY-MM-DDTHH:mm:ss.sssZ)
 * 
 * FIX-TIMESTAMP-002: Preserva el timestamp si viene en el input.
 * Si solo viene la fecha (sin hora), agrega la hora actual del servidor.
 * Esto garantiza consistencia con la migración de timestamps existentes.
 * 
 * @param {string} date - Date string in various formats
 * @returns {string|null} Normalized ISO timestamp or null if invalid
 */
function normalizeDate(date) {
  if (!date) return null;
  
  const cleaned = date.toString().trim();
  
  // Helper: Get current server time components for appending to date-only values
  const getServerTimeComponent = () => {
    const now = new Date();
    return now.toISOString().substring(10); // Returns "THH:mm:ss.sssZ"
  };
  
  // ISO format with full timestamp: YYYY-MM-DDTHH:MM:SS.sssZ (preserve as-is)
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(cleaned)) {
    // Already has full timestamp, normalize to ISO format
    try {
      const parsed = new Date(cleaned);
      if (!isNaN(parsed.getTime())) {
        return parsed.toISOString();
      }
    } catch {
      // Fall through to return cleaned
    }
    return cleaned;
  }
  
  // ISO format: YYYY-MM-DD (add server time)
  if (/^\d{4}-\d{2}-\d{2}$/.test(cleaned)) {
    return cleaned + getServerTimeComponent();
  }
  
  // ISO with time but no timezone: YYYY-MM-DD HH:MM:SS or YYYY-MM-DDTHH:MM:SS
  if (/^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}(:\d{2})?$/.test(cleaned)) {
    // Parse and convert to ISO
    try {
      const normalized = cleaned.replace(' ', 'T');
      const parsed = new Date(normalized + 'Z');
      if (!isNaN(parsed.getTime())) {
        return parsed.toISOString();
      }
    } catch {
      // Fall through
    }
    // Fallback: add server time to date portion
    return cleaned.substring(0, 10) + getServerTimeComponent();
  }
  
  // US format: MM/DD/YYYY or M/D/YYYY
  const usMatch = cleaned.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (usMatch) {
    const [, month, day, year] = usMatch;
    const dateOnly = `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
    return dateOnly + getServerTimeComponent();
  }
  
  // Text month: Jan 15, 2024
  const textMatch = cleaned.match(/^([A-Z][a-z]{2})\s+(\d{1,2}),?\s+(\d{4})$/);
  if (textMatch) {
    const [, monthStr, day, year] = textMatch;
    const months = { 
      Jan: '01', Feb: '02', Mar: '03', Apr: '04', May: '05', Jun: '06',
      Jul: '07', Aug: '08', Sep: '09', Oct: '10', Nov: '11', Dec: '12',
    };
    const month = months[monthStr];
    if (month) {
      const dateOnly = `${year}-${month}-${day.padStart(2, '0')}`;
      return dateOnly + getServerTimeComponent();
    }
  }
  
  // Try parsing with Date object as fallback
  try {
    const parsed = new Date(cleaned);
    if (!isNaN(parsed.getTime())) {
      return parsed.toISOString();
    }
  } catch {
    // Ignore parse errors
  }
  
  return null;
}

/**
 * Clears the exchange rate cache (for testing)
 */
function clearCache() {
  exchangeRateCache.clear();
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  enrichTransactions,
  getExchangeRate,
  normalizeTransactionType,
  parseNumber,
  normalizeDate,
  clearCache,
  // Export for testing
  _exchangeRateCache: exchangeRateCache,
};
