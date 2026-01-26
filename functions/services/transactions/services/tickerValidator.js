/**
 * Ticker Validation Service
 * 
 * Validates ALL unique tickers against the finance-query API /v1/quotes endpoint.
 * Uses batch validation for efficiency (single API call for all tickers).
 * Returns validation results with asset info for valid tickers.
 * 
 * @module transactions/services/tickerValidator
 * @see docs/stories/89.story.md (IMPORT-001)
 */

const { getQuotes, search } = require('../../financeQuery');
const { LIMITS } = require('../types');

// ============================================================================
// CONSTANTS
// ============================================================================

const QUOTES_TIMEOUT_MS = 15000;  // 15 seconds timeout for batch validation
const MAX_TICKERS_PER_REQUEST = 100;  // API limit

// ============================================================================
// MAIN VALIDATION FUNCTION
// ============================================================================

/**
 * Validates ALL unique tickers against the market data API using /v1/quotes
 * 
 * This function validates all tickers in a single API call (or batched if >100).
 * Much more efficient than individual /search calls.
 * 
 * @param {string[]} tickers - Array of ALL tickers from file (can include duplicates)
 * @returns {Promise<Object>} Validation summary with details per ticker
 * 
 * @example
 * const result = await validateTickerSample(['AAPL', 'NVDA', 'APPL', 'XYZ']);
 * // {
 * //   total: 4,
 * //   valid: 2,
 * //   invalid: 2,
 * //   invalidTickers: ['APPL', 'XYZ'],
 * //   validDetails: { 'AAPL': { name: 'Apple Inc', ... }, ... }
 * // }
 */
async function validateTickerSample(tickers) {
  const result = {
    total: 0,
    valid: 0,
    invalid: 0,
    unverified: 0,
    invalidTickers: [],
    unverifiedTickers: [],
    suggestions: {},
    details: {},
    validDetails: {},  // NEW: Contains metadata for valid tickers (name, sector, logo)
  };
  
  if (!tickers || tickers.length === 0) {
    return result;
  }
  
  // Get unique tickers, normalized
  const uniqueTickers = [...new Set(
    tickers
      .map(t => normalizeTicker(t))
      .filter(t => t && t.length > 0)
  )];
  
  result.total = uniqueTickers.length;
  
  console.log(`[tickerValidator] Validating ${uniqueTickers.length} unique tickers using /quotes`);
  
  try {
    // Call /v1/quotes with all tickers at once
    const quotes = await validateWithQuotes(uniqueTickers);
    
    // Build set of valid symbols from response
    const validSymbols = new Set(quotes.map(q => q.symbol?.toUpperCase()).filter(Boolean));
    
    // Process each ticker
    for (const ticker of uniqueTickers) {
      const upperTicker = ticker.toUpperCase();
      const quote = quotes.find(q => q.symbol?.toUpperCase() === upperTicker);
      
      if (quote) {
        // Valid ticker - store details
        result.valid++;
        result.details[ticker] = {
          originalTicker: ticker,
          isValid: true,
          normalizedTicker: quote.symbol,
          companyName: quote.name || quote.shortName,
          sector: quote.sector,
          industry: quote.industry,
          logo: quote.logo,
          assetType: detectAssetType(quote),
          currency: quote.currency || 'USD',
        };
        result.validDetails[ticker] = {
          symbol: quote.symbol,
          name: quote.name || quote.shortName,
          sector: quote.sector,
          industry: quote.industry,
          logo: quote.logo,
        };
      } else {
        // Invalid ticker - try to find suggestion
        result.invalid++;
        result.invalidTickers.push(ticker);
        result.details[ticker] = {
          originalTicker: ticker,
          isValid: false,
          error: 'Ticker not found',
        };
        
        // Try to find a suggestion using search (only for invalid tickers)
        const suggestion = await findSuggestion(ticker);
        if (suggestion) {
          result.suggestions[ticker] = suggestion;
          result.details[ticker].suggestion = suggestion;
        }
      }
    }
    
  } catch (error) {
    console.error(`[tickerValidator] Batch validation failed: ${error.message}`);
    
    // Mark all as unverified due to API error
    result.unverified = uniqueTickers.length;
    result.unverifiedTickers = [...uniqueTickers];
    
    for (const ticker of uniqueTickers) {
      result.details[ticker] = {
        originalTicker: ticker,
        isValid: false,
        isUnverified: true,
        error: error.message,
      };
    }
  }
  
  console.log(`[tickerValidator] Results: ${result.valid} valid, ${result.invalid} invalid, ${result.unverified} unverified`);
  
  return result;
}

// ============================================================================
// BATCH VALIDATION WITH /v1/quotes
// ============================================================================

/**
 * Validates tickers using /v1/quotes endpoint (batch)
 * 
 * @param {string[]} tickers - Array of unique tickers
 * @returns {Promise<Object[]>} Array of quote objects for valid tickers
 */
async function validateWithQuotes(tickers) {
  if (tickers.length === 0) return [];
  
  // If more than 100 tickers, batch them (rare case)
  if (tickers.length > MAX_TICKERS_PER_REQUEST) {
    console.log(`[tickerValidator] Batching ${tickers.length} tickers`);
    const batches = [];
    for (let i = 0; i < tickers.length; i += MAX_TICKERS_PER_REQUEST) {
      batches.push(tickers.slice(i, i + MAX_TICKERS_PER_REQUEST));
    }
    
    const results = await Promise.all(
      batches.map(batch => validateSingleBatch(batch))
    );
    
    return results.flat();
  }
  
  return validateSingleBatch(tickers);
}

/**
 * Validates a single batch of tickers
 * 
 * @param {string[]} tickers - Batch of tickers (max 100)
 * @returns {Promise<Object[]>} Array of quote objects
 */
async function validateSingleBatch(tickers) {
  const symbolsParam = tickers.join(',');
  
  // Call with timeout
  const quotes = await Promise.race([
    getQuotes(symbolsParam),
    new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Timeout')), QUOTES_TIMEOUT_MS)
    )
  ]);
  
  // Ensure we return an array
  if (!quotes || !Array.isArray(quotes)) {
    console.warn('[tickerValidator] Unexpected response from /quotes:', typeof quotes);
    return [];
  }
  
  return quotes;
}

// ============================================================================
// SINGLE TICKER VALIDATION
// ============================================================================

/**
 * Validates a single ticker against the search API
 * 
 * @param {string} ticker - Ticker symbol to validate
 * @returns {Promise<Object>} Validation result for the ticker
 */
async function validateSingleTicker(ticker) {
  const result = {
    originalTicker: ticker,
    isValid: false,
    isUnverified: false,  // True if couldn't verify due to API error (not the same as invalid)
    normalizedTicker: null,
    assetType: null,
    market: null,
    currency: null,
    companyName: null,
    logo: null,
    suggestion: null,
    error: null,
  };
  
  if (!ticker || ticker.length === 0) {
    result.error = 'Empty ticker';
    return result;
  }
  
  try {
    // Call finance-query /v1/search with timeout
    // Note: 15 seconds to account for Cloud Functions cold start + API latency
    const searchResults = await Promise.race([
      search(ticker),
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Timeout')), 15000)
      )
    ]);
    
    if (!searchResults || !Array.isArray(searchResults) || searchResults.length === 0) {
      // No results - skip suggestion lookup for speed
      result.error = 'Ticker not found';
      return result;
    }
    
    // Find exact match first
    const exactMatch = searchResults.find(r => 
      r.symbol?.toUpperCase() === ticker.toUpperCase()
    );
    
    if (exactMatch) {
      result.isValid = true;
      result.normalizedTicker = exactMatch.symbol?.toUpperCase();
      result.assetType = detectAssetType(exactMatch);
      result.market = exactMatch.exchange || exactMatch.exchDisp || null;
      result.currency = exactMatch.currency || null;
      result.companyName = exactMatch.shortname || exactMatch.longname || null;
      result.logo = exactMatch.logo || null;
      return result;
    }
    
    // No exact match - first result might be suggestion
    const firstResult = searchResults[0];
    
    // Check if first result is very similar (could be different class of stock)
    if (firstResult.symbol?.toUpperCase().startsWith(ticker.toUpperCase().slice(0, 3))) {
      result.suggestion = firstResult.symbol?.toUpperCase();
      result.error = `Ticker not found. Did you mean ${result.suggestion}?`;
    } else {
      result.error = 'Ticker not found';
    }
    
    return result;
    
  } catch (error) {
    console.error(`[tickerValidator] Error validating ${ticker}:`, error.message);
    // API errors (timeout, network issues) should mark as unverified, not invalid
    // The ticker might still be valid, we just couldn't check it
    result.isUnverified = true;
    result.error = `No se pudo verificar: ${error.message}`;
    return result;
  }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Normalizes a ticker symbol
 * 
 * @param {string} ticker - Raw ticker from file
 * @returns {string} Normalized ticker
 */
function normalizeTicker(ticker) {
  if (!ticker) return '';
  
  return String(ticker)
    .trim()
    .toUpperCase()
    // Remove common prefixes/suffixes
    .replace(/^\$/, '')           // Remove leading $
    .replace(/\s+/g, '')          // Remove whitespace
    .replace(/\.$/g, '');         // Remove trailing dot
}

/**
 * Detects asset type from search result
 * 
 * @param {Object} searchResult - Result from search API
 * @returns {string} Asset type (stock, etf, crypto)
 */
function detectAssetType(searchResult) {
  const type = searchResult.quoteType?.toLowerCase() || 
               searchResult.typeDisp?.toLowerCase() || '';
  
  if (type.includes('etf') || type.includes('fund')) {
    return 'etf';
  }
  
  if (type.includes('crypto') || type.includes('cryptocurrency')) {
    return 'crypto';
  }
  
  // Default to stock
  return 'stock';
}

/**
 * Tries to find a suggestion for an invalid ticker
 * 
 * @param {string} invalidTicker - The invalid ticker
 * @returns {Promise<string|null>} Suggestion or null
 */
async function findSuggestion(invalidTicker) {
  // Common typos and corrections
  const commonCorrections = {
    'GOOG': 'GOOGL',
    'FB': 'META',
    'BRKB': 'BRK-B',
    'BRKA': 'BRK-A',
  };
  
  if (commonCorrections[invalidTicker]) {
    return commonCorrections[invalidTicker];
  }
  
  // Try searching with partial match
  try {
    // Search with first 3 chars
    if (invalidTicker.length >= 3) {
      const partial = invalidTicker.slice(0, 3);
      const results = await search(partial);
      
      if (results && results.length > 0) {
        // Find most similar
        const similar = results.find(r => 
          r.symbol?.toUpperCase().startsWith(partial)
        );
        
        if (similar) {
          return similar.symbol?.toUpperCase();
        }
      }
    }
  } catch (error) {
    // Ignore errors in suggestion finding
  }
  
  return null;
}

/**
 * Batch validates multiple tickers efficiently
 * 
 * @param {string[]} tickers - Tickers to validate
 * @param {number} batchSize - Max concurrent requests
 * @returns {Promise<Object>} Validation summary
 */
async function validateTickersBatched(tickers, batchSize = 5) {
  const result = {
    total: 0,
    valid: 0,
    invalid: 0,
    invalidTickers: [],
    suggestions: {},
    details: {},
  };
  
  const uniqueTickers = [...new Set(
    tickers.map(t => normalizeTicker(t)).filter(t => t)
  )];
  
  result.total = uniqueTickers.length;
  
  // Process in batches to avoid rate limiting
  for (let i = 0; i < uniqueTickers.length; i += batchSize) {
    const batch = uniqueTickers.slice(i, i + batchSize);
    
    const batchResults = await Promise.all(
      batch.map(ticker => 
        validateSingleTicker(ticker).catch(err => ({
          originalTicker: ticker,
          isValid: false,
          error: err.message,
        }))
      )
    );
    
    for (const validation of batchResults) {
      result.details[validation.originalTicker] = validation;
      
      if (validation.isValid) {
        result.valid++;
      } else {
        result.invalid++;
        result.invalidTickers.push(validation.originalTicker);
        
        if (validation.suggestion) {
          result.suggestions[validation.originalTicker] = validation.suggestion;
        }
      }
    }
    
    // Small delay between batches
    if (i + batchSize < uniqueTickers.length) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
  
  return result;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  validateTickerSample,
  validateSingleTicker,
  validateTickersBatched,
  validateWithQuotes,
  normalizeTicker,
  detectAssetType,
};
