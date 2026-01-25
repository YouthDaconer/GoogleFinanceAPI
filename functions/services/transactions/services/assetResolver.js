/**
 * Asset Resolver Service
 * 
 * IMPORT-002: Resolves tickers to assets - finds existing or creates new.
 * 
 * @module transactions/services/assetResolver
 * @see docs/stories/90.story.md (IMPORT-002)
 */

const admin = require('../../firebaseAdmin');
const { search, getQuotes } = require('../../financeQuery');
const { QUOTE_TYPE_MAPPING, MARKET_CURRENCY_MAP } = require('../types');

const db = admin.firestore();

// ============================================================================
// INTERFACES (JSDoc)
// ============================================================================

/**
 * @typedef {Object} AssetInfo
 * @property {string} id - Firestore document ID
 * @property {string} ticker - Asset ticker symbol
 * @property {'stock'|'etf'|'crypto'} assetType - Type of asset
 * @property {string} market - Exchange/market
 * @property {string} currency - Asset currency
 * @property {boolean} isNew - True if asset was created during this import
 */

/**
 * @typedef {Object} AssetResolutionResult
 * @property {Map<string, AssetInfo>} assetMap - Map of ticker -> asset info
 * @property {string[]} created - IDs of newly created assets
 * @property {Map<string, string>} errors - Map of ticker -> error message
 */

/**
 * @typedef {Object} TickerInfo
 * @property {'stock'|'etf'|'crypto'} assetType - Type of asset
 * @property {string} market - Exchange/market
 * @property {string} currency - Asset currency
 * @property {string} name - Company/asset name
 */

// ============================================================================
// TICKER INFO CACHE
// ============================================================================

/** @type {Map<string, TickerInfo>} */
const tickerInfoCache = new Map();

// ============================================================================
// MAIN FUNCTIONS
// ============================================================================

/**
 * Resolves assets for all unique tickers in the transaction batch.
 * Finds existing assets or creates new ones if createMissing is true.
 * 
 * @param {Map<string, Object[]>} groupedByTicker - Transactions grouped by ticker
 * @param {string} portfolioAccountId - Target portfolio account ID
 * @param {string} userId - User ID for ownership
 * @param {boolean} createMissing - Whether to create missing assets (AC-007)
 * @returns {Promise<AssetResolutionResult>}
 */
async function resolveAssets(groupedByTicker, portfolioAccountId, userId, createMissing) {
  console.log(`[assetResolver] Resolving ${groupedByTicker.size} unique tickers`);
  
  /** @type {Map<string, AssetInfo>} */
  const assetMap = new Map();
  /** @type {string[]} */
  const created = [];
  /** @type {Map<string, string>} */
  const errors = new Map();
  
  for (const [ticker, transactions] of groupedByTicker) {
    try {
      const normalizedTicker = normalizeTicker(ticker);
      
      // AC-005: Search for existing asset by ticker + portfolioAccountId
      const existingAsset = await findExistingAsset(
        normalizedTicker, 
        portfolioAccountId
      );
      
      if (existingAsset) {
        // AC-006: Found existing asset
        console.log(`[assetResolver] Found existing asset for ${normalizedTicker}: ${existingAsset.id}`);
        assetMap.set(normalizedTicker, {
          id: existingAsset.id,
          ticker: normalizedTicker,
          assetType: existingAsset.assetType || 'stock',
          market: existingAsset.market || '',
          currency: existingAsset.currency || 'USD',
          isNew: false,
        });
        continue;
      }
      
      // Asset not found - create if allowed
      if (!createMissing) {
        errors.set(normalizedTicker, 'Asset not found and createMissingAssets=false');
        continue;
      }
      
      // AC-007: Create new asset
      // First, get ticker info from API (AC-011 to AC-014)
      const tickerInfo = await getTickerInfo(normalizedTicker);
      
      if (!tickerInfo) {
        errors.set(normalizedTicker, `Unable to get market data for ticker: ${normalizedTicker}`);
        continue;
      }
      
      // AC-009: Find first buy date for acquisitionDate
      const firstBuy = findFirstBuyTransaction(transactions);
      const acquisitionDate = firstBuy ? firstBuy.date : new Date().toISOString().split('T')[0];
      const acquisitionPrice = firstBuy ? firstBuy.price : 0;
      
      // Create new asset document
      const newAssetRef = db.collection('assets').doc();
      
      const newAssetData = {
        name: normalizedTicker,
        portfolioAccount: portfolioAccountId,
        userId,
        assetType: tickerInfo.assetType,
        market: tickerInfo.market,
        currency: tickerInfo.currency,
        units: 0,  // Will be updated by batchWriter
        unitValue: acquisitionPrice,
        acquisitionDate: acquisitionDate,
        company: 'Imported',  // Placeholder, can be enriched later
        commission: 0,
        isActive: true,  // AC-010
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        importSource: 'batch_import',
      };
      
      await newAssetRef.set(newAssetData);
      
      console.log(`[assetResolver] Created new asset for ${normalizedTicker}: ${newAssetRef.id}`);
      
      assetMap.set(normalizedTicker, {
        id: newAssetRef.id,
        ticker: normalizedTicker,
        assetType: tickerInfo.assetType,
        market: tickerInfo.market,
        currency: tickerInfo.currency,
        isNew: true,
      });
      
      created.push(newAssetRef.id);
      
    } catch (error) {
      console.error(`[assetResolver] Error resolving ${ticker}:`, error);
      errors.set(ticker, error.message || 'Unknown error during asset resolution');
    }
  }
  
  console.log(`[assetResolver] Resolution complete: ${assetMap.size} resolved, ${created.length} created, ${errors.size} errors`);
  
  return { assetMap, created, errors };
}

/**
 * Finds an existing asset by ticker and portfolio account
 * 
 * @param {string} ticker - Normalized ticker symbol
 * @param {string} portfolioAccountId - Portfolio account ID
 * @returns {Promise<Object|null>} Asset document data or null
 */
async function findExistingAsset(ticker, portfolioAccountId) {
  const snapshot = await db.collection('assets')
    .where('name', '==', ticker)
    .where('portfolioAccount', '==', portfolioAccountId)
    .where('isActive', '==', true)
    .limit(1)
    .get();
  
  if (snapshot.empty) {
    return null;
  }
  
  const doc = snapshot.docs[0];
  return {
    id: doc.id,
    ...doc.data(),
  };
}

/**
 * Gets ticker info from finance-query API
 * Uses search endpoint to get assetType, market, currency
 * 
 * @param {string} ticker - Ticker symbol to look up
 * @returns {Promise<TickerInfo|null>}
 */
async function getTickerInfo(ticker) {
  // Check cache first
  if (tickerInfoCache.has(ticker)) {
    return tickerInfoCache.get(ticker);
  }
  
  try {
    // Try search first (more reliable for classification)
    const searchResults = await search(ticker);
    
    if (searchResults && searchResults.length > 0) {
      const exactMatch = searchResults.find(r => 
        r.symbol?.toUpperCase() === ticker.toUpperCase()
      ) || searchResults[0];
      
      const quoteType = exactMatch.quoteType || exactMatch.typeDisp || 'EQUITY';
      const market = exactMatch.exchange || exactMatch.exchDisp || '';
      
      const info = {
        assetType: mapQuoteType(quoteType),
        market: market,
        currency: inferCurrency(market),
        name: exactMatch.shortname || exactMatch.longname || ticker,
      };
      
      tickerInfoCache.set(ticker, info);
      return info;
    }
    
    // Fallback to quotes endpoint
    const quotes = await getQuotes(ticker);
    
    if (quotes && quotes[ticker]) {
      const quote = quotes[ticker];
      const info = {
        assetType: mapQuoteType(quote.quoteType || 'EQUITY'),
        market: quote.exchange || '',
        currency: quote.currency || inferCurrency(quote.exchange || ''),
        name: quote.shortName || quote.longName || ticker,
      };
      
      tickerInfoCache.set(ticker, info);
      return info;
    }
    
    console.warn(`[assetResolver] No data found for ticker: ${ticker}`);
    return null;
    
  } catch (error) {
    console.error(`[assetResolver] Error fetching ticker info for ${ticker}:`, error);
    return null;
  }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Normalizes a ticker symbol
 * @param {string} ticker - Raw ticker
 * @returns {string} Normalized ticker (uppercase, trimmed)
 */
function normalizeTicker(ticker) {
  if (!ticker) return '';
  return ticker.toString().trim().toUpperCase();
}

/**
 * Maps API quoteType to internal asset type
 * @param {string} quoteType - Quote type from API
 * @returns {'stock'|'etf'|'crypto'}
 */
function mapQuoteType(quoteType) {
  const type = (quoteType || '').toUpperCase();
  return QUOTE_TYPE_MAPPING[type] || 'stock';
}

/**
 * Infers currency from market/exchange
 * @param {string} market - Market/exchange code
 * @returns {string} Currency code
 */
function inferCurrency(market) {
  const normalizedMarket = (market || '').toUpperCase();
  return MARKET_CURRENCY_MAP[normalizedMarket] || 'USD';
}

/**
 * Finds the first buy transaction (by date) for acquisitionDate
 * @param {Object[]} transactions - List of transactions
 * @returns {Object|null}
 */
function findFirstBuyTransaction(transactions) {
  const buys = transactions.filter(tx => 
    tx.type?.toLowerCase() === 'buy' || 
    tx.type?.toLowerCase() === 'compra' ||
    tx.amount > 0
  );
  
  if (buys.length === 0) return null;
  
  // Sort by date ascending
  buys.sort((a, b) => {
    const dateA = new Date(a.date || '9999-12-31');
    const dateB = new Date(b.date || '9999-12-31');
    return dateA - dateB;
  });
  
  return buys[0];
}

/**
 * Clears the ticker info cache (for testing)
 */
function clearCache() {
  tickerInfoCache.clear();
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  resolveAssets,
  findExistingAsset,
  getTickerInfo,
  normalizeTicker,
  mapQuoteType,
  inferCurrency,
  clearCache,
  // Export for testing
  _tickerInfoCache: tickerInfoCache,
};
