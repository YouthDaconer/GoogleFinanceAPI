/**
 * Duplicate Detector Service
 * 
 * IMPORT-002: Detects duplicate transactions in the import batch.
 * 
 * @module transactions/services/duplicateDetector
 * @see docs/stories/90.story.md (IMPORT-002)
 */

const admin = require('../../firebaseAdmin');

const db = admin.firestore();

// ============================================================================
// INTERFACES (JSDoc)
// ============================================================================

/**
 * @typedef {Object} EnrichedTransaction
 * @property {string} assetId
 * @property {string} assetName
 * @property {'buy'|'sell'} type
 * @property {number} amount
 * @property {number} price
 * @property {string} date
 * @property {number} originalRowNumber
 */

/**
 * @typedef {Object} DuplicateResult
 * @property {EnrichedTransaction[]} unique - Transactions to import
 * @property {EnrichedTransaction[]} duplicates - Detected duplicates
 */

// ============================================================================
// MAIN FUNCTIONS
// ============================================================================

/**
 * Detects duplicate transactions by comparing with existing Firestore data.
 * Duplicates are identified by: ticker + date + amount + price + type (AC-018)
 * 
 * @param {EnrichedTransaction[]} transactions - Enriched transactions to check
 * @param {string} userId - User ID for query
 * @returns {Promise<DuplicateResult>}
 */
async function detectDuplicates(transactions, userId) {
  console.log(`[duplicateDetector] Checking ${transactions.length} transactions for duplicates`);
  
  if (!transactions || transactions.length === 0) {
    return { unique: [], duplicates: [] };
  }
  
  /** @type {EnrichedTransaction[]} */
  const unique = [];
  /** @type {EnrichedTransaction[]} */
  const duplicates = [];
  
  // Group transactions by ticker for efficient querying
  const byTicker = groupByTicker(transactions);
  
  // Build set of existing transaction signatures
  const existingSignatures = new Set();
  
  for (const [ticker, txs] of byTicker) {
    // Get existing transactions for this ticker
    const existing = await getExistingTransactionsForTicker(ticker, userId);
    
    // Add signatures to set
    for (const tx of existing) {
      const signature = createSignature(tx);
      existingSignatures.add(signature);
    }
  }
  
  // Check within-batch duplicates too
  const batchSignatures = new Set();
  
  for (const tx of transactions) {
    const signature = createSignature(tx);
    
    // Check against existing in Firestore
    if (existingSignatures.has(signature)) {
      duplicates.push(tx);
      console.log(`[duplicateDetector] Found existing duplicate: ${tx.assetName} ${tx.date} ${tx.type}`);
      continue;
    }
    
    // Check within-batch duplicates
    if (batchSignatures.has(signature)) {
      duplicates.push(tx);
      console.log(`[duplicateDetector] Found in-batch duplicate: ${tx.assetName} ${tx.date} ${tx.type}`);
      continue;
    }
    
    batchSignatures.add(signature);
    unique.push(tx);
  }
  
  console.log(`[duplicateDetector] Results: ${unique.length} unique, ${duplicates.length} duplicates`);
  
  return { unique, duplicates };
}

/**
 * Gets existing transactions for a ticker from Firestore
 * 
 * @param {string} ticker - Ticker symbol
 * @param {string} userId - User ID
 * @returns {Promise<Object[]>}
 */
async function getExistingTransactionsForTicker(ticker, userId) {
  try {
    const snapshot = await db.collection('transactions')
      .where('userId', '==', userId)
      .where('assetName', '==', ticker)
      .get();
    
    return snapshot.docs.map(doc => ({
      id: doc.id,
      ...doc.data(),
    }));
  } catch (error) {
    console.error(`[duplicateDetector] Error querying transactions for ${ticker}:`, error);
    return [];
  }
}

/**
 * Creates a unique signature for a transaction (AC-018)
 * Format: "TICKER|DATE|AMOUNT|PRICE|TYPE"
 * 
 * @param {Object} tx - Transaction object
 * @returns {string}
 */
function createSignature(tx) {
  // Normalize values for comparison
  const ticker = (tx.assetName || tx.ticker || '').toUpperCase();
  const date = (tx.date || '').substring(0, 10);  // Just date part
  const amount = roundToDecimals(parseFloat(tx.amount) || 0, 4);
  const price = roundToDecimals(parseFloat(tx.price) || 0, 2);
  const type = (tx.type || '').toLowerCase();
  
  return `${ticker}|${date}|${amount}|${price}|${type}`;
}

/**
 * Groups transactions by ticker
 * 
 * @param {EnrichedTransaction[]} transactions
 * @returns {Map<string, EnrichedTransaction[]>}
 */
function groupByTicker(transactions) {
  const groups = new Map();
  
  for (const tx of transactions) {
    const ticker = (tx.assetName || '').toUpperCase();
    
    if (!groups.has(ticker)) {
      groups.set(ticker, []);
    }
    
    groups.get(ticker).push(tx);
  }
  
  return groups;
}

/**
 * Rounds a number to specified decimal places
 * 
 * @param {number} num - Number to round
 * @param {number} decimals - Decimal places
 * @returns {number}
 */
function roundToDecimals(num, decimals) {
  const factor = Math.pow(10, decimals);
  return Math.round(num * factor) / factor;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  detectDuplicates,
  getExistingTransactionsForTicker,
  createSignature,
  groupByTicker,
  roundToDecimals,
};
