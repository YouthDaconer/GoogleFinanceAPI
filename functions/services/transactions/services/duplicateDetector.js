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
 * @property {string} portfolioAccountId - Account where transaction belongs
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
 * Duplicates are identified by: ticker + date + amount + price + type + portfolioAccountId (AC-018)
 * 
 * FIX 2026-01-27: Added portfolioAccountId to signature to prevent false positives
 * when same stock is bought on same day in different accounts (e.g., XTB and IBKR).
 * 
 * FIX 2026-01-28: Use occurrence counter instead of blocking in-batch "duplicates".
 * When user sells 3 lots of same stock at same price on same day, these are NOT
 * duplicates - they're legitimate separate transactions. We now count occurrences
 * of each signature and only mark as duplicate if it exceeds existing + batch count.
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
  
  // Count existing transaction signatures (same signature can appear multiple times)
  // Map: signature -> count of existing transactions with that signature
  const existingSignatureCounts = new Map();
  
  for (const [ticker, txs] of byTicker) {
    // Get existing transactions for this ticker
    const existing = await getExistingTransactionsForTicker(ticker, userId);
    
    // Count signatures (not just existence, but HOW MANY)
    for (const tx of existing) {
      const signature = createSignature(tx);
      existingSignatureCounts.set(signature, (existingSignatureCounts.get(signature) || 0) + 1);
    }
  }
  
  // Track how many of each signature we've seen in the import batch
  // Map: signature -> count of times we've accepted it in this batch
  const batchSignatureCounts = new Map();
  
  for (const tx of transactions) {
    const signature = createSignature(tx);
    
    // How many of this signature already exist in Firestore?
    const existingCount = existingSignatureCounts.get(signature) || 0;
    
    // How many have we already accepted in this batch?
    const batchCount = batchSignatureCounts.get(signature) || 0;
    
    // Total count if we accept this transaction
    const totalCount = existingCount + batchCount + 1;
    
    // Check: is this a duplicate (i.e., would exceed the count we should allow)?
    // For identical transactions in the batch, we allow up to N new ones where N = batch occurrences
    // This means if the batch has 3 identical sells, we accept all 3 unless they already exist
    
    // Count how many times this signature appears in the ENTIRE import batch
    const batchTotalForSignature = transactions.filter(t => createSignature(t) === signature).length;
    
    // If this signature already exists in Firestore with count >= batch occurrences, mark as duplicate
    if (existingCount >= batchTotalForSignature) {
      // All occurrences of this signature already exist - this is a true duplicate
      duplicates.push(tx);
      console.log(`[duplicateDetector] Found duplicate (existing=${existingCount}, batch=${batchTotalForSignature}): ${tx.assetName} ${tx.date} ${tx.type}`);
      continue;
    }
    
    // How many more can we accept? = batch total - existing count
    const allowedNew = batchTotalForSignature - existingCount;
    
    // If we've already accepted enough new ones, this is a duplicate
    if (batchCount >= allowedNew) {
      duplicates.push(tx);
      console.log(`[duplicateDetector] Found in-batch duplicate (batchCount=${batchCount}, allowedNew=${allowedNew}): ${tx.assetName} ${tx.date} ${tx.type}`);
      continue;
    }
    
    // Accept this transaction
    batchSignatureCounts.set(signature, batchCount + 1);
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
 * Format: "TICKER|DATE|AMOUNT|PRICE|TYPE|ACCOUNT"
 * 
 * FIX 2026-01-27: Added portfolioAccountId to prevent false positives
 * when importing transactions for same stock on same day in different accounts.
 * 
 * OPT-IMPORT-003 2026-01-28: Now uses full datetime if available.
 * If the date includes time (e.g., "2024-01-15T10:30:00"), it's used in the signature.
 * This allows distinguishing multiple transactions on the same day at different times.
 * Falls back to date-only if no time is present (uses occurrence counter as fallback).
 * 
 * @param {Object} tx - Transaction object
 * @returns {string}
 */
function createSignature(tx) {
  // Normalize values for comparison
  const ticker = (tx.assetName || tx.ticker || '').toUpperCase();
  
  // OPT-IMPORT-003: Use full datetime if available, otherwise just date
  // If date contains "T" (ISO datetime format), use it as-is for more precision
  const rawDate = tx.date || '';
  const date = rawDate.includes('T') ? rawDate : rawDate.substring(0, 10);
  
  const amount = roundToDecimals(parseFloat(tx.amount) || 0, 4);
  const price = roundToDecimals(parseFloat(tx.price) || 0, 2);
  const type = (tx.type || '').toLowerCase();
  // Include portfolioAccountId to differentiate transactions across accounts
  const accountId = tx.portfolioAccountId || '';
  
  return `${ticker}|${date}|${amount}|${price}|${type}|${accountId}`;
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
