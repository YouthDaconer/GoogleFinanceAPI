/**
 * Cloud Function: importTransactionBatch
 * 
 * IMPORT-002: Imports a batch of validated transactions to Firestore.
 * Creates assets if they don't exist, enriches with market data,
 * and updates asset balances atomically.
 * 
 * @module transactions/importTransactionBatch
 * @see docs/stories/90.story.md (IMPORT-002)
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require('../firebaseAdmin');

const { resolveAssets, normalizeTicker } = require('./services/assetResolver');
const { enrichTransactions } = require('./services/transactionEnricher');
const { detectDuplicates } = require('./services/duplicateDetector');
const { writeBatches } = require('./services/batchWriter');
const { LIMITS, IMPORT_ERROR_CODES } = require('./types');

const db = admin.firestore();

// ============================================================================
// CLOUD FUNCTION CONFIGURATION
// ============================================================================

/**
 * Cloud Function configuration for importTransactionBatch
 * 
 * - 5 minute timeout for large batches
 * - 512MB memory for processing
 * - Requires authentication
 */
const FUNCTION_CONFIG = {
  cors: [
    'https://portafolio-inversiones.web.app',
    'https://portafolio-inversiones.firebaseapp.com',
    'http://localhost:3000',
    'http://localhost:3001',
  ],
  timeoutSeconds: 300,  // 5 minutes for large batches
  memory: "512MiB",
  minInstances: 0,
  maxInstances: 10,
};

// ============================================================================
// MAIN CLOUD FUNCTION
// ============================================================================

/**
 * Cloud Function: importTransactionBatch
 * 
 * Imports a batch of validated transactions to Firestore.
 * 
 * @param {Object} data - Request payload
 * @param {string} data.portfolioAccountId - Target account ID
 * @param {Object[]} data.transactions - Transactions to import
 * @param {Object} data.options - Import options
 * @param {boolean} data.options.createMissingAssets - Create assets if they don't exist
 * @param {boolean} data.options.skipDuplicates - Skip duplicate transactions
 * @param {string} data.options.defaultCurrency - Default currency (e.g., USD)
 * @param {Object} context - Firebase callable context
 * @returns {Promise<Object>} Import result
 */
const importTransactionBatch = onCall(FUNCTION_CONFIG, async (request) => {
  const startTime = Date.now();
  const { data, auth } = request;
  
  console.log('[importTransactionBatch] Starting import');
  
  // =========================================================================
  // AUTHENTICATION (AC-001, AC-002)
  // =========================================================================
  
  if (!auth) {
    throw new HttpsError(
      'unauthenticated',
      'Debe estar autenticado para importar transacciones'
    );
  }
  
  const userId = auth.uid;
  console.log(`[importTransactionBatch] User: ${userId}`);
  
  // =========================================================================
  // PAYLOAD VALIDATION
  // =========================================================================
  
  const { portfolioAccountId, transactions, options = {} } = data || {};
  
  // Validate portfolioAccountId
  if (!portfolioAccountId || typeof portfolioAccountId !== 'string') {
    throw new HttpsError(
      'invalid-argument',
      'Se requiere portfolioAccountId'
    );
  }
  
  // Validate transactions array
  if (!Array.isArray(transactions)) {
    throw new HttpsError(
      'invalid-argument',
      'transactions debe ser un array'
    );
  }
  
  if (transactions.length === 0) {
    throw new HttpsError(
      'invalid-argument',
      'El array de transacciones está vacío'
    );
  }
  
  // AC-004: Limit batch size
  if (transactions.length > LIMITS.maxBatchTransactions) {
    throw new HttpsError(
      'invalid-argument',
      `Máximo ${LIMITS.maxBatchTransactions} transacciones por lote. Recibidas: ${transactions.length}`
    );
  }
  
  // Extract options with defaults
  const {
    createMissingAssets = true,
    skipDuplicates = true,
    defaultCurrency = 'USD',
  } = options;
  
  console.log(`[importTransactionBatch] Processing ${transactions.length} transactions for account ${portfolioAccountId}`);
  console.log(`[importTransactionBatch] Options: createMissingAssets=${createMissingAssets}, skipDuplicates=${skipDuplicates}`);
  
  // =========================================================================
  // ACCOUNT VERIFICATION (AC-002, AC-003)
  // =========================================================================
  
  const account = await verifyAccountAccess(portfolioAccountId, userId);
  
  if (!account) {
    throw new HttpsError(
      'permission-denied',
      'No tiene acceso a esta cuenta de portafolio'
    );
  }
  
  console.log(`[importTransactionBatch] Account verified: ${account.name || portfolioAccountId}`);
  
  // =========================================================================
  // PROCESS TRANSACTIONS
  // =========================================================================
  
  try {
    // Add row numbers if not present
    const indexedTransactions = transactions.map((tx, index) => ({
      ...tx,
      originalRowNumber: tx.originalRowNumber || index + 1,
    }));
    
    // Group transactions by ticker for asset resolution
    const groupedByTicker = groupByTicker(indexedTransactions);
    console.log(`[importTransactionBatch] Found ${groupedByTicker.size} unique tickers`);
    
    // -----------------------------------------------------------------------
    // 1. RESOLVE ASSETS (AC-005 to AC-010)
    // -----------------------------------------------------------------------
    
    const { assetMap, created: assetsCreated, errors: assetErrors } = 
      await resolveAssets(groupedByTicker, portfolioAccountId, userId, createMissingAssets);
    
    console.log(`[importTransactionBatch] Assets resolved: ${assetMap.size} found/created, ${assetErrors.size} errors`);
    
    // Filter out transactions for assets that couldn't be resolved
    const resolvableTransactions = indexedTransactions.filter(tx => 
      assetMap.has(normalizeTicker(tx.ticker))
    );
    
    // Build initial errors from asset resolution failures
    /** @type {Object[]} */
    const allErrors = [];
    
    for (const [ticker, errorMsg] of assetErrors) {
      // Find all transactions for this ticker to report errors
      const tickerTxs = indexedTransactions.filter(tx => 
        normalizeTicker(tx.ticker) === ticker
      );
      
      for (const tx of tickerTxs) {
        allErrors.push({
          rowNumber: tx.originalRowNumber,
          ticker,
          code: IMPORT_ERROR_CODES.ASSET_NOT_FOUND,
          message: errorMsg,
        });
      }
    }
    
    // -----------------------------------------------------------------------
    // 2. ENRICH TRANSACTIONS (AC-011 to AC-017)
    // -----------------------------------------------------------------------
    
    const { data: enrichedTransactions, errors: enrichmentErrors } = 
      await enrichTransactions(
        resolvableTransactions, 
        assetMap, 
        portfolioAccountId, 
        userId,
        defaultCurrency
      );
    
    allErrors.push(...enrichmentErrors);
    console.log(`[importTransactionBatch] Enriched: ${enrichedTransactions.length} transactions`);
    
    // -----------------------------------------------------------------------
    // 3. DETECT DUPLICATES (AC-018 to AC-021)
    // -----------------------------------------------------------------------
    
    let transactionsToWrite = enrichedTransactions;
    let duplicatesDetected = [];
    
    if (skipDuplicates) {
      const { unique, duplicates } = await detectDuplicates(enrichedTransactions, userId);
      transactionsToWrite = unique;
      duplicatesDetected = duplicates;
      
      // Add duplicate warnings (not errors since we're skipping them)
      for (const dup of duplicates) {
        allErrors.push({
          rowNumber: dup.originalRowNumber,
          ticker: dup.assetName,
          code: IMPORT_ERROR_CODES.DUPLICATE_DETECTED,
          message: `Transacción duplicada detectada y omitida: ${dup.assetName} ${dup.date} ${dup.type}`,
        });
      }
      
      console.log(`[importTransactionBatch] Duplicates: ${duplicates.length} found, ${unique.length} to import`);
    }
    
    // -----------------------------------------------------------------------
    // 4. WRITE TO FIRESTORE (AC-022 to AC-027)
    // -----------------------------------------------------------------------
    
    const { transactionIds, assetsUpdated, errors: writeErrors } = 
      await writeBatches(transactionsToWrite);
    
    allErrors.push(...writeErrors);
    console.log(`[importTransactionBatch] Written: ${transactionIds.length} transactions, ${assetsUpdated.length} assets updated`);
    
    // -----------------------------------------------------------------------
    // 5. BUILD RESPONSE (AC-031 to AC-039)
    // -----------------------------------------------------------------------
    
    const processingTimeMs = Date.now() - startTime;
    
    // Count actual errors (not duplicate warnings)
    const actualErrors = allErrors.filter(e => 
      e.code !== IMPORT_ERROR_CODES.DUPLICATE_DETECTED
    );
    
    const response = {
      success: actualErrors.length === 0,  // AC-031
      
      summary: {
        totalProcessed: transactions.length,  // AC-032
        imported: transactionIds.length,       // AC-033
        skipped: duplicatesDetected.length,    // AC-034
        errors: actualErrors.length,           // AC-035
      },
      
      assetsCreated,                           // AC-036
      assetsUpdated,                           // AC-037
      errors: allErrors,                       // AC-038
      importedTransactionIds: transactionIds,  // AC-039
      
      // Additional metadata
      processingTimeMs,
      portfolioAccountId,
    };
    
    console.log(`[importTransactionBatch] Complete in ${processingTimeMs}ms:`, response.summary);
    
    return response;
    
  } catch (error) {
    console.error('[importTransactionBatch] Unexpected error:', error);
    
    throw new HttpsError(
      'internal',
      `Error durante la importación: ${error.message}`
    );
  }
});

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Verifies the user has access to the portfolio account (AC-002, AC-003)
 * 
 * @param {string} portfolioAccountId - Account ID to verify
 * @param {string} userId - User ID to check ownership
 * @returns {Promise<Object|null>} Account data or null if not accessible
 */
async function verifyAccountAccess(portfolioAccountId, userId) {
  try {
    const accountDoc = await db.collection('portfolioAccounts')
      .doc(portfolioAccountId)
      .get();
    
    if (!accountDoc.exists) {
      console.warn(`[importTransactionBatch] Account ${portfolioAccountId} not found`);
      return null;
    }
    
    const accountData = accountDoc.data();
    
    // AC-003: Verify user owns this account
    if (accountData.userId !== userId) {
      console.warn(`[importTransactionBatch] Account ${portfolioAccountId} belongs to different user`);
      return null;
    }
    
    return {
      id: accountDoc.id,
      ...accountData,
    };
    
  } catch (error) {
    console.error(`[importTransactionBatch] Error verifying account:`, error);
    return null;
  }
}

/**
 * Groups transactions by ticker symbol
 * 
 * @param {Object[]} transactions - Transactions to group
 * @returns {Map<string, Object[]>}
 */
function groupByTicker(transactions) {
  const groups = new Map();
  
  for (const tx of transactions) {
    const ticker = normalizeTicker(tx.ticker);
    
    if (!groups.has(ticker)) {
      groups.set(ticker, []);
    }
    
    groups.get(ticker).push(tx);
  }
  
  return groups;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  importTransactionBatch,
  // Export helpers for testing
  verifyAccountAccess,
  groupByTicker,
};
