/**
 * Batch Writer Service
 * 
 * IMPORT-002: Writes transactions to Firestore using atomic batches.
 * Also updates asset units and unit values.
 * 
 * @module transactions/services/batchWriter
 * @see docs/stories/90.story.md (IMPORT-002)
 */

const admin = require('../../firebaseAdmin');
const { LIMITS, IMPORT_ERROR_CODES } = require('../types');

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
 * @property {string} currency
 * @property {number} commission
 * @property {'stock'|'etf'|'crypto'} assetType
 * @property {string} market
 * @property {number} dollarPriceToDate
 * @property {string} defaultCurrencyForAdquisitionDollar
 * @property {string} portfolioAccountId
 * @property {string} userId
 * @property {number} originalRowNumber
 */

/**
 * @typedef {Object} WriteResult
 * @property {string[]} transactionIds - Created transaction document IDs
 * @property {string[]} assetsUpdated - IDs of assets that were updated
 * @property {ImportError[]} errors - Any errors during writing
 */

/**
 * @typedef {Object} ImportError
 * @property {number} rowNumber
 * @property {string} ticker
 * @property {string} code
 * @property {string} message
 */

/**
 * @typedef {Object} AssetUpdate
 * @property {string} assetId
 * @property {number} unitsChange - Positive for buys, negative for sells
 * @property {number} totalCost - Total cost for weighted average calculation
 */

// ============================================================================
// MAIN FUNCTIONS
// ============================================================================

/**
 * Writes transactions to Firestore using batched writes.
 * Also updates asset units and weighted average unit value.
 * 
 * @param {EnrichedTransaction[]} transactions - Transactions to write
 * @returns {Promise<WriteResult>}
 */
async function writeBatches(transactions) {
  console.log(`[batchWriter] Writing ${transactions.length} transactions`);
  
  if (!transactions || transactions.length === 0) {
    return { transactionIds: [], assetsUpdated: [], errors: [] };
  }
  
  /** @type {string[]} */
  const transactionIds = [];
  /** @type {ImportError[]} */
  const errors = [];
  
  // Calculate asset updates from all transactions
  const assetUpdates = calculateAssetUpdates(transactions);
  
  // Split into chunks of BATCH_SIZE
  const chunks = splitIntoChunks(transactions, LIMITS.maxFirestoreBatch);
  console.log(`[batchWriter] Split into ${chunks.length} batches`);
  
  for (let chunkIndex = 0; chunkIndex < chunks.length; chunkIndex++) {
    const chunk = chunks[chunkIndex];
    
    try {
      const batch = db.batch();
      const chunkIds = [];
      
      for (const tx of chunk) {
        const docRef = db.collection('transactions').doc();
        
        const transactionData = {
          assetId: tx.assetId,
          assetName: tx.assetName,
          type: tx.type,
          amount: tx.amount,
          price: tx.price,
          date: tx.date,
          currency: tx.currency,
          commission: tx.commission,
          assetType: tx.assetType,
          market: tx.market,
          dollarPriceToDate: tx.dollarPriceToDate,
          defaultCurrencyForAdquisitionDollar: tx.defaultCurrencyForAdquisitionDollar,
          portfolioAccountId: tx.portfolioAccountId,
          userId: tx.userId,
          createdAt: new Date().toISOString(),  // AC-024
          importSource: 'batch_import',
        };
        
        batch.set(docRef, transactionData);
        chunkIds.push(docRef.id);
      }
      
      // Commit the batch
      await batch.commit();
      
      transactionIds.push(...chunkIds);
      console.log(`[batchWriter] Batch ${chunkIndex + 1}/${chunks.length} committed: ${chunkIds.length} transactions`);
      
    } catch (error) {
      console.error(`[batchWriter] Batch ${chunkIndex + 1} failed:`, error);
      
      // Add errors for all transactions in this chunk
      for (const tx of chunk) {
        errors.push({
          rowNumber: tx.originalRowNumber,
          ticker: tx.assetName,
          code: IMPORT_ERROR_CODES.WRITE_FAILED,
          message: `Batch write failed: ${error.message}`,
        });
      }
      
      // AC-027: Rollback is automatic - Firestore batches are atomic
      // If batch fails, nothing in that batch is written
    }
  }
  
  // Update assets after all transactions are written
  const assetsUpdated = await updateAssets(assetUpdates);
  
  console.log(`[batchWriter] Complete: ${transactionIds.length} transactions, ${assetsUpdated.length} assets updated, ${errors.length} errors`);
  
  return { transactionIds, assetsUpdated, errors };
}

/**
 * Updates asset units and weighted average unit value
 * 
 * @param {Map<string, AssetUpdate>} assetUpdates
 * @returns {Promise<string[]>} List of updated asset IDs
 */
async function updateAssets(assetUpdates) {
  console.log(`[batchWriter] Updating ${assetUpdates.size} assets`);
  
  /** @type {string[]} */
  const updated = [];
  
  for (const [assetId, update] of assetUpdates) {
    try {
      const assetRef = db.collection('assets').doc(assetId);
      const assetDoc = await assetRef.get();
      
      if (!assetDoc.exists) {
        console.warn(`[batchWriter] Asset ${assetId} not found for update`);
        continue;
      }
      
      const currentData = assetDoc.data();
      const currentUnits = currentData.units || 0;
      const currentUnitValue = currentData.unitValue || 0;
      
      // Calculate new units (AC-028)
      const newUnits = currentUnits + update.unitsChange;
      
      // Calculate new weighted average unit value for buys (AC-029)
      let newUnitValue = currentUnitValue;
      
      if (update.unitsChange > 0 && update.totalCost > 0) {
        // Weighted average: (old_units * old_price + new_units * new_price) / total_units
        const oldTotalCost = currentUnits * currentUnitValue;
        const newTotalCost = oldTotalCost + update.totalCost;
        newUnitValue = newUnits > 0 ? newTotalCost / newUnits : 0;
      }
      
      // AC-030: Mark as inactive if units reach 0
      const isActive = newUnits > 0;
      
      await assetRef.update({
        units: Math.max(0, newUnits),  // Prevent negative units
        unitValue: newUnitValue,
        isActive,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      
      updated.push(assetId);
      console.log(`[batchWriter] Updated asset ${assetId}: units ${currentUnits} -> ${newUnits}`);
      
    } catch (error) {
      console.error(`[batchWriter] Error updating asset ${assetId}:`, error);
    }
  }
  
  return updated;
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Calculates aggregated updates for each asset from transactions.
 * Aggregates all buys and sells to compute net unit change.
 * 
 * @param {EnrichedTransaction[]} transactions
 * @returns {Map<string, AssetUpdate>}
 */
function calculateAssetUpdates(transactions) {
  /** @type {Map<string, AssetUpdate>} */
  const updates = new Map();
  
  for (const tx of transactions) {
    const assetId = tx.assetId;
    
    if (!updates.has(assetId)) {
      updates.set(assetId, {
        assetId,
        unitsChange: 0,
        totalCost: 0,
      });
    }
    
    const update = updates.get(assetId);
    
    if (tx.type === 'buy') {
      update.unitsChange += tx.amount;
      update.totalCost += tx.amount * tx.price;
    } else if (tx.type === 'sell') {
      update.unitsChange -= tx.amount;
      // Don't add to totalCost for sells (doesn't affect avg cost)
    }
  }
  
  return updates;
}

/**
 * Splits an array into chunks of specified size.
 * 
 * @param {any[]} array - Array to split
 * @param {number} chunkSize - Maximum chunk size
 * @returns {any[][]}
 */
function splitIntoChunks(array, chunkSize) {
  const chunks = [];
  
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  
  return chunks;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  writeBatches,
  updateAssets,
  calculateAssetUpdates,
  splitIntoChunks,
};
