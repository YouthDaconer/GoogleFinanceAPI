/**
 * Transaction Import Module - Index
 * 
 * Exports all Cloud Functions for transaction import feature.
 * 
 * Functions:
 * - analyzeTransactionFile: Analyzes file and returns column mappings (IMPORT-001)
 * - importTransactionBatch: Imports validated transactions to Firestore (IMPORT-002)
 * 
 * @module transactions
 * @see docs/stories/89.story.md (IMPORT-001)
 * @see docs/stories/90.story.md (IMPORT-002)
 * @see docs/architecture/FEAT-IMPORT-001-smart-transaction-import-design.md
 */

const { analyzeTransactionFile } = require('./analyzeTransactionFile');
const { importTransactionBatch } = require('./importTransactionBatch');

// Re-export types for consumers
const types = require('./types');

// Services (for testing)
const assetResolver = require('./services/assetResolver');
const transactionEnricher = require('./services/transactionEnricher');
const duplicateDetector = require('./services/duplicateDetector');
const batchWriter = require('./services/batchWriter');

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Cloud Functions
  analyzeTransactionFile,
  importTransactionBatch,
  
  // Types and constants (for testing and documentation)
  types,
  
  // Services (for testing)
  services: {
    assetResolver,
    transactionEnricher,
    duplicateDetector,
    batchWriter,
  },
};