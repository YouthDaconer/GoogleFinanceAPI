/**
 * Cloud Function: analyzeTransactionFile
 * 
 * Analyzes a sample of an uploaded transaction file and returns
 * automatic column mappings with confidence levels.
 * 
 * Features:
 * - Broker format detection (IBKR, TD Ameritrade, Fidelity, eToro)
 * - Generic column detection by headers and content
 * - Ticker validation against market data API
 * - Confidence scoring and feedback generation
 * 
 * @module transactions/analyzeTransactionFile
 * @see docs/stories/89.story.md (IMPORT-001)
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");

// Import services
const { detectBrokerFormat, getBrokerMappings } = require('./services/brokerPatterns');
const { detectColumnsGeneric, detectHasHeader } = require('./services/columnDetector');
const { validateTickerSample } = require('./services/tickerValidator');
const { calculateOverallConfidence, generateFeedback, evaluateReadiness } = require('./services/confidenceCalculator');
const { REQUIRED_FIELDS, LIMITS } = require('./types');

// ============================================================================
// SECRET DEFINITIONS
// ============================================================================

// SEC-TOKEN-003: Secret for service-to-service authentication with finance-query API
const cfServiceToken = defineSecret("CF_SERVICE_TOKEN");

// ============================================================================
// CLOUD FUNCTION CONFIGURATION
// ============================================================================

/**
 * Function configuration optimized for analysis workload
 */
const FUNCTION_CONFIG = {
  cors: true,
  memory: "256MiB",
  timeoutSeconds: 60,     // Increased for ticker validation API calls
  maxInstances: 20,
  minInstances: 0,
  region: 'us-central1',
  secrets: [cfServiceToken],  // SEC-TOKEN-003: Bind secret for API authentication
};

// ============================================================================
// MAIN CLOUD FUNCTION
// ============================================================================

/**
 * Analyzes transaction file sample and returns column mappings
 * 
 * @param {Object} request - Cloud Function request
 * @param {Object} request.data - Request payload
 * @param {string[][]} request.data.sampleData - Sample rows from file (max 100)
 * @param {string} request.data.fileName - Original filename
 * @param {boolean} request.data.hasHeader - Whether first row is header
 * @param {Object} request.auth - Authentication context
 * @returns {Object} Analysis response with mappings and confidence
 * 
 * @example
 * const result = await analyzeTransactionFile({
 *   sampleData: [
 *     ['Symbol', 'Action', 'Qty', 'Price', 'Date'],
 *     ['AAPL', 'Buy', '10', '150.50', '2024-01-15'],
 *   ],
 *   fileName: 'trades_2024.xlsx',
 *   hasHeader: true
 * });
 */
const analyzeTransactionFile = onCall(
  FUNCTION_CONFIG,
  async (request) => {
    const startTime = Date.now();
    const { auth, data } = request;
    
    // ─────────────────────────────────────────────────────────────────────
    // 1. AUTHENTICATION (AC-001, AC-002)
    // ─────────────────────────────────────────────────────────────────────
    if (!auth) {
      throw new HttpsError(
        'unauthenticated',
        'Usuario debe estar autenticado para analizar archivos'
      );
    }
    
    const userId = auth.uid;
    console.log(`[analyzeTransactionFile] Start - userId: ${userId}, file: ${data?.fileName}`);
    
    // ─────────────────────────────────────────────────────────────────────
    // 2. PAYLOAD VALIDATION (AC-003, AC-004)
    // ─────────────────────────────────────────────────────────────────────
    const { sampleData, fileName, hasHeader: providedHasHeader } = data || {};
    
    // Validate sampleData
    if (!sampleData || !Array.isArray(sampleData)) {
      throw new HttpsError(
        'invalid-argument',
        'sampleData debe ser un array de arrays'
      );
    }
    
    if (sampleData.length === 0) {
      throw new HttpsError(
        'invalid-argument',
        'El archivo está vacío'
      );
    }
    
    // Check payload size (rough estimate)
    const payloadSize = JSON.stringify(sampleData).length;
    if (payloadSize > LIMITS.maxPayloadSize) {
      throw new HttpsError(
        'invalid-argument',
        `El payload excede el límite de ${LIMITS.maxPayloadSize / 1024}KB`
      );
    }
    
    // Truncate to max rows
    let truncatedData = sampleData;
    if (sampleData.length > LIMITS.maxSampleRows) {
      console.log(`[analyzeTransactionFile] Truncating from ${sampleData.length} to ${LIMITS.maxSampleRows} rows`);
      truncatedData = sampleData.slice(0, LIMITS.maxSampleRows);
    }
    
    // Auto-detect header if not provided
    const hasHeader = providedHasHeader !== undefined 
      ? providedHasHeader 
      : detectHasHeader(truncatedData);
    
    console.log(`[analyzeTransactionFile] Rows: ${truncatedData.length}, hasHeader: ${hasHeader}`);
    
    // ─────────────────────────────────────────────────────────────────────
    // 3. BROKER DETECTION (AC-005 to AC-009)
    // ─────────────────────────────────────────────────────────────────────
    const headers = hasHeader ? truncatedData[0] : null;
    const detectedBroker = detectBrokerFormat(headers, fileName);
    
    console.log(`[analyzeTransactionFile] Detected broker: ${detectedBroker || 'generic'}`);
    
    // ─────────────────────────────────────────────────────────────────────
    // 4. COLUMN DETECTION (AC-010 to AC-021)
    // ─────────────────────────────────────────────────────────────────────
    let mappings = [];
    
    if (detectedBroker) {
      // Use pre-defined broker mappings
      mappings = getBrokerMappings(detectedBroker, truncatedData, hasHeader);
      console.log(`[analyzeTransactionFile] Broker mappings: ${mappings.length} columns`);
    }
    
    // If broker detection didn't map all required fields, fall back to generic
    const mappedFields = new Set(mappings.map(m => m.targetField));
    const missingFromBroker = REQUIRED_FIELDS.filter(f => !mappedFields.has(f));
    
    if (!detectedBroker || missingFromBroker.length > 0) {
      const genericMappings = detectColumnsGeneric(truncatedData, hasHeader);
      
      // Merge: prefer broker mappings, add generic for unmapped columns
      const mappedColumns = new Set(mappings.map(m => m.sourceColumn));
      
      for (const genericMapping of genericMappings) {
        if (!mappedColumns.has(genericMapping.sourceColumn) &&
            !mappedFields.has(genericMapping.targetField)) {
          mappings.push(genericMapping);
          mappedColumns.add(genericMapping.sourceColumn);
          mappedFields.add(genericMapping.targetField);
        }
      }
      
      console.log(`[analyzeTransactionFile] After generic: ${mappings.length} columns`);
    }
    
    // ─────────────────────────────────────────────────────────────────────
    // 5. IDENTIFY UNMAPPED COLUMNS AND MISSING FIELDS
    // ─────────────────────────────────────────────────────────────────────
    const totalColumns = truncatedData[0]?.length || 0;
    const mappedColumnIndices = new Set(mappings.map(m => m.sourceColumn));
    const unmappedColumns = Array.from(
      { length: totalColumns }, 
      (_, i) => i
    ).filter(i => !mappedColumnIndices.has(i));
    
    const finalMappedFields = new Set(mappings.map(m => m.targetField));
    const missingRequiredFields = REQUIRED_FIELDS.filter(f => !finalMappedFields.has(f));
    
    console.log(`[analyzeTransactionFile] Unmapped columns: ${unmappedColumns.length}, Missing required: ${missingRequiredFields.length}`);
    
    // ─────────────────────────────────────────────────────────────────────
    // 6. TICKER VALIDATION (AC-022 to AC-026)
    // ─────────────────────────────────────────────────────────────────────
    const tickerMapping = mappings.find(m => m.targetField === 'ticker');
    let tickerValidation = {
      total: 0,
      valid: 0,
      invalid: 0,
      invalidTickers: [],
      suggestions: {},
      details: {},
    };
    
    if (tickerMapping) {
      const dataStartRow = hasHeader ? 1 : 0;
      const tickerColumnIndex = tickerMapping.sourceColumn;
      
      const tickerSample = truncatedData
        .slice(dataStartRow)
        .map(row => row[tickerColumnIndex])
        .filter(Boolean);
      
      if (tickerSample.length > 0) {
        console.log(`[analyzeTransactionFile] Validating ${tickerSample.length} ticker samples`);
        tickerValidation = await validateTickerSample(tickerSample);
      }
    } else {
      console.log(`[analyzeTransactionFile] No ticker column mapped - skipping validation`);
    }
    
    // ─────────────────────────────────────────────────────────────────────
    // 7. CALCULATE CONFIDENCE (AC-027 to AC-030)
    // ─────────────────────────────────────────────────────────────────────
    const overallConfidence = calculateOverallConfidence(
      mappings,
      missingRequiredFields,
      tickerValidation,
      detectedBroker
    );
    
    console.log(`[analyzeTransactionFile] Overall confidence: ${overallConfidence}`);
    
    // ─────────────────────────────────────────────────────────────────────
    // 8. GENERATE FEEDBACK
    // ─────────────────────────────────────────────────────────────────────
    const { warnings, suggestions } = generateFeedback(
      mappings,
      missingRequiredFields,
      tickerValidation,
      detectedBroker
    );
    
    const readiness = evaluateReadiness(overallConfidence, missingRequiredFields);
    
    // ─────────────────────────────────────────────────────────────────────
    // 9. DETECT DATE FORMAT
    // ─────────────────────────────────────────────────────────────────────
    const dateMapping = mappings.find(m => m.targetField === 'date');
    const detectedDateFormat = dateMapping?.detectedFormat || 
                               extractDateFormat(dateMapping?.sampleValues);
    
    // ─────────────────────────────────────────────────────────────────────
    // 10. BUILD RESPONSE (AC-031 to AC-039)
    // ─────────────────────────────────────────────────────────────────────
    const duration = Date.now() - startTime;
    console.log(`[analyzeTransactionFile] Complete - duration: ${duration}ms`);
    
    // AC-040: Ensure response within time limit
    if (duration > 3000) {
      console.warn(`[analyzeTransactionFile] Response time exceeded target: ${duration}ms > 3000ms`);
    }
    
    const response = {
      success: true,
      
      // AC-032: Detected broker
      detectedBroker,
      
      // AC-033: Column mappings
      mappings: mappings.map(m => ({
        sourceColumn: m.sourceColumn,
        sourceHeader: m.sourceHeader,
        targetField: m.targetField,
        confidence: m.confidence,
        detectionMethod: m.detectionMethod,
        sampleValues: m.sampleValues,
        transformation: m.transformation,
      })),
      
      // AC-034: Unmapped columns
      unmappedColumns,
      
      // AC-035: Missing required fields
      missingRequiredFields,
      
      // AC-036: Overall confidence
      overallConfidence,
      
      // AC-037: Warnings
      warnings,
      
      // AC-038: Suggestions
      suggestions,
      
      // AC-039: Ticker validation
      tickerValidation: {
        total: tickerValidation.total,
        valid: tickerValidation.valid,
        invalid: tickerValidation.invalid,
        invalidTickers: tickerValidation.invalidTickers.slice(0, 10), // Limit for response size
        suggestions: tickerValidation.suggestions,
      },
      
      // Additional metadata
      detectedDateFormat,
      hasHeader,
      totalRows: truncatedData.length,
      totalColumns,
      
      // Readiness assessment
      readiness,
      
      // Performance info
      processingTimeMs: duration,
    };
    
    return response;
  }
);

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Extracts date format from sample values
 * 
 * @param {string[]} sampleValues - Sample date values
 * @returns {string|null} Detected date format
 */
function extractDateFormat(sampleValues) {
  if (!sampleValues || sampleValues.length === 0) {
    return null;
  }
  
  const sample = sampleValues[0];
  
  // ISO format
  if (/^\d{4}-\d{2}-\d{2}/.test(sample)) {
    return 'YYYY-MM-DD';
  }
  
  // US format with slashes
  if (/^\d{1,2}\/\d{1,2}\/\d{4}/.test(sample)) {
    // Try to distinguish MM/DD from DD/MM
    const parts = sample.split('/');
    const firstPart = parseInt(parts[0], 10);
    const secondPart = parseInt(parts[1], 10);
    
    if (firstPart > 12) {
      return 'DD/MM/YYYY';
    } else if (secondPart > 12) {
      return 'MM/DD/YYYY';
    }
    // Ambiguous - assume US format
    return 'MM/DD/YYYY';
  }
  
  // EU format with dashes
  if (/^\d{1,2}-\d{1,2}-\d{4}/.test(sample)) {
    return 'DD-MM-YYYY';
  }
  
  // Text month format
  if (/^[A-Za-z]{3}\s+\d{1,2}/.test(sample)) {
    return 'MMM DD, YYYY';
  }
  
  return null;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  analyzeTransactionFile,
};
