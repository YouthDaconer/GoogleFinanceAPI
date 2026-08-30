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

// GATE-006: Validación de features por plan
const { validateFeatureAccess } = require('../helpers/subscriptionValidator');

// Import services
const {
  detectBrokerFormat,
  getBrokerMappings,
  // HU 1.5: formato numérico declarado del broker
  getBrokerNumberFormat,
  // IMPORT-004: formato numérico inferido del contenido (verifica la declaración)
  inferNumberFormatFromValues,
} = require('./services/brokerPatterns');
const { detectColumnsGeneric, detectHasHeader } = require('./services/columnDetector');
const { validateTickerSample } = require('./services/tickerValidator');
const { calculateOverallConfidence, generateFeedback, evaluateReadiness } = require('./services/confidenceCalculator');
// HU 1.1: memoria del mapeo confirmado (perfil de importación recordado)
const { buildSourceFormatId } = require('./services/formatFingerprint');
const { getProfile, isProfileStillValid } = require('./services/importMemoryRepository');
// HU 1.2: memoria de equivalencias de símbolo
const { resolveSymbols } = require('./services/symbolEquivalenceResolver');
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

    // GATE-006: Validar acceso a import según plan
    await validateFeatureAccess(userId, 'hasImport');
    
    // ─────────────────────────────────────────────────────────────────────
    // 2. PAYLOAD VALIDATION (AC-003, AC-004)
    // ─────────────────────────────────────────────────────────────────────
    const { 
      sampleData, 
      fileName, 
      hasHeader: providedHasHeader,
      uniqueTickers: providedUniqueTickers  // NEW: All unique tickers from full file
    } = data || {};
    
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
    // 3b. FORMAT IDENTITY + REMEMBERED MAPPING (HU 1.1)
    // ─────────────────────────────────────────────────────────────────────
    const columnCount = truncatedData[0]?.length || 0;
    const sourceFormatId = buildSourceFormatId({ detectedBroker, headers, columnCount });

    console.log(`[analyzeTransactionFile] Source format: ${sourceFormatId}`);

    // RN-04: un perfil que dejó de coincidir se descarta EN SILENCIO. No se añade
    // warning ni suggestion a la respuesta, para no exponer terminología interna.
    const storedProfile = await getProfile(userId, sourceFormatId);
    const profileIsUsable = isProfileStillValid(storedProfile, headers, columnCount);

    if (storedProfile && !profileIsUsable) {
      console.log('[analyzeTransactionFile] Stored profile no longer matches - falling back to detection');
    }

    let rememberedMapping = null;

    // ─────────────────────────────────────────────────────────────────────
    // 4. COLUMN DETECTION (AC-010 to AC-021)
    // ─────────────────────────────────────────────────────────────────────
    let mappings = [];

    if (profileIsUsable) {
      // El mapeo confirmado por el usuario reemplaza a la detección automática.
      // Sigue siendo editable en el wizard y no se importa nada sin confirmación (RN-01).
      mappings = hydrateRememberedMappings(storedProfile, truncatedData, hasHeader);

      rememberedMapping = {
        matched: true,
        defaultValues: storedProfile.defaultValues || null,
        confirmedImportCount: storedProfile.confirmedImportCount || 1,
      };

      console.log(`[analyzeTransactionFile] Applied remembered mapping: ${mappings.length} columns`);
    } else if (detectedBroker) {
      // Use pre-defined broker mappings
      mappings = getBrokerMappings(detectedBroker, truncatedData, hasHeader);
      console.log(`[analyzeTransactionFile] Broker mappings: ${mappings.length} columns`);
    }
    
    // If detection didn't map all required fields, fall back to generic
    const mappedFields = new Set(mappings.map(m => m.targetField));
    const missingFromDetection = REQUIRED_FIELDS.filter(f => !mappedFields.has(f));

    // HU 1.1: con un mapeo recordado completo NO se ejecuta la detección genérica.
    // Añadir campos que el usuario había dejado sin asignar contradiría "el asistente
    // presenta el mapeo ya resuelto" y reintroduciría decisiones que él ya tomó.
    const needsGenericFallback = profileIsUsable
      ? missingFromDetection.length > 0
      : (!detectedBroker || missingFromDetection.length > 0);

    if (needsGenericFallback) {
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
    const totalColumns = columnCount;
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
    // Uses /v1/quotes for batch validation of ALL unique tickers
    // ─────────────────────────────────────────────────────────────────────
    const tickerMapping = mappings.find(m => m.targetField === 'ticker');
    let tickerValidation = {
      total: 0,
      valid: 0,
      invalid: 0,
      unverified: 0,
      invalidTickers: [],
      unverifiedTickers: [],
      suggestions: {},
      details: {},
      validDetails: {},
    };

    // HU 1.2: símbolos que llegaron resueltos desde la memoria del usuario
    let equivalences = {};

    if (tickerMapping) {
      // Determine which tickers to validate:
      // 1. If frontend provided uniqueTickers (all from full file), use those
      // 2. Otherwise, extract from sampleData (backward compatibility)
      let tickersToValidate;
      
      if (providedUniqueTickers && Array.isArray(providedUniqueTickers) && providedUniqueTickers.length > 0) {
        // Frontend sent all unique tickers from the complete file
        tickersToValidate = providedUniqueTickers;
        console.log(`[analyzeTransactionFile] Using ${tickersToValidate.length} tickers from frontend`);
      } else {
        // Extract from sample data (legacy behavior)
        const dataStartRow = hasHeader ? 1 : 0;
        const tickerColumnIndex = tickerMapping.sourceColumn;
        
        tickersToValidate = truncatedData
          .slice(dataStartRow)
          .map(row => row[tickerColumnIndex])
          .filter(Boolean);
        console.log(`[analyzeTransactionFile] Extracted ${tickersToValidate.length} tickers from sample`);
      }
      
      if (tickersToValidate.length > 0) {
        console.log(`[analyzeTransactionFile] Validating tickers using /quotes...`);

        // HU 1.2: los símbolos con equivalencia recordada se validan por su ticker
        // canónico, no por el texto del archivo. Así el escenario 7 (activo que
        // dejó de existir) se detecta sin lógica adicional.
        const resolution = await resolveSymbols({
          userId,
          sourceFormatId,
          symbols: tickersToValidate,
          validate: validateTickerSample,
        });

        tickerValidation = resolution.tickerValidation;
        equivalences = resolution.equivalences;

        console.log(`[analyzeTransactionFile] Equivalences applied: ${Object.keys(equivalences).length}`);
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
    // IMPORT-004: verificar la declaración del broker contra el contenido real.
    // Un reporte genérico en español puede coincidir al 80% con la firma de
    // Trii y heredar su formato 'eu'; si el contenido usa puntos decimales,
    // esa declaración corrompe cada cantidad ("101.70" → 10170).
    const declaredNumberFormat = getBrokerNumberFormat(detectedBroker);
    let detectedNumberFormat = declaredNumberFormat;
    try {
      const numericMappings = mappings.filter(m =>
        m.targetField === 'amount' || m.targetField === 'price' || m.targetField === 'total'
      );
      if (numericMappings.length > 0) {
        const numericColumns = numericMappings.map(m => m.sourceColumn);
        const dataStartRow = hasHeader ? 1 : 0;
        const sampleValues = [];

        for (const row of truncatedData.slice(dataStartRow)) {
          for (const col of numericColumns) {
            const value = row?.[col];
            if (value) {
              sampleValues.push(String(value));
            }
          }
          if (sampleValues.length >= 400) break;
        }

        const inferredFormat = inferNumberFormatFromValues(sampleValues);

        if (inferredFormat && inferredFormat !== declaredNumberFormat) {
          detectedNumberFormat = inferredFormat;
          console.warn(
            `[analyzeTransactionFile] declared number format "${declaredNumberFormat}" ` +
            `(broker ${detectedBroker || 'generic'}) contradicted by content evidence "${inferredFormat}" — using content`
          );
          warnings.push(
            'El formato numérico del archivo difiere del esperado para este formato; se usó el formato detectado en los datos.'
          );
        }
      }
    } catch (e) {
      // La verificación es defensiva: cualquier fallo conserva la declaración
      console.warn(`[analyzeTransactionFile] Number format verification failed: ${e.message}`);
    }

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

      // HU 1.1: identidad del formato y mapeo recordado.
      // rememberedMapping = null cubre tanto "sin memoria previa" (RN-11) como
      // "el perfil dejó de coincidir" (RN-04), sin distinguirlos para el usuario.
      sourceFormatId,
      rememberedMapping,

      // HU 1.2: símbolos que llegaron ya resueltos desde la memoria del usuario.
      // Mapa símbolo del archivo → activo al que quedó vinculado, con su origen.
      equivalences,

      // HU 1.5: separador decimal del archivo, declarado por el broker detectado
      // y verificado contra el contenido real (IMPORT-004). El frontend lo usa
      // al parsear cantidades y precios: sin esto, un importe europeo como
      // "1.234,56" se leería como 1.23456 sin lanzar ningún error.
      detectedNumberFormat,


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
 * Reconstruye los ColumnMapping completos a partir de un perfil recordado (HU 1.1).
 *
 * El perfil solo persiste columna → campo. Los valores de muestra son de la sesión
 * actual, así que se extraen del archivo que el usuario acaba de cargar para que el
 * paso de mapeo muestre datos reales y verificables.
 *
 * detectionMethod 'remembered' con confianza 1.0: el usuario ya confirmó este mapeo,
 * así que no hay incertidumbre que comunicar.
 *
 * @param {Object} profile - Perfil almacenado
 * @param {string[][]} sampleData - Muestra del archivo actual
 * @param {boolean} hasHeader - Si la primera fila es cabecera
 * @returns {Object[]} Mappings listos para el wizard
 */
function hydrateRememberedMappings(profile, sampleData, hasHeader) {
  const dataRows = hasHeader ? sampleData.slice(1) : sampleData;

  return profile.mappings.map((mapping) => {
    const sampleValues = dataRows
      .slice(0, 5)
      .map(row => String(row[mapping.sourceColumn] ?? ''))
      .filter(v => v.length > 0);

    return {
      sourceColumn: mapping.sourceColumn,
      sourceHeader: mapping.sourceHeader,
      targetField: mapping.targetField,
      confidence: 1.0,
      detectionMethod: 'remembered',
      sampleValues,
      transformation: undefined,
    };
  });
}

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

  // For testing
  hydrateRememberedMappings,
};
