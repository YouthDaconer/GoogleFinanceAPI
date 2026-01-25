/**
 * Generic Column Detection Service
 * 
 * Detects column mappings for files that don't match known broker patterns.
 * Uses a combination of header pattern matching and content analysis.
 * 
 * @module transactions/services/columnDetector
 * @see docs/stories/89.story.md (IMPORT-001)
 */

const { 
  HEADER_PATTERNS, 
  CONTENT_PATTERNS, 
  REQUIRED_FIELDS,
  DETECTION_CONFIDENCE,
  LIMITS,
} = require('../types');

// ============================================================================
// MAIN DETECTION FUNCTION
// ============================================================================

/**
 * Detects column mappings using generic pattern matching
 * 
 * Strategy:
 * 1. If hasHeader, analyze headers first (high confidence)
 * 2. Analyze content patterns for unmapped columns (medium confidence)
 * 3. Use context clues for remaining columns (low confidence)
 * 
 * @param {string[][]} sampleData - Sample rows from file
 * @param {boolean} hasHeader - Whether first row contains headers
 * @returns {import('../types').ColumnMapping[]} Detected column mappings
 */
function detectColumnsGeneric(sampleData, hasHeader) {
  if (!sampleData || sampleData.length === 0) {
    return [];
  }
  
  const headers = hasHeader ? sampleData[0] : null;
  const dataRows = hasHeader ? sampleData.slice(1) : sampleData;
  const columnCount = sampleData[0]?.length || 0;
  
  const mappings = [];
  const mappedColumns = new Set();
  const mappedFields = new Set();
  
  // Phase 1: Header-based detection (highest confidence)
  if (headers) {
    const headerMappings = detectByHeaders(headers, dataRows);
    for (const mapping of headerMappings) {
      if (!mappedFields.has(mapping.targetField)) {
        mappings.push(mapping);
        mappedColumns.add(mapping.sourceColumn);
        mappedFields.add(mapping.targetField);
      }
    }
  }
  
  // Phase 2: Content-based detection for unmapped columns
  for (let colIndex = 0; colIndex < columnCount; colIndex++) {
    if (mappedColumns.has(colIndex)) continue;
    
    const columnValues = dataRows
      .slice(0, 20) // Analyze first 20 rows
      .map(row => String(row[colIndex] || '').trim())
      .filter(v => v.length > 0);
    
    if (columnValues.length === 0) continue;
    
    const contentMapping = detectByContent(
      colIndex, 
      headers?.[colIndex] || `Column ${colIndex + 1}`,
      columnValues,
      mappedFields
    );
    
    if (contentMapping) {
      mappings.push(contentMapping);
      mappedColumns.add(contentMapping.sourceColumn);
      mappedFields.add(contentMapping.targetField);
    }
  }
  
  // Phase 3: Context-based detection for remaining required fields
  for (const requiredField of REQUIRED_FIELDS) {
    if (mappedFields.has(requiredField)) continue;
    
    const contextMapping = detectByContext(
      requiredField,
      sampleData,
      hasHeader,
      mappedColumns
    );
    
    if (contextMapping) {
      mappings.push(contextMapping);
      mappedColumns.add(contextMapping.sourceColumn);
      mappedFields.add(contextMapping.targetField);
    }
  }
  
  return mappings;
}

// ============================================================================
// HEADER-BASED DETECTION (AC-010 to AC-016)
// ============================================================================

/**
 * Detects mappings based on header names
 * 
 * @param {string[]} headers - Column headers
 * @param {string[][]} dataRows - Data rows (for sample values)
 * @returns {import('../types').ColumnMapping[]} Header-based mappings
 */
function detectByHeaders(headers, dataRows) {
  const mappings = [];
  
  headers.forEach((header, columnIndex) => {
    const normalizedHeader = String(header || '').trim();
    if (!normalizedHeader) return;
    
    // Try each field pattern
    for (const [field, config] of Object.entries(HEADER_PATTERNS)) {
      for (const pattern of config.patterns) {
        if (pattern.test(normalizedHeader)) {
          // Extract sample values
          const sampleValues = dataRows
            .slice(0, LIMITS.maxSampleValuesPerColumn)
            .map(row => String(row[columnIndex] || ''))
            .filter(v => v.length > 0);
          
          mappings.push({
            sourceColumn: columnIndex,
            sourceHeader: normalizedHeader,
            targetField: field,
            confidence: DETECTION_CONFIDENCE.header,
            detectionMethod: 'header',
            sampleValues,
            transformation: getTransformationHint(field),
          });
          
          return; // Found match for this column, move to next
        }
      }
    }
  });
  
  return mappings;
}

// ============================================================================
// CONTENT-BASED DETECTION (AC-017 to AC-021)
// ============================================================================

/**
 * Detects mapping based on column content analysis
 * 
 * @param {number} columnIndex - Column index
 * @param {string} header - Column header (if any)
 * @param {string[]} values - Column values
 * @param {Set<string>} mappedFields - Already mapped fields
 * @returns {import('../types').ColumnMapping|null} Detected mapping or null
 */
function detectByContent(columnIndex, header, values, mappedFields) {
  const analysis = analyzeColumnContent(values);
  
  // Priority order for content detection
  const detectionOrder = [
    { field: 'ticker', check: () => analysis.isLikelyTicker && !mappedFields.has('ticker') },
    { field: 'type', check: () => analysis.isLikelyType && !mappedFields.has('type') },
    { field: 'date', check: () => analysis.isLikelyDate && !mappedFields.has('date') },
    { field: 'currency', check: () => analysis.isLikelyCurrency && !mappedFields.has('currency') },
    { field: 'amount', check: () => analysis.isLikelyAmount && !mappedFields.has('amount') },
    { field: 'price', check: () => analysis.isLikelyPrice && !mappedFields.has('price') },
    { field: 'commission', check: () => analysis.isLikelyCommission && !mappedFields.has('commission') },
  ];
  
  for (const detection of detectionOrder) {
    if (detection.check()) {
      return {
        sourceColumn: columnIndex,
        sourceHeader: header,
        targetField: detection.field,
        confidence: DETECTION_CONFIDENCE.content * analysis.confidence,
        detectionMethod: 'content',
        sampleValues: values.slice(0, LIMITS.maxSampleValuesPerColumn),
        transformation: getTransformationHint(detection.field),
        detectedFormat: analysis.detectedFormat,
      };
    }
  }
  
  return null;
}

/**
 * Analyzes column values to determine likely field type
 * 
 * @param {string[]} values - Column values to analyze
 * @returns {Object} Analysis result with likelihood flags
 */
function analyzeColumnContent(values) {
  const result = {
    isLikelyTicker: false,
    isLikelyType: false,
    isLikelyDate: false,
    isLikelyCurrency: false,
    isLikelyAmount: false,
    isLikelyPrice: false,
    isLikelyCommission: false,
    confidence: 0,
    detectedFormat: null,
  };
  
  if (values.length === 0) return result;
  
  // Count matches for each pattern
  let tickerMatches = 0;
  let typeMatches = 0;
  let dateMatches = 0;
  let currencyMatches = 0;
  let numberMatches = 0;
  let detectedDateFormat = null;
  
  for (const value of values) {
    const normalized = value.trim();
    
    // Check ticker pattern
    if (CONTENT_PATTERNS.ticker.test(normalized.toUpperCase())) {
      tickerMatches++;
    }
    
    // Check type pattern
    if (CONTENT_PATTERNS.type.buy.test(normalized) || 
        CONTENT_PATTERNS.type.sell.test(normalized)) {
      typeMatches++;
    }
    
    // Check date patterns
    for (const [formatName, pattern] of Object.entries(CONTENT_PATTERNS.date)) {
      if (pattern.test(normalized)) {
        dateMatches++;
        detectedDateFormat = formatName;
        break;
      }
    }
    
    // Check currency pattern
    if (CONTENT_PATTERNS.currency.test(normalized.toUpperCase())) {
      currencyMatches++;
    }
    
    // Check number pattern
    if (CONTENT_PATTERNS.number.test(normalized.replace(/[,$]/g, ''))) {
      numberMatches++;
    }
  }
  
  const total = values.length;
  const threshold = 0.7; // 70% match required
  
  // Determine most likely type based on match ratios
  if (tickerMatches / total >= threshold) {
    result.isLikelyTicker = true;
    result.confidence = tickerMatches / total;
  } else if (typeMatches / total >= threshold) {
    result.isLikelyType = true;
    result.confidence = typeMatches / total;
  } else if (dateMatches / total >= threshold) {
    result.isLikelyDate = true;
    result.confidence = dateMatches / total;
    result.detectedFormat = detectedDateFormat;
  } else if (currencyMatches / total >= threshold) {
    result.isLikelyCurrency = true;
    result.confidence = currencyMatches / total;
  } else if (numberMatches / total >= threshold) {
    // For numbers, need to distinguish amount, price, commission
    const numericAnalysis = analyzeNumericColumn(values);
    if (numericAnalysis.likelyField) {
      result[`isLikely${capitalize(numericAnalysis.likelyField)}`] = true;
      result.confidence = numericAnalysis.confidence;
    }
  }
  
  return result;
}

/**
 * Analyzes numeric column to distinguish between amount, price, commission
 * 
 * @param {string[]} values - Numeric values
 * @returns {Object} Analysis with likely field
 */
function analyzeNumericColumn(values) {
  const numbers = values
    .map(v => parseFloat(v.replace(/[,$]/g, '')))
    .filter(n => !isNaN(n));
  
  if (numbers.length === 0) {
    return { likelyField: null, confidence: 0 };
  }
  
  // Statistical analysis
  const min = Math.min(...numbers);
  const max = Math.max(...numbers);
  const avg = numbers.reduce((a, b) => a + b, 0) / numbers.length;
  const hasDecimals = numbers.some(n => n % 1 !== 0);
  const allPositive = numbers.every(n => n >= 0);
  const hasNegative = numbers.some(n => n < 0);
  
  // Heuristics:
  // - Commission: usually small positive numbers (< 100)
  // - Amount: often integers or simple decimals, can be small
  // - Price: usually has decimals, can be high
  
  // Commission check: all small positive numbers
  if (allPositive && max < 100 && avg < 20) {
    return { likelyField: 'commission', confidence: 0.6 };
  }
  
  // Amount check: often whole numbers or has negatives (sells)
  if (!hasDecimals || (hasDecimals && max < 10000)) {
    // If most values are integers, likely amount
    const integerCount = numbers.filter(n => n % 1 === 0).length;
    if (integerCount / numbers.length > 0.7) {
      return { likelyField: 'amount', confidence: 0.6 };
    }
  }
  
  // Price check: typically has 2 decimal places, positive
  if (hasDecimals && allPositive && avg > 1) {
    return { likelyField: 'price', confidence: 0.5 };
  }
  
  // Default to amount if can't determine
  return { likelyField: 'amount', confidence: 0.4 };
}

// ============================================================================
// CONTEXT-BASED DETECTION
// ============================================================================

/**
 * Uses context clues to detect remaining required fields
 * 
 * @param {string} targetField - Field to find
 * @param {string[][]} sampleData - All sample data
 * @param {boolean} hasHeader - Whether file has headers
 * @param {Set<number>} mappedColumns - Already mapped column indices
 * @returns {import('../types').ColumnMapping|null} Detected mapping or null
 */
function detectByContext(targetField, sampleData, hasHeader, mappedColumns) {
  const headers = hasHeader ? sampleData[0] : null;
  const dataRows = hasHeader ? sampleData.slice(1) : sampleData;
  const columnCount = sampleData[0]?.length || 0;
  
  // Find unmapped columns
  const unmappedIndices = [];
  for (let i = 0; i < columnCount; i++) {
    if (!mappedColumns.has(i)) {
      unmappedIndices.push(i);
    }
  }
  
  if (unmappedIndices.length === 0) return null;
  
  // Try each unmapped column
  for (const colIndex of unmappedIndices) {
    const values = dataRows
      .slice(0, 10)
      .map(row => String(row[colIndex] || '').trim())
      .filter(v => v.length > 0);
    
    if (values.length === 0) continue;
    
    let matches = false;
    
    switch (targetField) {
      case 'ticker':
        // Any column with uppercase short strings
        matches = values.every(v => 
          v.length <= 6 && /^[A-Za-z]+/.test(v)
        );
        break;
        
      case 'type':
        // Look for columns with exactly 2 distinct values
        const uniqueValues = [...new Set(values.map(v => v.toLowerCase()))];
        matches = uniqueValues.length === 2;
        break;
        
      case 'date':
        // Any column with slashes or dashes in consistent format
        matches = values.every(v => 
          (v.includes('/') || v.includes('-')) && v.length >= 8
        );
        break;
        
      case 'amount':
      case 'price':
        // Numeric columns
        matches = values.every(v => 
          !isNaN(parseFloat(v.replace(/[,$]/g, '')))
        );
        break;
    }
    
    if (matches) {
      return {
        sourceColumn: colIndex,
        sourceHeader: headers?.[colIndex] || `Column ${colIndex + 1}`,
        targetField,
        confidence: DETECTION_CONFIDENCE.context,
        detectionMethod: 'context',
        sampleValues: values.slice(0, LIMITS.maxSampleValuesPerColumn),
        transformation: getTransformationHint(targetField),
      };
    }
  }
  
  return null;
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Gets transformation hint for a field type
 * 
 * @param {string} field - Target field
 * @returns {string|undefined} Transformation hint
 */
function getTransformationHint(field) {
  const hints = {
    ticker: 'uppercase',
    type: 'normalizeType',
    amount: 'parseNumber',
    price: 'parseNumber',
    date: 'parseDate:auto',
    currency: 'uppercase',
    commission: 'parseNumber:absolute',
  };
  
  return hints[field];
}

/**
 * Capitalizes first letter
 * 
 * @param {string} str - String to capitalize
 * @returns {string} Capitalized string
 */
function capitalize(str) {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Detects if the first row is likely a header
 * 
 * @param {string[][]} data - File data
 * @returns {boolean} True if first row is likely header
 */
function detectHasHeader(data) {
  if (data.length < 2) return false;
  
  const firstRow = data[0];
  const secondRow = data[1];
  
  // Check if first row is all text and second row has numbers
  const firstRowAllText = firstRow.every(cell => {
    const str = String(cell || '').trim();
    return str.length > 0 && isNaN(parseFloat(str.replace(/[,$]/g, '')));
  });
  
  const secondRowHasNumbers = secondRow.some(cell => {
    const str = String(cell || '').trim();
    return !isNaN(parseFloat(str.replace(/[,$]/g, '')));
  });
  
  return firstRowAllText && secondRowHasNumbers;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  detectColumnsGeneric,
  detectByHeaders,
  detectByContent,
  detectByContext,
  analyzeColumnContent,
  analyzeNumericColumn,
  detectHasHeader,
};
