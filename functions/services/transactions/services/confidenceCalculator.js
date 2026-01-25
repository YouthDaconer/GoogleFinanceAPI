/**
 * Confidence Calculator Service
 * 
 * Calculates overall confidence score for column mappings based on:
 * - Detection method used per column
 * - Coverage of required fields
 * - Ticker validation results
 * - Broker detection bonus
 * 
 * @module transactions/services/confidenceCalculator
 * @see docs/stories/89.story.md (IMPORT-001)
 */

const { REQUIRED_FIELDS, DETECTION_CONFIDENCE } = require('../types');

// ============================================================================
// WEIGHTS FOR CONFIDENCE CALCULATION
// ============================================================================

/**
 * Weight of each component in overall confidence
 */
const CONFIDENCE_WEIGHTS = {
  requiredFieldsCoverage: 0.40,   // 40% - Do we have all required fields?
  mappingConfidence: 0.30,        // 30% - How confident are the individual mappings?
  tickerValidation: 0.20,         // 20% - Are the tickers valid?
  brokerDetection: 0.10,          // 10% - Did we detect a known broker?
};

/**
 * Bonus for detecting a known broker format
 */
const BROKER_DETECTION_BONUS = 0.05;

/**
 * Minimum confidence to consider mapping acceptable
 */
const MIN_ACCEPTABLE_CONFIDENCE = 0.6;

// ============================================================================
// MAIN CALCULATION FUNCTION
// ============================================================================

/**
 * Calculates overall confidence score for the analysis
 * 
 * @param {Object[]} mappings - Column mappings with individual confidence
 * @param {string[]} missingRequiredFields - Required fields that weren't mapped
 * @param {Object} tickerValidation - Ticker validation results
 * @param {string|null} detectedBroker - Detected broker ID
 * @returns {number} Overall confidence score (0.0 - 1.0)
 * 
 * @example
 * const confidence = calculateOverallConfidence(
 *   mappings,
 *   [],  // no missing fields
 *   { valid: 18, total: 20 },
 *   'interactive_brokers'
 * );
 * // Returns: 0.92
 */
function calculateOverallConfidence(
  mappings, 
  missingRequiredFields, 
  tickerValidation, 
  detectedBroker
) {
  // Component scores
  const requiredFieldsScore = calculateRequiredFieldsScore(mappings, missingRequiredFields);
  const mappingScore = calculateMappingConfidenceScore(mappings);
  const tickerScore = calculateTickerValidationScore(tickerValidation);
  const brokerScore = detectedBroker ? 1.0 : 0.0;
  
  // Weighted average
  let overallConfidence = (
    requiredFieldsScore * CONFIDENCE_WEIGHTS.requiredFieldsCoverage +
    mappingScore * CONFIDENCE_WEIGHTS.mappingConfidence +
    tickerScore * CONFIDENCE_WEIGHTS.tickerValidation +
    brokerScore * CONFIDENCE_WEIGHTS.brokerDetection
  );
  
  // Apply broker detection bonus
  if (detectedBroker) {
    overallConfidence = Math.min(1.0, overallConfidence + BROKER_DETECTION_BONUS);
  }
  
  // Apply penalties for critical issues
  overallConfidence = applyPenalties(overallConfidence, mappings, missingRequiredFields, tickerValidation);
  
  // Round to 2 decimal places
  return Math.round(overallConfidence * 100) / 100;
}

// ============================================================================
// COMPONENT SCORE CALCULATIONS
// ============================================================================

/**
 * Calculates score based on coverage of required fields
 * 
 * @param {Object[]} mappings - Column mappings
 * @param {string[]} missingFields - Missing required fields
 * @returns {number} Score (0.0 - 1.0)
 */
function calculateRequiredFieldsScore(mappings, missingFields) {
  const totalRequired = REQUIRED_FIELDS.length;
  const covered = totalRequired - missingFields.length;
  
  // Base score from coverage
  let score = covered / totalRequired;
  
  // Bonus for having required fields with high confidence
  const requiredMappings = mappings.filter(m => 
    REQUIRED_FIELDS.includes(m.targetField)
  );
  
  if (requiredMappings.length > 0) {
    const avgConfidence = requiredMappings.reduce(
      (sum, m) => sum + m.confidence, 0
    ) / requiredMappings.length;
    
    // Blend coverage score with confidence
    score = score * 0.7 + avgConfidence * 0.3;
  }
  
  return score;
}

/**
 * Calculates average confidence of all mappings
 * 
 * @param {Object[]} mappings - Column mappings with confidence scores
 * @returns {number} Average confidence (0.0 - 1.0)
 */
function calculateMappingConfidenceScore(mappings) {
  if (!mappings || mappings.length === 0) {
    return 0;
  }
  
  // Weighted average - required fields matter more
  let weightedSum = 0;
  let totalWeight = 0;
  
  for (const mapping of mappings) {
    const isRequired = REQUIRED_FIELDS.includes(mapping.targetField);
    const weight = isRequired ? 2 : 1;
    
    weightedSum += mapping.confidence * weight;
    totalWeight += weight;
  }
  
  return totalWeight > 0 ? weightedSum / totalWeight : 0;
}

/**
 * Calculates score based on ticker validation results
 * 
 * @param {Object} tickerValidation - Validation results
 * @returns {number} Score (0.0 - 1.0)
 */
function calculateTickerValidationScore(tickerValidation) {
  if (!tickerValidation || tickerValidation.total === 0) {
    return 0.5; // Neutral if no validation
  }
  
  const validRatio = tickerValidation.valid / tickerValidation.total;
  
  // Apply threshold - below 50% is very concerning
  if (validRatio < 0.5) {
    return validRatio * 0.5; // Heavy penalty
  }
  
  return validRatio;
}

// ============================================================================
// PENALTY CALCULATIONS
// ============================================================================

/**
 * Applies penalties for critical issues
 * 
 * @param {number} score - Current score
 * @param {Object[]} mappings - Column mappings
 * @param {string[]} missingFields - Missing required fields
 * @param {Object} tickerValidation - Ticker validation results
 * @returns {number} Adjusted score after penalties
 */
function applyPenalties(score, mappings, missingFields, tickerValidation) {
  let adjustedScore = score;
  
  // Critical penalty: missing ticker or date mapping
  if (missingFields.includes('ticker')) {
    adjustedScore *= 0.3; // 70% penalty
  }
  if (missingFields.includes('date')) {
    adjustedScore *= 0.5; // 50% penalty
  }
  
  // Penalty for low-confidence required field mappings
  const lowConfidenceRequired = mappings.filter(m =>
    REQUIRED_FIELDS.includes(m.targetField) && 
    m.confidence < MIN_ACCEPTABLE_CONFIDENCE
  );
  
  if (lowConfidenceRequired.length > 0) {
    adjustedScore *= (1 - 0.1 * lowConfidenceRequired.length);
  }
  
  // Penalty for many invalid tickers
  if (tickerValidation && tickerValidation.total > 0) {
    const invalidRatio = tickerValidation.invalid / tickerValidation.total;
    if (invalidRatio > 0.3) {
      adjustedScore *= (1 - invalidRatio * 0.3);
    }
  }
  
  return Math.max(0, adjustedScore);
}

// ============================================================================
// FEEDBACK GENERATION
// ============================================================================

/**
 * Generates warnings and suggestions based on analysis
 * 
 * @param {Object[]} mappings - Column mappings
 * @param {string[]} missingFields - Missing required fields
 * @param {Object} tickerValidation - Ticker validation results
 * @param {string|null} detectedBroker - Detected broker
 * @returns {Object} Warnings and suggestions arrays
 */
function generateFeedback(mappings, missingFields, tickerValidation, detectedBroker) {
  const warnings = [];
  const suggestions = [];
  
  // Warnings for missing required fields
  if (missingFields.length > 0) {
    warnings.push(
      `Campos requeridos sin mapear: ${missingFields.join(', ')}`
    );
    
    for (const field of missingFields) {
      suggestions.push(
        `Asigna manualmente la columna para '${field}'`
      );
    }
  }
  
  // Warnings for low confidence mappings
  const lowConfidence = mappings.filter(m => m.confidence < MIN_ACCEPTABLE_CONFIDENCE);
  if (lowConfidence.length > 0) {
    const fieldNames = lowConfidence.map(m => m.targetField).join(', ');
    warnings.push(
      `${lowConfidence.length} mapeo(s) con baja confianza: ${fieldNames}`
    );
    suggestions.push(
      'Revisa los mapeos marcados con baja confianza antes de continuar'
    );
  }
  
  // Warnings for invalid tickers
  if (tickerValidation && tickerValidation.invalid > 0) {
    warnings.push(
      `${tickerValidation.invalid} de ${tickerValidation.total} tickers no fueron reconocidos`
    );
    
    if (Object.keys(tickerValidation.suggestions).length > 0) {
      const suggestionList = Object.entries(tickerValidation.suggestions)
        .slice(0, 3)
        .map(([invalid, suggested]) => `${invalid} → ${suggested}`)
        .join(', ');
      suggestions.push(
        `Correcciones sugeridas: ${suggestionList}`
      );
    }
  }
  
  // Suggestions for optional fields
  const mappedFields = new Set(mappings.map(m => m.targetField));
  
  if (!mappedFields.has('currency')) {
    suggestions.push(
      'No se detectó columna de moneda. Se asumirá USD para todas las transacciones.'
    );
  }
  
  if (!mappedFields.has('commission')) {
    suggestions.push(
      'No se detectó columna de comisión. Se asumirá $0 de comisión por transacción.'
    );
  }
  
  // Info about broker detection
  if (detectedBroker) {
    suggestions.push(
      `Se detectó formato de ${getBrokerDisplayName(detectedBroker)}. Los mapeos se optimizaron automáticamente.`
    );
  }
  
  return { warnings, suggestions };
}

/**
 * Gets broker display name
 * @param {string} brokerId - Broker ID
 * @returns {string} Display name
 */
function getBrokerDisplayName(brokerId) {
  const names = {
    interactive_brokers: 'Interactive Brokers',
    td_ameritrade: 'TD Ameritrade',
    fidelity: 'Fidelity',
    etoro: 'eToro',
    charles_schwab: 'Charles Schwab',
    robinhood: 'Robinhood',
  };
  return names[brokerId] || brokerId;
}

/**
 * Evaluates if the analysis is good enough to proceed
 * 
 * @param {number} overallConfidence - Overall confidence score
 * @param {string[]} missingFields - Missing required fields
 * @returns {Object} Evaluation result
 */
function evaluateReadiness(overallConfidence, missingFields) {
  const criticalMissing = missingFields.filter(f => 
    ['ticker', 'type', 'amount', 'price', 'date'].includes(f)
  );
  
  return {
    canProceed: criticalMissing.length === 0 && overallConfidence >= MIN_ACCEPTABLE_CONFIDENCE,
    requiresManualMapping: criticalMissing.length > 0,
    confidence: overallConfidence >= 0.8 ? 'high' : 
                overallConfidence >= 0.6 ? 'medium' : 'low',
    criticalMissingFields: criticalMissing,
  };
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  calculateOverallConfidence,
  calculateRequiredFieldsScore,
  calculateMappingConfidenceScore,
  calculateTickerValidationScore,
  generateFeedback,
  evaluateReadiness,
  // Constants for testing
  CONFIDENCE_WEIGHTS,
  MIN_ACCEPTABLE_CONFIDENCE,
};
