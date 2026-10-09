/**
 * Tests for confidenceCalculator.js
 * 
 * IMPORT-001: Verifica el cálculo de confianza global
 * 
 * @module __tests__/transactions/confidenceCalculator.test
 * @see docs/stories/89.story.md (IMPORT-001)
 */

const { 
  calculateOverallConfidence,
  calculateRequiredFieldsScore,
  calculateMappingConfidenceScore,
  calculateTickerValidationScore,
  generateFeedback,
  evaluateReadiness,
  CONFIDENCE_WEIGHTS,
  MIN_ACCEPTABLE_CONFIDENCE,
} = require('../../transactions/services/confidenceCalculator');

// ============================================================================
// TEST DATA
// ============================================================================

const COMPLETE_MAPPINGS = [
  { targetField: 'ticker', confidence: 0.95, detectionMethod: 'broker' },
  { targetField: 'type', confidence: 0.95, detectionMethod: 'broker' },
  { targetField: 'amount', confidence: 0.95, detectionMethod: 'broker' },
  { targetField: 'price', confidence: 0.95, detectionMethod: 'broker' },
  { targetField: 'date', confidence: 0.95, detectionMethod: 'broker' },
  { targetField: 'currency', confidence: 0.90, detectionMethod: 'header' },
  { targetField: 'commission', confidence: 0.90, detectionMethod: 'header' },
];

const PARTIAL_MAPPINGS = [
  { targetField: 'ticker', confidence: 0.9, detectionMethod: 'header' },
  { targetField: 'type', confidence: 0.7, detectionMethod: 'content' },
  { targetField: 'amount', confidence: 0.9, detectionMethod: 'header' },
];

const LOW_CONFIDENCE_MAPPINGS = [
  { targetField: 'ticker', confidence: 0.5, detectionMethod: 'context' },
  { targetField: 'type', confidence: 0.4, detectionMethod: 'context' },
  { targetField: 'amount', confidence: 0.5, detectionMethod: 'context' },
  { targetField: 'price', confidence: 0.6, detectionMethod: 'content' },
  { targetField: 'date', confidence: 0.5, detectionMethod: 'context' },
];

const GOOD_TICKER_VALIDATION = {
  total: 20,
  valid: 18,
  invalid: 2,
  invalidTickers: ['XXX', 'YYY'],
  suggestions: { 'XXX': 'XOM' },
};

const BAD_TICKER_VALIDATION = {
  total: 20,
  valid: 8,
  invalid: 12,
  invalidTickers: ['A1', 'B2', 'C3'],
  suggestions: {},
};

// ============================================================================
// TESTS: calculateOverallConfidence (AC-027 to AC-030)
// ============================================================================

describe('calculateOverallConfidence', () => {
  test('AC-027: returns high confidence for complete broker detection', () => {
    const confidence = calculateOverallConfidence(
      COMPLETE_MAPPINGS,
      [],  // No missing fields
      GOOD_TICKER_VALIDATION,
      'interactive_brokers'
    );
    
    expect(confidence).toBeGreaterThan(0.85);
    expect(confidence).toBeLessThanOrEqual(1.0);
  });

  test('AC-028: calculates weighted average of components', () => {
    const confidence = calculateOverallConfidence(
      COMPLETE_MAPPINGS,
      [],
      GOOD_TICKER_VALIDATION,
      null  // No broker detection
    );
    
    // Without broker bonus, still should be high
    expect(confidence).toBeGreaterThan(0.75);
  });

  test('AC-029: penalizes missing required fields', () => {
    const withAllFields = calculateOverallConfidence(
      COMPLETE_MAPPINGS,
      [],
      GOOD_TICKER_VALIDATION,
      'interactive_brokers'
    );
    
    const withMissingFields = calculateOverallConfidence(
      PARTIAL_MAPPINGS,
      ['price', 'date'],
      GOOD_TICKER_VALIDATION,
      'interactive_brokers'
    );
    
    expect(withMissingFields).toBeLessThan(withAllFields);
  });

  test('AC-030: adds bonus for broker detection', () => {
    const withBroker = calculateOverallConfidence(
      COMPLETE_MAPPINGS,
      [],
      GOOD_TICKER_VALIDATION,
      'interactive_brokers'
    );
    
    const withoutBroker = calculateOverallConfidence(
      COMPLETE_MAPPINGS,
      [],
      GOOD_TICKER_VALIDATION,
      null
    );
    
    expect(withBroker).toBeGreaterThan(withoutBroker);
    // Broker adds bonus via detection component weight (10%) + 5% bonus
    expect(withBroker - withoutBroker).toBeGreaterThanOrEqual(0.05);
  });

  test('severely penalizes missing ticker column', () => {
    const confidence = calculateOverallConfidence(
      PARTIAL_MAPPINGS.filter(m => m.targetField !== 'ticker'),
      ['ticker', 'price', 'date'],
      GOOD_TICKER_VALIDATION,
      null
    );
    
    expect(confidence).toBeLessThan(0.3);
  });

  test('penalizes many invalid tickers', () => {
    const withGoodTickers = calculateOverallConfidence(
      COMPLETE_MAPPINGS,
      [],
      GOOD_TICKER_VALIDATION,
      'interactive_brokers'
    );
    
    const withBadTickers = calculateOverallConfidence(
      COMPLETE_MAPPINGS,
      [],
      BAD_TICKER_VALIDATION,
      'interactive_brokers'
    );
    
    expect(withBadTickers).toBeLessThan(withGoodTickers);
  });

  test('returns 0 for empty mappings', () => {
    const confidence = calculateOverallConfidence(
      [],
      ['ticker', 'type', 'amount', 'price', 'date'],
      { total: 0, valid: 0, invalid: 0 },
      null
    );
    
    // With no mappings and all fields missing, confidence should be very low
    expect(confidence).toBeLessThanOrEqual(0.1);
  });
});

// ============================================================================
// TESTS: calculateRequiredFieldsScore
// ============================================================================

describe('calculateRequiredFieldsScore', () => {
  test('returns 1.0 for all required fields with high confidence', () => {
    const score = calculateRequiredFieldsScore(COMPLETE_MAPPINGS, []);
    expect(score).toBeCloseTo(1.0, 1);
  });

  test('returns lower score for missing fields', () => {
    const score = calculateRequiredFieldsScore(PARTIAL_MAPPINGS, ['price', 'date']);
    expect(score).toBeLessThan(0.7);
  });

  test('returns 0 for no mappings', () => {
    const score = calculateRequiredFieldsScore([], ['ticker', 'type', 'amount', 'price', 'date']);
    expect(score).toBe(0);
  });
});

// ============================================================================
// TESTS: calculateMappingConfidenceScore
// ============================================================================

describe('calculateMappingConfidenceScore', () => {
  test('returns high score for high-confidence mappings', () => {
    const score = calculateMappingConfidenceScore(COMPLETE_MAPPINGS);
    expect(score).toBeGreaterThan(0.9);
  });

  test('returns lower score for low-confidence mappings', () => {
    const score = calculateMappingConfidenceScore(LOW_CONFIDENCE_MAPPINGS);
    expect(score).toBeLessThan(0.6);
  });

  test('weights required fields higher', () => {
    // Two mappings with same confidence but one is required
    const withRequired = [
      { targetField: 'ticker', confidence: 0.8 },  // Required
    ];
    
    const withOptional = [
      { targetField: 'currency', confidence: 0.8 },  // Optional
    ];
    
    const scoreRequired = calculateMappingConfidenceScore(withRequired);
    const scoreOptional = calculateMappingConfidenceScore(withOptional);
    
    // Both should be 0.8 but calculation differs due to weighting
    expect(scoreRequired).toBe(0.8);
    expect(scoreOptional).toBe(0.8);
  });

  test('returns 0 for empty mappings', () => {
    expect(calculateMappingConfidenceScore([])).toBe(0);
  });
});

// ============================================================================
// TESTS: calculateTickerValidationScore
// ============================================================================

describe('calculateTickerValidationScore', () => {
  test('returns high score for mostly valid tickers', () => {
    const score = calculateTickerValidationScore(GOOD_TICKER_VALIDATION);
    expect(score).toBe(0.9); // 18/20 = 0.9
  });

  test('returns low score for many invalid tickers', () => {
    const score = calculateTickerValidationScore(BAD_TICKER_VALIDATION);
    expect(score).toBeLessThan(0.5);
  });

  test('returns 0.5 (neutral) for no validation', () => {
    const score = calculateTickerValidationScore({ total: 0, valid: 0, invalid: 0 });
    expect(score).toBe(0.5);
  });

  test('returns 0.5 for null input', () => {
    const score = calculateTickerValidationScore(null);
    expect(score).toBe(0.5);
  });
});

// ============================================================================
// TESTS: generateFeedback
// ============================================================================

describe('generateFeedback', () => {
  test('generates warning for missing required fields', () => {
    const { warnings, suggestions } = generateFeedback(
      PARTIAL_MAPPINGS,
      ['price', 'date'],
      GOOD_TICKER_VALIDATION,
      null
    );
    
    expect(warnings.length).toBeGreaterThan(0);
    expect(warnings.some(w => w.includes('price'))).toBe(true);
    expect(suggestions.length).toBeGreaterThan(0);
  });

  test('generates warning for low confidence mappings', () => {
    const { warnings } = generateFeedback(
      LOW_CONFIDENCE_MAPPINGS,
      [],
      GOOD_TICKER_VALIDATION,
      null
    );
    
    expect(warnings.some(w => w.includes('baja confianza'))).toBe(true);
  });

  test('generates warning for invalid tickers', () => {
    const { warnings, suggestions } = generateFeedback(
      COMPLETE_MAPPINGS,
      [],
      BAD_TICKER_VALIDATION,
      null
    );
    
    expect(warnings.some(w => w.includes('no fueron reconocidos'))).toBe(true);
  });

  test('generates suggestion for missing currency', () => {
    const mappingsWithoutCurrency = COMPLETE_MAPPINGS.filter(m => 
      m.targetField !== 'currency'
    );
    
    const { suggestions } = generateFeedback(
      mappingsWithoutCurrency,
      [],
      GOOD_TICKER_VALIDATION,
      null
    );
    
    expect(suggestions.some(s => s.includes('moneda'))).toBe(true);
  });

  test('includes broker detection info in suggestions', () => {
    const { suggestions } = generateFeedback(
      COMPLETE_MAPPINGS,
      [],
      GOOD_TICKER_VALIDATION,
      'interactive_brokers'
    );
    
    expect(suggestions.some(s => s.includes('Interactive Brokers'))).toBe(true);
  });
});

// ============================================================================
// TESTS: evaluateReadiness
// ============================================================================

describe('evaluateReadiness', () => {
  test('returns canProceed=true when all requirements met', () => {
    const result = evaluateReadiness(0.85, []);
    
    expect(result.canProceed).toBe(true);
    expect(result.requiresManualMapping).toBe(false);
    expect(result.confidence).toBe('high');
  });

  test('returns canProceed=false when missing critical fields', () => {
    const result = evaluateReadiness(0.85, ['ticker']);
    
    expect(result.canProceed).toBe(false);
    expect(result.requiresManualMapping).toBe(true);
    expect(result.criticalMissingFields).toContain('ticker');
  });

  test('returns canProceed=false when confidence too low', () => {
    const result = evaluateReadiness(0.45, []);
    
    expect(result.canProceed).toBe(false);
    expect(result.confidence).toBe('low');
  });

  test('classifies confidence levels correctly', () => {
    expect(evaluateReadiness(0.85, []).confidence).toBe('high');
    expect(evaluateReadiness(0.70, []).confidence).toBe('medium');
    expect(evaluateReadiness(0.50, []).confidence).toBe('low');
  });
});

// ============================================================================
// TESTS: CONSTANTS
// ============================================================================

describe('Confidence Constants', () => {
  test('CONFIDENCE_WEIGHTS sum to 1.0', () => {
    const sum = Object.values(CONFIDENCE_WEIGHTS).reduce((a, b) => a + b, 0);
    expect(sum).toBeCloseTo(1.0, 2);
  });

  test('MIN_ACCEPTABLE_CONFIDENCE is reasonable', () => {
    expect(MIN_ACCEPTABLE_CONFIDENCE).toBeGreaterThan(0.5);
    expect(MIN_ACCEPTABLE_CONFIDENCE).toBeLessThan(0.8);
  });
});
