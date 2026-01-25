/**
 * Integration Tests for analyzeTransactionFile Cloud Function
 * 
 * IMPORT-001: Tests del flujo completo de análisis
 * 
 * @module __tests__/transactions/analyzeTransactionFile.test
 * @see docs/stories/89.story.md (IMPORT-001)
 */

// Mock Firebase Functions
jest.mock('firebase-functions/v2/https', () => ({
  onCall: jest.fn((config, handler) => {
    return { _handler: handler, _config: config };
  }),
  HttpsError: class HttpsError extends Error {
    constructor(code, message) {
      super(message);
      this.code = code;
      this.message = message;
    }
  }
}));

// Mock financeQuery for ticker validation
jest.mock('../../financeQuery', () => ({
  search: jest.fn().mockImplementation(async (query) => {
    const knownTickers = {
      AAPL: [{ symbol: 'AAPL', shortname: 'Apple Inc.', exchange: 'NASDAQ', quoteType: 'EQUITY', currency: 'USD' }],
      NVDA: [{ symbol: 'NVDA', shortname: 'NVIDIA Corp.', exchange: 'NASDAQ', quoteType: 'EQUITY', currency: 'USD' }],
      MSFT: [{ symbol: 'MSFT', shortname: 'Microsoft', exchange: 'NASDAQ', quoteType: 'EQUITY', currency: 'USD' }],
      SPY: [{ symbol: 'SPY', shortname: 'SPDR S&P 500', exchange: 'NYSE', quoteType: 'ETF', currency: 'USD' }],
    };
    return knownTickers[query.toUpperCase()] || [];
  }),
}));

const { analyzeTransactionFile } = require('../../transactions/analyzeTransactionFile');
const { HttpsError } = require('firebase-functions/v2/https');

// ============================================================================
// TEST DATA
// ============================================================================

const IBKR_SAMPLE = [
  ['Symbol', 'Date/Time', 'Quantity', 'T. Price', 'Comm/Fee', 'Currency'],
  ['AAPL', '2024-01-15, 09:30:00', '10', '150.50', '1.00', 'USD'],
  ['NVDA', '2024-01-16, 10:15:00', '-5', '500.00', '1.00', 'USD'],
  ['MSFT', '2024-01-17, 14:00:00', '15', '375.00', '1.50', 'USD'],
];

const GENERIC_SAMPLE = [
  ['Ticker', 'Type', 'Shares', 'Price', 'Date'],
  ['AAPL', 'Buy', '10', '150.50', '2024-01-15'],
  ['NVDA', 'Sell', '5', '500.00', '2024-01-16'],
  ['MSFT', 'Buy', '15', '375.00', '2024-01-17'],
];

const SPANISH_SAMPLE = [
  ['Símbolo', 'Operación', 'Cantidad', 'Precio', 'Fecha'],
  ['AAPL', 'Compra', '10', '150.50', '15/01/2024'],
  ['NVDA', 'Venta', '5', '500.00', '16/01/2024'],
];

const MINIMAL_SAMPLE = [
  ['Symbol', 'Action', 'Qty', 'Price'],
  ['AAPL', 'Buy', '10', '150.50'],
];

const INVALID_SAMPLE = [
  ['Col1', 'Col2', 'Col3'],
  ['xxx', 'yyy', 'zzz'],
];

// Helper to call the Cloud Function
async function callFunction(data, auth = { uid: 'test-user-123' }) {
  const handler = analyzeTransactionFile._handler;
  return handler({ data, auth });
}

// ============================================================================
// TESTS: Authentication (AC-001, AC-002)
// ============================================================================

describe('Authentication', () => {
  test('AC-001: requires authentication', async () => {
    await expect(callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    }, null)).rejects.toThrow('autenticado');
  });

  test('AC-002: accepts request with auth object', async () => {
    // With valid auth object, request should succeed
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    }, { uid: 'valid-user-id' });
    
    expect(result.success).toBe(true);
  });

  test('accepts authenticated request', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(result.success).toBe(true);
  });
});

// ============================================================================
// TESTS: Payload Validation (AC-003, AC-004)
// ============================================================================

describe('Payload Validation', () => {
  test('AC-003: validates sampleData is array', async () => {
    await expect(callFunction({
      sampleData: 'not an array',
      fileName: 'test.xlsx',
    })).rejects.toThrow('array');
  });

  test('rejects empty sampleData', async () => {
    await expect(callFunction({
      sampleData: [],
      fileName: 'test.xlsx',
    })).rejects.toThrow('vacío');
  });

  test('AC-004: truncates to 100 rows max', async () => {
    const largeData = Array.from({ length: 150 }, (_, i) => 
      ['AAPL', 'Buy', '10', '150', `2024-01-${String(i % 28 + 1).padStart(2, '0')}`]
    );
    largeData.unshift(['Symbol', 'Type', 'Qty', 'Price', 'Date']);
    
    const result = await callFunction({
      sampleData: largeData,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(result.success).toBe(true);
    expect(result.totalRows).toBeLessThanOrEqual(100);
  });
});

// ============================================================================
// TESTS: Broker Detection (AC-005 to AC-009)
// ============================================================================

describe('Broker Detection', () => {
  test('AC-005: detects Interactive Brokers format', async () => {
    const result = await callFunction({
      sampleData: IBKR_SAMPLE,
      fileName: 'ibkr_export.xlsx',
      hasHeader: true,
    });
    
    expect(result.detectedBroker).toBe('interactive_brokers');
  });

  test('AC-009: uses generic detection for unknown format', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'my_trades.xlsx',
      hasHeader: true,
    });
    
    expect(result.detectedBroker).toBeNull();
    expect(result.mappings.length).toBeGreaterThan(0);
  });
});

// ============================================================================
// TESTS: Column Detection (AC-010 to AC-016)
// ============================================================================

describe('Column Detection', () => {
  test('detects all required fields from clear headers', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'trades.xlsx',
      hasHeader: true,
    });
    
    const mappedFields = result.mappings.map(m => m.targetField);
    
    expect(mappedFields).toContain('ticker');
    expect(mappedFields).toContain('type');
    expect(mappedFields).toContain('amount');
    expect(mappedFields).toContain('price');
    expect(mappedFields).toContain('date');
  });

  test('AC-010: detects ticker by various patterns', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'trades.xlsx',
      hasHeader: true,
    });
    
    const tickerMapping = result.mappings.find(m => m.targetField === 'ticker');
    expect(tickerMapping).toBeDefined();
    expect(tickerMapping.sourceHeader).toBe('Ticker');
  });

  test('detects Spanish headers', async () => {
    const result = await callFunction({
      sampleData: SPANISH_SAMPLE,
      fileName: 'operaciones.xlsx',
      hasHeader: true,
    });
    
    const mappedFields = result.mappings.map(m => m.targetField);
    expect(mappedFields).toContain('ticker');
    expect(mappedFields).toContain('type');
    expect(mappedFields).toContain('amount');
  });
});

// ============================================================================
// TESTS: Response Format (AC-031 to AC-039)
// ============================================================================

describe('Response Format', () => {
  test('AC-031: returns success boolean', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(typeof result.success).toBe('boolean');
    expect(result.success).toBe(true);
  });

  test('AC-032: returns detectedBroker', async () => {
    const result = await callFunction({
      sampleData: IBKR_SAMPLE,
      fileName: 'ibkr.xlsx',
      hasHeader: true,
    });
    
    expect(result).toHaveProperty('detectedBroker');
    expect(result.detectedBroker).toBe('interactive_brokers');
  });

  test('AC-033: returns mappings array', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(Array.isArray(result.mappings)).toBe(true);
    
    for (const mapping of result.mappings) {
      expect(mapping).toHaveProperty('sourceColumn');
      expect(mapping).toHaveProperty('sourceHeader');
      expect(mapping).toHaveProperty('targetField');
      expect(mapping).toHaveProperty('confidence');
      expect(mapping).toHaveProperty('detectionMethod');
      expect(mapping).toHaveProperty('sampleValues');
    }
  });

  test('AC-034: returns unmappedColumns', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(Array.isArray(result.unmappedColumns)).toBe(true);
  });

  test('AC-035: returns missingRequiredFields', async () => {
    const result = await callFunction({
      sampleData: MINIMAL_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(Array.isArray(result.missingRequiredFields)).toBe(true);
    expect(result.missingRequiredFields).toContain('date');
  });

  test('AC-036: returns overallConfidence', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(typeof result.overallConfidence).toBe('number');
    expect(result.overallConfidence).toBeGreaterThanOrEqual(0);
    expect(result.overallConfidence).toBeLessThanOrEqual(1);
  });

  test('AC-037: returns warnings array', async () => {
    const result = await callFunction({
      sampleData: MINIMAL_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(Array.isArray(result.warnings)).toBe(true);
    // Should have warning about missing date field
    expect(result.warnings.length).toBeGreaterThan(0);
  });

  test('AC-038: returns suggestions array', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(Array.isArray(result.suggestions)).toBe(true);
  });

  test('AC-039: returns tickerValidation summary', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(result.tickerValidation).toHaveProperty('total');
    expect(result.tickerValidation).toHaveProperty('valid');
    expect(result.tickerValidation).toHaveProperty('invalid');
    expect(result.tickerValidation).toHaveProperty('invalidTickers');
    expect(result.tickerValidation).toHaveProperty('suggestions');
  });

  test('returns hasHeader flag', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(result.hasHeader).toBe(true);
  });

  test('returns readiness assessment', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(result.readiness).toHaveProperty('canProceed');
    expect(result.readiness).toHaveProperty('confidence');
  });
});

// ============================================================================
// TESTS: Performance (AC-040)
// ============================================================================

describe('Performance', () => {
  test('AC-040: responds within time limit', async () => {
    const startTime = Date.now();
    
    await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    const duration = Date.now() - startTime;
    
    // Should complete quickly (< 3 seconds target, < 5 seconds test allowance)
    expect(duration).toBeLessThan(5000);
  });

  test('includes processing time in response', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(result.processingTimeMs).toBeDefined();
    expect(typeof result.processingTimeMs).toBe('number');
    expect(result.processingTimeMs).toBeGreaterThanOrEqual(0);
  });
});

// ============================================================================
// TESTS: Edge Cases
// ============================================================================

describe('Edge Cases', () => {
  test('auto-detects header when not specified', async () => {
    const result = await callFunction({
      sampleData: GENERIC_SAMPLE,
      fileName: 'test.xlsx',
      // hasHeader not provided
    });
    
    expect(result.success).toBe(true);
    expect(result.hasHeader).toBe(true);
  });

  test('handles data without headers', async () => {
    const dataOnly = [
      ['AAPL', 'Buy', '10', '150.50', '2024-01-15'],
      ['NVDA', 'Sell', '5', '500.00', '2024-01-16'],
    ];
    
    const result = await callFunction({
      sampleData: dataOnly,
      fileName: 'test.xlsx',
      hasHeader: false,
    });
    
    expect(result.success).toBe(true);
    expect(result.hasHeader).toBe(false);
  });

  test('handles sparse data with empty cells', async () => {
    const sparseData = [
      ['Symbol', 'Type', 'Qty', 'Price', 'Date'],
      ['AAPL', 'Buy', '10', '', '2024-01-15'],
      ['', 'Sell', '5', '500.00', ''],
      ['MSFT', '', '15', '375.00', '2024-01-17'],
    ];
    
    const result = await callFunction({
      sampleData: sparseData,
      fileName: 'test.xlsx',
      hasHeader: true,
    });
    
    expect(result.success).toBe(true);
  });

  test('handles completely unrecognizable data', async () => {
    const result = await callFunction({
      sampleData: INVALID_SAMPLE,
      fileName: 'random.xlsx',
      hasHeader: true,
    });
    
    expect(result.success).toBe(true);
    expect(result.missingRequiredFields.length).toBeGreaterThan(0);
    expect(result.overallConfidence).toBeLessThan(0.5);
  });
});
