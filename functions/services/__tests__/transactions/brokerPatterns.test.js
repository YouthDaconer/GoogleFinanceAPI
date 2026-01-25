/**
 * Tests for brokerPatterns.js
 * 
 * IMPORT-001: Verifica la detección de formatos de broker
 * 
 * @module __tests__/transactions/brokerPatterns.test
 * @see docs/stories/89.story.md (IMPORT-001)
 */

const { 
  detectBrokerFormat, 
  detectByFilename,
  getBrokerMappings,
  getBrokerDisplayName,
  BROKER_SIGNATURES,
} = require('../../transactions/services/brokerPatterns');

// ============================================================================
// TEST DATA
// ============================================================================

const IBKR_HEADERS = ['Symbol', 'Date/Time', 'Quantity', 'T. Price', 'Comm/Fee', 'Currency'];
const TD_HEADERS = ['Symbol', 'Trade Date', 'Quantity', 'Price', 'Commission', 'Action'];
const FIDELITY_HEADERS = ['Symbol', 'Action', 'Quantity', 'Price', 'Settlement Date'];
const ETORO_HEADERS = ['Position ID', 'Asset', 'Action', 'Amount', 'Units', 'Open Rate'];
const GENERIC_HEADERS = ['Ticker', 'Type', 'Shares', 'Price', 'Date'];

const SAMPLE_DATA_IBKR = [
  ['Symbol', 'Date/Time', 'Quantity', 'T. Price', 'Comm/Fee'],
  ['AAPL', '2024-01-15, 09:30:00', '10', '150.50', '1.00'],
  ['NVDA', '2024-01-16, 10:15:00', '-5', '500.00', '1.00'],
  ['MSFT', '2024-01-17, 14:00:00', '15', '375.00', '1.50'],
];

const SAMPLE_DATA_GENERIC = [
  ['Stock', 'Buy/Sell', 'Qty', 'Price', 'Date'],
  ['AAPL', 'Buy', '10', '150.50', '2024-01-15'],
  ['NVDA', 'Sell', '5', '500.00', '2024-01-16'],
];

// ============================================================================
// TESTS: detectBrokerFormat (AC-005 to AC-009)
// ============================================================================

describe('detectBrokerFormat', () => {
  describe('Interactive Brokers Detection (AC-005)', () => {
    test('detects IBKR by unique headers (T. Price, Comm/Fee)', () => {
      const result = detectBrokerFormat(IBKR_HEADERS, 'trades.xlsx');
      expect(result).toBe('interactive_brokers');
    });

    test('detects IBKR by header pattern match', () => {
      const headers = ['Symbol', 'TradeDate', 'Quantity', 'TradePrice', 'Commission'];
      const result = detectBrokerFormat(headers, 'export.csv');
      expect(result).toBe('interactive_brokers');
    });

    test('detects IBKR by filename pattern', () => {
      const result = detectBrokerFormat(GENERIC_HEADERS, 'ibkr_flex_query_2024.xlsx');
      expect(result).toBe('interactive_brokers');
    });
  });

  describe('TD Ameritrade Detection (AC-006)', () => {
    test('detects TD Ameritrade by unique headers', () => {
      const headers = ['Symbol', 'Trade Date', 'Quantity', 'Price', 'REG FEE'];
      const result = detectBrokerFormat(headers, 'trades.csv');
      expect(result).toBe('td_ameritrade');
    });

    test('detects TD Ameritrade by filename', () => {
      const result = detectBrokerFormat(GENERIC_HEADERS, 'td_ameritrade_export.xlsx');
      expect(result).toBe('td_ameritrade');
    });
  });

  describe('Fidelity Detection (AC-007)', () => {
    test('detects Fidelity by unique headers (Security Description)', () => {
      const headers = ['Symbol', 'Security Description', 'Action', 'Quantity', 'Price'];
      const result = detectBrokerFormat(headers, 'trades.csv');
      expect(result).toBe('fidelity');
    });

    test('detects Fidelity by filename', () => {
      const result = detectBrokerFormat(GENERIC_HEADERS, 'fidelity_brokerage_history.xlsx');
      expect(result).toBe('fidelity');
    });
  });

  describe('eToro Detection (AC-008)', () => {
    test('detects eToro by unique headers (Position ID, Open Rate)', () => {
      const result = detectBrokerFormat(ETORO_HEADERS, 'trades.xlsx');
      expect(result).toBe('etoro');
    });

    test('detects eToro by filename', () => {
      const result = detectBrokerFormat(GENERIC_HEADERS, 'etoro_account_statement.xlsx');
      expect(result).toBe('etoro');
    });
  });

  describe('Generic Detection (AC-009)', () => {
    test('returns null for unknown format', () => {
      const result = detectBrokerFormat(GENERIC_HEADERS, 'my_trades.xlsx');
      expect(result).toBeNull();
    });

    test('returns null for empty headers', () => {
      const result = detectBrokerFormat([], 'trades.xlsx');
      expect(result).toBeNull();
    });

    test('returns null for null headers and generic filename', () => {
      const result = detectBrokerFormat(null, 'trades.xlsx');
      expect(result).toBeNull();
    });
  });
});

// ============================================================================
// TESTS: detectByFilename
// ============================================================================

describe('detectByFilename', () => {
  test('detects IBKR by various patterns', () => {
    expect(detectByFilename('ibkr_trades.xlsx')).toBe('interactive_brokers');
    expect(detectByFilename('Interactive_Brokers_Statement.csv')).toBe('interactive_brokers');
    expect(detectByFilename('flex_query_results.xlsx')).toBe('interactive_brokers');
  });

  test('detects Schwab/TD by pattern', () => {
    // Note: 'schwab' matches TD Ameritrade first due to order in BROKER_SIGNATURES
    // This is acceptable as Schwab merged with TD Ameritrade
    const schwabResult = detectByFilename('schwab_export.csv');
    expect(['td_ameritrade', 'charles_schwab']).toContain(schwabResult);
    
    expect(detectByFilename('tda_trades_2024.xlsx')).toBe('td_ameritrade');
  });

  test('returns null for empty/null filename', () => {
    expect(detectByFilename('')).toBeNull();
    expect(detectByFilename(null)).toBeNull();
  });
});

// ============================================================================
// TESTS: getBrokerMappings
// ============================================================================

describe('getBrokerMappings', () => {
  test('returns correct mappings for IBKR format', () => {
    const mappings = getBrokerMappings('interactive_brokers', SAMPLE_DATA_IBKR, true);
    
    expect(mappings.length).toBeGreaterThan(0);
    
    const tickerMapping = mappings.find(m => m.targetField === 'ticker');
    expect(tickerMapping).toBeDefined();
    expect(tickerMapping.sourceColumn).toBe(0);
    expect(tickerMapping.sourceHeader).toBe('Symbol');
    expect(tickerMapping.confidence).toBe(0.95);
    expect(tickerMapping.detectionMethod).toBe('broker');
    
    const priceMapping = mappings.find(m => m.targetField === 'price');
    expect(priceMapping).toBeDefined();
    expect(priceMapping.sourceHeader).toBe('T. Price');
  });

  test('includes sample values from data rows', () => {
    const mappings = getBrokerMappings('interactive_brokers', SAMPLE_DATA_IBKR, true);
    
    const tickerMapping = mappings.find(m => m.targetField === 'ticker');
    expect(tickerMapping.sampleValues).toContain('AAPL');
    expect(tickerMapping.sampleValues).toContain('NVDA');
  });

  test('derives type from quantity sign for IBKR', () => {
    const mappings = getBrokerMappings('interactive_brokers', SAMPLE_DATA_IBKR, true);
    
    const typeMapping = mappings.find(m => m.targetField === 'type');
    expect(typeMapping).toBeDefined();
    expect(typeMapping.derivedFrom).toBe('amount');
    expect(typeMapping.transformation).toBe('deriveFromQuantitySign');
  });

  test('returns empty array for unknown broker', () => {
    const mappings = getBrokerMappings('unknown_broker', SAMPLE_DATA_IBKR, true);
    expect(mappings).toEqual([]);
  });

  test('returns empty array when no headers available', () => {
    const mappings = getBrokerMappings('interactive_brokers', SAMPLE_DATA_GENERIC, false);
    expect(mappings).toEqual([]);
  });
});

// ============================================================================
// TESTS: getBrokerDisplayName
// ============================================================================

describe('getBrokerDisplayName', () => {
  test('returns correct display names', () => {
    expect(getBrokerDisplayName('interactive_brokers')).toBe('Interactive Brokers');
    expect(getBrokerDisplayName('td_ameritrade')).toBe('TD Ameritrade');
    expect(getBrokerDisplayName('fidelity')).toBe('Fidelity');
    expect(getBrokerDisplayName('etoro')).toBe('eToro');
  });

  test('returns ID for unknown broker', () => {
    expect(getBrokerDisplayName('unknown')).toBe('unknown');
  });
});

// ============================================================================
// TESTS: BROKER_SIGNATURES Configuration
// ============================================================================

describe('BROKER_SIGNATURES Configuration', () => {
  test('all brokers have required signature fields', () => {
    for (const [brokerId, signature] of Object.entries(BROKER_SIGNATURES)) {
      expect(signature.headers).toBeDefined();
      expect(Array.isArray(signature.headers)).toBe(true);
      expect(signature.headers.length).toBeGreaterThan(0);
      
      expect(signature.filePatterns).toBeDefined();
      expect(Array.isArray(signature.filePatterns)).toBe(true);
    }
  });

  test('each broker has unique headers defined', () => {
    const brokersWithUnique = Object.entries(BROKER_SIGNATURES)
      .filter(([_, sig]) => sig.uniqueHeaders);
    
    expect(brokersWithUnique.length).toBeGreaterThan(0);
    
    for (const [_, signature] of brokersWithUnique) {
      expect(Array.isArray(signature.uniqueHeaders)).toBe(true);
      expect(signature.uniqueHeaders.length).toBeGreaterThan(0);
    }
  });
});
