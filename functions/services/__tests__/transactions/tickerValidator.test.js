/**
 * Tests for tickerValidator.js
 * 
 * IMPORT-001: Verifica la validación de tickers contra la API
 * 
 * @module __tests__/transactions/tickerValidator.test
 * @see docs/stories/89.story.md (IMPORT-001)
 */

// Mock financeQuery before importing
jest.mock('../../financeQuery', () => ({
  search: jest.fn(),
}));

const { search } = require('../../financeQuery');
const { 
  validateTickerSample,
  validateSingleTicker,
  normalizeTicker,
  detectAssetType,
} = require('../../transactions/services/tickerValidator');

// ============================================================================
// TEST DATA
// ============================================================================

const MOCK_SEARCH_RESULTS = {
  AAPL: [
    {
      symbol: 'AAPL',
      shortname: 'Apple Inc.',
      exchange: 'NASDAQ',
      quoteType: 'EQUITY',
      currency: 'USD',
    },
  ],
  NVDA: [
    {
      symbol: 'NVDA',
      shortname: 'NVIDIA Corporation',
      exchange: 'NASDAQ',
      quoteType: 'EQUITY',
      currency: 'USD',
    },
  ],
  SPY: [
    {
      symbol: 'SPY',
      shortname: 'SPDR S&P 500 ETF Trust',
      exchange: 'NYSE',
      quoteType: 'ETF',
      currency: 'USD',
    },
  ],
  'BTC-USD': [
    {
      symbol: 'BTC-USD',
      shortname: 'Bitcoin USD',
      quoteType: 'CRYPTOCURRENCY',
      currency: 'USD',
    },
  ],
  GOOG: [
    {
      symbol: 'GOOGL',  // Returns GOOGL not GOOG
      shortname: 'Alphabet Inc.',
      exchange: 'NASDAQ',
      quoteType: 'EQUITY',
    },
  ],
};

// ============================================================================
// TEST SETUP
// ============================================================================

beforeEach(() => {
  jest.clearAllMocks();
  
  // Default mock implementation
  search.mockImplementation(async (query) => {
    const result = MOCK_SEARCH_RESULTS[query.toUpperCase()];
    if (result) return result;
    
    // For unknown tickers, return empty or partial match
    if (query.startsWith('XX')) {
      return []; // Not found
    }
    
    // Partial match suggestion
    return [{ symbol: 'XOM', shortname: 'Exxon Mobil' }];
  });
});

// ============================================================================
// TESTS: validateTickerSample (AC-022 to AC-026)
// ============================================================================

describe('validateTickerSample', () => {
  test('AC-022: extracts unique tickers from sample', async () => {
    const tickers = ['AAPL', 'NVDA', 'AAPL', 'AAPL', 'NVDA', 'AAPL'];
    
    await validateTickerSample(tickers);
    
    // Should only call search for unique tickers (2 unique: AAPL, NVDA)
    expect(search).toHaveBeenCalledTimes(2);
  });

  test('AC-023: validates tickers against search API', async () => {
    const tickers = ['AAPL', 'NVDA'];
    
    await validateTickerSample(tickers);
    
    expect(search).toHaveBeenCalledWith('AAPL');
    expect(search).toHaveBeenCalledWith('NVDA');
  });

  test('AC-024: returns valid=true for existing tickers', async () => {
    const result = await validateTickerSample(['AAPL', 'NVDA']);
    
    expect(result.valid).toBe(2);
    expect(result.invalid).toBe(0);
    expect(result.details['AAPL'].isValid).toBe(true);
    expect(result.details['NVDA'].isValid).toBe(true);
  });

  test('AC-025: returns suggestion for similar tickers', async () => {
    search.mockImplementation(async (query) => {
      if (query === 'GOOG') {
        return [{ symbol: 'GOOGL', shortname: 'Alphabet Inc.' }];
      }
      return [];
    });
    
    const result = await validateTickerSample(['GOOG']);
    
    expect(result.invalid).toBe(1);
    expect(result.suggestions['GOOG']).toBe('GOOGL');
  });

  test('AC-026: returns assetType and market for valid tickers', async () => {
    const result = await validateTickerSample(['AAPL', 'SPY']);
    
    expect(result.details['AAPL'].assetType).toBe('stock');
    expect(result.details['AAPL'].market).toBe('NASDAQ');
    expect(result.details['SPY'].assetType).toBe('etf');
    expect(result.details['SPY'].market).toBe('NYSE');
  });

  test('limits validation to max 20 tickers', async () => {
    const manyTickers = Array.from({ length: 30 }, (_, i) => `TKR${i}`);
    
    await validateTickerSample(manyTickers);
    
    // Should only validate first 20
    expect(search).toHaveBeenCalledTimes(20);
  });

  test('returns correct summary statistics', async () => {
    search.mockImplementation(async (query) => {
      if (['AAPL', 'NVDA', 'MSFT'].includes(query)) {
        return [{ symbol: query, quoteType: 'EQUITY' }];
      }
      return [];
    });
    
    const result = await validateTickerSample(['AAPL', 'NVDA', 'MSFT', 'XXXX', 'YYYY']);
    
    expect(result.total).toBe(5);
    expect(result.valid).toBe(3);
    expect(result.invalid).toBe(2);
    expect(result.invalidTickers).toContain('XXXX');
    expect(result.invalidTickers).toContain('YYYY');
  });

  test('handles empty input', async () => {
    const result = await validateTickerSample([]);
    
    expect(result.total).toBe(0);
    expect(result.valid).toBe(0);
    expect(result.invalid).toBe(0);
    expect(search).not.toHaveBeenCalled();
  });

  test('handles null input', async () => {
    const result = await validateTickerSample(null);
    
    expect(result.total).toBe(0);
    expect(search).not.toHaveBeenCalled();
  });

  test('handles API errors gracefully', async () => {
    search.mockRejectedValue(new Error('Network error'));
    
    const result = await validateTickerSample(['AAPL']);
    
    expect(result.invalid).toBe(1);
    expect(result.details['AAPL'].error).toContain('Network error');
  });
});

// ============================================================================
// TESTS: validateSingleTicker
// ============================================================================

describe('validateSingleTicker', () => {
  test('returns valid result for exact match', async () => {
    const result = await validateSingleTicker('AAPL');
    
    expect(result.isValid).toBe(true);
    expect(result.normalizedTicker).toBe('AAPL');
    expect(result.companyName).toBe('Apple Inc.');
    expect(result.currency).toBe('USD');
  });

  test('returns invalid for non-existent ticker', async () => {
    search.mockResolvedValue([]);
    
    const result = await validateSingleTicker('XXXXX');
    
    expect(result.isValid).toBe(false);
    expect(result.error).toContain('not found');
  });

  test('detects ETF asset type', async () => {
    const result = await validateSingleTicker('SPY');
    
    expect(result.assetType).toBe('etf');
  });

  test('detects crypto asset type', async () => {
    search.mockResolvedValue([{
      symbol: 'BTC-USD',
      quoteType: 'CRYPTOCURRENCY',
    }]);
    
    const result = await validateSingleTicker('BTC-USD');
    
    expect(result.assetType).toBe('crypto');
  });

  test('handles empty ticker', async () => {
    const result = await validateSingleTicker('');
    
    expect(result.isValid).toBe(false);
    expect(result.error).toBe('Empty ticker');
  });
});

// ============================================================================
// TESTS: normalizeTicker
// ============================================================================

describe('normalizeTicker', () => {
  test('converts to uppercase', () => {
    expect(normalizeTicker('aapl')).toBe('AAPL');
    expect(normalizeTicker('Aapl')).toBe('AAPL');
  });

  test('removes leading $ symbol', () => {
    expect(normalizeTicker('$AAPL')).toBe('AAPL');
  });

  test('trims whitespace', () => {
    expect(normalizeTicker('  AAPL  ')).toBe('AAPL');
    expect(normalizeTicker(' NVDA')).toBe('NVDA');
  });

  test('removes trailing dot', () => {
    expect(normalizeTicker('AAPL.')).toBe('AAPL');
  });

  test('handles null/undefined', () => {
    expect(normalizeTicker(null)).toBe('');
    expect(normalizeTicker(undefined)).toBe('');
  });

  test('preserves valid suffixes', () => {
    expect(normalizeTicker('BRK.B')).toBe('BRK.B');
  });
});

// ============================================================================
// TESTS: detectAssetType
// ============================================================================

describe('detectAssetType', () => {
  test('detects stock from EQUITY quoteType', () => {
    expect(detectAssetType({ quoteType: 'EQUITY' })).toBe('stock');
    expect(detectAssetType({ quoteType: 'equity' })).toBe('stock');
  });

  test('detects ETF from ETF quoteType', () => {
    expect(detectAssetType({ quoteType: 'ETF' })).toBe('etf');
    expect(detectAssetType({ typeDisp: 'ETF' })).toBe('etf');
  });

  test('detects fund as ETF', () => {
    expect(detectAssetType({ quoteType: 'MUTUALFUND' })).toBe('etf');
    expect(detectAssetType({ typeDisp: 'Fund' })).toBe('etf');
  });

  test('detects crypto', () => {
    expect(detectAssetType({ quoteType: 'CRYPTOCURRENCY' })).toBe('crypto');
    expect(detectAssetType({ typeDisp: 'Crypto' })).toBe('crypto');
  });

  test('defaults to stock for unknown types', () => {
    expect(detectAssetType({})).toBe('stock');
    expect(detectAssetType({ quoteType: 'UNKNOWN' })).toBe('stock');
  });
});
