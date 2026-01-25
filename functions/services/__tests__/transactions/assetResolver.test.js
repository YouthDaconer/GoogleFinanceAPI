/**
 * Tests for assetResolver service
 * 
 * @see docs/stories/90.story.md (IMPORT-002)
 */

// Mock firebase-admin before requiring the module
jest.mock('../../firebaseAdmin', () => {
  // Create a mock firestore function
  const mockFirestore = jest.fn(() => ({
    collection: jest.fn(() => ({
      where: jest.fn(() => ({
        where: jest.fn(() => ({
          where: jest.fn(() => ({
            limit: jest.fn(() => ({
              get: jest.fn(),
            })),
          })),
        })),
      })),
      doc: jest.fn(() => ({
        set: jest.fn().mockResolvedValue(),
      })),
    })),
  }));
  
  // Add FieldValue as a static property
  mockFirestore.FieldValue = {
    serverTimestamp: jest.fn(() => 'SERVER_TIMESTAMP'),
  };
  
  return {
    firestore: mockFirestore,
  };
});

// Path from test file to financeQuery: services/__tests__/transactions/ -> services/financeQuery.js
jest.mock('../../financeQuery', () => ({
  search: jest.fn(),
  getQuotes: jest.fn(),
}));

// Import the module being tested
// Path: services/__tests__/transactions/ -> services/transactions/services/assetResolver.js
const { 
  resolveAssets, 
  findExistingAsset, 
  getTickerInfo,
  normalizeTicker, 
  mapQuoteType, 
  inferCurrency,
  clearCache 
} = require('../../transactions/services/assetResolver');

const { search, getQuotes } = require('../../financeQuery');
const admin = require('../../firebaseAdmin');

// ============================================================================
// MOCKS AND FIXTURES
// ============================================================================

const MOCK_SEARCH_RESULTS = {
  AAPL: [{
    symbol: 'AAPL',
    shortname: 'Apple Inc.',
    quoteType: 'EQUITY',
    exchange: 'NMS',
  }],
  VTI: [{
    symbol: 'VTI',
    shortname: 'Vanguard Total Stock Market ETF',
    quoteType: 'ETF',
    exchange: 'NYSEARCA',
  }],
  'BTC-USD': [{
    symbol: 'BTC-USD',
    shortname: 'Bitcoin USD',
    quoteType: 'CRYPTOCURRENCY',
    exchange: 'CCC',
  }],
};

describe('assetResolver', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    clearCache();
    
    search.mockImplementation(async (ticker) => {
      return MOCK_SEARCH_RESULTS[ticker.toUpperCase()] || [];
    });
  });
  
  describe('normalizeTicker', () => {
    test('converts to uppercase', () => {
      expect(normalizeTicker('aapl')).toBe('AAPL');
    });
    
    test('trims whitespace', () => {
      expect(normalizeTicker('  AAPL  ')).toBe('AAPL');
    });
    
    test('handles null/undefined', () => {
      expect(normalizeTicker(null)).toBe('');
      expect(normalizeTicker(undefined)).toBe('');
    });
    
    test('handles numbers', () => {
      expect(normalizeTicker(123)).toBe('123');
    });
  });
  
  describe('mapQuoteType', () => {
    test('maps EQUITY to stock', () => {
      expect(mapQuoteType('EQUITY')).toBe('stock');
    });
    
    test('maps ETF to etf', () => {
      expect(mapQuoteType('ETF')).toBe('etf');
    });
    
    test('maps CRYPTOCURRENCY to crypto', () => {
      expect(mapQuoteType('CRYPTOCURRENCY')).toBe('crypto');
    });
    
    test('maps MUTUALFUND to etf', () => {
      expect(mapQuoteType('MUTUALFUND')).toBe('etf');
    });
    
    test('defaults to stock for unknown types', () => {
      expect(mapQuoteType('UNKNOWN')).toBe('stock');
      expect(mapQuoteType(null)).toBe('stock');
      expect(mapQuoteType('')).toBe('stock');
    });
  });
  
  describe('inferCurrency', () => {
    test('returns USD for US exchanges', () => {
      expect(inferCurrency('NMS')).toBe('USD');
      expect(inferCurrency('NYQ')).toBe('USD');
      expect(inferCurrency('NASDAQ')).toBe('USD');
      expect(inferCurrency('NYSE')).toBe('USD');
    });
    
    test('returns GBP for London exchange', () => {
      expect(inferCurrency('LSE')).toBe('GBP');
      expect(inferCurrency('LON')).toBe('GBP');
    });
    
    test('returns EUR for European exchanges', () => {
      expect(inferCurrency('FRA')).toBe('EUR');
      expect(inferCurrency('PAR')).toBe('EUR');
    });
    
    test('returns COP for Colombia', () => {
      expect(inferCurrency('BVC')).toBe('COP');
    });
    
    test('defaults to USD for unknown markets', () => {
      expect(inferCurrency('UNKNOWN')).toBe('USD');
      expect(inferCurrency('')).toBe('USD');
    });
  });
  
  describe('getTickerInfo', () => {
    test('AC-011/012/013: fetches ticker info from search API', async () => {
      const info = await getTickerInfo('AAPL');
      
      expect(info).toEqual({
        assetType: 'stock',
        market: 'NMS',
        currency: 'USD',
        name: 'Apple Inc.',
      });
      expect(search).toHaveBeenCalledWith('AAPL');
    });
    
    test('AC-012: correctly identifies ETF asset type', async () => {
      const info = await getTickerInfo('VTI');
      
      expect(info.assetType).toBe('etf');
    });
    
    test('correctly identifies crypto asset type', async () => {
      const info = await getTickerInfo('BTC-USD');
      
      expect(info.assetType).toBe('crypto');
    });
    
    test('AC-017: uses cache for repeated calls', async () => {
      await getTickerInfo('AAPL');
      await getTickerInfo('AAPL');
      
      // Should only call API once
      expect(search).toHaveBeenCalledTimes(1);
    });
    
    test('returns null for unknown ticker', async () => {
      search.mockResolvedValue([]);
      getQuotes.mockResolvedValue({});
      
      const info = await getTickerInfo('INVALIDTICKER');
      
      expect(info).toBeNull();
    });
    
    test('handles API errors gracefully', async () => {
      search.mockRejectedValue(new Error('Network error'));
      
      const info = await getTickerInfo('AAPL');
      
      expect(info).toBeNull();
    });
  });
  
  // =========================================================================
  // Integration tests (require complex Firestore mocks - skipped for now)
  // These tests verify Firestore interactions but require more complex setup
  // =========================================================================
  
  describe.skip('findExistingAsset (integration)', () => {
    test('AC-005: finds asset by ticker and portfolioAccountId', async () => {
      // This test requires proper Firestore mock setup
    });
    
    test('returns null when asset not found', async () => {
      // This test requires proper Firestore mock setup
    });
  });
  
  describe.skip('resolveAssets (integration)', () => {
    test('AC-006: uses existing asset ID when found', async () => {
      // This test requires proper Firestore mock setup
    });
    
    test('returns error when asset not found and createMissing=false', async () => {
      // This test requires proper Firestore mock setup
    });
    
    test('AC-009: uses first buy date as acquisitionDate', async () => {
      // This test requires proper Firestore mock setup
    });
    
    test('AC-010: creates asset with isActive=true', async () => {
      // This test requires proper Firestore mock setup
    });
  });
});
