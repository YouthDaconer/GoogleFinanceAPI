/**
 * Tests para dailyEODSnapshot
 * 
 * @see docs/stories/84.story.md (OPT-DEMAND-301)
 */

// ============================================================================
// Mocks
// ============================================================================

// Mock de firebase-admin
const mockBatch = {
  update: jest.fn(),
  commit: jest.fn().mockResolvedValue(),
};

const mockDocRef = { id: 'test-doc' };

jest.mock('firebase-admin', () => ({
  firestore: Object.assign(jest.fn(() => ({})), {
    FieldValue: {
      serverTimestamp: jest.fn(() => 'mock-server-timestamp'),
    },
  }),
  initializeApp: jest.fn(),
}));

// Mock de axios
jest.mock('axios', () => ({
  get: jest.fn(),
}));

// Mock de StructuredLogger
jest.mock('../../utils/logger', () => ({
  StructuredLogger: jest.fn().mockImplementation(() => ({
    info: jest.fn(),
    debug: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  })),
}));

// ============================================================================
// Tests
// ============================================================================

describe('dailyEODSnapshot (OPT-DEMAND-301)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('getAllSymbols', () => {
    it('should return unique symbols from currentPrices', async () => {
      // Arrange
      const mockDocs = [
        { data: () => ({ symbol: 'AAPL' }) },
        { data: () => ({ symbol: 'VOO' }) },
        { data: () => ({ symbol: 'AAPL' }) }, // Duplicate
        { data: () => ({ symbol: 'MSFT' }) },
      ];
      
      const mockSnapshot = { docs: mockDocs };
      
      const db = {
        collection: jest.fn(() => ({
          get: jest.fn().mockResolvedValue(mockSnapshot),
        })),
      };
      
      // Act
      const { getAllSymbols } = require('../dailyEODSnapshot');
      const symbols = await getAllSymbols(db);
      
      // Assert
      expect(symbols).toHaveLength(3);
      expect(symbols).toContain('AAPL');
      expect(symbols).toContain('VOO');
      expect(symbols).toContain('MSFT');
    });

    it('should filter out empty symbols', async () => {
      const mockDocs = [
        { data: () => ({ symbol: 'AAPL' }) },
        { data: () => ({ symbol: '' }) },
        { data: () => ({ symbol: null }) },
        { data: () => ({ noSymbol: true }) },
      ];
      
      const db = {
        collection: jest.fn(() => ({
          get: jest.fn().mockResolvedValue({ docs: mockDocs }),
        })),
      };
      
      const { getAllSymbols } = require('../dailyEODSnapshot');
      const symbols = await getAllSymbols(db);
      
      expect(symbols).toHaveLength(1);
      expect(symbols).toContain('AAPL');
    });
  });

  describe('getActiveCurrencies', () => {
    it('should return currencies with isActive=true', async () => {
      const mockDocs = [
        { 
          ref: { id: 'cop-doc' },
          data: () => ({ code: 'COP', name: 'Peso Colombiano', isActive: true }) 
        },
        { 
          ref: { id: 'usd-doc' },
          data: () => ({ code: 'USD', name: 'US Dollar', isActive: true }) 
        },
      ];
      
      const db = {
        collection: jest.fn(() => ({
          where: jest.fn(() => ({
            get: jest.fn().mockResolvedValue({ docs: mockDocs }),
          })),
        })),
      };
      
      const { getActiveCurrencies } = require('../dailyEODSnapshot');
      const currencies = await getActiveCurrencies(db);
      
      expect(currencies).toHaveLength(2);
      expect(currencies[0].code).toBe('COP');
      expect(currencies[1].code).toBe('USD');
    });

    it('should return empty array when no active currencies', async () => {
      const db = {
        collection: jest.fn(() => ({
          where: jest.fn(() => ({
            get: jest.fn().mockResolvedValue({ docs: [] }),
          })),
        })),
      };
      
      const { getActiveCurrencies } = require('../dailyEODSnapshot');
      const currencies = await getActiveCurrencies(db);
      
      expect(currencies).toHaveLength(0);
    });
  });

  describe('fetchQuotesFromAPI', () => {
    it('should return empty map for empty symbols', async () => {
      const { fetchQuotesFromAPI } = require('../dailyEODSnapshot');
      const quotes = await fetchQuotesFromAPI([]);
      
      expect(quotes.size).toBe(0);
    });

    it('should batch requests for large symbol lists', async () => {
      const axios = require('axios');
      axios.get.mockResolvedValue({
        data: [{ symbol: 'AAPL', regularMarketPrice: 185 }],
      });
      
      const { fetchQuotesFromAPI } = require('../dailyEODSnapshot');
      
      // 150 símbolos = 2 batches de 100
      const symbols = Array(150).fill(0).map((_, i) => `SYM${i}`);
      await fetchQuotesFromAPI(symbols);
      
      expect(axios.get).toHaveBeenCalledTimes(2);
    });

    it('should handle API errors gracefully', async () => {
      const axios = require('axios');
      axios.get.mockRejectedValue(new Error('Network error'));
      
      const { fetchQuotesFromAPI } = require('../dailyEODSnapshot');
      
      // No debe lanzar excepción
      const quotes = await fetchQuotesFromAPI(['AAPL', 'VOO']);
      
      expect(quotes.size).toBe(0);
    });

    it('should map quotes correctly', async () => {
      const axios = require('axios');
      axios.get.mockResolvedValue({
        data: [
          { symbol: 'AAPL', regularMarketPrice: 185.50 },
          { symbol: 'VOO', regularMarketPrice: 450.25 },
        ],
      });
      
      const { fetchQuotesFromAPI } = require('../dailyEODSnapshot');
      const quotes = await fetchQuotesFromAPI(['AAPL', 'VOO']);
      
      expect(quotes.size).toBe(2);
      expect(quotes.get('AAPL').regularMarketPrice).toBe(185.50);
      expect(quotes.get('VOO').regularMarketPrice).toBe(450.25);
    });
  });

  describe('updateCurrentPricesSnapshot', () => {
    it('should preserve existing metadata', async () => {
      const mockDoc = {
        ref: mockDocRef,
        data: () => ({
          symbol: 'AAPL',
          name: 'Apple Inc',
          logo: 'https://logo.com/aapl.png',
          isin: 'US0378331005',
          sector: 'Technology',
          industry: 'Consumer Electronics',
        }),
      };
      
      const db = {
        collection: jest.fn(() => ({
          get: jest.fn().mockResolvedValue({ docs: [mockDoc] }),
        })),
        batch: jest.fn(() => mockBatch),
      };
      
      const quotes = new Map([
        ['AAPL', { 
          regularMarketPrice: 185,
          regularMarketChange: 2,
          regularMarketChangePercent: 1.1,
          currency: 'USD',
        }],
      ]);
      
      const { updateCurrentPricesSnapshot } = require('../dailyEODSnapshot');
      const result = await updateCurrentPricesSnapshot(db, quotes);
      
      expect(result.updated).toBe(1);
      expect(result.failed).toBe(0);
      expect(mockBatch.update).toHaveBeenCalled();
      
      const updateCall = mockBatch.update.mock.calls[0][1];
      expect(updateCall.name).toBe('Apple Inc');
      expect(updateCall.logo).toBe('https://logo.com/aapl.png');
      expect(updateCall.isin).toBe('US0378331005');
      expect(updateCall.sector).toBe('Technology');
      expect(updateCall.price).toBe(185);
      expect(updateCall.snapshotType).toBe('eod');
    });

    it('should count failed updates for missing quotes', async () => {
      const mockDocs = [
        { ref: mockDocRef, data: () => ({ symbol: 'AAPL' }) },
        { ref: mockDocRef, data: () => ({ symbol: 'UNKNOWN' }) },
      ];
      
      const db = {
        collection: jest.fn(() => ({
          get: jest.fn().mockResolvedValue({ docs: mockDocs }),
        })),
        batch: jest.fn(() => mockBatch),
      };
      
      const quotes = new Map([
        ['AAPL', { regularMarketPrice: 185 }],
        // 'UNKNOWN' no tiene quote
      ]);
      
      const { updateCurrentPricesSnapshot } = require('../dailyEODSnapshot');
      const result = await updateCurrentPricesSnapshot(db, quotes);
      
      expect(result.updated).toBe(1);
      expect(result.failed).toBe(1);
    });
  });

  describe('updateCurrenciesSnapshot', () => {
    it('should update currencies with exchange rate', async () => {
      const currencies = [
        { 
          code: 'COP', 
          ref: mockDocRef, 
          data: { code: 'COP', name: 'Peso Colombiano', symbol: '$' } 
        },
      ];
      
      const quotes = new Map([
        ['COP=X', { regularMarketPrice: 4200.50 }],
      ]);
      
      const db = {
        batch: jest.fn(() => mockBatch),
      };
      
      const { updateCurrenciesSnapshot } = require('../dailyEODSnapshot');
      const result = await updateCurrenciesSnapshot(db, quotes, currencies);
      
      expect(result.updated).toBe(1);
      expect(result.failed).toBe(0);
      expect(mockBatch.update).toHaveBeenCalled();
    });

    it('should return zeros for empty currencies', async () => {
      const db = { batch: jest.fn(() => mockBatch) };
      
      const { updateCurrenciesSnapshot } = require('../dailyEODSnapshot');
      const result = await updateCurrenciesSnapshot(db, new Map(), []);
      
      expect(result.updated).toBe(0);
      expect(result.failed).toBe(0);
    });
  });

  describe('getSnapshotType', () => {
    it('should return valid snapshot type', () => {
      const { getSnapshotType } = require('../dailyEODSnapshot');
      const type = getSnapshotType();
      
      expect(['pre-market', 'post-market', 'unknown']).toContain(type);
    });
  });
});
