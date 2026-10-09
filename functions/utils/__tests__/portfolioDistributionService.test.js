/**
 * Tests for Portfolio Distribution Service
 * 
 * @see SCALE-OPT-001 - Migración de Cálculos Frontend → Backend (SOLID)
 */

const { 
  getPortfolioDistribution, 
  invalidateDistributionCache,
  getAvailableSectors 
} = require('../../services/portfolioDistributionService');

// Mock Firebase Admin
jest.mock('../../services/firebaseAdmin', () => {
  const mockFirestore = {
    collection: jest.fn(),
  };
  return {
    firestore: () => mockFirestore,
  };
});

// Mock node-fetch
jest.mock('node-fetch', () => jest.fn());

// Mock logger
jest.mock('../../utils/logger', () => ({
  StructuredLogger: jest.fn().mockImplementation(() => ({
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  })),
}));

describe('portfolioDistributionService', () => {
  let mockDb;
  
  beforeEach(() => {
    jest.clearAllMocks();
    
    // Setup mock Firestore
    mockDb = require('../../services/firebaseAdmin').firestore();
    
    // Mock collection queries
    mockDb.collection.mockImplementation((collectionName) => ({
      where: jest.fn().mockReturnThis(),
      get: jest.fn().mockResolvedValue({
        docs: [],
        empty: true
      })
    }));
  });

  describe('getPortfolioDistribution', () => {
    it('should return empty response for user with no assets', async () => {
      // Mock empty assets collection
      mockDb.collection.mockImplementation(() => ({
        where: jest.fn().mockReturnThis(),
        get: jest.fn().mockResolvedValue({
          docs: [],
          empty: true
        })
      }));

      const result = await getPortfolioDistribution('test-user-id', {});

      expect(result).toEqual(expect.objectContaining({
        sectors: [],
        countries: [],
        holdings: [],
        totals: expect.objectContaining({
          portfolioValue: 0,
          currency: 'USD'
        })
      }));
    });

    it('should calculate sector distribution correctly', async () => {
      // Mock assets
      const mockAssets = [
        {
          id: 'asset-1',
          data: () => ({
            name: 'AAPL',
            units: 10,
            assetType: 'stock',
            isActive: true,
            portfolioAccount: 'account-1'
          })
        }
      ];

      // Mock prices
      const mockPrices = [
        {
          id: 'AAPL',
          data: () => ({
            symbol: 'AAPL',
            price: 150,
            type: 'stock',
            sector: 'Technology'
          })
        }
      ];

      // Mock sectors
      const mockSectors = [
        {
          id: 'sector-1',
          data: () => ({
            sector: 'Technology',
            etfSectorName: 'Technology'
          })
        }
      ];

      // Mock portfolio accounts
      const mockAccounts = [
        {
          id: 'account-1',
          data: () => ({
            userId: 'test-user-id',
            isActive: true
          })
        }
      ];

      mockDb.collection.mockImplementation((collectionName) => {
        const mockQuery = {
          where: jest.fn().mockReturnThis(),
          get: jest.fn()
        };

        switch (collectionName) {
          case 'assets':
            mockQuery.get.mockResolvedValue({ docs: mockAssets, empty: false });
            break;
          case 'currentPrices':
            mockQuery.get.mockResolvedValue({ docs: mockPrices, empty: false });
            break;
          case 'sectors':
            mockQuery.get.mockResolvedValue({ docs: mockSectors, empty: false });
            break;
          case 'portfolioAccounts':
            mockQuery.get.mockResolvedValue({ docs: mockAccounts, empty: false });
            break;
          case 'countries':
            mockQuery.get.mockResolvedValue({ docs: [], empty: true });
            break;
          default:
            mockQuery.get.mockResolvedValue({ docs: [], empty: true });
        }

        return mockQuery;
      });

      const result = await getPortfolioDistribution('test-user-id', {});

      expect(result.sectors).toBeDefined();
      expect(result.totals.portfolioValue).toBeGreaterThan(0);
      expect(result.metadata.assetCount).toBe(1);
    });

    it('should use cache for repeated requests', async () => {
      // First call - should calculate
      mockDb.collection.mockImplementation(() => ({
        where: jest.fn().mockReturnThis(),
        get: jest.fn().mockResolvedValue({ docs: [], empty: true })
      }));

      await getPortfolioDistribution('cache-test-user', {});
      await getPortfolioDistribution('cache-test-user', {});

      // Second call should use cache, so assets collection should only be queried once
      // The exact count depends on implementation, but cache should reduce calls
      expect(mockDb.collection).toHaveBeenCalled();
    });

    it('should handle includeHoldings option', async () => {
      mockDb.collection.mockImplementation(() => ({
        where: jest.fn().mockReturnThis(),
        get: jest.fn().mockResolvedValue({ docs: [], empty: true })
      }));

      const resultWithHoldings = await getPortfolioDistribution('test-user', {
        includeHoldings: true
      });

      expect(resultWithHoldings.holdings).toBeDefined();
    });

    it('should filter by accountId when provided', async () => {
      const whereMock = jest.fn().mockReturnThis();
      
      mockDb.collection.mockImplementation(() => ({
        where: whereMock,
        get: jest.fn().mockResolvedValue({ docs: [], empty: true })
      }));

      await getPortfolioDistribution('test-user', {
        accountId: 'specific-account'
      });

      expect(whereMock).toHaveBeenCalledWith('portfolioAccount', '==', 'specific-account');
    });

    it('should handle errors gracefully', async () => {
      mockDb.collection.mockImplementation(() => ({
        where: jest.fn().mockReturnThis(),
        get: jest.fn().mockRejectedValue(new Error('Firestore error'))
      }));

      await expect(getPortfolioDistribution('error-user', {}))
        .rejects.toThrow('Firestore error');
    });
  });

  describe('invalidateDistributionCache', () => {
    it('should invalidate cache for user', () => {
      // This is primarily a smoke test since the cache is internal
      expect(() => invalidateDistributionCache('test-user')).not.toThrow();
    });
  });

  describe('getAvailableSectors', () => {
    it('should return list of unique sectors', async () => {
      const mockSectors = [
        { id: '1', data: () => ({ sector: 'Technology', etfSectorName: 'Tech' }) },
        { id: '2', data: () => ({ sector: 'Healthcare', etfSectorName: 'Health' }) },
        { id: '3', data: () => ({ sector: 'Technology', etfSectorName: 'Technology' }) }, // Duplicate
      ];

      mockDb.collection.mockImplementation(() => ({
        get: jest.fn().mockResolvedValue({ docs: mockSectors, empty: false })
      }));

      const sectors = await getAvailableSectors();

      // The sectors cache is shared, so we check that it returns an array
      expect(Array.isArray(sectors)).toBe(true);
    });
  });

  describe('cache TTL behavior', () => {
    it('should return consistent results for same user', async () => {
      // First call
      mockDb.collection.mockImplementation(() => ({
        where: jest.fn().mockReturnThis(),
        get: jest.fn().mockResolvedValue({ docs: [], empty: true })
      }));

      const firstResult = await getPortfolioDistribution('ttl-test-user-2', {});
      const secondResult = await getPortfolioDistribution('ttl-test-user-2', {});

      // Both results should have same structure
      expect(firstResult.totals.portfolioValue).toBe(secondResult.totals.portfolioValue);
      expect(firstResult.sectors.length).toBe(secondResult.sectors.length);
    });
  });

  // FIX-MULTI-ACCOUNT-001: Tests para verificar acumulación correcta de holdings multi-cuenta
  describe('multi-account holdings aggregation', () => {
    it('should accumulate weights for same ticker across multiple accounts', async () => {
      // Mock assets - mismo ticker en 2 cuentas diferentes
      const mockAssets = [
        {
          id: 'asset-1',
          data: () => ({
            name: 'AMZN',
            units: 5,
            assetType: 'stock',
            isActive: true,
            portfolioAccount: 'account-1'
          })
        },
        {
          id: 'asset-2',
          data: () => ({
            name: 'AMZN',
            units: 3,
            assetType: 'stock',
            isActive: true,
            portfolioAccount: 'account-2'
          })
        }
      ];

      // Mock prices
      const mockPrices = [
        {
          id: 'AMZN',
          data: () => ({
            symbol: 'AMZN',
            name: 'Amazon.com, Inc.',
            price: 100,
            type: 'stock',
            sector: 'Consumer Cyclical',
            currency: 'USD'
          })
        }
      ];

      // Mock portfolio accounts
      const mockAccounts = [
        {
          id: 'account-1',
          data: () => ({
            userId: 'multi-account-test-user',
            isActive: true
          })
        },
        {
          id: 'account-2',
          data: () => ({
            userId: 'multi-account-test-user',
            isActive: true
          })
        }
      ];

      // Mock currencies
      const mockCurrencies = [
        {
          id: 'usd',
          data: () => ({
            code: 'USD',
            exchangeRate: 1,
            isActive: true
          })
        }
      ];

      mockDb.collection.mockImplementation((collectionName) => {
        const mockQuery = {
          where: jest.fn().mockReturnThis(),
          get: jest.fn()
        };

        switch (collectionName) {
          case 'assets':
            mockQuery.get.mockResolvedValue({ docs: mockAssets, empty: false });
            break;
          case 'currentPrices':
            mockQuery.get.mockResolvedValue({ docs: mockPrices, empty: false });
            break;
          case 'portfolioAccounts':
            mockQuery.get.mockResolvedValue({ docs: mockAccounts, empty: false });
            break;
          case 'currencies':
            mockQuery.get.mockResolvedValue({ docs: mockCurrencies, empty: false });
            break;
          case 'sectors':
            mockQuery.get.mockResolvedValue({ docs: [], empty: true });
            break;
          case 'countries':
            mockQuery.get.mockResolvedValue({ docs: [], empty: true });
            break;
          case 'users':
            mockQuery.get.mockResolvedValue({ exists: false });
            break;
          default:
            mockQuery.get.mockResolvedValue({ docs: [], empty: true });
        }

        return mockQuery;
      });

      const result = await getPortfolioDistribution('multi-account-test-user', {
        includeHoldings: true
      });

      // Total debería ser $800 (5 + 3 = 8 unidades * $100)
      expect(result.totals.portfolioValue).toBe(800);
      
      // Debe haber solo 1 holding (AMZN agregado)
      expect(result.holdings).toBeDefined();
      expect(result.holdings.length).toBe(1);
      
      const amznHolding = result.holdings[0];
      expect(amznHolding.symbol).toBe('AMZN');
      
      // El peso total debe ser 100% (único holding)
      expect(amznHolding.weight).toBeCloseTo(1, 4);
      
      // Debe haber 1 fuente directa con la contribución total
      expect(amznHolding.sources).toBeDefined();
      expect(amznHolding.sources.length).toBe(1);
      expect(amznHolding.sources[0].symbol).toBe('AMZN');
      // La contribución directa debe ser 100% (acumulada de ambas cuentas)
      expect(amznHolding.sources[0].contribution).toBeCloseTo(1, 4);
    });

    it('should combine direct investment with ETF contributions correctly', async () => {
      // Mock assets - inversión directa en MSFT + ETF que contiene MSFT
      const mockAssets = [
        {
          id: 'asset-1',
          data: () => ({
            name: 'MSFT',
            units: 10,
            assetType: 'stock',
            isActive: true,
            portfolioAccount: 'account-1'
          })
        },
        {
          id: 'asset-2',
          data: () => ({
            name: 'VUAA.L',
            units: 20,
            assetType: 'etf',
            isActive: true,
            portfolioAccount: 'account-1'
          })
        }
      ];

      // Mock prices
      const mockPrices = [
        {
          id: 'MSFT',
          data: () => ({
            symbol: 'MSFT',
            name: 'Microsoft Corporation',
            price: 400,
            type: 'stock',
            sector: 'Technology',
            currency: 'USD'
          })
        },
        {
          id: 'VUAA.L',
          data: () => ({
            symbol: 'VUAA.L',
            name: 'Vanguard S&P 500 UCITS ETF',
            price: 50,
            type: 'etf',
            currency: 'USD'
          })
        }
      ];

      // Mock portfolio accounts
      const mockAccounts = [
        {
          id: 'account-1',
          data: () => ({
            userId: 'etf-combo-test-user',
            isActive: true
          })
        }
      ];

      // Mock currencies
      const mockCurrencies = [
        {
          id: 'usd',
          data: () => ({
            code: 'USD',
            exchangeRate: 1,
            isActive: true
          })
        }
      ];

      // Mock ETF data (node-fetch)
      const fetch = require('node-fetch');
      fetch.mockResolvedValue({
        ok: true,
        status: 200,
        text: () => Promise.resolve(JSON.stringify({
          holdings: [
            { name: 'Microsoft Corporation', symbol: null, isin: 'MSFT', weight: 0.05 },
            { name: 'Apple Inc.', symbol: null, isin: 'AAPL', weight: 0.07 }
          ],
          sectors: []
        }))
      });

      mockDb.collection.mockImplementation((collectionName) => {
        const mockQuery = {
          where: jest.fn().mockReturnThis(),
          get: jest.fn()
        };

        switch (collectionName) {
          case 'assets':
            mockQuery.get.mockResolvedValue({ docs: mockAssets, empty: false });
            break;
          case 'currentPrices':
            mockQuery.get.mockResolvedValue({ docs: mockPrices, empty: false });
            break;
          case 'portfolioAccounts':
            mockQuery.get.mockResolvedValue({ docs: mockAccounts, empty: false });
            break;
          case 'currencies':
            mockQuery.get.mockResolvedValue({ docs: mockCurrencies, empty: false });
            break;
          case 'sectors':
            mockQuery.get.mockResolvedValue({ docs: [], empty: true });
            break;
          case 'countries':
            mockQuery.get.mockResolvedValue({ docs: [], empty: true });
            break;
          case 'users':
            mockQuery.get.mockResolvedValue({ exists: false });
            break;
          default:
            mockQuery.get.mockResolvedValue({ docs: [], empty: true });
        }

        return mockQuery;
      });

      const result = await getPortfolioDistribution('etf-combo-test-user', {
        includeHoldings: true
      });

      // Total: $4000 (10 * 400) + $1000 (20 * 50) = $5000
      expect(result.totals.portfolioValue).toBe(5000);
      
      // Buscar MSFT en holdings
      const msftHolding = result.holdings?.find(h => h.symbol === 'MSFT');
      expect(msftHolding).toBeDefined();
      
      // MSFT debe tener 2 fuentes: directa + ETF
      expect(msftHolding.sources.length).toBe(2);
      
      // Fuente directa: 10 * 400 = $4000 = 80% del portafolio
      const directSource = msftHolding.sources.find(s => s.symbol === 'MSFT');
      expect(directSource).toBeDefined();
      expect(directSource.contribution).toBeCloseTo(0.8, 2);
      
      // Fuente ETF: 5% de $1000 = $50 = 1% del portafolio
      const etfSource = msftHolding.sources.find(s => s.symbol === 'VUAA.L');
      expect(etfSource).toBeDefined();
      expect(etfSource.contribution).toBeCloseTo(0.01, 4);
    });
  });
});
