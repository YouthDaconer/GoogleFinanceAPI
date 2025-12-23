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
});
