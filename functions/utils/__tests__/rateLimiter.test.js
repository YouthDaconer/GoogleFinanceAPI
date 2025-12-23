/**
 * Rate Limiter Unit Tests
 * 
 * Tests the rate limiting implementation including:
 * - Request counting within sliding window
 * - Limit enforcement and blocking
 * - Rate limit info in responses
 * - Error format validation
 * 
 * @see SCALE-BE-004 - Rate Limiting Implementation
 */

const { RateLimiter, withRateLimit, RATE_LIMITS_COLLECTION } = require('../rateLimiter');
const { getRateLimitConfig, RATE_LIMITS } = require('../../config/rateLimits');

// Mock Firebase Admin and Firestore
jest.mock('../../services/firebaseAdmin', () => {
  const mockTransaction = {
    get: jest.fn(),
    set: jest.fn(),
  };
  
  const mockDoc = {
    get: jest.fn(),
  };
  
  const mockCollection = {
    doc: jest.fn(() => mockDoc),
  };
  
  const mockDb = {
    collection: jest.fn(() => mockCollection),
    runTransaction: jest.fn((callback) => callback(mockTransaction)),
  };
  
  return {
    firestore: () => mockDb,
    __mockDb: mockDb,
    __mockTransaction: mockTransaction,
    __mockDoc: mockDoc,
    __mockCollection: mockCollection,
  };
});

// Mock the logger
jest.mock('../logger', () => ({
  StructuredLogger: jest.fn().mockImplementation(() => ({
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  })),
}));

describe('RateLimiter', () => {
  let rateLimiter;
  let mockAdmin;
  
  beforeEach(() => {
    jest.clearAllMocks();
    mockAdmin = require('../../services/firebaseAdmin');
    rateLimiter = new RateLimiter();
  });

  describe('Constructor', () => {
    it('should use default values when no options provided', () => {
      const limiter = new RateLimiter();
      expect(limiter.defaultLimit).toBe(30);
      expect(limiter.defaultWindowMs).toBe(60000);
    });

    it('should accept custom default values', () => {
      const limiter = new RateLimiter({ defaultLimit: 50, defaultWindowMs: 120000 });
      expect(limiter.defaultLimit).toBe(50);
      expect(limiter.defaultWindowMs).toBe(120000);
    });
  });

  describe('checkLimit', () => {
    it('should allow first request', async () => {
      const mockDocData = { exists: false };
      mockAdmin.__mockTransaction.get.mockResolvedValue(mockDocData);
      
      const result = await rateLimiter.checkLimit('user123', 'testFunction', { limit: 10 });
      
      expect(result.limit).toBe(10);
      expect(result.remaining).toBe(9);
      expect(result.reset).toBeDefined();
    });

    it('should allow requests within limit', async () => {
      const now = Date.now();
      const mockDocData = { 
        exists: true, 
        data: () => ({ 
          requests: [now - 1000, now - 2000, now - 3000],
          lastUpdated: now - 1000,
        }),
      };
      mockAdmin.__mockTransaction.get.mockResolvedValue(mockDocData);
      
      const result = await rateLimiter.checkLimit('user123', 'testFunction', { limit: 10 });
      
      expect(result.remaining).toBe(6);
    });

    it('should throw HttpsError when limit exceeded', async () => {
      const now = Date.now();
      const requests = Array(10).fill(0).map((_, i) => now - (i * 1000));
      
      const mockDocData = { 
        exists: true, 
        data: () => ({ 
          requests,
          lastUpdated: now - 1000,
        }),
      };
      mockAdmin.__mockTransaction.get.mockResolvedValue(mockDocData);
      
      await expect(
        rateLimiter.checkLimit('user123', 'testFunction', { limit: 10 })
      ).rejects.toMatchObject({
        code: 'resource-exhausted',
        message: 'Rate limit exceeded. Please try again later.',
      });
    });

    it('should include retryAfter in error details', async () => {
      const now = Date.now();
      const requests = Array(10).fill(0).map((_, i) => now - (i * 1000));
      
      const mockDocData = { 
        exists: true, 
        data: () => ({ requests }),
      };
      mockAdmin.__mockTransaction.get.mockResolvedValue(mockDocData);
      
      try {
        await rateLimiter.checkLimit('user123', 'testFunction', { limit: 10, windowMs: 60000 });
        fail('Should have thrown');
      } catch (error) {
        expect(error.details).toHaveProperty('retryAfter');
        expect(error.details).toHaveProperty('limit', 10);
        expect(error.details).toHaveProperty('remaining', 0);
      }
    });

    it('should filter out expired requests', async () => {
      const now = Date.now();
      const oldRequests = [now - 120000, now - 180000];
      const recentRequests = [now - 1000, now - 2000];
      
      const mockDocData = { 
        exists: true, 
        data: () => ({ 
          requests: [...oldRequests, ...recentRequests],
        }),
      };
      mockAdmin.__mockTransaction.get.mockResolvedValue(mockDocData);
      
      const result = await rateLimiter.checkLimit('user123', 'testFunction', { 
        limit: 10, 
        windowMs: 60000,
      });
      
      expect(result.remaining).toBe(7);
    });
  });

  describe('getInfo', () => {
    it('should return full limit when no previous requests', async () => {
      mockAdmin.__mockDoc.get.mockResolvedValue({ exists: false });
      
      const result = await rateLimiter.getInfo('user123', 'testFunction', { limit: 10 });
      
      expect(result.limit).toBe(10);
      expect(result.remaining).toBe(10);
    });

    it('should return correct remaining count', async () => {
      const now = Date.now();
      mockAdmin.__mockDoc.get.mockResolvedValue({ 
        exists: true, 
        data: () => ({ 
          requests: [now - 1000, now - 2000, now - 3000],
        }),
      });
      
      const result = await rateLimiter.getInfo('user123', 'testFunction', { limit: 10 });
      
      expect(result.remaining).toBe(7);
    });
  });
});

describe('withRateLimit', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should throw unauthenticated error when no auth', async () => {
    const handler = jest.fn();
    const wrappedHandler = withRateLimit('testFunction')(handler);
    
    await expect(wrappedHandler({ auth: null }))
      .rejects.toMatchObject({ code: 'unauthenticated' });
    
    expect(handler).not.toHaveBeenCalled();
  });
});

describe('getRateLimitConfig', () => {
  it('should return config for known function', () => {
    const config = getRateLimitConfig('getHistoricalReturns');
    expect(config.limit).toBe(15);
    expect(config.windowMs).toBe(60000);
  });

  it('should return default config for unknown function', () => {
    const config = getRateLimitConfig('unknownFunction');
    expect(config.limit).toBe(30);
    expect(config.windowMs).toBe(60000);
  });

  it('should have config for all 22 functions', () => {
    const expectedFunctions = [
      'getHistoricalReturns', 'getMultiAccountHistoricalReturns',
      'createAsset', 'sellAsset', 'sellPartialAssetsFIFO', 'addCashTransaction',
      'updateAsset', 'deleteAsset', 'deleteAssets', 'updateStockSector',
      'getCurrentPricesForUser', 'getIndexHistory',
      'addCurrency', 'updateCurrency', 'deleteCurrency', 
      'updateDefaultCurrency', 'updateUserCountry', 'updateUserDisplayName',
      'addPortfolioAccount', 'updatePortfolioAccount', 
      'deletePortfolioAccount', 'updatePortfolioAccountBalance',
    ];
    
    expectedFunctions.forEach(fn => {
      expect(RATE_LIMITS[fn]).toBeDefined();
      expect(RATE_LIMITS[fn].limit).toBeGreaterThan(0);
      expect(RATE_LIMITS[fn].windowMs).toBe(60000);
    });
    
    expect(Object.keys(RATE_LIMITS).length).toBe(22);
  });
});
