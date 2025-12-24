/**
 * Tests for Asset Handlers
 * 
 * SCALE-CF-001: Unit tests for extracted handlers
 * 
 * @see docs/stories/56.story.md
 */

const { HttpsError } = require('firebase-functions/v2/https');

// Mock Firestore first (before other mocks that depend on it)
const mockBatch = {
  set: jest.fn(),
  update: jest.fn(),
  delete: jest.fn(),
  commit: jest.fn().mockResolvedValue(undefined),
};

const mockDocRef = {
  id: 'test-doc-id',
  get: jest.fn(),
  set: jest.fn().mockResolvedValue(undefined),
  update: jest.fn().mockResolvedValue(undefined),
};

const mockDb = {
  collection: jest.fn().mockReturnThis(),
  doc: jest.fn().mockReturnValue(mockDocRef),
  batch: jest.fn().mockReturnValue(mockBatch),
  where: jest.fn().mockReturnThis(),
  orderBy: jest.fn().mockReturnThis(),
  limit: jest.fn().mockReturnThis(),
  get: jest.fn(),
};

// Mock Firebase Admin - path relative to handlers folder
jest.mock('../../firebaseAdmin', () => ({
  firestore: jest.fn(() => mockDb),
}));

// Mock dependencies - paths relative to handlers folder
jest.mock('../../historicalReturnsService', () => ({
  invalidatePerformanceCache: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../../portfolioDistributionService', () => ({
  invalidateDistributionCache: jest.fn(),
}));

jest.mock('../../financeQuery', () => ({
  getQuotes: jest.fn().mockResolvedValue([{ price: 100, name: 'Test', currency: 'USD' }]),
}));

jest.mock('../../../utils/logoGenerator', () => ({
  generateLogoUrl: jest.fn().mockReturnValue('https://logo.url'),
}));

// Import handlers after mocks
const assetHandlers = require('../assetHandlers');

describe('Asset Handlers', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('createAsset', () => {
    const validPayload = {
      portfolioAccount: 'account-123',
      name: 'AAPL',
      assetType: 'stock',
      currency: 'USD',
      units: 10,
      unitValue: 150,
      acquisitionDate: '2024-01-15',
      commission: 5,
    };

    const mockContext = {
      auth: { uid: 'user-123' },
    };

    it('should throw if required fields are missing', async () => {
      const incompletePayload = { ...validPayload };
      delete incompletePayload.name;

      await expect(
        assetHandlers.createAsset(mockContext, incompletePayload)
      ).rejects.toThrow(HttpsError);
    });

    it('should validate account ownership', async () => {
      mockDocRef.get.mockResolvedValueOnce({
        exists: true,
        data: () => ({ userId: 'different-user' }),
      });

      await expect(
        assetHandlers.createAsset(mockContext, validPayload)
      ).rejects.toThrow('No tienes permiso');
    });

    it('should check sufficient funds', async () => {
      mockDocRef.get.mockResolvedValueOnce({
        exists: true,
        id: 'account-123',
        data: () => ({
          userId: 'user-123',
          balances: { USD: 100 }, // Not enough for 10 * 150 + 5 = 1505
        }),
      });

      await expect(
        assetHandlers.createAsset(mockContext, validPayload)
      ).rejects.toThrow('Saldo insuficiente');
    });

    it('should create asset successfully with sufficient funds', async () => {
      mockDocRef.get.mockResolvedValueOnce({
        exists: true,
        id: 'account-123',
        data: () => ({
          userId: 'user-123',
          balances: { USD: 5000 }, // Enough funds
        }),
      });

      // Mock currentPrices check
      mockDocRef.get.mockResolvedValueOnce({
        exists: true,
      });

      const result = await assetHandlers.createAsset(mockContext, validPayload);

      expect(result.success).toBe(true);
      expect(result.assetId).toBeDefined();
      expect(result.transactionId).toBeDefined();
      expect(mockBatch.commit).toHaveBeenCalled();
    });
  });

  describe('deleteAsset', () => {
    const mockContext = {
      auth: { uid: 'user-123' },
    };

    it('should throw if assetId is missing', async () => {
      await expect(
        assetHandlers.deleteAsset(mockContext, {})
      ).rejects.toThrow('assetId es requerido');
    });

    it('should throw if asset does not exist', async () => {
      mockDocRef.get.mockResolvedValueOnce({
        exists: false,
      });

      await expect(
        assetHandlers.deleteAsset(mockContext, { assetId: 'non-existent' })
      ).rejects.toThrow('El asset no existe');
    });
  });

  describe('cleanDecimal', () => {
    it('should clean decimal values correctly', () => {
      expect(assetHandlers.cleanDecimal(1.23456789012)).toBe(1.23456789);
      expect(assetHandlers.cleanDecimal(0.1 + 0.2)).toBe(0.3);
    });

    it('should handle custom decimal places', () => {
      expect(assetHandlers.cleanDecimal(1.23456, 2)).toBe(1.23);
      expect(assetHandlers.cleanDecimal(1.235, 2)).toBe(1.24);
    });
  });
});

describe('Utility Functions', () => {
  describe('validateAccountOwnership', () => {
    it('should return account data if user owns it', async () => {
      mockDocRef.get.mockResolvedValueOnce({
        exists: true,
        id: 'account-123',
        data: () => ({
          userId: 'user-123',
          name: 'My Account',
        }),
      });

      const result = await assetHandlers.validateAccountOwnership('account-123', 'user-123');
      
      expect(result.id).toBe('account-123');
      expect(result.name).toBe('My Account');
    });

    it('should throw if account does not exist', async () => {
      mockDocRef.get.mockResolvedValueOnce({
        exists: false,
      });

      await expect(
        assetHandlers.validateAccountOwnership('account-123', 'user-123')
      ).rejects.toThrow('La cuenta de portafolio no existe');
    });

    it('should throw if user does not own account', async () => {
      mockDocRef.get.mockResolvedValueOnce({
        exists: true,
        data: () => ({
          userId: 'different-user',
        }),
      });

      await expect(
        assetHandlers.validateAccountOwnership('account-123', 'user-123')
      ).rejects.toThrow('No tienes permiso');
    });
  });
});
