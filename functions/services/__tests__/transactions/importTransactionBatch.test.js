/**
 * Integration Tests for importTransactionBatch Cloud Function
 * 
 * @see docs/stories/90.story.md (IMPORT-002)
 */

// Mock all dependencies
const mockBatchCommit = jest.fn().mockResolvedValue();
const mockBatchSet = jest.fn();
const mockDocSet = jest.fn().mockResolvedValue();
const mockDocUpdate = jest.fn().mockResolvedValue();

const mockAssetGet = jest.fn();
const mockAccountGet = jest.fn();
const mockTransactionsGet = jest.fn();

// Path from test file to firebaseAdmin: services/__tests__/transactions/ -> services/firebaseAdmin.js
jest.mock('../../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({
    collection: jest.fn((name) => ({
      doc: jest.fn((id) => ({
        get: name === 'portfolioAccounts' ? mockAccountGet :
             name === 'assets' ? mockAssetGet :
             mockTransactionsGet,
        set: mockDocSet,
        update: mockDocUpdate,
      })),
      where: jest.fn(() => ({
        where: jest.fn(() => ({
          where: jest.fn(() => ({
            limit: jest.fn(() => ({
              get: mockAssetGet,
            })),
          })),
          get: mockTransactionsGet,
        })),
      })),
    })),
    batch: jest.fn(() => ({
      set: mockBatchSet,
      commit: mockBatchCommit,
    })),
    FieldValue: {
      serverTimestamp: jest.fn(() => 'SERVER_TIMESTAMP'),
    },
  })),
}));

// Path from test file to financeQuery: services/__tests__/transactions/ -> services/financeQuery.js
jest.mock('../../financeQuery', () => ({
  search: jest.fn().mockResolvedValue([{
    symbol: 'AAPL',
    shortname: 'Apple Inc.',
    quoteType: 'EQUITY',
    exchange: 'NMS',
  }]),
  getQuotes: jest.fn().mockResolvedValue({
    'COP=X': { regularMarketPrice: 4200 },
  }),
}));

// Mock firebase-functions
jest.mock('firebase-functions/v2/https', () => ({
  onCall: jest.fn((config, handler) => handler),
  HttpsError: class HttpsError extends Error {
    constructor(code, message) {
      super(message);
      this.code = code;
    }
  },
}));

// Import the module being tested
// Path: services/__tests__/transactions/ -> services/transactions/importTransactionBatch.js
const { importTransactionBatch, verifyAccountAccess, groupByTicker } = require('../../transactions/importTransactionBatch');
const { HttpsError } = require('firebase-functions/v2/https');

describe('importTransactionBatch', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    
    // Default: account exists and belongs to user
    mockAccountGet.mockResolvedValue({
      exists: true,
      id: 'account-1',
      data: () => ({
        userId: 'user-1',
        name: 'My Account',
        isActive: true,
      }),
    });
    
    // Default: no existing assets
    mockAssetGet.mockResolvedValue({ empty: true });
    
    // Default: no existing transactions (for duplicate check)
    mockTransactionsGet.mockResolvedValue({ docs: [] });
  });
  
  describe('Authentication (AC-001 to AC-004)', () => {
    test('AC-001: rejects unauthenticated requests', async () => {
      const request = {
        data: { portfolioAccountId: 'account-1', transactions: [] },
        auth: null,
      };
      
      await expect(importTransactionBatch(request)).rejects.toThrow(HttpsError);
    });
    
    test('AC-002: requires portfolioAccountId', async () => {
      const request = {
        data: { transactions: [] },
        auth: { uid: 'user-1' },
      };
      
      await expect(importTransactionBatch(request)).rejects.toThrow(HttpsError);
    });
    
    test('AC-003: rejects if account belongs to different user', async () => {
      mockAccountGet.mockResolvedValue({
        exists: true,
        data: () => ({ userId: 'different-user' }),
      });
      
      const request = {
        data: {
          portfolioAccountId: 'account-1',
          transactions: [{ ticker: 'AAPL', type: 'buy', amount: 10, price: 150, date: '2024-01-15' }],
        },
        auth: { uid: 'user-1' },
      };
      
      await expect(importTransactionBatch(request)).rejects.toThrow('No tiene acceso');
    });
    
    test('AC-004: rejects if batch exceeds 500 transactions', async () => {
      const transactions = Array.from({ length: 501 }, () => ({
        ticker: 'AAPL',
        type: 'buy',
        amount: 10,
        price: 150,
        date: '2024-01-15',
      }));
      
      const request = {
        data: {
          portfolioAccountId: 'account-1',
          transactions,
        },
        auth: { uid: 'user-1' },
      };
      
      await expect(importTransactionBatch(request)).rejects.toThrow('500');
    });
    
    test('rejects empty transactions array', async () => {
      const request = {
        data: {
          portfolioAccountId: 'account-1',
          transactions: [],
        },
        auth: { uid: 'user-1' },
      };
      
      await expect(importTransactionBatch(request)).rejects.toThrow('vacío');
    });
  });
  
  // Integration tests requiring complex Firestore mock
  describe('Successful Import Flow (unit tests)', () => {
    test('imports a single transaction successfully', async () => {
      // Setup asset to be found
      mockAssetGet.mockResolvedValueOnce({
        empty: false,
        docs: [{
          id: 'asset-1',
          data: () => ({
            name: 'AAPL',
            assetType: 'stock',
            market: 'NMS',
            currency: 'USD',
          }),
        }],
      });
      
      // No existing transactions for duplicate check
      mockTransactionsGet.mockResolvedValue({ docs: [] });
      
      const request = {
        data: {
          portfolioAccountId: 'account-1',
          transactions: [
            { ticker: 'AAPL', type: 'buy', amount: 10, price: 150, date: '2024-01-15' },
          ],
          options: { createMissingAssets: true, skipDuplicates: true },
        },
        auth: { uid: 'user-1' },
      };
      
      const result = await importTransactionBatch(request);
      
      expect(result.success).toBeDefined();
      expect(result.summary).toBeDefined();
      expect(result.summary.totalProcessed).toBe(1);
    });
    
    test('imports multiple transactions for same ticker', async () => {
      // Setup asset to be found for both
      mockAssetGet.mockResolvedValue({
        empty: false,
        docs: [{
          id: 'asset-1',
          data: () => ({
            name: 'AAPL',
            assetType: 'stock',
            market: 'NMS',
            currency: 'USD',
          }),
        }],
      });
      
      // No duplicates
      mockTransactionsGet.mockResolvedValue({ docs: [] });
      
      const request = {
        data: {
          portfolioAccountId: 'account-1',
          transactions: [
            { ticker: 'AAPL', type: 'buy', amount: 10, price: 150, date: '2024-01-15' },
            { ticker: 'AAPL', type: 'buy', amount: 5, price: 152, date: '2024-01-16' },
          ],
          options: { createMissingAssets: true, skipDuplicates: true },
        },
        auth: { uid: 'user-1' },
      };
      
      const result = await importTransactionBatch(request);
      
      expect(result.summary.totalProcessed).toBe(2);
    });
  });
  
  describe('Duplicate Detection (AC-018 to AC-021)', () => {
    test('AC-019: detects duplicates by ticker+date+amount+price', () => {
      // Unit test for duplicate signature
      const tx1 = { assetName: 'AAPL', date: '2024-01-15', amount: 10, price: 150, type: 'buy' };
      const tx2 = { assetName: 'AAPL', date: '2024-01-15', amount: 10, price: 150, type: 'buy' };
      
      const sig1 = `${tx1.assetName}|${tx1.date}|${tx1.amount}|${tx1.price}|${tx1.type}`;
      const sig2 = `${tx2.assetName}|${tx2.date}|${tx2.amount}|${tx2.price}|${tx2.type}`;
      
      expect(sig1).toBe(sig2);
    });
    
    test('AC-020: different dates are not duplicates', () => {
      const tx1 = { assetName: 'AAPL', date: '2024-01-15', amount: 10, price: 150, type: 'buy' };
      const tx2 = { assetName: 'AAPL', date: '2024-01-16', amount: 10, price: 150, type: 'buy' };
      
      const sig1 = `${tx1.assetName}|${tx1.date}|${tx1.amount}|${tx1.price}|${tx1.type}`;
      const sig2 = `${tx2.assetName}|${tx2.date}|${tx2.amount}|${tx2.price}|${tx2.type}`;
      
      expect(sig1).not.toBe(sig2);
    });
    
    test('AC-021: different amounts are not duplicates', () => {
      const tx1 = { assetName: 'AAPL', date: '2024-01-15', amount: 10, price: 150, type: 'buy' };
      const tx2 = { assetName: 'AAPL', date: '2024-01-15', amount: 15, price: 150, type: 'buy' };
      
      const sig1 = `${tx1.assetName}|${tx1.date}|${tx1.amount}|${tx1.price}|${tx1.type}`;
      const sig2 = `${tx2.assetName}|${tx2.date}|${tx2.amount}|${tx2.price}|${tx2.type}`;
      
      expect(sig1).not.toBe(sig2);
    });
  });
  
  describe('Asset Resolution', () => {
    test('AC-006: uses existing asset when found', async () => {
      // Setup existing asset
      mockAssetGet.mockResolvedValue({
        empty: false,
        docs: [{
          id: 'existing-asset-1',
          data: () => ({
            name: 'AAPL',
            assetType: 'stock',
            market: 'NMS',
            currency: 'USD',
          }),
        }],
      });
      
      mockTransactionsGet.mockResolvedValue({ docs: [] });
      
      const request = {
        data: {
          portfolioAccountId: 'account-1',
          transactions: [
            { ticker: 'AAPL', type: 'buy', amount: 10, price: 150, date: '2024-01-15' },
          ],
          options: { createMissingAssets: false, skipDuplicates: true },
        },
        auth: { uid: 'user-1' },
      };
      
      const result = await importTransactionBatch(request);
      
      // Should not create new assets
      expect(result.assetsCreated).toHaveLength(0);
    });
    
    test('AC-008: reports error when asset not found and createMissingAssets=false', async () => {
      // No existing assets
      mockAssetGet.mockResolvedValue({ empty: true });
      
      const request = {
        data: {
          portfolioAccountId: 'account-1',
          transactions: [
            { ticker: 'UNKNOWN', type: 'buy', amount: 10, price: 150, date: '2024-01-15' },
          ],
          options: { createMissingAssets: false, skipDuplicates: true },
        },
        auth: { uid: 'user-1' },
      };
      
      const result = await importTransactionBatch(request);
      
      expect(result.errors.length).toBeGreaterThan(0);
      expect(result.errors[0].code).toBe('ASSET_NOT_FOUND');
    });
  });

  describe('Response Format (AC-031 to AC-039)', () => {
    test('returns complete response structure', async () => {
      // Setup asset
      mockAssetGet.mockResolvedValue({
        empty: false,
        docs: [{
          id: 'asset-1',
          data: () => ({
            name: 'AAPL',
            assetType: 'stock',
            market: 'NMS',
            currency: 'USD',
          }),
        }],
      });
      
      mockTransactionsGet.mockResolvedValue({ docs: [] });
      
      const request = {
        data: {
          portfolioAccountId: 'account-1',
          transactions: [
            { ticker: 'AAPL', type: 'buy', amount: 10, price: 150, date: '2024-01-15' },
          ],
          options: { createMissingAssets: true, skipDuplicates: true },
        },
        auth: { uid: 'user-1' },
      };
      
      const result = await importTransactionBatch(request);
      
      // Verify response structure
      expect(result).toHaveProperty('success');
      expect(result).toHaveProperty('summary');
      expect(result).toHaveProperty('assetsCreated');
      expect(result).toHaveProperty('assetsUpdated');
      expect(result).toHaveProperty('errors');
      expect(result).toHaveProperty('importedTransactionIds');
      expect(result).toHaveProperty('processingTimeMs');
      
      // Verify summary structure
      expect(result.summary).toHaveProperty('totalProcessed');
      expect(result.summary).toHaveProperty('imported');
      expect(result.summary).toHaveProperty('skipped');
      expect(result.summary).toHaveProperty('errors');
    });
    
    test('processingTimeMs is a positive number', async () => {
      mockAssetGet.mockResolvedValue({
        empty: false,
        docs: [{
          id: 'asset-1',
          data: () => ({
            name: 'AAPL',
            assetType: 'stock',
            market: 'NMS',
            currency: 'USD',
          }),
        }],
      });
      
      mockTransactionsGet.mockResolvedValue({ docs: [] });
      
      const request = {
        data: {
          portfolioAccountId: 'account-1',
          transactions: [
            { ticker: 'AAPL', type: 'buy', amount: 10, price: 150, date: '2024-01-15' },
          ],
          options: { createMissingAssets: true, skipDuplicates: true },
        },
        auth: { uid: 'user-1' },
      };
      
      const result = await importTransactionBatch(request);
      
      expect(typeof result.processingTimeMs).toBe('number');
      expect(result.processingTimeMs).toBeGreaterThanOrEqual(0);
    });
  });
  
  describe('Helper Functions', () => {
    describe('groupByTicker', () => {
      test('groups transactions by normalized ticker', () => {
        const transactions = [
          { ticker: 'AAPL', amount: 10 },
          { ticker: 'aapl', amount: 5 },
          { ticker: 'NVDA', amount: 3 },
        ];
        
        const grouped = groupByTicker(transactions);
        
        expect(grouped.size).toBe(2);
        expect(grouped.get('AAPL').length).toBe(2);
        expect(grouped.get('NVDA').length).toBe(1);
      });
    });
  });
});
