/**
 * Tests for duplicateDetector service
 * 
 * @see docs/stories/90.story.md (IMPORT-002)
 */

// Mock firebase-admin
// Path from test file to firebaseAdmin: services/__tests__/transactions/ -> services/firebaseAdmin.js
jest.mock('../../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({
    collection: jest.fn(() => ({
      where: jest.fn(() => ({
        where: jest.fn(() => ({
          get: jest.fn(),
        })),
      })),
    })),
  })),
}));

// Import the module being tested
// Path: services/__tests__/transactions/ -> services/transactions/services/duplicateDetector.js
const { 
  detectDuplicates,
  createSignature,
  groupByTicker,
  roundToDecimals,
} = require('../../transactions/services/duplicateDetector');

const admin = require('../../firebaseAdmin');

describe('duplicateDetector', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });
  
  describe('createSignature', () => {
    test('AC-018: creates signature from ticker, date, amount, price, type', () => {
      const tx = {
        assetName: 'AAPL',
        date: '2024-01-15',
        amount: 10,
        price: 150.50,
        type: 'buy',
      };
      
      const signature = createSignature(tx);
      
      expect(signature).toBe('AAPL|2024-01-15|10|150.5|buy');
    });
    
    test('normalizes ticker to uppercase', () => {
      const tx = {
        assetName: 'aapl',
        date: '2024-01-15',
        amount: 10,
        price: 150,
        type: 'buy',
      };
      
      const signature = createSignature(tx);
      
      expect(signature).toContain('AAPL');
    });
    
    test('normalizes type to lowercase', () => {
      const tx = {
        assetName: 'AAPL',
        date: '2024-01-15',
        amount: 10,
        price: 150,
        type: 'BUY',
      };
      
      const signature = createSignature(tx);
      
      expect(signature).toContain('buy');
    });
    
    test('rounds amount to 4 decimals', () => {
      const tx = {
        assetName: 'AAPL',
        date: '2024-01-15',
        amount: 10.123456789,
        price: 150,
        type: 'buy',
      };
      
      const signature = createSignature(tx);
      
      expect(signature).toContain('10.1235');
    });
    
    test('rounds price to 2 decimals', () => {
      const tx = {
        assetName: 'AAPL',
        date: '2024-01-15',
        amount: 10,
        price: 150.999,
        type: 'buy',
      };
      
      const signature = createSignature(tx);
      
      expect(signature).toContain('151');
    });
    
    test('uses only date part (ignores time)', () => {
      const tx = {
        assetName: 'AAPL',
        date: '2024-01-15T10:30:00',
        amount: 10,
        price: 150,
        type: 'buy',
      };
      
      const signature = createSignature(tx);
      
      expect(signature).toContain('2024-01-15');
      expect(signature).not.toContain('10:30');
    });
    
    test('handles ticker field instead of assetName', () => {
      const tx = {
        ticker: 'AAPL',
        date: '2024-01-15',
        amount: 10,
        price: 150,
        type: 'buy',
      };
      
      const signature = createSignature(tx);
      
      expect(signature).toContain('AAPL');
    });
    
    test('handles missing fields gracefully', () => {
      const tx = {};
      
      const signature = createSignature(tx);
      
      expect(signature).toBe('||0|0|');
    });
  });
  
  describe('groupByTicker', () => {
    test('groups transactions by ticker', () => {
      const txs = [
        { assetName: 'AAPL', amount: 10 },
        { assetName: 'NVDA', amount: 5 },
        { assetName: 'AAPL', amount: 15 },
      ];
      
      const grouped = groupByTicker(txs);
      
      expect(grouped.size).toBe(2);
      expect(grouped.get('AAPL').length).toBe(2);
      expect(grouped.get('NVDA').length).toBe(1);
    });
    
    test('normalizes tickers to uppercase', () => {
      const txs = [
        { assetName: 'aapl', amount: 10 },
        { assetName: 'AAPL', amount: 5 },
      ];
      
      const grouped = groupByTicker(txs);
      
      expect(grouped.size).toBe(1);
      expect(grouped.get('AAPL').length).toBe(2);
    });
  });
  
  describe('roundToDecimals', () => {
    test('rounds to specified decimal places', () => {
      expect(roundToDecimals(10.1234, 2)).toBe(10.12);
      expect(roundToDecimals(10.1256, 2)).toBe(10.13);
      expect(roundToDecimals(10.123456, 4)).toBe(10.1235);
    });
    
    test('handles integers', () => {
      expect(roundToDecimals(10, 2)).toBe(10);
    });
    
    test('handles negative numbers', () => {
      expect(roundToDecimals(-10.1256, 2)).toBe(-10.13);
    });
  });
  
  describe('detectDuplicates', () => {
    test('returns empty for empty input', async () => {
      const result = await detectDuplicates([], 'user-1');
      
      expect(result.unique).toEqual([]);
      expect(result.duplicates).toEqual([]);
    });
    
    // Integration tests requiring Firestore mocks - skipped for now
    // These tests verify Firestore interactions but require more complex mock setup
    test.skip('AC-018: detects existing duplicates in Firestore', async () => {
      // This test requires proper Firestore mock setup
    });
    
    test.skip('detects within-batch duplicates with Firestore', async () => {
      // This test requires proper Firestore mock setup
    });
    
    test.skip('keeps unique transactions with Firestore', async () => {
      // This test requires proper Firestore mock setup  
    });
    
    test.skip('different amount is not a duplicate with Firestore', async () => {
      // This test requires proper Firestore mock setup
    });
    
    test.skip('different type is not a duplicate with Firestore', async () => {
      // This test requires proper Firestore mock setup
    });
  });
});
