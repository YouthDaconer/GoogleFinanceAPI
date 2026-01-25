/**
 * Tests for transactionEnricher service
 * 
 * @see docs/stories/90.story.md (IMPORT-002)
 */

// Path from test file to financeQuery: services/__tests__/transactions/ -> services/financeQuery.js
jest.mock('../../financeQuery', () => ({
  getQuotes: jest.fn(),
}));

// Import the module being tested
// Path: services/__tests__/transactions/ -> services/transactions/services/transactionEnricher.js
const { 
  enrichTransactions,
  normalizeTransactionType,
  parseNumber,
  normalizeDate,
  getExchangeRate,
  clearCache,
} = require('../../transactions/services/transactionEnricher');

const { getQuotes } = require('../../financeQuery');

describe('transactionEnricher', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    clearCache();
    
    getQuotes.mockResolvedValue({
      'COP=X': { regularMarketPrice: 4200 },
      'EUR=X': { regularMarketPrice: 0.92 },
    });
  });
  
  describe('normalizeTransactionType', () => {
    test('normalizes buy variants', () => {
      expect(normalizeTransactionType('buy')).toBe('buy');
      expect(normalizeTransactionType('BUY')).toBe('buy');
      expect(normalizeTransactionType('Buy')).toBe('buy');
      expect(normalizeTransactionType('b')).toBe('buy');
      expect(normalizeTransactionType('B')).toBe('buy');
      expect(normalizeTransactionType('compra')).toBe('buy');
      expect(normalizeTransactionType('Compra')).toBe('buy');
      expect(normalizeTransactionType('c')).toBe('buy');
      expect(normalizeTransactionType('bot')).toBe('buy');
      expect(normalizeTransactionType('BOT')).toBe('buy');
      expect(normalizeTransactionType('bought')).toBe('buy');
      expect(normalizeTransactionType('open')).toBe('buy');
      expect(normalizeTransactionType('long')).toBe('buy');
    });
    
    test('normalizes sell variants', () => {
      expect(normalizeTransactionType('sell')).toBe('sell');
      expect(normalizeTransactionType('SELL')).toBe('sell');
      expect(normalizeTransactionType('Sell')).toBe('sell');
      expect(normalizeTransactionType('s')).toBe('sell');
      expect(normalizeTransactionType('S')).toBe('sell');
      expect(normalizeTransactionType('venta')).toBe('sell');
      expect(normalizeTransactionType('Venta')).toBe('sell');
      expect(normalizeTransactionType('v')).toBe('sell');
      expect(normalizeTransactionType('sld')).toBe('sell');
      expect(normalizeTransactionType('SLD')).toBe('sell');
      expect(normalizeTransactionType('sold')).toBe('sell');
      expect(normalizeTransactionType('close')).toBe('sell');
      expect(normalizeTransactionType('short')).toBe('sell');
    });
    
    test('returns null for invalid types', () => {
      expect(normalizeTransactionType('invalid')).toBeNull();
      expect(normalizeTransactionType('')).toBeNull();
      expect(normalizeTransactionType(null)).toBeNull();
      expect(normalizeTransactionType(undefined)).toBeNull();
    });
    
    test('handles whitespace', () => {
      expect(normalizeTransactionType('  buy  ')).toBe('buy');
      expect(normalizeTransactionType('\tsell\t')).toBe('sell');
    });
  });
  
  describe('parseNumber', () => {
    test('parses integers', () => {
      expect(parseNumber(10)).toBe(10);
      expect(parseNumber('10')).toBe(10);
    });
    
    test('parses decimals', () => {
      expect(parseNumber(10.5)).toBe(10.5);
      expect(parseNumber('10.5')).toBe(10.5);
    });
    
    test('handles comma separators', () => {
      expect(parseNumber('1,000')).toBe(1000);
      expect(parseNumber('1,000.50')).toBe(1000.5);
      expect(parseNumber('1,234,567.89')).toBe(1234567.89);
    });
    
    test('handles negative numbers', () => {
      expect(parseNumber(-10)).toBe(-10);
      expect(parseNumber('-10')).toBe(-10);
    });
    
    test('handles empty/null values', () => {
      expect(parseNumber('')).toBe(0);
      expect(parseNumber(null)).toBe(0);
      expect(parseNumber(undefined)).toBe(0);
    });
  });
  
  describe('normalizeDate', () => {
    test('handles ISO format (YYYY-MM-DD)', () => {
      expect(normalizeDate('2024-01-15')).toBe('2024-01-15');
      expect(normalizeDate('2024-12-31')).toBe('2024-12-31');
    });
    
    test('handles ISO with time', () => {
      expect(normalizeDate('2024-01-15T10:30:00')).toBe('2024-01-15');
      expect(normalizeDate('2024-01-15 14:22:33')).toBe('2024-01-15');
    });
    
    test('handles US format (MM/DD/YYYY)', () => {
      expect(normalizeDate('01/15/2024')).toBe('2024-01-15');
      expect(normalizeDate('12/31/2024')).toBe('2024-12-31');
      expect(normalizeDate('1/5/2024')).toBe('2024-01-05');
    });
    
    test('handles text month format (Jan 15, 2024)', () => {
      expect(normalizeDate('Jan 15, 2024')).toBe('2024-01-15');
      expect(normalizeDate('Dec 31 2024')).toBe('2024-12-31');
      expect(normalizeDate('Mar 1, 2024')).toBe('2024-03-01');
    });
    
    test('returns null for invalid dates', () => {
      expect(normalizeDate('invalid')).toBeNull();
      expect(normalizeDate('')).toBeNull();
      expect(normalizeDate(null)).toBeNull();
    });
  });
  
  describe('getExchangeRate', () => {
    test('AC-016: returns 1 for USD', async () => {
      const rate = await getExchangeRate('USD', '2024-01-15');
      
      expect(rate).toBe(1);
      expect(getQuotes).not.toHaveBeenCalled();
    });
    
    test('AC-015: fetches rate from API', async () => {
      const rate = await getExchangeRate('COP', '2024-01-15');
      
      expect(rate).toBe(4200);
      expect(getQuotes).toHaveBeenCalledWith('COP=X');
    });
    
    test('AC-017: caches rates to avoid duplicate calls', async () => {
      await getExchangeRate('COP', '2024-01-15');
      await getExchangeRate('COP', '2024-01-15');
      
      // Should only call API once
      expect(getQuotes).toHaveBeenCalledTimes(1);
    });
    
    test('uses fallback for unknown currencies', async () => {
      getQuotes.mockResolvedValue({});
      
      const rate = await getExchangeRate('COP', '2024-01-15');
      
      expect(rate).toBe(4200); // Fallback value
    });
    
    test('returns 1 for empty currency', async () => {
      const rate = await getExchangeRate('', '2024-01-15');
      
      expect(rate).toBe(1);
    });
  });
  
  describe('enrichTransactions', () => {
    const mockAssetMap = new Map([
      ['AAPL', {
        id: 'asset-123',
        ticker: 'AAPL',
        assetType: 'stock',
        market: 'NMS',
        currency: 'USD',
      }],
    ]);
    
    const baseTransaction = {
      ticker: 'AAPL',
      type: 'buy',
      amount: 10,
      price: 150,
      date: '2024-01-15',
      originalRowNumber: 1,
    };
    
    test('enriches transaction with all required fields', async () => {
      const { data, errors } = await enrichTransactions(
        [baseTransaction],
        mockAssetMap,
        'account-1',
        'user-1',
        'USD'
      );
      
      expect(errors.length).toBe(0);
      expect(data.length).toBe(1);
      
      const tx = data[0];
      expect(tx.assetId).toBe('asset-123');
      expect(tx.assetName).toBe('AAPL');
      expect(tx.type).toBe('buy');
      expect(tx.amount).toBe(10);
      expect(tx.price).toBe(150);
      expect(tx.date).toBe('2024-01-15');
      expect(tx.assetType).toBe('stock');
      expect(tx.market).toBe('NMS');
      expect(tx.dollarPriceToDate).toBe(1);
      expect(tx.portfolioAccountId).toBe('account-1');
      expect(tx.userId).toBe('user-1');
    });
    
    test('converts amount to absolute value', async () => {
      const tx = { ...baseTransaction, amount: -10 };
      const { data } = await enrichTransactions([tx], mockAssetMap, 'account-1', 'user-1', 'USD');
      
      expect(data[0].amount).toBe(10);
    });
    
    test('uses default commission of 0', async () => {
      const { data } = await enrichTransactions(
        [baseTransaction],
        mockAssetMap,
        'account-1',
        'user-1',
        'USD'
      );
      
      expect(data[0].commission).toBe(0);
    });
    
    test('returns error for missing asset', async () => {
      const tx = { ...baseTransaction, ticker: 'NONEXISTENT' };
      const { data, errors } = await enrichTransactions([tx], mockAssetMap, 'account-1', 'user-1', 'USD');
      
      expect(data.length).toBe(0);
      expect(errors.length).toBe(1);
      expect(errors[0].code).toBe('ASSET_NOT_FOUND');
    });
    
    test('returns error for invalid type', async () => {
      const tx = { ...baseTransaction, type: 'invalid' };
      const { data, errors } = await enrichTransactions([tx], mockAssetMap, 'account-1', 'user-1', 'USD');
      
      expect(data.length).toBe(0);
      expect(errors.length).toBe(1);
      expect(errors[0].code).toBe('INVALID_DATA');
    });
    
    test('returns error for invalid amount', async () => {
      const tx = { ...baseTransaction, amount: 0 };
      const { errors } = await enrichTransactions([tx], mockAssetMap, 'account-1', 'user-1', 'USD');
      
      expect(errors.length).toBe(1);
      expect(errors[0].code).toBe('INVALID_DATA');
      expect(errors[0].message).toContain('amount');
    });
    
    test('returns error for invalid price', async () => {
      const tx = { ...baseTransaction, price: 'not-a-number' };
      const { errors } = await enrichTransactions([tx], mockAssetMap, 'account-1', 'user-1', 'USD');
      
      expect(errors.length).toBe(1);
      expect(errors[0].code).toBe('INVALID_DATA');
      expect(errors[0].message).toContain('price');
    });
    
    test('returns error for invalid date', async () => {
      const tx = { ...baseTransaction, date: 'not-a-date' };
      const { errors } = await enrichTransactions([tx], mockAssetMap, 'account-1', 'user-1', 'USD');
      
      expect(errors.length).toBe(1);
      expect(errors[0].code).toBe('INVALID_DATA');
      expect(errors[0].message).toContain('date');
    });
    
    test('fetches exchange rate for non-USD default currency', async () => {
      const { data } = await enrichTransactions(
        [baseTransaction],
        mockAssetMap,
        'account-1',
        'user-1',
        'COP'
      );
      
      expect(data[0].dollarPriceToDate).toBe(4200);
      expect(data[0].defaultCurrencyForAdquisitionDollar).toBe('COP');
    });
  });
});
