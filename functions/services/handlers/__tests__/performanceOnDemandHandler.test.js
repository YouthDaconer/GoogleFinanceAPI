/**
 * Tests para performanceOnDemandHandler
 * 
 * OPT-DEMAND-102: Tests unitarios para el handler de rendimiento on-demand.
 * 
 * @see docs/stories/74.story.md (OPT-DEMAND-102)
 */

const { 
  _calculatePerformance,
  _convertCurrency,
  _cache,
  _clearCache
} = require('../performanceOnDemandHandler');

// ============================================================================
// Tests para convertCurrency
// ============================================================================

describe('convertCurrency', () => {
  const mockCurrencies = {
    USD: 1,
    COP: 4200,
    EUR: 0.92,
    MXN: 17.5
  };
  
  it('should return same value when currencies are equal', () => {
    expect(_convertCurrency(100, 'USD', 'USD', mockCurrencies)).toBe(100);
    expect(_convertCurrency(100, 'COP', 'COP', mockCurrencies)).toBe(100);
  });
  
  it('should convert USD to COP correctly', () => {
    // 100 USD * 4200 = 420,000 COP
    expect(_convertCurrency(100, 'USD', 'COP', mockCurrencies)).toBe(420000);
  });
  
  it('should convert COP to USD correctly', () => {
    // 420,000 COP / 4200 = 100 USD
    expect(_convertCurrency(420000, 'COP', 'USD', mockCurrencies)).toBe(100);
  });
  
  it('should handle cross-currency conversion', () => {
    // 100 EUR -> USD: 100 / 0.92 = 108.70 USD
    // 108.70 USD -> COP: 108.70 * 4200 = 456,521.74 COP
    const result = _convertCurrency(100, 'EUR', 'COP', mockCurrencies);
    expect(result).toBeCloseTo(456521.74, -1);
  });
  
  it('should return 0 for null or NaN values', () => {
    expect(_convertCurrency(null, 'USD', 'COP', mockCurrencies)).toBe(0);
    expect(_convertCurrency(NaN, 'USD', 'COP', mockCurrencies)).toBe(0);
    expect(_convertCurrency(undefined, 'USD', 'COP', mockCurrencies)).toBe(0);
  });
  
  it('should handle missing currency rate (default to 1)', () => {
    // Unknown currency should default to rate of 1
    expect(_convertCurrency(100, 'USD', 'GBP', mockCurrencies)).toBe(100);
  });
});

// ============================================================================
// Tests para calculatePerformance
// ============================================================================

describe('calculatePerformance', () => {
  const mockAssets = [
    {
      id: 'asset-1',
      name: 'AAPL',
      units: 10,
      averagePrice: 150,
      currency: 'USD',
      totalCashFlow: -1500,
      doneProfitAndLoss: 0
    },
    {
      id: 'asset-2',
      name: 'VOO',
      units: 5,
      averagePrice: 400,
      currency: 'USD',
      totalCashFlow: -2000,
      doneProfitAndLoss: 100
    }
  ];
  
  const mockAccounts = [
    { id: 'acc-1', balances: { USD: 500 } }
  ];
  
  const mockPrices = {
    'AAPL': { 
      price: 175, 
      previousClose: 170, 
      currency: 'USD',
      change: 5,
      changePercent: 2.94
    },
    'VOO': { 
      price: 450, 
      previousClose: 445, 
      currency: 'USD',
      change: 5,
      changePercent: 1.12
    }
  };
  
  const mockCurrencies = { 
    USD: 1, 
    COP: 4200 
  };
  
  it('should calculate total value correctly in USD', () => {
    const result = _calculatePerformance(
      mockAssets, 
      mockAccounts, 
      mockPrices, 
      mockCurrencies, 
      'USD'
    );
    
    // AAPL: 10 * 175 = 1750
    // VOO: 5 * 450 = 2250
    // Cash: 500
    // Total: 4500
    expect(result.totalValue).toBe(4500);
  });
  
  it('should calculate total investment correctly', () => {
    const result = _calculatePerformance(
      mockAssets, 
      mockAccounts, 
      mockPrices, 
      mockCurrencies, 
      'USD'
    );
    
    // AAPL: 10 * 150 = 1500
    // VOO: 5 * 400 = 2000
    // Total: 3500
    expect(result.totalInvestment).toBe(3500);
  });
  
  it('should calculate cash balance correctly', () => {
    const result = _calculatePerformance(
      mockAssets, 
      mockAccounts, 
      mockPrices, 
      mockCurrencies, 
      'USD'
    );
    
    expect(result.cashBalance).toBe(500);
  });
  
  it('should calculate unrealized PnL correctly', () => {
    const result = _calculatePerformance(
      mockAssets, 
      mockAccounts, 
      mockPrices, 
      mockCurrencies, 
      'USD'
    );
    
    // Assets value: 1750 + 2250 = 4000
    // Investment: 3500
    // Unrealized PnL: 4000 - 3500 = 500
    expect(result.unrealizedPnL).toBe(500);
  });
  
  it('should calculate unrealized PnL percent correctly', () => {
    const result = _calculatePerformance(
      mockAssets, 
      mockAccounts, 
      mockPrices, 
      mockCurrencies, 
      'USD'
    );
    
    // Unrealized PnL: 500
    // Investment: 3500
    // Percent: (500 / 3500) * 100 = 14.29%
    expect(result.unrealizedPnLPercent).toBeCloseTo(14.29, 1);
  });
  
  it('should calculate daily change correctly', () => {
    const result = _calculatePerformance(
      mockAssets, 
      mockAccounts, 
      mockPrices, 
      mockCurrencies, 
      'USD'
    );
    
    // AAPL: (175-170) * 10 = 50
    // VOO: (450-445) * 5 = 25
    // Total daily change: 75
    expect(result.dailyChange).toBe(75);
  });
  
  it('should calculate daily change percent correctly', () => {
    const result = _calculatePerformance(
      mockAssets, 
      mockAccounts, 
      mockPrices, 
      mockCurrencies, 
      'USD'
    );
    
    // Previous value: (170*10) + (445*5) = 1700 + 2225 = 3925
    // Daily change: 75
    // Percent: (75 / 3925) * 100 = 1.91%
    expect(result.dailyChangePercent).toBeCloseTo(1.91, 1);
  });
  
  it('should sum realized PnL from assets', () => {
    const result = _calculatePerformance(
      mockAssets, 
      mockAccounts, 
      mockPrices, 
      mockCurrencies, 
      'USD'
    );
    
    // Only asset-2 has realized PnL of 100
    expect(result.realizedPnL).toBe(100);
  });
  
  it('should convert all values to target currency (COP)', () => {
    const result = _calculatePerformance(
      mockAssets, 
      mockAccounts, 
      mockPrices, 
      mockCurrencies, 
      'COP'
    );
    
    // USD total value: 4500
    // COP total value: 4500 * 4200 = 18,900,000
    expect(result.totalValue).toBeCloseTo(18900000, -2);
    
    // USD investment: 3500
    // COP investment: 3500 * 4200 = 14,700,000
    expect(result.totalInvestment).toBeCloseTo(14700000, -2);
  });
  
  it('should handle empty assets array', () => {
    const result = _calculatePerformance(
      [], 
      mockAccounts, 
      mockPrices, 
      mockCurrencies, 
      'USD'
    );
    
    // Only cash balance
    expect(result.totalValue).toBe(500);
    expect(result.totalInvestment).toBe(0);
    expect(result.unrealizedPnL).toBe(0);
  });
  
  it('should handle empty accounts array', () => {
    const result = _calculatePerformance(
      mockAssets, 
      [], 
      mockPrices, 
      mockCurrencies, 
      'USD'
    );
    
    expect(result.cashBalance).toBe(0);
    // Assets value only
    expect(result.totalValue).toBe(4000);
  });
  
  it('should use average price as fallback when no market price', () => {
    const assetsWithMissing = [
      {
        id: 'asset-3',
        name: 'UNKNOWN',
        units: 10,
        averagePrice: 50,
        currency: 'USD'
      }
    ];
    
    const result = _calculatePerformance(
      assetsWithMissing, 
      [], 
      {}, // No prices
      mockCurrencies, 
      'USD'
    );
    
    // Fallback: 10 * 50 = 500
    expect(result.totalValue).toBe(500);
    expect(result.totalInvestment).toBe(500);
    expect(result.unrealizedPnL).toBe(0);
  });
  
  it('should handle accounts with multiple currency balances', () => {
    const accountsMultiCurrency = [
      { 
        id: 'acc-1', 
        balances: { 
          USD: 500,
          COP: 420000 // = 100 USD
        } 
      }
    ];
    
    const result = _calculatePerformance(
      [], 
      accountsMultiCurrency, 
      {}, 
      mockCurrencies, 
      'USD'
    );
    
    // 500 USD + (420000 COP / 4200) = 500 + 100 = 600 USD
    expect(result.cashBalance).toBe(600);
    expect(result.totalValue).toBe(600);
  });
});

// ============================================================================
// Tests para cache
// ============================================================================

describe('cache', () => {
  beforeEach(() => {
    _clearCache();
  });
  
  afterEach(() => {
    _clearCache();
  });
  
  it('should store and retrieve from cache', () => {
    const key = 'test-key';
    const data = { totalValue: 1000 };
    
    _cache.set(key, { data, timestamp: Date.now() });
    
    const cached = _cache.get(key);
    expect(cached).toBeDefined();
    expect(cached.data).toEqual(data);
  });
  
  it('should clear cache', () => {
    _cache.set('key1', { data: {}, timestamp: Date.now() });
    _cache.set('key2', { data: {}, timestamp: Date.now() });
    
    expect(_cache.size).toBe(2);
    
    _clearCache();
    
    expect(_cache.size).toBe(0);
  });
  
  it('should generate unique cache keys', () => {
    const key1 = `perf:user1:USD:all:`;
    const key2 = `perf:user1:COP:all:`;
    const key3 = `perf:user2:USD:all:`;
    
    _cache.set(key1, { data: { currency: 'USD' }, timestamp: Date.now() });
    _cache.set(key2, { data: { currency: 'COP' }, timestamp: Date.now() });
    _cache.set(key3, { data: { user: 'user2' }, timestamp: Date.now() });
    
    expect(_cache.size).toBe(3);
    expect(_cache.get(key1).data.currency).toBe('USD');
    expect(_cache.get(key2).data.currency).toBe('COP');
  });
});

// ============================================================================
// Tests para edge cases
// ============================================================================

describe('edge cases', () => {
  it('should handle assets with zero units', () => {
    const assets = [
      {
        id: 'asset-1',
        name: 'AAPL',
        units: 0,
        averagePrice: 150,
        currency: 'USD'
      }
    ];
    
    const prices = {
      'AAPL': { price: 175, previousClose: 170, currency: 'USD' }
    };
    
    const result = _calculatePerformance(
      assets, 
      [], 
      prices, 
      { USD: 1 }, 
      'USD'
    );
    
    expect(result.totalValue).toBe(0);
    expect(result.totalInvestment).toBe(0);
  });
  
  it('should handle negative cash balances', () => {
    const accounts = [
      { id: 'acc-1', balances: { USD: -500 } }
    ];
    
    const result = _calculatePerformance(
      [], 
      accounts, 
      {}, 
      { USD: 1 }, 
      'USD'
    );
    
    expect(result.cashBalance).toBe(-500);
    expect(result.totalValue).toBe(-500);
  });
  
  it('should handle missing asset properties gracefully', () => {
    const assets = [
      {
        id: 'asset-1',
        name: 'AAPL'
        // Missing: units, averagePrice, currency
      }
    ];
    
    const prices = {
      'AAPL': { price: 175, previousClose: 170, currency: 'USD' }
    };
    
    const result = _calculatePerformance(
      assets, 
      [], 
      prices, 
      { USD: 1 }, 
      'USD'
    );
    
    // Should not throw, values should be 0
    expect(result.totalValue).toBe(0);
    expect(result.totalInvestment).toBe(0);
  });
});
