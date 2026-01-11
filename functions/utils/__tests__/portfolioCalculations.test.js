/**
 * Tests para portfolioCalculations.js
 * 
 * @module __tests__/utils/portfolioCalculations.test
 * @see docs/stories/53.story.md (SCALE-CORE-001)
 */

const {
  convertCurrency,
  calculateTotalROIAndReturns,
  calculateMaxDaysInvested,
  calculateDaysInvested,
} = require('../../utils/portfolioCalculations');

// ============================================================================
// Test Data Builders - Basados en estructuras reales de Firestore
// ============================================================================

const createMockCurrency = (overrides = {}) => ({
  code: 'USD',
  exchangeRate: 1,
  ...overrides,
});

const createMockAsset = (overrides = {}) => ({
  id: 'test-asset-id',
  name: 'SPYG',
  market: 'NYSEARCA',
  acquisitionDate: '2024-01-01',
  company: 'Interactive Brokers',
  portfolioAccount: 'test-account',
  currency: 'USD',
  isActive: true,
  assetType: 'etf',
  defaultCurrencyForAdquisitionDollar: 'COP',
  acquisitionDollarValue: 4305.66,
  commission: 0,
  unitValue: 82.7556,
  units: 0.33,
  ...overrides,
});

// ============================================================================
// Tests
// ============================================================================

describe('portfolioCalculations', () => {
  // ==========================================================================
  // convertCurrency
  // ==========================================================================

  describe('convertCurrency', () => {
    const mockCurrencies = [
      createMockCurrency({ code: 'USD', exchangeRate: 1 }),
      createMockCurrency({ code: 'COP', exchangeRate: 4200 }),
      createMockCurrency({ code: 'EUR', exchangeRate: 0.92 }),
    ];

    it('should return the same amount when converting between same currency', () => {
      const result = convertCurrency(100, 'USD', 'USD', mockCurrencies, 'USD');
      expect(result).toBe(100);
    });

    it('should convert USD to COP correctly', () => {
      const result = convertCurrency(100, 'USD', 'COP', mockCurrencies, 'USD');
      expect(result).toBe(420000); // 100 * 4200
    });

    it('should convert COP to USD correctly', () => {
      const result = convertCurrency(420000, 'COP', 'USD', mockCurrencies, 'USD');
      expect(result).toBe(100); // 420000 / 4200
    });

    it('should convert EUR to USD correctly', () => {
      const result = convertCurrency(92, 'EUR', 'USD', mockCurrencies, 'USD');
      expect(result).toBe(100); // 92 / 0.92
    });

    it('should convert USD to EUR correctly', () => {
      const result = convertCurrency(100, 'USD', 'EUR', mockCurrencies, 'USD');
      expect(result).toBe(92); // 100 * 0.92
    });

    it('should use acquisitionDollarValue when converting USD to defaultCurrency', () => {
      const result = convertCurrency(100, 'USD', 'COP', mockCurrencies, 'COP', 4500);
      expect(result).toBe(450000); // 100 * 4500 (historical value)
    });

    it('should NOT use acquisitionDollarValue when converting to non-default currency', () => {
      // Cuando convertimos USD a EUR, no debería usar acquisitionDollarValue
      const result = convertCurrency(100, 'USD', 'EUR', mockCurrencies, 'COP', 4500);
      expect(result).toBe(92); // Debería usar la tasa normal, no acquisitionDollarValue
    });

    it('should return amount * 1 if currency not found (rate = 1)', () => {
      const result = convertCurrency(100, 'XXX', 'YYY', mockCurrencies, 'USD');
      expect(result).toBe(100); // (100 * 1) / 1 = 100
    });

    it('should handle zero amount', () => {
      const result = convertCurrency(0, 'USD', 'COP', mockCurrencies, 'USD');
      expect(result).toBe(0);
    });

    it('should handle negative amounts', () => {
      const result = convertCurrency(-100, 'USD', 'COP', mockCurrencies, 'USD');
      expect(result).toBe(-420000);
    });

    it('should handle empty currencies array', () => {
      const result = convertCurrency(100, 'USD', 'COP', [], 'USD');
      expect(result).toBe(100); // Both rates default to 1
    });
  });

  // ==========================================================================
  // calculateTotalROIAndReturns
  // ==========================================================================

  describe('calculateTotalROIAndReturns', () => {
    it('should calculate positive ROI correctly', () => {
      const result = calculateTotalROIAndReturns(1000, 1200, 365);
      
      expect(result.totalROI).toBe(20); // (1200-1000)/1000 * 100 = 20%
      expect(result.dailyReturn).toBeGreaterThan(0);
      expect(result.monthlyReturn).toBeGreaterThan(0);
      expect(result.annualReturn).toBeCloseTo(20, 0);
    });

    it('should return 0 for zero investment', () => {
      const result = calculateTotalROIAndReturns(0, 100, 365);
      
      expect(result.totalROI).toBe(0);
      expect(result.dailyReturn).toBe(0);
      expect(result.monthlyReturn).toBe(0);
      expect(result.annualReturn).toBe(0);
    });

    it('should handle negative ROI (loss)', () => {
      const result = calculateTotalROIAndReturns(1000, 800, 365);
      
      expect(result.totalROI).toBe(-20); // (800-1000)/1000 * 100 = -20%
    });

    it('should not calculate monthly return if less than 30 days', () => {
      const result = calculateTotalROIAndReturns(1000, 1100, 15);
      
      expect(result.monthlyReturn).toBe(0);
      expect(result.annualReturn).toBe(0);
    });

    it('should not calculate annual return if less than 365 days', () => {
      const result = calculateTotalROIAndReturns(1000, 1100, 100);
      
      expect(result.monthlyReturn).toBeGreaterThan(0);
      expect(result.annualReturn).toBe(0);
    });

    it('should calculate correctly for exactly 30 days', () => {
      const result = calculateTotalROIAndReturns(1000, 1100, 30);
      
      expect(result.monthlyReturn).toBeGreaterThan(0);
      expect(result.annualReturn).toBe(0); // Still < 365 days
    });

    it('should calculate correctly for exactly 365 days', () => {
      const result = calculateTotalROIAndReturns(1000, 1200, 365);
      
      expect(result.annualReturn).toBeGreaterThan(0);
    });

    it('should handle break-even (value equals investment)', () => {
      const result = calculateTotalROIAndReturns(1000, 1000, 365);
      
      expect(result.totalROI).toBe(0);
      expect(result.dailyReturn).toBe(0);
    });

    it('should handle very small gains', () => {
      const result = calculateTotalROIAndReturns(1000, 1001, 365);
      
      expect(result.totalROI).toBeCloseTo(0.1, 1);
    });

    it('should handle very large gains', () => {
      const result = calculateTotalROIAndReturns(1000, 10000, 365);
      
      expect(result.totalROI).toBe(900); // 900%
    });
  });

  // ==========================================================================
  // calculateMaxDaysInvested
  // ==========================================================================

  describe('calculateMaxDaysInvested', () => {
    it('should return 0 for empty assets array', () => {
      const result = calculateMaxDaysInvested([]);
      expect(result).toBe(0);
    });

    it('should return 0 for array with no active assets', () => {
      const assets = [
        createMockAsset({ isActive: false, acquisitionDate: '2020-01-01' }),
        createMockAsset({ isActive: false, acquisitionDate: '2021-01-01' }),
      ];
      
      const result = calculateMaxDaysInvested(assets);
      expect(result).toBe(0);
    });

    it('should return max days for single active asset', () => {
      // Asset acquired 365 days ago (approximately)
      const oneYearAgo = new Date();
      oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
      
      const assets = [
        createMockAsset({ 
          isActive: true, 
          acquisitionDate: oneYearAgo.toISOString().split('T')[0]
        }),
      ];
      
      const result = calculateMaxDaysInvested(assets);
      // Should be approximately 365 days, with some variance due to date calculation
      expect(result).toBeGreaterThanOrEqual(364);
      expect(result).toBeLessThanOrEqual(366);
    });

    it('should return max days among multiple assets', () => {
      const twoYearsAgo = new Date();
      twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
      
      const oneMonthAgo = new Date();
      oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
      
      const assets = [
        createMockAsset({ 
          isActive: true, 
          acquisitionDate: twoYearsAgo.toISOString().split('T')[0]
        }),
        createMockAsset({ 
          isActive: true, 
          acquisitionDate: oneMonthAgo.toISOString().split('T')[0]
        }),
      ];
      
      const result = calculateMaxDaysInvested(assets);
      // Should be approximately 730 days
      expect(result).toBeGreaterThan(700);
    });
  });

  // ==========================================================================
  // calculateDaysInvested
  // ==========================================================================

  describe('calculateDaysInvested', () => {
    it('should calculate days correctly for recent date', () => {
      const oneWeekAgo = new Date();
      oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);
      
      const dateString = oneWeekAgo.toISOString().split('T')[0];
      const result = calculateDaysInvested(dateString);
      
      expect(result).toBeGreaterThanOrEqual(6);
      expect(result).toBeLessThanOrEqual(8);
    });

    it('should return positive number for past date', () => {
      const result = calculateDaysInvested('2020-01-01');
      expect(result).toBeGreaterThan(1000);
    });
  });
});
