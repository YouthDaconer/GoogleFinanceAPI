/**
 * Tests for normalizeToUsdBase (SCALE-005)
 * 
 * @module __tests__/services/marketDataHelper.normalizeToUsdBase.test
 * @see docs/stories/SCALE-005.story.md
 */

const { normalizeToUsdBase } = require('../marketDataHelper');

describe('normalizeToUsdBase', () => {

  test('USD always returns 1 regardless of input', () => {
    expect(normalizeToUsdBase('USD', 1)).toBe(1);
    expect(normalizeToUsdBase('USD', 999)).toBe(1);
    expect(normalizeToUsdBase('USD', 0)).toBe(1);
  });

  test('EUR with rawRate > 1 is inverted (1 EUR = 1.09 USD → 1 USD = 0.917 EUR)', () => {
    const result = normalizeToUsdBase('EUR', 1.09);
    expect(result).toBeCloseTo(0.9174, 3);
  });

  test('GBP with rawRate > 1 is inverted (1 GBP = 1.27 USD → 1 USD = 0.787 GBP)', () => {
    const result = normalizeToUsdBase('GBP', 1.27);
    expect(result).toBeCloseTo(0.7874, 3);
  });

  test('AUD with rawRate > 1 is inverted', () => {
    const result = normalizeToUsdBase('AUD', 1.55);
    expect(result).toBeCloseTo(0.6452, 3);
  });

  test('NZD with rawRate > 1 is inverted', () => {
    const result = normalizeToUsdBase('NZD', 1.72);
    expect(result).toBeCloseTo(0.5814, 3);
  });

  test('COP is returned directly (already "1 USD = X COP")', () => {
    expect(normalizeToUsdBase('COP', 4285.50)).toBe(4285.50);
  });

  test('MXN is returned directly', () => {
    expect(normalizeToUsdBase('MXN', 17.82)).toBe(17.82);
  });

  test('BRL is returned directly', () => {
    expect(normalizeToUsdBase('BRL', 5.1245)).toBe(5.1245);
  });

  test('CAD is returned directly', () => {
    expect(normalizeToUsdBase('CAD', 1.3645)).toBe(1.3645);
  });

  test('rawRate of 0 does not cause division by zero', () => {
    expect(normalizeToUsdBase('EUR', 0)).toBe(0);
    expect(normalizeToUsdBase('COP', 0)).toBe(0);
  });

  test('null or undefined rawRate is returned as-is', () => {
    expect(normalizeToUsdBase('EUR', null)).toBe(null);
    expect(normalizeToUsdBase('COP', undefined)).toBe(undefined);
  });

  test('negative rawRate is returned as-is (no inversion)', () => {
    expect(normalizeToUsdBase('EUR', -1.09)).toBe(-1.09);
    expect(normalizeToUsdBase('COP', -4285)).toBe(-4285);
  });
});
