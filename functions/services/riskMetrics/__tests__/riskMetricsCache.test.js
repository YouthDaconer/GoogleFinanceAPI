const { DateTime, Settings } = require('luxon');

const {
  buildRiskCacheKey,
  getCachedRiskMetrics,
  setCachedRiskMetrics,
  getRiskCacheTTL,
  isNYSEMarketOpen,
  calculateTTLUntilNextEOD,
  clearCache,
  getCacheSize,
  RISK_CACHE_MAX_SIZE,
  MARKET_CACHE_TTL_MS,
} = require('../riskMetricsCache');

function setFakeNow(opts) {
  const ms = DateTime.fromObject(opts, { zone: 'America/New_York' }).toMillis();
  Settings.now = () => ms;
  return ms;
}

beforeEach(() => {
  clearCache();
  Settings.now = Date.now;
});

afterEach(() => {
  Settings.now = Date.now;
});

// =============================================================================
// buildRiskCacheKey
// =============================================================================
describe('buildRiskCacheKey', () => {
  it('should generate key with sorted accountIds (AC2)', () => {
    const key = buildRiskCacheKey('user1', 'YTD', 'USD', ['acc3', 'acc1', 'acc2']);
    expect(key).toBe('user1_YTD_USD_acc1,acc2,acc3');
  });

  it('should generate key with "overall" when accountIds is empty (AC2)', () => {
    const key = buildRiskCacheKey('user1', '1Y', 'COP', []);
    expect(key).toBe('user1_1Y_COP_overall');
  });

  it('should generate key with "overall" when accountIds is null/undefined (AC2)', () => {
    expect(buildRiskCacheKey('u1', 'YTD', 'USD', null)).toBe('u1_YTD_USD_overall');
    expect(buildRiskCacheKey('u1', 'YTD', 'USD', undefined)).toBe('u1_YTD_USD_overall');
  });

  it('should produce different keys for different parameters', () => {
    const k1 = buildRiskCacheKey('user1', 'YTD', 'USD', []);
    const k2 = buildRiskCacheKey('user1', '1Y', 'USD', []);
    const k3 = buildRiskCacheKey('user1', 'YTD', 'COP', []);
    expect(k1).not.toBe(k2);
    expect(k1).not.toBe(k3);
  });
});

// =============================================================================
// getCachedRiskMetrics / setCachedRiskMetrics
// =============================================================================
describe('getCachedRiskMetrics', () => {
  it('should return data within TTL (AC1)', () => {
    const data = { success: true, sharpe: 1.5 };
    setCachedRiskMetrics('key1', data);

    const result = getCachedRiskMetrics('key1');
    expect(result).toEqual(data);
  });

  it('should return null for non-existent key (AC1)', () => {
    expect(getCachedRiskMetrics('nonexistent')).toBeNull();
  });

  it('should return null when TTL expired (AC3)', () => {
    const data = { success: true };
    setCachedRiskMetrics('key1', data);

    const originalNow = Date.now;
    Date.now = () => originalNow() + MARKET_CACHE_TTL_MS + 1000;

    expect(getCachedRiskMetrics('key1')).toBeNull();

    Date.now = originalNow;
  });
});

// =============================================================================
// setCachedRiskMetrics — eviction
// =============================================================================
describe('setCachedRiskMetrics', () => {
  it('should evict oldest entry when exceeding RISK_CACHE_MAX_SIZE (AC4)', () => {
    for (let i = 0; i <= RISK_CACHE_MAX_SIZE; i++) {
      setCachedRiskMetrics(`key_${i}`, { i });
    }

    expect(getCacheSize()).toBe(RISK_CACHE_MAX_SIZE);
    expect(getCachedRiskMetrics('key_0')).toBeNull();
    expect(getCachedRiskMetrics(`key_${RISK_CACHE_MAX_SIZE}`)).toEqual({ i: RISK_CACHE_MAX_SIZE });
  });
});

// =============================================================================
// LRU re-insert on hit
// =============================================================================
describe('LRU behavior', () => {
  it('should move accessed entry to end of Map on get (AC4)', () => {
    setCachedRiskMetrics('a', { val: 'a' });
    setCachedRiskMetrics('b', { val: 'b' });
    setCachedRiskMetrics('c', { val: 'c' });

    getCachedRiskMetrics('a');

    setCachedRiskMetrics('d', { val: 'd' });
    setCachedRiskMetrics('e', { val: 'e' });

    for (let i = 0; i < RISK_CACHE_MAX_SIZE - 5; i++) {
      setCachedRiskMetrics(`fill_${i}`, { i });
    }
    setCachedRiskMetrics('overflow1', { val: 'overflow1' });

    expect(getCachedRiskMetrics('a')).not.toBeNull();
  });
});

// =============================================================================
// isNYSEMarketOpen
// =============================================================================
describe('isNYSEMarketOpen', () => {
  it('should return true during market hours on a weekday (AC3)', () => {
    setFakeNow({ year: 2026, month: 4, day: 8, hour: 10, minute: 0 });
    expect(isNYSEMarketOpen()).toBe(true);
  });

  it('should return true at 9:30 AM ET exactly (market open)', () => {
    setFakeNow({ year: 2026, month: 4, day: 8, hour: 9, minute: 30 });
    expect(isNYSEMarketOpen()).toBe(true);
  });

  it('should return false before 9:30 AM ET (AC3)', () => {
    setFakeNow({ year: 2026, month: 4, day: 8, hour: 9, minute: 29 });
    expect(isNYSEMarketOpen()).toBe(false);
  });

  it('should return false at 4:00 PM ET exactly (market close)', () => {
    setFakeNow({ year: 2026, month: 4, day: 8, hour: 16, minute: 0 });
    expect(isNYSEMarketOpen()).toBe(false);
  });

  it('should return false on Saturday (AC3)', () => {
    setFakeNow({ year: 2026, month: 4, day: 11, hour: 12, minute: 0 });
    expect(isNYSEMarketOpen()).toBe(false);
  });

  it('should return false on Sunday (AC3)', () => {
    setFakeNow({ year: 2026, month: 4, day: 12, hour: 12, minute: 0 });
    expect(isNYSEMarketOpen()).toBe(false);
  });
});

// =============================================================================
// getRiskCacheTTL
// =============================================================================
describe('getRiskCacheTTL', () => {
  it('should return 5 minutes during market hours (AC3)', () => {
    setFakeNow({ year: 2026, month: 4, day: 8, hour: 10, minute: 0 });
    expect(getRiskCacheTTL()).toBe(MARKET_CACHE_TTL_MS);
  });

  it('should return ms until next EOD outside market hours (AC3)', () => {
    // Wednesday 5:00 PM ET → Thursday 4:00 PM ET = ~23h
    setFakeNow({ year: 2026, month: 4, day: 8, hour: 17, minute: 0 });

    const ttl = getRiskCacheTTL();
    expect(ttl).toBeGreaterThan(MARKET_CACHE_TTL_MS);

    const expectedMs = 23 * 60 * 60 * 1000;
    expect(ttl).toBeCloseTo(expectedMs, -4);
  });

  it('should return ms until Monday EOD on Friday evening (AC3)', () => {
    // Friday 2026-04-10 5:00 PM ET → Monday 2026-04-13 4:00 PM ET = ~71h
    setFakeNow({ year: 2026, month: 4, day: 10, hour: 17, minute: 0 });

    const ttl = getRiskCacheTTL();
    const expectedMs = 71 * 60 * 60 * 1000;
    expect(ttl).toBeCloseTo(expectedMs, -4);
  });
});

// =============================================================================
// calculateTTLUntilNextEOD
// =============================================================================
describe('calculateTTLUntilNextEOD', () => {
  it('should return at least MARKET_CACHE_TTL_MS', () => {
    setFakeNow({ year: 2026, month: 4, day: 10, hour: 15, minute: 59 });
    expect(calculateTTLUntilNextEOD()).toBeGreaterThanOrEqual(MARKET_CACHE_TTL_MS);
  });

  it('should skip weekends', () => {
    // Saturday 10:00 AM → Monday 4:00 PM = ~54h
    setFakeNow({ year: 2026, month: 4, day: 11, hour: 10, minute: 0 });

    const ttl = calculateTTLUntilNextEOD();
    const expectedMs = 54 * 60 * 60 * 1000;
    expect(ttl).toBeCloseTo(expectedMs, -4);
  });
});
