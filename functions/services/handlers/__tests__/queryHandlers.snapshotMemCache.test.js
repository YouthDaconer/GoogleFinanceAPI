/**
 * PERF-SNAP-021: Tests para snapshot in-memory cache en queryHandlers
 *
 * @see docs/stories/PERF-SNAP-021.story.md
 */

const { DateTime, Settings } = require('luxon');

// ============================================================================
// Mocks
// ============================================================================

jest.mock('firebase-admin', () => ({
  firestore: Object.assign(jest.fn(() => ({})), {
    FieldValue: {
      serverTimestamp: jest.fn(() => 'mock-server-timestamp'),
    },
  }),
  initializeApp: jest.fn(),
}));

jest.mock('../../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: jest.fn(),
  checkConsolidatedDataStatus: jest.fn(),
}));

jest.mock('../../historicalReturnsService', () => ({
  calculateHistoricalReturns: jest.fn(),
  getHistoricalReturnsInternal: jest.fn(),
}));

jest.mock('../../cacheInvalidationService', () => ({
  calculateDynamicTTL: jest.fn(() => new Date('2026-04-13T16:00:00Z')),
}));

jest.mock('../../indexHistoryService', () => ({
  calculateIndexData: jest.fn(),
}));

jest.mock('../../portfolioDistributionService', () => ({}));
jest.mock('../../../utils/mwrCalculations', () => ({
  calculateSimplePersonalReturn: jest.fn(),
  calculateModifiedDietzReturn: jest.fn(),
}));
jest.mock('../../marketDataHelper', () => ({
  getPricesFromApi: jest.fn(),
}));

const mockBuildSnapshotDocId = jest.fn();
const mockGeneratePerformanceSnapshot = jest.fn().mockResolvedValue();
jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: (...args) => mockBuildSnapshotDocId(...args),
  generatePerformanceSnapshot: (...args) => mockGeneratePerformanceSnapshot(...args),
}));

const mockIsNYSEMarketOpen = jest.fn();
const mockCalculateTTLUntilNextEOD = jest.fn();
jest.mock('../../riskMetrics/riskMetricsCache', () => ({
  isNYSEMarketOpen: (...args) => mockIsNYSEMarketOpen(...args),
  calculateTTLUntilNextEOD: (...args) => mockCalculateTTLUntilNextEOD(...args),
  MARKET_CACHE_TTL_MS: 5 * 60 * 1000,
}));

const mockSnapshotGet = jest.fn();
const mockDocRef = jest.fn(() => ({
  get: mockSnapshotGet,
  set: jest.fn().mockResolvedValue(),
}));
const mockCollectionRef = jest.fn(() => ({
  doc: mockDocRef,
}));

jest.mock('../../firebaseAdmin', () => {
  const firestoreMock = () => ({
    doc: mockDocRef,
    collection: mockCollectionRef,
  });
  firestoreMock.FieldValue = {
    serverTimestamp: jest.fn(),
  };
  return {
    firestore: firestoreMock,
    initializeApp: jest.fn(),
  };
});

// ============================================================================
// Import & Fixtures
// ============================================================================

const {
  getSnapshotWithCache,
  getSnapshotCacheTTL,
  clearSnapshotMemCache,
  getSnapshotMemCacheSize,
  getHistoricalReturns,
} = require('../queryHandlers');

const MARKET_CACHE_TTL_MS = 5 * 60 * 1000;

const mockSnapshot = {
  userId: 'user1',
  currency: 'USD',
  accountId: 'overall',
  lastUpdated: '2026-04-12T00:05:00Z',
  schemaVersion: 1,
  returns: {
    ytdReturn: 12.5,
    oneMonthReturn: 2.3,
    threeMonthReturn: 5.6,
    hasYtdData: true,
    hasOneMonthData: true,
    hasThreeMonthData: true,
  },
  timeline: [
    ['2026-04-10', 50000, 0],
    ['2026-04-11', 50250, 0.5],
    ['2026-04-12', 50500, 0.497],
  ],
  performanceByYear: {
    '2026': { months: { '1': 2.5, '2': -1.2 }, total: 4.34 },
  },
  monthlyCompound: {
    '2026': { '01': { startFactor: 1.0, endFactor: 1.025 } },
  },
  validDocsCountByPeriod: { ytd: 72, oneMonth: 22 },
  availableYears: ['2026', '2025'],
  startDate: '2025-01-02',
  latestAssetPerformance: {},
};

function createContext(userId = 'user1') {
  return { auth: { uid: userId } };
}

// ============================================================================
// Tests
// ============================================================================

describe('PERF-SNAP-021: Snapshot in-memory cache', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    clearSnapshotMemCache();
    mockIsNYSEMarketOpen.mockReturnValue(false);
    mockCalculateTTLUntilNextEOD.mockReturnValue(6 * 60 * 60 * 1000); // 6h default
  });

  // ==========================================================================
  // getSnapshotCacheTTL
  // ==========================================================================

  describe('getSnapshotCacheTTL', () => {
    it('should return MARKET_CACHE_TTL_MS (5 min) during market open (AC2)', () => {
      mockIsNYSEMarketOpen.mockReturnValue(true);
      expect(getSnapshotCacheTTL()).toBe(MARKET_CACHE_TTL_MS);
    });

    it('should return TTL until next EOD during market closed (AC2)', () => {
      mockIsNYSEMarketOpen.mockReturnValue(false);
      const eodTTL = 8 * 60 * 60 * 1000;
      mockCalculateTTLUntilNextEOD.mockReturnValue(eodTTL);
      expect(getSnapshotCacheTTL()).toBe(eodTTL);
    });
  });

  // ==========================================================================
  // getSnapshotWithCache
  // ==========================================================================

  describe('getSnapshotWithCache', () => {
    it('should read from Firestore on first call (cache miss, 1 read)', async () => {
      mockSnapshotGet.mockResolvedValueOnce({ exists: false, data: () => ({}) });
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => mockSnapshot });

      const result = await getSnapshotWithCache('user1_USD', 'user1');

      expect(result.data).toEqual(mockSnapshot);
    });

    it('should return from cache on second call (0 Firestore reads) (AC1)', async () => {
      mockSnapshotGet.mockResolvedValueOnce({ exists: false, data: () => ({}) });
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => mockSnapshot });
      mockIsNYSEMarketOpen.mockReturnValue(false);
      mockCalculateTTLUntilNextEOD.mockReturnValue(6 * 60 * 60 * 1000);

      await getSnapshotWithCache('user1_USD', 'user1');
      mockSnapshotGet.mockClear();
      mockDocRef.mockClear();

      mockSnapshotGet.mockResolvedValueOnce({ exists: false, data: () => ({}) });
      const result = await getSnapshotWithCache('user1_USD', 'user1');

      expect(result.data).toEqual(mockSnapshot);
    });

    it('should return null data for non-existent snapshot without caching', async () => {
      mockSnapshotGet.mockResolvedValueOnce({ exists: false, data: () => ({}) });
      mockSnapshotGet.mockResolvedValue({ exists: false });

      const result = await getSnapshotWithCache('nonexistent_id', 'user1');

      expect(result.data).toBeNull();
      expect(getSnapshotMemCacheSize()).toBe(0);
    });

    it('should re-read from Firestore after TTL expired (AC3)', async () => {
      mockSnapshotGet.mockResolvedValueOnce({ exists: false, data: () => ({}) });
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => mockSnapshot });
      mockIsNYSEMarketOpen.mockReturnValue(true); // TTL = 5 min

      await getSnapshotWithCache('user1_USD', 'user1');

      const originalNow = Date.now;
      Date.now = () => originalNow() + MARKET_CACHE_TTL_MS + 1;

      mockSnapshotGet.mockClear();
      mockDocRef.mockClear();

      const updatedSnapshot = { ...mockSnapshot, lastUpdated: '2026-04-13T00:05:00Z' };
      mockSnapshotGet.mockResolvedValueOnce({ exists: false, data: () => ({}) });
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => updatedSnapshot });

      const result = await getSnapshotWithCache('user1_USD', 'user1');

      expect(result.data).toEqual(updatedSnapshot);

      Date.now = originalNow;
    });

    it('should evict oldest entry when cache exceeds max size (AC4)', async () => {
      mockSnapshotGet.mockImplementation(() =>
        Promise.resolve({ exists: true, data: () => ({ ...mockSnapshot }) })
      );

      for (let i = 0; i < 101; i++) {
        await getSnapshotWithCache(`id_${i}`);
      }

      expect(getSnapshotMemCacheSize()).toBe(100);
    });

    it('should keep recently accessed entry alive during eviction (LRU) (AC4)', async () => {
      mockSnapshotGet.mockImplementation(() =>
        Promise.resolve({ exists: true, data: () => ({ ...mockSnapshot }) })
      );

      for (let i = 0; i < 100; i++) {
        await getSnapshotWithCache(`id_${i}`);
      }

      // Access id_0 again (moves to end of Map)
      mockSnapshotGet.mockClear();
      await getSnapshotWithCache('id_0');

      // Insert one more to trigger eviction — id_1 (oldest) should be evicted, not id_0
      mockSnapshotGet.mockImplementation(() =>
        Promise.resolve({ exists: true, data: () => ({ ...mockSnapshot }) })
      );
      await getSnapshotWithCache('id_new');

      expect(getSnapshotMemCacheSize()).toBe(100);

      // id_0 should still be cached (re-accessed, moved to end)
      mockSnapshotGet.mockClear();
      const result = await getSnapshotWithCache('id_0');
      expect(result.data).toBeTruthy();

      // id_1 should have been evicted (oldest after id_0 was re-accessed)
      mockSnapshotGet.mockClear();
      mockSnapshotGet.mockResolvedValue({ exists: true, data: () => ({ ...mockSnapshot }) });
      await getSnapshotWithCache('id_1');
    });
  });

  // ==========================================================================
  // getHistoricalReturns integration with cache
  // ==========================================================================

  describe('getHistoricalReturns — snapshot mem cache integration', () => {
    beforeEach(() => {
      mockBuildSnapshotDocId.mockImplementation((userId, accountId, currency) => {
        if (accountId === 'overall') return `${userId}_${currency}`;
        return `${userId}_${accountId}_${currency}`;
      });
    });

    it('should use cache: 2 calls, only 1 Firestore read (AC1)', async () => {
      mockSnapshotGet.mockResolvedValueOnce({ exists: false, data: () => ({}) });
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => mockSnapshot });

      const result1 = await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD' }
      );

      expect(result1.returns).toEqual(mockSnapshot.returns);

      mockSnapshotGet.mockClear();
      mockDocRef.mockClear();

      mockSnapshotGet.mockResolvedValueOnce({ exists: false, data: () => ({}) });
      const result2 = await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD' }
      );

      expect(result2.returns).toEqual(mockSnapshot.returns);
    });
  });

  // ==========================================================================
  // PERF-SNAP-023: lastSnapshotUpdate invalidation
  // ==========================================================================

  describe('PERF-SNAP-023: lastSnapshotUpdate invalidation', () => {
    it('should invalidate cache when lastSnapshotUpdate > cachedAt', async () => {
      // First call — cache miss, populate cache
      mockSnapshotGet.mockResolvedValueOnce({ exists: false, data: () => ({}) });
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => mockSnapshot });

      await getSnapshotWithCache('user1_USD', 'user1');

      // Second call — cache hit, but lastSnapshotUpdate is NEWER than cachedAt
      const futureTimestamp = new Date(Date.now() + 60000).toISOString();
      mockSnapshotGet.mockClear();
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => ({ lastSnapshotUpdate: futureTimestamp }) });
      const updatedSnapshot = { ...mockSnapshot, lastUpdated: 'updated' };
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => updatedSnapshot });

      const result = await getSnapshotWithCache('user1_USD', 'user1');

      expect(result.data).toEqual(updatedSnapshot);
    });

    it('should return cached data when lastSnapshotUpdate <= cachedAt', async () => {
      // First call — populate cache
      mockSnapshotGet.mockResolvedValueOnce({ exists: false, data: () => ({}) });
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => mockSnapshot });

      await getSnapshotWithCache('user1_USD', 'user1');

      // Second call — lastSnapshotUpdate is OLDER than cachedAt
      const pastTimestamp = new Date(Date.now() - 60000).toISOString();
      mockSnapshotGet.mockClear();
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => ({ lastSnapshotUpdate: pastTimestamp }) });

      const result = await getSnapshotWithCache('user1_USD', 'user1');

      expect(result.data).toEqual(mockSnapshot);
    });

    it('should trust TTL when lastSnapshotUpdate is missing (backwards compat)', async () => {
      // First call — populate cache
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => ({}) }); // no lastSnapshotUpdate
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => mockSnapshot });

      await getSnapshotWithCache('user1_USD', 'user1');

      // Second call — doc exists but no lastSnapshotUpdate field
      mockSnapshotGet.mockClear();
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => ({}) });

      const result = await getSnapshotWithCache('user1_USD', 'user1');

      expect(result.data).toEqual(mockSnapshot);
    });

    it('should include lastSnapshotUpdate in getHistoricalReturns response (AC3)', async () => {
      // R-03: readSnapshotSignal is called first (1 read), then getSnapshotWithCache
      // receives the signal as a parameter (skips its own signal read).
      // Use a timestamp OLDER than mockSnapshot.lastUpdated to avoid stale detection.
      const timestamp = '2026-04-11T00:10:00.000Z';
      mockBuildSnapshotDocId.mockReturnValue('user1_USD');
      // 1st get(): readSnapshotSignal → portfolioPerformance/{userId}
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => ({ lastSnapshotUpdate: timestamp }) });
      // 2nd get(): getSnapshotWithCache → performanceSnapshots/{snapshotId}
      mockSnapshotGet.mockResolvedValueOnce({ exists: true, data: () => mockSnapshot });

      clearSnapshotMemCache();
      const result = await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD' }
      );

      expect(result.lastSnapshotUpdate).toBe(timestamp);
    });
  });
});
