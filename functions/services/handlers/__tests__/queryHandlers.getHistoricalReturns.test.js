/**
 * PERF-SNAP-007: Tests para getHistoricalReturns con snapshots
 *
 * @see docs/stories/PERF-SNAP-007.story.md
 */

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

const mockGetHistoricalReturnsV2 = jest.fn();
const mockCheckConsolidatedDataStatus = jest.fn();

jest.mock('../../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: (...args) => mockGetHistoricalReturnsV2(...args),
  checkConsolidatedDataStatus: (...args) => mockCheckConsolidatedDataStatus(...args),
}));

jest.mock('../../historicalReturnsService', () => ({
  calculateHistoricalReturns: jest.fn(),
  getHistoricalReturnsInternal: jest.fn(),
}));

jest.mock('../../cacheInvalidationService', () => ({
  calculateDynamicTTL: jest.fn(() => new Date('2026-04-12T16:00:00Z')),
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

// Mock snapshotGenerator
const mockBuildSnapshotDocId = jest.fn();
const mockGeneratePerformanceSnapshot = jest.fn();
jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: (...args) => mockBuildSnapshotDocId(...args),
  generatePerformanceSnapshot: (...args) => mockGeneratePerformanceSnapshot(...args),
}));

// PERF-SNAP-021: Mock riskMetricsCache (required by snapshot mem cache)
jest.mock('../../riskMetrics/riskMetricsCache', () => ({
  isNYSEMarketOpen: jest.fn(() => false),
  calculateTTLUntilNextEOD: jest.fn(() => 6 * 60 * 60 * 1000),
  MARKET_CACHE_TTL_MS: 5 * 60 * 1000,
}));

// Mock firebaseAdmin BEFORE requiring queryHandlers
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
  getHistoricalReturns,
  transformSnapshotToResponse,
  getHistoricalReturnsLegacy,
  clearSnapshotMemCache,
} = require('../queryHandlers');

const mockSnapshot = {
  userId: 'user1',
  currency: 'USD',
  accountId: 'overall',
  lastUpdated: '2026-04-11T00:05:00Z',
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
    ['2026-04-09', 50000, 0],
    ['2026-04-10', 50250, 0.5],
    ['2026-04-11', 50500, 0.497],
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
  latestAssetPerformance: {
    'AAPL_stock': { totalValue: 18500, totalInvestment: 15000, units: 10, unrealizedPnL: 3500, totalROI: 23.33, dailyChangePercentage: 1.2 },
  },
};

const mockV2Result = {
  returns: { ytdReturn: 12.5, hasYtdData: true },
  totalValueData: { dates: ['2026-04-11'], values: [50500], percentChanges: [0.497], overallPercentChange: 1.0 },
  performanceByYear: {},
  monthlyCompoundData: {},
  availableYears: ['2026'],
  startDate: '2025-01-02',
  validDocsCountByPeriod: { ytd: 72 },
  _metadata: { version: 'v2', duration: 50, docsRead: 30 },
};

function createContext(userId = 'user1') {
  return { auth: { uid: userId } };
}

// ============================================================================
// Tests
// ============================================================================

describe('PERF-SNAP-007: getHistoricalReturns with snapshots', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    clearSnapshotMemCache();
    mockBuildSnapshotDocId.mockImplementation((userId, accountId, currency) => {
      if (accountId === 'overall') return `${userId}_${currency}`;
      return `${userId}_${accountId}_${currency}`;
    });
  });

  // ==========================================================================
  // transformSnapshotToResponse
  // ==========================================================================

  describe('transformSnapshotToResponse', () => {
    it('should expand timeline to totalValueData format (AC3)', () => {
      const result = transformSnapshotToResponse(mockSnapshot);

      expect(result.totalValueData.dates).toEqual(['2026-04-09', '2026-04-10', '2026-04-11']);
      expect(result.totalValueData.values).toEqual([50000, 50250, 50500]);
      expect(result.totalValueData.percentChanges).toEqual([0, 0.5, 0.497]);
    });

    it('should calculate overallPercentChange from first and last value', () => {
      const result = transformSnapshotToResponse(mockSnapshot);

      const expected = ((50500 - 50000) / 50000) * 100;
      expect(result.totalValueData.overallPercentChange).toBe(expected);
    });

    it('should pass returns directly (AC3)', () => {
      const result = transformSnapshotToResponse(mockSnapshot);
      expect(result.returns).toEqual(mockSnapshot.returns);
    });

    it('should map monthlyCompound to monthlyCompoundData (AC3)', () => {
      const result = transformSnapshotToResponse(mockSnapshot);
      expect(result.monthlyCompoundData).toEqual(mockSnapshot.monthlyCompound);
    });

    it('should include latestAssetPerformance', () => {
      const result = transformSnapshotToResponse(mockSnapshot);
      expect(result.latestAssetPerformance).toEqual(mockSnapshot.latestAssetPerformance);
    });

    it('should include _metadata with source snapshot', () => {
      const result = transformSnapshotToResponse(mockSnapshot);
      expect(result._metadata.version).toBe('snapshot');
      expect(result._metadata.schemaVersion).toBe(1);
      expect(result._metadata.snapshotLastUpdated).toBe('2026-04-11T00:05:00Z');
    });

    it('should handle empty timeline gracefully', () => {
      const result = transformSnapshotToResponse({ ...mockSnapshot, timeline: [] });

      expect(result.totalValueData.dates).toEqual([]);
      expect(result.totalValueData.values).toEqual([]);
      expect(result.totalValueData.overallPercentChange).toBe(0);
    });

    it('should pass through validDocsCountByPeriod and availableYears (AC3)', () => {
      const result = transformSnapshotToResponse(mockSnapshot);
      expect(result.validDocsCountByPeriod).toEqual(mockSnapshot.validDocsCountByPeriod);
      expect(result.availableYears).toEqual(mockSnapshot.availableYears);
      expect(result.startDate).toBe(mockSnapshot.startDate);
    });
  });

  // ==========================================================================
  // getHistoricalReturns — snapshot path
  // ==========================================================================

  describe('getHistoricalReturns — snapshot path', () => {
    it('should read snapshot for overall and not call V2 (AC1, AC5)', async () => {
      mockSnapshotGet.mockResolvedValue({ exists: true, data: () => mockSnapshot });

      const result = await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD' }
      );

      expect(mockBuildSnapshotDocId).toHaveBeenCalledWith('user1', 'overall', 'USD');
      expect(result.returns).toEqual(mockSnapshot.returns);
      expect(result._metadata.version).toBe('snapshot');
      expect(mockGetHistoricalReturnsV2).not.toHaveBeenCalled();
    });

    it('should read snapshot for specific account (AC2)', async () => {
      mockSnapshotGet.mockResolvedValue({ exists: true, data: () => mockSnapshot });

      await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD', accountId: 'acc123' }
      );

      expect(mockBuildSnapshotDocId).toHaveBeenCalledWith('user1', 'acc123', 'USD');
    });

    it('should include cacheHit: false and timestamps in response', async () => {
      mockSnapshotGet.mockResolvedValue({ exists: true, data: () => mockSnapshot });

      const result = await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD' }
      );

      expect(result.cacheHit).toBe(false);
      expect(result.lastCalculated).toBeDefined();
      expect(result.validUntil).toBeDefined();
    });

    it('should produce totalValueData identical to V2 format (AC3)', async () => {
      mockSnapshotGet.mockResolvedValue({ exists: true, data: () => mockSnapshot });

      const result = await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD' }
      );

      expect(result.totalValueData).toHaveProperty('dates');
      expect(result.totalValueData).toHaveProperty('values');
      expect(result.totalValueData).toHaveProperty('percentChanges');
      expect(result.totalValueData).toHaveProperty('overallPercentChange');
      expect(result.monthlyCompoundData).toBeDefined();
    });
  });

  // ==========================================================================
  // getHistoricalReturns — ticker/assetType → legacy (AC4)
  // ==========================================================================

  describe('getHistoricalReturns — ticker/assetType redirect (AC4)', () => {
    it('should use legacy path when ticker is provided', async () => {
      mockSnapshotGet.mockResolvedValue({ exists: false });
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);

      const result = await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD', ticker: 'AAPL' }
      );

      expect(mockBuildSnapshotDocId).not.toHaveBeenCalled();
      expect(mockGetHistoricalReturnsV2).toHaveBeenCalled();
    });

    it('should use legacy path when assetType is provided', async () => {
      mockSnapshotGet.mockResolvedValue({ exists: false });
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);

      const result = await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD', assetType: 'stock' }
      );

      expect(mockBuildSnapshotDocId).not.toHaveBeenCalled();
      expect(mockGetHistoricalReturnsV2).toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // getHistoricalReturns — fallback to legacy
  // ==========================================================================

  describe('getHistoricalReturns — fallback', () => {
    it('should fallback to legacy when snapshot does not exist', async () => {
      mockSnapshotGet.mockResolvedValue({ exists: false });
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      mockGeneratePerformanceSnapshot.mockResolvedValue(true);

      const result = await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD' }
      );

      expect(mockGetHistoricalReturnsV2).toHaveBeenCalled();
      expect(result.cacheHit).toBe(false);
    });

    it('should skip snapshot and use legacy when forceRefresh is true', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);

      const result = await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD', forceRefresh: true }
      );

      expect(mockBuildSnapshotDocId).not.toHaveBeenCalled();
      expect(mockGetHistoricalReturnsV2).toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // PERF-SNAP-009: Fallback logs + on-demand generation
  // ==========================================================================

  describe('PERF-SNAP-009: fallback log + on-demand generation', () => {
    let consoleSpy;

    beforeEach(() => {
      consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      mockGeneratePerformanceSnapshot.mockResolvedValue(true);
    });

    afterEach(() => {
      consoleSpy.mockRestore();
    });

    it('should log [PERF] with snapshotId on fallback (AC3)', async () => {
      mockSnapshotGet.mockResolvedValue({ exists: false });
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);

      await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD' }
      );

      const perfLog = consoleSpy.mock.calls.find(
        call => typeof call[0] === 'string' && call[0].includes('[PERF] Snapshot not found')
      );
      expect(perfLog).toBeDefined();
      expect(perfLog[0]).toContain('user1_USD');
    });

    it('should invoke generatePerformanceSnapshot fire-and-forget on fallback (AC4)', async () => {
      mockSnapshotGet.mockResolvedValue({ exists: false });
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);

      await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD', accountId: 'acc1' }
      );

      expect(mockGeneratePerformanceSnapshot).toHaveBeenCalledWith(
        expect.anything(), 'user1', 'acc1', 'USD'
      );
    });

    it('should not propagate error from on-demand generation failure (AC5)', async () => {
      mockSnapshotGet.mockResolvedValue({ exists: false });
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      mockGeneratePerformanceSnapshot.mockRejectedValue(new Error('generation failed'));

      const warnSpy = jest.spyOn(console, 'warn').mockImplementation();

      const result = await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD' }
      );

      // Should return normally despite on-demand failure
      expect(result.returns).toBeDefined();

      // Wait for fire-and-forget promise to settle
      await new Promise(resolve => setTimeout(resolve, 10));
      
      const warnLog = warnSpy.mock.calls.find(
        call => typeof call[0] === 'string' && call[0].includes('[PERF] On-demand snapshot generation failed')
      );
      expect(warnLog).toBeDefined();
      warnSpy.mockRestore();
    });

    it('should not invoke on-demand generation when forceRefresh skips snapshot (AC4)', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);

      await getHistoricalReturns(
        createContext('user1'),
        { currency: 'USD', forceRefresh: true }
      );

      expect(mockGeneratePerformanceSnapshot).not.toHaveBeenCalled();
    });
  });
});
