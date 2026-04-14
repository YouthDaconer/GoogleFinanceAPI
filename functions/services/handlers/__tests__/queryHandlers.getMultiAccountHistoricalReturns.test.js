/**
 * PERF-SNAP-008: Tests para getMultiAccountHistoricalReturns con snapshots
 *
 * @see docs/stories/PERF-SNAP-008.story.md
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

const mockCalculateHistoricalReturns = jest.fn();
const mockGetHistoricalReturnsInternal = jest.fn();

jest.mock('../../historicalReturnsService', () => ({
  calculateHistoricalReturns: (...args) => mockCalculateHistoricalReturns(...args),
  getHistoricalReturnsInternal: (...args) => mockGetHistoricalReturnsInternal(...args),
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

const mockBuildSnapshotDocId = jest.fn();
const mockGeneratePerformanceSnapshot = jest.fn();
jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: (...args) => mockBuildSnapshotDocId(...args),
  generatePerformanceSnapshot: (...args) => mockGeneratePerformanceSnapshot(...args),
}));

// Mock Firestore
const mockSnapshotGet = jest.fn();
const mockDocSet = jest.fn().mockResolvedValue();
const mockDocRef = jest.fn(() => ({
  get: mockSnapshotGet,
  set: mockDocSet,
}));

const mockCollectionGet = jest.fn();
const mockCollectionWhere = jest.fn();
const mockCollectionOrderBy = jest.fn();
const mockCollectionRef = jest.fn(() => ({
  doc: mockDocRef,
  where: mockCollectionWhere,
  orderBy: mockCollectionOrderBy,
}));

mockCollectionWhere.mockReturnValue({
  where: mockCollectionWhere,
  get: mockCollectionGet,
});

mockCollectionOrderBy.mockReturnValue({
  get: mockCollectionGet,
});

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
  getMultiAccountHistoricalReturns,
  aggregateSnapshotTimelines,
  getMultiAccountHistoricalReturnsLegacy,
} = require('../queryHandlers');

function createContext(userId = 'user1') {
  return { auth: { uid: userId } };
}

function makeSnapshot(accountId, timeline) {
  return {
    userId: 'user1',
    currency: 'USD',
    accountId,
    lastUpdated: '2026-04-11T00:05:00Z',
    schemaVersion: 1,
    returns: {
      ytdReturn: 10,
      oneMonthReturn: 2,
      threeMonthReturn: 5,
      hasYtdData: true,
      hasOneMonthData: true,
      hasThreeMonthData: true,
    },
    timeline,
    performanceByYear: { '2026': { months: { '1': 2 }, total: 2 } },
    monthlyCompound: { '2026': { '01': { startFactor: 1.0, endFactor: 1.02 } } },
    validDocsCountByPeriod: { ytd: 50, oneMonth: 20 },
    availableYears: ['2026'],
    startDate: '2026-01-02',
  };
}

const MOCK_CALC_RESULT = {
  returns: { ytdReturn: 8.5, hasYtdData: true },
  totalValueData: {
    dates: ['2026-04-09', '2026-04-10', '2026-04-11'],
    values: [80000, 80600, 81200],
    percentChanges: [0, 0.75, 0.744],
    overallPercentChange: 1.5,
  },
  performanceByYear: { '2026': { total: 8.5 } },
  monthlyCompoundData: {},
  availableYears: ['2026'],
  startDate: '2026-01-02',
  validDocsCountByPeriod: { ytd: 50 },
};

// ============================================================================
// Tests
// ============================================================================

describe('PERF-SNAP-008: aggregateSnapshotTimelines', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockCalculateHistoricalReturns.mockReturnValue(MOCK_CALC_RESULT);
  });

  it('should align overlapping timelines and sum totalValue (AC2)', () => {
    const snap1 = makeSnapshot('acc1', [
      ['2026-04-09', 30000, 0],
      ['2026-04-10', 30150, 0.5],
      ['2026-04-11', 30300, 0.497],
    ]);
    const snap2 = makeSnapshot('acc2', [
      ['2026-04-09', 50000, 0],
      ['2026-04-10', 50450, 0.9],
      ['2026-04-11', 50900, 0.892],
    ]);

    aggregateSnapshotTimelines([snap1, snap2], 'USD');

    expect(mockCalculateHistoricalReturns).toHaveBeenCalledTimes(1);

    const fakeDocs = mockCalculateHistoricalReturns.mock.calls[0][0];
    expect(fakeDocs).toHaveLength(3);

    const day1 = fakeDocs[0].data();
    expect(day1.date).toBe('2026-04-09');
    expect(day1.USD.totalValue).toBe(80000);

    const day2 = fakeDocs[1].data();
    expect(day2.date).toBe('2026-04-10');
    expect(day2.USD.totalValue).toBeCloseTo(80600, 0);

    const day3 = fakeDocs[2].data();
    expect(day3.date).toBe('2026-04-11');
    expect(day3.USD.totalValue).toBeCloseTo(81200, 0);
  });

  it('should weight dailyChangePercentage by pre-change value (TWR) (AC2)', () => {
    const snap1 = makeSnapshot('acc1', [
      ['2026-04-10', 30000, 0],
      ['2026-04-11', 30300, 1.0],
    ]);
    const snap2 = makeSnapshot('acc2', [
      ['2026-04-10', 70000, 0],
      ['2026-04-11', 70350, 0.5],
    ]);

    aggregateSnapshotTimelines([snap1, snap2], 'USD');

    const fakeDocs = mockCalculateHistoricalReturns.mock.calls[0][0];
    const day2 = fakeDocs[1].data();

    // acc1 preChangeValue = 30300 / (1 + 1.0/100) = 30000
    // acc2 preChangeValue = 70350 / (1 + 0.5/100) = 70000
    // totalWeight = 100000
    // weighted = (1.0 * 30000/100000) + (0.5 * 70000/100000) = 0.30 + 0.35 = 0.65
    expect(day2.USD.dailyChangePercentage).toBeCloseTo(0.65, 4);
    expect(day2.USD.adjustedDailyChangePercentage).toBeCloseTo(0.65, 4);
  });

  it('should handle accounts with different date ranges (AC2)', () => {
    const snap1 = makeSnapshot('acc1', [
      ['2026-04-09', 30000, 0],
      ['2026-04-10', 30150, 0.5],
      ['2026-04-11', 30300, 0.497],
    ]);
    const snap2 = makeSnapshot('acc2', [
      ['2026-04-10', 50000, 0],
      ['2026-04-11', 50250, 0.5],
    ]);

    aggregateSnapshotTimelines([snap1, snap2], 'USD');

    const fakeDocs = mockCalculateHistoricalReturns.mock.calls[0][0];
    expect(fakeDocs).toHaveLength(3);

    // 2026-04-09: only acc1 present
    const day1 = fakeDocs[0].data();
    expect(day1.USD.totalValue).toBe(30000);
    expect(day1.USD.dailyChangePercentage).toBe(0);

    // 2026-04-10: both accounts
    const day2 = fakeDocs[1].data();
    expect(day2.USD.totalValue).toBeCloseTo(80150, 0);
  });

  it('should return the result from calculateHistoricalReturns (AC3)', () => {
    const snap1 = makeSnapshot('acc1', [['2026-04-11', 30000, 0]]);

    const result = aggregateSnapshotTimelines([snap1], 'USD');

    expect(result).toEqual(MOCK_CALC_RESULT);
    expect(mockCalculateHistoricalReturns).toHaveBeenCalledWith(
      expect.any(Array),
      'USD'
    );
  });

  it('should handle empty timelines gracefully', () => {
    const snap1 = makeSnapshot('acc1', []);
    const snap2 = makeSnapshot('acc2', []);

    aggregateSnapshotTimelines([snap1, snap2], 'USD');

    const fakeDocs = mockCalculateHistoricalReturns.mock.calls[0][0];
    expect(fakeDocs).toHaveLength(0);
  });

  it('should handle zero totalWeight without division by zero', () => {
    const snap1 = makeSnapshot('acc1', [['2026-04-11', 0, 0]]);
    const snap2 = makeSnapshot('acc2', [['2026-04-11', 0, 0]]);

    aggregateSnapshotTimelines([snap1, snap2], 'USD');

    const fakeDocs = mockCalculateHistoricalReturns.mock.calls[0][0];
    const day = fakeDocs[0].data();
    expect(day.USD.dailyChangePercentage).toBe(0);
  });
});

describe('PERF-SNAP-008: getMultiAccountHistoricalReturns — snapshot path', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockBuildSnapshotDocId.mockImplementation((userId, accountId, currency) =>
      `${userId}_${accountId}_${currency}`
    );
    mockCalculateHistoricalReturns.mockReturnValue(MOCK_CALC_RESULT);

    // PERF-SNAP-009: Default mock for on-demand generation
    mockGeneratePerformanceSnapshot.mockResolvedValue(true);

    // Default: portfolioAccounts returns 3 accounts (so selectedIds !== allIds)
    mockCollectionGet.mockResolvedValue({
      empty: false,
      docs: [
        { id: 'acc1' }, { id: 'acc2' }, { id: 'acc3' }, { id: 'acc4' },
      ],
    });
  });

  it('should read exactly N snapshots for N accounts (AC1, AC4)', async () => {
    const snap1 = makeSnapshot('acc1', [['2026-04-11', 30000, 0.5]]);
    const snap2 = makeSnapshot('acc2', [['2026-04-11', 50000, 0.9]]);
    const snap3 = makeSnapshot('acc3', [['2026-04-11', 20000, 0.3]]);

    let callIndex = 0;
    mockSnapshotGet.mockImplementation(() => {
      const snaps = [snap1, snap2, snap3];
      return Promise.resolve({
        exists: true,
        data: () => snaps[callIndex++],
      });
    });

    const result = await getMultiAccountHistoricalReturns(
      createContext('user1'),
      { accountIds: ['acc1', 'acc2', 'acc3'], currency: 'USD' }
    );

    // Should call db.doc() 3 times for snapshot reads
    expect(mockBuildSnapshotDocId).toHaveBeenCalledTimes(3);
    expect(mockBuildSnapshotDocId).toHaveBeenCalledWith('user1', 'acc1', 'USD');
    expect(mockBuildSnapshotDocId).toHaveBeenCalledWith('user1', 'acc2', 'USD');
    expect(mockBuildSnapshotDocId).toHaveBeenCalledWith('user1', 'acc3', 'USD');

    expect(result.cacheHit).toBe(false);
    expect(result._metadata.version).toBe('snapshot-multi');
    expect(result.lastCalculated).toBeDefined();
    expect(result.validUntil).toBeDefined();
  });

  it('should fallback to legacy when one snapshot does not exist', async () => {
    let callIndex = 0;
    mockSnapshotGet.mockImplementation(() => {
      callIndex++;
      if (callIndex === 2) {
        return Promise.resolve({ exists: false });
      }
      return Promise.resolve({
        exists: true,
        data: () => makeSnapshot(`acc${callIndex}`, [['2026-04-11', 30000, 0]]),
      });
    });

    // Mock legacy path: collection scan returns empty
    mockCollectionGet.mockResolvedValueOnce({
      empty: false,
      docs: [{ id: 'acc1' }, { id: 'acc2' }, { id: 'acc3' }, { id: 'acc4' }],
    });
    // Second call is from legacy for the actual data scan
    mockCollectionGet.mockResolvedValue({ size: 0, docs: [] });

    const result = await getMultiAccountHistoricalReturns(
      createContext('user1'),
      { accountIds: ['acc1', 'acc2', 'acc3'], currency: 'USD' }
    );

    // Legacy path returns empty result since we mocked empty data
    expect(result.returns.hasYtdData).toBe(false);
  });

  it('should skip snapshot and use legacy when ticker is provided', async () => {
    mockCollectionGet.mockResolvedValueOnce({
      empty: false,
      docs: [{ id: 'acc1' }, { id: 'acc2' }, { id: 'acc3' }, { id: 'acc4' }],
    });
    mockCollectionGet.mockResolvedValue({ size: 0, docs: [] });

    await getMultiAccountHistoricalReturns(
      createContext('user1'),
      { accountIds: ['acc1', 'acc2', 'acc3'], currency: 'USD', ticker: 'AAPL' }
    );

    // Snapshot path should NOT be attempted
    expect(mockBuildSnapshotDocId).not.toHaveBeenCalled();
  });

  it('should skip snapshot and use legacy when assetType is provided', async () => {
    mockCollectionGet.mockResolvedValueOnce({
      empty: false,
      docs: [{ id: 'acc1' }, { id: 'acc2' }, { id: 'acc3' }, { id: 'acc4' }],
    });
    mockCollectionGet.mockResolvedValue({ size: 0, docs: [] });

    await getMultiAccountHistoricalReturns(
      createContext('user1'),
      { accountIds: ['acc1', 'acc2', 'acc3'], currency: 'USD', assetType: 'stock' }
    );

    expect(mockBuildSnapshotDocId).not.toHaveBeenCalled();
  });

  it('should skip snapshot when forceRefresh is true', async () => {
    mockCollectionGet.mockResolvedValueOnce({
      empty: false,
      docs: [{ id: 'acc1' }, { id: 'acc2' }, { id: 'acc3' }, { id: 'acc4' }],
    });
    // Cache miss
    mockSnapshotGet.mockResolvedValue({ exists: false });
    // Legacy scan empty
    mockCollectionGet.mockResolvedValue({ size: 0, docs: [] });

    await getMultiAccountHistoricalReturns(
      createContext('user1'),
      { accountIds: ['acc1', 'acc2', 'acc3'], currency: 'USD', forceRefresh: true }
    );

    expect(mockBuildSnapshotDocId).not.toHaveBeenCalled();
  });

  it('should produce response format with totalValueData, returns, performanceByYear (AC3)', async () => {
    const snap1 = makeSnapshot('acc1', [['2026-04-11', 30000, 0.5]]);
    const snap2 = makeSnapshot('acc2', [['2026-04-11', 50000, 0.9]]);

    let callIndex = 0;
    mockSnapshotGet.mockImplementation(() => {
      const snaps = [snap1, snap2];
      return Promise.resolve({
        exists: true,
        data: () => snaps[callIndex++],
      });
    });

    const result = await getMultiAccountHistoricalReturns(
      createContext('user1'),
      { accountIds: ['acc1', 'acc2'], currency: 'USD' }
    );

    expect(result).toHaveProperty('returns');
    expect(result).toHaveProperty('totalValueData');
    expect(result).toHaveProperty('performanceByYear');
    expect(result).toHaveProperty('availableYears');
    expect(result).toHaveProperty('startDate');
    expect(result).toHaveProperty('validDocsCountByPeriod');
  });
});

// ============================================================================
// PERF-SNAP-009: Fallback logs + on-demand generation (multi-account)
// ============================================================================

describe('PERF-SNAP-009: getMultiAccountHistoricalReturns — fallback log + on-demand', () => {
  let consoleSpy;

  beforeEach(() => {
    jest.clearAllMocks();
    consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    mockBuildSnapshotDocId.mockImplementation((userId, accountId, currency) =>
      `${userId}_${accountId}_${currency}`
    );
    mockCalculateHistoricalReturns.mockReturnValue(MOCK_CALC_RESULT);
    mockGeneratePerformanceSnapshot.mockResolvedValue(true);

    // 4 total accounts (so 3 selected !== all)
    mockCollectionGet.mockResolvedValue({
      empty: false,
      docs: [
        { id: 'acc1' }, { id: 'acc2' }, { id: 'acc3' }, { id: 'acc4' },
      ],
    });
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should log [PERF] with missing snapshot IDs on partial miss (AC3)', async () => {
    let callIndex = 0;
    mockSnapshotGet.mockImplementation(() => {
      callIndex++;
      // acc2 missing
      if (callIndex === 2) return Promise.resolve({ exists: false });
      return Promise.resolve({
        exists: true,
        data: () => makeSnapshot(`acc${callIndex}`, [['2026-04-11', 30000, 0]]),
      });
    });

    // Legacy path: cache miss + empty scan
    mockCollectionGet
      .mockResolvedValueOnce({ empty: false, docs: [{ id: 'acc1' }, { id: 'acc2' }, { id: 'acc3' }, { id: 'acc4' }] })
      .mockResolvedValue({ size: 0, docs: [] });

    await getMultiAccountHistoricalReturns(
      createContext('user1'),
      { accountIds: ['acc1', 'acc2', 'acc3'], currency: 'USD' }
    );

    const perfLog = consoleSpy.mock.calls.find(
      call => typeof call[0] === 'string' && call[0].includes('[PERF] Snapshot not found')
    );
    expect(perfLog).toBeDefined();
    expect(perfLog[0]).toContain('user1_acc2_USD');
  });

  it('should invoke generatePerformanceSnapshot only for missing accounts (AC4)', async () => {
    let callIndex = 0;
    mockSnapshotGet.mockImplementation(() => {
      callIndex++;
      // acc2 missing
      if (callIndex === 2) return Promise.resolve({ exists: false });
      return Promise.resolve({
        exists: true,
        data: () => makeSnapshot(`acc${callIndex}`, [['2026-04-11', 30000, 0]]),
      });
    });

    mockCollectionGet
      .mockResolvedValueOnce({ empty: false, docs: [{ id: 'acc1' }, { id: 'acc2' }, { id: 'acc3' }, { id: 'acc4' }] })
      .mockResolvedValue({ size: 0, docs: [] });

    await getMultiAccountHistoricalReturns(
      createContext('user1'),
      { accountIds: ['acc1', 'acc2', 'acc3'], currency: 'USD' }
    );

    expect(mockGeneratePerformanceSnapshot).toHaveBeenCalledTimes(1);
    expect(mockGeneratePerformanceSnapshot).toHaveBeenCalledWith(
      expect.anything(), 'user1', 'acc2', 'USD'
    );
  });

  it('should not propagate error from on-demand generation failure (AC5)', async () => {
    mockSnapshotGet.mockResolvedValue({ exists: false });
    mockGeneratePerformanceSnapshot.mockRejectedValue(new Error('generation failed'));

    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();

    mockCollectionGet
      .mockResolvedValueOnce({ empty: false, docs: [{ id: 'acc1' }, { id: 'acc2' }, { id: 'acc3' }, { id: 'acc4' }] })
      .mockResolvedValue({ size: 0, docs: [] });

    const result = await getMultiAccountHistoricalReturns(
      createContext('user1'),
      { accountIds: ['acc1', 'acc2', 'acc3'], currency: 'USD' }
    );

    // Should return legacy result normally despite on-demand failures
    expect(result.returns).toBeDefined();

    // Wait for fire-and-forget promises to settle
    await new Promise(resolve => setTimeout(resolve, 10));

    const warnLogs = warnSpy.mock.calls.filter(
      call => typeof call[0] === 'string' && call[0].includes('[PERF] On-demand snapshot generation failed')
    );
    expect(warnLogs.length).toBeGreaterThan(0);
    warnSpy.mockRestore();
  });
});
