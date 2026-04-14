/**
 * Tests para snapshotGenerator
 *
 * @see docs/stories/PERF-SNAP-003.story.md
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

const mockV2Result = {
  returns: {
    ytdReturn: 12.5,
    oneMonthReturn: 2.3,
    threeMonthReturn: 5.6,
    sixMonthReturn: 8.1,
    oneYearReturn: 18.2,
    twoYearReturn: 35.4,
    fiveYearReturn: 82.1,
    ytdPersonalReturn: 11.8,
    oneMonthPersonalReturn: 2.1,
    threeMonthPersonalReturn: 5.0,
    sixMonthPersonalReturn: 7.5,
    oneYearPersonalReturn: 16.9,
    twoYearPersonalReturn: 33.0,
    fiveYearPersonalReturn: 78.0,
    hasYtdData: true,
    hasOneMonthData: true,
    hasThreeMonthData: true,
    hasSixMonthData: true,
    hasOneYearData: true,
    hasTwoYearData: true,
    hasFiveYearData: true,
  },
  validDocsCountByPeriod: {
    ytd: 72,
    oneMonth: 22,
    threeMonths: 63,
    sixMonths: 126,
    oneYear: 252,
    twoYears: 504,
    fiveYears: 1260,
  },
  totalValueData: {
    dates: ['2026-04-09', '2026-04-10', '2026-04-11'],
    values: [50000, 50250, 50500],
    percentChanges: [0, 0.5, 0.497],
    overallPercentChange: 1.0,
  },
  performanceByYear: {
    '2026': {
      months: { '1': 2.5, '2': -1.2, '3': 3.0 },
      personalMonths: { '1': 2.3, '2': -1.0, '3': 2.8 },
      total: 4.34,
      personalTotal: 4.1,
    },
  },
  availableYears: ['2026', '2025'],
  startDate: '2025-01-02',
  monthlyCompoundData: {},
  consolidatedVersion: true,
  _metadata: { version: 'v2', duration: 45, docsRead: 30 },
};

const mockLatestDailyDoc = {
  date: '2026-04-11',
  USD: {
    totalValue: 30500,
    totalInvestment: 25000,
    dailyChangePercentage: 0.497,
    assetPerformance: {
      'AAPL_stock': {
        totalValue: 18500,
        totalInvestment: 15000,
        units: 10,
        unrealizedProfitAndLoss: 3500,
        totalROI: 23.33,
        dailyChangePercentage: 1.2,
        adjustedDailyChangePercentage: 1.2,
        rawDailyChangePercentage: 1.2,
        dailyReturn: 0.06,
        monthlyReturn: 1.85,
        annualReturn: 24.56,
        totalCashFlow: 0,
        doneProfitAndLoss: 0,
      },
      'MSFT_stock': {
        totalValue: 12000,
        totalInvestment: 10000,
        units: 8,
        unrealizedProfitAndLoss: 2000,
        totalROI: 20.0,
        dailyChangePercentage: 0.8,
        adjustedDailyChangePercentage: 0.8,
        rawDailyChangePercentage: 0.8,
        dailyReturn: 0.04,
        monthlyReturn: 1.5,
        annualReturn: 20.0,
        totalCashFlow: 0,
        doneProfitAndLoss: 0,
      },
    },
  },
  COP: {
    totalValue: 122000000,
    totalInvestment: 100000000,
    assetPerformance: {
      'AAPL_stock': {
        totalValue: 74000000,
        totalInvestment: 60000000,
        units: 10,
        unrealizedProfitAndLoss: 14000000,
        totalROI: 23.33,
        dailyChangePercentage: 1.5,
      },
    },
  },
};

const mockGetHistoricalReturnsV2 = jest.fn();

jest.mock('../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: (...args) => mockGetHistoricalReturnsV2(...args),
}));

// ============================================================================
// Tests
// ============================================================================

const {
  generatePerformanceSnapshot,
  generateAllSnapshots,
  generateAssetSnapshot,
  generateAllAssetSnapshots,
  buildSnapshotDocId,
  transformToCompactTimeline,
  extractAssetPerformanceFields,
  fetchLatestAssetPerformance,
  fetchAllDailyDocs,
  buildDailyTimeline,
  extractLatestAssetPerformanceFromDocs,
} = require('../snapshotGenerator');

// Default mock daily docs for fetchAllDailyDocs (ascending order)
const mockDailyDocsList = [
  { data: () => ({
    date: '2026-04-09',
    USD: { totalValue: 50000, dailyChangePercentage: 0, assetPerformance: {} },
    COP: { totalValue: 120000000, dailyChangePercentage: 0, assetPerformance: {} },
  })},
  { data: () => ({
    date: '2026-04-10',
    USD: { totalValue: 50250, dailyChangePercentage: 0.5, assetPerformance: {} },
    COP: { totalValue: 121000000, dailyChangePercentage: 0.83, assetPerformance: {} },
  })},
  { data: () => mockLatestDailyDoc },
];

function createMockDb(dailyDocsArg) {
  // Build daily docs list from argument
  let mockDocs;
  if (dailyDocsArg === null) {
    mockDocs = [];
  } else if (Array.isArray(dailyDocsArg)) {
    mockDocs = dailyDocsArg;
  } else if (dailyDocsArg !== undefined) {
    // Backward compat: single doc object → wrap as single-element list
    mockDocs = [{ data: () => dailyDocsArg }];
  } else {
    mockDocs = mockDailyDocsList;
  }

  const mockSet = jest.fn().mockResolvedValue();
  const mockDoc = jest.fn(() => ({ set: mockSet }));

  // For fetchAllDailyDocs: collection().orderBy('date', 'asc').get()
  const mockGetAll = jest.fn().mockResolvedValue({
    empty: mockDocs.length === 0,
    docs: mockDocs,
  });

  // For fetchLatestAssetPerformance: collection().orderBy('date', 'desc').limit(1).get()
  const mockGetLatest = jest.fn().mockResolvedValue({
    empty: mockDocs.length === 0,
    docs: mockDocs.length > 0 ? [mockDocs[mockDocs.length - 1]] : [],
  });
  const mockLimit = jest.fn(() => ({ get: mockGetLatest }));

  const mockOrderBy = jest.fn((field, direction) => {
    if (direction === 'asc') {
      return { get: mockGetAll };
    }
    return { limit: mockLimit };
  });
  const mockCollection = jest.fn(() => ({ doc: mockDoc, orderBy: mockOrderBy }));

  return {
    collection: mockCollection,
    _mockSet: mockSet,
    _mockDoc: mockDoc,
    _mockCollection: mockCollection,
    _mockGetAll: mockGetAll,
    _mockGetLatest: mockGetLatest,
    _mockOrderBy: mockOrderBy,
    _mockLimit: mockLimit,
  };
}

describe('PERF-SNAP-003: snapshotGenerator', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  // ==========================================================================
  // buildSnapshotDocId
  // ==========================================================================

  describe('buildSnapshotDocId', () => {
    it('should return {userId}_{currency} for overall', () => {
      expect(buildSnapshotDocId('user1', 'overall', 'USD')).toBe('user1_USD');
    });

    it('should return {userId}_{accountId}_{currency} for specific account', () => {
      expect(buildSnapshotDocId('user1', 'acc123', 'USD')).toBe('user1_acc123_USD');
    });
  });

  // ==========================================================================
  // transformToCompactTimeline
  // ==========================================================================

  describe('transformToCompactTimeline', () => {
    it('should return array of objects with {d, v, c} per entry', () => {
      const timeline = transformToCompactTimeline(mockV2Result.totalValueData);
      expect(timeline).toHaveLength(3);
      expect(timeline[0]).toEqual({ d: '2026-04-09', v: 50000, c: 0 });
      expect(timeline[1]).toEqual({ d: '2026-04-10', v: 50250, c: 0.5 });
      expect(timeline[2]).toEqual({ d: '2026-04-11', v: 50500, c: 0.497 });
    });

    it('should return empty array for null/empty data', () => {
      expect(transformToCompactTimeline(null)).toEqual([]);
      expect(transformToCompactTimeline({})).toEqual([]);
      expect(transformToCompactTimeline({ dates: [] })).toEqual([]);
    });
  });

  // ==========================================================================
  // generatePerformanceSnapshot
  // ==========================================================================

  describe('generatePerformanceSnapshot', () => {
    it('should write snapshot with correct docId for overall', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      expect(db._mockCollection).toHaveBeenCalledWith('performanceSnapshots');
      expect(db._mockDoc).toHaveBeenCalledWith('user1_USD');
      expect(db._mockSet).toHaveBeenCalledTimes(1);
    });

    it('should write snapshot with correct docId for specific account', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'acc123', 'COP');

      expect(db._mockDoc).toHaveBeenCalledWith('user1_acc123_COP');
      expect(db._mockSet).toHaveBeenCalledTimes(1);
    });

    it('should include schemaVersion: 2', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.schemaVersion).toBe(2);
    });

    it('should include returns identical to V2 result', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.returns).toEqual(mockV2Result.returns);
    });

    it('should include timeline from daily docs as array of objects', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(Array.isArray(writtenDoc.timeline)).toBe(true);
      expect(writtenDoc.timeline.length).toBe(3);
      writtenDoc.timeline.forEach(entry => {
        expect(entry).toHaveProperty('d');
        expect(entry).toHaveProperty('v');
        expect(entry).toHaveProperty('c');
      });
      // Values come from daily docs, not V2 totalValueData
      expect(writtenDoc.timeline[0]).toEqual({ d: '2026-04-09', v: 50000, c: 0 });
      expect(writtenDoc.timeline[1]).toEqual({ d: '2026-04-10', v: 50250, c: 0.5 });
      expect(writtenDoc.timeline[2]).toEqual({ d: '2026-04-11', v: 30500, c: 0.497 });
    });

    it('should include performanceByYear from V2', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.performanceByYear).toEqual(mockV2Result.performanceByYear);
      expect(writtenDoc.availableYears).toEqual(mockV2Result.availableYears);
      expect(writtenDoc.startDate).toBe(mockV2Result.startDate);
      expect(writtenDoc.validDocsCountByPeriod).toEqual(mockV2Result.validDocsCountByPeriod);
    });

    it('should include metadata fields', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.userId).toBe('user1');
      expect(writtenDoc.currency).toBe('USD');
      expect(writtenDoc.accountId).toBe('overall');
      expect(writtenDoc.lastUpdated).toBeDefined();
    });

    it('should include latestAssetPerformance with correct fields (AC1)', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.latestAssetPerformance).toBeDefined();
      expect(writtenDoc.latestAssetPerformance['AAPL_stock']).toEqual({
        totalValue: 18500,
        totalInvestment: 15000,
        units: 10,
        unrealizedPnL: 3500,
        totalROI: 23.33,
        dailyChangePercentage: 1.2,
      });
      expect(writtenDoc.latestAssetPerformance['MSFT_stock']).toEqual({
        totalValue: 12000,
        totalInvestment: 10000,
        units: 8,
        unrealizedPnL: 2000,
        totalROI: 20.0,
        dailyChangePercentage: 0.8,
      });
    });

    it('should read daily docs from correct path for overall', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      expect(db._mockCollection).toHaveBeenCalledWith('portfolioPerformance/user1/dates');
      expect(db._mockOrderBy).toHaveBeenCalledWith('date', 'asc');
    });

    it('should read daily docs from correct path for account', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'acc123', 'COP');

      expect(db._mockCollection).toHaveBeenCalledWith('portfolioPerformance/user1/accounts/acc123/dates');
    });

    it('should use only snapshot currency for latestAssetPerformance (AC3)', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'COP');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.latestAssetPerformance['AAPL_stock'].totalValue).toBe(74000000);
      expect(writtenDoc.latestAssetPerformance['MSFT_stock']).toBeUndefined();
    });

    it('should return empty latestAssetPerformance for empty portfolio (AC4)', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb({ date: '2026-04-11', USD: { totalValue: 0, assetPerformance: {} } });

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.latestAssetPerformance).toEqual({});
    });

    it('should return empty latestAssetPerformance when no daily doc exists (AC4)', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb(null);

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.latestAssetPerformance).toEqual({});
    });

    it('should not write snapshot when V2 returns null', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(null);
      const db = createMockDb();

      const result = await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      expect(result).toBe(false);
      expect(db._mockSet).not.toHaveBeenCalled();
    });

    it('should not write snapshot when V2 returns empty result (no data flags)', async () => {
      const emptyResult = {
        returns: {
          ytdReturn: 0,
          hasYtdData: false,
          hasOneMonthData: false,
          hasThreeMonthData: false,
        },
        totalValueData: { dates: [], values: [], percentChanges: [], overallPercentChange: 0 },
        performanceByYear: {},
        availableYears: [],
        startDate: '',
        monthlyCompoundData: {},
      };
      mockGetHistoricalReturnsV2.mockResolvedValue(emptyResult);
      const db = createMockDb();

      const result = await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      expect(result).toBe(false);
      expect(db._mockSet).not.toHaveBeenCalled();
    });

    it('should call getHistoricalReturnsV2 with correct params', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'acc123', 'COP');

      expect(mockGetHistoricalReturnsV2).toHaveBeenCalledWith('user1', {
        currency: 'COP',
        accountId: 'acc123',
        fallbackToV1: true,
      });
    });
  });

  // ==========================================================================
  // generateAllSnapshots
  // ==========================================================================

  describe('generateAllSnapshots', () => {
    it('should generate snapshots for all account × currency combinations', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      const result = await generateAllSnapshots(db, 'user1', ['USD', 'COP'], ['acc1']);

      // overall × 2 currencies + acc1 × 2 currencies = 4
      expect(mockGetHistoricalReturnsV2).toHaveBeenCalledTimes(4);
      expect(result.total).toBe(4);
      expect(result.success).toBe(4);
      expect(result.failed).toBe(0);
    });

    it('should continue if one snapshot fails (error resilience)', async () => {
      let callCount = 0;
      mockGetHistoricalReturnsV2.mockImplementation(() => {
        callCount++;
        if (callCount === 2) throw new Error('Simulated failure');
        return Promise.resolve(mockV2Result);
      });
      const db = createMockDb();

      const result = await generateAllSnapshots(db, 'user1', ['USD', 'COP'], []);

      // overall × 2 currencies = 2 calls
      expect(mockGetHistoricalReturnsV2).toHaveBeenCalledTimes(2);
      expect(result.success).toBe(1);
      expect(result.failed).toBe(1);
      expect(result.total).toBe(2);
    });

    it('should include overall as first accountId', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generateAllSnapshots(db, 'user1', ['USD'], ['acc1', 'acc2']);

      expect(mockGetHistoricalReturnsV2).toHaveBeenCalledTimes(3);
      expect(mockGetHistoricalReturnsV2.mock.calls[0][1].accountId).toBe('overall');
      expect(mockGetHistoricalReturnsV2.mock.calls[1][1].accountId).toBe('acc1');
      expect(mockGetHistoricalReturnsV2.mock.calls[2][1].accountId).toBe('acc2');
    });
  });

  // ==========================================================================
  // extractAssetPerformanceFields (PERF-SNAP-006)
  // ==========================================================================

  describe('extractAssetPerformanceFields', () => {
    it('should extract only the 6 required fields per asset (AC1)', () => {
      const assetPerf = mockLatestDailyDoc.USD.assetPerformance;
      const result = extractAssetPerformanceFields(assetPerf);

      expect(Object.keys(result['AAPL_stock'])).toEqual([
        'totalValue', 'totalInvestment', 'units', 'unrealizedPnL', 'totalROI', 'dailyChangePercentage',
      ]);
      expect(result['AAPL_stock']).toEqual({
        totalValue: 18500,
        totalInvestment: 15000,
        units: 10,
        unrealizedPnL: 3500,
        totalROI: 23.33,
        dailyChangePercentage: 1.2,
      });
    });

    it('should map unrealizedProfitAndLoss to unrealizedPnL', () => {
      const result = extractAssetPerformanceFields({
        'BTC-USD_crypto': { unrealizedProfitAndLoss: 5000, totalValue: 0, totalInvestment: 0, units: 0, totalROI: 0, dailyChangePercentage: 0 },
      });
      expect(result['BTC-USD_crypto'].unrealizedPnL).toBe(5000);
      expect(result['BTC-USD_crypto'].unrealizedProfitAndLoss).toBeUndefined();
    });

    it('should default missing fields to 0', () => {
      const result = extractAssetPerformanceFields({ 'EMPTY_stock': {} });
      expect(result['EMPTY_stock']).toEqual({
        totalValue: 0,
        totalInvestment: 0,
        units: 0,
        unrealizedPnL: 0,
        totalROI: 0,
        dailyChangePercentage: 0,
      });
    });

    it('should return empty object for empty assetPerformance', () => {
      expect(extractAssetPerformanceFields({})).toEqual({});
    });
  });

  // ==========================================================================
  // fetchLatestAssetPerformance (PERF-SNAP-006)
  // ==========================================================================

  describe('fetchLatestAssetPerformance', () => {
    it('should query correct Firestore path for overall (AC2)', async () => {
      const db = createMockDb();

      await fetchLatestAssetPerformance(db, 'user1', 'overall', 'USD');

      expect(db._mockCollection).toHaveBeenCalledWith('portfolioPerformance/user1/dates');
      expect(db._mockOrderBy).toHaveBeenCalledWith('date', 'desc');
      expect(db._mockLimit).toHaveBeenCalledWith(1);
    });

    it('should query correct Firestore path for account (AC2)', async () => {
      const db = createMockDb();

      await fetchLatestAssetPerformance(db, 'user1', 'acc123', 'USD');

      expect(db._mockCollection).toHaveBeenCalledWith('portfolioPerformance/user1/accounts/acc123/dates');
    });

    it('should extract only the snapshot currency data (AC3)', async () => {
      const db = createMockDb();

      const result = await fetchLatestAssetPerformance(db, 'user1', 'overall', 'USD');

      expect(result['AAPL_stock'].totalValue).toBe(18500);
      expect(result['MSFT_stock'].totalValue).toBe(12000);
    });

    it('should not include assets from other currencies (AC3)', async () => {
      const db = createMockDb();

      const resultCOP = await fetchLatestAssetPerformance(db, 'user1', 'overall', 'COP');

      expect(resultCOP['AAPL_stock'].totalValue).toBe(74000000);
      expect(resultCOP['MSFT_stock']).toBeUndefined();
    });

    it('should return empty object when no daily doc exists (AC4)', async () => {
      const db = createMockDb(null);

      const result = await fetchLatestAssetPerformance(db, 'user1', 'overall', 'USD');

      expect(result).toEqual({});
    });

    it('should return empty object when currency has no assetPerformance (AC4)', async () => {
      const db = createMockDb({ date: '2026-04-11', USD: { totalValue: 0 } });

      const result = await fetchLatestAssetPerformance(db, 'user1', 'overall', 'USD');

      expect(result).toEqual({});
    });

    it('should return empty object when currency key is missing (AC4)', async () => {
      const db = createMockDb({ date: '2026-04-11', COP: { assetPerformance: {} } });

      const result = await fetchLatestAssetPerformance(db, 'user1', 'overall', 'EUR');

      expect(result).toEqual({});
    });
  });

  // ==========================================================================
  // buildDailyTimeline
  // ==========================================================================

  describe('buildDailyTimeline', () => {
    it('should extract {d, v, c} from daily docs for given currency', () => {
      const result = buildDailyTimeline(mockDailyDocsList, 'USD');
      expect(result).toHaveLength(3);
      expect(result[0]).toEqual({ d: '2026-04-09', v: 50000, c: 0 });
      expect(result[1]).toEqual({ d: '2026-04-10', v: 50250, c: 0.5 });
      expect(result[2]).toEqual({ d: '2026-04-11', v: 30500, c: 0.497 });
    });

    it('should filter currency-specific data (COP)', () => {
      const result = buildDailyTimeline(mockDailyDocsList, 'COP');
      expect(result).toHaveLength(3);
      expect(result[0].v).toBe(120000000);
      expect(result[2].v).toBe(122000000);
    });

    it('should return empty array for empty docs', () => {
      expect(buildDailyTimeline([], 'USD')).toEqual([]);
    });

    it('should skip docs missing the requested currency', () => {
      const docs = [
        { data: () => ({ date: '2026-01-01', USD: { totalValue: 100, dailyChangePercentage: 0 } }) },
        { data: () => ({ date: '2026-01-02', COP: { totalValue: 500000 } }) },
      ];
      const result = buildDailyTimeline(docs, 'USD');
      expect(result).toHaveLength(1);
      expect(result[0].d).toBe('2026-01-01');
    });

    it('should default missing dailyChangePercentage to 0', () => {
      const docs = [{ data: () => ({ date: '2026-01-01', USD: { totalValue: 100 } }) }];
      const result = buildDailyTimeline(docs, 'USD');
      expect(result[0].c).toBe(0);
    });

    it('should prefer adjustedDailyChangePercentage over dailyChangePercentage (TWR)', () => {
      const docs = [{
        data: () => ({
          date: '2026-01-01',
          USD: { totalValue: 100, dailyChangePercentage: 46.9, adjustedDailyChangePercentage: 17.89 },
        }),
      }];
      const result = buildDailyTimeline(docs, 'USD');
      expect(result[0].c).toBe(17.89);
    });

    it('should fall back to dailyChangePercentage when adjusted is missing', () => {
      const docs = [{
        data: () => ({
          date: '2026-01-01',
          USD: { totalValue: 100, dailyChangePercentage: 1.5 },
        }),
      }];
      const result = buildDailyTimeline(docs, 'USD');
      expect(result[0].c).toBe(1.5);
    });
  });

  // ==========================================================================
  // extractLatestAssetPerformanceFromDocs
  // ==========================================================================

  describe('extractLatestAssetPerformanceFromDocs', () => {
    it('should extract asset performance from last doc', () => {
      const result = extractLatestAssetPerformanceFromDocs(mockDailyDocsList, 'USD');
      expect(result['AAPL_stock']).toEqual({
        totalValue: 18500,
        totalInvestment: 15000,
        units: 10,
        unrealizedPnL: 3500,
        totalROI: 23.33,
        dailyChangePercentage: 1.2,
      });
    });

    it('should return empty object for empty docs array', () => {
      expect(extractLatestAssetPerformanceFromDocs([], 'USD')).toEqual({});
    });

    it('should return empty object for null docs', () => {
      expect(extractLatestAssetPerformanceFromDocs(null, 'USD')).toEqual({});
    });

    it('should return empty object when currency missing in last doc', () => {
      const docs = [{ data: () => ({ date: '2026-01-01', COP: { assetPerformance: {} } }) }];
      expect(extractLatestAssetPerformanceFromDocs(docs, 'EUR')).toEqual({});
    });
  });

  // ==========================================================================
  // fetchAllDailyDocs
  // ==========================================================================

  describe('fetchAllDailyDocs', () => {
    it('should query correct path for overall', async () => {
      const db = createMockDb();
      await fetchAllDailyDocs(db, 'user1', 'overall');
      expect(db._mockCollection).toHaveBeenCalledWith('portfolioPerformance/user1/dates');
      expect(db._mockOrderBy).toHaveBeenCalledWith('date', 'asc');
    });

    it('should query correct path for account', async () => {
      const db = createMockDb();
      await fetchAllDailyDocs(db, 'user1', 'acc123');
      expect(db._mockCollection).toHaveBeenCalledWith('portfolioPerformance/user1/accounts/acc123/dates');
    });

    it('should return docs array', async () => {
      const db = createMockDb();
      const result = await fetchAllDailyDocs(db, 'user1', 'overall');
      expect(result).toHaveLength(3);
    });

    it('should return empty array when no docs', async () => {
      const db = createMockDb(null);
      const result = await fetchAllDailyDocs(db, 'user1', 'overall');
      expect(result).toHaveLength(0);
    });
  });

  // ==========================================================================
  // PERF-SNAP-024: buildSnapshotDocId — per-asset extension
  // ==========================================================================

  describe('buildSnapshotDocId — per-asset (PERF-SNAP-024)', () => {
    it('should build per-asset overall docId: userId_AAPL_stock_USD', () => {
      expect(buildSnapshotDocId('user1', 'overall', 'USD', 'AAPL', 'stock'))
        .toBe('user1_AAPL_stock_USD');
    });

    it('should build per-asset account docId: userId_AAPL_stock_acc1_USD', () => {
      expect(buildSnapshotDocId('user1', 'acc1', 'USD', 'AAPL', 'stock'))
        .toBe('user1_AAPL_stock_acc1_USD');
    });

    it('should remain backward-compatible without ticker/assetType', () => {
      expect(buildSnapshotDocId('user1', 'overall', 'USD')).toBe('user1_USD');
      expect(buildSnapshotDocId('user1', 'acc1', 'USD')).toBe('user1_acc1_USD');
      expect(buildSnapshotDocId('user1', 'overall', 'COP', null, null)).toBe('user1_COP');
      expect(buildSnapshotDocId('user1', 'overall', 'USD', undefined, undefined)).toBe('user1_USD');
    });

    it('should handle ticker with special characters (e.g. BTC-USD)', () => {
      expect(buildSnapshotDocId('user1', 'overall', 'USD', 'BTC-USD', 'crypto'))
        .toBe('user1_BTC-USD_crypto_USD');
    });
  });

  // ==========================================================================
  // PERF-SNAP-024: generateAssetSnapshot
  // ==========================================================================

  describe('generateAssetSnapshot (PERF-SNAP-024)', () => {
    it('should write asset snapshot with schemaVersion 2 and type asset (AC2)', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.schemaVersion).toBe(2);
      expect(writtenDoc.type).toBe('asset');
      expect(writtenDoc.ticker).toBe('AAPL');
      expect(writtenDoc.assetType).toBe('stock');
    });

    it('should call getHistoricalReturnsV2 with ticker and assetType', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      expect(mockGetHistoricalReturnsV2).toHaveBeenCalledWith('user1', {
        currency: 'USD',
        accountId: 'overall',
        ticker: 'AAPL',
        assetType: 'stock',
        fallbackToV1: true,
      });
    });

    it('should write to correct docId for per-asset overall', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      expect(db._mockCollection).toHaveBeenCalledWith('performanceSnapshots');
      expect(db._mockDoc).toHaveBeenCalledWith('user1_AAPL_stock_USD');
    });

    it('should skip if V2 returns null', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(null);
      const db = createMockDb();

      const result = await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      expect(result).toBe(false);
      expect(db._mockSet).not.toHaveBeenCalled();
    });

    it('should skip if no hasYtdData/hasOneMonthData/hasThreeMonthData', async () => {
      const emptyResult = {
        returns: { hasYtdData: false, hasOneMonthData: false, hasThreeMonthData: false },
        totalValueData: { dates: [], values: [], percentChanges: [] },
        performanceByYear: {},
        availableYears: [],
        startDate: '',
        monthlyCompoundData: {},
      };
      mockGetHistoricalReturnsV2.mockResolvedValue(emptyResult);
      const db = createMockDb();

      const result = await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      expect(result).toBe(false);
      expect(db._mockSet).not.toHaveBeenCalled();
    });

    it('should produce identical returns structure to portfolio snapshot (AC6)', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.returns).toEqual(mockV2Result.returns);
      expect(writtenDoc.timeline).toEqual(transformToCompactTimeline(mockV2Result.totalValueData));
      expect(writtenDoc.performanceByYear).toEqual(mockV2Result.performanceByYear);
      expect(writtenDoc.availableYears).toEqual(mockV2Result.availableYears);
      expect(writtenDoc.startDate).toBe(mockV2Result.startDate);
    });

    it('should NOT include latestAssetPerformance (asset snapshots are single-asset)', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.latestAssetPerformance).toBeUndefined();
    });

    it('should return true on successful write', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      const result = await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      expect(result).toBe(true);
    });
  });

  // ==========================================================================
  // PERF-SNAP-024: generateAllAssetSnapshots
  // ==========================================================================

  describe('generateAllAssetSnapshots (PERF-SNAP-024)', () => {
    it('should generate snapshots for all assets in latestAssetPerformance', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();
      const latestAssetPerformance = {
        'AAPL_stock': { totalValue: 18500 },
        'MSFT_stock': { totalValue: 12000 },
      };

      const result = await generateAllAssetSnapshots(db, 'user1', 'USD', latestAssetPerformance);

      expect(mockGetHistoricalReturnsV2).toHaveBeenCalledTimes(2);
      expect(result.total).toBe(2);
      expect(result.success).toBe(2);
      expect(result.failed).toBe(0);
    });

    it('should continue on individual asset failure (resiliencia)', async () => {
      let callCount = 0;
      mockGetHistoricalReturnsV2.mockImplementation(() => {
        callCount++;
        if (callCount === 1) throw new Error('Simulated asset failure');
        return Promise.resolve(mockV2Result);
      });
      const db = createMockDb();
      const latestAssetPerformance = {
        'AAPL_stock': { totalValue: 18500 },
        'MSFT_stock': { totalValue: 12000 },
      };

      const result = await generateAllAssetSnapshots(db, 'user1', 'USD', latestAssetPerformance);

      expect(result.success).toBe(1);
      expect(result.failed).toBe(1);
      expect(result.total).toBe(2);
    });

    it('should return correct counts for empty latestAssetPerformance', async () => {
      const db = createMockDb();

      const result = await generateAllAssetSnapshots(db, 'user1', 'USD', {});

      expect(result).toEqual({ success: 0, failed: 0, total: 0 });
    });

    it('should return correct counts for null latestAssetPerformance', async () => {
      const db = createMockDb();

      const result = await generateAllAssetSnapshots(db, 'user1', 'USD', null);

      expect(result).toEqual({ success: 0, failed: 0, total: 0 });
    });

    it('should always use overall as accountId', async () => {
      mockGetHistoricalReturnsV2.mockResolvedValue(mockV2Result);
      const db = createMockDb();

      await generateAllAssetSnapshots(db, 'user1', 'USD', { 'AAPL_stock': {} });

      expect(mockGetHistoricalReturnsV2).toHaveBeenCalledWith('user1', expect.objectContaining({
        accountId: 'overall',
        ticker: 'AAPL',
        assetType: 'stock',
      }));
    });

    it('should skip malformed asset keys without underscore', async () => {
      const db = createMockDb();

      const result = await generateAllAssetSnapshots(db, 'user1', 'USD', { 'malformedkey': {} });

      expect(mockGetHistoricalReturnsV2).not.toHaveBeenCalled();
      expect(result.total).toBe(1);
    });
  });
});
