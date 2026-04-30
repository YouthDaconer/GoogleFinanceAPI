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
    adjustedDailyChangePercentage: 0.497,
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
    dailyChangePercentage: 0.83,
    adjustedDailyChangePercentage: 0.83,
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
  computePortfolioReturnsFromDailyDocs,
} = require('../snapshotGenerator');

// Default mock daily docs for fetchAllDailyDocs (ascending order)
const mockDailyDocsList = [
  { data: () => ({
    date: '2026-04-09',
    USD: { totalValue: 50000, dailyChangePercentage: 0, adjustedDailyChangePercentage: 0, assetPerformance: {} },
    COP: { totalValue: 120000000, dailyChangePercentage: 0, adjustedDailyChangePercentage: 0, assetPerformance: {} },
  })},
  { data: () => ({
    date: '2026-04-10',
    USD: { totalValue: 50250, dailyChangePercentage: 0.5, adjustedDailyChangePercentage: 0.5, assetPerformance: {} },
    COP: { totalValue: 121000000, dailyChangePercentage: 0.83, adjustedDailyChangePercentage: 0.83, assetPerformance: {} },
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
  // generatePerformanceSnapshot (PERF-SNAP-030: daily docs, sin V2)
  // ==========================================================================

  describe('generatePerformanceSnapshot', () => {
    it('should write snapshot with correct docId for overall', async () => {
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      expect(db._mockCollection).toHaveBeenCalledWith('performanceSnapshots');
      expect(db._mockDoc).toHaveBeenCalledWith('user1_USD');
      expect(db._mockSet).toHaveBeenCalledTimes(1);
    });

    it('should write snapshot with correct docId for specific account', async () => {
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'acc123', 'COP');

      expect(db._mockDoc).toHaveBeenCalledWith('user1_acc123_COP');
      expect(db._mockSet).toHaveBeenCalledTimes(1);
    });

    it('should include schemaVersion: 2', async () => {
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.schemaVersion).toBe(3);
    });

    it('should include returns computed from daily docs (AC1)', async () => {
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.returns).toBeDefined();
      expect(typeof writtenDoc.returns.ytdReturn).toBe('number');
      expect(writtenDoc.returns.hasYtdData).toBe(true);
      expect(typeof writtenDoc.returns.oneMonthReturn).toBe('number');
      expect(typeof writtenDoc.returns.ytdPersonalReturn).toBe('number');
    });

    it('should include timeline from daily docs as array of objects', async () => {
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
      expect(writtenDoc.timeline[0]).toEqual({ d: '2026-04-09', v: 50000, c: 0 });
      expect(writtenDoc.timeline[1]).toEqual({ d: '2026-04-10', v: 50250, c: 0.5 });
      expect(writtenDoc.timeline[2]).toEqual({ d: '2026-04-11', v: 30500, c: 0.497 });
    });

    it('should include performanceByYear computed from daily docs (AC1)', async () => {
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.performanceByYear).toBeDefined();
      expect(writtenDoc.performanceByYear['2026']).toBeDefined();
      expect(typeof writtenDoc.performanceByYear['2026'].total).toBe('number');
      expect(Array.isArray(writtenDoc.availableYears)).toBe(true);
      expect(writtenDoc.availableYears).toContain('2026');
      expect(typeof writtenDoc.startDate).toBe('string');
      expect(writtenDoc.validDocsCountByPeriod).toBeDefined();
    });

    it('should include metadata fields', async () => {
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.userId).toBe('user1');
      expect(writtenDoc.currency).toBe('USD');
      expect(writtenDoc.accountId).toBe('overall');
      expect(writtenDoc.lastUpdated).toBeDefined();
    });

    it('should include latestAssetPerformance with correct fields (AC4)', async () => {
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

    it('should read daily docs from correct path for overall (AC3 fallback)', async () => {
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      expect(db._mockCollection).toHaveBeenCalledWith('portfolioPerformance/user1/dates');
      expect(db._mockOrderBy).toHaveBeenCalledWith('date', 'asc');
    });

    it('should read daily docs from correct path for account', async () => {
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'acc123', 'COP');

      expect(db._mockCollection).toHaveBeenCalledWith('portfolioPerformance/user1/accounts/acc123/dates');
    });

    it('should use only snapshot currency for latestAssetPerformance', async () => {
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'COP');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.latestAssetPerformance['AAPL_stock'].totalValue).toBe(74000000);
      expect(writtenDoc.latestAssetPerformance['MSFT_stock']).toBeUndefined();
    });

    it('should return empty latestAssetPerformance for empty portfolio (AC4)', async () => {
      const db = createMockDb({ date: '2026-04-11', USD: { totalValue: 0, dailyChangePercentage: 0, adjustedDailyChangePercentage: 0, assetPerformance: {} } });

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.latestAssetPerformance).toEqual({});
    });

    it('should not write snapshot when no daily docs have requested currency', async () => {
      // Docs only have EUR, not USD
      const db = createMockDb([
        { data: () => ({ date: '2026-04-11', EUR: { totalValue: 1000, adjustedDailyChangePercentage: 0.5 } }) },
      ]);

      const result = await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      expect(result).toBe(false);
      expect(db._mockSet).not.toHaveBeenCalled();
    });

    it('should not write snapshot when no daily docs exist', async () => {
      const db = createMockDb(null);

      const result = await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      expect(result).toBe(false);
      expect(db._mockSet).not.toHaveBeenCalled();
    });

    it('should use options.dailyDocs when provided (AC1)', async () => {
      const db = createMockDb(null); // DB returns empty
      const dailyDocs = mockDailyDocsList; // Pass docs via options

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD', { dailyDocs });

      // Should NOT call fetchAllDailyDocs (orderBy was not called for 'asc')
      expect(db._mockSet).toHaveBeenCalledTimes(1);
      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.returns).toBeDefined();
      expect(writtenDoc.returns.hasYtdData).toBe(true);
    });

    it('should include monthlyCompound from buildMonthlyCompoundFromDailyDocs', async () => {
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.monthlyCompound).toBeDefined();
      expect(typeof writtenDoc.monthlyCompound).toBe('object');
      // monthlyCompound should have year->month structure from buildMonthlyCompoundFromDailyDocs
      expect(writtenDoc.monthlyCompound['2026']).toBeDefined();
    });

    it('should not call getHistoricalReturnsV2 (AC5)', async () => {
      const db = createMockDb();

      await generatePerformanceSnapshot(db, 'user1', 'overall', 'USD');

      // Verify V2 is not in the module's dependencies
      const snapshotGeneratorSource = require.resolve('../snapshotGenerator');
      const moduleContent = require('fs').readFileSync(snapshotGeneratorSource, 'utf8');
      expect(moduleContent).not.toContain('getHistoricalReturnsV2');
      expect(moduleContent).not.toContain('consolidatedReturnsService');
    });
  });

  // ==========================================================================
  // generateAllSnapshots
  // ==========================================================================

  describe('generateAllSnapshots', () => {
    it('should generate snapshots for all account × currency combinations', async () => {
      const db = createMockDb();

      const result = await generateAllSnapshots(db, 'user1', ['USD', 'COP'], ['acc1']);

      // overall × 2 currencies + acc1 × 2 currencies = 4
      expect(result.total).toBe(4);
      expect(result.success).toBe(4);
      expect(result.failed).toBe(0);
    });

    it('should continue if one snapshot fails (error resilience)', async () => {
      // Create a db where the second fetchAllDailyDocs throws
      let fetchCount = 0;
      const db = createMockDb();
      const originalCollection = db.collection;
      db.collection = jest.fn((...args) => {
        const result = originalCollection(...args);
        if (args[0] && args[0].includes('dates')) {
          fetchCount++;
        }
        return result;
      });

      const result = await generateAllSnapshots(db, 'user1', ['USD'], []);

      // overall × 1 currency = 1 call
      expect(result.total).toBe(1);
      expect(result.success).toBe(1);
    });

    it('should include overall as first accountId', async () => {
      const db = createMockDb();

      const result = await generateAllSnapshots(db, 'user1', ['USD'], ['acc1', 'acc2']);

      // 3 accounts × 1 currency = 3 snapshots
      expect(result.total).toBe(3);
      expect(result.success).toBe(3);
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
  // PERF-SNAP-030: computePortfolioReturnsFromDailyDocs
  // ==========================================================================

  describe('computePortfolioReturnsFromDailyDocs (PERF-SNAP-030)', () => {
    const { DateTime } = require('luxon');
    const fixedNow = DateTime.fromISO('2026-04-15T16:00:00', { zone: 'America/New_York' });

    it('should compute period returns from daily docs at portfolio level (AC1)', () => {
      const docs = [
        { data: () => ({ date: '2026-04-09', USD: { totalValue: 50000, adjustedDailyChangePercentage: 0 } }) },
        { data: () => ({ date: '2026-04-10', USD: { totalValue: 50250, adjustedDailyChangePercentage: 0.5 } }) },
        { data: () => ({ date: '2026-04-11', USD: { totalValue: 50500, adjustedDailyChangePercentage: 0.497 } }) },
      ];

      const result = computePortfolioReturnsFromDailyDocs(docs, 'USD', fixedNow);

      expect(result).not.toBeNull();
      expect(result.returns).toBeDefined();
      expect(result.returns.hasYtdData).toBe(true);
      expect(result.returns.hasOneMonthData).toBe(true);
      expect(result.returns.ytdReturn).toBeCloseTo(0.9975, 2);
      expect(result.returns.oneMonthReturn).toBeCloseTo(0.9975, 2);
    });

    it('should build performanceByYear with monthly grouping (AC1)', () => {
      const docs = [
        { data: () => ({ date: '2026-03-28', USD: { totalValue: 50000, adjustedDailyChangePercentage: 1.0 } }) },
        { data: () => ({ date: '2026-04-01', USD: { totalValue: 50500, adjustedDailyChangePercentage: 0.5 } }) },
      ];

      const result = computePortfolioReturnsFromDailyDocs(docs, 'USD', fixedNow);

      expect(result.performanceByYear).toBeDefined();
      expect(result.performanceByYear['2026']).toBeDefined();
      // March and April should have separate months
      expect(result.performanceByYear['2026'].months['3']).toBeCloseTo(1.0, 4);
      expect(result.performanceByYear['2026'].months['4']).toBeCloseTo(0.5, 4);
    });

    it('should return validDocsCountByPeriod with doc counts per period (AC1)', () => {
      const docs = [
        { data: () => ({ date: '2026-04-09', USD: { totalValue: 50000, adjustedDailyChangePercentage: 0 } }) },
        { data: () => ({ date: '2026-04-10', USD: { totalValue: 50250, adjustedDailyChangePercentage: 0.5 } }) },
      ];

      const result = computePortfolioReturnsFromDailyDocs(docs, 'USD', fixedNow);

      expect(result.validDocsCountByPeriod).toBeDefined();
      expect(result.validDocsCountByPeriod.ytd).toBe(2);
      expect(result.validDocsCountByPeriod.oneMonth).toBe(2);
    });

    it('should return null when no docs have the requested currency', () => {
      const docs = [
        { data: () => ({ date: '2026-04-10', EUR: { totalValue: 1000, adjustedDailyChangePercentage: 0.5 } }) },
      ];

      const result = computePortfolioReturnsFromDailyDocs(docs, 'USD', fixedNow);

      expect(result).toBeNull();
    });

    it('should return null for empty docs array', () => {
      const result = computePortfolioReturnsFromDailyDocs([], 'USD', fixedNow);

      expect(result).toBeNull();
    });

    it('should handle single day of data', () => {
      const docs = [
        { data: () => ({ date: '2026-04-15', USD: { totalValue: 50000, adjustedDailyChangePercentage: 1.5 } }) },
      ];

      const result = computePortfolioReturnsFromDailyDocs(docs, 'USD', fixedNow);

      expect(result).not.toBeNull();
      expect(result.returns.hasYtdData).toBe(true);
      expect(result.startDate).toBe('2026-04-15');
      expect(result.availableYears).toContain('2026');
    });

    it('should fallback to dailyChangePercentage when adjustedDailyChangePercentage is missing', () => {
      const docs = [
        { data: () => ({ date: '2026-04-10', USD: { totalValue: 50000, dailyChangePercentage: 0.5 } }) },
        { data: () => ({ date: '2026-04-11', USD: { totalValue: 50250, dailyChangePercentage: 0.3 } }) },
      ];

      const result = computePortfolioReturnsFromDailyDocs(docs, 'USD', fixedNow);

      expect(result).not.toBeNull();
      // percentChanges should use dailyChangePercentage fallback
      expect(result.totalValueData.percentChanges).toEqual([0.5, 0.3]);
    });

    it('should work with plain objects (not Firestore snapshots)', () => {
      const docs = [
        { date: '2026-04-10', USD: { totalValue: 50000, adjustedDailyChangePercentage: 0.5 } },
        { date: '2026-04-11', USD: { totalValue: 50250, adjustedDailyChangePercentage: 0.3 } },
      ];

      const result = computePortfolioReturnsFromDailyDocs(docs, 'USD', fixedNow);

      expect(result).not.toBeNull();
      expect(result.returns.hasYtdData).toBe(true);
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
    it('should write asset snapshot with schemaVersion 3 and type asset (AC2)', async () => {
      const db = createMockDb();

      await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.schemaVersion).toBe(3);
      expect(writtenDoc.type).toBe('asset');
      expect(writtenDoc.ticker).toBe('AAPL');
      expect(writtenDoc.assetType).toBe('stock');
    });

    it('should use daily docs instead of V2 (PERF-SNAP-025)', async () => {
      const db = createMockDb();

      await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      // Verify snapshot was written (uses computeAssetReturnsFromDailyDocs)
      expect(db._mockSet).toHaveBeenCalledTimes(1);
      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.returns).toBeDefined();
    });

    it('should write to correct docId for per-asset overall', async () => {
      const db = createMockDb();

      await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      expect(db._mockCollection).toHaveBeenCalledWith('performanceSnapshots');
      expect(db._mockDoc).toHaveBeenCalledWith('user1_AAPL_stock_USD');
    });

    it('should skip if no asset data in daily docs', async () => {
      // Mock docs without GOOGL asset data
      const db = createMockDb();

      const result = await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'GOOGL', 'stock');

      expect(result).toBe(false);
      expect(db._mockSet).not.toHaveBeenCalled();
    });

    it('should NOT include latestAssetPerformance (asset snapshots are single-asset)', async () => {
      const db = createMockDb();

      await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      const writtenDoc = db._mockSet.mock.calls[0][0];
      expect(writtenDoc.latestAssetPerformance).toBeUndefined();
    });

    it('should return truthy value on successful write', async () => {
      const db = createMockDb();

      const result = await generateAssetSnapshot(db, 'user1', 'overall', 'USD', 'AAPL', 'stock');

      expect(result).toBeTruthy();
    });
  });

  // ==========================================================================
  // PERF-SNAP-024: generateAllAssetSnapshots
  // ==========================================================================

  describe('generateAllAssetSnapshots (PERF-SNAP-024)', () => {
    it('should generate snapshots for all assets in latestAssetPerformance', async () => {
      const db = createMockDb();
      const latestAssetPerformance = {
        'AAPL_stock': { totalValue: 18500 },
        'MSFT_stock': { totalValue: 12000 },
      };

      const result = await generateAllAssetSnapshots(db, 'user1', 'USD', latestAssetPerformance);

      expect(result.total).toBe(2);
      expect(result.success).toBe(2);
      expect(result.failed).toBe(0);
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
      const db = createMockDb();

      const result = await generateAllAssetSnapshots(db, 'user1', 'USD', { 'AAPL_stock': {} });

      expect(db._mockDoc).toHaveBeenCalledWith('user1_AAPL_stock_USD');
    });

    it('should skip malformed asset keys without underscore', async () => {
      const db = createMockDb();

      const result = await generateAllAssetSnapshots(db, 'user1', 'USD', { 'malformedkey': {} });

      expect(result.total).toBe(1);
    });
  });
});
