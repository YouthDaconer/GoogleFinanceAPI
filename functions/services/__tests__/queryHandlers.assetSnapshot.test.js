/**
 * PERF-SNAP-024: Tests para ruta de snapshot per-asset en queryHandlers
 *
 * Valida que getHistoricalReturns intenta leer snapshot per-asset
 * antes de caer a la ruta legacy cuando se pasa ticker + assetType.
 *
 * @see docs/stories/PERF-SNAP-024.story.md
 */

// ============================================================================
// Mocks
// ============================================================================

jest.mock('firebase-admin', () => ({
  firestore: Object.assign(jest.fn(() => ({})), {
    FieldValue: { serverTimestamp: jest.fn(() => 'mock-server-timestamp') },
  }),
  initializeApp: jest.fn(),
}));

jest.mock('firebase-functions/v2/https', () => ({
  HttpsError: class HttpsError extends Error {
    constructor(code, message) { super(message); this.code = code; }
  },
}));

const mockCalculateDynamicTTL = jest.fn(() => new Date('2026-04-13T23:59:59Z'));
jest.mock('../cacheInvalidationService', () => ({
  calculateDynamicTTL: (...args) => mockCalculateDynamicTTL(...args),
}));

jest.mock('../historicalReturnsService', () => ({
  calculateHistoricalReturns: jest.fn(),
  getHistoricalReturnsInternal: jest.fn(),
}));

jest.mock('../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: jest.fn(),
  checkConsolidatedDataStatus: jest.fn(),
}));

jest.mock('../portfolioDistributionService', () => ({}));
jest.mock('../indexHistoryService', () => ({ calculateIndexData: jest.fn() }));
jest.mock('../marketDataHelper', () => ({ getPricesFromApi: jest.fn() }));
jest.mock('../../utils/mwrCalculations', () => ({
  calculateSimplePersonalReturn: jest.fn(),
  calculateModifiedDietzReturn: jest.fn(),
}));
jest.mock('luxon', () => ({
  DateTime: { now: jest.fn(() => ({ toISO: () => '2026-04-13T12:00:00Z' })) },
}));

const mockIsNYSEMarketOpen = jest.fn(() => false);
const mockCalculateTTLUntilNextEOD = jest.fn(() => 86400000);
jest.mock('../riskMetrics/riskMetricsCache', () => ({
  isNYSEMarketOpen: (...args) => mockIsNYSEMarketOpen(...args),
  calculateTTLUntilNextEOD: (...args) => mockCalculateTTLUntilNextEOD(...args),
  MARKET_CACHE_TTL_MS: 300000,
}));

const mockGeneratePerformanceSnapshot = jest.fn().mockResolvedValue(true);
const mockGenerateAssetSnapshot = jest.fn().mockResolvedValue(true);
const mockBuildSnapshotDocId = jest.fn(
  (userId, accountId, currency, ticker, assetType) => {
    if (ticker && assetType) {
      return accountId === 'overall'
        ? `${userId}_${ticker}_${assetType}_${currency}`
        : `${userId}_${ticker}_${assetType}_${accountId}_${currency}`;
    }
    return accountId === 'overall' ? `${userId}_${currency}` : `${userId}_${accountId}_${currency}`;
  }
);

jest.mock('../snapshotGenerator', () => ({
  buildSnapshotDocId: (...args) => mockBuildSnapshotDocId(...args),
  generatePerformanceSnapshot: (...args) => mockGeneratePerformanceSnapshot(...args),
  generateAssetSnapshot: (...args) => mockGenerateAssetSnapshot(...args),
}));

// Mock firebaseAdmin (db) — needs to be before require
const mockDocGet = jest.fn();
const mockDocSet = jest.fn().mockResolvedValue();
const mockFirestoreDoc = jest.fn(() => ({ get: mockDocGet, set: mockDocSet }));
const mockCollectionGet = jest.fn();
const mockCollectionWhere = jest.fn(() => ({ where: mockCollectionWhere, get: mockCollectionGet }));
const mockFirestoreCollection = jest.fn(() => ({
  doc: mockFirestoreDoc,
  where: mockCollectionWhere,
  get: mockCollectionGet,
}));

jest.mock('../firebaseAdmin', () => {
  const mockDb = {
    doc: (...args) => mockFirestoreDoc(...args),
    collection: (...args) => mockFirestoreCollection(...args),
  };
  return {
    firestore: () => mockDb,
    ...mockDb,
  };
});

// ============================================================================
// Require under test AFTER all mocks
// ============================================================================

const {
  getHistoricalReturns,
  getHistoricalReturnsLegacy,
  transformSnapshotToResponse,
  clearSnapshotMemCache,
} = require('../handlers/queryHandlers');

// ============================================================================
// Test Data
// ============================================================================

const mockAssetSnapshot = {
  userId: 'user1',
  currency: 'USD',
  accountId: 'overall',
  ticker: 'AAPL',
  assetType: 'stock',
  type: 'asset',
  schemaVersion: 2,
  lastUpdated: '2026-04-12T00:05:00Z',
  returns: {
    ytdReturn: 15.2,
    oneMonthReturn: 2.3,
    hasYtdData: true,
    hasOneMonthData: true,
    hasThreeMonthData: true,
    hasSixMonthData: true,
    hasOneYearData: false,
    hasTwoYearData: false,
    hasFiveYearData: false,
  },
  timeline: [
    ['2026-01-02', 18000, 0],
    ['2026-01-03', 18150, 0.83],
    ['2026-04-12', 20736, 15.2],
  ],
  performanceByYear: { '2026': { months: { '1': 2.5 }, total: 15.2 } },
  monthlyCompound: {},
  validDocsCountByPeriod: { ytd: 72, oneMonth: 22 },
  availableYears: ['2026'],
  startDate: '2026-01-02',
};

const createContext = (uid = 'user1') => ({ auth: { uid } });

// ============================================================================
// Tests
// ============================================================================

describe('PERF-SNAP-024: getHistoricalReturns — per-asset snapshot path', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    clearSnapshotMemCache();
    // Default: portfolioPerformance doc exists with no lastSnapshotUpdate
    mockDocGet.mockResolvedValue({ exists: false });
  });

  it('should read asset snapshot when ticker and assetType provided (AC1)', async () => {
    mockDocGet.mockImplementation((path) => {
      // getSnapshotWithCache reads portfolioPerformance/{userId} first
      // then performanceSnapshots/{snapshotId}
      return Promise.resolve({ exists: true, data: () => mockAssetSnapshot });
    });

    const result = await getHistoricalReturns(
      createContext('user1'),
      { currency: 'USD', accountId: 'overall', ticker: 'AAPL', assetType: 'stock' }
    );

    expect(mockBuildSnapshotDocId).toHaveBeenCalledWith('user1', 'overall', 'USD', 'AAPL', 'stock');
    expect(result.returns).toEqual(mockAssetSnapshot.returns);
    expect(result._metadata.version).toBe('snapshot');
    expect(result._metadata.schemaVersion).toBe(2);
  });

  it('should fallback to legacy when asset snapshot not found (AC4)', async () => {
    // All doc reads return { exists: false }
    mockDocGet.mockResolvedValue({ exists: false });
    // Legacy path needs performanceCache miss + V2 result
    const { getHistoricalReturnsV2 } = require('../consolidatedReturnsService');
    getHistoricalReturnsV2.mockResolvedValue({
      returns: { ytdReturn: 10, hasYtdData: true, hasOneMonthData: false, hasThreeMonthData: false },
      totalValueData: { dates: ['2026-04-12'], values: [1000], percentChanges: [0], overallPercentChange: 0 },
      performanceByYear: {},
      availableYears: ['2026'],
      startDate: '2026-04-12',
      monthlyCompoundData: {},
    });

    const result = await getHistoricalReturns(
      createContext('user1'),
      { currency: 'USD', accountId: 'overall', ticker: 'AAPL', assetType: 'stock' }
    );

    expect(result.returns.ytdReturn).toBe(10);
  });

  it('should fire-and-forget generateAssetSnapshot on snapshot miss (AC5)', async () => {
    mockDocGet.mockResolvedValue({ exists: false });
    const { getHistoricalReturnsV2 } = require('../consolidatedReturnsService');
    getHistoricalReturnsV2.mockResolvedValue({
      returns: { ytdReturn: 10, hasYtdData: true, hasOneMonthData: false, hasThreeMonthData: false },
      totalValueData: { dates: [], values: [], percentChanges: [] },
      performanceByYear: {},
      availableYears: [],
      startDate: '',
      monthlyCompoundData: {},
    });

    await getHistoricalReturns(
      createContext('user1'),
      { currency: 'USD', accountId: 'overall', ticker: 'AAPL', assetType: 'stock' }
    );

    expect(mockGenerateAssetSnapshot).toHaveBeenCalledWith(
      expect.anything(), 'user1', 'overall', 'USD', 'AAPL', 'stock'
    );
  });

  it('should bypass snapshot when forceRefresh=true', async () => {
    mockDocGet.mockResolvedValue({ exists: false });
    const { getHistoricalReturnsV2 } = require('../consolidatedReturnsService');
    getHistoricalReturnsV2.mockResolvedValue({
      returns: { ytdReturn: 5, hasYtdData: true, hasOneMonthData: false, hasThreeMonthData: false },
      totalValueData: { dates: [], values: [], percentChanges: [] },
      performanceByYear: {},
      availableYears: [],
      startDate: '',
      monthlyCompoundData: {},
    });

    await getHistoricalReturns(
      createContext('user1'),
      { currency: 'USD', accountId: 'overall', ticker: 'AAPL', assetType: 'stock', forceRefresh: true }
    );

    expect(mockBuildSnapshotDocId).not.toHaveBeenCalledWith(
      expect.anything(), expect.anything(), expect.anything(), 'AAPL', 'stock'
    );
    expect(mockGenerateAssetSnapshot).not.toHaveBeenCalled();
  });

  it('should fallback to legacy when only ticker provided (no assetType)', async () => {
    mockDocGet.mockResolvedValue({ exists: false });
    const { getHistoricalReturnsV2 } = require('../consolidatedReturnsService');
    getHistoricalReturnsV2.mockResolvedValue({
      returns: { ytdReturn: 3, hasYtdData: true, hasOneMonthData: false, hasThreeMonthData: false },
      totalValueData: { dates: [], values: [], percentChanges: [] },
      performanceByYear: {},
      availableYears: [],
      startDate: '',
      monthlyCompoundData: {},
    });

    await getHistoricalReturns(
      createContext('user1'),
      { currency: 'USD', accountId: 'overall', ticker: 'AAPL', assetType: null }
    );

    // Should NOT try to build per-asset snapshot docId
    expect(mockBuildSnapshotDocId).not.toHaveBeenCalledWith(
      expect.anything(), expect.anything(), expect.anything(), 'AAPL', expect.anything()
    );
    expect(mockGenerateAssetSnapshot).not.toHaveBeenCalled();
  });
});
