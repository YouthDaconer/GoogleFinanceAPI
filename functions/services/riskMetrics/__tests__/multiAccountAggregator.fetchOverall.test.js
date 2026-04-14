const mockGet = jest.fn();
const mockDocGet = jest.fn();
const mockWhere = jest.fn();
const mockOrderBy = jest.fn();
const mockCollectionGet = jest.fn();

const mockDb = {
  doc: jest.fn(() => ({ get: mockDocGet })),
  collection: jest.fn(() => ({
    where: mockWhere,
  })),
};

mockWhere.mockReturnValue({ where: mockWhere, orderBy: mockOrderBy });
mockOrderBy.mockReturnValue({ get: mockCollectionGet });

jest.mock('firebase-admin', () => ({
  firestore: jest.fn(() => mockDb),
  initializeApp: jest.fn(),
  apps: [{}],
  credential: { cert: jest.fn() },
}));

jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: jest.fn((userId, accountId, currency) => {
    if (accountId === 'overall') return `${userId}_${currency}`;
    return `${userId}_${accountId}_${currency}`;
  }),
}));

const {
  fetchOverallPerformanceData,
  fetchOverallPerformanceDataLegacy,
  extractDailyDataFromSnapshot,
  clearSnapshotCache,
} = require('../multiAccountAggregator');
const { buildSnapshotDocId } = require('../../snapshotGenerator');

beforeEach(() => {
  jest.clearAllMocks();
  clearSnapshotCache();
  mockWhere.mockReturnValue({ where: mockWhere, orderBy: mockOrderBy });
  mockOrderBy.mockReturnValue({ get: mockCollectionGet });
});

// =============================================================================
// extractDailyDataFromSnapshot — unit tests
// =============================================================================
describe('extractDailyDataFromSnapshot', () => {
  it('should filter timeline by date range and return correct format', () => {
    const snapshot = {
      timeline: [
        ['2026-01-01', 10000, 1.5],
        ['2026-01-02', 10150, 0.8],
        ['2026-01-03', 10200, -0.3],
        ['2026-01-04', 10180, 1.2],
        ['2026-01-05', 10300, 0.5],
      ],
    };

    const result = extractDailyDataFromSnapshot(snapshot, '2026-01-02', '2026-01-04');

    expect(result).toHaveLength(3);
    expect(result[0]).toEqual({ date: '2026-01-02', return: 0.008, value: 10150 });
    expect(result[1]).toEqual({ date: '2026-01-03', return: -0.003, value: 10200 });
    expect(result[2]).toEqual({ date: '2026-01-04', return: 0.012, value: 10180 });
  });

  it('should return empty array for empty timeline', () => {
    expect(extractDailyDataFromSnapshot({ timeline: [] }, '2026-01-01', '2026-12-31')).toEqual([]);
  });

  it('should return empty array for null/undefined snapshot', () => {
    expect(extractDailyDataFromSnapshot(null, '2026-01-01', '2026-12-31')).toEqual([]);
    expect(extractDailyDataFromSnapshot(undefined, '2026-01-01', '2026-12-31')).toEqual([]);
    expect(extractDailyDataFromSnapshot({}, '2026-01-01', '2026-12-31')).toEqual([]);
  });

  it('should convert percentChange to decimal (entry[2] / 100)', () => {
    const snapshot = {
      timeline: [['2026-03-15', 5000, 2.5]],
    };

    const result = extractDailyDataFromSnapshot(snapshot, '2026-01-01', '2026-12-31');
    expect(result[0].return).toBe(0.025);
  });

  it('should use totalValue from entry[1]', () => {
    const snapshot = {
      timeline: [['2026-03-15', 12345.67, 1.0]],
    };

    const result = extractDailyDataFromSnapshot(snapshot, '2026-01-01', '2026-12-31');
    expect(result[0].value).toBe(12345.67);
  });

  it('should handle null percentChange as 0', () => {
    const snapshot = {
      timeline: [['2026-03-15', 5000, null]],
    };

    const result = extractDailyDataFromSnapshot(snapshot, '2026-01-01', '2026-12-31');
    expect(result[0].return).toBe(0);
  });

  it('should handle undefined percentChange (2-element entry) as 0', () => {
    const snapshot = {
      timeline: [['2026-03-15', 5000]],
    };

    const result = extractDailyDataFromSnapshot(snapshot, '2026-01-01', '2026-12-31');
    expect(result[0].return).toBe(0);
  });
});

// =============================================================================
// fetchOverallPerformanceData — integration tests with snapshot
// =============================================================================
describe('fetchOverallPerformanceData', () => {
  it('should read from snapshot when it exists and return correct format (AC1, AC2)', async () => {
    const snapshotData = {
      timeline: [
        ['2026-01-01', 10000, 0],
        ['2026-01-02', 10100, 1.0],
        ['2026-01-03', 10200, 0.99],
      ],
    };

    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => snapshotData,
    });

    const result = await fetchOverallPerformanceData('user1', '2026-01-01', '2026-01-03', 'USD');

    expect(result.accountId).toBe('overall');
    expect(result.currentValue).toBe(10200);
    expect(result.dataPoints).toBe(3);
    expect(result.dailyData).toHaveLength(3);
    expect(result.dailyData[0]).toEqual({ date: '2026-01-01', return: 0, value: 10000 });
    expect(result.dailyData[1]).toEqual({ date: '2026-01-02', return: 0.01, value: 10100 });
    expect(result.dailyData[2].date).toBe('2026-01-03');
    expect(result.dailyData[2].value).toBe(10200);
    expect(result.dailyData[2].return).toBeCloseTo(0.0099, 6);
  });

  it('should filter timeline by startDate/endDate in memory (AC3)', async () => {
    const snapshotData = {
      timeline: [
        ['2026-01-01', 10000, 0],
        ['2026-01-02', 10100, 1.0],
        ['2026-01-03', 10200, 0.99],
        ['2026-01-04', 10300, 0.98],
        ['2026-01-05', 10400, 0.97],
      ],
    };

    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => snapshotData,
    });

    const result = await fetchOverallPerformanceData('user1', '2026-01-02', '2026-01-04', 'USD');

    expect(result.dailyData).toHaveLength(3);
    expect(result.dailyData[0].date).toBe('2026-01-02');
    expect(result.dailyData[2].date).toBe('2026-01-04');
  });

  it('should make exactly 1 Firestore read when snapshot exists (AC4)', async () => {
    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => ({
        timeline: [['2026-01-01', 10000, 1.0]],
      }),
    });

    await fetchOverallPerformanceData('user1', '2026-01-01', '2026-01-01', 'USD');

    expect(mockDb.doc).toHaveBeenCalledTimes(1);
    expect(mockDocGet).toHaveBeenCalledTimes(1);
    expect(mockDb.collection).not.toHaveBeenCalled();
  });

  it('should use buildSnapshotDocId with correct params', async () => {
    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => ({
        timeline: [['2026-01-01', 10000, 1.0]],
      }),
    });

    await fetchOverallPerformanceData('user123', '2026-01-01', '2026-01-01', 'COP');

    expect(buildSnapshotDocId).toHaveBeenCalledWith('user123', 'overall', 'COP');
    expect(mockDb.doc).toHaveBeenCalledWith('performanceSnapshots/user123_COP');
  });

  it('should default currency to USD when not provided', async () => {
    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => ({
        timeline: [['2026-01-01', 10000, 1.0]],
      }),
    });

    await fetchOverallPerformanceData('user1', '2026-01-01', '2026-01-01', undefined);

    expect(buildSnapshotDocId).toHaveBeenCalledWith('user1', 'overall', 'USD');
  });

  it('should fallback to legacy when snapshot does not exist (AC5)', async () => {
    mockDocGet.mockResolvedValue({ exists: false });

    const legacyDocs = [
      { id: '2026-01-01', data: () => ({ USD: { totalValue: 10000, adjustedDailyChangePercentage: 0 } }) },
      { id: '2026-01-02', data: () => ({ USD: { totalValue: 10100, adjustedDailyChangePercentage: 1.0 } }) },
    ];
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    const result = await fetchOverallPerformanceData('user1', '2026-01-01', '2026-01-02', 'USD');

    expect(result.accountId).toBe('overall');
    expect(result.dailyData).toHaveLength(2);
    expect(mockDb.collection).toHaveBeenCalledWith('portfolioPerformance/user1/dates');
  });

  it('should fallback to legacy when snapshot timeline is empty (AC5)', async () => {
    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => ({ timeline: [] }),
    });

    const legacyDocs = [
      { id: '2026-01-01', data: () => ({ USD: { totalValue: 10000, dailyChangePercentage: 0.5 } }) },
    ];
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    const result = await fetchOverallPerformanceData('user1', '2026-01-01', '2026-01-01', 'USD');

    expect(result.accountId).toBe('overall');
    expect(result.dailyData).toHaveLength(1);
    expect(mockDb.collection).toHaveBeenCalled();
  });

  it('should fallback to legacy when snapshot read throws an error', async () => {
    mockDocGet.mockRejectedValue(new Error('Firestore unavailable'));

    const legacyDocs = [
      { id: '2026-01-01', data: () => ({ USD: { totalValue: 10000, adjustedDailyChangePercentage: 0 } }) },
    ];
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    const result = await fetchOverallPerformanceData('user1', '2026-01-01', '2026-01-01', 'USD');

    expect(result).not.toBeNull();
    expect(result.accountId).toBe('overall');
  });

  it('should fallback when snapshot exists but filtered dailyData is empty', async () => {
    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => ({
        timeline: [['2025-01-01', 10000, 1.0]],
      }),
    });

    const legacyDocs = [
      { id: '2026-06-01', data: () => ({ USD: { totalValue: 15000, adjustedDailyChangePercentage: 0.5 } }) },
    ];
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    const result = await fetchOverallPerformanceData('user1', '2026-06-01', '2026-06-01', 'USD');

    expect(result.accountId).toBe('overall');
    expect(mockDb.collection).toHaveBeenCalled();
  });
});
