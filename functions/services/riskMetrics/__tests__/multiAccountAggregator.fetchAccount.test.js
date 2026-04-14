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
  fetchAccountPerformanceData,
  fetchAccountPerformanceDataLegacy,
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
// fetchAccountPerformanceData — snapshot path
// =============================================================================
describe('fetchAccountPerformanceData', () => {
  it('should return { accountId, currentValue, dailyData, dataPoints } from snapshot (AC1, AC2)', async () => {
    const snapshotData = {
      timeline: [
        ['2026-01-01', 5000, 0],
        ['2026-01-02', 5100, 2.0],
        ['2026-01-03', 5200, 1.96],
      ],
    };

    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => snapshotData,
    });

    const result = await fetchAccountPerformanceData('user1', 'acc1', '2026-01-01', '2026-01-03', 'USD');

    expect(result.accountId).toBe('acc1');
    expect(result.currentValue).toBe(5200);
    expect(result.dataPoints).toBe(3);
    expect(result.dailyData).toHaveLength(3);
    expect(result.dailyData[0]).toEqual({ date: '2026-01-01', return: 0, value: 5000 });
    expect(result.dailyData[1]).toEqual({ date: '2026-01-02', return: 0.02, value: 5100 });
  });

  it('should call buildSnapshotDocId with (userId, accountId, currency) (AC1)', async () => {
    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => ({
        timeline: [['2026-01-01', 5000, 1.0]],
      }),
    });

    await fetchAccountPerformanceData('user123', 'acc42', '2026-01-01', '2026-01-01', 'COP');

    expect(buildSnapshotDocId).toHaveBeenCalledWith('user123', 'acc42', 'COP');
    expect(mockDb.doc).toHaveBeenCalledWith('performanceSnapshots/user123_acc42_COP');
  });

  it('should default currency to USD when not provided', async () => {
    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => ({
        timeline: [['2026-01-01', 5000, 1.0]],
      }),
    });

    await fetchAccountPerformanceData('user1', 'acc1', '2026-01-01', '2026-01-01', undefined);

    expect(buildSnapshotDocId).toHaveBeenCalledWith('user1', 'acc1', 'USD');
  });

  it('should filter timeline by startDate/endDate in memory (AC2)', async () => {
    const snapshotData = {
      timeline: [
        ['2026-01-01', 5000, 0],
        ['2026-01-02', 5100, 2.0],
        ['2026-01-03', 5200, 1.96],
        ['2026-01-04', 5300, 1.92],
        ['2026-01-05', 5400, 1.89],
      ],
    };

    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => snapshotData,
    });

    const result = await fetchAccountPerformanceData('user1', 'acc1', '2026-01-02', '2026-01-04', 'USD');

    expect(result.dailyData).toHaveLength(3);
    expect(result.dailyData[0].date).toBe('2026-01-02');
    expect(result.dailyData[2].date).toBe('2026-01-04');
  });

  it('should make exactly 1 db.doc().get() call when snapshot exists (AC1)', async () => {
    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => ({
        timeline: [['2026-01-01', 5000, 1.0]],
      }),
    });

    await fetchAccountPerformanceData('user1', 'acc1', '2026-01-01', '2026-01-01', 'USD');

    expect(mockDb.doc).toHaveBeenCalledTimes(1);
    expect(mockDocGet).toHaveBeenCalledTimes(1);
    expect(mockDb.collection).not.toHaveBeenCalled();
  });

  it('should fallback to legacy when snapshot does not exist (AC1)', async () => {
    mockDocGet.mockResolvedValue({ exists: false });

    const legacyDocs = [
      { id: '2026-01-01', data: () => ({ USD: { totalValue: 5000, adjustedDailyChangePercentage: 0 } }) },
      { id: '2026-01-02', data: () => ({ USD: { totalValue: 5100, adjustedDailyChangePercentage: 2.0 } }) },
    ];
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    const result = await fetchAccountPerformanceData('user1', 'acc1', '2026-01-01', '2026-01-02', 'USD');

    expect(result.accountId).toBe('acc1');
    expect(result.dailyData).toHaveLength(2);
    expect(mockDb.collection).toHaveBeenCalledWith('portfolioPerformance/user1/accounts/acc1/dates');
  });

  it('should fallback to legacy when snapshot timeline is empty (AC1)', async () => {
    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => ({ timeline: [] }),
    });

    const legacyDocs = [
      { id: '2026-01-01', data: () => ({ USD: { totalValue: 5000, dailyChangePercentage: 0.5 } }) },
    ];
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    const result = await fetchAccountPerformanceData('user1', 'acc1', '2026-01-01', '2026-01-01', 'USD');

    expect(result.accountId).toBe('acc1');
    expect(result.dailyData).toHaveLength(1);
    expect(mockDb.collection).toHaveBeenCalled();
  });

  it('should fallback to legacy when snapshot read throws an error (AC1)', async () => {
    mockDocGet.mockRejectedValue(new Error('Firestore unavailable'));

    const legacyDocs = [
      { id: '2026-01-01', data: () => ({ USD: { totalValue: 5000, adjustedDailyChangePercentage: 0 } }) },
    ];
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    const result = await fetchAccountPerformanceData('user1', 'acc1', '2026-01-01', '2026-01-01', 'USD');

    expect(result).not.toBeNull();
    expect(result.accountId).toBe('acc1');
  });

  it('should fallback when snapshot exists but filtered dailyData is empty', async () => {
    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => ({
        timeline: [['2025-01-01', 5000, 1.0]],
      }),
    });

    const legacyDocs = [
      { id: '2026-06-01', data: () => ({ USD: { totalValue: 7000, adjustedDailyChangePercentage: 0.5 } }) },
    ];
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    const result = await fetchAccountPerformanceData('user1', 'acc1', '2026-06-01', '2026-06-01', 'USD');

    expect(result.accountId).toBe('acc1');
    expect(mockDb.collection).toHaveBeenCalled();
  });
});
