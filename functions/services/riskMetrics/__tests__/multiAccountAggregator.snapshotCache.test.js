const mockDocGet = jest.fn();

const mockDb = {
  doc: jest.fn(() => ({ get: mockDocGet })),
  collection: jest.fn(() => ({
    where: jest.fn().mockReturnThis(),
    orderBy: jest.fn().mockReturnThis(),
    get: jest.fn().mockResolvedValue({ empty: true, docs: [] }),
  })),
};

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
  getOrReadSnapshot,
  clearSnapshotCache,
  getSnapshotCacheSize,
  SNAPSHOT_LOCAL_CACHE_TTL,
  fetchOverallPerformanceData,
  fetchAccountPerformanceData,
} = require('../multiAccountAggregator');

const SAMPLE_SNAPSHOT = {
  timeline: [
    ['2026-01-01', 10000, 1.5],
    ['2026-01-02', 10150, 0.8],
    ['2026-01-03', 10200, -0.3],
    ['2026-06-01', 10500, 0.5],
    ['2026-12-31', 11000, 1.0],
  ],
};

beforeEach(() => {
  jest.clearAllMocks();
  clearSnapshotCache();
});

// =============================================================================
// getOrReadSnapshot — unit tests
// =============================================================================
describe('getOrReadSnapshot', () => {
  it('should read from Firestore on cache miss', async () => {
    mockDocGet.mockResolvedValue({ exists: true, data: () => SAMPLE_SNAPSHOT });

    const result = await getOrReadSnapshot('user1_USD');

    expect(mockDb.doc).toHaveBeenCalledWith('performanceSnapshots/user1_USD');
    expect(mockDocGet).toHaveBeenCalledTimes(1);
    expect(result).toEqual(SAMPLE_SNAPSHOT);
  });

  it('should return cached data on second call without reading Firestore again (AC1)', async () => {
    mockDocGet.mockResolvedValue({ exists: true, data: () => SAMPLE_SNAPSHOT });

    await getOrReadSnapshot('user1_USD');
    const result = await getOrReadSnapshot('user1_USD');

    expect(mockDocGet).toHaveBeenCalledTimes(1);
    expect(result).toEqual(SAMPLE_SNAPSHOT);
  });

  it('should return null for non-existent snapshot without caching', async () => {
    mockDocGet.mockResolvedValue({ exists: false });

    const result1 = await getOrReadSnapshot('missing_snapshot');
    expect(result1).toBeNull();
    expect(getSnapshotCacheSize()).toBe(0);

    mockDocGet.mockResolvedValue({ exists: true, data: () => SAMPLE_SNAPSHOT });
    const result2 = await getOrReadSnapshot('missing_snapshot');
    expect(result2).toEqual(SAMPLE_SNAPSHOT);
    expect(mockDocGet).toHaveBeenCalledTimes(2);
  });

  it('should re-read from Firestore after TTL expires', async () => {
    const updatedSnapshot = { timeline: [['2026-01-01', 12000, 2.0]] };
    mockDocGet.mockResolvedValue({ exists: true, data: () => SAMPLE_SNAPSHOT });

    await getOrReadSnapshot('user1_USD');
    expect(mockDocGet).toHaveBeenCalledTimes(1);

    const originalDateNow = Date.now;
    Date.now = jest.fn(() => originalDateNow() + SNAPSHOT_LOCAL_CACHE_TTL + 1);

    mockDocGet.mockResolvedValue({ exists: true, data: () => updatedSnapshot });

    const result = await getOrReadSnapshot('user1_USD');

    expect(mockDocGet).toHaveBeenCalledTimes(2);
    expect(result).toEqual(updatedSnapshot);

    Date.now = originalDateNow;
  });

  it('should evict oldest entry when cache exceeds max size', async () => {
    mockDocGet.mockImplementation(() =>
      Promise.resolve({ exists: true, data: () => ({ timeline: [] }) })
    );

    const SNAPSHOT_LOCAL_CACHE_MAX_SIZE = 200;
    for (let i = 0; i < SNAPSHOT_LOCAL_CACHE_MAX_SIZE; i++) {
      await getOrReadSnapshot(`snapshot_${i}`);
    }
    expect(getSnapshotCacheSize()).toBe(SNAPSHOT_LOCAL_CACHE_MAX_SIZE);

    await getOrReadSnapshot('snapshot_new');

    expect(getSnapshotCacheSize()).toBe(SNAPSHOT_LOCAL_CACHE_MAX_SIZE);
  });
});

// =============================================================================
// fetchOverallPerformanceData — snapshot cache integration
// =============================================================================
describe('fetchOverallPerformanceData with snapshot cache', () => {
  it('should use cache for 2 calls with different periods, 1 Firestore read (AC1, AC2)', async () => {
    mockDocGet.mockResolvedValue({ exists: true, data: () => SAMPLE_SNAPSHOT });

    const resultYTD = await fetchOverallPerformanceData('user1', '2026-01-01', '2026-06-01', 'USD');
    const result1Y = await fetchOverallPerformanceData('user1', '2026-01-01', '2026-12-31', 'USD');

    expect(mockDocGet).toHaveBeenCalledTimes(1);

    expect(resultYTD.dailyData).toHaveLength(4);
    expect(result1Y.dailyData).toHaveLength(5);
    expect(resultYTD.accountId).toBe('overall');
    expect(result1Y.accountId).toBe('overall');
  });
});

// =============================================================================
// fetchAccountPerformanceData — snapshot cache integration
// =============================================================================
describe('fetchAccountPerformanceData with snapshot cache', () => {
  it('should use cache for 2 calls with different periods, 1 Firestore read (AC1, AC2)', async () => {
    mockDocGet.mockResolvedValue({ exists: true, data: () => SAMPLE_SNAPSHOT });

    const resultYTD = await fetchAccountPerformanceData('user1', 'acc1', '2026-01-01', '2026-06-01', 'USD');
    const resultALL = await fetchAccountPerformanceData('user1', 'acc1', '2026-01-01', '2026-12-31', 'USD');

    expect(mockDocGet).toHaveBeenCalledTimes(1);

    expect(resultYTD.dailyData).toHaveLength(4);
    expect(resultALL.dailyData).toHaveLength(5);
    expect(resultYTD.accountId).toBe('acc1');
    expect(resultALL.accountId).toBe('acc1');
  });
});
