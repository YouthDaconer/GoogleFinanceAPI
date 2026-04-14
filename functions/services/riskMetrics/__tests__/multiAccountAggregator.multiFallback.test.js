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
  aggregateMultiAccountData,
  clearSnapshotCache,
} = require('../multiAccountAggregator');

const makeSnapshotData = (dates) => ({
  timeline: dates.map(([date, value, pct]) => [date, value, pct]),
});

const makeLegacyDocs = (entries, currency = 'USD') =>
  entries.map(([id, totalValue, pct]) => ({
    id,
    data: () => ({ [currency]: { totalValue, adjustedDailyChangePercentage: pct } }),
  }));

beforeEach(() => {
  jest.clearAllMocks();
  clearSnapshotCache();
  mockWhere.mockReturnValue({ where: mockWhere, orderBy: mockOrderBy });
  mockOrderBy.mockReturnValue({ get: mockCollectionGet });
});

// =============================================================================
// aggregateMultiAccountData — multi strategy uniform fallback (AC3)
// =============================================================================
describe('aggregateMultiAccountData multi strategy fallback', () => {

  it('should use snapshot data when ALL accounts have snapshots (AC3)', async () => {
    const acc1Snapshot = makeSnapshotData([
      ['2026-01-01', 5000, 0],
      ['2026-01-02', 5100, 2.0],
    ]);
    const acc2Snapshot = makeSnapshotData([
      ['2026-01-01', 3000, 0],
      ['2026-01-02', 3060, 2.0],
    ]);

    mockDocGet
      .mockResolvedValueOnce({ exists: true, data: () => acc1Snapshot })
      .mockResolvedValueOnce({ exists: true, data: () => acc2Snapshot });

    const result = await aggregateMultiAccountData(
      'user1', ['acc1', 'acc2'], '2026-01-01', '2026-01-02', 'USD'
    );

    expect(result.strategy).toBe('multi');
    expect(result.accountsProcessed).toBe(2);
    expect(result.accountsRequested).toBe(2);
    expect(result.metadata.accountsIncluded).toEqual(['acc1', 'acc2']);
    expect(mockDb.collection).not.toHaveBeenCalled();
  });

  it('should fallback to legacy for ALL accounts when one snapshot is missing (AC3)', async () => {
    const acc1Snapshot = makeSnapshotData([
      ['2026-01-01', 5000, 0],
      ['2026-01-02', 5100, 2.0],
    ]);

    mockDocGet
      .mockResolvedValueOnce({ exists: true, data: () => acc1Snapshot })
      .mockResolvedValueOnce({ exists: false });

    const legacyDocs1 = makeLegacyDocs([
      ['2026-01-01', 5000, 0],
      ['2026-01-02', 5100, 2.0],
    ]);
    const legacyDocs2 = makeLegacyDocs([
      ['2026-01-01', 3000, 0],
      ['2026-01-02', 3060, 2.0],
    ]);

    mockCollectionGet
      .mockResolvedValueOnce({ empty: false, docs: legacyDocs1 })
      .mockResolvedValueOnce({ empty: false, docs: legacyDocs2 });

    const result = await aggregateMultiAccountData(
      'user1', ['acc1', 'acc2'], '2026-01-01', '2026-01-02', 'USD'
    );

    expect(result.strategy).toBe('multi');
    expect(result.accountsProcessed).toBe(2);
    expect(mockDb.collection).toHaveBeenCalledWith('portfolioPerformance/user1/accounts/acc1/dates');
    expect(mockDb.collection).toHaveBeenCalledWith('portfolioPerformance/user1/accounts/acc2/dates');
  });

  it('should fallback to legacy for ALL accounts when NO snapshots exist (AC3)', async () => {
    mockDocGet
      .mockResolvedValueOnce({ exists: false })
      .mockResolvedValueOnce({ exists: false });

    const legacyDocs1 = makeLegacyDocs([['2026-01-01', 5000, 0]]);
    const legacyDocs2 = makeLegacyDocs([['2026-01-01', 3000, 0]]);

    mockCollectionGet
      .mockResolvedValueOnce({ empty: false, docs: legacyDocs1 })
      .mockResolvedValueOnce({ empty: false, docs: legacyDocs2 });

    const result = await aggregateMultiAccountData(
      'user1', ['acc1', 'acc2'], '2026-01-01', '2026-01-01', 'USD'
    );

    expect(result.strategy).toBe('multi');
    expect(result.accountsProcessed).toBe(2);
    expect(mockDb.collection).toHaveBeenCalledTimes(2);
  });

  it('should log mixed availability message on partial fallback (AC4)', async () => {
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    mockDocGet
      .mockResolvedValueOnce({ exists: true, data: () => makeSnapshotData([['2026-01-01', 5000, 0]]) })
      .mockResolvedValueOnce({ exists: false });

    const legacyDocs = makeLegacyDocs([['2026-01-01', 5000, 0]]);
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    await aggregateMultiAccountData(
      'user1', ['acc1', 'acc2'], '2026-01-01', '2026-01-01', 'USD'
    );

    expect(consoleSpy).toHaveBeenCalledWith(
      '[RiskMetrics] Mixed snapshot availability in multi-account, falling back to legacy for all accounts'
    );

    consoleSpy.mockRestore();
  });

  it('should return identical result structure from both snapshot and legacy paths (AC5)', async () => {
    const snapshotData = makeSnapshotData([
      ['2026-01-01', 5000, 0],
      ['2026-01-02', 5100, 2.0],
    ]);

    mockDocGet
      .mockResolvedValueOnce({ exists: true, data: () => snapshotData })
      .mockResolvedValueOnce({ exists: true, data: () => snapshotData });

    const snapshotResult = await aggregateMultiAccountData(
      'user1', ['acc1', 'acc2'], '2026-01-01', '2026-01-02', 'USD'
    );

    clearSnapshotCache();
    jest.clearAllMocks();
    mockWhere.mockReturnValue({ where: mockWhere, orderBy: mockOrderBy });
    mockOrderBy.mockReturnValue({ get: mockCollectionGet });

    mockDocGet
      .mockResolvedValueOnce({ exists: false })
      .mockResolvedValueOnce({ exists: false });

    const legacyDocs = makeLegacyDocs([
      ['2026-01-01', 5000, 0],
      ['2026-01-02', 5100, 2.0],
    ]);
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    const legacyResult = await aggregateMultiAccountData(
      'user1', ['acc1', 'acc2'], '2026-01-01', '2026-01-02', 'USD'
    );

    expect(snapshotResult).toHaveProperty('strategy', 'multi');
    expect(legacyResult).toHaveProperty('strategy', 'multi');
    expect(snapshotResult).toHaveProperty('dailyReturns');
    expect(legacyResult).toHaveProperty('dailyReturns');
    expect(snapshotResult).toHaveProperty('dailyData');
    expect(legacyResult).toHaveProperty('dailyData');
    expect(snapshotResult).toHaveProperty('totalValue');
    expect(legacyResult).toHaveProperty('totalValue');
    expect(snapshotResult).toHaveProperty('metadata');
    expect(legacyResult).toHaveProperty('metadata');
  });
});

// =============================================================================
// aggregateMultiAccountData — overall and single strategies non-regression (AC1, AC2)
// =============================================================================
describe('aggregateMultiAccountData non-regression', () => {

  it('overall strategy maintains individual fallback (AC1)', async () => {
    mockDocGet.mockResolvedValue({ exists: false });

    const legacyDocs = makeLegacyDocs([
      ['2026-01-01', 10000, 0],
      ['2026-01-02', 10100, 1.0],
    ]);
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    const result = await aggregateMultiAccountData(
      'user1', ['overall'], '2026-01-01', '2026-01-02', 'USD'
    );

    expect(result.strategy).toBe('overall');
    expect(result.accountsProcessed).toBe(1);
    expect(mockDb.collection).toHaveBeenCalledWith('portfolioPerformance/user1/dates');
  });

  it('single strategy maintains individual fallback (AC2)', async () => {
    mockDocGet.mockResolvedValue({ exists: false });

    const legacyDocs = makeLegacyDocs([
      ['2026-01-01', 5000, 0],
      ['2026-01-02', 5100, 2.0],
    ]);
    mockCollectionGet.mockResolvedValue({ empty: false, docs: legacyDocs });

    const result = await aggregateMultiAccountData(
      'user1', ['acc1'], '2026-01-01', '2026-01-02', 'USD'
    );

    expect(result.strategy).toBe('single');
    expect(result.accountsProcessed).toBe(1);
    expect(mockDb.collection).toHaveBeenCalledWith('portfolioPerformance/user1/accounts/acc1/dates');
  });
});
