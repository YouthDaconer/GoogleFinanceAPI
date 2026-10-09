/**
 * PERF-SNAP-005: Tests para script de backfill de snapshots
 *
 * @see docs/stories/PERF-SNAP-005.story.md
 */

// === Mocks ===

const mockGenerateAllSnapshots = jest.fn().mockResolvedValue({ success: 3, failed: 0, total: 3 });
jest.mock('../../services/snapshotGenerator', () => ({
  generateAllSnapshots: (...args) => mockGenerateAllSnapshots(...args),
}));

const mockGetActiveCurrencies = jest.fn().mockResolvedValue(['USD', 'COP']);
const mockGetUserAccounts = jest.fn().mockResolvedValue([
  { id: 'acc-1', name: 'Main' },
  { id: 'acc-2', name: 'Savings' },
]);
jest.mock('../../services/backfillCoreModule', () => ({
  getActiveCurrencies: (...args) => mockGetActiveCurrencies(...args),
  getUserAccounts: (...args) => mockGetUserAccounts(...args),
}));

// === Import ===

const { backfillAllSnapshots } = require('../backfillPerformanceSnapshots');

// === Helpers ===

function createMockDb(userIds) {
  const docs = userIds.map(id => ({ id, data: () => ({}) }));
  return {
    collection: jest.fn(() => ({
      get: jest.fn().mockResolvedValue({ docs }),
    })),
  };
}

// === Tests ===

describe('PERF-SNAP-005: backfillAllSnapshots', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockGenerateAllSnapshots.mockResolvedValue({ success: 3, failed: 0, total: 3 });
    mockGetActiveCurrencies.mockResolvedValue(['USD', 'COP']);
    mockGetUserAccounts.mockResolvedValue([
      { id: 'acc-1', name: 'Main' },
      { id: 'acc-2', name: 'Savings' },
    ]);
  });

  it('should iterate all users from portfolioPerformance (AC1)', async () => {
    const db = createMockDb(['user-1', 'user-2', 'user-3']);

    const result = await backfillAllSnapshots(db);

    expect(db.collection).toHaveBeenCalledWith('portfolioPerformance');
    expect(mockGenerateAllSnapshots).toHaveBeenCalledTimes(3);
    expect(result.processed).toBe(3);
    expect(result.total).toBe(3);
  });

  it('should call generateAllSnapshots with correct params per user (AC1, AC2)', async () => {
    const db = createMockDb(['user-1']);

    await backfillAllSnapshots(db);

    expect(mockGenerateAllSnapshots).toHaveBeenCalledWith(
      db,
      'user-1',
      ['USD', 'COP'],
      ['acc-1', 'acc-2']
    );
  });

  it('should process in batches of batchSize (AC3)', async () => {
    const callOrder = [];
    mockGenerateAllSnapshots.mockImplementation(async (_db, userId) => {
      callOrder.push(userId);
      return { success: 1, failed: 0, total: 1 };
    });

    const db = createMockDb(['u1', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7']);

    await backfillAllSnapshots(db, { batchSize: 3 });

    expect(mockGenerateAllSnapshots).toHaveBeenCalledTimes(7);
    // All 7 users processed
    expect(callOrder).toHaveLength(7);
  });

  it('should NOT call generateAllSnapshots in dry-run mode (AC5)', async () => {
    const db = createMockDb(['user-1', 'user-2']);

    const result = await backfillAllSnapshots(db, { dryRun: true });

    expect(mockGenerateAllSnapshots).not.toHaveBeenCalled();
    expect(result.processed).toBe(2);
    expect(result.failed).toBe(0);
  });

  it('should continue processing when a user fails (resiliencia)', async () => {
    mockGenerateAllSnapshots
      .mockResolvedValueOnce({ success: 2, failed: 0, total: 2 })
      .mockRejectedValueOnce(new Error('Firestore timeout'))
      .mockResolvedValueOnce({ success: 2, failed: 0, total: 2 });

    const db = createMockDb(['user-ok-1', 'user-fail', 'user-ok-2']);

    const result = await backfillAllSnapshots(db, { batchSize: 1 });

    expect(mockGenerateAllSnapshots).toHaveBeenCalledTimes(3);
    expect(result.processed).toBe(2);
    expect(result.failed).toBe(1);
    expect(result.total).toBe(3);
  });

  it('should return summary with totals', async () => {
    const db = createMockDb(['user-1', 'user-2']);

    const result = await backfillAllSnapshots(db);

    expect(result).toEqual({
      processed: 2,
      failed: 0,
      total: 2,
    });
  });

  it('should handle empty user list', async () => {
    const db = createMockDb([]);

    const result = await backfillAllSnapshots(db);

    expect(mockGetActiveCurrencies).not.toHaveBeenCalled();
    expect(mockGenerateAllSnapshots).not.toHaveBeenCalled();
    expect(result).toEqual({ processed: 0, failed: 0, total: 0 });
  });

  it('should fetch accounts per user, not globally', async () => {
    const db = createMockDb(['user-A', 'user-B']);

    await backfillAllSnapshots(db);

    expect(mockGetUserAccounts).toHaveBeenCalledWith('user-A');
    expect(mockGetUserAccounts).toHaveBeenCalledWith('user-B');
    expect(mockGetUserAccounts).toHaveBeenCalledTimes(2);
  });
});
