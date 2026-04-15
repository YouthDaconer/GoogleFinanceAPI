/**
 * PERF-SNAP-028: Tests para snapshotCleanup
 * 
 * Vector 1: cleanupSoldAssetSnapshots
 * Vector 3: archiveInactiveUserSnapshots / archiveUserSnapshots
 * 
 * @see docs/stories/PERF-SNAP-028.story.md
 */

// ============================================================================
// MOCKS
// ============================================================================

const mockBatchSet = jest.fn();
const mockBatchDelete = jest.fn();
const mockBatchCommit = jest.fn().mockResolvedValue();
const mockBatch = jest.fn(() => ({
  set: mockBatchSet,
  delete: mockBatchDelete,
  commit: mockBatchCommit,
}));

let mockGetResults = [];
let mockGetCallIndex = 0;

const mockGet = jest.fn(() => {
  const result = mockGetResults[mockGetCallIndex] || buildQueryResult([]);
  mockGetCallIndex++;
  return Promise.resolve(result);
});

function setupMockGetSequence(...results) {
  mockGetResults = results;
  mockGetCallIndex = 0;
}

const mockStartAfter = jest.fn();
const mockLimit = jest.fn();
const mockOrderBy = jest.fn();
const mockWhere = jest.fn();

function resetChain() {
  mockGet.mockImplementation(() => {
    const result = mockGetResults[mockGetCallIndex] || buildQueryResult([]);
    mockGetCallIndex++;
    return Promise.resolve(result);
  });
  mockWhere.mockImplementation(() => ({
    where: mockWhere,
    orderBy: mockOrderBy,
    limit: mockLimit,
    get: mockGet,
  }));
  mockOrderBy.mockImplementation(() => ({
    limit: mockLimit,
  }));
  mockLimit.mockImplementation(() => ({
    get: mockGet,
    startAfter: mockStartAfter,
  }));
  mockStartAfter.mockImplementation(() => ({
    get: mockGet,
  }));
}

const mockDocSet = jest.fn().mockResolvedValue();

const mockCollection = jest.fn((name) => ({
  where: mockWhere,
  doc: jest.fn((id) => ({
    get: mockGet,
    set: mockDocSet,
    delete: jest.fn().mockResolvedValue(),
    id,
  })),
}));

const mockDocRef = jest.fn((path) => ({
  get: mockGet,
  set: mockDocSet,
}));

const mockFirestore = {
  collection: mockCollection,
  doc: mockDocRef,
  batch: mockBatch,
};

jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => mockFirestore,
}));

jest.mock('firebase-functions/v2/scheduler', () => ({
  onSchedule: jest.fn((opts, handler) => handler),
}));

jest.mock('../firebaseAdmin', () => ({
  auth: jest.fn(() => ({})),
}));

jest.mock('luxon', () => ({
  DateTime: {
    now: () => ({
      setZone: () => ({
        minus: ({ days }) => ({
          toISODate: () => '2025-04-14',
        }),
        toISODate: () => '2026-04-14',
      }),
    }),
  },
}));

const {
  cleanupSoldAssetSnapshots,
  archiveInactiveUserSnapshots,
  archiveUserSnapshots,
  QUERY_BATCH_SIZE,
  INACTIVE_USER_DAYS,
} = require('../snapshotCleanup');

// ============================================================================
// HELPERS
// ============================================================================

function buildAssetDoc(id, { timeline, userId = 'user-1', ticker = 'AAPL' } = {}) {
  return {
    id,
    data: () => ({ userId, ticker, type: 'asset', timeline: timeline || [] }),
    ref: { delete: jest.fn().mockResolvedValue() },
  };
}

function buildQueryResult(docs) {
  return {
    empty: docs.length === 0,
    docs,
    size: docs.length,
  };
}

function buildSnapshotDoc(id, data) {
  return {
    id,
    data: () => data,
    ref: { delete: jest.fn().mockResolvedValue() },
  };
}

// ============================================================================
// VECTOR 1: cleanupSoldAssetSnapshots
// ============================================================================

describe('PERF-SNAP-028 Vector 1: cleanupSoldAssetSnapshots', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockGetResults = [];
    mockGetCallIndex = 0;
    resetChain();
  });

  test('deletes asset sold >365 days ago with no re-purchase (AC1, AC2)', async () => {
    const doc = buildAssetDoc('snap-1', {
      timeline: [
        { d: '2024-01-15', v: 1000, c: 0.5, u: 10 },
        { d: '2024-06-01', v: 500, c: -0.2, u: 5 },
        { d: '2024-10-01', v: 0, c: -1, u: 0 },
      ],
    });

    setupMockGetSequence(
      buildQueryResult([doc]),   // performanceSnapshots query
      buildQueryResult([]),      // transactions query (no re-buys)
    );

    const result = await cleanupSoldAssetSnapshots(mockFirestore);

    expect(result.deleted).toBe(1);
    expect(result.scanned).toBe(1);
    expect(doc.ref.delete).toHaveBeenCalledTimes(1);
  });

  test('does NOT delete asset sold <365 days ago (AC3)', async () => {
    // cutoffDate is '2025-04-14', so lastActive on 2025-08-01 > cutoff → skip
    const doc = buildAssetDoc('snap-2', {
      timeline: [
        { d: '2025-08-01', v: 1000, c: 0.5, u: 10 },
        { d: '2025-12-01', v: 0, c: -1, u: 0 },
      ],
    });

    setupMockGetSequence(buildQueryResult([doc]));

    const result = await cleanupSoldAssetSnapshots(mockFirestore);

    expect(result.deleted).toBe(0);
    expect(result.skipped).toBe(1);
    expect(doc.ref.delete).not.toHaveBeenCalled();
  });

  test('does NOT delete asset with u > 0 (active/re-purchased) (AC4)', async () => {
    const doc = buildAssetDoc('snap-3', {
      timeline: [
        { d: '2023-01-01', v: 1000, c: 0.5, u: 10 },
        { d: '2023-06-01', v: 0, c: -1, u: 0 },
        { d: '2024-01-01', v: 2000, c: 1.0, u: 20 },
      ],
    });

    setupMockGetSequence(buildQueryResult([doc]));

    const result = await cleanupSoldAssetSnapshots(mockFirestore);

    expect(result.deleted).toBe(0);
    expect(result.skipped).toBe(1);
    expect(doc.ref.delete).not.toHaveBeenCalled();
  });

  test('does NOT delete asset with re-purchase transaction (AC4)', async () => {
    const doc = buildAssetDoc('snap-4', {
      timeline: [
        { d: '2024-01-01', v: 1000, c: 0.5, u: 10 },
        { d: '2024-06-01', v: 0, c: -1, u: 0 },
      ],
    });

    setupMockGetSequence(
      buildQueryResult([doc]),
      buildQueryResult([{ id: 'tx-1' }]),   // transactions: has a buy
    );

    const result = await cleanupSoldAssetSnapshots(mockFirestore);

    expect(result.deleted).toBe(0);
    expect(result.skipped).toBe(1);
    expect(doc.ref.delete).not.toHaveBeenCalled();
  });

  test('handles pagination with startAfter when >BATCH_SIZE docs', async () => {
    const largeBatch = Array.from({ length: QUERY_BATCH_SIZE }, (_, i) =>
      buildAssetDoc(`snap-page-${i}`, {
        timeline: [{ d: '2026-01-01', v: 1000, c: 0.5, u: 10 }],
      })
    );
    const secondBatch = [
      buildAssetDoc('snap-final', {
        timeline: [{ d: '2026-01-01', v: 1000, c: 0.5, u: 10 }],
      }),
    ];

    setupMockGetSequence(
      buildQueryResult(largeBatch),
      buildQueryResult(secondBatch),
    );

    const result = await cleanupSoldAssetSnapshots(mockFirestore);

    expect(result.scanned).toBe(QUERY_BATCH_SIZE + 1);
    expect(result.skipped).toBe(QUERY_BATCH_SIZE + 1);
    expect(mockStartAfter).toHaveBeenCalled();
  });

  test('skips doc with empty timeline without error', async () => {
    const doc = buildAssetDoc('snap-empty', { timeline: [] });

    setupMockGetSequence(buildQueryResult([doc]));

    const result = await cleanupSoldAssetSnapshots(mockFirestore);

    expect(result.deleted).toBe(0);
    expect(result.scanned).toBe(1);
    expect(doc.ref.delete).not.toHaveBeenCalled();
  });

  test('returns zeros when no asset snapshots exist', async () => {
    setupMockGetSequence(buildQueryResult([]));

    const result = await cleanupSoldAssetSnapshots(mockFirestore);

    expect(result).toEqual({ scanned: 0, deleted: 0, skipped: 0 });
  });
});

// ============================================================================
// VECTOR 3: archiveInactiveUserSnapshots
// ============================================================================

describe('PERF-SNAP-028 Vector 3: archiveInactiveUserSnapshots', () => {
  const cutoffMs = Date.now() - (INACTIVE_USER_DAYS * 24 * 60 * 60 * 1000);

  function buildUser(uid, lastSignInTime) {
    return { uid, metadata: { lastSignInTime } };
  }

  function buildListUsersResult(users, pageToken = undefined) {
    return { users, pageToken };
  }

  let mockAuth;

  beforeEach(() => {
    jest.clearAllMocks();
    mockGetResults = [];
    mockGetCallIndex = 0;
    resetChain();
    mockAuth = { listUsers: jest.fn() };
  });

  test('archives user inactive >365 days with snapshots (AC7, AC8, AC9)', async () => {
    const inactiveUser = buildUser('inactive-1', new Date(cutoffMs - 86400000).toISOString());
    mockAuth.listUsers.mockResolvedValueOnce(buildListUsersResult([inactiveUser]));

    const snapshotDoc = buildSnapshotDoc('snap-inactive-1', {
      userId: 'inactive-1', currency: 'USD', timeline: [],
    });

    setupMockGetSequence(
      buildQueryResult([snapshotDoc]),   // probe: has snapshots
      { exists: false },                 // userData: not archived
      buildQueryResult([snapshotDoc]),   // archiveUserSnapshots: get all
    );

    const result = await archiveInactiveUserSnapshots(mockFirestore, mockAuth);

    expect(result.archived).toBe(1);
    expect(result.errors).toBe(0);
    expect(mockBatchCommit).toHaveBeenCalled();
  });

  test('does NOT archive user active <365 days', async () => {
    const activeUser = buildUser('active-1', new Date().toISOString());
    mockAuth.listUsers.mockResolvedValueOnce(buildListUsersResult([activeUser]));

    const result = await archiveInactiveUserSnapshots(mockFirestore, mockAuth);

    expect(result.archived).toBe(0);
    expect(result.errors).toBe(0);
  });

  test('archives user with null lastSignInTime', async () => {
    const nullUser = buildUser('null-signin', null);
    mockAuth.listUsers.mockResolvedValueOnce(buildListUsersResult([nullUser]));

    const snapshotDoc = buildSnapshotDoc('snap-null-1', {
      userId: 'null-signin', currency: 'USD', timeline: [],
    });

    setupMockGetSequence(
      buildQueryResult([snapshotDoc]),
      { exists: false },
      buildQueryResult([snapshotDoc]),
    );

    const result = await archiveInactiveUserSnapshots(mockFirestore, mockAuth);

    expect(result.archived).toBe(1);
  });

  test('skips user already archived (snapshotsArchived: true)', async () => {
    const inactiveUser = buildUser('archived-1', new Date(cutoffMs - 86400000).toISOString());
    mockAuth.listUsers.mockResolvedValueOnce(buildListUsersResult([inactiveUser]));

    setupMockGetSequence(
      buildQueryResult([{ id: 'x' }]),
      { exists: true, data: () => ({ snapshotsArchived: true }) },
    );

    const result = await archiveInactiveUserSnapshots(mockFirestore, mockAuth);

    expect(result.archived).toBe(0);
  });

  test('skips user without snapshots', async () => {
    const inactiveUser = buildUser('no-snaps', new Date(cutoffMs - 86400000).toISOString());
    mockAuth.listUsers.mockResolvedValueOnce(buildListUsersResult([inactiveUser]));

    setupMockGetSequence(buildQueryResult([]));

    const result = await archiveInactiveUserSnapshots(mockFirestore, mockAuth);

    expect(result.archived).toBe(0);
  });

  test('error archiving one user does not affect others', async () => {
    const user1 = buildUser('fail-user', new Date(cutoffMs - 86400000).toISOString());
    const user2 = buildUser('ok-user', new Date(cutoffMs - 86400000).toISOString());
    mockAuth.listUsers.mockResolvedValueOnce(buildListUsersResult([user1, user2]));

    const snapshotDoc2 = buildSnapshotDoc('snap-ok', {
      userId: 'ok-user', currency: 'USD', timeline: [],
    });

    // Custom mockGet for this test with error on 3rd call
    let callIdx = 0;
    const sequence = [
      buildQueryResult([{ id: 'x' }]),     // user1 probe
      { exists: false },                    // user1 userData
      'ERROR',                              // user1 archiveUserSnapshots → fails
      buildQueryResult([snapshotDoc2]),      // user2 probe
      { exists: false },                    // user2 userData
      buildQueryResult([snapshotDoc2]),      // user2 archive
    ];
    mockGet.mockImplementation(() => {
      const val = sequence[callIdx++];
      if (val === 'ERROR') return Promise.reject(new Error('Firestore unavailable'));
      return Promise.resolve(val || buildQueryResult([]));
    });

    const result = await archiveInactiveUserSnapshots(mockFirestore, mockAuth);

    expect(result.archived).toBe(1);
    expect(result.errors).toBe(1);
  });

  test('handles pagination with pageToken', async () => {
    const user1 = buildUser('page1-user', new Date().toISOString());
    const user2 = buildUser('page2-user', new Date().toISOString());

    mockAuth.listUsers
      .mockResolvedValueOnce(buildListUsersResult([user1], 'next-page'))
      .mockResolvedValueOnce(buildListUsersResult([user2]));

    const result = await archiveInactiveUserSnapshots(mockFirestore, mockAuth);

    expect(mockAuth.listUsers).toHaveBeenCalledTimes(2);
    expect(mockAuth.listUsers).toHaveBeenCalledWith(1000, 'next-page');
  });
});

// ============================================================================
// VECTOR 3: archiveUserSnapshots
// ============================================================================

describe('PERF-SNAP-028 Vector 3: archiveUserSnapshots', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockGetResults = [];
    mockGetCallIndex = 0;
    resetChain();
  });

  test('archived doc contains archivedAt, originalDocId and original data', async () => {
    const originalData = { userId: 'u1', currency: 'USD', timeline: [{ d: '2025-01-01', v: 100 }] };
    const snapshotDoc = buildSnapshotDoc('snap-archive-test', originalData);

    setupMockGetSequence(buildQueryResult([snapshotDoc]));

    await archiveUserSnapshots(mockFirestore, 'u1');

    expect(mockBatchSet).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        ...originalData,
        archivedAt: expect.any(String),
        originalDocId: 'snap-archive-test',
      })
    );
    expect(mockBatchDelete).toHaveBeenCalled();
    expect(mockBatchCommit).toHaveBeenCalled();
  });

  test('splits batches when snapshots exceed 249 docs', async () => {
    const docs = Array.from({ length: 260 }, (_, i) =>
      buildSnapshotDoc(`snap-batch-${i}`, { userId: 'batch-user', currency: 'USD' })
    );

    setupMockGetSequence(buildQueryResult(docs));

    await archiveUserSnapshots(mockFirestore, 'batch-user');

    // 249 docs × 2 ops = 498. 250th → 500 → commit + new batch. 11 remaining → final commit.
    expect(mockBatchCommit).toHaveBeenCalledTimes(2);
  });

  test('marks user as archived in userData', async () => {
    const snapshotDoc = buildSnapshotDoc('snap-mark', { userId: 'mark-user', currency: 'USD' });

    setupMockGetSequence(buildQueryResult([snapshotDoc]));

    await archiveUserSnapshots(mockFirestore, 'mark-user');

    expect(mockDocRef).toHaveBeenCalledWith('userData/mark-user');
    expect(mockDocSet).toHaveBeenCalledWith(
      expect.objectContaining({
        snapshotsArchived: true,
        snapshotsArchivedAt: expect.any(String),
      }),
      { merge: true }
    );
  });

  test('does nothing for user with no snapshots', async () => {
    setupMockGetSequence(buildQueryResult([]));

    await archiveUserSnapshots(mockFirestore, 'empty-user');

    expect(mockBatchCommit).not.toHaveBeenCalled();
  });
});
