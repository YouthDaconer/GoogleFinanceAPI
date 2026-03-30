/**
 * Tests for capturePreBackfillSnapshot (SCALE-003)
 * 
 * @module __tests__/services/backfillCoreModule.snapshot.test
 * @see docs/stories/SCALE-003.story.md
 */

// Mock firebaseAdmin before importing
const mockGet = jest.fn();
const mockSet = jest.fn().mockResolvedValue();
const mockWhere = jest.fn();
const mockDoc = jest.fn();
const mockCollection = jest.fn();

jest.mock('../firebaseAdmin', () => {
  mockWhere.mockReturnThis();
  
  mockDoc.mockReturnValue({
    collection: mockCollection,
    set: mockSet,
  });

  const collectionReturn = {
    where: mockWhere,
    get: mockGet,
    doc: mockDoc,
  };

  mockCollection.mockReturnValue(collectionReturn);

  return {
    firestore: jest.fn(() => ({
      collection: mockCollection,
      doc: mockDoc,
    })),
  };
});

// Mock node-fetch
jest.mock('node-fetch', () => {
  const fn = jest.fn().mockResolvedValue({ json: () => ({}) });
  fn.default = fn;
  return fn;
});

const { capturePreBackfillSnapshot } = require('../backfillCoreModule');

// ============================================================================
// HELPERS
// ============================================================================

function buildFirestoreDoc(id, data) {
  return {
    id,
    data: () => data,
    ref: { path: `portfolioPerformance/user1/dates/${id}` },
  };
}

function buildEmptySnapshot() {
  return { empty: true, docs: [] };
}

function buildSnapshot(docs) {
  return { empty: docs.length === 0, docs };
}

// ============================================================================
// TESTS
// ============================================================================

describe('capturePreBackfillSnapshot', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('returns null when no existing documents', async () => {
    mockGet.mockResolvedValue(buildEmptySnapshot());

    const result = await capturePreBackfillSnapshot(
      'user1',
      [{ id: 'acc1' }],
      ['2026-03-01', '2026-03-05']
    );

    expect(result).toBeNull();
  });

  test('returns correct structure with accounts and overall', async () => {
    const accountDoc = {
      id: '2026-03-01',
      data: () => ({ date: '2026-03-01', USD: { totalValue: 1000 } }),
      ref: { path: 'portfolioPerformance/user1/accounts/acc1/dates/2026-03-01' },
    };
    const overallDoc = {
      id: '2026-03-01',
      data: () => ({ date: '2026-03-01', USD: { totalValue: 1000 } }),
      ref: { path: 'portfolioPerformance/user1/dates/2026-03-01' },
    };

    // First call: account query returns 1 doc
    // Second call: overall query returns 1 doc
    mockGet
      .mockResolvedValueOnce(buildSnapshot([accountDoc]))
      .mockResolvedValueOnce(buildSnapshot([overallDoc]));

    const result = await capturePreBackfillSnapshot(
      'user1',
      [{ id: 'acc1' }],
      ['2026-03-01', '2026-03-05']
    );

    expect(result).not.toBeNull();
    expect(result.userId).toBe('user1');
    expect(result.tradingDays).toEqual(['2026-03-01', '2026-03-05']);
    expect(result.source).toBe('reconcileStalePerformance');
    expect(result.totalDocs).toBe(2);
    expect(result.snapshotAt).toBeDefined();
    expect(new Date(result.snapshotAt).getTime()).not.toBeNaN();
    expect(result.accounts.acc1).toBeDefined();
    expect(result.accounts.acc1['2026-03-01']).toEqual({ date: '2026-03-01', USD: { totalValue: 1000 } });
    expect(result.overall['2026-03-01']).toEqual({ date: '2026-03-01', USD: { totalValue: 1000 } });
  });

  test('truncates to OVERALL only when estimated size exceeds 800 KB', async () => {
    // AVG_DOC_SIZE_BYTES = 12 * 1024. Need > 800KB / 12KB ≈ 67 docs to trigger truncation
    const manyAccountDocs = Array.from({ length: 68 }, (_, i) => ({
      id: `2026-03-${String(i + 1).padStart(2, '0')}`,
      data: () => ({ date: `2026-03-${String(i + 1).padStart(2, '0')}`, USD: { totalValue: 1000 } }),
    }));

    const overallDoc = {
      id: '2026-03-01',
      data: () => ({ date: '2026-03-01', USD: { totalValue: 1000 } }),
      ref: { path: 'portfolioPerformance/user1/dates/2026-03-01' },
    };

    mockGet
      .mockResolvedValueOnce(buildSnapshot(manyAccountDocs))
      .mockResolvedValueOnce(buildSnapshot([overallDoc]));

    const result = await capturePreBackfillSnapshot(
      'user1',
      [{ id: 'acc1' }],
      ['2026-03-01', '2026-03-05']
    );

    expect(result).not.toBeNull();
    expect(result.truncated).toBe(true);
    expect(result.affectedAccounts).toEqual(['acc1']);
    expect(Object.keys(result.accounts)).toHaveLength(0);
    expect(result.overall['2026-03-01']).toBeDefined();
  });

  test('handles multiple accounts correctly', async () => {
    const acc1Doc = {
      id: '2026-03-01',
      data: () => ({ date: '2026-03-01', USD: { totalValue: 500 } }),
    };
    const acc2Doc = {
      id: '2026-03-01',
      data: () => ({ date: '2026-03-01', USD: { totalValue: 700 } }),
    };
    const overallDoc = {
      id: '2026-03-01',
      data: () => ({ date: '2026-03-01', USD: { totalValue: 1200 } }),
    };

    mockGet
      .mockResolvedValueOnce(buildSnapshot([acc1Doc]))
      .mockResolvedValueOnce(buildSnapshot([acc2Doc]))
      .mockResolvedValueOnce(buildSnapshot([overallDoc]));

    const result = await capturePreBackfillSnapshot(
      'user1',
      [{ id: 'acc1' }, { id: 'acc2' }],
      ['2026-03-01']
    );

    expect(result.totalDocs).toBe(3);
    expect(result.accounts.acc1).toBeDefined();
    expect(result.accounts.acc2).toBeDefined();
    expect(result.overall).toBeDefined();
  });
});
