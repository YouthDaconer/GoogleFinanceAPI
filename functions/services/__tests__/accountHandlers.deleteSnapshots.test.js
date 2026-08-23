/**
 * PERF-SNAP-028: Tests para cleanup de snapshots en deletePortfolioAccount
 * (Vector 2: Event-driven cleanup al eliminar cuenta)
 * 
 * @see docs/stories/PERF-SNAP-028.story.md (AC5, AC6)
 */

const { HttpsError } = require("firebase-functions/v2/https");

// ============================================================================
// MOCKS
// ============================================================================

const mockBatchDelete = jest.fn();
const mockBatchCommit = jest.fn().mockResolvedValue();
const mockBatch = jest.fn(() => ({
  delete: mockBatchDelete,
  commit: mockBatchCommit,
}));

const mockSnapshotsGet = jest.fn();
const mockAccountGet = jest.fn();
const mockAssetsGet = jest.fn();
const mockTransactionsGet = jest.fn();
const mockDistributionGet = jest.fn();
const mockDocDelete = jest.fn().mockResolvedValue();
const mockDocUpdate = jest.fn().mockResolvedValue();

const mockWhere = jest.fn();

const mockDocInstance = (getResult) => ({
  get: jest.fn().mockResolvedValue(getResult),
  delete: mockDocDelete,
  update: mockDocUpdate,
});

jest.mock("firebase-admin/firestore", () => {
  const FieldValue = {
    serverTimestamp: () => "SERVER_TIMESTAMP",
    delete: () => "FIELD_DELETE",
  };

  // Route queries based on collection name
  const getFirestore = () => ({
    collection: jest.fn((name) => {
      if (name === 'portfolioAccounts') {
        return {
          doc: jest.fn(() => ({
            get: mockAccountGet,
            delete: mockDocDelete,
          })),
        };
      }
      if (name === 'assets') {
        // FIX-DELETE-002: la consulta de assets ya no encadena un filtro por
        // userId, asi que `get` cuelga del primer where. Se deja tambien tras un
        // segundo where para no acoplar este test a la forma de la consulta.
        return {
          where: jest.fn(() => ({
            get: mockAssetsGet,
            where: jest.fn(() => ({
              get: mockAssetsGet,
            })),
          })),
        };
      }
      if (name === 'transactions') {
        return {
          where: jest.fn(() => ({
            get: mockTransactionsGet,
          })),
        };
      }
      if (name === 'portfolioDistribution') {
        return {
          doc: jest.fn(() => ({
            get: mockDistributionGet,
            update: mockDocUpdate,
          })),
        };
      }
      if (name === 'performanceSnapshots') {
        return {
          where: jest.fn(() => ({
            where: jest.fn(() => ({
              get: mockSnapshotsGet,
            })),
          })),
        };
      }
      return { doc: jest.fn(() => ({ get: jest.fn(), set: jest.fn() })) };
    }),
    batch: mockBatch,
  });

  return { getFirestore, FieldValue };
});

jest.mock("../portfolioDistributionService", () => ({
  invalidateDistributionCache: jest.fn(),
}));

jest.mock("../helpers/subscriptionValidator", () => ({
  validateQuantityLimit: jest.fn(),
  validateFeatureAccess: jest.fn(),
}));

const { deletePortfolioAccount } = require("../handlers/accountHandlers");

// ============================================================================
// HELPERS
// ============================================================================

function setupSuccessfulDelete(accountId = 'acc-123', userId = 'user-1') {
  mockAccountGet.mockResolvedValue({
    exists: true,
    data: () => ({ userId, name: 'Test Account' }),
  });
  mockAssetsGet.mockResolvedValue({ empty: true, docs: [] });
  mockTransactionsGet.mockResolvedValue({ empty: true, docs: [] });
  mockDistributionGet.mockResolvedValue({ exists: false });
}

// ============================================================================
// TESTS
// ============================================================================

describe('PERF-SNAP-028 Vector 2: deletePortfolioAccount snapshot cleanup', () => {
  const context = { auth: { uid: 'user-1' } };
  const payload = { accountId: 'acc-123' };

  beforeEach(() => {
    jest.clearAllMocks();
    setupSuccessfulDelete();
  });

  test('deletes snapshots for the deleted account (AC5)', async () => {
    const snapshotDocs = [
      { ref: { delete: jest.fn() } },
      { ref: { delete: jest.fn() } },
    ];
    mockSnapshotsGet.mockResolvedValue({
      empty: false,
      docs: snapshotDocs,
      size: 2,
    });

    const result = await deletePortfolioAccount(context, payload);

    expect(result.success).toBe(true);
    expect(mockBatch).toHaveBeenCalled();
    expect(mockBatchCommit).toHaveBeenCalled();
  });

  test('snapshot cleanup failure does NOT block account deletion (AC6)', async () => {
    mockSnapshotsGet.mockRejectedValue(new Error('Firestore unavailable'));

    const result = await deletePortfolioAccount(context, payload);

    expect(result.success).toBe(true);
    expect(result.accountId).toBe('acc-123');
  });

  test('no snapshots for account does not cause error', async () => {
    mockSnapshotsGet.mockResolvedValue({ empty: true, docs: [], size: 0 });

    const result = await deletePortfolioAccount(context, payload);

    expect(result.success).toBe(true);
    // batch should not be created for snapshot cleanup when empty
    // note: batch may be called for asset/transaction cleanup
  });
});
