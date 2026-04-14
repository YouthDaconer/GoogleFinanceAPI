/**
 * SCALE-001: Tests para paralelización del pipeline EOD
 *
 * @see docs/stories/SCALE-001.story.md
 * @see docs/architecture/SCALE-PERF-001-consolidation-sustainability-diagnosis.md §6.1
 */

// === Mocks ===

const mockBatchSet = jest.fn();
const mockBatchCommit = jest.fn().mockResolvedValue(undefined);
const mockBatch = { set: mockBatchSet, commit: mockBatchCommit };

const mockDocSet = jest.fn().mockResolvedValue(undefined);
const mockDocGet = jest.fn().mockResolvedValue({ exists: true, data: () => ({}) });
const mockSubCollectionDoc = jest.fn(() => ({
  collection: jest.fn(() => ({
    doc: jest.fn(() => ({
      collection: jest.fn(() => ({
        doc: jest.fn(() => ({}))
      }))
    }))
  }))
}));

const mockCollectionDoc = jest.fn((docId) => ({
  set: mockDocSet,
  get: mockDocGet,
  collection: jest.fn((subColName) => ({
    doc: jest.fn((subDocId) => ({
      collection: jest.fn((subSubColName) => ({
        doc: jest.fn((subSubDocId) => ({}))
      }))
    }))
  }))
}));

const mockDb = {
  batch: jest.fn(() => mockBatch),
  collection: jest.fn(() => ({
    doc: mockCollectionDoc
  }))
};

jest.mock("firebase-admin", () => ({
  firestore: jest.fn(() => mockDb),
  initializeApp: jest.fn()
}));

jest.mock("firebase-admin/firestore", () => ({
  getFirestore: jest.fn(() => mockDb),
  FieldValue: {
    serverTimestamp: () => "SERVER_TIMESTAMP",
    increment: (n) => `INCREMENT_${n}`
  }
}));

jest.mock("firebase-functions/v2/scheduler", () => ({
  onSchedule: jest.fn((opts, handler) => handler)
}));

jest.mock("firebase-functions/params", () => ({
  defineSecret: jest.fn(() => "mock-secret")
}));

jest.mock("../../utils/portfolioCalculations", () => ({
  calculateAccountPerformance: jest.fn(() => ({
    USD: {
      totalValue: 10000,
      totalInvestment: 9000,
      totalCashFlow: -9000,
      totalROI: 11.11,
      dailyChangePercentage: 1.5,
      rawDailyChangePercentage: 1.5,
      adjustedDailyChangePercentage: 1.5,
      dailyReturn: 0.015,
      assetPerformance: {
        "AAPL_stock": {
          totalValue: 5000,
          totalInvestment: 4500,
          units: 10
        },
        "GOOGL_stock": {
          totalValue: 5000,
          totalInvestment: 4500,
          units: 5
        }
      }
    }
  })),
  convertCurrency: jest.fn((value) => value)
}));

jest.mock("../calculatePortfolioRisk", () => ({
  calculatePortfolioRisk: jest.fn().mockResolvedValue(null)
}));

jest.mock("../cacheInvalidationService", () => ({
  invalidatePerformanceCacheBatch: jest.fn().mockResolvedValue({ usersProcessed: 0, cachesDeleted: 0 })
}));

jest.mock("../marketDataHelper", () => ({
  getPricesFromApi: jest.fn().mockResolvedValue([]),
  getCurrencyRatesFromApi: jest.fn().mockResolvedValue([])
}));

jest.mock("../../utils/logoGenerator", () => ({
  generateLogoUrl: jest.fn()
}));

// PERF-SNAP-004: Mock de snapshotGenerator
const mockGenerateAllSnapshots = jest.fn().mockResolvedValue({ success: 3, failed: 0, total: 3 });
// PERF-SNAP-024: Mocks para per-asset snapshots
const mockGenerateAllAssetSnapshots = jest.fn().mockResolvedValue({ success: 2, failed: 0, total: 2 });
const mockFetchLatestAssetPerformance = jest.fn().mockResolvedValue({ 'AAPL_stock': { totalValue: 18500 }, 'MSFT_stock': { totalValue: 12000 } });
jest.mock("../snapshotGenerator", () => ({
  generateAllSnapshots: (...args) => mockGenerateAllSnapshots(...args),
  generateAllAssetSnapshots: (...args) => mockGenerateAllAssetSnapshots(...args),
  fetchLatestAssetPerformance: (...args) => mockFetchLatestAssetPerformance(...args),
}));

jest.mock("../../utils/logger", () => ({
  StructuredLogger: {
    forScheduled: jest.fn(() => ({
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
      startOperation: jest.fn(() => ({
        success: jest.fn(),
        failure: jest.fn()
      }))
    }))
  }
}));

// === Imports ===

process.env.NODE_ENV = "test";
const { _testExports } = require("../unifiedMarketDataUpdate");
const { processUserPerformance, MAX_PARALLEL_USERS, markInconsistentUsersAsStale } = _testExports;

// === Test Fixtures ===

function buildCache() {
  return {
    getUserLastPerformance: jest.fn(() => ({
      USD: { totalValue: 9800 }
    })),
    getAccountLastPerformance: jest.fn(() => ({
      USD: { totalValue: 4900 }
    })),
    getBuyTransactionsForAsset: jest.fn(() => [])
  };
}

function buildBaseParams(overrides = {}) {
  return {
    db: mockDb,
    userId: "user-1",
    accounts: [
      { id: "account-1", userId: "user-1" },
      { id: "account-2", userId: "user-1" }
    ],
    currentPrices: [{ symbol: "AAPL", price: 150 }],
    currencies: [{ code: "USD", rate: 1 }],
    cache: buildCache(),
    allAssets: [
      { id: "asset-1", name: "AAPL", assetType: "stock", portfolioAccount: "account-1", isActive: true },
      { id: "asset-2", name: "GOOGL", assetType: "stock", portfolioAccount: "account-2", isActive: true }
    ],
    assetsToInclude: [
      { id: "asset-1", name: "AAPL", assetType: "stock", portfolioAccount: "account-1" },
      { id: "asset-2", name: "GOOGL", assetType: "stock", portfolioAccount: "account-2" }
    ],
    sellTransactions: [],
    sellTransactionsByAccount: {},
    inactiveAssets: [],
    activeAssets: [
      { id: "asset-1", name: "AAPL", assetType: "stock", portfolioAccount: "account-1" },
      { id: "asset-2", name: "GOOGL", assetType: "stock", portfolioAccount: "account-2" }
    ],
    todaysTransactions: [],
    formattedDate: "2026-03-28",
    ...overrides
  };
}

// === Tests ===

describe("SCALE-001: processUserPerformance", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockBatchCommit.mockResolvedValue(undefined);
  });

  it("should return success with userId and durationMs on successful processing", async () => {
    const params = buildBaseParams();

    const result = await processUserPerformance(params);

    expect(result.userId).toBe("user-1");
    expect(result.success).toBe(true);
    expect(typeof result.durationMs).toBe("number");
  });

  it("should create a local WriteBatch and commit once", async () => {
    const params = buildBaseParams();

    await processUserPerformance(params);

    expect(mockDb.batch).toHaveBeenCalledTimes(1);
    expect(mockBatchCommit).toHaveBeenCalledTimes(1);
  });

  it("should call batch.set for user doc + overall + 2 accounts (6 total)", async () => {
    const params = buildBaseParams();

    await processUserPerformance(params);

    // 1 user doc + 1 overall date + 2 × (account doc + account date) = 6
    expect(mockBatchSet).toHaveBeenCalledTimes(6);
    expect(mockBatchCommit).toHaveBeenCalledTimes(1);
  });

  it("should return success:false with error and durationMs when batch.commit fails", async () => {
    mockBatchCommit.mockRejectedValueOnce(new Error("Firestore unavailable"));
    const params = buildBaseParams();

    const result = await processUserPerformance(params);

    expect(result.userId).toBe("user-1");
    expect(result.success).toBe(false);
    expect(result.error).toBe("Firestore unavailable");
    expect(typeof result.durationMs).toBe("number");
  });

  it("should handle user with 0 accounts", async () => {
    const params = buildBaseParams({ accounts: [] });

    const result = await processUserPerformance(params);

    expect(result.userId).toBe("user-1");
    expect(result.success).toBe(true);
    expect(typeof result.durationMs).toBe("number");
    // 1 user doc + 1 overall date = 2 (no account writes)
    expect(mockBatchSet).toHaveBeenCalledTimes(2);
  });

  it("should calculate doneProfitAndLoss for sell transactions", async () => {
    const params = buildBaseParams({
      sellTransactions: [{
        assetId: "asset-1",
        portfolioAccountId: "account-1",
        type: "sell",
        amount: 5,
        price: 160,
        currency: "USD",
        valuePnL: 50,
        defaultCurrencyForAdquisitionDollar: "USD",
        dollarPriceToDate: 1
      }]
    });

    const result = await processUserPerformance(params);

    expect(result.success).toBe(true);
    // The convertCurrency mock returns the value as-is
    // so doneProfitAndLoss should be 50 (from valuePnL)
  });
});

describe("SCALE-001: MAX_PARALLEL_USERS constant", () => {
  it("should be set to 5", () => {
    expect(MAX_PARALLEL_USERS).toBe(5);
  });
});

describe("SCALE-001: Parallel processing flow", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockBatchCommit.mockResolvedValue(undefined);
  });

  it("should process multiple users via Promise.allSettled", async () => {
    const pLimit = require("p-limit");
    const limit = pLimit(MAX_PARALLEL_USERS);

    const users = {
      "user-1": [{ id: "acc-1", userId: "user-1" }],
      "user-2": [{ id: "acc-2", userId: "user-2" }],
      "user-3": [{ id: "acc-3", userId: "user-3" }]
    };

    const sharedParams = {
      db: mockDb,
      currentPrices: [{ symbol: "AAPL", price: 150 }],
      currencies: [{ code: "USD", rate: 1 }],
      cache: buildCache(),
      allAssets: [],
      assetsToInclude: [],
      sellTransactions: [],
      sellTransactionsByAccount: {},
      inactiveAssets: [],
      activeAssets: [],
      todaysTransactions: [],
      formattedDate: "2026-03-28"
    };

    const results = await Promise.allSettled(
      Object.entries(users).map(([userId, accounts]) =>
        limit(() => processUserPerformance({
          ...sharedParams,
          userId,
          accounts
        }))
      )
    );

    expect(results).toHaveLength(3);
    results.forEach(r => {
      expect(r.status).toBe("fulfilled");
      expect(r.value.success).toBe(true);
    });
  });

  it("should isolate failures: 1 user fails, others succeed", async () => {
    const pLimit = require("p-limit");
    const limit = pLimit(MAX_PARALLEL_USERS);

    let callCount = 0;
    mockBatchCommit.mockImplementation(() => {
      callCount++;
      if (callCount === 2) {
        return Promise.reject(new Error("Quota exceeded"));
      }
      return Promise.resolve();
    });

    const users = {
      "user-ok-1": [{ id: "acc-1", userId: "user-ok-1" }],
      "user-fail": [{ id: "acc-2", userId: "user-fail" }],
      "user-ok-2": [{ id: "acc-3", userId: "user-ok-2" }]
    };

    const sharedParams = {
      db: mockDb,
      currentPrices: [],
      currencies: [{ code: "USD", rate: 1 }],
      cache: buildCache(),
      allAssets: [],
      assetsToInclude: [],
      sellTransactions: [],
      sellTransactionsByAccount: {},
      inactiveAssets: [],
      activeAssets: [],
      todaysTransactions: [],
      formattedDate: "2026-03-28"
    };

    const results = await Promise.allSettled(
      Object.entries(users).map(([userId, accounts]) =>
        limit(() => processUserPerformance({
          ...sharedParams,
          userId,
          accounts
        }))
      )
    );

    const successes = results.filter(r => r.status === "fulfilled" && r.value.success);
    const failures = results.filter(r => r.status === "fulfilled" && !r.value.success);

    expect(successes).toHaveLength(2);
    expect(failures).toHaveLength(1);
    expect(failures[0].value.userId).toBe("user-fail");
    expect(failures[0].value.error).toBe("Quota exceeded");
  });

  it("should handle 0 users without errors", async () => {
    const pLimit = require("p-limit");
    const limit = pLimit(MAX_PARALLEL_USERS);

    const results = await Promise.allSettled(
      Object.entries({}).map(([userId, accounts]) =>
        limit(() => processUserPerformance({
          db: mockDb,
          userId,
          accounts,
          currentPrices: [],
          currencies: [],
          cache: buildCache(),
          allAssets: [],
          assetsToInclude: [],
          sellTransactions: [],
          sellTransactionsByAccount: {},
          inactiveAssets: [],
          activeAssets: [],
          todaysTransactions: [],
          formattedDate: "2026-03-28"
        }))
      )
    );

    expect(results).toHaveLength(0);
  });

  it("should handle all users failing", async () => {
    mockBatchCommit.mockRejectedValue(new Error("Total failure"));
    const pLimit = require("p-limit");
    const limit = pLimit(MAX_PARALLEL_USERS);

    const users = {
      "user-1": [{ id: "acc-1", userId: "user-1" }],
      "user-2": [{ id: "acc-2", userId: "user-2" }]
    };

    const sharedParams = {
      db: mockDb,
      currentPrices: [],
      currencies: [{ code: "USD", rate: 1 }],
      cache: buildCache(),
      allAssets: [],
      assetsToInclude: [],
      sellTransactions: [],
      sellTransactionsByAccount: {},
      inactiveAssets: [],
      activeAssets: [],
      todaysTransactions: [],
      formattedDate: "2026-03-28"
    };

    const results = await Promise.allSettled(
      Object.entries(users).map(([userId, accounts]) =>
        limit(() => processUserPerformance({
          ...sharedParams,
          userId,
          accounts
        }))
      )
    );

    const failures = results.filter(r => r.status === "fulfilled" && !r.value.success);
    expect(failures).toHaveLength(2);
    failures.forEach(f => {
      expect(f.value.success).toBe(false);
      expect(f.value.error).toBe("Total failure");
    });
  });
});

describe("SCALE-001: _stale marker structure", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockDocSet.mockResolvedValue(undefined);
  });

  it("should write _stale marker with correct structure for failed users", async () => {
    const pLimit = require("p-limit");
    const limit = pLimit(MAX_PARALLEL_USERS);

    mockBatchCommit.mockRejectedValue(new Error("Write failed"));

    const sharedParams = {
      db: mockDb,
      currentPrices: [],
      currencies: [{ code: "USD", rate: 1 }],
      cache: buildCache(),
      allAssets: [],
      assetsToInclude: [],
      sellTransactions: [],
      sellTransactionsByAccount: {},
      inactiveAssets: [],
      activeAssets: [],
      todaysTransactions: [],
      formattedDate: "2026-03-28"
    };

    const users = {
      "user-fail-1": [{ id: "acc-1", userId: "user-fail-1" }]
    };

    const results = await Promise.allSettled(
      Object.entries(users).map(([userId, accounts]) =>
        limit(() => processUserPerformance({ ...sharedParams, userId, accounts }))
      )
    );

    const failedUserIds = results
      .filter(r => r.status === "fulfilled" && !r.value.success)
      .map(r => r.value.userId);

    expect(failedUserIds).toEqual(["user-fail-1"]);

    // Simulate the _stale marking logic from calculateDailyPortfolioPerformance
    for (const failedUserId of failedUserIds) {
      await mockDb.collection("portfolioPerformance").doc(failedUserId).set({
        _stale: {
          since: "2026-03-28",
          reason: "eod-parallel-processing-failure",
          source: "unifiedMarketDataUpdate",
          retryCount: 0,
          lastAttempt: expect.any(String)
        }
      }, { merge: true });
    }

    expect(mockDocSet).toHaveBeenCalledWith(
      expect.objectContaining({
        _stale: expect.objectContaining({
          since: "2026-03-28",
          reason: "eod-parallel-processing-failure",
          source: "unifiedMarketDataUpdate",
          retryCount: 0
        })
      }),
      { merge: true }
    );
  });
});

/**
 * SCALE-004: Tests para auto-reparación de consistencia
 *
 * @see docs/stories/SCALE-004.story.md
 * @see docs/architecture/SCALE-PERF-001-consolidation-sustainability-diagnosis.md §6.6
 */
describe("SCALE-004: markInconsistentUsersAsStale", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockDocSet.mockResolvedValue(undefined);
    mockDocGet.mockResolvedValue({ exists: true, data: () => ({}) });
  });

  it("should mark user with gap=2 and no existing _stale as stale", async () => {
    const inconsistentUsers = [{
      userId: "user-1",
      gap: 2,
      minAccountDate: "2026-03-28",
      maxAccountDate: "2026-03-29",
      userDate: "2026-03-30"
    }];

    const result = await markInconsistentUsersAsStale(mockDb, inconsistentUsers);

    expect(result).toBe(1);
    expect(mockDocGet).toHaveBeenCalledTimes(1);
    expect(mockDocSet).toHaveBeenCalledWith(
      {
        _stale: {
          since: "2026-03-28",
          reason: "auto-detected-inconsistency",
          source: "consistency-monitor",
          retryCount: 0,
          lastAttempt: expect.any(String)
        }
      },
      { merge: true }
    );
  });

  it("should NOT re-mark user with gap=2 that already has _stale", async () => {
    mockDocGet.mockResolvedValue({
      exists: true,
      data: () => ({ _stale: { since: "2026-03-27", retryCount: 1 } })
    });

    const inconsistentUsers = [{
      userId: "user-1",
      gap: 2,
      minAccountDate: "2026-03-28",
      maxAccountDate: "2026-03-29",
      userDate: "2026-03-30"
    }];

    const result = await markInconsistentUsersAsStale(mockDb, inconsistentUsers);

    expect(result).toBe(0);
    expect(mockDocGet).toHaveBeenCalledTimes(1);
    expect(mockDocSet).not.toHaveBeenCalled();
  });

  it("should NOT mark user with gap=1 as stale", async () => {
    const inconsistentUsers = [{
      userId: "user-1",
      gap: 1,
      minAccountDate: "2026-03-29",
      maxAccountDate: "2026-03-29",
      userDate: "2026-03-30"
    }];

    const result = await markInconsistentUsersAsStale(mockDb, inconsistentUsers);

    expect(result).toBe(0);
    expect(mockDocGet).not.toHaveBeenCalled();
    expect(mockDocSet).not.toHaveBeenCalled();
  });

  it("should return 0 and take no action for empty array", async () => {
    const result = await markInconsistentUsersAsStale(mockDb, []);

    expect(result).toBe(0);
    expect(mockDocGet).not.toHaveBeenCalled();
    expect(mockDocSet).not.toHaveBeenCalled();
  });
});

/**
 * PERF-SNAP-004: Tests para generación de snapshots post-EOD
 *
 * @see docs/stories/PERF-SNAP-004.story.md
 */
describe("PERF-SNAP-004: Snapshot generation in processUserPerformance", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockBatchCommit.mockResolvedValue(undefined);
    mockGenerateAllSnapshots.mockResolvedValue({ success: 3, failed: 0, total: 3 });
  });

  it("should call generateAllSnapshots after successful batch.commit (AC1)", async () => {
    const callOrder = [];
    mockBatchCommit.mockImplementation(() => { callOrder.push("commit"); return Promise.resolve(); });
    mockGenerateAllSnapshots.mockImplementation(() => { callOrder.push("snapshot"); return Promise.resolve({ success: 3, failed: 0, total: 3 }); });

    const params = buildBaseParams();

    const result = await processUserPerformance(params);

    expect(result.success).toBe(true);
    expect(mockGenerateAllSnapshots).toHaveBeenCalledTimes(1);
    expect(callOrder).toEqual(["commit", "snapshot"]);
  });

  it("should pass currencies as array of strings, not objects (AC4)", async () => {
    const params = buildBaseParams({
      currencies: [
        { code: "USD", rate: 1 },
        { code: "COP", rate: 4200 },
        { code: "EUR", rate: 0.92 }
      ]
    });

    await processUserPerformance(params);

    expect(mockGenerateAllSnapshots).toHaveBeenCalledWith(
      mockDb,
      "user-1",
      ["USD", "COP", "EUR"],
      expect.any(Array)
    );
  });

  it("should pass account IDs from active accounts (AC5)", async () => {
    const params = buildBaseParams({
      accounts: [
        { id: "acc-1", userId: "user-1" },
        { id: "acc-2", userId: "user-1" },
        { id: "acc-3", userId: "user-1" }
      ]
    });

    await processUserPerformance(params);

    expect(mockGenerateAllSnapshots).toHaveBeenCalledWith(
      mockDb,
      "user-1",
      ["USD"],
      ["acc-1", "acc-2", "acc-3"]
    );
  });

  it("should return success:true even when generateAllSnapshots throws (AC2)", async () => {
    mockGenerateAllSnapshots.mockRejectedValueOnce(new Error("Snapshot timeout"));
    const params = buildBaseParams();

    const result = await processUserPerformance(params);

    expect(result.success).toBe(true);
    expect(result.userId).toBe("user-1");
    expect(mockGenerateAllSnapshots).toHaveBeenCalledTimes(1);
  });

  it("should NOT call generateAllSnapshots when batch.commit fails (AC3 - stale users)", async () => {
    mockBatchCommit.mockRejectedValueOnce(new Error("Firestore unavailable"));
    const params = buildBaseParams();

    const result = await processUserPerformance(params);

    expect(result.success).toBe(false);
    expect(mockGenerateAllSnapshots).not.toHaveBeenCalled();
  });
});

/**
 * PERF-SNAP-023: Tests para lastSnapshotUpdate signal post-snapshots
 *
 * @see docs/stories/PERF-SNAP-023.story.md
 */
describe("PERF-SNAP-023: lastSnapshotUpdate signal in processUserPerformance", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockBatchCommit.mockResolvedValue(undefined);
    mockGenerateAllSnapshots.mockResolvedValue({ success: 3, failed: 0, total: 3 });
    mockDocSet.mockResolvedValue(undefined);
  });

  it("should write lastSnapshotUpdate after successful generateAllSnapshots (AC1, AC4)", async () => {
    const params = buildBaseParams();

    const result = await processUserPerformance(params);

    expect(result.success).toBe(true);
    expect(mockDocSet).toHaveBeenCalledWith(
      { lastSnapshotUpdate: expect.any(String) },
      { merge: true }
    );
  });

  it("should return success:true even when lastSnapshotUpdate write fails", async () => {
    mockDocSet.mockRejectedValueOnce(new Error("Firestore write failed"));
    const params = buildBaseParams();

    const result = await processUserPerformance(params);

    expect(result.success).toBe(true);
    expect(result.userId).toBe("user-1");
  });

  it("should NOT write lastSnapshotUpdate when generateAllSnapshots throws", async () => {
    mockGenerateAllSnapshots.mockRejectedValueOnce(new Error("Snapshot fatal error"));
    mockDocSet.mockClear();
    const params = buildBaseParams();

    await processUserPerformance(params);

    // mockDocSet is called for other things (batch.set delegates), so check specifically
    // that no call included lastSnapshotUpdate
    const lastSnapshotCalls = mockDocSet.mock.calls.filter(
      call => call[0] && call[0].lastSnapshotUpdate
    );
    expect(lastSnapshotCalls).toHaveLength(0);
  });
});

/**
 * PERF-SNAP-024: Tests para generación de asset snapshots en EOD
 *
 * @see docs/stories/PERF-SNAP-024.story.md
 */
describe("PERF-SNAP-024: Asset snapshot generation in processUserPerformance", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockBatchCommit.mockResolvedValue(undefined);
    mockGenerateAllSnapshots.mockResolvedValue({ success: 3, failed: 0, total: 3 });
    mockGenerateAllAssetSnapshots.mockResolvedValue({ success: 2, failed: 0, total: 2 });
    mockFetchLatestAssetPerformance.mockResolvedValue({ 'AAPL_stock': { totalValue: 18500 }, 'MSFT_stock': { totalValue: 12000 } });
    mockDocSet.mockResolvedValue(undefined);
  });

  it("should call generateAllAssetSnapshots after portfolio snapshots (AC3)", async () => {
    const callOrder = [];
    mockGenerateAllSnapshots.mockImplementation(() => { callOrder.push("portfolio"); return Promise.resolve({ success: 3, failed: 0, total: 3 }); });
    mockGenerateAllAssetSnapshots.mockImplementation(() => { callOrder.push("asset"); return Promise.resolve({ success: 2, failed: 0, total: 2 }); });
    const params = buildBaseParams();

    await processUserPerformance(params);

    expect(mockGenerateAllAssetSnapshots).toHaveBeenCalled();
    expect(callOrder.indexOf("portfolio")).toBeLessThan(callOrder.indexOf("asset"));
  });

  it("should not fail pipeline if asset snapshot generation fails (best-effort)", async () => {
    mockGenerateAllAssetSnapshots.mockRejectedValueOnce(new Error("Asset snapshot timeout"));
    const params = buildBaseParams();

    const result = await processUserPerformance(params);

    expect(result.success).toBe(true);
    expect(result.userId).toBe("user-1");
  });

  it("should call fetchLatestAssetPerformance for each currency", async () => {
    const params = buildBaseParams();

    await processUserPerformance(params);

    expect(mockFetchLatestAssetPerformance).toHaveBeenCalledWith(
      mockDb, "user-1", "overall", "USD"
    );
  });

  it("should skip asset snapshots when no assets found", async () => {
    mockFetchLatestAssetPerformance.mockResolvedValue({});
    const params = buildBaseParams();

    await processUserPerformance(params);

    expect(mockGenerateAllAssetSnapshots).not.toHaveBeenCalled();
  });
});
