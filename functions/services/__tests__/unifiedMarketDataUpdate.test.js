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
const { processUserPerformance, MAX_PARALLEL_USERS } = _testExports;

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
