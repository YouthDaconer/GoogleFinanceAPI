// Mock firebaseAdmin BEFORE requiring planFeatures
const mockForEach = jest.fn();
const mockGet = jest.fn();

jest.mock("../../firebaseAdmin", () => ({
  firestore: () => ({
    collection: () => ({ get: mockGet }),
  }),
}));

const {
  PLAN_FEATURES,
  PLAN_FEATURES_FALLBACK,
  VALID_PLANS,
  UNLIMITED,
  buildSubscriptionData,
  getPlanFeatures,
  getAllPlanFeatures,
  _resetCache,
} = require("../../payment/planFeatures");

describe("PLAN_FEATURES", () => {
  test("free plan has restricted features with FEAT-PRICING-RESTRUCTURE-001 values", () => {
    const free = PLAN_FEATURES.free;
    expect(free.hasRiskMetrics).toBe(false);
    expect(free.maxAccounts).toBe(2);
    expect(free.historyDays).toBe(365);       // CAMBIO: 90 → 365
    expect(free.hasAlerts).toBe(true);         // CAMBIO: false → true
    expect(free.alertLimit).toBe(1);           // CAMBIO: 0 → 1
    expect(free.hasSimulators).toBe(false);
    expect(free.hasAttribution).toBe(false);
    expect(free.hasIntelligence).toBe(false);
    expect(free.hasBacktesting).toBe(false);
    expect(free.hasImport).toBe(true);         // CAMBIO: false → true
    expect(free.hasExportCsv).toBe(false);
    expect(free.hasExportPdf).toBe(false);
    expect(free.hasTaxReports).toBe(false);
    expect(free.hasAiInsights).toBe(false);
    expect(free.maxAssets).toBe(UNLIMITED);
    expect(free.supportLevel).toBe("community");
    // Nuevos keys (FEAT-PRICING-RESTRUCTURE-001)
    expect(free.hasRealtimeStreaming).toBe(false);
    expect(free.maxWatchlist).toBe(3);
    expect(free.hasDividendProjections).toBe(false);
    expect(free.hasBriefings).toBe(false);
    expect(free.hasEtfAnalyzer).toBe(false);
  });

  test("pro plan has all features enabled including new keys", () => {
    const pro = PLAN_FEATURES.pro;
    expect(pro.hasRiskMetrics).toBe(true);
    expect(pro.maxAccounts).toBe(UNLIMITED);
    expect(pro.historyDays).toBe(UNLIMITED);
    expect(pro.hasAlerts).toBe(true);
    expect(pro.alertLimit).toBe(UNLIMITED);
    expect(pro.hasSimulators).toBe(true);
    expect(pro.hasImport).toBe(true);
    expect(pro.hasExportCsv).toBe(true);
    expect(pro.hasExportPdf).toBe(true);
    expect(pro.hasTaxReports).toBe(true);
    expect(pro.hasAiInsights).toBe(true);
    expect(pro.supportLevel).toBe("email");
    // Nuevos keys habilitados en Pro
    expect(pro.hasRealtimeStreaming).toBe(true);
    expect(pro.maxWatchlist).toBe(UNLIMITED);
    expect(pro.hasDividendProjections).toBe(true);
    expect(pro.hasBriefings).toBe(true);
    expect(pro.hasEtfAnalyzer).toBe(true);
  });

  test("lifetime plan has priority support", () => {
    expect(PLAN_FEATURES.lifetime.supportLevel).toBe("priority");
  });

  test("lifetime plan has same features as pro except supportLevel", () => {
    const { supportLevel: _ls, ...lifetimeRest } = PLAN_FEATURES.lifetime;
    const { supportLevel: _ps, ...proRest } = PLAN_FEATURES.pro;
    expect(lifetimeRest).toEqual(proRest);
  });

  test("each plan has exactly 21 feature keys", () => {
    for (const planId of VALID_PLANS) {
      expect(Object.keys(PLAN_FEATURES[planId])).toHaveLength(21);
    }
  });

  test("free plan has the same keys as BASE_FEATURES (no drift)", () => {
    const baseKeys = Object.keys(PLAN_FEATURES.pro).sort();
    const freeKeys = Object.keys(PLAN_FEATURES.free).sort();
    expect(freeKeys).toEqual(baseKeys);
  });

  // BUG-GATE-002 prevention: canonical key names must match frontend contract
  test("all plans have the exact 21 canonical feature key names", () => {
    const canonicalKeys = [
      "alertLimit",
      "hasAiInsights",
      "hasAlerts",
      "hasAttribution",
      "hasBacktesting",
      "hasBriefings",
      "hasDividendProjections",
      "hasEtfAnalyzer",
      "hasExportCsv",
      "hasExportPdf",
      "hasImport",
      "hasIntelligence",
      "hasRealtimeStreaming",
      "hasRiskMetrics",
      "hasSimulators",
      "hasTaxReports",
      "historyDays",
      "maxAccounts",
      "maxAssets",
      "maxWatchlist",
      "supportLevel",
    ];
    for (const planId of VALID_PLANS) {
      expect(Object.keys(PLAN_FEATURES[planId]).sort()).toEqual(canonicalKeys);
    }
  });
});

describe("buildSubscriptionData", () => {
  beforeEach(() => {
    _resetCache();
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-03-29T12:00:00.000Z"));
    // Default: Firestore returns empty → triggers fallback
    mockGet.mockResolvedValue({ forEach: jest.fn() });
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test("pro monthly sets currentPeriodEnd ~30 days ahead", async () => {
    const result = await buildSubscriptionData("pro", "month");

    expect(result.planId).toBe("pro");
    expect(result.status).toBe("active");
    expect(result.interval).toBe("month");
    expect(result.features.hasRiskMetrics).toBe(true);

    const periodEnd = new Date(result.currentPeriodEnd);
    const diffDays = (periodEnd - new Date("2026-03-29T12:00:00.000Z")) / (1000 * 60 * 60 * 24);
    expect(diffDays).toBe(30);
  });

  test("pro yearly sets currentPeriodEnd ~365 days ahead", async () => {
    const result = await buildSubscriptionData("pro", "year");

    const periodEnd = new Date(result.currentPeriodEnd);
    const diffDays = (periodEnd - new Date("2026-03-29T12:00:00.000Z")) / (1000 * 60 * 60 * 24);
    expect(diffDays).toBe(365);
  });

  test("lifetime has purchasedAt and no currentPeriodEnd", async () => {
    const result = await buildSubscriptionData("lifetime", "lifetime");

    expect(result.planId).toBe("lifetime");
    expect(result.currentPeriodEnd).toBeNull();
    expect(result.purchasedAt).toBe("2026-03-29T12:00:00.000Z");
    expect(result.features.supportLevel).toBe("priority");
  });

  test("free has no currentPeriodEnd", async () => {
    const result = await buildSubscriptionData("free", "month");

    expect(result.planId).toBe("free");
    expect(result.currentPeriodEnd).toBeNull();
    expect(result.features.hasRiskMetrics).toBe(false);
    expect(result.features.maxAccounts).toBe(2);
    expect(result.features.historyDays).toBe(365);
    expect(Object.keys(result.features)).toHaveLength(21);
  });

  test("invalid planId defaults to free features", async () => {
    const result = await buildSubscriptionData("xxx", "month");

    expect(result.planId).toBe("free");
    expect(result.features.hasRiskMetrics).toBe(false);
    expect(result.features.maxAccounts).toBe(2);
  });

  test("status defaults to active", async () => {
    const result = await buildSubscriptionData("pro", "month");
    expect(result.status).toBe("active");
  });

  test("custom status is preserved", async () => {
    const result = await buildSubscriptionData("pro", "month", "past_due");
    expect(result.status).toBe("past_due");
  });

  test("subscription shape has all required fields", async () => {
    const result = await buildSubscriptionData("pro", "month");

    expect(result).toHaveProperty("planId");
    expect(result).toHaveProperty("status");
    expect(result).toHaveProperty("interval");
    expect(result).toHaveProperty("providerCustomerId", null);
    expect(result).toHaveProperty("subscriptionId", null);
    expect(result).toHaveProperty("currentPeriodEnd");
    expect(result).toHaveProperty("cancelAtPeriodEnd", false);
    expect(result).toHaveProperty("features");
    expect(result).toHaveProperty("updatedAt");
  });

  test("free plan has no purchasedAt", async () => {
    const result = await buildSubscriptionData("free", "month");
    expect(result.purchasedAt).toBeUndefined();
  });
});

// ============================================================================
// PAY-008: Cache behavior tests
// ============================================================================

function makePlanDefinitionsSnapshot(plans) {
  const docs = Object.entries(plans).map(([id, features]) => ({
    id,
    data: () => ({ features }),
  }));
  return {
    forEach: (cb) => docs.forEach(cb),
  };
}

const FIRESTORE_PRO_FEATURES = {
  ...PLAN_FEATURES_FALLBACK.pro,
  maxWatchlist: 50, // different from fallback to prove Firestore was read
};

const FULL_PLAN_SNAPSHOT = makePlanDefinitionsSnapshot({
  free: PLAN_FEATURES_FALLBACK.free,
  pro: FIRESTORE_PRO_FEATURES,
  lifetime: PLAN_FEATURES_FALLBACK.lifetime,
});

describe("PAY-008: getPlanFeatures with cache", () => {
  beforeEach(() => {
    _resetCache();
    mockGet.mockReset();
  });

  test("reads from Firestore on first call (cache miss)", async () => {
    mockGet.mockResolvedValue(FULL_PLAN_SNAPSHOT);

    const result = await getPlanFeatures("pro");
    expect(result.maxWatchlist).toBe(50);
    expect(mockGet).toHaveBeenCalledTimes(1);
  });

  test("returns cached data on second call (cache hit, no Firestore read)", async () => {
    mockGet.mockResolvedValue(FULL_PLAN_SNAPSHOT);

    await getPlanFeatures("pro");
    mockGet.mockClear();

    const result = await getPlanFeatures("pro");
    expect(result.maxWatchlist).toBe(50);
    expect(mockGet).not.toHaveBeenCalled();
  });

  test("re-reads Firestore after cache TTL expires", async () => {
    mockGet.mockResolvedValue(FULL_PLAN_SNAPSHOT);

    await getPlanFeatures("pro");
    mockGet.mockClear();

    // Advance time past TTL
    const originalDateNow = Date.now;
    Date.now = () => originalDateNow() + 6 * 60 * 1000;

    const updatedSnapshot = makePlanDefinitionsSnapshot({
      free: PLAN_FEATURES_FALLBACK.free,
      pro: { ...PLAN_FEATURES_FALLBACK.pro, maxWatchlist: 100 },
      lifetime: PLAN_FEATURES_FALLBACK.lifetime,
    });
    mockGet.mockResolvedValue(updatedSnapshot);

    const result = await getPlanFeatures("pro");
    expect(result.maxWatchlist).toBe(100);
    expect(mockGet).toHaveBeenCalledTimes(1);

    Date.now = originalDateNow;
  });

  test("returns fallback when Firestore read fails", async () => {
    mockGet.mockRejectedValue(new Error("Firestore unavailable"));

    const result = await getPlanFeatures("pro");
    expect(result).toEqual(PLAN_FEATURES_FALLBACK.pro);
  });

  test("returns fallback when planDefinitions is incomplete (< 3 docs)", async () => {
    const incompleteSnapshot = makePlanDefinitionsSnapshot({
      free: PLAN_FEATURES_FALLBACK.free,
    });
    mockGet.mockResolvedValue(incompleteSnapshot);

    const result = await getPlanFeatures("pro");
    expect(result).toEqual(PLAN_FEATURES_FALLBACK.pro);
  });

  test("getAllPlanFeatures returns all 3 plans", async () => {
    mockGet.mockResolvedValue(FULL_PLAN_SNAPSHOT);

    const all = await getAllPlanFeatures();
    expect(Object.keys(all)).toEqual(expect.arrayContaining(["free", "pro", "lifetime"]));
    expect(all.pro.maxWatchlist).toBe(50);
  });

  test("promise coalescing: concurrent calls produce only 1 Firestore read", async () => {
    mockGet.mockImplementation(() => new Promise((resolve) => {
      setTimeout(() => resolve(FULL_PLAN_SNAPSHOT), 50);
    }));

    const [r1, r2, r3] = await Promise.all([
      getPlanFeatures("pro"),
      getPlanFeatures("pro"),
      getPlanFeatures("pro"),
    ]);

    expect(mockGet).toHaveBeenCalledTimes(1);
    expect(r1.maxWatchlist).toBe(50);
    expect(r2.maxWatchlist).toBe(50);
    expect(r3.maxWatchlist).toBe(50);
  });
});
