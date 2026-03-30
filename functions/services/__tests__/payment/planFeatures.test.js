const { PLAN_FEATURES, VALID_PLANS, UNLIMITED, buildSubscriptionData } = require("../../payment/planFeatures");

describe("PLAN_FEATURES", () => {
  test("free plan has restricted features", () => {
    const free = PLAN_FEATURES.free;
    expect(free.hasRiskMetrics).toBe(false);
    expect(free.maxAccounts).toBe(2);
    expect(free.historyDays).toBe(90);
    expect(free.hasAlerts).toBe(false);
    expect(free.alertLimit).toBe(0);
    expect(free.hasSimulators).toBe(false);
    expect(free.hasAttribution).toBe(false);
    expect(free.hasIntelligence).toBe(false);
    expect(free.hasBacktesting).toBe(false);
    expect(free.hasImport).toBe(false);
    expect(free.hasExportCsv).toBe(false);
    expect(free.hasExportPdf).toBe(false);
    expect(free.hasTaxReports).toBe(false);
    expect(free.hasAiInsights).toBe(false);
    expect(free.maxAssets).toBe(UNLIMITED);
    expect(free.supportLevel).toBe("community");
  });

  test("pro plan has all features enabled", () => {
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
  });

  test("lifetime plan has priority support", () => {
    expect(PLAN_FEATURES.lifetime.supportLevel).toBe("priority");
  });

  test("lifetime plan has same features as pro except supportLevel", () => {
    const { supportLevel: _ls, ...lifetimeRest } = PLAN_FEATURES.lifetime;
    const { supportLevel: _ps, ...proRest } = PLAN_FEATURES.pro;
    expect(lifetimeRest).toEqual(proRest);
  });

  test("each plan has exactly 16 feature keys", () => {
    for (const planId of VALID_PLANS) {
      expect(Object.keys(PLAN_FEATURES[planId])).toHaveLength(16);
    }
  });
});

describe("buildSubscriptionData", () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-03-29T12:00:00.000Z"));
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test("pro monthly sets currentPeriodEnd ~30 days ahead", () => {
    const result = buildSubscriptionData("pro", "month");

    expect(result.planId).toBe("pro");
    expect(result.status).toBe("active");
    expect(result.interval).toBe("month");
    expect(result.features.hasRiskMetrics).toBe(true);

    const periodEnd = new Date(result.currentPeriodEnd);
    const diffDays = (periodEnd - new Date("2026-03-29T12:00:00.000Z")) / (1000 * 60 * 60 * 24);
    expect(diffDays).toBe(30);
  });

  test("pro yearly sets currentPeriodEnd ~365 days ahead", () => {
    const result = buildSubscriptionData("pro", "year");

    const periodEnd = new Date(result.currentPeriodEnd);
    const diffDays = (periodEnd - new Date("2026-03-29T12:00:00.000Z")) / (1000 * 60 * 60 * 24);
    expect(diffDays).toBe(365);
  });

  test("lifetime has purchasedAt and no currentPeriodEnd", () => {
    const result = buildSubscriptionData("lifetime", "lifetime");

    expect(result.planId).toBe("lifetime");
    expect(result.currentPeriodEnd).toBeNull();
    expect(result.purchasedAt).toBe("2026-03-29T12:00:00.000Z");
    expect(result.features.supportLevel).toBe("priority");
  });

  test("free has no currentPeriodEnd", () => {
    const result = buildSubscriptionData("free", "month");

    expect(result.planId).toBe("free");
    expect(result.currentPeriodEnd).toBeNull();
    expect(result.features.hasRiskMetrics).toBe(false);
    expect(result.features.maxAccounts).toBe(2);
    expect(result.features.historyDays).toBe(90);
  });

  test("invalid planId defaults to free features", () => {
    const result = buildSubscriptionData("xxx", "month");

    expect(result.planId).toBe("free");
    expect(result.features.hasRiskMetrics).toBe(false);
    expect(result.features.maxAccounts).toBe(2);
  });

  test("status defaults to active", () => {
    const result = buildSubscriptionData("pro", "month");
    expect(result.status).toBe("active");
  });

  test("custom status is preserved", () => {
    const result = buildSubscriptionData("pro", "month", "past_due");
    expect(result.status).toBe("past_due");
  });

  test("subscription shape has all required fields", () => {
    const result = buildSubscriptionData("pro", "month");

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

  test("free plan has no purchasedAt", () => {
    const result = buildSubscriptionData("free", "month");
    expect(result.purchasedAt).toBeUndefined();
  });
});
