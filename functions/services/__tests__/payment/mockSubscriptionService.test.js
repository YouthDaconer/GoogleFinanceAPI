const { PLAN_FEATURES, _resetCache } = require("../../payment/planFeatures");

// Mock firebaseAdmin before requiring the module under test
const mockSet = jest.fn().mockResolvedValue();
const mockGet = jest.fn().mockResolvedValue({ data: () => ({}) });
const mockCollectionGet = jest.fn().mockResolvedValue({ forEach: jest.fn() });
const mockDoc = jest.fn(() => ({ set: mockSet, get: mockGet }));
const mockCollection = jest.fn(() => ({ doc: mockDoc, get: mockCollectionGet }));

jest.mock("../../firebaseAdmin", () => ({
  firestore: () => ({ collection: mockCollection }),
}));

// Mock firebase-functions/v2/https — capture the handler
let capturedHandler;
jest.mock("firebase-functions/v2/https", () => ({
  onCall: (opts, handler) => {
    capturedHandler = handler;
    return handler;
  },
  HttpsError: class HttpsError extends Error {
    constructor(code, message) {
      super(message);
      this.code = code;
    }
  },
}));

// Now require the module (mocks are in place)
require("../../payment/mockSubscriptionService");

describe("mockSetSubscription", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    jest.clearAllMocks();
    process.env = { ...originalEnv, PAYMENT_MOCK_ENABLED: "true" };
    _resetCache();
    mockCollectionGet.mockResolvedValue({ forEach: jest.fn() });
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  test("rejects when PAYMENT_MOCK_ENABLED is not true", async () => {
    process.env.PAYMENT_MOCK_ENABLED = "false";

    await expect(
      capturedHandler({ auth: { uid: "user1" }, data: { planId: "pro" } })
    ).rejects.toMatchObject({ code: "failed-precondition" });
  });

  test("rejects when PAYMENT_MOCK_ENABLED is undefined", async () => {
    delete process.env.PAYMENT_MOCK_ENABLED;

    await expect(
      capturedHandler({ auth: { uid: "user1" }, data: { planId: "pro" } })
    ).rejects.toMatchObject({ code: "failed-precondition" });
  });

  test("rejects when no auth", async () => {
    await expect(
      capturedHandler({ auth: null, data: { planId: "pro" } })
    ).rejects.toMatchObject({ code: "unauthenticated" });
  });

  test("rejects when auth has no uid", async () => {
    await expect(
      capturedHandler({ auth: {}, data: { planId: "pro" } })
    ).rejects.toMatchObject({ code: "unauthenticated" });
  });

  test("rejects invalid planId", async () => {
    await expect(
      capturedHandler({ auth: { uid: "user1" }, data: { planId: "enterprise" } })
    ).rejects.toMatchObject({ code: "invalid-argument" });
  });

  test("rejects when data is null", async () => {
    await expect(
      capturedHandler({ auth: { uid: "user1" }, data: null })
    ).rejects.toMatchObject({ code: "invalid-argument" });
  });

  test("writes subscription to Firestore for pro plan", async () => {
    const result = await capturedHandler({
      auth: { uid: "user123", token: {} },
      data: { planId: "pro", interval: "month" },
    });

    expect(mockCollection).toHaveBeenCalledWith("userData");
    expect(mockDoc).toHaveBeenCalledWith("user123");
    expect(mockSet).toHaveBeenCalledWith(
      { subscription: expect.objectContaining({ planId: "pro", features: PLAN_FEATURES.pro }) },
      { merge: true }
    );

    expect(result).toEqual({
      success: true,
      plan: "pro",
      features: PLAN_FEATURES.pro,
      message: "Subscription updated to pro (month)",
    });
  });

  test("writes subscription to Firestore for free plan", async () => {
    const result = await capturedHandler({
      auth: { uid: "user456", token: {} },
      data: { planId: "free" },
    });

    expect(result.success).toBe(true);
    expect(result.plan).toBe("free");
    expect(result.features.hasRiskMetrics).toBe(false);
  });

  test("defaults interval to month and status to active", async () => {
    await capturedHandler({
      auth: { uid: "user1", token: {} },
      data: { planId: "pro" },
    });

    const writtenSubscription = mockSet.mock.calls[0][0].subscription;
    expect(writtenSubscription.interval).toBe("month");
    expect(writtenSubscription.status).toBe("active");
  });

  // F0-02: Admin guard in production
  describe("admin guard (F0-02)", () => {
    test("rejects non-admin in production (K_SERVICE set)", async () => {
      process.env.K_SERVICE = "mockSetSubscription";

      await expect(
        capturedHandler({ auth: { uid: "user1", token: {} }, data: { planId: "pro" } })
      ).rejects.toMatchObject({ code: "permission-denied" });

      delete process.env.K_SERVICE;
    });

    test("rejects non-admin when NODE_ENV is production", async () => {
      process.env.NODE_ENV = "production";

      await expect(
        capturedHandler({ auth: { uid: "user1", token: {} }, data: { planId: "pro" } })
      ).rejects.toMatchObject({ code: "permission-denied" });

      delete process.env.NODE_ENV;
    });

    test("allows admin in production", async () => {
      process.env.K_SERVICE = "mockSetSubscription";

      const result = await capturedHandler({
        auth: { uid: "admin1", token: { admin: true } },
        data: { planId: "pro" },
      });

      expect(result.success).toBe(true);
      delete process.env.K_SERVICE;
    });

    test("allows non-admin in development (no K_SERVICE, no NODE_ENV=production)", async () => {
      delete process.env.K_SERVICE;
      delete process.env.NODE_ENV;

      const result = await capturedHandler({
        auth: { uid: "dev1", token: {} },
        data: { planId: "pro" },
      });

      expect(result.success).toBe(true);
    });
  });

  // F0-03: Downgrade guard server-side
  describe("downgrade guard (F0-03)", () => {
    test("rejects upgrade from pro to lifetime via mock", async () => {
      mockGet.mockResolvedValueOnce({ data: () => ({ subscription: { planId: "pro" } }) });

      await expect(
        capturedHandler({ auth: { uid: "user1", token: {} }, data: { planId: "lifetime" } })
      ).rejects.toMatchObject({ code: "failed-precondition" });
    });

    test("allows downgrade from pro to free", async () => {
      mockGet.mockResolvedValueOnce({ data: () => ({ subscription: { planId: "pro" } }) });

      const result = await capturedHandler({
        auth: { uid: "user1", token: {} },
        data: { planId: "free" },
      });

      expect(result.success).toBe(true);
    });

    test("allows upgrade from free to any plan", async () => {
      mockGet.mockResolvedValueOnce({ data: () => ({ subscription: { planId: "free" } }) });

      const result = await capturedHandler({
        auth: { uid: "user1", token: {} },
        data: { planId: "lifetime" },
      });

      expect(result.success).toBe(true);
    });

    test("rejects same-plan re-subscription (pro → pro) when not on trial", async () => {
      mockGet.mockResolvedValueOnce({ data: () => ({ subscription: { planId: "pro", subscriptionOrigin: "mock_checkout" } }) });

      await expect(
        capturedHandler({ auth: { uid: "user1", token: {} }, data: { planId: "pro" } })
      ).rejects.toMatchObject({ code: "failed-precondition" });
    });

    test("rejects same-plan re-subscription (lifetime → lifetime)", async () => {
      mockGet.mockResolvedValueOnce({ data: () => ({ subscription: { planId: "lifetime" } }) });

      await expect(
        capturedHandler({ auth: { uid: "user1", token: {} }, data: { planId: "lifetime" } })
      ).rejects.toMatchObject({ code: "failed-precondition" });
    });

    test("allows trial user to convert pro(trial) → pro(paid)", async () => {
      mockGet.mockResolvedValueOnce({
        data: () => ({
          subscription: {
            planId: "pro",
            subscriptionOrigin: "trial",
            hasUsedTrial: true,
            trialStartedAt: "2026-03-06T00:00:00.000Z",
          },
        }),
      });

      const result = await capturedHandler({
        auth: { uid: "trial-convert", token: {} },
        data: { planId: "pro", interval: "month" },
      });

      expect(result.success).toBe(true);
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.subscriptionOrigin).toBe("mock_checkout");
      expect(writtenSub.hasUsedTrial).toBe(true);
      expect(writtenSub.trialStartedAt).toBe("2026-03-06T00:00:00.000Z");
      expect(writtenSub.trialEndedAt).toBeDefined();
    });

    test("allows trial user to upgrade pro(trial) → lifetime", async () => {
      mockGet.mockResolvedValueOnce({
        data: () => ({
          subscription: {
            planId: "pro",
            subscriptionOrigin: "trial",
            hasUsedTrial: true,
            trialStartedAt: "2026-03-06T00:00:00.000Z",
          },
        }),
      });

      const result = await capturedHandler({
        auth: { uid: "trial-upgrade", token: {} },
        data: { planId: "lifetime" },
      });

      expect(result.success).toBe(true);
      expect(result.plan).toBe("lifetime");
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.planId).toBe("lifetime");
      expect(writtenSub.hasUsedTrial).toBe(true);
      expect(writtenSub.trialEndedAt).toBeDefined();
    });
  });

  // PAY-009: Trial eligibility guard
  describe("trial eligibility guard (PAY-009)", () => {
    test("grants trial to Free user without prior trial (monthly)", async () => {
      mockGet.mockResolvedValueOnce({ data: () => ({ subscription: { planId: "free" } }) });

      await capturedHandler({
        auth: { uid: "new-user", token: {} },
        data: { planId: "pro", interval: "month" },
      });

      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.subscriptionOrigin).toBe("trial");
      expect(writtenSub.hasUsedTrial).toBe(true);
      expect(writtenSub.trialStartedAt).toBeDefined();
    });

    // BUG-TRIAL-001: Trial solo para Pro Monthly, nunca Annual
    test("denies trial to Free user subscribing to Pro Annual (interval=year)", async () => {
      mockGet.mockResolvedValueOnce({ data: () => ({ subscription: { planId: "free" } }) });

      await capturedHandler({
        auth: { uid: "new-user-annual", token: {} },
        data: { planId: "pro", interval: "year" },
      });

      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.subscriptionOrigin).toBe("mock_checkout");
      expect(writtenSub.hasUsedTrial).toBeUndefined();
      expect(writtenSub.trialStartedAt).toBeUndefined();
    });

    test("denies trial to Free user who already used trial (hasUsedTrial=true)", async () => {
      mockGet.mockResolvedValueOnce({
        data: () => ({ subscription: { planId: "free", hasUsedTrial: true } }),
      });

      await capturedHandler({
        auth: { uid: "returning-user", token: {} },
        data: { planId: "pro", interval: "month" },
      });

      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.subscriptionOrigin).toBe("mock_checkout");
      expect(writtenSub.hasUsedTrial).toBe(true);
    });

    test("grants trial when hasUsedTrial is undefined (backward compat)", async () => {
      mockGet.mockResolvedValueOnce({
        data: () => ({ subscription: { planId: "free" } }),
      });

      await capturedHandler({
        auth: { uid: "legacy-user", token: {} },
        data: { planId: "pro" },
      });

      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.subscriptionOrigin).toBe("trial");
      expect(writtenSub.hasUsedTrial).toBe(true);
    });

    test("forceOrigin overrides trial eligibility guard", async () => {
      mockGet.mockResolvedValueOnce({
        data: () => ({ subscription: { planId: "free", hasUsedTrial: true } }),
      });

      await capturedHandler({
        auth: { uid: "admin-user", token: {} },
        data: { planId: "pro", origin: "trial" },
      });

      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.subscriptionOrigin).toBe("trial");
    });
  });
});
