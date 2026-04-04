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
  });
});
