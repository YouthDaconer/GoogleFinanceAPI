const { PLAN_FEATURES } = require("../../payment/planFeatures");

// Mock firebaseAdmin before requiring the module under test
const mockSet = jest.fn().mockResolvedValue();
const mockDoc = jest.fn(() => ({ set: mockSet }));
const mockCollection = jest.fn(() => ({ doc: mockDoc }));

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
      auth: { uid: "user123" },
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
      auth: { uid: "user456" },
      data: { planId: "free" },
    });

    expect(result.success).toBe(true);
    expect(result.plan).toBe("free");
    expect(result.features.hasRiskMetrics).toBe(false);
  });

  test("defaults interval to month and status to active", async () => {
    await capturedHandler({
      auth: { uid: "user1" },
      data: { planId: "pro" },
    });

    const writtenSubscription = mockSet.mock.calls[0][0].subscription;
    expect(writtenSubscription.interval).toBe("month");
    expect(writtenSubscription.status).toBe("active");
  });
});
