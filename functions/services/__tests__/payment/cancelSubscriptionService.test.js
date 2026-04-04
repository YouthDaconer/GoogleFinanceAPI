const mockSet = jest.fn().mockResolvedValue();
const mockGet = jest.fn();
const mockDoc = jest.fn(() => ({ set: mockSet, get: mockGet }));
const mockCollection = jest.fn(() => ({ doc: mockDoc }));

jest.mock("../../firebaseAdmin", () => ({
  firestore: () => ({ collection: mockCollection }),
}));

const mockCancelSubscription = jest.fn();
const mockGetPaymentProvider = jest.fn(() => ({
  cancelSubscription: mockCancelSubscription,
}));

jest.mock("../../payment/providerFactory", () => ({
  getPaymentProvider: mockGetPaymentProvider,
}));

const mockExecute = jest.fn();
const mockGetCircuit = jest.fn(() => ({ execute: mockExecute }));

jest.mock("../../../utils/circuitBreaker", () => ({
  getCircuit: mockGetCircuit,
}));

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

jest.mock("firebase-functions/params", () => ({
  defineSecret: jest.fn((name) => name),
}));

require("../../payment/cancelSubscriptionService");

describe("cancelSubscriptionService", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    jest.clearAllMocks();
    process.env = { ...originalEnv };
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  test("rejects unauthenticated requests", async () => {
    await expect(
      capturedHandler({ auth: null, data: {} })
    ).rejects.toMatchObject({ code: "unauthenticated" });
  });

  test("rejects Free plan (no active subscription)", async () => {
    mockGet.mockResolvedValue({
      data: () => ({ subscription: { planId: "free" } }),
    });

    await expect(
      capturedHandler({ auth: { uid: "user1" }, data: {} })
    ).rejects.toMatchObject({ code: "failed-precondition" });
  });

  test("rejects when no subscription exists", async () => {
    mockGet.mockResolvedValue({ data: () => ({}) });

    await expect(
      capturedHandler({ auth: { uid: "user1" }, data: {} })
    ).rejects.toMatchObject({ code: "failed-precondition" });
  });

  test("rejects Lifetime plans", async () => {
    mockGet.mockResolvedValue({
      data: () => ({ subscription: { planId: "lifetime" } }),
    });

    await expect(
      capturedHandler({ auth: { uid: "user1" }, data: {} })
    ).rejects.toMatchObject({ code: "failed-precondition" });
  });

  describe("mock mode", () => {
    beforeEach(() => {
      process.env.PAYMENT_MOCK_ENABLED = "true";
    });

    test("downgrades to Free immediately via buildSubscriptionData", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.success).toBe(true);
      expect(result.newPlan).toBe("free");
      expect(result.message).toContain("mock mode");
      expect(mockSet).toHaveBeenCalledWith(
        { subscription: expect.objectContaining({ planId: "free" }) },
        { merge: true }
      );
      expect(mockGetPaymentProvider).not.toHaveBeenCalled();
    });
  });

  describe("real mode", () => {
    beforeEach(() => {
      process.env.PAYMENT_MOCK_ENABLED = "false";
    });

    test("rejects when subscriptionId is missing", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro", interval: "month" },
        }),
      });

      await expect(
        capturedHandler({ auth: { uid: "user1" }, data: {} })
      ).rejects.toMatchObject({
        code: "failed-precondition",
        message: expect.stringContaining("No subscription ID found"),
      });
    });

    test("provider success — marks Firestore cancelAtPeriodEnd + returns success", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            interval: "month",
            subscriptionId: "sub-123",
            currentPeriodEnd: "2026-05-01T00:00:00.000Z",
          },
        }),
      });

      mockExecute.mockImplementation(async (primaryFn) => {
        return primaryFn();
      });

      mockCancelSubscription.mockResolvedValue({
        success: true,
        effectiveDate: "2026-05-01T00:00:00.000Z",
        alreadyCanceled: false,
      });

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.success).toBe(true);
      expect(result.effectiveDate).toBe("2026-05-01T00:00:00.000Z");
      expect(mockGetPaymentProvider).toHaveBeenCalled();
      expect(mockGetCircuit).toHaveBeenCalledWith("lemonSqueezy");
      expect(mockCancelSubscription).toHaveBeenCalledWith("sub-123");
      expect(mockSet).toHaveBeenCalledWith(
        {
          subscription: expect.objectContaining({
            cancelAtPeriodEnd: true,
          }),
        },
        { merge: true }
      );
    });

    test("provider fails (circuit open) — fallback to Firestore-only cancel", async () => {
      const warnSpy = jest.spyOn(console, "warn").mockImplementation();
      const logSpy = jest.spyOn(console, "log").mockImplementation();

      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            interval: "month",
            subscriptionId: "sub-456",
            currentPeriodEnd: "2026-05-15T00:00:00.000Z",
          },
        }),
      });

      mockExecute.mockImplementation(async (_primaryFn, fallbackFn) => {
        return fallbackFn();
      });

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.success).toBe(true);
      expect(result.effectiveDate).toBe("2026-05-15T00:00:00.000Z");
      expect(mockSet).toHaveBeenCalledWith(
        {
          subscription: expect.objectContaining({
            cancelAtPeriodEnd: true,
          }),
        },
        { merge: true }
      );
      expect(logSpy).toHaveBeenCalledWith(
        expect.stringContaining("Firestore-only cancel")
      );

      warnSpy.mockRestore();
      logSpy.mockRestore();
    });

    test("provider throws error — fallback to Firestore-only cancel with warning", async () => {
      const warnSpy = jest.spyOn(console, "warn").mockImplementation();
      const logSpy = jest.spyOn(console, "log").mockImplementation();

      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            interval: "month",
            subscriptionId: "sub-789",
            currentPeriodEnd: "2026-06-01T00:00:00.000Z",
          },
        }),
      });

      mockExecute.mockRejectedValue(new Error("Network timeout"));

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.success).toBe(true);
      expect(mockSet).toHaveBeenCalledWith(
        {
          subscription: expect.objectContaining({
            cancelAtPeriodEnd: true,
          }),
        },
        { merge: true }
      );
      expect(warnSpy).toHaveBeenCalledWith(
        "[Cancel] LS API error — fallback:",
        "Network timeout"
      );

      warnSpy.mockRestore();
      logSpy.mockRestore();
    });
  });
});
