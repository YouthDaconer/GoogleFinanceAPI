const mockSet = jest.fn().mockResolvedValue();
const mockGet = jest.fn();
const mockDoc = jest.fn(() => ({ set: mockSet, get: mockGet }));
const mockCollection = jest.fn(() => ({ doc: mockDoc }));
const mockServerTimestamp = jest.fn(() => "SERVER_TIMESTAMP");
const mockFieldValueDelete = jest.fn(() => "FIELD_DELETE");

const mockFirestore = Object.assign(() => ({ collection: mockCollection }), {
  FieldValue: {
    serverTimestamp: mockServerTimestamp,
    delete: mockFieldValueDelete,
  },
});

jest.mock("../../firebaseAdmin", () => ({
  firestore: mockFirestore,
}));

const mockReactivateSubscription = jest.fn();
const mockGetPaymentProvider = jest.fn(() => ({
  reactivateSubscription: mockReactivateSubscription,
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

require("../../payment/reactivateSubscriptionService");

describe("reactivateSubscriptionService", () => {
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

  test("rejects Free plan", async () => {
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

  test("rejects when cancelAtPeriodEnd is not true", async () => {
    mockGet.mockResolvedValue({
      data: () => ({
        subscription: { planId: "pro", cancelAtPeriodEnd: false },
      }),
    });

    await expect(
      capturedHandler({ auth: { uid: "user1" }, data: {} })
    ).rejects.toMatchObject({ code: "failed-precondition" });
  });

  describe("mock mode", () => {
    beforeEach(() => {
      process.env.PAYMENT_MOCK_ENABLED = "true";
    });

    test("sets cancelAtPeriodEnd to false in Firestore", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro", cancelAtPeriodEnd: true },
        }),
      });

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.success).toBe(true);
      expect(result.plan).toBe("pro");
      expect(result.message).toContain("mock mode");
      expect(mockSet).toHaveBeenCalledWith(
        {
          subscription: expect.objectContaining({
            cancelAtPeriodEnd: false,
          }),
        },
        { merge: true }
      );
      expect(mockGetPaymentProvider).not.toHaveBeenCalled();
    });

    test("deletes cancellationReason, cancellationComment, cancelledAt with FieldValue.delete()", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            cancelAtPeriodEnd: true,
            cancellationReason: "too_expensive",
            cancellationComment: "Too pricey",
            cancelledAt: "2026-04-01T00:00:00Z",
          },
        }),
      });

      await capturedHandler({ auth: { uid: "user1" }, data: {} });

      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.cancellationReason).toBe("FIELD_DELETE");
      expect(writtenSub.cancellationComment).toBe("FIELD_DELETE");
      expect(writtenSub.cancelledAt).toBe("FIELD_DELETE");
    });

    test("creates subscriptionEvent with type SUBSCRIPTION_REACTIVATED", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro", cancelAtPeriodEnd: true },
        }),
      });

      await capturedHandler({ auth: { uid: "user-event" }, data: {} });

      expect(mockCollection).toHaveBeenCalledWith("subscriptionEvents");
      expect(mockSet).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "SUBSCRIPTION_REACTIVATED",
          userId: "user-event",
          planId: "pro",
          mode: "mock",
        })
      );
    });
  });

  describe("real mode", () => {
    beforeEach(() => {
      process.env.PAYMENT_MOCK_ENABLED = "false";
    });

    test("rejects when subscriptionId is missing", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro", cancelAtPeriodEnd: true },
        }),
      });

      await expect(
        capturedHandler({ auth: { uid: "user1" }, data: {} })
      ).rejects.toMatchObject({
        code: "failed-precondition",
        message: expect.stringContaining("No subscription ID found"),
      });
    });

    test("calls provider.reactivateSubscription via circuit breaker", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            cancelAtPeriodEnd: true,
            subscriptionId: "sub-123",
          },
        }),
      });

      mockExecute.mockImplementation(async (primaryFn) => primaryFn());
      mockReactivateSubscription.mockResolvedValue({
        success: true,
        providerStatus: "active",
      });

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.success).toBe(true);
      expect(mockGetPaymentProvider).toHaveBeenCalled();
      expect(mockGetCircuit).toHaveBeenCalledWith("lemonSqueezy");
      expect(mockReactivateSubscription).toHaveBeenCalledWith("sub-123");
      expect(mockSet).toHaveBeenCalledWith(
        {
          subscription: expect.objectContaining({
            cancelAtPeriodEnd: false,
          }),
        },
        { merge: true }
      );
    });

    test("circuit open — fallback to Firestore-only reactivation", async () => {
      const warnSpy = jest.spyOn(console, "warn").mockImplementation();
      const logSpy = jest.spyOn(console, "log").mockImplementation();

      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            cancelAtPeriodEnd: true,
            subscriptionId: "sub-456",
          },
        }),
      });

      mockExecute.mockImplementation(async (_primaryFn, fallbackFn) => fallbackFn());

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.success).toBe(true);
      expect(mockSet).toHaveBeenCalledWith(
        {
          subscription: expect.objectContaining({
            cancelAtPeriodEnd: false,
          }),
        },
        { merge: true }
      );
      expect(logSpy).toHaveBeenCalledWith(
        expect.stringContaining("Firestore-only reactivation")
      );

      warnSpy.mockRestore();
      logSpy.mockRestore();
    });

    test("provider throws — fallback to Firestore-only reactivation", async () => {
      const warnSpy = jest.spyOn(console, "warn").mockImplementation();
      const logSpy = jest.spyOn(console, "log").mockImplementation();

      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            cancelAtPeriodEnd: true,
            subscriptionId: "sub-789",
          },
        }),
      });

      mockExecute.mockRejectedValue(new Error("Network timeout"));

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.success).toBe(true);
      expect(mockSet).toHaveBeenCalledWith(
        {
          subscription: expect.objectContaining({
            cancelAtPeriodEnd: false,
          }),
        },
        { merge: true }
      );
      expect(warnSpy).toHaveBeenCalledWith(
        "[Reactivate] LS API error — fallback:",
        "Network timeout"
      );

      warnSpy.mockRestore();
      logSpy.mockRestore();
    });

    test("creates subscriptionEvent with providerSuccess flag", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            cancelAtPeriodEnd: true,
            subscriptionId: "sub-123",
          },
        }),
      });

      mockExecute.mockImplementation(async (primaryFn) => primaryFn());
      mockReactivateSubscription.mockResolvedValue({ success: true });

      await capturedHandler({ auth: { uid: "user-real" }, data: {} });

      expect(mockSet).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "SUBSCRIPTION_REACTIVATED",
          userId: "user-real",
          planId: "pro",
          providerSuccess: true,
          mode: "real",
        })
      );
    });
  });
});
