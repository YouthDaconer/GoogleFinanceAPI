const mockSet = jest.fn().mockResolvedValue();
const mockGet = jest.fn();
const mockDoc = jest.fn(() => ({ set: mockSet, get: mockGet }));
const mockCollection = jest.fn(() => ({ doc: mockDoc }));
const mockServerTimestamp = jest.fn(() => "SERVER_TIMESTAMP");

const mockFirestore = Object.assign(() => ({ collection: mockCollection }), {
  FieldValue: { serverTimestamp: mockServerTimestamp },
});

jest.mock("../../firebaseAdmin", () => ({
  firestore: mockFirestore,
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

const mockSendTransactionalEmail = jest.fn().mockResolvedValue("ses-msg-test");
jest.mock("../../payment/emailService", () => ({
  sendTransactionalEmail: (...args) => mockSendTransactionalEmail(...args),
  EMAIL_TEMPLATES: Object.freeze({
    TRIAL_EXPIRING: "trial_expiring",
    PAYMENT_FAILED: "payment_failed",
    SUBSCRIPTION_CANCELLED: "subscription_cancelled",
    PAYMENT_RENEWED: "payment_renewed",
  }),
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
      expect(mockGetCircuit).toHaveBeenCalledWith("whop");
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
        "[Cancel] Payment API error — fallback:",
        "Network timeout"
      );

      warnSpy.mockRestore();
      logSpy.mockRestore();
    });
  });

  // PAY-009: Trial field preservation in mock cancel
  describe("trial field preservation (PAY-009)", () => {
    beforeEach(() => {
      process.env.PAYMENT_MOCK_ENABLED = "true";
    });

    test("preserves hasUsedTrial and sets trialEndedAt when canceling trial subscription", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            subscriptionOrigin: "trial",
            hasUsedTrial: true,
            trialStartedAt: "2026-03-01T00:00:00.000Z",
          },
        }),
      });

      const result = await capturedHandler({ auth: { uid: "trial-user" }, data: {} });

      expect(result.success).toBe(true);
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.planId).toBe("free");
      expect(writtenSub.hasUsedTrial).toBe(true);
      expect(writtenSub.trialStartedAt).toBe("2026-03-01T00:00:00.000Z");
      expect(writtenSub.trialEndedAt).toBeDefined();
    });

    test("preserves hasUsedTrial when canceling mock_checkout subscription", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            subscriptionOrigin: "mock_checkout",
            hasUsedTrial: true,
            trialStartedAt: "2026-02-01T00:00:00.000Z",
          },
        }),
      });

      const result = await capturedHandler({ auth: { uid: "repeat-user" }, data: {} });

      expect(result.success).toBe(true);
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.hasUsedTrial).toBe(true);
      expect(writtenSub.trialStartedAt).toBe("2026-02-01T00:00:00.000Z");
      expect(writtenSub.trialEndedAt).toBeUndefined();
    });
  });

  // --------------------------------------------------------------------------
  // PAY-MOCK-001: Mock scheduled cancel (cancelAtPeriodEnd simulation)
  // --------------------------------------------------------------------------

  describe("mock scheduled cancel (PAY-MOCK-001)", () => {
    beforeEach(() => {
      process.env.PAYMENT_MOCK_ENABLED = "true";
      process.env.MOCK_CANCEL_GRACE_SECONDS = "30";
    });

    test("MOCK_CANCEL_GRACE_SECONDS=30 sets cancelAtPeriodEnd=true with future currentPeriodEnd", async () => {
      const before = Date.now();
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const result = await capturedHandler({ auth: { uid: "user-sched" }, data: {} });
      const after = Date.now();

      expect(result.success).toBe(true);
      expect(result.newPlan).toBe("free");
      expect(result.message).toContain("mock period");

      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.cancelAtPeriodEnd).toBe(true);
      expect(writtenSub.currentPeriodEnd).toBeDefined();
      expect(writtenSub.updatedAt).toBeDefined();

      const periodEnd = new Date(writtenSub.currentPeriodEnd).getTime();
      expect(periodEnd).toBeGreaterThanOrEqual(before + 30000);
      expect(periodEnd).toBeLessThanOrEqual(after + 30000);
    });

    test("returns effectiveDate matching gracePeriodEnd", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const result = await capturedHandler({ auth: { uid: "user-eff" }, data: {} });

      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(result.effectiveDate).toBe(writtenSub.currentPeriodEnd);
    });

    test("does NOT call buildSubscriptionData (no immediate downgrade)", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const result = await capturedHandler({ auth: { uid: "user-no-build" }, data: {} });

      expect(result.success).toBe(true);
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.planId).toBeUndefined();
    });

    test("creates audit event with type SUBSCRIPTION_CANCEL_SCHEDULED and mode mock-scheduled", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      await capturedHandler({ auth: { uid: "user-audit" }, data: {} });

      expect(mockCollection).toHaveBeenCalledWith("subscriptionEvents");
      expect(mockSet).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "SUBSCRIPTION_CANCEL_SCHEDULED",
          userId: "user-audit",
          planId: "pro",
          mode: "mock-scheduled",
        })
      );
    });

    test("preserves trial sticky fields via merge (does not overwrite them)", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            interval: "month",
            hasUsedTrial: true,
            trialStartedAt: "2026-03-01T00:00:00.000Z",
          },
        }),
      });

      const result = await capturedHandler({ auth: { uid: "user-trial-sched" }, data: {} });

      expect(result.success).toBe(true);
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.hasUsedTrial).toBeUndefined();
      expect(writtenSub.trialStartedAt).toBeUndefined();
      expect(mockSet).toHaveBeenCalledWith(
        expect.objectContaining({ subscription: expect.any(Object) }),
        { merge: true }
      );
    });

    test("persists cancellationReason in scheduled cancel merge", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const result = await capturedHandler({
        auth: { uid: "user-reason-sched" },
        data: { cancellationReason: "too_expensive", cancellationComment: "Costly" },
      });

      expect(result.success).toBe(true);
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.cancellationReason).toBe("too_expensive");
      expect(writtenSub.cancellationComment).toBe("Costly");
      expect(writtenSub.cancelledAt).toBeDefined();
    });

    test("sends cancel email with gracePeriodEnd as effectiveDate", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          displayName: "TestUser",
          subscription: { planId: "pro", interval: "month" },
        }),
      });

      await capturedHandler({
        auth: { uid: "user-email-sched", token: { email: "sched@test.com" } },
        data: {},
      });

      expect(mockSendTransactionalEmail).toHaveBeenCalledWith(
        "subscription_cancelled",
        "sched@test.com",
        expect.objectContaining({
          userName: "TestUser",
          planName: "Pro",
          effectiveDate: expect.any(String),
        })
      );

      const emailArgs = mockSendTransactionalEmail.mock.calls[0][2];
      const effectiveDate = new Date(emailArgs.effectiveDate).getTime();
      expect(effectiveDate).toBeGreaterThan(Date.now());
    });
  });

  describe("mock scheduled cancel — edge cases (PAY-MOCK-001)", () => {
    test("MOCK_CANCEL_GRACE_SECONDS=0 downgrades immediately (backward compat)", async () => {
      process.env.PAYMENT_MOCK_ENABLED = "true";
      process.env.MOCK_CANCEL_GRACE_SECONDS = "0";

      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const result = await capturedHandler({ auth: { uid: "user-zero" }, data: {} });

      expect(result.success).toBe(true);
      expect(result.newPlan).toBe("free");
      expect(result.message).toContain("immediate");
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.planId).toBe("free");
      expect(writtenSub.cancelAtPeriodEnd).toBeFalsy();
    });

    test("MOCK_CANCEL_GRACE_SECONDS undefined downgrades immediately", async () => {
      process.env.PAYMENT_MOCK_ENABLED = "true";
      delete process.env.MOCK_CANCEL_GRACE_SECONDS;

      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const result = await capturedHandler({ auth: { uid: "user-undef" }, data: {} });

      expect(result.success).toBe(true);
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.planId).toBe("free");
    });

    test("MOCK_CANCEL_GRACE_SECONDS='abc' (invalid) falls back to immediate mode", async () => {
      process.env.PAYMENT_MOCK_ENABLED = "true";
      process.env.MOCK_CANCEL_GRACE_SECONDS = "abc";

      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const result = await capturedHandler({ auth: { uid: "user-nan" }, data: {} });

      expect(result.success).toBe(true);
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.planId).toBe("free");
    });
  });

  // --------------------------------------------------------------------------
  // PAY-CANCEL-001: Cancellation reason persistence & validation
  // --------------------------------------------------------------------------

  describe("cancellation reason (PAY-CANCEL-001)", () => {
    beforeEach(() => {
      process.env.PAYMENT_MOCK_ENABLED = "true";
    });

    test("persists valid cancellationReason in Firestore subscription (mock mode)", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const result = await capturedHandler({
        auth: { uid: "user-reason" },
        data: { cancellationReason: "too_expensive", cancellationComment: "Too pricey" },
      });

      expect(result.success).toBe(true);
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.cancellationReason).toBe("too_expensive");
      expect(writtenSub.cancellationComment).toBe("Too pricey");
      expect(writtenSub.cancelledAt).toBeDefined();
    });

    test("rejects invalid cancellationReason with invalid-argument", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      await expect(
        capturedHandler({
          auth: { uid: "user-hack" },
          data: { cancellationReason: "hacked_value" },
        })
      ).rejects.toMatchObject({ code: "invalid-argument" });
    });

    test("works without cancellationReason (backward compatible)", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const result = await capturedHandler({
        auth: { uid: "user-compat" },
        data: {},
      });

      expect(result.success).toBe(true);
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.cancellationReason).toBeUndefined();

      // Audit event se crea incluso sin reason (con reason: null)
      expect(mockSet).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "SUBSCRIPTION_CANCELLED",
          userId: "user-compat",
          reason: null,
          comment: null,
          mode: "mock",
        })
      );
    });

    test("truncates cancellationComment to 500 chars", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const longComment = "a".repeat(600);
      const result = await capturedHandler({
        auth: { uid: "user-long" },
        data: { cancellationReason: "other", cancellationComment: longComment },
      });

      expect(result.success).toBe(true);
      const writtenSub = mockSet.mock.calls[0][0].subscription;
      expect(writtenSub.cancellationComment.length).toBe(500);
    });

    test("creates subscriptionEvent with type SUBSCRIPTION_CANCELLED", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      await capturedHandler({
        auth: { uid: "user-event" },
        data: { cancellationReason: "not_using_features", cancellationComment: "Rarely used" },
      });

      expect(mockCollection).toHaveBeenCalledWith("subscriptionEvents");
      expect(mockSet).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "SUBSCRIPTION_CANCELLED",
          userId: "user-event",
          reason: "not_using_features",
          comment: "Rarely used",
          mode: "mock",
        })
      );
    });

    test("persists reason in real mode Firestore merge", async () => {
      process.env.PAYMENT_MOCK_ENABLED = "false";
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            interval: "month",
            subscriptionId: "sub-reason",
            currentPeriodEnd: "2026-05-01T00:00:00.000Z",
          },
        }),
      });

      mockExecute.mockImplementation(async (primaryFn) => primaryFn());
      mockCancelSubscription.mockResolvedValue({ success: true });

      const result = await capturedHandler({
        auth: { uid: "user-real-reason" },
        data: { cancellationReason: "found_alternative" },
      });

      expect(result.success).toBe(true);
      expect(mockSet).toHaveBeenCalledWith(
        {
          subscription: expect.objectContaining({
            cancelAtPeriodEnd: true,
            cancellationReason: "found_alternative",
          }),
        },
        { merge: true }
      );
    });
  });

  // --------------------------------------------------------------------------
  // PAY-EMAIL-001: Email notification on cancel
  // --------------------------------------------------------------------------

  describe("email notification (PAY-EMAIL-001)", () => {
    beforeEach(() => {
      process.env.PAYMENT_MOCK_ENABLED = "true";
    });

    test("mock mode with email in token sends subscription_cancelled email", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          displayName: "Carlos",
          subscription: { planId: "pro", interval: "month" },
        }),
      });

      await capturedHandler({
        auth: { uid: "user-email", token: { email: "carlos@test.com" } },
        data: {},
      });

      expect(mockSendTransactionalEmail).toHaveBeenCalledWith(
        "subscription_cancelled",
        "carlos@test.com",
        expect.objectContaining({
          userName: "Carlos",
          planName: "Pro",
        })
      );
    });

    test("mock mode without email in token does NOT send email", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      await capturedHandler({
        auth: { uid: "user-no-email", token: {} },
        data: {},
      });

      expect(mockSendTransactionalEmail).not.toHaveBeenCalled();
    });

    test("real mode with email sends email with effectiveDate", async () => {
      process.env.PAYMENT_MOCK_ENABLED = "false";
      mockGet.mockResolvedValue({
        data: () => ({
          displayName: "Ana",
          subscription: {
            planId: "pro",
            interval: "month",
            subscriptionId: "sub-email",
            currentPeriodEnd: "2026-05-01T00:00:00.000Z",
          },
        }),
      });

      mockExecute.mockImplementation(async (primaryFn) => primaryFn());
      mockCancelSubscription.mockResolvedValue({ success: true });

      const logSpy = jest.spyOn(console, "log").mockImplementation();
      await capturedHandler({
        auth: { uid: "user-email-real", token: { email: "ana@test.com" } },
        data: {},
      });
      logSpy.mockRestore();

      expect(mockSendTransactionalEmail).toHaveBeenCalledWith(
        "subscription_cancelled",
        "ana@test.com",
        expect.objectContaining({
          userName: "Ana",
          effectiveDate: "2026-05-01T00:00:00.000Z",
        })
      );
    });

    test("email error does NOT prevent cancel from completing", async () => {
      mockSendTransactionalEmail.mockRejectedValueOnce(new Error("SES down"));
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { planId: "pro", interval: "month" } }),
      });

      const result = await capturedHandler({
        auth: { uid: "user-email-fail", token: { email: "fail@test.com" } },
        data: {},
      });

      expect(result.success).toBe(true);
    });
  });
});
