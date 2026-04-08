const { PLAN_FEATURES, buildSubscriptionData, _resetCache } = require("../../payment/planFeatures");
const { PAYMENT_EVENT_TYPES } = require("../../payment/paymentProvider");

// Mock firebaseAdmin — supports both collection().doc().set/get and collection().get()
const mockSet = jest.fn().mockResolvedValue();
const mockGet = jest.fn();
const mockCollectionGet = jest.fn().mockResolvedValue({ forEach: jest.fn() });
const mockDoc = jest.fn(() => ({ set: mockSet, get: mockGet }));
const mockCollection = jest.fn((name) => ({
  doc: mockDoc,
  get: mockCollectionGet,
}));

jest.mock("../../firebaseAdmin", () => ({
  firestore: () => ({ collection: mockCollection }),
}));

// Mock circuitBreaker — pass-through by default (CLOSED state)
const mockCircuitExecute = jest.fn((primaryFn) => primaryFn());
jest.mock("../../../utils/circuitBreaker", () => ({
  getCircuit: jest.fn(() => ({ execute: mockCircuitExecute })),
}));

// Mock providerFactory
const mockProvider = {
  createCheckoutSession: jest.fn(),
  createPortalSession: jest.fn(),
  getSubscription: jest.fn(),
  parseWebhook: jest.fn(),
};

jest.mock("../../payment/providerFactory", () => ({
  getPaymentProvider: jest.fn(() => mockProvider),
}));

const { initiateCheckout, createPortalSession, processWebhookEvent } = require("../../payment/subscriptionService");

beforeEach(() => {
  jest.clearAllMocks();
  jest.useFakeTimers();
  jest.setSystemTime(new Date("2026-04-03T12:00:00.000Z"));
  mockSet.mockResolvedValue();
  mockCircuitExecute.mockImplementation((primaryFn) => primaryFn());
  // PAY-008: Reset planFeatures cache so each test uses fallback
  _resetCache();
  mockCollectionGet.mockResolvedValue({ forEach: jest.fn() });
});

afterEach(() => {
  jest.useRealTimers();
});

describe("initiateCheckout", () => {
  test("calls provider.createCheckoutSession and returns { url }", async () => {
    mockProvider.createCheckoutSession.mockResolvedValue({
      checkoutUrl: "https://checkout.example.com/abc",
    });

    const result = await initiateCheckout("uid-1", "user@test.com", "pro", "month");

    expect(result).toEqual({ url: "https://checkout.example.com/abc" });
    expect(mockProvider.createCheckoutSession).toHaveBeenCalledWith(
      expect.objectContaining({
        userId: "uid-1",
        email: "user@test.com",
        planId: "pro",
        interval: "month",
        trialDays: 0,
      })
    );
  });

  test("WHOP-010: passes trialDays to provider when options.trialDays is set", async () => {
    mockProvider.createCheckoutSession.mockResolvedValue({
      checkoutUrl: "https://checkout.example.com/trial",
    });

    const result = await initiateCheckout("uid-1", "user@test.com", "pro", "month", { trialDays: 30 });

    expect(result).toEqual({ url: "https://checkout.example.com/trial" });
    expect(mockProvider.createCheckoutSession).toHaveBeenCalledWith(
      expect.objectContaining({
        planId: "pro",
        interval: "month",
        trialDays: 30,
      })
    );
  });

  test("lifetime forces interval to 'lifetime'", async () => {
    mockProvider.createCheckoutSession.mockResolvedValue({
      checkoutUrl: "https://checkout.example.com/lt",
    });

    await initiateCheckout("uid-1", "user@test.com", "lifetime", "lifetime");

    expect(mockProvider.createCheckoutSession).toHaveBeenCalledWith(
      expect.objectContaining({ planId: "lifetime", interval: "lifetime" })
    );
  });
});

describe("createPortalSession", () => {
  test("reads subscriptionId from Firestore and returns { url }", async () => {
    mockGet.mockResolvedValue({
      data: () => ({
        subscription: {
          subscriptionId: "sub-123",
          providerCustomerId: "cust-456",
        },
      }),
    });

    mockProvider.createPortalSession.mockResolvedValue({
      portalUrl: "https://portal.example.com/abc",
    });

    const result = await createPortalSession("uid-1");

    expect(result).toEqual({ url: "https://portal.example.com/abc" });
    expect(mockProvider.createPortalSession).toHaveBeenCalledWith("cust-456");
    expect(mockCollection).toHaveBeenCalledWith("userData");
    expect(mockDoc).toHaveBeenCalledWith("uid-1");
  });

  test("throws when no subscription found", async () => {
    mockGet.mockResolvedValue({ data: () => ({}) });

    await expect(createPortalSession("uid-1")).rejects.toThrow("No active subscription found");
  });
});

describe("processWebhookEvent", () => {
  const baseWebhookResult = {
    userId: "uid-1",
    subscriptionId: "sub-100",
    customerId: "cust-200",
    rawData: {},
  };

  describe("CHECKOUT_COMPLETED", () => {
    test("Pro writes Firestore with 21 features and subscriptionOrigin: checkout", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
        planId: "pro",
        interval: "month",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;

      expect(writtenData.planId).toBe("pro");
      expect(writtenData.status).toBe("active");
      expect(writtenData.subscriptionOrigin).toBe("checkout");
      expect(writtenData.subscriptionId).toBe("sub-100");
      expect(writtenData.providerCustomerId).toBe("cust-200");
      expect(Object.keys(writtenData.features)).toHaveLength(21);
      expect(writtenData.features).toEqual(PLAN_FEATURES.pro);

      // Verify merge: true
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });

    test("Lifetime writes with currentPeriodEnd=null, purchasedAt, interval=lifetime", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
        planId: "lifetime",
        interval: "lifetime",
      });

      const writtenData = mockSet.mock.calls[0][0].subscription;

      expect(writtenData.planId).toBe("lifetime");
      expect(writtenData.currentPeriodEnd).toBeNull();
      expect(writtenData.purchasedAt).toBe("2026-04-03T12:00:00.000Z");
      expect(writtenData.interval).toBe("lifetime");
      expect(writtenData.subscriptionOrigin).toBe("checkout");
      expect(writtenData.features).toEqual(PLAN_FEATURES.lifetime);
    });
  });

  describe("SUBSCRIPTION_CANCELED", () => {
    test("degrades to Free with 21 keys using buildSubscriptionData", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.SUBSCRIPTION_CANCELED,
        planId: "pro",
        interval: "month",
      });

      const writtenData = mockSet.mock.calls[0][0].subscription;

      expect(writtenData.planId).toBe("free");
      expect(Object.keys(writtenData.features)).toHaveLength(21);
      expect(writtenData.features).toEqual(PLAN_FEATURES.free);
      expect(writtenData.subscriptionId).toBeNull();
      expect(writtenData.providerCustomerId).toBeNull();
    });
  });

  describe("PAYMENT_FAILED", () => {
    test("marks status as past_due without changing features", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.PAYMENT_FAILED,
        planId: "pro",
        interval: "month",
      });

      const writtenData = mockSet.mock.calls[0][0].subscription;

      expect(writtenData.status).toBe("past_due");
      expect(writtenData).not.toHaveProperty("features");
      expect(writtenData).not.toHaveProperty("planId");
    });
  });

  describe("PAYMENT_SUCCEEDED", () => {
    test("restores status to active when was past_due", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { status: "past_due" } }),
      });

      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
        planId: "pro",
        interval: "month",
      });

      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.status).toBe("active");
    });

    test("does nothing when status is already active", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { status: "active" } }),
      });

      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
        planId: "pro",
        interval: "month",
      });

      // Only get() was called, no set()
      expect(mockSet).not.toHaveBeenCalled();
    });
  });

  describe("unknown event type", () => {
    test("does not process and does not throw", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: "unknown",
        planId: "pro",
        interval: "month",
      });

      expect(mockSet).not.toHaveBeenCalled();
    });
  });

  test("throws when userId is missing", async () => {
    await expect(
      processWebhookEvent({
        type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
        userId: null,
        planId: "pro",
        interval: "month",
      })
    ).rejects.toThrow("Webhook missing userId");
  });

  test("all Firestore writes use merge: true", async () => {
    // CHECKOUT_COMPLETED
    await processWebhookEvent({
      ...baseWebhookResult,
      type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
      planId: "pro",
      interval: "month",
    });

    // SUBSCRIPTION_CANCELED
    await processWebhookEvent({
      ...baseWebhookResult,
      type: PAYMENT_EVENT_TYPES.SUBSCRIPTION_CANCELED,
      planId: "pro",
      interval: "month",
    });

    // PAYMENT_FAILED
    await processWebhookEvent({
      ...baseWebhookResult,
      type: PAYMENT_EVENT_TYPES.PAYMENT_FAILED,
      planId: "pro",
      interval: "month",
    });

    for (const call of mockSet.mock.calls) {
      expect(call[1]).toEqual({ merge: true });
    }
  });

  describe("transaction support (PAY-004)", () => {
    test("uses transaction.set when transaction is provided", async () => {
      const mockTransactionSet = jest.fn();
      const mockTransactionGet = jest.fn().mockResolvedValue({
        data: () => ({ subscription: {} }),
      });
      const mockTransaction = { set: mockTransactionSet, get: mockTransactionGet };

      await processWebhookEvent(
        {
          ...baseWebhookResult,
          type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
          planId: "pro",
          interval: "month",
        },
        mockTransaction
      );

      expect(mockTransactionSet).toHaveBeenCalledTimes(1);
      expect(mockTransactionSet).toHaveBeenCalledWith(
        expect.anything(),
        expect.objectContaining({ subscription: expect.objectContaining({ planId: "pro" }) }),
        { merge: true }
      );
      // Direct doc.set should NOT have been called
      expect(mockSet).not.toHaveBeenCalled();
    });

    test("uses doc.set when transaction is null (backward compatible)", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
        planId: "pro",
        interval: "month",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      expect(mockSet.mock.calls[0][0].subscription.planId).toBe("pro");
    });

    test("PAYMENT_SUCCEEDED reads via transaction.get when transaction provided", async () => {
      const mockTransactionSet = jest.fn();
      const mockTransactionGet = jest.fn().mockResolvedValue({
        data: () => ({ subscription: { status: "past_due" } }),
      });
      const mockTransaction = { set: mockTransactionSet, get: mockTransactionGet };

      await processWebhookEvent(
        {
          ...baseWebhookResult,
          type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
          planId: "pro",
          interval: "month",
        },
        mockTransaction
      );

      expect(mockTransactionGet).toHaveBeenCalled();
      expect(mockTransactionSet).toHaveBeenCalledWith(
        expect.anything(),
        expect.objectContaining({ subscription: expect.objectContaining({ status: "active" }) }),
        { merge: true }
      );
      expect(mockGet).not.toHaveBeenCalled();
    });

    test("SUBSCRIPTION_CANCELED uses transaction.set when transaction provided", async () => {
      const mockTransactionSet = jest.fn();
      const mockTransaction = {
        set: mockTransactionSet,
        get: jest.fn().mockResolvedValue({ data: () => ({ subscription: {} }) }),
      };

      await processWebhookEvent(
        {
          ...baseWebhookResult,
          type: PAYMENT_EVENT_TYPES.SUBSCRIPTION_CANCELED,
          planId: "pro",
          interval: "month",
        },
        mockTransaction
      );

      expect(mockTransactionSet).toHaveBeenCalledTimes(1);
      const writtenData = mockTransactionSet.mock.calls[0][1].subscription;
      expect(writtenData.planId).toBe("free");
      expect(writtenData.subscriptionId).toBeNull();
      expect(mockSet).not.toHaveBeenCalled();
    });
  });
});

describe("Circuit Breaker (PAY-005 F5-01)", () => {
  describe("initiateCheckout", () => {
    test("circuit CLOSED — calls provider and returns { url }", async () => {
      mockProvider.createCheckoutSession.mockResolvedValue({
        checkoutUrl: "https://checkout.example.com/abc",
      });

      const result = await initiateCheckout("uid-1", "user@test.com", "pro", "month");

      expect(result).toEqual({ url: "https://checkout.example.com/abc" });
      expect(mockCircuitExecute).toHaveBeenCalledTimes(1);
      expect(mockProvider.createCheckoutSession).toHaveBeenCalled();
    });

    test("circuit OPEN — fallback throws HttpsError('unavailable')", async () => {
      mockCircuitExecute.mockImplementation((_primaryFn, fallbackFn) => fallbackFn());

      await expect(
        initiateCheckout("uid-1", "user@test.com", "pro", "month")
      ).rejects.toMatchObject({
        code: "unavailable",
        message: expect.stringContaining("servicio de pagos no está disponible"),
      });

      expect(mockProvider.createCheckoutSession).not.toHaveBeenCalled();
    });
  });

  describe("createPortalSession", () => {
    test("circuit OPEN — fallback throws HttpsError('unavailable')", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            subscriptionId: "sub-123",
            providerCustomerId: "cust-456",
          },
        }),
      });

      mockCircuitExecute.mockImplementation((_primaryFn, fallbackFn) => fallbackFn());

      await expect(createPortalSession("uid-1")).rejects.toMatchObject({
        code: "unavailable",
      });

      expect(mockProvider.createPortalSession).not.toHaveBeenCalled();
    });
  });

  describe("processWebhookEvent", () => {
    test("does NOT use circuit breaker", async () => {
      await processWebhookEvent({
        userId: "uid-1",
        subscriptionId: "sub-100",
        customerId: "cust-200",
        rawData: {},
        type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
        planId: "pro",
        interval: "month",
      });

      // circuit.execute called only 0 times for processWebhookEvent
      // (it was cleared in beforeEach, and processWebhookEvent doesn't call it)
      expect(mockCircuitExecute).not.toHaveBeenCalled();
    });
  });
});

// PAY-009: Trial field preservation in webhook handlers
describe("trial field preservation (PAY-009)", () => {
  const baseWebhookResult = {
    userId: "uid-trial",
    subscriptionId: "sub-100",
    customerId: "cust-200",
    rawData: {},
  };

  test("CHECKOUT_COMPLETED preserves hasUsedTrial from existing snapshot", async () => {
    mockGet.mockResolvedValue({
      data: () => ({
        subscription: {
          hasUsedTrial: true,
          trialStartedAt: "2026-03-01T00:00:00.000Z",
          trialEndedAt: "2026-03-15T00:00:00.000Z",
        },
      }),
    });

    await processWebhookEvent({
      ...baseWebhookResult,
      type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
      planId: "pro",
      interval: "month",
    });

    const writtenSub = mockSet.mock.calls[0][0].subscription;
    expect(writtenSub.hasUsedTrial).toBe(true);
    expect(writtenSub.trialStartedAt).toBe("2026-03-01T00:00:00.000Z");
    expect(writtenSub.trialEndedAt).toBe("2026-03-15T00:00:00.000Z");
    expect(writtenSub.subscriptionOrigin).toBe("checkout");
  });

  test("CHECKOUT_COMPLETED without prior trial does NOT add trial fields", async () => {
    mockGet.mockResolvedValue({
      data: () => ({ subscription: {} }),
    });

    await processWebhookEvent({
      ...baseWebhookResult,
      type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
      planId: "pro",
      interval: "month",
    });

    const writtenSub = mockSet.mock.calls[0][0].subscription;
    expect(writtenSub.hasUsedTrial).toBeUndefined();
    expect(writtenSub.trialStartedAt).toBeUndefined();
  });

  test("SUBSCRIPTION_CANCELED preserves hasUsedTrial in Free snapshot", async () => {
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

    await processWebhookEvent({
      ...baseWebhookResult,
      type: PAYMENT_EVENT_TYPES.SUBSCRIPTION_CANCELED,
      planId: "pro",
      interval: "month",
    });

    const writtenSub = mockSet.mock.calls[0][0].subscription;
    expect(writtenSub.planId).toBe("free");
    expect(writtenSub.hasUsedTrial).toBe(true);
    expect(writtenSub.trialStartedAt).toBe("2026-03-01T00:00:00.000Z");
    expect(writtenSub.trialEndedAt).toBeDefined();
  });
});
