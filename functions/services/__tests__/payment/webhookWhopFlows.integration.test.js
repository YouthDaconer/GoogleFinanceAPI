const { PLAN_FEATURES_FALLBACK, _resetCache } = require("../../payment/planFeatures");
const { PAYMENT_EVENT_TYPES } = require("../../payment/paymentProvider");

const FIELD_VALUE_DELETE_SENTINEL = "FIELD_VALUE_DELETE";

const mockSet = jest.fn().mockResolvedValue();
const mockGet = jest.fn();
const mockCollectionGet = jest.fn().mockResolvedValue({ forEach: jest.fn() });
const mockDoc = jest.fn(() => ({ set: mockSet, get: mockGet }));
const mockCollection = jest.fn(() => ({ doc: mockDoc, get: mockCollectionGet }));

jest.mock("../../firebaseAdmin", () => {
  const firestoreFn = () => ({ collection: mockCollection });
  firestoreFn.FieldValue = {
    delete: () => FIELD_VALUE_DELETE_SENTINEL,
    serverTimestamp: jest.fn(() => "SERVER_TIMESTAMP"),
  };
  return {
    firestore: Object.assign(firestoreFn, {
      FieldValue: firestoreFn.FieldValue,
    }),
  };
});

const mockCircuitExecute = jest.fn((primaryFn) => primaryFn());
jest.mock("../../../utils/circuitBreaker", () => ({
  getCircuit: jest.fn(() => ({ execute: mockCircuitExecute })),
}));

jest.mock("../../payment/providerFactory", () => ({
  getPaymentProvider: jest.fn(() => null),
}));

const ORIGINAL_ENV = { ...process.env };

const { processWebhookEvent } = require("../../payment/subscriptionService");

beforeEach(() => {
  jest.clearAllMocks();
  jest.useFakeTimers();
  jest.setSystemTime(new Date("2026-04-06T12:00:00.000Z"));
  _resetCache();
  mockCollectionGet.mockResolvedValue({ forEach: jest.fn() });
  mockSet.mockResolvedValue();
  mockGet.mockResolvedValue({ data: () => ({ subscription: {} }) });

  process.env = { ...ORIGINAL_ENV };
  process.env.WHOP_PLAN_PRO_MONTHLY = "plan_pro_monthly";
  process.env.WHOP_PLAN_PRO_ANNUAL = "plan_pro_annual";
  process.env.WHOP_PLAN_LIFETIME = "plan_lifetime";
});

afterEach(() => {
  jest.useRealTimers();
  process.env = ORIGINAL_ENV;
});

// ============================================================================
// Fase 1: Whop Webhook Flow Integration Tests — Event Handlers
// ============================================================================

describe("Whop Webhook → Firestore integration flows", () => {
  describe("PAYMENT_SUCCEEDED → Pro Monthly", () => {
    test("writes pro subscription with 21 features, subscriptionId, providerCustomerId, origin checkout", async () => {
      const payload = {
        plan: { id: "plan_pro_monthly" },
        membership: { id: "mem_xxx", status: "active" },
        user: { id: "user_xxx" },
        card_brand: "visa",
        card_last4: "4242",
      };

      await processWebhookEvent({
        type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
        data: payload,
        userId: "test-uid",
        eventId: "evt-1",
      });

      expect(mockCollection).toHaveBeenCalledWith("userData");
      expect(mockDoc).toHaveBeenCalledWith("test-uid");
      expect(mockSet).toHaveBeenCalledTimes(1);

      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.planId).toBe("pro");
      expect(writtenData.interval).toBe("month");
      expect(writtenData.status).toBe("active");
      expect(writtenData.subscriptionOrigin).toBe("checkout");
      expect(writtenData.subscriptionId).toBe("mem_xxx");
      expect(writtenData.providerCustomerId).toBe("user_xxx");
      expect(writtenData.cardBrand).toBe("visa");
      expect(writtenData.cardLast4).toBe("4242");
      expect(Object.keys(writtenData.features)).toHaveLength(21);
      expect(writtenData.features).toEqual(PLAN_FEATURES_FALLBACK.pro);
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });
  });

  describe("PAYMENT_FAILED → past_due", () => {
    test("sets status past_due without modifying planId, features, or interval", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro", features: PLAN_FEATURES_FALLBACK.pro, interval: "month", status: "active" },
        }),
      });

      await processWebhookEvent({
        type: PAYMENT_EVENT_TYPES.PAYMENT_FAILED,
        data: {},
        userId: "test-uid",
        eventId: "evt-2",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.status).toBe("past_due");
      expect(writtenData.updatedAt).toBe("2026-04-06T12:00:00.000Z");
      expect(writtenData).not.toHaveProperty("planId");
      expect(writtenData).not.toHaveProperty("features");
      expect(writtenData).not.toHaveProperty("interval");
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });
  });

  describe("MEMBERSHIP_ACTIVATED → metadata update only", () => {
    test("updates subscriptionId, manageUrl, currentPeriodEnd without overwriting planId/features/status", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro", features: PLAN_FEATURES_FALLBACK.pro, status: "active" },
        }),
      });

      const payload = {
        id: "mem_xxx",
        user: { id: "user_xxx" },
        manage_url: "https://whop.com/billing/manage/mem_xxx",
        renewal_period_end: "2026-05-06T00:00:00Z",
      };

      await processWebhookEvent({
        type: PAYMENT_EVENT_TYPES.MEMBERSHIP_ACTIVATED,
        data: payload,
        userId: "test-uid",
        eventId: "evt-3",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.subscriptionId).toBe("mem_xxx");
      expect(writtenData.providerCustomerId).toBe("user_xxx");
      expect(writtenData.manageUrl).toBe("https://whop.com/billing/manage/mem_xxx");
      expect(writtenData.currentPeriodEnd).toBe("2026-05-06T00:00:00Z");
      expect(writtenData.updatedAt).toBe("2026-04-06T12:00:00.000Z");
      expect(writtenData).not.toHaveProperty("planId");
      expect(writtenData).not.toHaveProperty("features");
      expect(writtenData).not.toHaveProperty("status");
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });
  });

  describe("MEMBERSHIP_DEACTIVATED → degradation to Free", () => {
    test("degrades to free plan preserving trial sticky fields", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: {
            planId: "pro",
            hasUsedTrial: true,
            trialStartedAt: "2026-03-01T00:00:00.000Z",
            trialEndedAt: "2026-03-31T00:00:00.000Z",
          },
        }),
      });

      await processWebhookEvent({
        type: PAYMENT_EVENT_TYPES.MEMBERSHIP_DEACTIVATED,
        data: {},
        userId: "test-uid",
        eventId: "evt-4",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.planId).toBe("free");
      expect(writtenData.status).toBe("active");
      expect(Object.keys(writtenData.features)).toHaveLength(21);
      expect(writtenData.features).toEqual(PLAN_FEATURES_FALLBACK.free);
      expect(writtenData.hasUsedTrial).toBe(true);
      expect(writtenData.trialStartedAt).toBe("2026-03-01T00:00:00.000Z");
      expect(writtenData.trialEndedAt).toBe("2026-03-31T00:00:00.000Z");
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });
  });
});

// ============================================================================
// Fase 2: Cancel + Reason Preservation Tests
// ============================================================================

describe("Cancel flow — CANCEL_AT_PERIOD_END_CHANGED", () => {
  describe("cancel toggle", () => {
    test("cancel=true writes cancelAtPeriodEnd, reason, comment, cancelledAt", async () => {
      const payload = {
        cancel_at_period_end: true,
        cancel_option: "too_expensive",
        cancellation_reason: "Muy caro para mí",
        canceled_at: "2026-04-06T10:00:00Z",
      };

      await processWebhookEvent({
        type: PAYMENT_EVENT_TYPES.CANCEL_AT_PERIOD_END_CHANGED,
        data: payload,
        userId: "test-uid",
        eventId: "evt-cancel-1",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.cancelAtPeriodEnd).toBe(true);
      expect(writtenData.cancellationReason).toBe("too_expensive");
      expect(writtenData.cancellationComment).toBe("Muy caro para mí");
      expect(writtenData.cancelledAt).toBe("2026-04-06T10:00:00Z");
      expect(writtenData.updatedAt).toBe("2026-04-06T12:00:00.000Z");
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });

    test("cancel=false (uncancel) clears cancellation fields via FieldValue.delete()", async () => {
      const payload = { cancel_at_period_end: false };

      await processWebhookEvent({
        type: PAYMENT_EVENT_TYPES.CANCEL_AT_PERIOD_END_CHANGED,
        data: payload,
        userId: "test-uid",
        eventId: "evt-cancel-2",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.cancelAtPeriodEnd).toBe(false);
      expect(writtenData.cancellationReason).toBe(FIELD_VALUE_DELETE_SENTINEL);
      expect(writtenData.cancellationComment).toBe(FIELD_VALUE_DELETE_SENTINEL);
      expect(writtenData.cancelledAt).toBe(FIELD_VALUE_DELETE_SENTINEL);
      expect(writtenData.updatedAt).toBe("2026-04-06T12:00:00.000Z");
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });
  });

  describe("cancel reason preservation", () => {
    test("webhook with null cancel_option does NOT overwrite existing cancellationReason from UI", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { cancellationReason: "too_expensive" },
        }),
      });

      const payload = {
        cancel_at_period_end: true,
        cancel_option: null,
        cancellation_reason: null,
      };

      await processWebhookEvent({
        type: PAYMENT_EVENT_TYPES.CANCEL_AT_PERIOD_END_CHANGED,
        data: payload,
        userId: "test-uid",
        eventId: "evt-cancel-preserve-1",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.cancelAtPeriodEnd).toBe(true);
      expect(writtenData).not.toHaveProperty("cancellationReason");
      expect(writtenData).not.toHaveProperty("cancellationComment");
    });

    test("webhook with cancel_option value writes cancellationReason when none exists", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: {} }),
      });

      const payload = {
        cancel_at_period_end: true,
        cancel_option: "switching",
      };

      await processWebhookEvent({
        type: PAYMENT_EVENT_TYPES.CANCEL_AT_PERIOD_END_CHANGED,
        data: payload,
        userId: "test-uid",
        eventId: "evt-cancel-preserve-2",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.cancellationReason).toBe("switching");
    });
  });
});

// ============================================================================
// Fase 3: Lifetime + Race Condition Tests
// ============================================================================

describe("Lifetime flow", () => {
  test("PAYMENT_SUCCEEDED Lifetime writes planId lifetime, interval lifetime, currentPeriodEnd null, purchasedAt present", async () => {
    const payload = {
      plan: { id: "plan_lifetime" },
      membership: { id: "mem_lt", status: "active" },
      user: { id: "user_lt" },
    };

    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
      data: payload,
      userId: "test-uid",
      eventId: "evt-lt-1",
    });

    expect(mockSet).toHaveBeenCalledTimes(1);
    const writtenData = mockSet.mock.calls[0][0].subscription;
    expect(writtenData.planId).toBe("lifetime");
    expect(writtenData.interval).toBe("lifetime");
    expect(writtenData.status).toBe("active");
    expect(writtenData.currentPeriodEnd).toBeNull();
    expect(writtenData.purchasedAt).toBe("2026-04-06T12:00:00.000Z");
    expect(Object.keys(writtenData.features)).toHaveLength(21);
    expect(writtenData.features).toEqual(PLAN_FEATURES_FALLBACK.lifetime);
    expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
  });

  test("MEMBERSHIP_ACTIVATED with status completed only updates metadata, not planId/features", async () => {
    mockGet.mockResolvedValue({
      data: () => ({
        subscription: { planId: "lifetime", features: PLAN_FEATURES_FALLBACK.lifetime, status: "active" },
      }),
    });

    const payload = {
      id: "mem_lt",
      user: { id: "user_lt" },
      manage_url: "https://whop.com/billing/manage/mem_lt",
      renewal_period_end: null,
      status: "completed",
    };

    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.MEMBERSHIP_ACTIVATED,
      data: payload,
      userId: "test-uid",
      eventId: "evt-lt-2",
    });

    expect(mockSet).toHaveBeenCalledTimes(1);
    const writtenData = mockSet.mock.calls[0][0].subscription;
    expect(writtenData.subscriptionId).toBe("mem_lt");
    expect(writtenData.manageUrl).toBe("https://whop.com/billing/manage/mem_lt");
    expect(writtenData.currentPeriodEnd).toBeNull();
    expect(writtenData).not.toHaveProperty("planId");
    expect(writtenData).not.toHaveProperty("features");
    expect(writtenData).not.toHaveProperty("status");
  });

  test("complete Lifetime sequence: PAYMENT_SUCCEEDED then MEMBERSHIP_ACTIVATED both use merge:true", async () => {
    const paymentPayload = {
      plan: { id: "plan_lifetime" },
      membership: { id: "mem_lt", status: "active" },
      user: { id: "user_lt" },
    };

    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
      data: paymentPayload,
      userId: "test-uid",
      eventId: "evt-lt-seq-1",
    });

    const firstWrite = mockSet.mock.calls[0][0].subscription;
    expect(firstWrite.planId).toBe("lifetime");
    expect(firstWrite.features).toEqual(PLAN_FEATURES_FALLBACK.lifetime);
    expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });

    mockSet.mockClear();
    mockGet.mockResolvedValue({
      data: () => ({
        subscription: { planId: "lifetime", features: PLAN_FEATURES_FALLBACK.lifetime, status: "active" },
      }),
    });

    const activatedPayload = {
      id: "mem_lt",
      user: { id: "user_lt" },
      manage_url: "https://whop.com/billing/manage/mem_lt",
      renewal_period_end: null,
    };

    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.MEMBERSHIP_ACTIVATED,
      data: activatedPayload,
      userId: "test-uid",
      eventId: "evt-lt-seq-2",
    });

    const secondWrite = mockSet.mock.calls[0][0].subscription;
    expect(secondWrite.manageUrl).toBe("https://whop.com/billing/manage/mem_lt");
    expect(secondWrite).not.toHaveProperty("planId");
    expect(secondWrite).not.toHaveProperty("features");
    expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
  });
});

describe("Webhook order independence (race condition)", () => {
  const proPaymentPayload = {
    plan: { id: "plan_pro_monthly" },
    membership: { id: "mem_pro", status: "active" },
    user: { id: "user_pro" },
  };

  const activatedPayload = {
    id: "mem_pro",
    user: { id: "user_pro" },
    manage_url: "https://whop.com/billing/manage/mem_pro",
    renewal_period_end: "2026-05-06T00:00:00Z",
  };

  test("order: PAYMENT_SUCCEEDED then MEMBERSHIP_ACTIVATED — both writes use merge:true with orthogonal fields", async () => {
    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
      data: proPaymentPayload,
      userId: "test-uid",
      eventId: "evt-race-1a",
    });

    const paymentWrite = mockSet.mock.calls[0][0].subscription;
    expect(paymentWrite.planId).toBe("pro");
    expect(paymentWrite.features).toBeDefined();
    expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });

    mockSet.mockClear();

    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.MEMBERSHIP_ACTIVATED,
      data: activatedPayload,
      userId: "test-uid",
      eventId: "evt-race-1b",
    });

    const activatedWrite = mockSet.mock.calls[0][0].subscription;
    expect(activatedWrite.manageUrl).toBe("https://whop.com/billing/manage/mem_pro");
    expect(activatedWrite.currentPeriodEnd).toBe("2026-05-06T00:00:00Z");
    expect(activatedWrite).not.toHaveProperty("planId");
    expect(activatedWrite).not.toHaveProperty("features");
    expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
  });

  test("order: MEMBERSHIP_ACTIVATED then PAYMENT_SUCCEEDED — same orthogonal writes, both merge:true", async () => {
    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.MEMBERSHIP_ACTIVATED,
      data: activatedPayload,
      userId: "test-uid",
      eventId: "evt-race-2a",
    });

    const activatedWrite = mockSet.mock.calls[0][0].subscription;
    expect(activatedWrite.manageUrl).toBe("https://whop.com/billing/manage/mem_pro");
    expect(activatedWrite.currentPeriodEnd).toBe("2026-05-06T00:00:00Z");
    expect(activatedWrite).not.toHaveProperty("planId");
    expect(activatedWrite).not.toHaveProperty("features");
    expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });

    mockSet.mockClear();

    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
      data: proPaymentPayload,
      userId: "test-uid",
      eventId: "evt-race-2b",
    });

    const paymentWrite = mockSet.mock.calls[0][0].subscription;
    expect(paymentWrite.planId).toBe("pro");
    expect(paymentWrite.features).toBeDefined();
    expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
  });

  test("both orders produce compatible writes — PAYMENT_SUCCEEDED and MEMBERSHIP_ACTIVATED fields are orthogonal", async () => {
    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
      data: proPaymentPayload,
      userId: "test-uid",
      eventId: "evt-race-compat-1",
    });

    const paymentFields = Object.keys(mockSet.mock.calls[0][0].subscription);

    mockSet.mockClear();

    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.MEMBERSHIP_ACTIVATED,
      data: activatedPayload,
      userId: "test-uid",
      eventId: "evt-race-compat-2",
    });

    const activatedFields = Object.keys(mockSet.mock.calls[0][0].subscription);

    const sharedFields = paymentFields.filter((f) => activatedFields.includes(f));
    const nonConflictingShared = ["subscriptionId", "providerCustomerId", "updatedAt", "currentPeriodEnd"];
    sharedFields.forEach((field) => {
      expect(nonConflictingShared).toContain(field);
    });
  });
});

describe("all Whop event writes use merge: true", () => {
  test("PAYMENT_SUCCEEDED, PAYMENT_FAILED, MEMBERSHIP_ACTIVATED, MEMBERSHIP_DEACTIVATED, CANCEL_AT_PERIOD_END_CHANGED all merge", async () => {
    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
      data: { plan: { id: "plan_pro_monthly" }, membership: { id: "mem_1" }, user: { id: "user_1" } },
      userId: "uid-merge",
      eventId: "evt-merge-1",
    });

    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.PAYMENT_FAILED,
      data: {},
      userId: "uid-merge",
      eventId: "evt-merge-2",
    });

    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.MEMBERSHIP_ACTIVATED,
      data: { id: "mem_1", user: { id: "user_1" }, manage_url: "https://whop.com/m/1", renewal_period_end: "2026-05-01" },
      userId: "uid-merge",
      eventId: "evt-merge-3",
    });

    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.MEMBERSHIP_DEACTIVATED,
      data: {},
      userId: "uid-merge",
      eventId: "evt-merge-4",
    });

    await processWebhookEvent({
      type: PAYMENT_EVENT_TYPES.CANCEL_AT_PERIOD_END_CHANGED,
      data: { cancel_at_period_end: true },
      userId: "uid-merge",
      eventId: "evt-merge-5",
    });

    expect(mockSet).toHaveBeenCalledTimes(5);
    for (const call of mockSet.mock.calls) {
      expect(call[1]).toEqual({ merge: true });
    }
  });
});
