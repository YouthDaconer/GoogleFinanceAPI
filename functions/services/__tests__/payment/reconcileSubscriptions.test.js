const mockSet = jest.fn().mockResolvedValue();
const mockBatchSet = jest.fn();
const mockBatchCommit = jest.fn().mockResolvedValue();
const mockGet = jest.fn();
const mockWhere = jest.fn();
const mockLimit = jest.fn();
const mockDoc = jest.fn(() => ({ set: mockSet }));
const mockEventDoc = jest.fn(() => ({ id: "event-doc-ref" }));

const buildQueryChain = () => {
  mockWhere.mockReturnValue({ where: mockWhere, limit: mockLimit });
  mockLimit.mockReturnValue({ get: mockGet });
  return { where: mockWhere, limit: mockLimit, get: mockGet };
};

jest.mock("../../firebaseAdmin", () => {
  const firestoreFn = () => ({
    collection: jest.fn((name) => {
      if (name === "subscriptionEvents") return { doc: mockEventDoc };
      return { doc: mockDoc, where: mockWhere };
    }),
    batch: jest.fn(() => ({
      set: mockBatchSet,
      commit: mockBatchCommit,
    })),
  });
  firestoreFn.FieldValue = { serverTimestamp: jest.fn(() => "SERVER_TIMESTAMP") };
  return { firestore: firestoreFn };
});

jest.mock("../../payment/planFeatures", () => ({
  buildSubscriptionData: jest.fn((planId, interval) => ({
    planId,
    interval,
    status: "active",
    features: {},
    updatedAt: "2026-04-04T03:00:00.000Z",
  })),
}));

jest.mock("../../payment/providerFactory", () => ({
  getPaymentProvider: jest.fn(() => null),
}));

const mockCircuitExecute = jest.fn();
jest.mock("../../../utils/circuitBreaker", () => ({
  getCircuit: jest.fn(() => ({ execute: mockCircuitExecute })),
}));

jest.mock("firebase-functions/v2/scheduler", () => ({
  onSchedule: jest.fn((_opts, handler) => handler),
}));

const {
  processExpiredActive,
  processPendingCancel,
  processStalePastDue,
  reconcileWithProvider,
  LIMITS,
} = require("../../payment/reconcileSubscriptions");
const { getPaymentProvider } = require("../../payment/providerFactory");

beforeEach(() => {
  jest.clearAllMocks();
  buildQueryChain();
  delete process.env.RECONCILE_WITH_PROVIDER;
});

function createUserDoc(id, subscription) {
  const docData = { subscription };
  return {
    id,
    ref: {
      set: mockSet,
      get: jest.fn().mockResolvedValue({ data: () => docData }),
    },
    data: () => docData,
  };
}

describe("processExpiredActive", () => {
  test("degrades active subscription with expired currentPeriodEnd to Free", async () => {
    const now = new Date("2026-04-04T03:00:00.000Z");
    const doc = createUserDoc("uid-expired", {
      status: "active",
      planId: "pro",
      currentPeriodEnd: "2026-04-01T00:00:00.000Z",
    });
    mockGet.mockResolvedValue({ docs: [doc] });

    const counters = { period_expired: 0, errors: 0 };
    await processExpiredActive(now, counters);

    expect(counters.period_expired).toBe(1);
    expect(mockBatchSet).toHaveBeenCalledWith(
      doc.ref,
      { subscription: expect.objectContaining({ planId: "free" }) },
      { merge: true }
    );
    expect(mockBatchSet).toHaveBeenCalledWith(
      { id: "event-doc-ref" },
      expect.objectContaining({
        type: "RECONCILIATION_DOWNGRADE",
        userId: "uid-expired",
        reason: "period_expired",
      })
    );
    expect(mockBatchCommit).toHaveBeenCalled();
  });

  test("skips Lifetime subscriptions (even if currentPeriodEnd is set)", async () => {
    const now = new Date("2026-04-04T03:00:00.000Z");
    const doc = createUserDoc("uid-lifetime", {
      status: "active",
      planId: "lifetime",
      currentPeriodEnd: null,
    });
    mockGet.mockResolvedValue({ docs: [doc] });

    const counters = { period_expired: 0, errors: 0 };
    await processExpiredActive(now, counters);

    expect(counters.period_expired).toBe(0);
    expect(mockBatchCommit).not.toHaveBeenCalled();
  });

  test("individual error does not stop the batch", async () => {
    const now = new Date("2026-04-04T03:00:00.000Z");
    const failDoc = createUserDoc("uid-fail", {
      status: "active",
      planId: "pro",
      currentPeriodEnd: "2026-04-01T00:00:00.000Z",
    });

    const okDoc = createUserDoc("uid-ok", {
      status: "active",
      planId: "pro",
      currentPeriodEnd: "2026-04-01T00:00:00.000Z",
    });

    mockGet.mockResolvedValue({ docs: [failDoc, okDoc] });
    mockBatchCommit
      .mockRejectedValueOnce(new Error("Firestore write failed"))
      .mockResolvedValueOnce();

    const counters = { period_expired: 0, errors: 0 };
    await processExpiredActive(now, counters);

    expect(counters.errors).toBe(1);
    expect(counters.period_expired).toBe(1);
  });
});

describe("processPendingCancel", () => {
  test("degrades cancelAtPeriodEnd subscription with past date to Free", async () => {
    const now = new Date("2026-04-04T03:00:00.000Z");
    const doc = createUserDoc("uid-cancel", {
      cancelAtPeriodEnd: true,
      currentPeriodEnd: "2026-04-01T00:00:00.000Z",
    });
    mockGet.mockResolvedValue({ docs: [doc] });

    const counters = { cancel_at_period_end: 0, errors: 0 };
    await processPendingCancel(now, counters);

    expect(counters.cancel_at_period_end).toBe(1);
    expect(mockBatchSet).toHaveBeenCalledWith(
      { id: "event-doc-ref" },
      expect.objectContaining({
        type: "RECONCILIATION_DOWNGRADE",
        reason: "cancel_at_period_end",
      })
    );
  });

  test("individual error does not stop the batch", async () => {
    const now = new Date("2026-04-04T03:00:00.000Z");
    const failDoc = createUserDoc("uid-cancel-fail", {
      cancelAtPeriodEnd: true,
      currentPeriodEnd: "2026-04-01T00:00:00.000Z",
    });

    const okDoc = createUserDoc("uid-cancel-ok", {
      cancelAtPeriodEnd: true,
      currentPeriodEnd: "2026-04-01T00:00:00.000Z",
    });

    mockGet.mockResolvedValue({ docs: [failDoc, okDoc] });
    mockBatchCommit
      .mockRejectedValueOnce(new Error("Write failed"))
      .mockResolvedValueOnce();

    const counters = { cancel_at_period_end: 0, errors: 0 };
    await processPendingCancel(now, counters);

    expect(counters.errors).toBe(1);
    expect(counters.cancel_at_period_end).toBe(1);
  });
});

describe("processStalePastDue", () => {
  test("degrades past_due > 30 days to Free", async () => {
    const now = new Date("2026-04-04T03:00:00.000Z");
    const doc = createUserDoc("uid-pastdue", {
      status: "past_due",
      updatedAt: "2026-02-01T00:00:00.000Z",
    });
    mockGet.mockResolvedValue({ docs: [doc] });

    const counters = { past_due_timeout: 0, errors: 0 };
    await processStalePastDue(now, counters);

    expect(counters.past_due_timeout).toBe(1);
    expect(mockBatchSet).toHaveBeenCalledWith(
      { id: "event-doc-ref" },
      expect.objectContaining({
        type: "RECONCILIATION_DOWNGRADE",
        reason: "past_due_timeout",
      })
    );
  });

  test("does NOT degrade past_due < 30 days (still in grace)", async () => {
    const now = new Date("2026-04-04T03:00:00.000Z");
    const doc = createUserDoc("uid-recent-pastdue", {
      status: "past_due",
      updatedAt: "2026-03-20T00:00:00.000Z",
    });
    mockGet.mockResolvedValue({ docs: [doc] });

    const counters = { past_due_timeout: 0, errors: 0 };
    await processStalePastDue(now, counters);

    expect(counters.past_due_timeout).toBe(0);
    expect(mockBatchCommit).not.toHaveBeenCalled();
  });

  test("individual error does not stop the batch", async () => {
    const now = new Date("2026-04-04T03:00:00.000Z");
    const failDoc = createUserDoc("uid-pastdue-fail", {
      status: "past_due",
      updatedAt: "2026-02-01T00:00:00.000Z",
    });

    const okDoc = createUserDoc("uid-pastdue-ok", {
      status: "past_due",
      updatedAt: "2026-02-01T00:00:00.000Z",
    });

    mockGet.mockResolvedValue({ docs: [failDoc, okDoc] });
    mockBatchCommit
      .mockRejectedValueOnce(new Error("Write failed"))
      .mockResolvedValueOnce();

    const counters = { past_due_timeout: 0, errors: 0 };
    await processStalePastDue(now, counters);

    expect(counters.errors).toBe(1);
    expect(counters.past_due_timeout).toBe(1);
  });

  test("skips doc without updatedAt", async () => {
    const now = new Date("2026-04-04T03:00:00.000Z");
    const doc = createUserDoc("uid-no-updated", {
      status: "past_due",
    });
    mockGet.mockResolvedValue({ docs: [doc] });

    const counters = { past_due_timeout: 0, errors: 0 };
    await processStalePastDue(now, counters);

    expect(counters.past_due_timeout).toBe(0);
    expect(mockBatchCommit).not.toHaveBeenCalled();
  });
});

describe("reconcileWithProvider", () => {
  test("RECONCILE_WITH_PROVIDER=false → skips provider reconciliation", async () => {
    process.env.RECONCILE_WITH_PROVIDER = "false";
    const counters = { errors: 0 };

    await reconcileWithProvider(counters);

    expect(getPaymentProvider).not.toHaveBeenCalled();
  });

  test("RECONCILE_WITH_PROVIDER=true + circuit open → skip without error", async () => {
    process.env.RECONCILE_WITH_PROVIDER = "true";
    const mockProvider = {
      getSubscription: jest.fn(),
    };
    getPaymentProvider.mockReturnValue(mockProvider);

    const doc = createUserDoc("uid-prov", {
      status: "active",
      subscriptionId: "sub-123",
    });
    mockGet.mockResolvedValue({ docs: [doc] });
    mockCircuitExecute.mockResolvedValue(null);

    const counters = { errors: 0, provider_discrepancy: 0 };
    await reconcileWithProvider(counters);

    expect(counters.provider_discrepancy).toBe(0);
    expect(counters.errors).toBe(0);
  });

  test("RECONCILE_WITH_PROVIDER=true + discrepancy → corrects Firestore", async () => {
    process.env.RECONCILE_WITH_PROVIDER = "true";
    const mockProvider = {
      getSubscription: jest.fn(),
    };
    getPaymentProvider.mockReturnValue(mockProvider);

    const doc = createUserDoc("uid-disc", {
      status: "active",
      subscriptionId: "sub-456",
    });
    mockGet.mockResolvedValue({ docs: [doc] });
    mockCircuitExecute.mockResolvedValue({ status: "cancelled" });

    const counters = { errors: 0, provider_discrepancy: 0 };
    await reconcileWithProvider(counters);

    expect(counters.provider_discrepancy).toBe(1);
    expect(mockBatchCommit).toHaveBeenCalled();
  });
});

describe("LIMITS", () => {
  test("MAX_USERS_PER_RUN is 50", () => {
    expect(LIMITS.MAX_USERS_PER_RUN).toBe(50);
  });

  test("PAST_DUE_TIMEOUT_DAYS is 30", () => {
    expect(LIMITS.PAST_DUE_TIMEOUT_DAYS).toBe(30);
  });
});

// PAY-009: Trial field preservation in degradeToFree
describe("trial field preservation (PAY-009)", () => {
  const { degradeToFree } = require("../../payment/reconcileSubscriptions");

  beforeEach(() => {
    jest.clearAllMocks();
    buildQueryChain();
  });

  test("preserves hasUsedTrial and trialStartedAt when degrading trial subscription", async () => {
    const doc = createUserDoc("uid-trial-expire", {
      status: "active",
      planId: "pro",
      subscriptionOrigin: "trial",
      hasUsedTrial: true,
      trialStartedAt: "2026-03-01T00:00:00.000Z",
    });

    const counters = { period_expired: 0, errors: 0 };
    await degradeToFree(doc.ref, doc.id, "period_expired", counters);

    expect(counters.period_expired).toBe(1);
    const writtenSub = mockBatchSet.mock.calls[0][1].subscription;
    expect(writtenSub.hasUsedTrial).toBe(true);
    expect(writtenSub.trialStartedAt).toBe("2026-03-01T00:00:00.000Z");
    expect(writtenSub.trialEndedAt).toBeDefined();
  });

  test("does NOT set trialEndedAt for non-trial subscriptions", async () => {
    const doc = createUserDoc("uid-checkout-expire", {
      status: "active",
      planId: "pro",
      subscriptionOrigin: "checkout",
      hasUsedTrial: true,
      trialStartedAt: "2026-02-01T00:00:00.000Z",
    });

    const counters = { period_expired: 0, errors: 0 };
    await degradeToFree(doc.ref, doc.id, "period_expired", counters);

    const writtenSub = mockBatchSet.mock.calls[0][1].subscription;
    expect(writtenSub.hasUsedTrial).toBe(true);
    expect(writtenSub.trialEndedAt).toBeUndefined();
  });
});
