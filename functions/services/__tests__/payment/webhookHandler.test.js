// Mock Firestore transaction primitives
const mockEventDocGet = jest.fn();
const mockTransactionGet = jest.fn();
const mockTransactionSet = jest.fn();
const mockRunTransaction = jest.fn();
const mockDoc = jest.fn();
const mockCollectionDoc = jest.fn();
const mockCollection = jest.fn(() => ({ doc: mockCollectionDoc }));

jest.mock("../../firebaseAdmin", () => {
  const firestoreFn = () => ({
    doc: mockDoc,
    collection: mockCollection,
    runTransaction: mockRunTransaction,
  });
  firestoreFn.FieldValue = { serverTimestamp: jest.fn(() => "SERVER_TIMESTAMP") };
  return { firestore: firestoreFn };
});

const mockProvider = {
  createCheckoutSession: jest.fn(),
  createPortalSession: jest.fn(),
  getSubscription: jest.fn(),
  parseWebhook: jest.fn(),
};

jest.mock("../../payment/providerFactory", () => ({
  getPaymentProvider: jest.fn(() => mockProvider),
}));

jest.mock("../../payment/subscriptionService", () => ({
  processWebhookEvent: jest.fn(),
}));

const { handleWebhook, extractEventId, deriveAfterState } = require("../../payment/webhookHandler");
const { processWebhookEvent } = require("../../payment/subscriptionService");

function createMockRes() {
  const res = {
    statusCode: null,
    body: null,
    status: jest.fn(function (code) {
      res.statusCode = code;
      return res;
    }),
    json: jest.fn(function (body) {
      res.body = body;
      return res;
    }),
  };
  return res;
}

beforeEach(() => {
  jest.clearAllMocks();
  processWebhookEvent.mockResolvedValue();

  // Default: transaction executes its callback inline
  mockRunTransaction.mockImplementation(async (fn) => {
    const transaction = { get: mockTransactionGet, set: mockTransactionSet };
    return await fn(transaction);
  });

  // Default: event doc does NOT exist (new event), user doc has Free plan
  mockTransactionGet.mockImplementation((ref) => {
    if (ref?._isUserRef) {
      return Promise.resolve({
        exists: true,
        data: () => ({ subscription: { planId: "free", status: "active" } }),
      });
    }
    return Promise.resolve({ exists: false });
  });
  mockDoc.mockReturnValue({ id: "test-event-id" });
  mockCollectionDoc.mockImplementation(() => ({ _isUserRef: true }));
});

describe("extractEventId", () => {
  test("returns eventId from webhookResult.eventId", () => {
    expect(extractEventId({ eventId: "evt-123" })).toBe("evt-123");
  });

  test("falls back to rawData.meta.webhook_event_id", () => {
    expect(extractEventId({ rawData: { meta: { webhook_event_id: "evt-456" } } })).toBe("evt-456");
  });

  test("returns null when no eventId available", () => {
    expect(extractEventId({})).toBeNull();
    expect(extractEventId({ rawData: {} })).toBeNull();
  });
});

describe("handleWebhook", () => {
  test("valid signature with new eventId processes and writes subscriptionEvent", async () => {
    const webhookResult = {
      type: "checkout_completed",
      userId: "uid-1",
      planId: "pro",
      interval: "month",
      rawData: { meta: { webhook_event_id: "evt-100", event_name: "order_created" } },
    };
    mockProvider.parseWebhook.mockResolvedValue(webhookResult);

    const req = { headers: { "x-signature": "valid-sig" }, rawBody: '{"meta": {}}' };
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(mockProvider.parseWebhook).toHaveBeenCalledWith('{"meta": {}}', "valid-sig");
    expect(processWebhookEvent).toHaveBeenCalledWith(webhookResult, expect.objectContaining({ get: expect.any(Function), set: expect.any(Function) }));
    expect(mockTransactionSet).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        eventId: "evt-100",
        type: "checkout_completed",
        userId: "uid-1",
        processedAt: "SERVER_TIMESTAMP",
        raw: { meta: { webhook_event_id: "evt-100", event_name: "order_created" } },
        before: { planId: "free", status: "active" },
        after: { planId: "pro", status: "active" },
      })
    );
    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.body).toEqual({ received: true });
  });

  test("duplicate eventId deduplicates without calling processWebhookEvent", async () => {
    mockTransactionGet.mockResolvedValue({ exists: true });

    const webhookResult = {
      type: "checkout_completed",
      userId: "uid-1",
      rawData: { meta: { webhook_event_id: "evt-dup" } },
    };
    mockProvider.parseWebhook.mockResolvedValue(webhookResult);

    const req = { headers: { "x-signature": "valid-sig" }, rawBody: '{"meta": {}}' };
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(processWebhookEvent).not.toHaveBeenCalled();
    expect(mockTransactionSet).not.toHaveBeenCalled();
    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.body).toEqual({ received: true });
  });

  test("invalid signature responds 400", async () => {
    mockProvider.parseWebhook.mockRejectedValue(new Error("Invalid webhook signature"));

    const req = { headers: { "x-signature": "bad-sig" }, rawBody: '{"meta": {}}' };
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(res.status).toHaveBeenCalledWith(400);
    expect(res.body).toEqual({ error: "Invalid webhook signature" });
  });

  test("missing x-signature header responds 400", async () => {
    const req = { headers: {}, rawBody: '{"meta": {}}' };
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(res.status).toHaveBeenCalledWith(400);
    expect(res.body).toEqual({ error: "Missing x-signature header" });
  });

  test("unknown event type responds 200 without processing or writing subscriptionEvent", async () => {
    mockProvider.parseWebhook.mockResolvedValue({
      type: "unknown",
      userId: "uid-1",
      rawData: { meta: { event_name: "something_unknown", webhook_event_id: "evt-unk" } },
    });

    const req = { headers: { "x-signature": "valid-sig" }, rawBody: '{"meta": {}}' };
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(processWebhookEvent).not.toHaveBeenCalled();
    expect(mockRunTransaction).not.toHaveBeenCalled();
    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.body).toEqual({ received: true, processed: false });
  });

  test("webhook without eventId processes without transaction (graceful degradation)", async () => {
    const webhookResult = {
      type: "checkout_completed",
      userId: "uid-1",
      planId: "pro",
      interval: "month",
      rawData: { meta: { event_name: "order_created" } },
    };
    mockProvider.parseWebhook.mockResolvedValue(webhookResult);

    const req = { headers: { "x-signature": "valid-sig" }, rawBody: '{"meta": {}}' };
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(processWebhookEvent).toHaveBeenCalledWith(webhookResult);
    expect(mockRunTransaction).not.toHaveBeenCalled();
    expect(res.status).toHaveBeenCalledWith(200);
  });

  test("Firestore transaction error responds 500 (LS retries)", async () => {
    mockRunTransaction.mockRejectedValue(new Error("Firestore unavailable"));

    const webhookResult = {
      type: "checkout_completed",
      userId: "uid-1",
      rawData: { meta: { webhook_event_id: "evt-fail" } },
    };
    mockProvider.parseWebhook.mockResolvedValue(webhookResult);

    const req = { headers: { "x-signature": "valid-sig" }, rawBody: '{"meta": {}}' };
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.body).toEqual({ error: "Webhook processing failed" });
  });

  test("processing error without eventId responds 500", async () => {
    processWebhookEvent.mockRejectedValue(new Error("DB write failed"));

    mockProvider.parseWebhook.mockResolvedValue({
      type: "checkout_completed",
      userId: "uid-1",
      planId: "pro",
      rawData: { meta: {} },
    });

    const req = { headers: { "x-signature": "valid-sig" }, rawBody: '{"meta": {}}' };
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.body).toEqual({ error: "Webhook processing failed" });
  });
});

describe("Audit Trail Enriquecido (PAY-005 F5-03)", () => {
  test("CHECKOUT_COMPLETED for Free user → before:free/active, after:pro/active", async () => {
    mockTransactionGet.mockImplementation((ref) => {
      if (ref?._isUserRef) {
        return Promise.resolve({
          exists: true,
          data: () => ({ subscription: { planId: "free", status: "active" } }),
        });
      }
      return Promise.resolve({ exists: false });
    });

    const webhookResult = {
      type: "checkout_completed",
      userId: "uid-1",
      planId: "pro",
      interval: "month",
      rawData: { meta: { webhook_event_id: "evt-audit-1" } },
    };
    mockProvider.parseWebhook.mockResolvedValue(webhookResult);

    const req = { headers: { "x-signature": "valid-sig" }, rawBody: '{"meta": {}}' };
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(mockTransactionSet).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        before: { planId: "free", status: "active" },
        after: { planId: "pro", status: "active" },
      })
    );
  });

  test("webhook for new user (no subscription) → before:null/null", async () => {
    mockTransactionGet.mockImplementation((ref) => {
      if (ref?._isUserRef) {
        return Promise.resolve({
          exists: false,
          data: () => null,
        });
      }
      return Promise.resolve({ exists: false });
    });

    const webhookResult = {
      type: "checkout_completed",
      userId: "uid-new",
      planId: "pro",
      interval: "month",
      rawData: { meta: { webhook_event_id: "evt-audit-2" } },
    };
    mockProvider.parseWebhook.mockResolvedValue(webhookResult);

    const req = { headers: { "x-signature": "valid-sig" }, rawBody: '{"meta": {}}' };
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(mockTransactionSet).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        before: { planId: null, status: null },
        after: { planId: "pro", status: "active" },
      })
    );
  });

  test("SUBSCRIPTION_CANCELED → after:free/canceled", async () => {
    mockTransactionGet.mockImplementation((ref) => {
      if (ref?._isUserRef) {
        return Promise.resolve({
          exists: true,
          data: () => ({ subscription: { planId: "pro", status: "active" } }),
        });
      }
      return Promise.resolve({ exists: false });
    });

    const webhookResult = {
      type: "subscription_canceled",
      userId: "uid-1",
      rawData: { meta: { webhook_event_id: "evt-audit-3" } },
    };
    mockProvider.parseWebhook.mockResolvedValue(webhookResult);

    const req = { headers: { "x-signature": "valid-sig" }, rawBody: '{"meta": {}}' };
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(mockTransactionSet).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        before: { planId: "pro", status: "active" },
        after: { planId: "free", status: "canceled" },
      })
    );
  });
});

describe("deriveAfterState", () => {
  test("maps all event types correctly", () => {
    expect(deriveAfterState({ type: "checkout_completed", planId: "pro" }))
      .toEqual({ planId: "pro", status: "active" });
    expect(deriveAfterState({ type: "subscription_canceled" }))
      .toEqual({ planId: "free", status: "canceled" });
    expect(deriveAfterState({ type: "payment_failed" }))
      .toEqual({ planId: null, status: "past_due" });
    expect(deriveAfterState({ type: "payment_succeeded" }))
      .toEqual({ planId: null, status: "active" });
    expect(deriveAfterState({ type: "unknown_event" }))
      .toEqual({ planId: null, status: null });
  });
});
