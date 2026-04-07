const { PAYMENT_EVENT_TYPES } = require("../../payment/paymentProvider");

const mockEventGet = jest.fn();
const mockEventSet = jest.fn().mockResolvedValue();
const mockUserGet = jest.fn();
const mockUserSet = jest.fn().mockResolvedValue();

const mockEventDocRef = { get: mockEventGet, set: mockEventSet };
const mockUserDocRef = {
  get: mockUserGet,
  set: mockUserSet,
};

const mockCollection = jest.fn((name) => ({
  doc: jest.fn((id) => {
    if (name === "subscriptionEvents") return mockEventDocRef;
    return mockUserDocRef;
  }),
}));

jest.mock("../../firebaseAdmin", () => {
  const firestoreFn = () => ({ collection: mockCollection });
  firestoreFn.FieldValue = {
    delete: () => "FIELD_VALUE_DELETE",
    serverTimestamp: jest.fn(() => "SERVER_TIMESTAMP"),
  };
  const admin = {
    firestore: Object.assign(firestoreFn, { FieldValue: firestoreFn.FieldValue }),
    auth: () => ({
      getUserByEmail: jest.fn().mockRejectedValue(new Error("not found")),
    }),
  };
  return admin;
});

jest.mock("../../payment/subscriptionService", () => ({
  processWebhookEvent: jest.fn().mockResolvedValue(),
}));

const mockSendTransactionalEmail = jest.fn().mockResolvedValue();
jest.mock("../../payment/emailService", () => ({
  sendTransactionalEmail: (...args) => mockSendTransactionalEmail(...args),
  EMAIL_TEMPLATES: Object.freeze({
    PAYMENT_FAILED: "payment_failed",
    PAYMENT_RENEWED: "payment_renewed",
  }),
}));

const mockParseWebhook = jest.fn();
jest.mock("../../payment/providerFactory", () => ({
  getPaymentProvider: jest.fn(() => ({
    parseWebhook: (...args) => mockParseWebhook(...args),
  })),
}));

const { handleWebhook } = require("../../payment/webhookHandler");
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

function createMockReq(eventId) {
  return {
    headers: {
      "webhook-id": eventId,
      "webhook-signature": "v1,test-sig",
      "webhook-timestamp": "1712404800",
    },
    rawBody: Buffer.from(JSON.stringify({ data: {} })),
  };
}

beforeEach(() => {
  jest.clearAllMocks();

  mockParseWebhook.mockResolvedValue({
    type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
    eventId: "msg_abc123",
    data: {
      plan: { id: "plan_pro_monthly" },
      membership: { id: "mem_test", status: "active" },
      user: { id: "user_test" },
      metadata: { firebase_uid: "abc123def456ghi789jk" },
    },
    rawType: "payment.succeeded",
  });

  mockEventGet.mockResolvedValue({ exists: false });
  mockUserGet.mockResolvedValue({
    data: () => ({ email: "test@example.com", displayName: "Test" }),
  });
});

describe("Idempotency via handleWebhook()", () => {
  test("first invocation processes event and writes subscriptionEvents record", async () => {
    const req = createMockReq("msg_abc123");
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(processWebhookEvent).toHaveBeenCalledTimes(1);
    expect(processWebhookEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
        userId: "abc123def456ghi789jk",
        eventId: "msg_abc123",
      })
    );

    expect(mockEventSet).toHaveBeenCalledTimes(1);
    expect(mockEventSet).toHaveBeenCalledWith(
      expect.objectContaining({
        type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
        rawType: "payment.succeeded",
        userId: "abc123def456ghi789jk",
        provider: "whop",
        result: "success",
      })
    );

    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ received: true });
  });

  test("second invocation with same eventId returns duplicate without processing", async () => {
    mockEventGet.mockResolvedValue({ exists: true });

    const req = createMockReq("msg_abc123");
    const res = createMockRes();

    await handleWebhook(req, res);

    expect(processWebhookEvent).not.toHaveBeenCalled();
    expect(mockEventSet).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ received: true, duplicate: true });
  });

  test("sequential calls: first processes, second deduplicates", async () => {
    const req = createMockReq("msg_abc123");

    const res1 = createMockRes();
    await handleWebhook(req, res1);
    expect(processWebhookEvent).toHaveBeenCalledTimes(1);
    expect(res1.body).toEqual({ received: true });

    processWebhookEvent.mockClear();
    mockEventGet.mockResolvedValue({ exists: true });

    const res2 = createMockRes();
    await handleWebhook(req, res2);
    expect(processWebhookEvent).not.toHaveBeenCalled();
    expect(res2.body).toEqual({ received: true, duplicate: true });
  });
});
