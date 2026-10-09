const mockSet = jest.fn().mockResolvedValue();
const mockGet = jest.fn();
const mockDoc = jest.fn(() => ({ set: mockSet, get: mockGet }));
const mockCollection = jest.fn(() => ({ doc: mockDoc }));

const mockFirestore = Object.assign(() => ({ collection: mockCollection }), {
  FieldValue: { serverTimestamp: jest.fn(() => "SERVER_TIMESTAMP") },
});

jest.mock("../../firebaseAdmin", () => ({
  firestore: mockFirestore,
}));

const mockGetPaymentMethod = jest.fn();
const mockGetPaymentProvider = jest.fn(() => ({
  getPaymentMethod: mockGetPaymentMethod,
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

require("../../payment/paymentMethodService");

describe("paymentMethodService", () => {
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

  test("returns null for Free plan users", async () => {
    mockGet.mockResolvedValue({
      data: () => ({ subscription: { planId: "free" } }),
    });

    const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

    expect(result).toEqual({ paymentMethod: null, reason: "no_active_subscription" });
  });

  test("returns null when no subscription exists", async () => {
    mockGet.mockResolvedValue({ data: () => ({}) });

    const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

    expect(result).toEqual({ paymentMethod: null, reason: "no_active_subscription" });
  });

  test("returns null when subscriptionId is missing", async () => {
    mockGet.mockResolvedValue({
      data: () => ({ subscription: { planId: "pro" } }),
    });

    const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

    expect(result).toEqual({ paymentMethod: null, reason: "no_active_subscription" });
  });

  test("returns null in mock mode", async () => {
    process.env.PAYMENT_MOCK_ENABLED = "true";
    mockGet.mockResolvedValue({
      data: () => ({
        subscription: { planId: "pro", subscriptionId: "sub-123" },
      }),
    });

    const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

    expect(result).toEqual({ paymentMethod: null, reason: "mock_mode" });
    expect(mockGetPaymentProvider).not.toHaveBeenCalled();
  });

  test("returns card info from Whop provider", async () => {
    process.env.PAYMENT_MOCK_ENABLED = "false";
    mockGet.mockResolvedValue({
      data: () => ({
        subscription: { planId: "pro", subscriptionId: "sub-456" },
      }),
    });

    mockExecute.mockImplementation(async (primaryFn) => primaryFn());
    mockGetPaymentMethod.mockResolvedValue({
      cardBrand: "visa",
      cardLastFour: "4242",
    });

    const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

    expect(result).toEqual({
      paymentMethod: { cardBrand: "visa", cardLastFour: "4242" },
    });
    expect(mockGetCircuit).toHaveBeenCalledWith("whop");
  });

  test("returns null when circuit breaker fallback returns null", async () => {
    process.env.PAYMENT_MOCK_ENABLED = "false";
    mockGet.mockResolvedValue({
      data: () => ({
        subscription: { planId: "pro", subscriptionId: "sub-789" },
      }),
    });

    mockExecute.mockImplementation(async (_primaryFn, fallbackFn) => fallbackFn());

    const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

    expect(result).toEqual({ paymentMethod: null, reason: "provider_unavailable" });
  });

  test("returns null on provider exception", async () => {
    const warnSpy = jest.spyOn(console, "warn").mockImplementation();
    process.env.PAYMENT_MOCK_ENABLED = "false";
    mockGet.mockResolvedValue({
      data: () => ({
        subscription: { planId: "pro", subscriptionId: "sub-err" },
      }),
    });

    mockExecute.mockRejectedValue(new Error("Network timeout"));

    const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

    expect(result).toEqual({ paymentMethod: null, reason: "fetch_error" });
    expect(warnSpy).toHaveBeenCalledWith(
      "[PaymentMethod] Failed to fetch:",
      "Network timeout"
    );

    warnSpy.mockRestore();
  });
});
