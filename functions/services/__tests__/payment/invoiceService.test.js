const mockSet = jest.fn().mockResolvedValue();
const mockGet = jest.fn();
const mockDoc = jest.fn(() => ({ set: mockSet, get: mockGet }));
const mockLimit = jest.fn();
const mockOrderBy = jest.fn(() => ({ limit: mockLimit }));
const mockWhere = jest.fn(() => ({ orderBy: mockOrderBy }));
const mockCollection = jest.fn((name) => {
  if (name === "subscriptionEvents") {
    return { where: mockWhere };
  }
  return { doc: mockDoc };
});
const mockServerTimestamp = jest.fn(() => "SERVER_TIMESTAMP");

const mockFirestore = Object.assign(() => ({ collection: mockCollection }), {
  FieldValue: { serverTimestamp: mockServerTimestamp },
});

jest.mock("../../firebaseAdmin", () => ({
  firestore: mockFirestore,
}));

const mockGetSubscriptionInvoices = jest.fn();
const mockGetPaymentProvider = jest.fn(() => ({
  getSubscriptionInvoices: mockGetSubscriptionInvoices,
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

require("../../payment/invoiceService");

describe("getSubscriptionInvoices", () => {
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

  test("returns empty invoices for Free plan", async () => {
    mockGet.mockResolvedValue({
      data: () => ({ subscription: { planId: "free" } }),
    });

    const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

    expect(result).toEqual({ invoices: [], source: "none" });
  });

  test("returns empty invoices when no subscription exists", async () => {
    mockGet.mockResolvedValue({ data: () => ({}) });

    const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

    expect(result).toEqual({ invoices: [], source: "none" });
  });

  describe("real mode with subscriptionId", () => {
    beforeEach(() => {
      process.env.PAYMENT_MOCK_ENABLED = "false";
    });

    test("returns invoices from provider API with source provider", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro", subscriptionId: "sub-123" },
        }),
      });

      const providerInvoices = [
        {
          id: "inv-1",
          createdAt: "2026-03-01T10:00:00Z",
          total: 999,
          totalFormatted: "$9.99",
          currency: "USD",
          status: "paid",
          invoiceUrl: "https://example.com/invoice/1",
          cardBrand: "visa",
          cardLastFour: "4242",
        },
      ];

      mockExecute.mockImplementation(async (primaryFn) => primaryFn());
      mockGetSubscriptionInvoices.mockResolvedValue(providerInvoices);

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.source).toBe("provider");
      expect(result.invoices).toHaveLength(1);
      expect(result.invoices[0]).toEqual({
        id: "inv-1",
        date: "2026-03-01T10:00:00Z",
        amount: 999,
        amountFormatted: "$9.99",
        currency: "USD",
        status: "paid",
        invoiceUrl: "https://example.com/invoice/1",
        cardBrand: "visa",
        cardLastFour: "4242",
      });
      expect(mockGetCircuit).toHaveBeenCalledWith("whop");
    });

    test("falls back to events when provider API returns empty array", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro", subscriptionId: "sub-123" },
        }),
      });

      mockExecute.mockImplementation(async (primaryFn) => primaryFn());
      mockGetSubscriptionInvoices.mockResolvedValue([]);

      const mockDocs = [
        {
          id: "evt-1",
          data: () => ({
            type: "SUBSCRIPTION_CREATED",
            userId: "user1",
            processedAt: { toDate: () => new Date("2026-03-01T10:00:00Z") },
          }),
        },
      ];
      mockLimit.mockReturnValue({ get: jest.fn().mockResolvedValue({ docs: mockDocs }) });

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.source).toBe("events");
      expect(result.invoices).toHaveLength(1);
      expect(result.invoices[0].type).toBe("SUBSCRIPTION_CREATED");
    });

    test("falls back to events when circuit breaker returns null", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro", subscriptionId: "sub-123" },
        }),
      });

      mockExecute.mockImplementation(async (_primary, fallback) => fallback());

      const mockDocs = [
        {
          id: "evt-1",
          data: () => ({
            type: "SUBSCRIPTION_CREATED",
            userId: "user1",
            processedAt: { toDate: () => new Date("2026-03-01T10:00:00Z") },
          }),
        },
      ];
      mockLimit.mockReturnValue({ get: jest.fn().mockResolvedValue({ docs: mockDocs }) });

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.source).toBe("events");
      expect(result.invoices).toHaveLength(1);
      expect(result.invoices[0].type).toBe("SUBSCRIPTION_CREATED");
      expect(result.invoices[0].amount).toBeNull();
      expect(result.invoices[0].invoiceUrl).toBeNull();
    });

    test("falls back to events when provider is null (mock provider)", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro", subscriptionId: "sub-123" },
        }),
      });

      mockGetPaymentProvider.mockReturnValue(null);

      const mockDocs = [];
      mockLimit.mockReturnValue({ get: jest.fn().mockResolvedValue({ docs: mockDocs }) });

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.source).toBe("events");
      expect(result.invoices).toEqual([]);
    });
  });

  describe("mock mode", () => {
    beforeEach(() => {
      process.env.PAYMENT_MOCK_ENABLED = "true";
    });

    test("skips provider API and falls back to subscriptionEvents", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro", subscriptionId: "sub-123" },
        }),
      });

      const mockDocs = [
        {
          id: "evt-1",
          data: () => ({
            type: "SUBSCRIPTION_CREATED",
            userId: "user1",
            processedAt: { toDate: () => new Date("2026-03-15T10:00:00Z") },
          }),
        },
        {
          id: "evt-2",
          data: () => ({
            type: "SUBSCRIPTION_UPDATED",
            userId: "user1",
            processedAt: null,
          }),
        },
      ];
      mockLimit.mockReturnValue({ get: jest.fn().mockResolvedValue({ docs: mockDocs }) });

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.source).toBe("events");
      expect(result.invoices).toHaveLength(2);
      expect(result.invoices[0].date).toBe("2026-03-15T10:00:00.000Z");
      expect(result.invoices[1].date).toBeNull();
      expect(mockGetPaymentProvider).not.toHaveBeenCalled();
    });

    test("returns empty events when no subscriptionEvents exist", async () => {
      mockGet.mockResolvedValue({
        data: () => ({
          subscription: { planId: "pro" },
        }),
      });

      mockLimit.mockReturnValue({ get: jest.fn().mockResolvedValue({ docs: [] }) });

      const result = await capturedHandler({ auth: { uid: "user1" }, data: {} });

      expect(result.source).toBe("events");
      expect(result.invoices).toEqual([]);
    });
  });

  test("queries subscriptionEvents with limit 20", async () => {
    process.env.PAYMENT_MOCK_ENABLED = "true";

    mockGet.mockResolvedValue({
      data: () => ({
        subscription: { planId: "pro" },
      }),
    });

    mockLimit.mockReturnValue({ get: jest.fn().mockResolvedValue({ docs: [] }) });

    await capturedHandler({ auth: { uid: "user1" }, data: {} });

    expect(mockWhere).toHaveBeenCalledWith("userId", "==", "user1");
    expect(mockOrderBy).toHaveBeenCalledWith("processedAt", "desc");
    expect(mockLimit).toHaveBeenCalledWith(20);
  });
});
