const mockInitiateCheckout = jest.fn();
const mockCreatePortalSession = jest.fn();

jest.mock("../../payment/subscriptionService", () => ({
  initiateCheckout: mockInitiateCheckout,
  createPortalSession: mockCreatePortalSession,
  processWebhookEvent: jest.fn(),
}));

jest.mock("../../payment/webhookHandler", () => ({
  handleWebhook: jest.fn(),
}));

jest.mock("../../firebaseAdmin", () => ({
  firestore: () => ({
    collection: jest.fn(() => ({
      doc: jest.fn(() => ({
        get: jest.fn().mockResolvedValue({ data: () => ({}) }),
        set: jest.fn().mockResolvedValue(),
      })),
    })),
  }),
}));

// Capture CF handlers registered via onCall/onRequest
jest.mock("firebase-functions/v2/https", () => ({
  onCall: jest.fn((opts, handler) => handler),
  onRequest: jest.fn((opts, handler) => handler),
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

jest.mock("firebase-functions/v2/scheduler", () => ({
  onSchedule: jest.fn(() => jest.fn()),
}));

jest.mock("dotenv", () => ({ config: jest.fn() }));
jest.mock("../../../httpApi", () => ({}));
jest.mock("../../unifiedMarketDataUpdate", () => ({ unifiedMarketDataUpdate: jest.fn() }));
jest.mock("../../reconcileStalePerformance", () => ({ reconcileStalePerformance: jest.fn() }));
jest.mock("../../processDividendPayments", () => jest.fn());
jest.mock("../../marketStatusService", () => jest.fn());
jest.mock("../../marketDataScheduled", () => ({
  saveIndicesHistoryData: jest.fn(),
  saveSectorsSnapshot: jest.fn(),
  updateRiskFreeRate: jest.fn(),
}));
jest.mock("../../calculateProfitableWeeks", () => ({ calculateProfitableWeeks: jest.fn() }));
jest.mock("../../historicalReturnsService", () => jest.fn());
jest.mock("../../indexHistoryService", () => jest.fn());
jest.mock("../../../utils/rateLimiter", () => ({
  withRateLimit: jest.fn(() => (handler) => handler),
  getRateLimitConfig: jest.fn(),
  RATE_LIMITS_COLLECTION: "rateLimits",
}));
jest.mock("../../../utils/circuitBreaker", () => ({
  getAllCircuitStates: jest.fn(),
  resetCircuit: jest.fn(),
}));
jest.mock("../../unified/portfolioOperations", () => ({ portfolioOperations: jest.fn() }));
jest.mock("../../unified/settingsOperations", () => ({ settingsOperations: jest.fn() }));
jest.mock("../../unified/accountOperations", () => ({ accountOperations: jest.fn() }));
jest.mock("../../unified/queryOperations", () => ({ queryOperations: jest.fn() }));
jest.mock("../../marketDataTokenService", () => ({ getMarketDataToken: jest.fn() }));
jest.mock("../../transactions", () => ({
  analyzeTransactionFile: jest.fn(),
  importTransactionBatch: jest.fn(),
}));
jest.mock("../../periodConsolidationScheduled", () => ({
  consolidateMonthlyPerformance: jest.fn(),
  consolidateYearlyPerformance: jest.fn(),
}));
jest.mock("../../payment/mockSubscriptionService", () => ({ mockSetSubscription: jest.fn() }));
jest.mock("../../payment/cancelSubscriptionService", () => ({ cancelSubscription: jest.fn() }));

let indexModule;
beforeAll(() => {
  indexModule = require("../../../index");
});

describe("STRIPE-001: Payment Cloud Functions (index.js)", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("createCheckoutSession", () => {
    test("rejects unauthenticated requests", async () => {
      await expect(
        indexModule.createCheckoutSession({ auth: null, data: { planId: "pro", interval: "month" } })
      ).rejects.toMatchObject({ code: "unauthenticated" });
    });

    test("rejects invalid planId", async () => {
      await expect(
        indexModule.createCheckoutSession({
          auth: { uid: "uid-1", token: { email: "a@b.com" } },
          data: { planId: "invalid" },
        })
      ).rejects.toMatchObject({ code: "invalid-argument" });
    });

    test("rejects Pro plan without valid interval", async () => {
      await expect(
        indexModule.createCheckoutSession({
          auth: { uid: "uid-1", token: { email: "a@b.com" } },
          data: { planId: "pro", interval: "weekly" },
        })
      ).rejects.toMatchObject({ code: "invalid-argument" });
    });

    test("rejects Pro plan without interval", async () => {
      await expect(
        indexModule.createCheckoutSession({
          auth: { uid: "uid-1", token: { email: "a@b.com" } },
          data: { planId: "pro" },
        })
      ).rejects.toMatchObject({ code: "invalid-argument" });
    });

    test("accepts lifetime plan and calls initiateCheckout", async () => {
      mockInitiateCheckout.mockResolvedValue({ url: "https://checkout.example.com" });

      const result = await indexModule.createCheckoutSession({
        auth: { uid: "uid-1", token: { email: "a@b.com" } },
        data: { planId: "lifetime" },
      });

      expect(mockInitiateCheckout).toHaveBeenCalledWith("uid-1", "a@b.com", "lifetime", "lifetime");
      expect(result).toEqual({ url: "https://checkout.example.com" });
    });

    test("accepts pro/month and calls initiateCheckout", async () => {
      mockInitiateCheckout.mockResolvedValue({ url: "https://checkout.example.com/pro" });

      const result = await indexModule.createCheckoutSession({
        auth: { uid: "uid-1", token: { email: "a@b.com" } },
        data: { planId: "pro", interval: "month" },
      });

      expect(mockInitiateCheckout).toHaveBeenCalledWith("uid-1", "a@b.com", "pro", "month");
      expect(result).toEqual({ url: "https://checkout.example.com/pro" });
    });
  });

  describe("createPortalSession", () => {
    test("rejects unauthenticated requests", async () => {
      await expect(
        indexModule.createPortalSession({ auth: null, data: {} })
      ).rejects.toMatchObject({ code: "unauthenticated" });
    });

    test("calls createPortalSession with uid and returns result", async () => {
      mockCreatePortalSession.mockResolvedValue({ url: "https://portal.example.com" });

      const result = await indexModule.createPortalSession({
        auth: { uid: "uid-1", token: { email: "a@b.com" } },
        data: {},
      });

      expect(mockCreatePortalSession).toHaveBeenCalledWith("uid-1");
      expect(result).toEqual({ url: "https://portal.example.com" });
    });
  });

  describe("handlePaymentWebhook", () => {
    test("is exported and is a function", () => {
      expect(indexModule.handlePaymentWebhook).toBeDefined();
      expect(typeof indexModule.handlePaymentWebhook).toBe("function");
    });
  });
});
