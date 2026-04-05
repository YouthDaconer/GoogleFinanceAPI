const { PLAN_FEATURES_FALLBACK, _resetCache } = require("../../payment/planFeatures");
const { PAYMENT_EVENT_TYPES } = require("../../payment/paymentProvider");

// Mock Firestore — supports userData (doc/get/set) and planDefinitions (collection/get)
const mockSet = jest.fn().mockResolvedValue();
const mockGet = jest.fn();
const mockCollectionGet = jest.fn().mockResolvedValue({ forEach: jest.fn() });
const mockDoc = jest.fn(() => ({ set: mockSet, get: mockGet }));
const mockCollection = jest.fn(() => ({ doc: mockDoc, get: mockCollectionGet }));

jest.mock("../../firebaseAdmin", () => ({
  firestore: () => ({ collection: mockCollection }),
}));

const mockCircuitExecute = jest.fn((primaryFn) => primaryFn());
jest.mock("../../../utils/circuitBreaker", () => ({
  getCircuit: jest.fn(() => ({ execute: mockCircuitExecute })),
}));

jest.mock("../../payment/providerFactory", () => ({
  getPaymentProvider: jest.fn(() => null),
}));

const { processWebhookEvent } = require("../../payment/subscriptionService");

beforeEach(() => {
  jest.clearAllMocks();
  jest.useFakeTimers();
  jest.setSystemTime(new Date("2026-04-04T12:00:00.000Z"));
  _resetCache();
  mockCollectionGet.mockResolvedValue({ forEach: jest.fn() });
  mockSet.mockResolvedValue();
  mockGet.mockResolvedValue({ data: () => ({ subscription: {} }) });
});

afterEach(() => {
  jest.useRealTimers();
});

describe("Webhook → Firestore integration flows", () => {
  const baseWebhookResult = {
    userId: "test-uid",
    subscriptionId: "sub_123",
    customerId: "cust_456",
    rawData: {},
  };

  describe("CHECKOUT_COMPLETED Pro", () => {
    test("writes pro subscription with 21 features and subscriptionOrigin: checkout", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
        planId: "pro",
        interval: "month",
      });

      expect(mockCollection).toHaveBeenCalledWith("userData");
      expect(mockDoc).toHaveBeenCalledWith("test-uid");
      expect(mockSet).toHaveBeenCalledTimes(1);

      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.planId).toBe("pro");
      expect(writtenData.status).toBe("active");
      expect(writtenData.subscriptionOrigin).toBe("checkout");
      expect(writtenData.subscriptionId).toBe("sub_123");
      expect(writtenData.providerCustomerId).toBe("cust_456");
      expect(Object.keys(writtenData.features)).toHaveLength(21);
      expect(writtenData.features).toEqual(PLAN_FEATURES_FALLBACK.pro);
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });
  });

  describe("CHECKOUT_COMPLETED Lifetime", () => {
    test("writes lifetime subscription with currentPeriodEnd=null and purchasedAt present", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
        planId: "lifetime",
        interval: "lifetime",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.planId).toBe("lifetime");
      expect(writtenData.status).toBe("active");
      expect(writtenData.currentPeriodEnd).toBeNull();
      expect(writtenData.purchasedAt).toBe("2026-04-04T12:00:00.000Z");
      expect(writtenData.interval).toBe("lifetime");
      expect(writtenData.subscriptionOrigin).toBe("checkout");
      expect(Object.keys(writtenData.features)).toHaveLength(21);
      expect(writtenData.features).toEqual(PLAN_FEATURES_FALLBACK.lifetime);
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });
  });

  describe("PAYMENT_FAILED", () => {
    test("marks status as past_due without changing features or planId", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.PAYMENT_FAILED,
        planId: "pro",
        interval: "month",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.status).toBe("past_due");
      expect(writtenData).not.toHaveProperty("features");
      expect(writtenData).not.toHaveProperty("planId");
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });
  });

  describe("PAYMENT_SUCCEEDED recovery", () => {
    test("restores status to active when previous status was past_due", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { status: "past_due" } }),
      });

      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
        planId: "pro",
        interval: "month",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.status).toBe("active");
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });

    test("does not write when status is already active", async () => {
      mockGet.mockResolvedValue({
        data: () => ({ subscription: { status: "active" } }),
      });

      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
        planId: "pro",
        interval: "month",
      });

      expect(mockSet).not.toHaveBeenCalled();
    });
  });

  describe("SUBSCRIPTION_CANCELED", () => {
    test("degrades to Free plan with 21 features", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.SUBSCRIPTION_CANCELED,
        planId: "pro",
        interval: "month",
      });

      expect(mockSet).toHaveBeenCalledTimes(1);
      const writtenData = mockSet.mock.calls[0][0].subscription;
      expect(writtenData.planId).toBe("free");
      expect(writtenData.status).toBe("canceled");
      expect(Object.keys(writtenData.features)).toHaveLength(21);
      expect(writtenData.features).toEqual(PLAN_FEATURES_FALLBACK.free);
      expect(writtenData.subscriptionId).toBeNull();
      expect(writtenData.providerCustomerId).toBeNull();
      expect(mockSet.mock.calls[0][1]).toEqual({ merge: true });
    });
  });

  describe("unknown event type", () => {
    test("does not write to Firestore and does not throw", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: "some_unknown_event",
        planId: "pro",
        interval: "month",
      });

      expect(mockSet).not.toHaveBeenCalled();
    });
  });

  describe("all writes use merge: true", () => {
    test("CHECKOUT_COMPLETED, SUBSCRIPTION_CANCELED, and PAYMENT_FAILED all merge", async () => {
      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
        planId: "pro",
        interval: "month",
      });

      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.SUBSCRIPTION_CANCELED,
        planId: "pro",
        interval: "month",
      });

      await processWebhookEvent({
        ...baseWebhookResult,
        type: PAYMENT_EVENT_TYPES.PAYMENT_FAILED,
        planId: "pro",
        interval: "month",
      });

      expect(mockSet).toHaveBeenCalledTimes(3);
      for (const call of mockSet.mock.calls) {
        expect(call[1]).toEqual({ merge: true });
      }
    });
  });
});
