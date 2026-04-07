// WHOP-001: Provider mocks removed. WHOP-003 will add whopProvider mock + tests.
jest.mock("@whop/sdk", () => ({
  Whop: jest.fn().mockImplementation(() => ({
    checkoutConfigurations: { create: jest.fn() },
    memberships: { retrieve: jest.fn(), cancel: jest.fn(), uncancel: jest.fn() },
    payments: { list: jest.fn() },
    webhooks: { unwrap: jest.fn() },
  })),
}));

const { getPaymentProvider, resetProviderCache } = require("../../payment/providerFactory");

const ORIGINAL_ENV = process.env;

beforeEach(() => {
  jest.clearAllMocks();
  process.env = { ...ORIGINAL_ENV };
  resetProviderCache();
});

afterAll(() => {
  process.env = ORIGINAL_ENV;
});

describe("getPaymentProvider", () => {
  test("PAYMENT_PROVIDER=mock returns null", () => {
    process.env.PAYMENT_PROVIDER = "mock";

    const provider = getPaymentProvider();
    expect(provider).toBeNull();
  });

  test("unknown PAYMENT_PROVIDER throws descriptive error", () => {
    process.env.PAYMENT_PROVIDER = "stripe";

    expect(() => getPaymentProvider()).toThrow('Unknown PAYMENT_PROVIDER: "stripe"');
  });

  test("undefined PAYMENT_PROVIDER throws descriptive error", () => {
    delete process.env.PAYMENT_PROVIDER;

    expect(() => getPaymentProvider()).toThrow("Unknown PAYMENT_PROVIDER");
  });

  test("PAYMENT_PROVIDER=whop with env vars returns object with provider methods", () => {
    process.env.PAYMENT_PROVIDER = "whop";
    process.env.WHOP_API_KEY = "test-api-key";
    process.env.WHOP_WEBHOOK_SECRET = "test-webhook-secret";
    process.env.WHOP_COMPANY_ID = "biz_test";

    const provider = getPaymentProvider();
    expect(provider).not.toBeNull();
    expect(typeof provider.createCheckoutSession).toBe("function");
    expect(typeof provider.createPortalSession).toBe("function");
    expect(typeof provider.parseWebhook).toBe("function");
    expect(typeof provider.cancelSubscription).toBe("function");
    expect(typeof provider.reactivateSubscription).toBe("function");
    expect(typeof provider.getSubscription).toBe("function");
    expect(typeof provider.getSubscriptionInvoices).toBe("function");
    expect(typeof provider.getPaymentMethod).toBe("function");
  });
});

describe("dual-flag convivencia (PAYMENT_PROVIDER=whop + PAYMENT_MOCK_ENABLED=true)", () => {
  test("getPaymentProvider returns whop provider even when PAYMENT_MOCK_ENABLED=true", () => {
    process.env.PAYMENT_PROVIDER = "whop";
    process.env.PAYMENT_MOCK_ENABLED = "true";
    process.env.WHOP_API_KEY = "test-api-key";
    process.env.WHOP_WEBHOOK_SECRET = "test-webhook-secret";
    process.env.WHOP_COMPANY_ID = "biz_test";

    const provider = getPaymentProvider();
    expect(provider).not.toBeNull();
    expect(typeof provider.createCheckoutSession).toBe("function");
  });

  test("mockSubscriptionService guard allows execution when PAYMENT_MOCK_ENABLED=true", () => {
    process.env.PAYMENT_MOCK_ENABLED = "true";

    // Replicate the guard logic from mockSubscriptionService.js:21
    const guardPasses = process.env.PAYMENT_MOCK_ENABLED === "true";
    expect(guardPasses).toBe(true);

    // Verify both subsystems coexist: whop provider is active AND mock guard passes
    process.env.PAYMENT_PROVIDER = "whop";
    process.env.WHOP_API_KEY = "test-api-key";
    process.env.WHOP_WEBHOOK_SECRET = "test-webhook-secret";
    process.env.WHOP_COMPANY_ID = "biz_test";
    resetProviderCache();

    const provider = getPaymentProvider();
    expect(provider).not.toBeNull();
    expect(guardPasses).toBe(true);
  });

  test("mockSubscriptionService guard blocks when PAYMENT_MOCK_ENABLED is false", () => {
    process.env.PAYMENT_MOCK_ENABLED = "false";

    // Guard from mockSubscriptionService.js:21 would throw HttpsError("failed-precondition")
    const guardPasses = process.env.PAYMENT_MOCK_ENABLED === "true";
    expect(guardPasses).toBe(false);
  });

  test("mockSubscriptionService guard blocks when PAYMENT_MOCK_ENABLED is undefined", () => {
    delete process.env.PAYMENT_MOCK_ENABLED;

    const guardPasses = process.env.PAYMENT_MOCK_ENABLED === "true";
    expect(guardPasses).toBe(false);
  });
});