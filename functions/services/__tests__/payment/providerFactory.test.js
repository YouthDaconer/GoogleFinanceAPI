const mockProvider = {
  createCheckoutSession: jest.fn(),
  createPortalSession: jest.fn(),
  getSubscription: jest.fn(),
  parseWebhook: jest.fn(),
};

const mockCreateProvider = jest.fn(() => mockProvider);

jest.mock("../../payment/lemonSqueezyProvider", () => ({
  createLemonSqueezyProvider: mockCreateProvider,
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
  test("PAYMENT_PROVIDER=lemonsqueezy returns provider with 4 functions", () => {
    process.env.PAYMENT_PROVIDER = "lemonsqueezy";

    const provider = getPaymentProvider();

    expect(provider).toBeTruthy();
    expect(typeof provider.createCheckoutSession).toBe("function");
    expect(typeof provider.createPortalSession).toBe("function");
    expect(typeof provider.getSubscription).toBe("function");
    expect(typeof provider.parseWebhook).toBe("function");
  });

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

  test("singleton: returns same instance on repeated calls", () => {
    process.env.PAYMENT_PROVIDER = "lemonsqueezy";

    const first = getPaymentProvider();
    const second = getPaymentProvider();

    expect(first).toBe(second);
    expect(mockCreateProvider).toHaveBeenCalledTimes(1);
  });
});
