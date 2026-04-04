const crypto = require("crypto");

const mockLsSetup = jest.fn();
const mockCreateCheckout = jest.fn();
const mockGetSubscription = jest.fn();
const mockGetCustomer = jest.fn();
const mockUpdateSubscription = jest.fn();

jest.mock("@lemonsqueezy/lemonsqueezy.js", () => ({
  lemonSqueezySetup: mockLsSetup,
  createCheckout: mockCreateCheckout,
  getSubscription: mockGetSubscription,
  getCustomer: mockGetCustomer,
  updateSubscription: mockUpdateSubscription,
}));

const ORIGINAL_ENV = process.env;

beforeEach(() => {
  jest.clearAllMocks();
  process.env = {
    ...ORIGINAL_ENV,
    LEMONSQUEEZY_API_KEY: "test-api-key",
    LEMONSQUEEZY_WEBHOOK_SECRET: "test-webhook-secret",
    LEMONSQUEEZY_STORE_ID: "store-123",
    LEMONSQUEEZY_VARIANT_PRO_MONTHLY: "variant-pm",
    LEMONSQUEEZY_VARIANT_PRO_ANNUAL: "variant-pa",
    LEMONSQUEEZY_VARIANT_LIFETIME: "variant-lt",
  };
});

afterAll(() => {
  process.env = ORIGINAL_ENV;
});

const { createLemonSqueezyProvider } = require("../../payment/lemonSqueezyProvider");

describe("createLemonSqueezyProvider", () => {
  test("returns object with 5 functions", () => {
    const provider = createLemonSqueezyProvider();

    expect(typeof provider.createCheckoutSession).toBe("function");
    expect(typeof provider.createPortalSession).toBe("function");
    expect(typeof provider.getSubscription).toBe("function");
    expect(typeof provider.cancelSubscription).toBe("function");
    expect(typeof provider.parseWebhook).toBe("function");
  });

  test("throws if LEMONSQUEEZY_API_KEY is not set", () => {
    delete process.env.LEMONSQUEEZY_API_KEY;

    expect(() => createLemonSqueezyProvider()).toThrow("LEMONSQUEEZY_API_KEY");
  });

  test("calls lemonSqueezySetup with API key", () => {
    createLemonSqueezyProvider();

    expect(mockLsSetup).toHaveBeenCalledWith({ apiKey: "test-api-key" });
  });
});

describe("createCheckoutSession", () => {
  test("calls createCheckout with correct params including customData", async () => {
    mockCreateCheckout.mockResolvedValue({
      data: { data: { attributes: { url: "https://checkout.lemonsqueezy.com/test" } } },
    });

    const provider = createLemonSqueezyProvider();
    const result = await provider.createCheckoutSession({
      userId: "uid-123",
      email: "test@test.com",
      planId: "pro",
      interval: "month",
    });

    expect(result).toEqual({ checkoutUrl: "https://checkout.lemonsqueezy.com/test" });
    expect(mockCreateCheckout).toHaveBeenCalledWith(
      "store-123",
      "variant-pm",
      expect.objectContaining({
        checkoutData: expect.objectContaining({
          email: "test@test.com",
          custom: { user_id: "uid-123", plan_id: "pro", interval: "month" },
        }),
      })
    );
  });

  test("throws for unknown variant", async () => {
    const provider = createLemonSqueezyProvider();

    await expect(
      provider.createCheckoutSession({
        userId: "uid-123",
        email: "test@test.com",
        planId: "unknown",
        interval: "month",
      })
    ).rejects.toThrow("No variant configured");
  });

  test("throws on SDK error", async () => {
    mockCreateCheckout.mockResolvedValue({ error: { message: "SDK error" } });

    const provider = createLemonSqueezyProvider();

    await expect(
      provider.createCheckoutSession({
        userId: "uid-123",
        email: "test@test.com",
        planId: "pro",
        interval: "month",
      })
    ).rejects.toThrow("Lemon Squeezy checkout failed");
  });
});

describe("createPortalSession", () => {
  test("returns portalUrl from customer data", async () => {
    mockGetCustomer.mockResolvedValue({
      data: {
        data: {
          attributes: {
            urls: { customer_portal: "https://portal.lemonsqueezy.com/test" },
          },
        },
      },
    });

    const provider = createLemonSqueezyProvider();
    const result = await provider.createPortalSession("customer-123");

    expect(result).toEqual({ portalUrl: "https://portal.lemonsqueezy.com/test" });
    expect(mockGetCustomer).toHaveBeenCalledWith("customer-123");
  });
});

describe("parseWebhook — HMAC-SHA256 verification", () => {
  function buildSignedPayload(payload, secret) {
    const raw = JSON.stringify(payload);
    const hmac = crypto.createHmac("sha256", secret);
    const signature = hmac.update(raw).digest("hex");
    return { raw, signature };
  }

  const samplePayload = {
    meta: {
      event_name: "order_created",
      custom_data: { user_id: "uid-456", plan_id: "pro", interval: "month" },
    },
    data: {
      id: "sub-789",
      attributes: { customer_id: 42 },
    },
  };

  test("valid signature returns WebhookResult with userId and mapped type", async () => {
    const { raw, signature } = buildSignedPayload(samplePayload, "test-webhook-secret");

    const provider = createLemonSqueezyProvider();
    const result = await provider.parseWebhook(raw, signature);

    expect(result.type).toBe("checkout_completed");
    expect(result.userId).toBe("uid-456");
    expect(result.planId).toBe("pro");
    expect(result.interval).toBe("month");
    expect(result.subscriptionId).toBe("sub-789");
    expect(result.customerId).toBe("42");
    expect(result.rawData).toEqual(samplePayload);
  });

  test("invalid signature throws error", async () => {
    const raw = JSON.stringify(samplePayload);

    const provider = createLemonSqueezyProvider();

    await expect(provider.parseWebhook(raw, "invalid-signature")).rejects.toThrow(
      "Invalid webhook signature"
    );
  });

  test("missing LEMONSQUEEZY_WEBHOOK_SECRET throws", async () => {
    delete process.env.LEMONSQUEEZY_WEBHOOK_SECRET;

    const provider = createLemonSqueezyProvider();

    await expect(provider.parseWebhook("body", "sig")).rejects.toThrow(
      "LEMONSQUEEZY_WEBHOOK_SECRET"
    );
  });

  test("unknown event name maps to 'unknown'", async () => {
    const unknownPayload = {
      ...samplePayload,
      meta: { ...samplePayload.meta, event_name: "some_future_event" },
    };
    const { raw, signature } = buildSignedPayload(unknownPayload, "test-webhook-secret");

    const provider = createLemonSqueezyProvider();
    const result = await provider.parseWebhook(raw, signature);

    expect(result.type).toBe("unknown");
  });

  test("subscription_cancelled maps to SUBSCRIPTION_CANCELED", async () => {
    const cancelPayload = {
      ...samplePayload,
      meta: { ...samplePayload.meta, event_name: "subscription_cancelled" },
    };
    const { raw, signature } = buildSignedPayload(cancelPayload, "test-webhook-secret");

    const provider = createLemonSqueezyProvider();
    const result = await provider.parseWebhook(raw, signature);

    expect(result.type).toBe("subscription_canceled");
  });
});

describe("cancelSubscription", () => {
  test("calls updateSubscription with cancelled: true and returns effectiveDate", async () => {
    mockUpdateSubscription.mockResolvedValue({
      data: {
        data: {
          attributes: {
            status: "active",
            ends_at: "2026-05-01T00:00:00.000Z",
          },
        },
      },
    });

    const provider = createLemonSqueezyProvider();
    const result = await provider.cancelSubscription("sub-123");

    expect(mockUpdateSubscription).toHaveBeenCalledWith("sub-123", { cancelled: true });
    expect(result).toEqual({
      success: true,
      effectiveDate: "2026-05-01T00:00:00.000Z",
      alreadyCanceled: false,
    });
  });

  test("returns alreadyCanceled: true when status is cancelled", async () => {
    mockUpdateSubscription.mockResolvedValue({
      data: {
        data: {
          attributes: {
            status: "cancelled",
            ends_at: "2026-04-15T00:00:00.000Z",
          },
        },
      },
    });

    const provider = createLemonSqueezyProvider();
    const result = await provider.cancelSubscription("sub-456");

    expect(result).toEqual({
      success: true,
      effectiveDate: "2026-04-15T00:00:00.000Z",
      alreadyCanceled: true,
    });
  });

  test("throws on SDK error", async () => {
    mockUpdateSubscription.mockResolvedValue({
      error: { message: "Subscription not found" },
    });

    const provider = createLemonSqueezyProvider();

    await expect(provider.cancelSubscription("sub-999")).rejects.toThrow(
      "LS cancel failed: Subscription not found"
    );
  });
});
