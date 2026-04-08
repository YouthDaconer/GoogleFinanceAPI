const ORIGINAL_ENV = process.env;

let mockCheckoutCreate;
let mockMembershipsRetrieve;
let mockMembershipsCancel;
let mockMembershipsUncancel;
let mockPaymentsList;
let mockWebhooksUnwrap;

jest.mock("@whop/sdk", () => {
  mockCheckoutCreate = jest.fn();
  mockMembershipsRetrieve = jest.fn();
  mockMembershipsCancel = jest.fn();
  mockMembershipsUncancel = jest.fn();
  mockPaymentsList = jest.fn();
  mockWebhooksUnwrap = jest.fn();

  return {
    Whop: jest.fn(() => ({
      checkoutConfigurations: { create: mockCheckoutCreate },
      memberships: {
        retrieve: mockMembershipsRetrieve,
        cancel: mockMembershipsCancel,
        uncancel: mockMembershipsUncancel,
      },
      payments: { list: mockPaymentsList },
      webhooks: { unwrap: mockWebhooksUnwrap },
    })),
  };
});

const { createWhopProvider, resolvePortastockPlan } = require("../../payment/whopProvider");
const { PAYMENT_EVENT_TYPES } = require("../../payment/paymentProvider");

let provider;

beforeEach(() => {
  jest.clearAllMocks();
  process.env = { ...ORIGINAL_ENV };
  process.env.WHOP_PLAN_PRO_MONTHLY = "plan_pro_m";
  process.env.WHOP_PLAN_PRO_ANNUAL = "plan_pro_a";
  process.env.WHOP_PLAN_LIFETIME = "plan_lt";

  provider = createWhopProvider({
    apiKey: "test-key",
    webhookSecret: "test-secret",
    companyId: "biz_test",
  });
});

afterAll(() => {
  process.env = ORIGINAL_ENV;
});

// ─── createCheckoutSession ──────────────────────────────────────────────────

describe("createCheckoutSession", () => {
  test("AC-01: Pro Monthly calls checkoutConfigurations.create with correct params", async () => {
    mockCheckoutCreate.mockResolvedValue({
      purchase_url: "https://whop.com/checkout/abc",
      id: "ch_001",
    });

    await provider.createCheckoutSession({
      planId: "pro",
      interval: "month",
      userId: "uid_123",
      email: "user@test.com",
      successUrl: "https://app.com/success",
      cancelUrl: "https://app.com/cancel",
    });

    expect(mockCheckoutCreate).toHaveBeenCalledWith({
      company_id: "biz_test",
      plan_id: "plan_pro_m",
      metadata: {
        firebase_uid: "uid_123",
        portastock_plan_id: "pro",
        interval: "month",
      },
      redirect_url: "https://app.com/success",
      source_url: "https://app.com/cancel",
    });
  });

  test("AC-02: returns checkoutUrl and sessionId", async () => {
    mockCheckoutCreate.mockResolvedValue({
      purchase_url: "https://whop.com/checkout/abc",
      id: "ch_001",
    });

    const result = await provider.createCheckoutSession({
      planId: "pro",
      interval: "month",
      userId: "uid_123",
    });

    expect(result).toEqual({
      checkoutUrl: "https://whop.com/checkout/abc",
      sessionId: "ch_001",
    });
  });

  test("AC-16/17: Pro Annual uses WHOP_PLAN_PRO_ANNUAL env var", async () => {
    mockCheckoutCreate.mockResolvedValue({ purchase_url: "url", id: "ch_002" });

    await provider.createCheckoutSession({
      planId: "pro",
      interval: "year",
      userId: "uid_123",
    });

    expect(mockCheckoutCreate).toHaveBeenCalledWith(
      expect.objectContaining({ plan_id: "plan_pro_a" })
    );
  });

  test("AC-18: Lifetime uses WHOP_PLAN_LIFETIME env var", async () => {
    mockCheckoutCreate.mockResolvedValue({ purchase_url: "url", id: "ch_003" });

    await provider.createCheckoutSession({
      planId: "lifetime",
      interval: "lifetime",
      userId: "uid_123",
    });

    expect(mockCheckoutCreate).toHaveBeenCalledWith(
      expect.objectContaining({ plan_id: "plan_lt" })
    );
  });

  test("AC-03/19: unknown plan throws error", async () => {
    await expect(
      provider.createCheckoutSession({
        planId: "unknown",
        interval: "month",
        userId: "uid_123",
      })
    ).rejects.toThrow('No Whop plan configured for planId="unknown", interval="month"');
  });

  test("unset env var throws error", async () => {
    delete process.env.WHOP_PLAN_PRO_MONTHLY;
    provider = createWhopProvider({
      apiKey: "test-key",
      webhookSecret: "test-secret",
      companyId: "biz_test",
    });

    await expect(
      provider.createCheckoutSession({
        planId: "pro",
        interval: "month",
        userId: "uid_123",
      })
    ).rejects.toThrow("Environment variable WHOP_PLAN_PRO_MONTHLY is not set");
  });

  test("WHOP-010 AC-03: trialDays=30 passes trial_period_days to SDK", async () => {
    mockCheckoutCreate.mockResolvedValue({
      purchase_url: "https://whop.com/checkout/trial",
      id: "ch_trial",
    });

    await provider.createCheckoutSession({
      planId: "pro",
      interval: "month",
      userId: "uid_123",
      email: "user@test.com",
      successUrl: "https://app.com/success",
      cancelUrl: "https://app.com/cancel",
      trialDays: 30,
    });

    expect(mockCheckoutCreate).toHaveBeenCalledWith(
      expect.objectContaining({
        plan_id: "plan_pro_m",
        trial_period_days: 30,
      })
    );
  });

  test("WHOP-010: checkout without trial does not include trial_period_days", async () => {
    mockCheckoutCreate.mockResolvedValue({
      purchase_url: "https://whop.com/checkout/abc",
      id: "ch_001",
    });

    await provider.createCheckoutSession({
      planId: "pro",
      interval: "month",
      userId: "uid_123",
    });

    const callArgs = mockCheckoutCreate.mock.calls[0][0];
    expect(callArgs).not.toHaveProperty("trial_period_days");
  });
});

// ─── parseWebhook ───────────────────────────────────────────────────────────

describe("parseWebhook", () => {
  const validHeaders = {
    "webhook-id": "wh_001",
    "webhook-signature": "v1,sig",
    "webhook-timestamp": "1700000000",
  };

  test("AC-04: valid signature returns normalized event", async () => {
    mockWebhooksUnwrap.mockResolvedValue({
      type: "payment.succeeded",
      id: "evt_001",
      data: { amount: 999 },
      company_id: "biz_test",
    });

    const result = await provider.parseWebhook('{"raw":"body"}', validHeaders);

    expect(result).toEqual({
      type: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
      eventId: "evt_001",
      data: { amount: 999 },
      rawType: "payment.succeeded",
      companyId: "biz_test",
    });
  });

  test("unwrap receives rawBody and wrapped headers object", async () => {
    mockWebhooksUnwrap.mockResolvedValue({
      type: "payment.succeeded",
      id: "evt_001",
      data: {},
      company_id: "biz_test",
    });

    await provider.parseWebhook("raw-body-string", validHeaders);

    expect(mockWebhooksUnwrap).toHaveBeenCalledWith("raw-body-string", {
      headers: validHeaders,
    });
  });

  test("AC-05: invalid signature propagates exception", async () => {
    mockWebhooksUnwrap.mockRejectedValue(new Error("Invalid signature"));

    await expect(
      provider.parseWebhook("bad-body", validHeaders)
    ).rejects.toThrow("Invalid signature");
  });

  test("AC-06: expired timestamp propagates exception", async () => {
    mockWebhooksUnwrap.mockRejectedValue(
      new Error("Message timestamp too old")
    );

    await expect(
      provider.parseWebhook("old-body", validHeaders)
    ).rejects.toThrow("Message timestamp too old");
  });

  test("AC-07: all 5 event types normalize correctly", async () => {
    const eventMap = {
      "payment.succeeded": PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
      "payment.failed": PAYMENT_EVENT_TYPES.PAYMENT_FAILED,
      "membership.activated": PAYMENT_EVENT_TYPES.MEMBERSHIP_ACTIVATED,
      "membership.deactivated": PAYMENT_EVENT_TYPES.MEMBERSHIP_DEACTIVATED,
      "membership.cancel_at_period_end_changed":
        PAYMENT_EVENT_TYPES.CANCEL_AT_PERIOD_END_CHANGED,
    };

    for (const [rawType, expectedType] of Object.entries(eventMap)) {
      mockWebhooksUnwrap.mockResolvedValueOnce({
        type: rawType,
        id: `evt_${rawType}`,
        data: {},
        company_id: "biz_test",
      });

      const result = await provider.parseWebhook("{}", validHeaders);
      expect(result.type).toBe(expectedType);
      expect(result.rawType).toBe(rawType);
    }
  });

  test("AC-08: unknown event type returns UNKNOWN_ prefix", async () => {
    mockWebhooksUnwrap.mockResolvedValue({
      type: "some.new.event",
      id: "evt_unk",
      data: {},
      company_id: "biz_test",
    });

    const result = await provider.parseWebhook("{}", validHeaders);

    expect(result.type).toBe("UNKNOWN_some.new.event");
  });
});

// ─── cancelSubscription ─────────────────────────────────────────────────────

describe("cancelSubscription", () => {
  test("AC-09: default mode uses at_period_end", async () => {
    mockMembershipsCancel.mockResolvedValue({
      renewal_period_end: "2026-05-01T00:00:00Z",
    });

    const result = await provider.cancelSubscription("mem_1");

    expect(mockMembershipsCancel).toHaveBeenCalledWith("mem_1", {
      cancellation_mode: "at_period_end",
    });
    expect(result).toEqual({
      success: true,
      effectiveDate: "2026-05-01T00:00:00Z",
    });
  });

  test("AC-10: immediate mode", async () => {
    mockMembershipsCancel.mockResolvedValue({ renewal_period_end: null });

    await provider.cancelSubscription("mem_1", "immediate");

    expect(mockMembershipsCancel).toHaveBeenCalledWith("mem_1", {
      cancellation_mode: "immediate",
    });
  });

  test("invalid cancellation mode throws", () => {
    expect(() => provider.cancelSubscription("mem_1", "force")).rejects.toThrow(
      "Invalid cancellation mode"
    );
  });
});

// ─── reactivateSubscription ─────────────────────────────────────────────────

describe("reactivateSubscription", () => {
  test("AC-11: calls memberships.uncancel and returns success", async () => {
    mockMembershipsUncancel.mockResolvedValue({});

    const result = await provider.reactivateSubscription("mem_1");

    expect(mockMembershipsUncancel).toHaveBeenCalledWith("mem_1");
    expect(result).toEqual({ success: true });
  });
});

// ─── createPortalSession ────────────────────────────────────────────────────

describe("createPortalSession", () => {
  test("AC-11b: returns portalUrl from membership manage_url", async () => {
    mockMembershipsRetrieve.mockResolvedValue({
      manage_url: "https://whop.com/manage/mem_1",
    });

    const result = await provider.createPortalSession("mem_1");

    expect(mockMembershipsRetrieve).toHaveBeenCalledWith("mem_1");
    expect(result).toEqual({ portalUrl: "https://whop.com/manage/mem_1" });
  });

  test("AC-11c: invalid membershipId propagates exception", async () => {
    mockMembershipsRetrieve.mockRejectedValue(new Error("Not found"));

    await expect(provider.createPortalSession("bad_id")).rejects.toThrow(
      "Not found"
    );
  });
});

// ─── getSubscription ────────────────────────────────────────────────────────

describe("getSubscription", () => {
  test("AC-11d: returns full membership object", async () => {
    const membership = {
      id: "mem_1",
      status: "active",
      plan_id: "plan_pro_m",
      renewal_period_end: "2026-05-01",
    };
    mockMembershipsRetrieve.mockResolvedValue(membership);

    const result = await provider.getSubscription("mem_1");

    expect(mockMembershipsRetrieve).toHaveBeenCalledWith("mem_1");
    expect(result).toEqual(membership);
  });

  test("AC-11e: nonexistent membershipId propagates exception", async () => {
    mockMembershipsRetrieve.mockRejectedValue(new Error("Not found"));

    await expect(provider.getSubscription("nonexistent")).rejects.toThrow(
      "Not found"
    );
  });
});

// ─── getSubscriptionInvoices ────────────────────────────────────────────────

describe("getSubscriptionInvoices", () => {
  test("AC-12: maps payments to internal invoice format", async () => {
    mockPaymentsList.mockResolvedValue({
      data: [
        {
          id: "pay_001",
          paid_at: "2026-03-15T10:00:00Z",
          created_at: "2026-03-15T09:00:00Z",
          total: 999,
          currency: "usd",
          substatus: "completed",
          status: "paid",
          card_brand: "visa",
          card_last4: "4242",
        },
      ],
    });

    const invoices = await provider.getSubscriptionInvoices("mem_1");

    expect(mockPaymentsList).toHaveBeenCalledWith({
      company_id: "biz_test",
      membership_id: "mem_1",
    });
    expect(invoices).toEqual([
      {
        id: "pay_001",
        createdAt: "2026-03-15T10:00:00Z",
        total: 999,
        totalFormatted: "$9.99",
        currency: "usd",
        status: "completed",
        invoiceUrl: null,
        cardBrand: "visa",
        cardLastFour: "4242",
      },
    ]);
  });

  test("AC-13: empty payments list returns empty array", async () => {
    mockPaymentsList.mockResolvedValue({ data: [] });

    const invoices = await provider.getSubscriptionInvoices("mem_1");

    expect(invoices).toEqual([]);
  });

  test("AC-13b: date uses paid_at when available, falls back to created_at", async () => {
    mockPaymentsList.mockResolvedValue({
      data: [
        {
          id: "pay_with_paid_at",
          paid_at: "2026-03-15T10:00:00Z",
          created_at: "2026-03-15T09:00:00Z",
          total: 500,
          currency: "usd",
          status: "paid",
        },
        {
          id: "pay_without_paid_at",
          paid_at: null,
          created_at: "2026-03-14T09:00:00Z",
          total: 500,
          currency: "usd",
          status: "paid",
        },
      ],
    });

    const invoices = await provider.getSubscriptionInvoices("mem_1");

    expect(invoices[0].createdAt).toBe("2026-03-15T10:00:00Z");
    expect(invoices[1].createdAt).toBe("2026-03-14T09:00:00Z");
  });

  test("AC-13c: status uses substatus when available, falls back to status", async () => {
    mockPaymentsList.mockResolvedValue({
      data: [
        {
          id: "pay_with_substatus",
          total: 500,
          currency: "usd",
          substatus: "refunded",
          status: "paid",
        },
        {
          id: "pay_without_substatus",
          total: 500,
          currency: "usd",
          substatus: null,
          status: "pending",
        },
      ],
    });

    const invoices = await provider.getSubscriptionInvoices("mem_1");

    expect(invoices[0].status).toBe("refunded");
    expect(invoices[1].status).toBe("pending");
  });

  test("AC-13d: invoiceUrl is always null", async () => {
    mockPaymentsList.mockResolvedValue({
      data: [
        { id: "pay_1", total: 100, currency: "usd", status: "paid" },
        { id: "pay_2", total: 200, currency: "usd", status: "paid" },
      ],
    });

    const invoices = await provider.getSubscriptionInvoices("mem_1");

    invoices.forEach((inv) => {
      expect(inv.invoiceUrl).toBeNull();
    });
  });

  test("totalFormatted: USD uses $ symbol, non-USD omits symbol", async () => {
    mockPaymentsList.mockResolvedValue({
      data: [
        { id: "pay_usd", total: 1999, currency: "usd", status: "paid" },
        { id: "pay_eur", total: 1999, currency: "eur", status: "paid" },
      ],
    });

    const invoices = await provider.getSubscriptionInvoices("mem_1");

    expect(invoices[0].totalFormatted).toBe("$19.99");
    expect(invoices[1].totalFormatted).toBe("19.99");
  });
});

// ─── getPaymentMethod ───────────────────────────────────────────────────────

describe("getPaymentMethod", () => {
  test("AC-14: extracts card info from latest payment", async () => {
    mockPaymentsList.mockResolvedValue({
      data: [
        {
          id: "pay_latest",
          card_brand: "mastercard",
          card_last4: "1234",
          payment_method_type: "card",
        },
      ],
    });

    const result = await provider.getPaymentMethod("mem_1");

    expect(result).toEqual({
      cardBrand: "mastercard",
      cardLastFour: "1234",
      paymentMethodType: "card",
    });
  });

  test("AC-15: no payments returns null", async () => {
    mockPaymentsList.mockResolvedValue({ data: [] });

    const result = await provider.getPaymentMethod("mem_1");

    expect(result).toBeNull();
  });
});

// ─── resolvePortastockPlan (exported helper) ────────────────────────────────

describe("resolvePortastockPlan", () => {
  test("AC-20: maps pro monthly plan ID back to portastock plan", () => {
    const result = resolvePortastockPlan("plan_pro_m");

    expect(result).toEqual({ planId: "pro", interval: "month" });
  });

  test("AC-20: maps pro annual plan ID", () => {
    const result = resolvePortastockPlan("plan_pro_a");

    expect(result).toEqual({ planId: "pro", interval: "year" });
  });

  test("AC-20: maps lifetime plan ID", () => {
    const result = resolvePortastockPlan("plan_lt");

    expect(result).toEqual({ planId: "lifetime", interval: "lifetime" });
  });

  // ─── Edge cases for branch coverage ──────────────────────────────────────

  describe("mapPaymentToInvoice edge cases (via getSubscriptionInvoices)", () => {
    test("uses amount when total is undefined", async () => {
      mockPaymentsList.mockResolvedValue({
        data: [{ id: "pay_amt", amount: 750, currency: "usd", status: "paid" }],
      });

      const invoices = await provider.getSubscriptionInvoices("mem_1");

      expect(invoices[0].total).toBe(750);
    });

    test("defaults total to 0 when total and amount are both undefined", async () => {
      mockPaymentsList.mockResolvedValue({
        data: [{ id: "pay_zero", currency: "usd", status: "paid" }],
      });

      const invoices = await provider.getSubscriptionInvoices("mem_1");

      expect(invoices[0].total).toBe(0);
    });

    test("defaults currency to usd when missing", async () => {
      mockPaymentsList.mockResolvedValue({
        data: [{ id: "pay_nocur", total: 100, status: "paid" }],
      });

      const invoices = await provider.getSubscriptionInvoices("mem_1");

      expect(invoices[0].currency).toBe("usd");
      expect(invoices[0].totalFormatted).toBe("$1.00");
    });

    test("createdAt is null when both paid_at and created_at are missing", async () => {
      mockPaymentsList.mockResolvedValue({
        data: [{ id: "pay_nodate", total: 100, currency: "usd", status: "paid" }],
      });

      const invoices = await provider.getSubscriptionInvoices("mem_1");

      expect(invoices[0].createdAt).toBeNull();
    });

    test("status defaults to unknown when substatus and status are both missing", async () => {
      mockPaymentsList.mockResolvedValue({
        data: [{ id: "pay_nost", total: 100, currency: "usd" }],
      });

      const invoices = await provider.getSubscriptionInvoices("mem_1");

      expect(invoices[0].status).toBe("unknown");
    });
  });

  describe("getSubscriptionInvoices with null response.data", () => {
    test("returns empty array when response has no data property", async () => {
      mockPaymentsList.mockResolvedValue({});

      const invoices = await provider.getSubscriptionInvoices("mem_1");

      expect(invoices).toEqual([]);
    });

    test("returns empty array when response is null", async () => {
      mockPaymentsList.mockResolvedValue(null);

      const invoices = await provider.getSubscriptionInvoices("mem_1");

      expect(invoices).toEqual([]);
    });
  });

  describe("getPaymentMethod with missing card fields", () => {
    test("returns null fields when card info is absent", async () => {
      mockPaymentsList.mockResolvedValue({
        data: [{ id: "pay_nocard" }],
      });

      const result = await provider.getPaymentMethod("mem_1");

      expect(result).toEqual({
        cardBrand: null,
        cardLastFour: null,
        paymentMethodType: null,
      });
    });
  });

  describe("createCheckoutSession without optional URLs", () => {
    test("omits redirect_url and source_url when not provided", async () => {
      mockCheckoutCreate.mockResolvedValue({ purchase_url: "url", id: "ch_x" });

      await provider.createCheckoutSession({
        planId: "pro",
        interval: "month",
        userId: "uid_123",
      });

      expect(mockCheckoutCreate).toHaveBeenCalledWith(
        expect.objectContaining({
          redirect_url: undefined,
          source_url: undefined,
        })
      );
    });
  });

  test("AC-21: unknown plan ID falls back to pro/month with warning", () => {
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});

    const result = resolvePortastockPlan("plan_desconocido");

    expect(result).toEqual({
      planId: "pro",
      interval: "month",
      planResolutionWarning: true,
    });
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining("Unknown Whop plan ID")
    );

    warnSpy.mockRestore();
  });
});
