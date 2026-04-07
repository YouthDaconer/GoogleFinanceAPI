const { PAYMENT_EVENT_TYPES } = require("../../payment/paymentProvider");

describe("PAYMENT_EVENT_TYPES", () => {
  test("exports exactly 9 event constants", () => {
    expect(Object.keys(PAYMENT_EVENT_TYPES)).toHaveLength(9);
  });

  test("exports CHECKOUT_COMPLETED", () => {
    expect(PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED).toBe("checkout_completed");
  });

  test("exports SUBSCRIPTION_UPDATED", () => {
    expect(PAYMENT_EVENT_TYPES.SUBSCRIPTION_UPDATED).toBe("subscription_updated");
  });

  test("exports SUBSCRIPTION_CANCELED", () => {
    expect(PAYMENT_EVENT_TYPES.SUBSCRIPTION_CANCELED).toBe("subscription_canceled");
  });

  test("exports SUBSCRIPTION_CREATED", () => {
    expect(PAYMENT_EVENT_TYPES.SUBSCRIPTION_CREATED).toBe("subscription_created");
  });

  test("exports PAYMENT_FAILED", () => {
    expect(PAYMENT_EVENT_TYPES.PAYMENT_FAILED).toBe("payment_failed");
  });

  test("exports PAYMENT_SUCCEEDED", () => {
    expect(PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED).toBe("payment_succeeded");
  });

  test("exports MEMBERSHIP_ACTIVATED", () => {
    expect(PAYMENT_EVENT_TYPES.MEMBERSHIP_ACTIVATED).toBe("membership_activated");
  });

  test("exports MEMBERSHIP_DEACTIVATED", () => {
    expect(PAYMENT_EVENT_TYPES.MEMBERSHIP_DEACTIVATED).toBe("membership_deactivated");
  });

  test("exports CANCEL_AT_PERIOD_END_CHANGED", () => {
    expect(PAYMENT_EVENT_TYPES.CANCEL_AT_PERIOD_END_CHANGED).toBe("cancel_at_period_end_changed");
  });

  test("is frozen (immutable)", () => {
    expect(Object.isFrozen(PAYMENT_EVENT_TYPES)).toBe(true);
  });
});
