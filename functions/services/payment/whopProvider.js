/**
 * Whop Payment Provider — Adapter for Whop REST API via @whop/sdk
 *
 * Implements the PaymentProvider interface (paymentProvider.js) using Whop's
 * SDK. Factory function pattern: createWhopProvider() returns an object
 * with the 8 contract methods.
 *
 * @see docs/architecture/MIGRATION-WHOP-PAYMENT-GATEWAY-2026-04-06.md § 4.2
 * @module services/payment/whopProvider
 */

const { PAYMENT_EVENT_TYPES } = require("./paymentProvider");

// ---------------------------------------------------------------------------
// Helpers — internal, not exported
// ---------------------------------------------------------------------------

const PLAN_ENV_MAP = {
  pro_month: "WHOP_PLAN_PRO_MONTHLY",
  pro_year: "WHOP_PLAN_PRO_ANNUAL",
  lifetime_lifetime: "WHOP_PLAN_LIFETIME",
};

const REVERSE_PLAN_MAP = {
  WHOP_PLAN_PRO_MONTHLY: { planId: "pro", interval: "month" },
  WHOP_PLAN_PRO_ANNUAL: { planId: "pro", interval: "year" },
  WHOP_PLAN_LIFETIME: { planId: "lifetime", interval: "lifetime" },
};

const WHOP_EVENT_TYPE_MAP = {
  "payment.succeeded": PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
  "payment.failed": PAYMENT_EVENT_TYPES.PAYMENT_FAILED,
  "membership.activated": PAYMENT_EVENT_TYPES.MEMBERSHIP_ACTIVATED,
  "membership.deactivated": PAYMENT_EVENT_TYPES.MEMBERSHIP_DEACTIVATED,
  "membership.cancel_at_period_end_changed":
    PAYMENT_EVENT_TYPES.CANCEL_AT_PERIOD_END_CHANGED,
};

function sanitizeInput(value) {
  return String(value || "").slice(0, 32).replace(/[\n\r]/g, "");
}

function resolveWhopPlanId(planId, interval) {
  const key = `${planId}_${interval}`;
  const envKey = PLAN_ENV_MAP[key];
  if (!envKey) {
    throw new Error(
      `No Whop plan configured for planId="${sanitizeInput(planId)}", interval="${sanitizeInput(interval)}"`
    );
  }
  const whopPlanId = process.env[envKey];
  if (!whopPlanId) {
    throw new Error(
      `Environment variable ${envKey} is not set`
    );
  }
  return whopPlanId;
}

function resolvePortastockPlan(whopPlanId) {
  for (const [envKey, mapping] of Object.entries(REVERSE_PLAN_MAP)) {
    if (process.env[envKey] === whopPlanId) {
      return { ...mapping };
    }
  }
  console.warn(
    `[whopProvider] Unknown Whop plan ID "${whopPlanId}" — falling back to pro/month`
  );
  return { planId: "pro", interval: "month", planResolutionWarning: true };
}

function mapWhopEventType(whopType) {
  return WHOP_EVENT_TYPE_MAP[whopType] || `UNKNOWN_${whopType}`;
}

function formatCurrency(cents, currency) {
  const amount = (cents / 100).toFixed(2);
  const symbol = currency?.toUpperCase() === "USD" ? "$" : "";
  return `${symbol}${amount}`;
}

function mapPaymentToInvoice(payment) {
  const total = payment.total ?? payment.amount ?? 0;
  const currency = payment.currency || "usd";
  return {
    id: String(payment.id),
    createdAt: payment.paid_at || payment.created_at || null,
    total,
    totalFormatted: formatCurrency(total, currency),
    currency,
    status: payment.substatus || payment.status || "unknown",
    invoiceUrl: null,
    cardBrand: payment.card_brand || null,
    cardLastFour: payment.card_last4 || null,
  };
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

function createWhopProvider({ apiKey, webhookSecret, companyId }) {
  const { Whop } = require("@whop/sdk");

  const client = new Whop({
    apiKey,
    webhookKey: Buffer.from(webhookSecret || "").toString("base64"),
  });

  // --- Checkout (AC-03, AC-04, AC-05) ---
  async function createCheckoutSession({
    planId,
    interval,
    userId,
    email,
    successUrl,
    cancelUrl,
  }) {
    const whopPlanId = resolveWhopPlanId(planId, interval);

    const config = await client.checkoutConfigurations.create({
      company_id: companyId,
      plan_id: whopPlanId,
      metadata: {
        firebase_uid: userId,
        portastock_plan_id: planId,
        interval,
      },
      redirect_url: successUrl || undefined,
      source_url: cancelUrl || undefined,
    });

    return { checkoutUrl: config.purchase_url, sessionId: config.id };
  }

  // --- Portal (AC-06) ---
  async function createPortalSession(membershipId) {
    const membership = await client.memberships.retrieve(membershipId);
    return { portalUrl: membership.manage_url };
  }

  // --- Subscription (AC-07) ---
  async function getSubscription(membershipId) {
    return client.memberships.retrieve(membershipId);
  }

  // --- Cancel (AC-08, AC-09) ---
  const VALID_CANCEL_MODES = ["at_period_end", "immediate"];

  async function cancelSubscription(membershipId, mode = "at_period_end") {
    if (!VALID_CANCEL_MODES.includes(mode)) {
      throw new Error(
        `Invalid cancellation mode. Must be one of: ${VALID_CANCEL_MODES.join(", ")}`
      );
    }

    const result = await client.memberships.cancel(membershipId, {
      cancellation_mode: mode,
    });
    return {
      success: true,
      effectiveDate: result.renewal_period_end || null,
    };
  }

  // --- Reactivate (AC-10) ---
  async function reactivateSubscription(membershipId) {
    await client.memberships.uncancel(membershipId);
    return { success: true };
  }

  // --- Webhook (AC-11, AC-12, AC-13) ---
  async function parseWebhook(rawBody, headers) {
    const verified = await client.webhooks.unwrap(rawBody, { headers });

    const normalizedType = mapWhopEventType(verified.type);

    return {
      type: normalizedType,
      eventId: verified.id,
      data: verified.data,
      rawType: verified.type,
      companyId: verified.company_id,
    };
  }

  // --- Invoices (AC-14) ---
  async function getSubscriptionInvoices(membershipId) {
    const response = await client.payments.list({
      company_id: companyId,
      membership_id: membershipId,
    });

    const payments = response?.data || [];
    return payments.map(mapPaymentToInvoice);
  }

  // --- Payment Method (AC-15) ---
  async function getPaymentMethod(membershipId) {
    const response = await client.payments.list({
      company_id: companyId,
      membership_id: membershipId,
    });

    const payments = response?.data || [];
    if (payments.length === 0) return null;

    const latest = payments[0];
    return {
      cardBrand: latest.card_brand || null,
      cardLastFour: latest.card_last4 || null,
      paymentMethodType: latest.payment_method_type || null,
    };
  }

  return {
    createCheckoutSession,
    createPortalSession,
    getSubscription,
    cancelSubscription,
    reactivateSubscription,
    parseWebhook,
    getSubscriptionInvoices,
    getPaymentMethod,
  };
}

module.exports = { createWhopProvider, resolvePortastockPlan };
