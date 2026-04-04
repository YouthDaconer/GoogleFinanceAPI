/**
 * Lemon Squeezy Provider — Adapter for the Lemon Squeezy payments SDK
 *
 * Implements the PaymentProvider interface contract. This is the ONLY file
 * in the project that imports @lemonsqueezy/lemonsqueezy.js directly.
 *
 * @see docs/architecture/STRIPE-001-payment-subscription-integration-design.md
 * @module services/payment/lemonSqueezyProvider
 */

const {
  lemonSqueezySetup,
  createCheckout,
  getSubscription: lsGetSubscription,
  updateSubscription: lsUpdateSubscription,
  getCustomer,
} = require("@lemonsqueezy/lemonsqueezy.js");
const crypto = require("crypto");
const { PAYMENT_EVENT_TYPES } = require("./paymentProvider");

const LS_EVENT_MAP = {
  order_created: PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED,
  subscription_created: PAYMENT_EVENT_TYPES.SUBSCRIPTION_CREATED,
  subscription_updated: PAYMENT_EVENT_TYPES.SUBSCRIPTION_UPDATED,
  subscription_cancelled: PAYMENT_EVENT_TYPES.SUBSCRIPTION_CANCELED,
  subscription_payment_failed: PAYMENT_EVENT_TYPES.PAYMENT_FAILED,
  subscription_payment_success: PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED,
};

function createLemonSqueezyProvider() {
  const apiKey = process.env.LEMONSQUEEZY_API_KEY;
  if (!apiKey) {
    throw new Error("LEMONSQUEEZY_API_KEY environment variable is not set");
  }

  lemonSqueezySetup({ apiKey });

  const storeId = process.env.LEMONSQUEEZY_STORE_ID;
  const variantMap = {
    pro_month: process.env.LEMONSQUEEZY_VARIANT_PRO_MONTHLY,
    pro_year: process.env.LEMONSQUEEZY_VARIANT_PRO_ANNUAL,
    lifetime_lifetime: process.env.LEMONSQUEEZY_VARIANT_LIFETIME,
  };

  async function createCheckoutSession({ userId, email, planId, interval, successUrl, cancelUrl }) {
    const variantKey = `${planId}_${interval}`;
    const variantId = variantMap[variantKey];
    if (!variantId) {
      throw new Error(`No variant configured for plan=${planId}, interval=${interval}`);
    }

    const { data, error } = await createCheckout(storeId, variantId, {
      checkoutData: {
        email,
        custom: { user_id: userId, plan_id: planId, interval },
      },
      productOptions: { redirectUrl: successUrl },
      checkoutOptions: { embed: false },
    });

    if (error) {
      throw new Error(`Lemon Squeezy checkout failed: ${error.message}`);
    }

    return { checkoutUrl: data.data.attributes.url };
  }

  async function createPortalSession(customerId) {
    const { data, error } = await getCustomer(customerId);

    if (error) {
      throw new Error(`Lemon Squeezy portal failed: ${error.message}`);
    }

    return { portalUrl: data.data.attributes.urls.customer_portal };
  }

  async function getSubscriptionDetails(subscriptionId) {
    const { data, error } = await lsGetSubscription(subscriptionId);

    if (error) {
      throw new Error(`Lemon Squeezy getSubscription failed: ${error.message}`);
    }

    const attrs = data.data.attributes;
    return {
      subscriptionId: data.data.id,
      customerId: String(attrs.customer_id),
      planId: attrs.first_subscription_item?.price_id ? "pro" : "pro",
      status: attrs.status,
      interval: attrs.billing_anchor === 0 ? "lifetime" : attrs.variant_id ? "month" : "year",
      currentPeriodEnd: attrs.renews_at || null,
      cancelAtPeriodEnd: attrs.cancelled,
    };
  }

  async function parseWebhook(rawBody, signature) {
    const secret = process.env.LEMONSQUEEZY_WEBHOOK_SECRET;
    if (!secret) {
      throw new Error("LEMONSQUEEZY_WEBHOOK_SECRET is not configured");
    }

    const hmac = crypto.createHmac("sha256", secret);
    const digest = hmac.update(rawBody).digest("hex");

    const sigBuffer = Buffer.from(signature);
    const digestBuffer = Buffer.from(digest);

    if (sigBuffer.length !== digestBuffer.length || !crypto.timingSafeEqual(sigBuffer, digestBuffer)) {
      throw new Error("Invalid webhook signature");
    }

    const payload = JSON.parse(rawBody);
    const eventName = payload.meta?.event_name;
    const customData = payload.meta?.custom_data || {};
    const attrs = payload.data?.attributes || {};

    const type = LS_EVENT_MAP[eventName] || "unknown";

    return {
      type,
      userId: customData.user_id || null,
      planId: customData.plan_id || "pro",
      interval: customData.interval || "month",
      subscriptionId: payload.data?.id ? String(payload.data.id) : null,
      customerId: attrs.customer_id ? String(attrs.customer_id) : null,
      rawData: payload,
    };
  }

  async function cancelSubscription(subscriptionId) {
    const { data, error } = await lsUpdateSubscription(subscriptionId, {
      cancelled: true,
    });

    if (error) {
      throw new Error(`LS cancel failed: ${error.message}`);
    }

    const attrs = data?.data?.attributes || {};
    return {
      success: true,
      effectiveDate: attrs.ends_at || null,
      alreadyCanceled: attrs.status === "cancelled",
    };
  }

  return {
    createCheckoutSession,
    createPortalSession,
    getSubscription: getSubscriptionDetails,
    cancelSubscription,
    parseWebhook,
  };
}

module.exports = { createLemonSqueezyProvider };
