/**
 * Subscription Service — Business logic for checkout, portal, and webhook processing
 *
 * Uses buildSubscriptionData from planFeatures.js (single source of truth for 21 keys).
 * Delegates all provider-specific calls via getPaymentProvider() adapter.
 *
 * @see docs/architecture/MIGRATION-WHOP-PAYMENT-GATEWAY-2026-04-06.md
 * @module services/payment/subscriptionService
 */

const admin = require("../firebaseAdmin");
const { getPaymentProvider } = require("./providerFactory");
const { buildSubscriptionData } = require("./planFeatures");
const { PAYMENT_EVENT_TYPES } = require("./paymentProvider");
const { resolvePortastockPlan } = require("./whopProvider");
const { getCircuit } = require("../../utils/circuitBreaker");
const { HttpsError } = require("firebase-functions/v2/https");

const paymentCircuit = getCircuit("whop", {
  failureThreshold: 3,
  resetTimeout: 30000,
  halfOpenRequests: 1,
});

function paymentCircuitFallback() {
  throw new HttpsError(
    "unavailable",
    "El servicio de pagos no está disponible temporalmente. Intenta de nuevo en unos minutos."
  );
}

async function initiateCheckout(userId, email, planId, interval) {
  const provider = getPaymentProvider();
  if (!provider) {
    throw new Error("Payment provider is not configured (mock mode)");
  }

  const appUrl = process.env.APP_URL || 'https://portastock.net';
  const successUrl = `${appUrl}/settings/subscription?checkout=success`;
  const cancelUrl = `${appUrl}/pricing`;

  const result = await paymentCircuit.execute(
    () => provider.createCheckoutSession({ userId, email, planId, interval, successUrl, cancelUrl }),
    paymentCircuitFallback
  );

  return { url: result.checkoutUrl };
}

async function createPortalSession(userId) {
  const provider = getPaymentProvider();
  if (!provider) {
    throw new Error("Payment provider is not configured (mock mode)");
  }

  const db = admin.firestore();
  const userDoc = await db.collection("userData").doc(userId).get();
  const subscription = userDoc.data()?.subscription;

  if (!subscription?.subscriptionId) {
    throw new Error("No active subscription found for portal access");
  }

  const customerId = subscription.providerCustomerId;
  if (!customerId) {
    throw new Error("No provider customer ID found");
  }

  const result = await paymentCircuit.execute(
    () => provider.createPortalSession(customerId),
    paymentCircuitFallback
  );
  return { url: result.portalUrl };
}

async function processWebhookEvent({ type, data, userId, eventId }) {
  const db = admin.firestore();
  const userRef = db.collection("userData").doc(userId);

  switch (type) {
    case PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED:
      await handlePaymentSucceeded(userRef, userId, data);
      break;

    case PAYMENT_EVENT_TYPES.MEMBERSHIP_ACTIVATED:
      await handleMembershipActivated(userRef, userId, data);
      break;

    case PAYMENT_EVENT_TYPES.MEMBERSHIP_DEACTIVATED:
      await handleMembershipDeactivated(userRef, userId, data);
      break;

    case PAYMENT_EVENT_TYPES.CANCEL_AT_PERIOD_END_CHANGED:
      await handleCancelAtPeriodEndChanged(userRef, userId, data);
      break;

    case PAYMENT_EVENT_TYPES.PAYMENT_FAILED:
      await handlePaymentFailed(userRef, userId, data);
      break;

    default:
      console.log("[Webhook] Unhandled webhook type:", type);
      break;
  }
}

async function handlePaymentSucceeded(userRef, userId, data) {
  const planResult = resolvePortastockPlan(data.plan?.id);
  if (planResult.planResolutionWarning) {
    throw new Error(`Unknown Whop plan ID: "${data.plan?.id}" — refusing to write incorrect plan to Firestore`);
  }
  const { planId, interval } = planResult;
  const origin = data.membership?.status === "trialing" ? "trial" : "checkout";

  const subscriptionData = await buildSubscriptionData(planId, interval, "active", origin);
  subscriptionData.updatedAt = new Date().toISOString();
  subscriptionData.subscriptionId = data.membership?.id || null;
  subscriptionData.providerCustomerId = data.user?.id || null;

  if (data.card_brand) subscriptionData.cardBrand = data.card_brand;
  if (data.card_last4) subscriptionData.cardLast4 = data.card_last4;

  const current = (await userRef.get()).data()?.subscription || {};
  if (current.hasUsedTrial) subscriptionData.hasUsedTrial = true;
  if (current.trialStartedAt) subscriptionData.trialStartedAt = current.trialStartedAt;

  if (planId === "lifetime") {
    subscriptionData.currentPeriodEnd = null;
    subscriptionData.purchasedAt = new Date().toISOString();
    subscriptionData.interval = "lifetime";
  }

  await userRef.set({ subscription: subscriptionData }, { merge: true });
}

async function handleMembershipActivated(userRef, userId, data) {
  const partialUpdate = {
    subscriptionId: data.id || null,
    providerCustomerId: data.user?.id || null,
    manageUrl: data.manage_url || null,
    currentPeriodEnd: data.renewal_period_end || null,
    updatedAt: new Date().toISOString(),
  };

  await userRef.set({ subscription: partialUpdate }, { merge: true });
}

async function handleMembershipDeactivated(userRef, userId, data) {
  const freeData = await buildSubscriptionData("free", "month", "active", "checkout");

  const current = (await userRef.get()).data()?.subscription || {};
  if (current.hasUsedTrial) freeData.hasUsedTrial = true;
  if (current.trialStartedAt) freeData.trialStartedAt = current.trialStartedAt;
  if (current.trialEndedAt) freeData.trialEndedAt = current.trialEndedAt;

  await userRef.set({ subscription: freeData }, { merge: true });
}

async function handleCancelAtPeriodEndChanged(userRef, userId, data) {
  if (data.cancel_at_period_end === true) {
    const update = {
      cancelAtPeriodEnd: true,
      updatedAt: new Date().toISOString(),
    };
    if (data.cancel_option) update.cancellationReason = data.cancel_option;
    if (data.cancellation_reason) update.cancellationComment = data.cancellation_reason;
    if (data.canceled_at) update.cancelledAt = data.canceled_at;

    await userRef.set({ subscription: update }, { merge: true });
  } else if (data.cancel_at_period_end === false) {
    const update = {
      cancelAtPeriodEnd: false,
      updatedAt: new Date().toISOString(),
      cancellationReason: admin.firestore.FieldValue.delete(),
      cancellationComment: admin.firestore.FieldValue.delete(),
      cancelledAt: admin.firestore.FieldValue.delete(),
    };

    await userRef.set({ subscription: update }, { merge: true });
  } else {
    console.log("[Webhook] cancel_at_period_end not boolean, skipping:", data.cancel_at_period_end);
  }
}

async function handlePaymentFailed(userRef, userId, data) {
  await userRef.set({
    subscription: {
      status: "past_due",
      updatedAt: new Date().toISOString(),
    },
  }, { merge: true });
}

module.exports = { initiateCheckout, createPortalSession, processWebhookEvent };
