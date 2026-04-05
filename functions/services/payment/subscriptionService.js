/**
 * Subscription Service — Business logic for checkout, portal, and webhook processing
 *
 * Uses buildSubscriptionData from planFeatures.js (single source of truth for 21 keys).
 * Delegates all provider-specific calls via getPaymentProvider() adapter.
 * Does NOT import @lemonsqueezy/lemonsqueezy.js directly.
 *
 * @see docs/architecture/STRIPE-001-payment-subscription-integration-design.md
 * @module services/payment/subscriptionService
 */

const admin = require("../firebaseAdmin");
const { getPaymentProvider } = require("./providerFactory");
const { buildSubscriptionData } = require("./planFeatures");
const { PAYMENT_EVENT_TYPES } = require("./paymentProvider");
const { getCircuit } = require("../../utils/circuitBreaker");
const { HttpsError } = require("firebase-functions/v2/https");

const lsCircuit = getCircuit("lemonSqueezy", {
  failureThreshold: 3,
  resetTimeout: 30000,
  halfOpenRequests: 1,
});

function lsCircuitFallback() {
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

  const result = await lsCircuit.execute(
    () => provider.createCheckoutSession({ userId, email, planId, interval }),
    lsCircuitFallback
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

  const result = await lsCircuit.execute(
    () => provider.createPortalSession(customerId),
    lsCircuitFallback
  );
  return { url: result.portalUrl };
}

async function processWebhookEvent(webhookResult, transaction = null) {
  const { type, userId, planId, interval, subscriptionId, customerId } = webhookResult;

  if (!userId) {
    throw new Error("Webhook missing userId in custom data");
  }

  const db = admin.firestore();
  const userRef = db.collection("userData").doc(userId);

  function writeToUser(data) {
    if (transaction) {
      return transaction.set(userRef, data, { merge: true });
    } else {
      return userRef.set(data, { merge: true });
    }
  }

  function readUser() {
    if (transaction) {
      return transaction.get(userRef);
    }
    return userRef.get();
  }

  switch (type) {
    case PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED:
    case PAYMENT_EVENT_TYPES.SUBSCRIPTION_CREATED: {
      // PAY-009: Read existing snapshot to preserve trial history
      const existingUserDoc = await readUser();
      const existingSub = existingUserDoc.data()?.subscription;

      const subscriptionData = await buildSubscriptionData(planId, interval, "active", "checkout");
      subscriptionData.subscriptionId = subscriptionId;
      subscriptionData.providerCustomerId = customerId;

      // PAY-009: Preserve sticky trial fields
      if (existingSub?.hasUsedTrial) {
        subscriptionData.hasUsedTrial = true;
      }
      if (existingSub?.trialStartedAt) {
        subscriptionData.trialStartedAt = existingSub.trialStartedAt;
      }
      if (existingSub?.trialEndedAt) {
        subscriptionData.trialEndedAt = existingSub.trialEndedAt;
      }

      if (planId === "lifetime") {
        subscriptionData.currentPeriodEnd = null;
        subscriptionData.purchasedAt = new Date().toISOString();
        subscriptionData.interval = "lifetime";
      }

      await writeToUser({ subscription: subscriptionData });
      break;
    }

    case PAYMENT_EVENT_TYPES.SUBSCRIPTION_UPDATED: {
      await writeToUser({
        subscription: {
          status: "active",
          updatedAt: new Date().toISOString(),
        },
      });
      break;
    }

    case PAYMENT_EVENT_TYPES.SUBSCRIPTION_CANCELED: {
      // PAY-009: Read existing snapshot to preserve trial history
      const existingCancelDoc = await readUser();
      const existingCancelSub = existingCancelDoc.data()?.subscription;

      const freeData = await buildSubscriptionData("free", "month", "canceled");
      freeData.subscriptionId = null;
      freeData.providerCustomerId = null;

      // PAY-009: Preserve sticky trial fields
      if (existingCancelSub?.hasUsedTrial) {
        freeData.hasUsedTrial = true;
      }
      if (existingCancelSub?.trialStartedAt) {
        freeData.trialStartedAt = existingCancelSub.trialStartedAt;
      }
      if (existingCancelSub?.subscriptionOrigin === "trial") {
        freeData.trialEndedAt = new Date().toISOString();
      } else if (existingCancelSub?.trialEndedAt) {
        freeData.trialEndedAt = existingCancelSub.trialEndedAt;
      }

      await writeToUser({ subscription: freeData });
      break;
    }

    case PAYMENT_EVENT_TYPES.PAYMENT_FAILED: {
      await writeToUser({
        subscription: {
          status: "past_due",
          updatedAt: new Date().toISOString(),
        },
      });
      break;
    }

    case PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED: {
      const userDoc = await readUser();
      const currentStatus = userDoc.data()?.subscription?.status;

      if (currentStatus === "past_due") {
        await writeToUser({
          subscription: {
            status: "active",
            updatedAt: new Date().toISOString(),
          },
        });
      }
      break;
    }

    default:
      break;
  }
}

module.exports = { initiateCheckout, createPortalSession, processWebhookEvent };
