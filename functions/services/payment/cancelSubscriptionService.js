/**
 * Cancel Subscription Service — Cloud Function Callable
 *
 * Permite a un usuario cancelar/degradar su suscripción Pro.
 * - Mock mode: Degrada a Free inmediatamente via buildSubscriptionData
 * - Real mode: Comunica cancelación a LS vía provider + circuit breaker
 *
 * Rechaza si el usuario es Free (nada que cancelar) o Lifetime (no cancelable).
 *
 * @see docs/architecture/FEAT-GATE-001-feature-gating-subscription-plans-design.md
 * @module services/payment/cancelSubscriptionService
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const admin = require("../firebaseAdmin");
const { buildSubscriptionData } = require("./planFeatures");
const { getPaymentProvider } = require("./providerFactory");
const { getCircuit } = require("../../utils/circuitBreaker");

const lsApiKey = defineSecret("LEMONSQUEEZY_API_KEY");

const cancelSubscription = onCall(
  { cors: true, memory: "256MiB", timeoutSeconds: 30, secrets: [lsApiKey] },
  async (request) => {
    if (!request.auth?.uid) {
      throw new HttpsError("unauthenticated", "Authentication required");
    }

    const userId = request.auth.uid;
    const db = admin.firestore();

    // Leer suscripción actual
    const userDoc = await db.collection("userData").doc(userId).get();
    const userData = userDoc.data();
    const subscription = userData?.subscription;

    if (!subscription || subscription.planId === "free") {
      throw new HttpsError(
        "failed-precondition",
        "No active subscription to cancel"
      );
    }

    if (subscription.planId === "lifetime") {
      throw new HttpsError(
        "failed-precondition",
        "Lifetime plans cannot be canceled"
      );
    }

    const isMockMode = process.env.PAYMENT_MOCK_ENABLED === "true";

    if (isMockMode) {
      // Mock mode: degradar a Free inmediatamente
      const freeSubscription = await buildSubscriptionData("free", "month");
      await db
        .collection("userData")
        .doc(userId)
        .set({ subscription: freeSubscription }, { merge: true });

      return {
        success: true,
        effectiveDate: new Date().toISOString(),
        newPlan: "free",
        message: "Subscription canceled — downgraded to Free (mock mode)",
      };
    }

    // Real mode: cancelar vía provider + circuit breaker, fallback a Firestore-only
    const subscriptionId = subscription.subscriptionId;
    if (!subscriptionId) {
      throw new HttpsError(
        "failed-precondition",
        "No subscription ID found — cannot cancel with payment provider"
      );
    }

    const provider = getPaymentProvider();
    const lsCircuit = getCircuit("lemonSqueezy");

    let providerSuccess = false;
    try {
      const result = await lsCircuit.execute(
        () => provider.cancelSubscription(subscriptionId),
        () => {
          console.warn("[Cancel] LS circuit open — fallback to Firestore-only");
          return { success: false, fallback: true };
        }
      );
      providerSuccess = result.success && !result.fallback;
    } catch (err) {
      console.warn("[Cancel] LS API error — fallback:", err.message);
    }

    const effectiveDate = subscription.currentPeriodEnd || new Date().toISOString();
    await db
      .collection("userData")
      .doc(userId)
      .set(
        {
          subscription: {
            cancelAtPeriodEnd: true,
            updatedAt: new Date().toISOString(),
          },
        },
        { merge: true }
      );

    const logMsg = providerSuccess
      ? "[Cancel] LS API confirmed cancel"
      : "[Cancel] Firestore-only cancel (LS API failed or unavailable)";
    console.log(logMsg);

    return {
      success: true,
      effectiveDate,
      newPlan: "free",
      message: `Subscription will be canceled at end of period: ${effectiveDate}`,
    };
  }
);

module.exports = { cancelSubscription };
