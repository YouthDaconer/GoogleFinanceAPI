const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const admin = require("../firebaseAdmin");
const { getPaymentProvider } = require("./providerFactory");
const { getCircuit } = require("../../utils/circuitBreaker");

const lsApiKey = defineSecret("LEMONSQUEEZY_API_KEY");

const reactivateSubscription = onCall(
  { cors: true, memory: "256MiB", timeoutSeconds: 30, secrets: [lsApiKey] },
  async (request) => {
    if (!request.auth?.uid) {
      throw new HttpsError("unauthenticated", "Authentication required");
    }

    const userId = request.auth.uid;
    const db = admin.firestore();

    const userDoc = await db.collection("userData").doc(userId).get();
    const userData = userDoc.data();
    const subscription = userData?.subscription;

    if (!subscription || subscription.planId === "free") {
      throw new HttpsError("failed-precondition", "No active subscription to reactivate");
    }

    if (subscription.planId === "lifetime") {
      throw new HttpsError("failed-precondition", "Lifetime plans don't require reactivation");
    }

    if (!subscription.cancelAtPeriodEnd) {
      throw new HttpsError("failed-precondition", "Subscription is not pending cancellation");
    }

    const reactivationFields = {
      cancelAtPeriodEnd: false,
      cancellationReason: admin.firestore.FieldValue.delete(),
      cancellationComment: admin.firestore.FieldValue.delete(),
      cancelledAt: admin.firestore.FieldValue.delete(),
      updatedAt: new Date().toISOString(),
    };

    const isMockMode = process.env.PAYMENT_MOCK_ENABLED === "true";

    if (isMockMode) {
      await db.collection("userData").doc(userId).set(
        { subscription: reactivationFields },
        { merge: true }
      );

      try {
        await db.collection("subscriptionEvents").doc().set({
          type: "SUBSCRIPTION_REACTIVATED",
          userId,
          planId: subscription.planId,
          mode: "mock",
          processedAt: admin.firestore.FieldValue.serverTimestamp(),
        });
      } catch (err) {
        console.error("[Reactivate] Failed to write audit event (mock):", err.message);
      }

      return {
        success: true,
        message: "Subscription reactivated (mock mode)",
        plan: subscription.planId,
      };
    }

    // Real mode
    const subscriptionId = subscription.subscriptionId;
    if (!subscriptionId) {
      throw new HttpsError(
        "failed-precondition",
        "No subscription ID found — cannot reactivate with payment provider"
      );
    }

    const provider = getPaymentProvider();
    const lsCircuit = getCircuit("lemonSqueezy");

    let providerSuccess = false;
    try {
      const result = await lsCircuit.execute(
        () => provider.reactivateSubscription(subscriptionId),
        () => {
          console.warn("[Reactivate] LS circuit open — fallback to Firestore-only");
          return { success: false, fallback: true };
        }
      );
      providerSuccess = result.success && !result.fallback;
    } catch (err) {
      console.warn("[Reactivate] LS API error — fallback:", err.message);
    }

    await db.collection("userData").doc(userId).set(
      { subscription: reactivationFields },
      { merge: true }
    );

    try {
      await db.collection("subscriptionEvents").doc().set({
        type: "SUBSCRIPTION_REACTIVATED",
        userId,
        planId: subscription.planId,
        providerSuccess,
        mode: "real",
        processedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
    } catch (err) {
      console.error("[Reactivate] Failed to write audit event (real):", err.message);
    }

    const logMsg = providerSuccess
      ? "[Reactivate] LS API confirmed reactivation"
      : "[Reactivate] Firestore-only reactivation (LS API failed or unavailable)";
    console.log(logMsg);

    return {
      success: true,
      message: "Subscription reactivated",
      plan: subscription.planId,
    };
  }
);

module.exports = { reactivateSubscription };
