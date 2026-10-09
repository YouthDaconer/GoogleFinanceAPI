const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require("../firebaseAdmin");
const { getPaymentProvider } = require("./providerFactory");
const { getCircuit } = require("../../utils/circuitBreaker");

// WHOP-004: Whop secrets wired via index.js CF declarations
const reactivateSubscription = onCall(
  { cors: true, memory: "256MiB", timeoutSeconds: 30,
    secrets: ["WHOP_API_KEY", "WHOP_WEBHOOK_SECRET", "WHOP_COMPANY_ID"] },
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

    // BUG-CANCEL-001: Restore original period end if saved during mock scheduled cancel
    if (subscription._originalPeriodEnd) {
      reactivationFields.currentPeriodEnd = subscription._originalPeriodEnd;
      reactivationFields._originalPeriodEnd = admin.firestore.FieldValue.delete();
    }

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
    const paymentCircuit = getCircuit("whop");

    let providerSuccess = false;
    try {
      const result = await paymentCircuit.execute(
        () => provider.reactivateSubscription(subscriptionId),
        () => {
          console.warn("[Reactivate] Payment circuit open — fallback to Firestore-only");
          return { success: false, fallback: true };
        }
      );
      providerSuccess = result.success && !result.fallback;
    } catch (err) {
      console.warn("[Reactivate] Payment API error — fallback:", err.message);
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
      ? "[Reactivate] Whop API confirmed reactivation"
      : "[Reactivate] Firestore-only reactivation (Whop API failed or unavailable)";
    console.log(logMsg);

    return {
      success: true,
      message: "Subscription reactivated",
      plan: subscription.planId,
    };
  }
);

module.exports = { reactivateSubscription };
