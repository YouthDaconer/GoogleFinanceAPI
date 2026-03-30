/**
 * Cancel Subscription Service — Cloud Function Callable
 *
 * Permite a un usuario cancelar/degradar su suscripción Pro.
 * - Mock mode: Degrada a Free inmediatamente via buildSubscriptionData
 * - Real mode (futuro): Marca cancelAtPeriodEnd=true, delega a pasarela
 *
 * Rechaza si el usuario es Free (nada que cancelar) o Lifetime (no cancelable).
 *
 * @see docs/architecture/FEAT-GATE-001-feature-gating-subscription-plans-design.md
 * @module services/payment/cancelSubscriptionService
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require("../firebaseAdmin");
const { buildSubscriptionData } = require("./planFeatures");

const cancelSubscription = onCall(
  { cors: true, memory: "256MiB", timeoutSeconds: 30 },
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
      const freeSubscription = buildSubscriptionData("free", "month");
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

    // Real mode (futuro): marcar cancelAtPeriodEnd sin borrar features
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

    return {
      success: true,
      effectiveDate,
      newPlan: "free",
      message: `Subscription will be canceled at end of period: ${effectiveDate}`,
    };
  }
);

module.exports = { cancelSubscription };
