/**
 * Mock Subscription Service — Cloud Function Callable para desarrollo
 *
 * Permite cambiar el plan de suscripción de un usuario en Firestore
 * sin depender de pasarela de pagos real. Protegida por PAYMENT_MOCK_ENABLED.
 *
 * @see docs/architecture/FEAT-GATE-001-feature-gating-subscription-plans-design.md
 * @module services/payment/mockSubscriptionService
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require("../firebaseAdmin");
const { buildSubscriptionData, VALID_PLANS } = require("./planFeatures");

const mockSetSubscription = onCall(
  { cors: true, memory: "256MiB", timeoutSeconds: 30 },
  async (request) => {
    if (process.env.PAYMENT_MOCK_ENABLED !== "true") {
      throw new HttpsError(
        "failed-precondition",
        "Mock subscription provider is disabled"
      );
    }

    if (!request.auth?.uid) {
      throw new HttpsError("unauthenticated", "Authentication required");
    }

    const { planId, interval = "month", status = "active" } = request.data || {};

    if (!VALID_PLANS.includes(planId)) {
      throw new HttpsError(
        "invalid-argument",
        `Invalid planId: "${planId}". Must be one of: ${VALID_PLANS.join(", ")}`
      );
    }

    const userId = request.auth.uid;
    const db = admin.firestore();

    // BUG-GATE-002: Determinar origin según el flujo
    // Free → Pro: trial (30 días de prueba gratuita)
    // Free → Lifetime / cualquier otro: mock_checkout (compra directa)
    // El caller puede forzar origin para DevPanel u otros flujos administrativos
    const forceOrigin = request.data?.origin;
    let origin;
    if (forceOrigin) {
      origin = forceOrigin;
    } else if (planId === "pro") {
      const userDoc = await db.collection("userData").doc(userId).get();
      const currentPlan = userDoc.data()?.subscription?.planId || "free";
      // Solo trial si viene de Free. Re-subscripciones post-cancelación = mock_checkout
      origin = currentPlan === "free" ? "trial" : "mock_checkout";
    } else {
      origin = "mock_checkout";
    }

    const subscription = buildSubscriptionData(planId, interval, status, origin);

    await db.collection("userData").doc(userId).set({ subscription }, { merge: true });

    return {
      success: true,
      plan: planId,
      features: subscription.features,
      message: `Subscription updated to ${planId} (${interval})`,
    };
  }
);

module.exports = { mockSetSubscription };
