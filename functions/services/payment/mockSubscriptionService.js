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
const { buildSubscriptionData, VALID_PLANS, TRIAL_CONFIG } = require("./planFeatures");

const mockSetSubscription = onCall(
  { cors: true, memory: "256MiB", timeoutSeconds: 30 },
  async (request) => {
    // F0-02: Defensa en profundidad — requiere PAYMENT_MOCK_ENABLED=true Y Custom Claim admin
    if (process.env.PAYMENT_MOCK_ENABLED !== "true") {
      throw new HttpsError(
        "failed-precondition",
        "Mock subscription provider is disabled"
      );
    }

    if (!request.auth?.uid) {
      throw new HttpsError("unauthenticated", "Authentication required");
    }

    // F0-02: Solo administradores pueden usar mock en producción
    // En emulador/dev, el Custom Claim puede no existir — se permite si NODE_ENV no es production
    const isAdmin = request.auth.token?.admin === true;
    const isProduction = process.env.NODE_ENV === "production" || process.env.K_SERVICE;
    if (isProduction && !isAdmin) {
      throw new HttpsError(
        "permission-denied",
        "Mock subscription requires admin privileges in production"
      );
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

    // F0-03: Downgrade guard server-side — impide upgrades fraudulentos
    const PLAN_RANK = { free: 0, pro: 1, lifetime: 2 };
    const userDoc = await db.collection("userData").doc(userId).get();
    const currentPlan = userDoc.data()?.subscription?.planId || "free";
    const currentRank = PLAN_RANK[currentPlan] ?? 0;
    const targetRank = PLAN_RANK[planId] ?? 0;

    // Block upgrades from paid plans AND same-plan re-subscription
    // Exception: trial users haven't paid — they can subscribe to any plan
    const isOnTrial = userDoc.data()?.subscription?.subscriptionOrigin === "trial";
    if (currentPlan !== "free" && targetRank >= currentRank && !isOnTrial) {
      throw new HttpsError(
        "failed-precondition",
        `Cannot ${targetRank === currentRank ? 're-subscribe to' : 'upgrade from'} ${currentPlan} to ${planId} via mock. Use the payment gateway.`
      );
    }

    // BUG-GATE-002 + PAY-009 + BUG-TRIAL-001: Determinar origin según el flujo
    // Free → Pro Monthly (sin trial previo): trial (30 días de prueba gratuita)
    // Free → Pro Annual (sin trial previo): mock_checkout (compra directa, sin trial)
    // Free → Pro (con trial previo): mock_checkout (compra directa, sin trial)
    // Free → Lifetime / cualquier otro: mock_checkout (compra directa)
    const VALID_ORIGINS = ["trial", "mock_checkout", "checkout"];
    const rawForceOrigin = request.data?.origin;
    const forceOrigin = VALID_ORIGINS.includes(rawForceOrigin) ? rawForceOrigin : null;
    const hasUsedTrial = userDoc.data()?.subscription?.hasUsedTrial === true;
    let origin;
    if (forceOrigin) {
      origin = forceOrigin;
    } else if (planId === TRIAL_CONFIG.ELIGIBLE_PLAN && interval === TRIAL_CONFIG.ELIGIBLE_INTERVAL) {
      origin = (currentPlan === "free" && !hasUsedTrial) ? "trial" : "mock_checkout";
    } else {
      origin = "mock_checkout";
    }

    const subscription = await buildSubscriptionData(planId, interval, status, origin);

    // PAY-009: Preserve sticky trial fields when converting trial → paid
    const previousSubscription = userDoc.data()?.subscription;
    if (previousSubscription?.hasUsedTrial) {
      subscription.hasUsedTrial = true;
    }
    if (previousSubscription?.trialStartedAt) {
      subscription.trialStartedAt = previousSubscription.trialStartedAt;
    }
    if (previousSubscription?.subscriptionOrigin === "trial" && origin !== "trial") {
      subscription.trialEndedAt = new Date().toISOString();
    } else if (previousSubscription?.trialEndedAt) {
      subscription.trialEndedAt = previousSubscription.trialEndedAt;
    }

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
