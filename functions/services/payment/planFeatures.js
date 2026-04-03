/**
 * Plan Features — Definición de features por plan de suscripción
 *
 * Módulo compartido entre Mock Provider (GATE-001) y futuro Payment Provider (STRIPE-001).
 * NO importa ningún SDK de pasarela de pagos.
 *
 * @see docs/architecture/FEAT-GATE-001-feature-gating-subscription-plans-design.md
 * @module services/payment/planFeatures
 */

const UNLIMITED = 999999;

const BASE_FEATURES = {
  maxAccounts: UNLIMITED,
  historyDays: UNLIMITED,
  hasAlerts: true,
  alertLimit: UNLIMITED,
  hasSimulators: true,
  hasRiskMetrics: true,
  hasAttribution: true,
  hasIntelligence: true,
  hasBacktesting: true,
  hasImport: true,
  hasExportCsv: true,
  hasExportPdf: true,
  hasTaxReports: true,
  hasAiInsights: true,
  maxAssets: UNLIMITED,
  supportLevel: "email",
  // ── NUEVOS (5) — FEAT-PRICING-RESTRUCTURE-001 ──
  hasRealtimeStreaming: true,
  maxWatchlist: UNLIMITED,
  hasDividendProjections: true,
  hasBriefings: true,
  hasEtfAnalyzer: true,
};

const PLAN_FEATURES = {
  free: {
    maxAccounts: 2,
    historyDays: 365,              // CAMBIO: 90 → 365 (FEAT-PRICING-RESTRUCTURE-001)
    hasAlerts: true,               // CAMBIO: false → true
    alertLimit: 1,                 // CAMBIO: 0 → 1
    hasSimulators: false,
    hasRiskMetrics: false,
    hasAttribution: false,
    hasIntelligence: false,
    hasBacktesting: false,
    hasImport: true,               // CAMBIO: false → true
    hasExportCsv: false,
    hasExportPdf: false,
    hasTaxReports: false,
    hasAiInsights: false,
    maxAssets: UNLIMITED,
    supportLevel: "community",
    // ── NUEVOS (5) — FEAT-PRICING-RESTRUCTURE-001 ──
    hasRealtimeStreaming: false,
    maxWatchlist: 3,
    hasDividendProjections: false,
    hasBriefings: false,
    hasEtfAnalyzer: false,
  },
  pro: { ...BASE_FEATURES },
  lifetime: { ...BASE_FEATURES, supportLevel: "priority" },
};

const VALID_PLANS = Object.keys(PLAN_FEATURES);
const VALID_INTERVALS = ["month", "year", "lifetime"];

/**
 * Construye el objeto de suscripción completo para escribir en Firestore.
 *
 * @param {string} planId - "free" | "pro" | "lifetime"
 * @param {string} interval - "month" | "year" | "lifetime"
 * @param {string} [status="active"] - Estado de la suscripción
 * @param {string} [origin="trial"] - Origen: "trial" | "mock_checkout" | "checkout"
 * @returns {object} Objeto de suscripción listo para Firestore
 */
function buildSubscriptionData(planId, interval, status = "active", origin = "trial") {
  const resolvedPlan = VALID_PLANS.includes(planId) ? planId : "free";
  const features = { ...PLAN_FEATURES[resolvedPlan] };
  const now = new Date();

  const subscription = {
    planId: resolvedPlan,
    status,
    interval: interval || "month",
    providerCustomerId: null,
    subscriptionId: null,
    currentPeriodEnd: null,
    cancelAtPeriodEnd: false,
    features,
    updatedAt: now.toISOString(),
    subscriptionOrigin: origin,
  };

  if (resolvedPlan === "lifetime") {
    subscription.purchasedAt = now.toISOString();
  } else if (resolvedPlan === "pro") {
    const periodEnd = new Date(now);
    const days = interval === "year" ? 365 : 30;
    periodEnd.setDate(periodEnd.getDate() + days);
    subscription.currentPeriodEnd = periodEnd.toISOString();
  }

  return subscription;
}

module.exports = { PLAN_FEATURES, VALID_PLANS, VALID_INTERVALS, UNLIMITED, buildSubscriptionData };
