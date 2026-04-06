/**
 * Plan Features — Definición de features por plan de suscripción
 *
 * PAY-008: Features se leen de Firestore planDefinitions con cache in-memory (TTL 5 min).
 * Fallback a constantes hardcodeadas si Firestore no disponible.
 *
 * @see docs/stories/PAY-008.story.md
 * @module services/payment/planFeatures
 */

const admin = require("../firebaseAdmin");

const UNLIMITED = 999999;
const CACHE_TTL_MS = 5 * 60 * 1000;

/**
 * BUG-TRIAL-001: Configuración centralizada de trial.
 * Single Source of Truth para elegibilidad, duración e intervalo.
 */
const TRIAL_CONFIG = {
  PERIOD_DAYS: 30,
  ELIGIBLE_PLAN: "pro",
  ELIGIBLE_INTERVAL: "month",
};

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
  hasRealtimeStreaming: true,
  maxWatchlist: UNLIMITED,
  hasDividendProjections: true,
  hasBriefings: true,
  hasEtfAnalyzer: true,
};

const PLAN_FEATURES_FALLBACK = {
  free: {
    maxAccounts: 2,
    historyDays: 365,
    hasAlerts: true,
    alertLimit: 1,
    hasSimulators: false,
    hasRiskMetrics: false,
    hasAttribution: false,
    hasIntelligence: false,
    hasBacktesting: false,
    hasImport: true,
    hasExportCsv: false,
    hasExportPdf: false,
    hasTaxReports: false,
    hasAiInsights: false,
    maxAssets: UNLIMITED,
    supportLevel: "community",
    hasRealtimeStreaming: false,
    maxWatchlist: 3,
    hasDividendProjections: false,
    hasBriefings: false,
    hasEtfAnalyzer: false,
  },
  pro: { ...BASE_FEATURES },
  lifetime: { ...BASE_FEATURES, supportLevel: "priority" },
};

// Backward compatibility alias
const PLAN_FEATURES = PLAN_FEATURES_FALLBACK;

const VALID_PLANS = Object.keys(PLAN_FEATURES_FALLBACK);
const VALID_INTERVALS = ["month", "year", "lifetime"];

let cache = { data: null, timestamp: 0 };
let fetchPromise = null;

async function loadPlanDefinitions() {
  const db = admin.firestore();
  const snapshot = await db.collection("planDefinitions").get();
  const plans = {};
  snapshot.forEach((doc) => {
    const data = doc.data();
    if (data.features) {
      plans[doc.id] = data.features;
    }
  });
  return plans;
}

/**
 * Returns all plan features from Firestore with in-memory cache (TTL 5 min).
 * Uses promise coalescing to prevent multiple concurrent Firestore reads on cache miss.
 * Falls back to PLAN_FEATURES_FALLBACK if Firestore is unavailable.
 *
 * @returns {Promise<Record<string, object>>} Plan features keyed by planId
 */
async function getAllPlanFeatures() {
  const now = Date.now();
  if (cache.data && now - cache.timestamp < CACHE_TTL_MS) {
    return cache.data;
  }

  if (fetchPromise) {
    return fetchPromise;
  }

  fetchPromise = (async () => {
    try {
      const plans = await loadPlanDefinitions();
      if (Object.keys(plans).length >= 3) {
        cache = { data: plans, timestamp: Date.now() };
        return plans;
      }
      console.warn("[planFeatures] planDefinitions incomplete — using fallback");
      return PLAN_FEATURES_FALLBACK;
    } catch (err) {
      console.warn("[planFeatures] Firestore read failed — using fallback:", err.message);
      return PLAN_FEATURES_FALLBACK;
    } finally {
      fetchPromise = null;
    }
  })();

  return fetchPromise;
}

/**
 * Returns features for a specific plan from cache or Firestore.
 *
 * @param {string} planId - "free" | "pro" | "lifetime"
 * @returns {Promise<object>} Feature flags for the requested plan
 */
async function getPlanFeatures(planId) {
  const plans = await getAllPlanFeatures();
  return plans[planId] || plans.free || PLAN_FEATURES_FALLBACK.free;
}

async function buildSubscriptionData(planId, interval, status = "active", origin = "trial") {
  const resolvedPlan = VALID_PLANS.includes(planId) ? planId : "free";
  const features = { ...(await getPlanFeatures(resolvedPlan)) };
  const now = new Date();

  // BUG-TRIAL-001: Defensa en profundidad — trial solo válido para plan+interval elegible
  if (origin === "trial" && (resolvedPlan !== TRIAL_CONFIG.ELIGIBLE_PLAN || interval !== TRIAL_CONFIG.ELIGIBLE_INTERVAL)) {
    origin = "mock_checkout";
  }

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
    const days = origin === "trial"
      ? TRIAL_CONFIG.PERIOD_DAYS
      : (interval === "year" ? 365 : 30);
    periodEnd.setDate(periodEnd.getDate() + days);
    subscription.currentPeriodEnd = periodEnd.toISOString();
  }

  if (origin === "trial") {
    subscription.trialStartedAt = now.toISOString();
    subscription.hasUsedTrial = true;
  }

  return subscription;
}

module.exports = {
  PLAN_FEATURES,
  PLAN_FEATURES_FALLBACK,
  VALID_PLANS,
  VALID_INTERVALS,
  UNLIMITED,
  TRIAL_CONFIG,
  buildSubscriptionData,
  getPlanFeatures,
  getAllPlanFeatures,
  // PAY-008: exposed for test cache manipulation
  _resetCache: () => { cache = { data: null, timestamp: 0 }; fetchPromise = null; },
};
