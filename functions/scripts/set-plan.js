#!/usr/bin/env node

/**
 * CLI Script — Cambiar plan de suscripción de un usuario en Firestore
 *
 * Uso: node scripts/set-plan.js <userId> <planId> [interval] [status] [origin]
 *
 * Ejemplos:
 *   node scripts/set-plan.js abc123 pro month active mock_checkout
 *   node scripts/set-plan.js abc123 lifetime lifetime active mock_checkout
 *   node scripts/set-plan.js abc123 free
 *
 * @see docs/architecture/FEAT-GATE-001-feature-gating-subscription-plans-design.md
 */

const admin = require("../services/firebaseAdmin");
const { buildSubscriptionData, VALID_PLANS, VALID_INTERVALS } = require("../services/payment/planFeatures");

if (process.env.PAYMENT_MOCK_ENABLED !== "true") {
  console.error("Error: PAYMENT_MOCK_ENABLED is not set to 'true'. Aborting.");
  process.exit(1);
}

const [,, userId, planId, interval = "month", status = "active", origin = "mock_checkout"] = process.argv;

if (!userId || !planId) {
  console.error("Usage: node scripts/set-plan.js <userId> <planId> [interval] [status] [origin]");
  console.error(`  planId: ${VALID_PLANS.join(" | ")}`);
  console.error(`  interval: ${VALID_INTERVALS.join(" | ")} (default: month)`);
  console.error("  status: active | past_due | canceled (default: active)");
  console.error("  origin: trial | mock_checkout | checkout (default: mock_checkout)");
  process.exit(1);
}

if (!VALID_PLANS.includes(planId)) {
  console.error(`Invalid planId: "${planId}". Must be one of: ${VALID_PLANS.join(", ")}`);
  process.exit(1);
}

if (!VALID_INTERVALS.includes(interval)) {
  console.error(`Invalid interval: "${interval}". Must be one of: ${VALID_INTERVALS.join(", ")}`);
  process.exit(1);
}

async function main() {
  const subscription = buildSubscriptionData(planId, interval, status, origin);
  const db = admin.firestore();

  await db.collection("userData").doc(userId).set({ subscription }, { merge: true });

  console.log(`\n✅ Subscription updated for user: ${userId}`);
  console.log(`   Plan: ${planId} (${interval})`);
  console.log(`   Status: ${status}`);
  console.log(`   Features:`, JSON.stringify(subscription.features, null, 2));

  if (subscription.currentPeriodEnd) {
    console.log(`   Period ends: ${subscription.currentPeriodEnd}`);
  }
  if (subscription.purchasedAt) {
    console.log(`   Purchased at: ${subscription.purchasedAt}`);
  }

  process.exit(0);
}

main().catch((err) => {
  console.error("Error:", err.message);
  process.exit(1);
});
