const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require("../firebaseAdmin");
const { buildSubscriptionData } = require("./planFeatures");
const { getPaymentProvider } = require("./providerFactory");
const { getCircuit } = require("../../utils/circuitBreaker");

const db = admin.firestore();

const LIMITS = {
  MAX_USERS_PER_RUN: 50,
  PAST_DUE_TIMEOUT_DAYS: 30,
};

async function degradeToFree(userRef, userId, reason, counters) {
  const freeData = await buildSubscriptionData("free", "month");
  freeData.subscriptionId = null;
  freeData.providerCustomerId = null;

  const batch = db.batch();
  batch.set(userRef, { subscription: freeData }, { merge: true });
  batch.set(db.collection("subscriptionEvents").doc(), {
    type: "RECONCILIATION_DOWNGRADE",
    userId,
    reason,
    processedAt: admin.firestore.FieldValue.serverTimestamp(),
  });
  await batch.commit();

  counters[reason] = (counters[reason] || 0) + 1;
}

async function processExpiredActive(now, counters) {
  const snapshot = await db
    .collection("userData")
    .where("subscription.status", "==", "active")
    .where("subscription.currentPeriodEnd", "<", now.toISOString())
    .limit(LIMITS.MAX_USERS_PER_RUN)
    .get();

  for (const doc of snapshot.docs) {
    const sub = doc.data()?.subscription;
    if (sub?.planId === "lifetime") continue;

    try {
      await degradeToFree(doc.ref, doc.id, "period_expired", counters);
    } catch (err) {
      console.error(`[reconcile-subs] Error expiring ${doc.id}:`, err.message);
      counters.errors++;
    }
  }
}

async function processPendingCancel(now, counters) {
  const snapshot = await db
    .collection("userData")
    .where("subscription.cancelAtPeriodEnd", "==", true)
    .where("subscription.currentPeriodEnd", "<", now.toISOString())
    .limit(LIMITS.MAX_USERS_PER_RUN)
    .get();

  for (const doc of snapshot.docs) {
    try {
      await degradeToFree(doc.ref, doc.id, "cancel_at_period_end", counters);
    } catch (err) {
      console.error(`[reconcile-subs] Error canceling ${doc.id}:`, err.message);
      counters.errors++;
    }
  }
}

async function processStalePastDue(now, counters) {
  const cutoff = new Date(now);
  cutoff.setDate(cutoff.getDate() - LIMITS.PAST_DUE_TIMEOUT_DAYS);

  const snapshot = await db
    .collection("userData")
    .where("subscription.status", "==", "past_due")
    .limit(LIMITS.MAX_USERS_PER_RUN)
    .get();

  for (const doc of snapshot.docs) {
    const sub = doc.data()?.subscription;
    if (!sub?.updatedAt) continue;

    const updatedAt = new Date(sub.updatedAt);
    if (updatedAt >= cutoff) continue;

    try {
      await degradeToFree(doc.ref, doc.id, "past_due_timeout", counters);
    } catch (err) {
      console.error(`[reconcile-subs] Error past_due ${doc.id}:`, err.message);
      counters.errors++;
    }
  }
}

async function reconcileWithProvider(counters) {
  if (process.env.RECONCILE_WITH_PROVIDER !== "true") return;

  const provider = getPaymentProvider();
  if (!provider) return;

  const lsCircuit = getCircuit("lemonSqueezy", {
    failureThreshold: 3,
    resetTimeout: 30000,
    halfOpenRequests: 1,
  });

  const activeWithSub = await db
    .collection("userData")
    .where("subscription.status", "==", "active")
    .where("subscription.subscriptionId", "!=", null)
    .limit(LIMITS.MAX_USERS_PER_RUN)
    .get();

  for (const doc of activeWithSub.docs) {
    const sub = doc.data()?.subscription;
    if (!sub?.subscriptionId) continue;

    try {
      const providerSub = await lsCircuit.execute(
        () => provider.getSubscription(sub.subscriptionId),
        () => null
      );

      if (!providerSub) continue;

      const providerStatus = providerSub.status;
      if (
        (providerStatus === "cancelled" || providerStatus === "expired") &&
        sub.status === "active"
      ) {
        console.warn(
          `[reconcile-subs] Discrepancy: user ${doc.id} LS=${providerStatus} Firestore=${sub.status}. Correcting.`
        );
        await degradeToFree(doc.ref, doc.id, "provider_discrepancy", counters);
      }
    } catch (err) {
      console.error(`[reconcile-subs] Provider check failed for ${doc.id}:`, err.message);
      counters.errors++;
    }
  }
}

const reconcileSubscriptions = onSchedule(
  {
    schedule: "0 3 * * *",
    timeZone: "America/New_York",
    memory: "512MiB",
    timeoutSeconds: 300,
    maxInstances: 1,
    labels: { component: "payment", purpose: "reconcile-subscriptions" },
  },
  async () => {
    console.log("[reconcile-subs] Starting subscription reconciliation...");
    const now = new Date();
    const counters = { period_expired: 0, cancel_at_period_end: 0, past_due_timeout: 0, errors: 0 };

    await processExpiredActive(now, counters);
    await processPendingCancel(now, counters);
    await processStalePastDue(now, counters);
    await reconcileWithProvider(counters);

    console.log(
      `[reconcile-subs] DONE: ${counters.period_expired} expired, ${counters.cancel_at_period_end} canceled, ${counters.past_due_timeout} past_due_timeout, ${counters.errors} errors`
    );
  }
);

module.exports = {
  reconcileSubscriptions,
  LIMITS,
  // Exported for testing
  processExpiredActive,
  processPendingCancel,
  processStalePastDue,
  reconcileWithProvider,
  degradeToFree,
};
