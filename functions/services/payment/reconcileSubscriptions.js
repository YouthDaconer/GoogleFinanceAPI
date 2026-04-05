const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require("../firebaseAdmin");
const { buildSubscriptionData } = require("./planFeatures");
const { getPaymentProvider } = require("./providerFactory");
const { getCircuit } = require("../../utils/circuitBreaker");
const { sendTransactionalEmail, EMAIL_TEMPLATES } = require("./emailService");

const db = admin.firestore();

const LIMITS = {
  MAX_USERS_PER_RUN: 50,
  PAST_DUE_TIMEOUT_DAYS: 30,
  MAX_TRIAL_WARNINGS: 20,
};

async function degradeToFree(userRef, userId, reason, counters) {
  const userDoc = await userRef.get();
  const currentSub = userDoc.data()?.subscription;

  const freeData = await buildSubscriptionData("free", "month");
  freeData.subscriptionId = null;
  freeData.providerCustomerId = null;

  // PAY-009: Preservar trial history (campos sticky)
  if (currentSub?.hasUsedTrial) {
    freeData.hasUsedTrial = true;
  }
  if (currentSub?.trialStartedAt) {
    freeData.trialStartedAt = currentSub.trialStartedAt;
  }
  if (currentSub?.subscriptionOrigin === "trial") {
    freeData.trialEndedAt = new Date().toISOString();
  }

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

async function processTrialWarnings(now, counters) {
  const warningCutoff = new Date(now);
  warningCutoff.setDate(warningCutoff.getDate() + 5);

  const snapshot = await db
    .collection("userData")
    .where("subscription.status", "==", "active")
    .where("subscription.subscriptionOrigin", "==", "trial")
    .where("subscription.currentPeriodEnd", "<=", warningCutoff.toISOString())
    .where("subscription.currentPeriodEnd", ">", now.toISOString())
    .limit(LIMITS.MAX_TRIAL_WARNINGS)
    .get();

  for (const doc of snapshot.docs) {
    const userData = doc.data();
    const sub = userData?.subscription;
    if (!sub || sub.planId === "lifetime") continue;
    if (sub.trialWarningEmailSent) continue;

    const email = userData?.email;
    if (!email) continue;

    const daysLeft = Math.ceil(
      (new Date(sub.currentPeriodEnd).getTime() - now.getTime()) / (1000 * 60 * 60 * 24)
    );

    try {
      await sendTransactionalEmail(EMAIL_TEMPLATES.TRIAL_EXPIRING, email, {
        userName: userData?.displayName || email.split("@")[0],
        daysLeft,
        expiryDate: sub.currentPeriodEnd,
        pricingUrl: `${process.env.APP_URL || "https://portastock.net"}/pricing`,
      }, { maxRetries: 0, timeoutMs: 3000 });

      await doc.ref.set(
        { subscription: { trialWarningEmailSent: true } },
        { merge: true }
      );

      counters.trial_warnings = (counters.trial_warnings || 0) + 1;
    } catch (err) {
      console.error(`[reconcile-subs] Trial warning email failed for ${doc.id}:`, err.message);
    }
  }
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
    secrets: ["AWS_SES_ACCESS_KEY_ID", "AWS_SES_SECRET_ACCESS_KEY"],
    labels: { component: "payment", purpose: "reconcile-subscriptions" },
  },
  async () => {
    console.log("[reconcile-subs] Starting subscription reconciliation...");
    const now = new Date();
    const counters = { trial_warnings: 0, period_expired: 0, cancel_at_period_end: 0, past_due_timeout: 0, errors: 0 };

    await processTrialWarnings(now, counters);
    await processExpiredActive(now, counters);
    await processPendingCancel(now, counters);
    await processStalePastDue(now, counters);
    await reconcileWithProvider(counters);

    console.log(
      `[reconcile-subs] DONE: ${counters.trial_warnings} trial_warnings, ${counters.period_expired} expired, ${counters.cancel_at_period_end} canceled, ${counters.past_due_timeout} past_due_timeout, ${counters.errors} errors`
    );
  }
);

module.exports = {
  reconcileSubscriptions,
  LIMITS,
  // Exported for testing
  processTrialWarnings,
  processExpiredActive,
  processPendingCancel,
  processStalePastDue,
  reconcileWithProvider,
  degradeToFree,
};
