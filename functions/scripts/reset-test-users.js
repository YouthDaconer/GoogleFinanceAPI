const admin = require('../services/firebaseAdmin');
const db = admin.firestore();

const FREE_SUB = {
  planId: 'free', status: 'active', interval: 'month',
  subscriptionOrigin: 'system', hasUsedTrial: false,
  trialStartedAt: null, trialEndedAt: null, currentPeriodEnd: null,
  cancelAtPeriodEnd: false, cancellationReason: null, cancellationComment: null,
  cancelledAt: null, scheduledCancelAt: null, purchasedAt: null,
  subscriptionId: null, providerCustomerId: null,
  updatedAt: new Date().toISOString(),
  features: {
    alertLimit: 1, hasAiInsights: false, hasAlerts: true, hasAttribution: false,
    hasBacktesting: false, hasBriefings: false, hasDividendProjections: false,
    hasEtfAnalyzer: false, hasExportCsv: false, hasExportPdf: false,
    hasImport: true, hasIntelligence: false, hasRealtimeStreaming: false,
    hasRiskMetrics: false, hasSimulators: false, hasTaxReports: false,
    historyDays: 365, maxAccounts: 2, maxAssets: 999999, maxWatchlist: 3,
    supportLevel: 'community'
  }
};

const UIDS = {
  A: 'gTZ6Ie8FckSqEqfMX9OWkMmJEqi2',
  B: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
};

async function main() {
  for (const [label, uid] of Object.entries(UIDS)) {
    await db.collection('userData').doc(uid).update({ subscription: FREE_SUB });
  }

  const evts = await db.collection('subscriptionEvents').get();
  if (evts.size > 0) {
    const batch = db.batch();
    evts.docs.forEach(doc => batch.delete(doc.ref));
    await batch.commit();
  }

  for (const [label, uid] of Object.entries(UIDS)) {
    const doc = await db.collection('userData').doc(uid).get();
    const s = doc.data().subscription;
    console.log(`User ${label}: planId=${s.planId} | hasUsedTrial=${s.hasUsedTrial} | origin=${s.subscriptionOrigin}`);
  }
  console.log(`subscriptionEvents cleaned: ${evts.size}`);
  console.log('--- RESET COMPLETE ---');
  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
