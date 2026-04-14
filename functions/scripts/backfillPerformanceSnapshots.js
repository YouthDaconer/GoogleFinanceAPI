/**
 * PERF-SNAP-005: Script de backfill para generar snapshots de datos existentes
 *
 * Itera sobre todos los usuarios con datos en portfolioPerformance/ y genera
 * snapshots pre-computados usando generateAllSnapshots().
 *
 * USO:
 *   cd src/GoogleFinanceAPI/functions
 *   node scripts/backfillPerformanceSnapshots.js --dry-run
 *   node scripts/backfillPerformanceSnapshots.js
 *
 * @see docs/stories/PERF-SNAP-005.story.md
 * @see docs/architecture/AUDIT-PORTFOLIO-PERFORMANCE-ARCHITECTURE.md
 */

const admin = require('firebase-admin');
const path = require('path');

const DEFAULT_BATCH_SIZE = 5;

async function backfillAllSnapshots(db, { dryRun = false, batchSize = DEFAULT_BATCH_SIZE } = {}) {
  const { getActiveCurrencies, getUserAccounts } = require('../services/backfillCoreModule');
  const { generateAllSnapshots } = require('../services/snapshotGenerator');

  const usersSnapshot = await db.collection('portfolioPerformance').get();
  const userIds = usersSnapshot.docs.map(doc => doc.id);

  console.log(`[Backfill] ${userIds.length} usuarios encontrados. Dry run: ${dryRun}`);

  if (userIds.length === 0) {
    return { processed: 0, failed: 0, total: 0 };
  }

  const currencies = await getActiveCurrencies();
  console.log(`[Backfill] Monedas activas: ${currencies.join(', ')}`);

  let processed = 0;
  let failed = 0;

  for (let i = 0; i < userIds.length; i += batchSize) {
    const batch = userIds.slice(i, i + batchSize);

    await Promise.all(batch.map(async (userId) => {
      try {
        const accounts = await getUserAccounts(userId);
        const accountIds = accounts.map(a => a.id);

        if (dryRun) {
          const combos = currencies.length * (accountIds.length + 1);
          processed++;
          console.log(`[DRY-RUN] [${processed}/${userIds.length}] ${userId}: ${currencies.length} monedas × ${accountIds.length + 1} cuentas = ${combos} snapshots`);
          return;
        }

        const result = await generateAllSnapshots(db, userId, currencies, accountIds);
        processed++;
        console.log(`[${processed}/${userIds.length}] ${userId}: ${result.success} OK, ${result.failed} fallidos`);
      } catch (error) {
        failed++;
        console.error(`[ERROR] [${processed + failed}/${userIds.length}] ${userId}: ${error.message}`);
      }
    }));
  }

  const summary = { processed, failed, total: userIds.length };
  console.log(`[Backfill] Completado: ${processed} procesados, ${failed} fallidos, ${userIds.length} total`);
  return summary;
}

async function main() {
  const dryRun = process.argv.includes('--dry-run');

  let credential;
  try {
    const serviceAccount = require('../key.json');
    credential = admin.credential.cert(serviceAccount);
  } catch (e) {
    credential = admin.credential.applicationDefault();
  }

  if (!admin.apps.length) {
    admin.initializeApp({ credential });
  }

  const db = admin.firestore();

  try {
    const result = await backfillAllSnapshots(db, { dryRun });
    process.exit(result.failed > 0 ? 1 : 0);
  } catch (error) {
    console.error('[Backfill] Error fatal:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = { backfillAllSnapshots };
