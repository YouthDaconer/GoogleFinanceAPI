/**
 * reconcileStalePerformance.js
 *
 * Cloud Function scheduled that recalculates portfolioPerformance
 * for users with stale data markers.
 *
 * Schedule: 02:00 ET, Tuesday through Saturday
 *
 * @see LATE-REG-003
 * @see docs/architecture/LATE-REGISTRATION-001-retroactive-transactions-analysis.md
 */

const { onSchedule } = require('firebase-functions/v2/scheduler');
const { getFirestore, FieldValue, FieldPath } = require('firebase-admin/firestore');
const { DateTime } = require('luxon');
const {
  generateTradingDays,
  backfillUserPerformance,
  clearHistoricalPriceCache,
} = require('./backfillCoreModule');
const { consolidateMonth, consolidateYear } = require('./periodConsolidationScheduled');
const { invalidatePerformanceCache } = require('./cacheInvalidationService');

const db = getFirestore();

// ============================================================================
// LIMITS AND CONFIGURATION
// ============================================================================

const LIMITS = {
  /** Usuarios por consulta; se siguen pidiendo mientras quede tiempo */
  USERS_PER_PAGE: 20,

  /**
   * Tope de días por usuario (~2 años, el rango '2y' de precios). Recalcular
   * es CPU y un batch por día; lo caro son los precios, que se piden una vez
   * por símbolo y corrida.
   */
  MAX_TRADING_DAYS: 520,

  /** Maximum retry attempts before skipping user */
  MAX_RETRY_COUNT: 3,

  /**
   * No empezar otro usuario pasado este tiempo: deja margen al timeout de la
   * función para terminar el que está en curso y propagarlo.
   */
  TIME_BUDGET_MS: 420 * 1000,
};

// ============================================================================
// HELPERS
// ============================================================================

/**
 * Último día que ya cerró el EOD: ayer en Nueva York. La corrida es a las
 * 02:00 ET y el día en curso no tiene cierre todavía; recalcularlo escribía un
 * doc con el precio del día anterior que el EOD tenía que pisar después.
 */
function getLastClosedDate(now = DateTime.now().setZone('America/New_York')) {
  return now.minus({ days: 1 }).toISODate();
}

/**
 * Meses (YYYY-MM) ya cerrados entre `firstDay` y el mes anterior al actual.
 * Sus consolidados se generaron una sola vez y el V2 de rendimientos los lee
 * en lugar de los docs diarios, así que hay que rehacerlos.
 */
function closedMonthsSince(firstDay, now = DateTime.now().setZone('America/New_York')) {
  const currentMonth = now.toFormat('yyyy-MM');
  const months = [];
  let month = DateTime.fromISO(firstDay).startOf('month');
  while (month.toFormat('yyyy-MM') < currentMonth) {
    months.push(month.toFormat('yyyy-MM'));
    month = month.plus({ months: 1 });
  }
  return months;
}

/**
 * Hace llegar el recálculo a todo lo que se deriva de los docs diarios:
 * consolidados de meses y años cerrados, snapshots, caché de rendimientos y la
 * señal que invalida las cachés del cliente. Sin esto los docs quedaban bien
 * pero el usuario seguía viendo los números viejos.
 */
async function propagateReconciledRange(userId, firstDay, accountIds) {
  const scopes = [null, ...accountIds];
  const months = closedMonthsSince(firstDay);
  const currentYear = DateTime.now().setZone('America/New_York').year;
  const closedYears = [...new Set(months.map((m) => m.slice(0, 4)))].filter((y) => Number(y) < currentYear);

  for (const periodKey of months) {
    for (const accountId of scopes) {
      await consolidateMonth(userId, accountId, periodKey);
    }
  }
  for (const yearKey of closedYears) {
    for (const accountId of scopes) {
      await consolidateYear(userId, accountId, yearKey);
    }
  }

  // Snapshots del usuario (portafolio, cuentas y activos): se regeneran al
  // leerlos o en el próximo EOD. Actualizarlos en sitio no sirve, el
  // incremental del EOD sólo añade el último día sobre la historia vieja.
  const snapshots = db.collection('performanceSnapshots');
  const prefix = `${userId}_`;
  const snapshotDocs = await snapshots
    .where(FieldPath.documentId(), '>=', snapshots.doc(prefix))
    .where(FieldPath.documentId(), '<', snapshots.doc(`${prefix}`))
    .get();
  for (let i = 0; i < snapshotDocs.docs.length; i += 400) {
    const batch = db.batch();
    snapshotDocs.docs.slice(i, i + 400).forEach((d) => batch.delete(d.ref));
    await batch.commit();
  }

  await invalidatePerformanceCache(userId);

  const signal = { lastSnapshotUpdate: new Date().toISOString() };
  await db.collection('portfolioPerformance').doc(userId).set(signal, { merge: true });
  await db.collection('userData').doc(userId).set(signal, { merge: true });

  console.log(`[reconcile] User ${userId}: propagado — ${months.length} meses, ${closedYears.length} años, ` +
    `${snapshotDocs.size} snapshots borrados`);
}

/**
 * Reconciliar un usuario: recalcula desde `_stale.since` hasta el último día
 * cerrado y lo propaga. Devuelve 'reconciled' | 'failed'.
 */
async function reconcileUser(doc) {
  const userId = doc.id;
  const stale = doc.data()._stale;

  let tradingDays = await generateTradingDays(stale.since, getLastClosedDate());

  if (tradingDays.length > LIMITS.MAX_TRADING_DAYS) {
    console.warn(`[reconcile] User ${userId}: ${tradingDays.length} days requested, truncating to ${LIMITS.MAX_TRADING_DAYS}`);
    tradingDays = tradingDays.slice(-LIMITS.MAX_TRADING_DAYS);
  }

  if (tradingDays.length === 0) {
    console.log(`[reconcile] User ${userId}: No trading days to process. Clearing stale.`);
    await doc.ref.update({ _stale: FieldValue.delete() });
    return { status: 'reconciled', days: 0 };
  }

  console.log(`[reconcile] User ${userId}: Processing ${tradingDays.length} days from ${tradingDays[0]}`);
  const result = await backfillUserPerformance(userId, tradingDays);

  if (!result.success) {
    await doc.ref.update({
      '_stale.retryCount': FieldValue.increment(1),
      '_stale.lastError': result.errors.slice(0, 3).join('; '), // Keep first 3 errors
      '_stale.lastAttempt': new Date().toISOString(),
    });
    console.error(`[reconcile] User ${userId}: FAILED - ${result.errors.length} errors`);
    return { status: 'failed', days: result.daysProcessed };
  }

  const accountIds = (await db.collection('portfolioAccounts').where('userId', '==', userId).get())
    .docs.map((d) => d.id);
  await propagateReconciledRange(userId, tradingDays[0], accountIds);

  // Borrar el marcador sólo si nadie lo movió mientras tanto: si entró otra
  // transacción tardía durante la corrida, su `since` debe sobrevivir.
  await db.runTransaction(async (tx) => {
    const fresh = await tx.get(doc.ref);
    const current = fresh.data()?._stale;
    if (current && current.registeredAt === stale.registeredAt && current.since === stale.since) {
      tx.update(doc.ref, { _stale: FieldValue.delete() });
    }
  });

  console.log(`[reconcile] User ${userId}: SUCCESS - ${result.daysProcessed} days reconciled`);
  return { status: 'reconciled', days: result.daysProcessed };
}

/** Abandona un usuario que agotó los reintentos y lo deja para revisión manual. */
async function abandonUser(doc) {
  const userId = doc.id;
  const stale = doc.data()._stale;
  console.error(`[reconcile] ALERT: User ${userId} exceeded max retries (${stale.retryCount}). Cleaning up stale marker.`);
  try {
    await db.collection('reconcileFailures').doc(userId).set({
      userId,
      staleData: stale,
      abandonedAt: new Date().toISOString(),
      reason: `Exceeded MAX_RETRY_COUNT (${LIMITS.MAX_RETRY_COUNT})`,
      affectedRange: {
        since: stale.since,
        tradingDaysEstimate: stale.truncated ? `>${LIMITS.MAX_TRADING_DAYS}` : 'unknown'
      }
    }, { merge: true });

    // Clean up the _stale marker to prevent infinite skip loop
    await doc.ref.update({ _stale: FieldValue.delete() });
    console.warn(`[reconcile] User ${userId}: Stale marker cleaned. Failure logged to reconcileFailures collection.`);
  } catch (cleanupError) {
    console.error(`[reconcile] Failed to cleanup stale for ${userId}:`, cleanupError.message);
  }
}

/**
 * Recorre los usuarios marcados en páginas hasta vaciar la cola o agotar el
 * tiempo. Antes eran 20 usuarios por corrida y cinco corridas por semana: con
 * más usuarios la cola crecía más rápido de lo que se vaciaba.
 */
async function runReconciliation({ timeBudgetMs = LIMITS.TIME_BUDGET_MS } = {}) {
  const startTime = Date.now();
  const seen = new Set();
  const totals = { reconciled: 0, failed: 0, skipped: 0, days: 0 };

  // Una corrida, una caché de precios: se comparte entre usuarios
  clearHistoricalPriceCache();

  while (Date.now() - startTime < timeBudgetMs) {
    // Los reconciliados salen de la consulta; los fallidos siguen marcados y
    // ocupan sitio, por eso el límite crece con los ya vistos.
    const limit = LIMITS.USERS_PER_PAGE + seen.size;
    const page = await db.collection('portfolioPerformance')
      .where('_stale', '!=', null)
      .orderBy('_stale.since', 'desc') // Most recent first
      .limit(limit)
      .get();

    const pending = page.docs.filter((d) => !seen.has(d.id));
    if (pending.length === 0) break;

    for (const doc of pending) {
      if (Date.now() - startTime >= timeBudgetMs) break;
      seen.add(doc.id);

      if ((doc.data()._stale.retryCount || 0) >= LIMITS.MAX_RETRY_COUNT) {
        await abandonUser(doc);
        totals.skipped++;
        continue;
      }

      try {
        const { status, days } = await reconcileUser(doc);
        totals[status]++;
        totals.days += days;
      } catch (userError) {
        console.error(`[reconcile] User ${doc.id}: Exception -`, userError.message);
        try {
          await doc.ref.update({
            '_stale.retryCount': FieldValue.increment(1),
            '_stale.lastError': userError.message,
            '_stale.lastAttempt': new Date().toISOString(),
          });
        } catch (updateError) {
          console.error(`[reconcile] Failed to update stale for ${doc.id}:`, updateError.message);
        }
        totals.failed++;
      }
    }

    // Página incompleta: no queda nadie más en la cola
    if (page.size < limit) break;
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`[reconcile] DONE: ${totals.reconciled} reconciled, ${totals.failed} failed, ` +
    `${totals.skipped} skipped, ${totals.days} total days in ${duration}s`);

  if (totals.failed > 5) {
    console.error(`[reconcile] ALERT: High failure rate - ${totals.failed} failures in this run`);
  }
  return totals;
}

// ============================================================================
// MAIN SCHEDULED FUNCTION
// ============================================================================

/**
 * Scheduled function to reconcile stale portfolio performance data
 *
 * Runs at 02:00 ET Tuesday through Saturday to:
 * 1. Find users with _stale markers
 * 2. Recalculate their performance for affected date ranges
 * 3. Propagate the result and clear the stale markers
 */
const reconcileStalePerformance = onSchedule({
  schedule: '0 2 * * 2-6',    // 02:00 ET, Tuesday to Saturday
  timeZone: 'America/New_York',
  memory: '512MiB',
  timeoutSeconds: 540,        // 9 minutes; TIME_BUDGET_MS deja margen
  maxInstances: 1,            // una corrida a la vez: comparten la cola
  labels: {
    component: 'late-reg',
    purpose: 'reconcile-stale',
  },
}, async () => {
  console.log('[reconcile] Starting stale performance reconciliation...');
  try {
    await runReconciliation();
  } catch (error) {
    console.error('[reconcile] Fatal error:', error);
    throw error; // Re-throw so Cloud Functions records it as a failure
  }
});

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  reconcileStalePerformance,
  runReconciliation,
  reconcileUser,
  propagateReconciledRange,
  getLastClosedDate,
  closedMonthsSince,
  LIMITS, // Export for testing
};
