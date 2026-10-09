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
const { getFirestore, FieldValue } = require('firebase-admin/firestore');
const { generateTradingDays, getTodayDate, backfillUserPerformance } = require('./backfillCoreModule');

const db = getFirestore();

// ============================================================================
// LIMITS AND CONFIGURATION
// ============================================================================

const LIMITS = {
  /** Maximum users to process per run */
  MAX_USERS_PER_RUN: 20,
  
  /** Maximum trading days to recalculate per user */
  MAX_TRADING_DAYS: 60,
  
  /** Maximum retry attempts before skipping user */
  MAX_RETRY_COUNT: 3,
};

// ============================================================================
// MAIN SCHEDULED FUNCTION
// ============================================================================

/**
 * Scheduled function to reconcile stale portfolio performance data
 * 
 * Runs at 02:00 ET Tuesday through Saturday to:
 * 1. Find users with _stale markers
 * 2. Recalculate their performance for affected date ranges
 * 3. Clear or update stale markers based on results
 */
const reconcileStalePerformance = onSchedule({
  schedule: '0 2 * * 2-6',    // 02:00 ET, Tuesday to Saturday
  timeZone: 'America/New_York',
  memory: '512MiB',
  timeoutSeconds: 300,        // 5 minutes
  maxInstances: 5,
  labels: {
    component: 'late-reg',
    purpose: 'reconcile-stale',
  },
}, async (event) => {
  
  console.log('[reconcile] Starting stale performance reconciliation...');
  
  const startTime = Date.now();
  let reconciled = 0;
  let failed = 0;
  let skipped = 0;
  let totalDays = 0;
  
  try {
    // 1. Find users with stale data
    const staleQuery = await db.collection('portfolioPerformance')
      .where('_stale', '!=', null)
      .orderBy('_stale.since', 'desc') // Most recent first
      .limit(LIMITS.MAX_USERS_PER_RUN)
      .get();
    
    if (staleQuery.empty) {
      console.log('[reconcile] No stale data found. Skipping.');
      return;
    }
    
    console.log(`[reconcile] Found ${staleQuery.size} users with stale data`);
    
    // 2. Process each user
    for (const doc of staleQuery.docs) {
      const userId = doc.id;
      const data = doc.data();
      const stale = data._stale;
      
      // Skip if exceeded max retries - clean up and log to failures collection
      if ((stale.retryCount || 0) >= LIMITS.MAX_RETRY_COUNT) {
        console.error(`[reconcile] ALERT: User ${userId} exceeded max retries (${stale.retryCount}). Cleaning up stale marker.`);
        
        // Log to reconcileFailures collection for manual review
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
        
        skipped++;
        continue;
      }
      
      try {
        // 3. Calculate date range to recalculate
        const today = getTodayDate();
        let tradingDays = await generateTradingDays(stale.since, today);
        
        // 4. Truncate if exceeds limit
        if (tradingDays.length > LIMITS.MAX_TRADING_DAYS) {
          console.warn(`[reconcile] User ${userId}: ${tradingDays.length} days requested, truncating to ${LIMITS.MAX_TRADING_DAYS}`);
          tradingDays = tradingDays.slice(-LIMITS.MAX_TRADING_DAYS);
        }
        
        if (tradingDays.length === 0) {
          console.log(`[reconcile] User ${userId}: No trading days to process. Clearing stale.`);
          await doc.ref.update({ _stale: FieldValue.delete() });
          reconciled++;
          continue;
        }
        
        // 5. Execute backfill
        console.log(`[reconcile] User ${userId}: Processing ${tradingDays.length} days from ${tradingDays[0]}`);
        
        const result = await backfillUserPerformance(userId, tradingDays);
        
        if (result.success) {
          // 6. Clear stale marker on success
          await doc.ref.update({ _stale: FieldValue.delete() });
          reconciled++;
          totalDays += result.daysProcessed;
          console.log(`[reconcile] User ${userId}: SUCCESS - ${result.daysProcessed} days reconciled`);
        } else {
          // 7. Increment retry count on failure
          await doc.ref.update({
            '_stale.retryCount': FieldValue.increment(1),
            '_stale.lastError': result.errors.slice(0, 3).join('; '), // Keep first 3 errors
            '_stale.lastAttempt': new Date().toISOString(),
          });
          failed++;
          console.error(`[reconcile] User ${userId}: FAILED - ${result.errors.length} errors`);
        }
        
      } catch (userError) {
        console.error(`[reconcile] User ${userId}: Exception -`, userError.message);
        
        // Update stale with error info
        try {
          await doc.ref.update({
            '_stale.retryCount': FieldValue.increment(1),
            '_stale.lastError': userError.message,
            '_stale.lastAttempt': new Date().toISOString(),
          });
        } catch (updateError) {
          console.error(`[reconcile] Failed to update stale for ${userId}:`, updateError.message);
        }
        
        failed++;
      }
    }
    
    // 8. Log summary
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[reconcile] DONE: ${reconciled} reconciled, ${failed} failed, ${skipped} skipped, ${totalDays} total days in ${duration}s`);
    
    // 9. Alert if high failure rate
    if (failed > 5) {
      console.error(`[reconcile] ALERT: High failure rate - ${failed} failures in this run`);
      // TODO: Integrate with alerting system (Slack, PagerDuty, etc.)
    }
    
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
  LIMITS, // Export for testing
};
