/**
 * OPT-SNAP-INCR: Simulación de 3 días + Validación de Accuracy
 * 
 * Este script:
 * 1. Lee los daily docs reales del usuario (para extraer datos de 3 días recientes)
 * 2. Resetea el snapshot al estado de hace 3 días
 * 3. Ejecuta 3 actualizaciones incrementales consecutivas
 * 4. Genera un full-rebuild desde los mismos daily docs
 * 5. Compara: incremental vs full-rebuild (accuracy check)
 * 6. REVIERTE el snapshot al estado original
 * 
 * USO: node tests/test-incremental-3days.js
 * 
 * SEGURO: Siempre revierte al final. Emergency revert en caso de error.
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const serviceAccount = require('../key.json');
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

const { updateSnapshotIncremental, computeReturnsFromTimeline, SCHEMA_VERSION_INCREMENTAL } = require('../services/snapshotIncrementalService');
const { buildSnapshotDocId, fetchAllDailyDocs, generatePerformanceSnapshot } = require('../services/snapshotGenerator');

// ============================================================================
// CONFIG
// ============================================================================
const TEST_CURRENCY = 'USD';
const ACCURACY_THRESHOLD_PCT = 0.01; // Max 0.01% difference allowed

// ============================================================================
// HELPERS
// ============================================================================

function printSection(title) {
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  ${title}`);
  console.log('═'.repeat(70));
}

function printStats(label, data) {
  console.log(`\n  📊 ${label}:`);
  for (const [key, value] of Object.entries(data)) {
    const display = typeof value === 'object' ? JSON.stringify(value, null, 0) : value;
    console.log(`     ${key}: ${display}`);
  }
}

function compareReturns(incremental, fullRebuild, label) {
  const diffs = [];
  const periods = ['ytd', 'oneMonth', 'threeMonth', 'sixMonth', 'oneYear', 'twoYear', 'fiveYear'];
  
  for (const period of periods) {
    const incVal = incremental?.[period];
    const fullVal = fullRebuild?.[period];
    
    if (incVal === undefined && fullVal === undefined) continue;
    if (incVal === undefined || fullVal === undefined) {
      diffs.push({ period, incremental: incVal, fullRebuild: fullVal, diff: 'MISSING' });
      continue;
    }
    
    const diff = Math.abs(incVal - fullVal);
    const relDiff = fullVal !== 0 ? (diff / Math.abs(fullVal)) * 100 : (diff === 0 ? 0 : 100);
    
    diffs.push({
      period,
      incremental: typeof incVal === 'number' ? incVal.toFixed(6) : incVal,
      fullRebuild: typeof fullVal === 'number' ? fullVal.toFixed(6) : fullVal,
      absDiff: diff.toFixed(8),
      relDiffPct: relDiff.toFixed(6),
      pass: relDiff <= ACCURACY_THRESHOLD_PCT || diff < 0.0001,
    });
  }
  
  return diffs;
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  let originalSnapshot = null;
  let snapshotDocId = null;
  let stats = { reads: 0, writes: 0, scenarios: [] };

  printSection('OPT-SNAP-INCR: Simulación 3 Días + Accuracy Validation');

  try {
    // ── Step 1: Identify user ──
    console.log('\n  🔍 Auto-detecting user...');
    const perfSnap = await db.collection('portfolioPerformance').limit(1).get();
    stats.reads++;
    const userId = perfSnap.docs[0].id;
    console.log(`  👤 User: ${userId}`);

    // ── Step 2: Fetch daily docs (the source of truth) ──
    console.log('  📚 Fetching all daily docs...');
    const dailyDocs = await fetchAllDailyDocs(db, userId, 'overall');
    stats.reads += dailyDocs.length;
    console.log(`     Daily docs: ${dailyDocs.length}`);

    // Find the last 4 daily docs with USD data (we need 3 "days" to simulate + 1 baseline)
    const docsWithData = dailyDocs.filter(doc => {
      const data = doc.data();
      return data[TEST_CURRENCY] && data[TEST_CURRENCY].totalValue !== undefined;
    });

    if (docsWithData.length < 4) {
      console.error(`  ❌ Need at least 4 daily docs with ${TEST_CURRENCY} data, found ${docsWithData.length}`);
      process.exit(1);
    }

    // Take last 4: index 0 = baseline, index 1-3 = the 3 simulated days
    const lastFour = docsWithData.slice(-4);
    const baselineDate = lastFour[0].data().date;
    const day1 = lastFour[1].data();
    const day2 = lastFour[2].data();
    const day3 = lastFour[3].data();

    console.log(`     Baseline date: ${baselineDate}`);
    console.log(`     Day 1: ${day1.date}`);
    console.log(`     Day 2: ${day2.date}`);
    console.log(`     Day 3: ${day3.date}`);

    // ── Step 3: Read current snapshot and save for revert ──
    snapshotDocId = buildSnapshotDocId(userId, 'overall', TEST_CURRENCY);
    const existingDoc = await db.doc(`performanceSnapshots/${snapshotDocId}`).get();
    stats.reads++;

    if (!existingDoc.exists) {
      console.error('  ❌ Snapshot does not exist. Run the EOD pipeline first.');
      process.exit(1);
    }
    originalSnapshot = existingDoc.data();
    console.log(`\n  💾 Original snapshot saved for revert (timeline: ${originalSnapshot.timeline?.length} points)`);

    // ── Step 4: Build a "baseline" snapshot (state as of baselineDate) ──
    // We simulate the snapshot state after baselineDate by using timeline up to that point
    printSection('PHASE 1: Build Baseline Snapshot (state before 3 days)');

    const baselineTimeline = (originalSnapshot.timeline || []).filter(p => p.d <= baselineDate);
    console.log(`  📐 Baseline timeline: ${baselineTimeline.length} points (up to ${baselineDate})`);

    const baselineNow = DateTime.fromISO(baselineDate, { zone: 'America/New_York' });
    const baselineComputed = computeReturnsFromTimeline(baselineTimeline, baselineNow);

    // Build a minimal baseline snapshot to start from
    const baselineSnapshot = {
      userId,
      currency: TEST_CURRENCY,
      accountId: 'overall',
      timeline: baselineTimeline,
      returns: baselineComputed?.returns || {},
      performanceByYear: baselineComputed?.performanceByYear || originalSnapshot.performanceByYear || {},
      monthlyCompound: buildBaselineMonthlyCompound(originalSnapshot.monthlyCompound || {}, baselineDate),
      validDocsCountByPeriod: baselineComputed?.validDocsCountByPeriod || {},
      availableYears: baselineComputed?.availableYears || [],
      startDate: baselineComputed?.startDate || originalSnapshot.startDate,
      latestAssetPerformance: {},
      lastUpdated: new Date().toISOString(),
      lastDateInTimeline: baselineDate,
      schemaVersion: SCHEMA_VERSION_INCREMENTAL,
    };

    // Write baseline as starting point
    await db.doc(`performanceSnapshots/${snapshotDocId}`).set(baselineSnapshot);
    stats.writes++;
    console.log(`  ✅ Baseline snapshot written (lastDate: ${baselineDate})`);

    // ── Step 5: Simulate 3 consecutive days of incremental updates ──
    printSection('PHASE 2: Simulate 3 Days Incremental');

    const days = [day1, day2, day3];
    let lastResult = null;

    for (let i = 0; i < days.length; i++) {
      const dayData = days[i];
      const currData = dayData[TEST_CURRENCY];
      const dayNow = DateTime.fromISO(dayData.date, { zone: 'America/New_York' });

      const dailyInput = {
        date: dayData.date,
        totalValue: currData.totalValue ?? 0,
        totalInvestment: currData.totalInvestment ?? 0,
        adjustedDailyChangePercentage: currData.adjustedDailyChangePercentage ?? currData.dailyChangePercentage ?? 0,
        dailyChangePercentage: currData.dailyChangePercentage ?? 0,
        totalCashFlow: currData.totalCashFlow ?? 0,
        doneProfitAndLoss: currData.doneProfitAndLoss ?? 0,
        unrealizedProfitAndLoss: currData.unrealizedProfitAndLoss ?? 0,
        assetPerformance: currData.assetPerformance || {},
      };

      const startMs = Date.now();
      const result = await updateSnapshotIncremental(db, snapshotDocId, dailyInput, { now: dayNow });
      const durationMs = Date.now() - startMs;
      stats.reads++; // updateSnapshotIncremental does 1 read
      if (result.method === 'incremental') stats.writes++;

      console.log(`\n  📅 Day ${i + 1} (${dayData.date}):`);
      console.log(`     method: ${result.method} | updated: ${result.updated} | ${durationMs}ms`);
      console.log(`     change%: ${dailyInput.adjustedDailyChangePercentage.toFixed(4)} | value: ${dailyInput.totalValue.toFixed(2)}`);

      stats.scenarios.push({ day: i + 1, date: dayData.date, method: result.method, durationMs });
      lastResult = result;
    }

    // ── Step 6: Read final incremental snapshot ──
    printSection('PHASE 3: Accuracy Comparison (Incremental vs Full-Rebuild)');

    const incrementalDoc = await db.doc(`performanceSnapshots/${snapshotDocId}`).get();
    stats.reads++;
    const incrementalSnapshot = incrementalDoc.data();

    console.log(`\n  📊 Incremental result:`);
    console.log(`     timeline: ${incrementalSnapshot.timeline.length} points`);
    console.log(`     lastDate: ${incrementalSnapshot.lastDateInTimeline}`);

    // ── Step 7: Generate full-rebuild for same date range ──
    // Use computeReturnsFromTimeline on the SAME timeline that full-rebuild would produce
    // (from dailyDocs up to day3.date)
    const fullRebuildDocs = docsWithData.filter(d => d.data().date <= day3.date);
    const fullRebuildTimeline = fullRebuildDocs.map(doc => {
      const data = doc.data();
      const curr = data[TEST_CURRENCY];
      return {
        d: data.date,
        v: curr?.totalValue ?? 0,
        c: curr?.adjustedDailyChangePercentage ?? curr?.dailyChangePercentage ?? 0,
      };
    });

    const fullRebuildNow = DateTime.fromISO(day3.date, { zone: 'America/New_York' });
    const fullRebuildComputed = computeReturnsFromTimeline(fullRebuildTimeline, fullRebuildNow);

    console.log(`\n  📊 Full-rebuild reference:`);
    console.log(`     timeline: ${fullRebuildTimeline.length} points`);
    console.log(`     last date: ${fullRebuildTimeline[fullRebuildTimeline.length - 1]?.d}`);

    // ── Step 8: Compare returns ──
    printSection('ACCURACY RESULTS');

    const diffs = compareReturns(
      incrementalSnapshot.returns,
      fullRebuildComputed?.returns || {},
      'Returns comparison'
    );

    let allPass = true;
    console.log('\n  Period           | Incremental     | Full-Rebuild    | Diff        | Status');
    console.log('  ' + '-'.repeat(85));

    for (const d of diffs) {
      const status = d.pass ? '✅' : '❌';
      if (!d.pass) allPass = false;
      console.log(`  ${d.period.padEnd(16)} | ${String(d.incremental).padEnd(15)} | ${String(d.fullRebuild).padEnd(15)} | ${String(d.absDiff).padEnd(11)} | ${status}`);
    }

    // Compare timeline lengths
    const timelineLenMatch = incrementalSnapshot.timeline.length === fullRebuildTimeline.length;
    console.log(`\n  Timeline length match: ${timelineLenMatch ? '✅' : '❌'} (incremental: ${incrementalSnapshot.timeline.length}, full: ${fullRebuildTimeline.length})`);

    // Compare last 3 timeline points
    console.log('\n  Last 3 timeline points comparison:');
    for (let i = 1; i <= 3; i++) {
      const incP = incrementalSnapshot.timeline[incrementalSnapshot.timeline.length - i];
      const fullP = fullRebuildTimeline[fullRebuildTimeline.length - i];
      const match = incP?.d === fullP?.d && Math.abs((incP?.v || 0) - (fullP?.v || 0)) < 0.01 && Math.abs((incP?.c || 0) - (fullP?.c || 0)) < 0.0001;
      console.log(`     [-${i}] inc: {d:${incP?.d}, v:${incP?.v?.toFixed(2)}, c:${incP?.c?.toFixed(4)}} | full: {d:${fullP?.d}, v:${fullP?.v?.toFixed(2)}, c:${fullP?.c?.toFixed(4)}} ${match ? '✅' : '❌'}`);
      if (!match) allPass = false;
    }

    // ── Step 9: REVERT ──
    printSection('REVERT');
    console.log('  ⏪ Restoring original snapshot...');
    await db.doc(`performanceSnapshots/${snapshotDocId}`).set(originalSnapshot);
    stats.writes++;

    const revertVerify = await db.doc(`performanceSnapshots/${snapshotDocId}`).get();
    stats.reads++;
    const reverted = revertVerify.data();
    const revertOk = reverted.lastDateInTimeline === originalSnapshot.lastDateInTimeline
                  && reverted.timeline?.length === originalSnapshot.timeline?.length;

    console.log(`  ${revertOk ? '✅' : '❌'} Revert ${revertOk ? 'successful' : 'FAILED'}`);

    // ── Final summary ──
    printSection('FINAL SUMMARY');

    printStats('3-Day Simulation', {
      day1: `${stats.scenarios[0]?.date} → ${stats.scenarios[0]?.method} (${stats.scenarios[0]?.durationMs}ms)`,
      day2: `${stats.scenarios[1]?.date} → ${stats.scenarios[1]?.method} (${stats.scenarios[1]?.durationMs}ms)`,
      day3: `${stats.scenarios[2]?.date} → ${stats.scenarios[2]?.method} (${stats.scenarios[2]?.durationMs}ms)`,
    });

    printStats('I/O Total (test overhead included)', {
      reads: stats.reads,
      writes: stats.writes,
    });

    printStats('Production EOD I/O (per user, per snapshot)', {
      readsPerDay: 1,
      writesPerDay: 1,
      totalFor3Days: '3 reads + 3 writes',
    });

    const verdict = allPass && timelineLenMatch;
    console.log(`\n  ${'═'.repeat(50)}`);
    console.log(`  ${verdict ? '✅ PASS' : '❌ FAIL'}: Incremental snapshot ${verdict ? 'is numerically equivalent to' : 'DIFFERS from'} full-rebuild`);
    console.log(`  ${verdict ? '✅' : '❌'} Fase 1 accuracy validated for 3 consecutive days`);
    console.log(`  ${'═'.repeat(50)}\n`);

    process.exit(verdict ? 0 : 1);

  } catch (error) {
    console.error(`\n  ❌ ERROR: ${error.message}`);
    console.error(error.stack);

    if (originalSnapshot && snapshotDocId) {
      console.log('\n  🚨 Emergency revert...');
      try {
        await db.doc(`performanceSnapshots/${snapshotDocId}`).set(originalSnapshot);
        console.log('  ✅ Emergency revert successful.');
      } catch (e) {
        console.error(`  ❌ EMERGENCY REVERT FAILED: ${e.message}`);
      }
    }
    process.exit(1);
  }
}

// ============================================================================
// HELPER: Build baseline monthlyCompound (keep only months before baselineDate)
// ============================================================================

function buildBaselineMonthlyCompound(monthlyCompound, baselineDate) {
  const baseMonth = baselineDate.substring(0, 7); // "2026-04"
  const result = {};

  for (const [year, months] of Object.entries(monthlyCompound)) {
    result[year] = {};
    for (const [month, data] of Object.entries(months)) {
      const monthKey = `${year}-${month.padStart(2, '0')}`;
      if (monthKey <= baseMonth) {
        result[year][month] = { ...data };
      }
    }
    if (Object.keys(result[year]).length === 0) delete result[year];
  }

  return result;
}

// ============================================================================
main();
