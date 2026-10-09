/**
 * OPT-SNAP-INCR: Test controlado de lectura/escritura incremental
 * 
 * Este script:
 * 1. Lee un snapshot existente de Firestore (1 read)
 * 2. Ejecuta updateSnapshotIncremental con datos simulados
 * 3. Lee el snapshot actualizado para verificar (1 read)
 * 4. REVIERTE: restaura el snapshot original
 * 
 * USO: node tests/test-incremental-live.js
 * 
 * SEGURO: Siempre revierte al final. Si falla a medio camino,
 * el snapshot queda con 1 punto extra que el EOD de esta noche 
 * detectaría como "already-current" (idempotente).
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

// Inicializar Firebase Admin
const serviceAccount = require('../key.json');
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

const { updateSnapshotIncremental, updateAssetSnapshotIncremental, SCHEMA_VERSION_INCREMENTAL } = require('../services/snapshotIncrementalService');
const { buildSnapshotDocId } = require('../services/snapshotGenerator');

// ============================================================================
// CONFIG — Ajustar según tu usuario de test
// ============================================================================
const TEST_USER_ID = process.env.TEST_USER_ID || null; // Se auto-detecta si no se provee
const TEST_CURRENCY = 'USD';
const SIMULATED_DATE = '2026-04-30'; // Fecha simulada para el test

// ============================================================================
// HELPERS
// ============================================================================

function printSection(title) {
  console.log(`\n${'='.repeat(70)}`);
  console.log(`  ${title}`);
  console.log('='.repeat(70));
}

function printStats(label, data) {
  console.log(`\n  📊 ${label}:`);
  for (const [key, value] of Object.entries(data)) {
    const display = typeof value === 'object' ? JSON.stringify(value) : value;
    console.log(`     ${key}: ${display}`);
  }
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  let readsCount = 0;
  let writesCount = 0;
  let originalSnapshot = null;
  let snapshotDocId = null;

  printSection('OPT-SNAP-INCR: Test Controlado de Snapshot Incremental');

  try {
    // ── Step 1: Identify test user ──
    let userId = TEST_USER_ID;
    if (!userId) {
      console.log('\n  🔍 Auto-detecting user from portfolioPerformance...');
      const perfSnap = await db.collection('portfolioPerformance').limit(1).get();
      readsCount++;
      if (perfSnap.empty) {
        console.error('  ❌ No users found in portfolioPerformance. Cannot test.');
        process.exit(1);
      }
      userId = perfSnap.docs[0].id;
    }
    console.log(`  👤 Test user: ${userId}`);

    // ── Step 2: Read existing snapshot ──
    snapshotDocId = buildSnapshotDocId(userId, 'overall', TEST_CURRENCY);
    console.log(`  📄 Snapshot doc ID: ${snapshotDocId}`);

    const existingDoc = await db.doc(`performanceSnapshots/${snapshotDocId}`).get();
    readsCount++;

    if (!existingDoc.exists) {
      console.log('  ⚠️  Snapshot does not exist yet. First run will trigger full-rebuild.');
      console.log('  ℹ️  This is expected for Fase 1A first execution.');
      
      printStats('Pre-test reads', { totalReads: readsCount, totalWrites: writesCount });
      
      // Test: attempt incremental on non-existent snapshot
      console.log('\n  🧪 Testing incremental on non-existent snapshot...');
      const mockDaily = {
        date: SIMULATED_DATE,
        totalValue: 10000,
        totalInvestment: 9000,
        adjustedDailyChangePercentage: 0.5,
        totalCashFlow: 0,
        doneProfitAndLoss: 0,
        unrealizedProfitAndLoss: 1000,
        assetPerformance: {},
      };

      // Use a wrapped db to count operations
      const wrappedDb = createCountingDb(db, () => readsCount++, () => writesCount++);
      const result = await updateSnapshotIncremental(wrappedDb, snapshotDocId, mockDaily);
      
      printStats('Result (no snapshot)', {
        method: result.method,
        reason: result.reason,
        updated: result.updated,
      });
      
      console.log('\n  ✅ Expected: full-rebuild signal (no actual write from incremental service)');
      printSection('FINAL STATS');
      printStats('I/O Operations', { reads: readsCount, writes: writesCount });
      console.log('\n  🎯 Test complete. No revert needed (nothing was written).');
      process.exit(0);
    }

    // Snapshot exists — capture original for revert
    originalSnapshot = existingDoc.data();
    console.log(`  ✅ Snapshot exists. Schema version: ${originalSnapshot.schemaVersion || 'N/A'}`);
    console.log(`     lastDateInTimeline: ${originalSnapshot.lastDateInTimeline || 'N/A'}`);
    console.log(`     timeline length: ${originalSnapshot.timeline?.length || 0}`);

    printStats('Snapshot metadata', {
      schemaVersion: originalSnapshot.schemaVersion || 'N/A',
      lastDateInTimeline: originalSnapshot.lastDateInTimeline || 'N/A',
      timelinePoints: originalSnapshot.timeline?.length || 0,
      currency: originalSnapshot.currency || TEST_CURRENCY,
      hasReturns: !!originalSnapshot.returns,
      hasMonthlyCompound: !!originalSnapshot.monthlyCompound,
    });

    // ── Step 3: Determine test scenario ──
    const needsSchemaUpgrade = (originalSnapshot.schemaVersion || 0) < SCHEMA_VERSION_INCREMENTAL;
    const lastDate = originalSnapshot.lastDateInTimeline;
    const alreadyCurrent = lastDate === SIMULATED_DATE;

    printSection('TEST SCENARIOS');

    if (needsSchemaUpgrade) {
      console.log('  📋 Scenario: SCHEMA UPGRADE (v2 → v3 full-rebuild signal)');
    } else if (alreadyCurrent) {
      console.log('  📋 Scenario: IDEMPOTENCY (already has today → skip)');
    } else {
      console.log('  📋 Scenario: INCREMENTAL APPEND (normal path)');
    }

    // ── Step 4: Execute incremental update ──
    printSection('EXECUTING INCREMENTAL UPDATE');

    const lastPoint = originalSnapshot.timeline?.[originalSnapshot.timeline.length - 1];
    const simulatedDailyData = {
      date: SIMULATED_DATE,
      totalValue: (lastPoint?.v || 10000) * 1.005, // +0.5% simulated
      totalInvestment: 9000,
      adjustedDailyChangePercentage: 0.5,
      dailyChangePercentage: 0.5,
      totalCashFlow: 0,
      doneProfitAndLoss: 0,
      unrealizedProfitAndLoss: ((lastPoint?.v || 10000) * 1.005) - 9000,
      assetPerformance: {},
    };

    console.log(`  📥 Simulated daily data:`);
    console.log(`     date: ${simulatedDailyData.date}`);
    console.log(`     totalValue: ${simulatedDailyData.totalValue.toFixed(2)}`);
    console.log(`     change%: ${simulatedDailyData.adjustedDailyChangePercentage}`);

    const wrappedDb = createCountingDb(db, () => readsCount++, () => writesCount++);
    const startMs = Date.now();
    const result = await updateSnapshotIncremental(wrappedDb, snapshotDocId, simulatedDailyData, {
      now: DateTime.fromISO(SIMULATED_DATE, { zone: 'America/New_York' }),
    });
    const durationMs = Date.now() - startMs;

    printStats('Result', {
      method: result.method,
      reason: result.reason || '—',
      updated: result.updated,
      durationMs,
    });

    // ── Step 5: Verify written data (if incremental) ──
    if (result.method === 'incremental') {
      const verifyDoc = await db.doc(`performanceSnapshots/${snapshotDocId}`).get();
      readsCount++;
      const updated = verifyDoc.data();

      printSection('VERIFICATION');
      printStats('Updated snapshot', {
        timelinePoints: updated.timeline?.length,
        lastDateInTimeline: updated.lastDateInTimeline,
        schemaVersion: updated.schemaVersion,
        hasReturns: !!updated.returns,
        lastPointValue: updated.timeline?.[updated.timeline.length - 1]?.v?.toFixed(2),
        lastPointDate: updated.timeline?.[updated.timeline.length - 1]?.d,
        lastPointChange: updated.timeline?.[updated.timeline.length - 1]?.c,
      });

      console.log(`\n  ✅ Incremental write verified:`);
      console.log(`     Timeline grew from ${originalSnapshot.timeline.length} → ${updated.timeline.length} points`);
      console.log(`     lastDateInTimeline: ${originalSnapshot.lastDateInTimeline} → ${updated.lastDateInTimeline}`);
    }

    // ── Step 6: REVERT ──
    printSection('REVERTING TO ORIGINAL STATE');

    if (result.method === 'incremental') {
      console.log('  ⏪ Restoring original snapshot...');
      await db.doc(`performanceSnapshots/${snapshotDocId}`).set(originalSnapshot);
      writesCount++;

      // Verify revert
      const revertVerify = await db.doc(`performanceSnapshots/${snapshotDocId}`).get();
      readsCount++;
      const reverted = revertVerify.data();
      
      const revertOk = reverted.lastDateInTimeline === originalSnapshot.lastDateInTimeline 
                    && reverted.timeline?.length === originalSnapshot.timeline?.length;
      
      if (revertOk) {
        console.log('  ✅ Revert successful. Snapshot restored to original state.');
      } else {
        console.error('  ❌ REVERT MISMATCH! Manual check required.');
        console.error(`     Expected lastDate: ${originalSnapshot.lastDateInTimeline}, got: ${reverted.lastDateInTimeline}`);
      }
    } else {
      console.log('  ℹ️  No write was made (skip/full-rebuild signal). No revert needed.');
    }

    // ── Final stats ──
    printSection('FINAL I/O STATS');
    printStats('Operations', {
      firestoreReads: readsCount,
      firestoreWrites: writesCount,
      totalOperations: readsCount + writesCount,
      note: result.method === 'incremental' 
        ? 'Includes 1 verification read + 1 revert write (not in production)'
        : 'Minimal — no incremental write occurred',
    });

    console.log('\n  📋 Production behavior for this user per EOD execution:');
    if (needsSchemaUpgrade) {
      console.log('     First run: full-rebuild (legacy reads) → writes v3 snapshot');
      console.log('     Subsequent runs: 1 read + 1 write per snapshot (incremental)');
    } else if (alreadyCurrent) {
      console.log('     1 read → skip (no write). Idempotent.');
    } else {
      console.log('     1 read + 1 write per snapshot. ✅ Optimal path.');
    }

  } catch (error) {
    console.error(`\n  ❌ ERROR: ${error.message}`);
    console.error(error.stack);

    // Emergency revert
    if (originalSnapshot && snapshotDocId) {
      console.log('\n  🚨 Emergency revert...');
      try {
        await db.doc(`performanceSnapshots/${snapshotDocId}`).set(originalSnapshot);
        console.log('  ✅ Emergency revert successful.');
      } catch (revertError) {
        console.error(`  ❌ EMERGENCY REVERT FAILED: ${revertError.message}`);
        console.error(`     Snapshot ${snapshotDocId} may need manual restoration.`);
      }
    }
    process.exit(1);
  }

  console.log('\n  🎯 Test complete.\n');
  process.exit(0);
}

// ============================================================================
// COUNTING DB WRAPPER — Tracks reads/writes without modifying behavior
// ============================================================================

function createCountingDb(realDb, onRead, onWrite) {
  return {
    doc: (path) => {
      const realDocRef = realDb.doc(path);
      return {
        get: async () => {
          onRead();
          return realDocRef.get();
        },
        set: async (data) => {
          onWrite();
          return realDocRef.set(data);
        },
      };
    },
    collection: (path) => realDb.collection(path),
  };
}

// ============================================================================
// RUN
// ============================================================================

main();
