/**
 * OPT-SNAP-INCR Fase 3: Smoke Test
 *
 * Validates that the consolidated pipeline steps can be imported and executed
 * without runtime errors. Does NOT write to production Firestore.
 *
 * Usage: node tests/test-phase3-smoke.js
 *
 * What it tests:
 * 1. Import chain resolves without errors (no circular deps, no missing modules)
 * 2. saveIndicesHistoryDataInternal signature and return contract
 * 3. refreshIndexCacheInternal signature and return contract
 * 4. Timeline windowing functions work end-to-end
 * 5. The combined pipeline steps fit within timeout budget
 */

// ============================================================================
// Setup: Mock firebase-admin to avoid needing real credentials
// ============================================================================

const mockDocs = [];
const mockBatchOps = [];

const mockBatch = {
  set: (...args) => { mockBatchOps.push({ op: 'set', args }); },
  delete: (...args) => { mockBatchOps.push({ op: 'delete', args }); },
  commit: () => Promise.resolve(),
};

const mockDocRef = (path) => ({
  get: () => Promise.resolve({ exists: false, data: () => null }),
  set: (data) => { mockDocs.push({ path, data }); return Promise.resolve(); },
  collection: (name) => mockCollectionRef(`${path}/${name}`),
});

const mockCollectionRef = (path) => ({
  doc: (id) => mockDocRef(`${path}/${id}`),
  where: () => ({
    where: () => ({
      orderBy: () => ({
        get: () => Promise.resolve({ docs: [] }),
      }),
    }),
    orderBy: () => ({
      get: () => Promise.resolve({ docs: [] }),
    }),
    get: () => Promise.resolve({ docs: [] }),
  }),
  get: () => Promise.resolve({ docs: [], empty: true, size: 0 }),
});

const mockDb = {
  collection: (name) => mockCollectionRef(name),
  doc: (path) => mockDocRef(path),
  batch: () => mockBatch,
};

// Intercept firebase-admin before any service module loads it
const Module = require('module');
const originalRequire = Module.prototype.require;
Module.prototype.require = function(id) {
  if (id === 'firebase-admin' || id === './firebaseAdmin') {
    return {
      apps: [{ name: 'mock' }],
      firestore: Object.assign(() => mockDb, {
        FieldValue: {
          serverTimestamp: () => 'MOCK_TIMESTAMP',
          increment: (n) => n,
        },
      }),
      initializeApp: () => {},
    };
  }
  if (id === 'firebase-admin/firestore') {
    return {
      getFirestore: () => mockDb,
      FieldValue: { serverTimestamp: () => 'MOCK_TIMESTAMP' },
    };
  }
  if (id === 'firebase-functions/v2/scheduler') {
    return { onSchedule: (opts, handler) => handler };
  }
  if (id === 'firebase-functions/v2/https') {
    return { onCall: (opts, handler) => handler, HttpsError: Error };
  }
  if (id === 'firebase-functions/params') {
    return { defineSecret: () => ({ value: () => 'mock-token' }) };
  }
  return originalRequire.apply(this, arguments);
};

// Mock axios for requestIndicesFromFinance
const axios = require('axios');
const originalAxiosGet = axios.get;
axios.get = async (url, opts) => {
  if (url.includes('/indices')) {
    return {
      data: [
        { code: 'GSPC', name: 'S&P 500', region: 'US', value: 5800.50, change: '+25.30', percentChange: '+0.44%' },
        { code: 'DJI', name: 'Dow Jones', region: 'US', value: 42500.00, change: '-50.00', percentChange: '-0.12%' },
      ]
    };
  }
  if (url.includes('/quotes')) {
    return {
      data: [{ symbol: '^GSPC', price: '5800.50', percentChange: '+0.44%' }]
    };
  }
  return originalAxiosGet(url, opts);
};

// Mock global fetch for _fetchIntradayPoint
global.fetch = async (url, opts) => ({
  ok: true,
  json: async () => [{ symbol: '^GSPC', price: '5800.50', percentChange: '+0.44%' }],
});

// ============================================================================
// Tests
// ============================================================================

async function runSmokeTests() {
  const startTime = Date.now();
  let passed = 0;
  let failed = 0;

  function assert(condition, label) {
    if (condition) {
      console.log(`  ✅ ${label}`);
      passed++;
    } else {
      console.error(`  ❌ ${label}`);
      failed++;
    }
  }

  console.log('\n═══════════════════════════════════════════════════════');
  console.log('  OPT-SNAP-INCR Fase 3 — Smoke Test');
  console.log('═══════════════════════════════════════════════════════\n');

  // ── Test 1: Import chain ──
  console.log('▸ Test 1: Import chain resolves');
  let saveIndicesHistoryDataInternal, refreshIndexCacheInternal;
  let compressToMonthly, applyTimelineWindowing, MAX_DAILY_POINTS;

  try {
    const mds = require('../services/marketDataScheduled');
    saveIndicesHistoryDataInternal = mds.saveIndicesHistoryDataInternal;
    assert(typeof saveIndicesHistoryDataInternal === 'function', 'saveIndicesHistoryDataInternal is a function');
  } catch (err) {
    assert(false, `Import marketDataScheduled failed: ${err.message}`);
  }

  try {
    const ihs = require('../services/indexHistoryService');
    refreshIndexCacheInternal = ihs.refreshIndexCacheInternal;
    assert(typeof refreshIndexCacheInternal === 'function', 'refreshIndexCacheInternal is a function');
  } catch (err) {
    assert(false, `Import indexHistoryService failed: ${err.message}`);
  }

  try {
    const sis = require('../services/snapshotIncrementalService');
    compressToMonthly = sis.compressToMonthly;
    applyTimelineWindowing = sis.applyTimelineWindowing;
    MAX_DAILY_POINTS = sis.MAX_DAILY_POINTS;
    assert(typeof compressToMonthly === 'function', 'compressToMonthly is a function');
    assert(typeof applyTimelineWindowing === 'function', 'applyTimelineWindowing is a function');
    assert(MAX_DAILY_POINTS === 1260, 'MAX_DAILY_POINTS = 1260');
  } catch (err) {
    assert(false, `Import snapshotIncrementalService failed: ${err.message}`);
  }

  // ── Test 2: saveIndicesHistoryDataInternal execution ──
  console.log('\n▸ Test 2: saveIndicesHistoryDataInternal execution');
  try {
    const t0 = Date.now();
    const result = await saveIndicesHistoryDataInternal({ formattedDate: '2026-04-29', skipCacheInvalidation: true });
    const elapsed = Date.now() - t0;

    assert(result !== null && result !== undefined, 'Returns non-null result');
    assert(typeof result.success === 'boolean', 'result.success is boolean');
    assert(typeof result.count === 'number', 'result.count is number');
    assert(typeof result.durationMs === 'number', 'result.durationMs is number');
    assert(result.count === 2, `Saved 2 indices (got ${result.count})`);
    assert(elapsed < 5000, `Completed in ${elapsed}ms (< 5s budget)`);
    console.log(`    → Duration: ${elapsed}ms, count: ${result.count}`);
  } catch (err) {
    assert(false, `Execution failed: ${err.message}`);
    console.error('    Stack:', err.stack?.split('\n').slice(0, 3).join('\n'));
  }

  // ── Test 3: saveIndicesHistoryDataInternal with skipCacheInvalidation=false ──
  console.log('\n▸ Test 3: saveIndicesHistoryDataInternal (with cache invalidation)');
  try {
    const result = await saveIndicesHistoryDataInternal({ formattedDate: '2026-04-29', skipCacheInvalidation: false });
    assert(result.success === true, 'Succeeds with cache invalidation enabled');
  } catch (err) {
    assert(false, `Execution with invalidation failed: ${err.message}`);
  }

  // ── Test 4: refreshIndexCacheInternal execution ──
  console.log('\n▸ Test 4: refreshIndexCacheInternal execution');
  try {
    const t0 = Date.now();
    const result = await refreshIndexCacheInternal();
    const elapsed = Date.now() - t0;

    assert(result !== null && result !== undefined, 'Returns non-null result');
    assert(typeof result.success === 'boolean', 'result.success is boolean');
    assert(typeof result.refreshed === 'number', 'result.refreshed is number');
    assert(typeof result.errors === 'number', 'result.errors is number');
    assert(typeof result.duration === 'number', 'result.duration is number');
    assert(elapsed < 10000, `Completed in ${elapsed}ms (< 10s budget)`);
    console.log(`    → Duration: ${elapsed}ms, refreshed: ${result.refreshed}, errors: ${result.errors}`);
  } catch (err) {
    assert(false, `Execution failed: ${err.message}`);
    console.error('    Stack:', err.stack?.split('\n').slice(0, 3).join('\n'));
  }

  // ── Test 5: Timeline windowing end-to-end ──
  console.log('\n▸ Test 5: Timeline windowing end-to-end');
  try {
    // Simulate 6 years of data (1512 trading days)
    const timeline = [];
    for (let i = 0; i < 1512; i++) {
      const date = new Date(2020, 0, 2);
      date.setDate(date.getDate() + Math.floor(i * 365 / 252));
      timeline.push({
        d: date.toISOString().split('T')[0],
        v: 10000 + i * 5,
        c: ((Math.random() - 0.5) * 2).toFixed(4) * 1,
      });
    }

    const t0 = Date.now();
    const windowed = applyTimelineWindowing(timeline);
    const elapsed = Date.now() - t0;

    assert(windowed.length <= MAX_DAILY_POINTS + 20, `Windowed length ${windowed.length} <= ${MAX_DAILY_POINTS + 20}`);
    assert(windowed.length < timeline.length, `Compressed: ${timeline.length} → ${windowed.length}`);
    assert(elapsed < 100, `Windowing took ${elapsed}ms (< 100ms budget)`);

    // Verify last point is preserved
    const lastOriginal = timeline[timeline.length - 1];
    const lastWindowed = windowed[windowed.length - 1];
    assert(lastOriginal.d === lastWindowed.d, 'Last point date preserved');
    assert(lastOriginal.v === lastWindowed.v, 'Last point value preserved');

    console.log(`    → ${timeline.length} points → ${windowed.length} points in ${elapsed}ms`);
  } catch (err) {
    assert(false, `Windowing failed: ${err.message}`);
  }

  // ── Test 6: Combined pipeline timing estimate ──
  console.log('\n▸ Test 6: Pipeline timing budget');
  const totalElapsed = Date.now() - startTime;
  const budgetMs = 540000; // 9 minutes (function timeout)
  const estimatedPipelineOverhead = totalElapsed * 3; // 3x safety factor for real Firestore
  assert(estimatedPipelineOverhead < budgetMs, 
    `Estimated pipeline time ${estimatedPipelineOverhead}ms < ${budgetMs}ms timeout (3x safety factor)`);

  // ── Summary ──
  console.log('\n═══════════════════════════════════════════════════════');
  console.log(`  Results: ${passed} passed, ${failed} failed (${Date.now() - startTime}ms)`);
  console.log('═══════════════════════════════════════════════════════\n');

  process.exit(failed > 0 ? 1 : 0);
}

runSmokeTests().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
