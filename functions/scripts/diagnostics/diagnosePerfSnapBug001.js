/**
 * BUG-PERF-SNAP-001: Diagnóstico completo de rendimientos faltantes
 * 
 * Verifica:
 * 1. Estado de documentos consolidados (monthly/yearly)
 * 2. Conteo de documentos diarios por período temporal
 * 3. Estado del snapshot pre-computado
 * 4. Estado del performanceCache
 * 5. Resultado simulado de V2 vs V1
 * 
 * USO:
 *   node diagnosePerfSnapBug001.js
 *   node diagnosePerfSnapBug001.js --user=<userId>
 *   node diagnosePerfSnapBug001.js --currency=COP
 */

const admin = require('firebase-admin');
const path = require('path');
const { DateTime } = require('luxon');

// Inicializar Firebase Admin
const serviceAccount = require('../../key.json');
if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}
const db = admin.firestore();

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

const args = process.argv.slice(2);
const getArg = (name, defaultVal) => {
  const arg = args.find(a => a.startsWith(`--${name}=`));
  return arg ? arg.split('=')[1] : defaultVal;
};

const USER_ID = getArg('user', 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2');
const CURRENCY = getArg('currency', 'USD');
const ACCOUNT_ID = getArg('account', 'overall');

// Umbrales del frontend (PortfolioSummary.tsx)
const FRONTEND_THRESHOLDS = {
  oneMonth: 5,
  threeMonths: 15,
  sixMonths: 30,
  ytd: 1,
  oneYear: 60,
};

// Umbrales del backend (periodCalculations.js MIN_DOCS)
const BACKEND_MIN_DOCS = {
  oneMonth: 5,
  threeMonths: 15,
  sixMonths: 30,
  ytd: 1,
  oneYear: 60,
  twoYears: 120,
  fiveYears: 300
};

// ============================================================================
// HELPERS
// ============================================================================

function separator(title) {
  console.log(`\n${'='.repeat(70)}`);
  console.log(`  ${title}`);
  console.log('='.repeat(70));
}

function buildBasePath(userId, accountId) {
  if (accountId === 'overall' || !accountId) {
    return `portfolioPerformance/${userId}`;
  }
  return `portfolioPerformance/${userId}/accounts/${accountId}`;
}

// ============================================================================
// DIAGNÓSTICO 1: Documentos Consolidados (Monthly y Yearly)
// ============================================================================

async function diagnoseConsolidatedDocs() {
  separator('1. DOCUMENTOS CONSOLIDADOS (Monthly + Yearly)');
  const basePath = buildBasePath(USER_ID, ACCOUNT_ID);

  // Monthly
  console.log(`\n📅 Monthly: ${basePath}/consolidatedPeriods/monthly/periods`);
  const monthlySnapshot = await db.collection(`${basePath}/consolidatedPeriods/monthly/periods`)
    .orderBy('periodKey', 'asc')
    .get();

  if (monthlySnapshot.empty) {
    console.log('  ❌ NO EXISTEN documentos monthly consolidados');
  } else {
    console.log(`  ✅ ${monthlySnapshot.size} documentos monthly encontrados:\n`);
    console.log('  periodKey    | docsCount | startDate  | endDate    | USD.startFactor | USD.endFactor | USD.periodReturn | USD.validDocsCount');
    console.log('  ' + '-'.repeat(130));
    
    monthlySnapshot.docs.forEach(doc => {
      const data = doc.data();
      const usd = data[CURRENCY] || {};
      console.log(
        `  ${(data.periodKey || doc.id).padEnd(12)} | ` +
        `${String(data.docsCount ?? 'UNDEF').padEnd(9)} | ` +
        `${(data.startDate || 'N/A').padEnd(10)} | ` +
        `${(data.endDate || 'N/A').padEnd(10)} | ` +
        `${String(usd.startFactor?.toFixed(6) ?? 'N/A').padEnd(15)} | ` +
        `${String(usd.endFactor?.toFixed(6) ?? 'N/A').padEnd(13)} | ` +
        `${String(usd.periodReturn?.toFixed(4) ?? 'N/A').padEnd(16)} | ` +
        `${String(usd.validDocsCount ?? 'N/A')}`
      );
    });
  }

  // Yearly
  console.log(`\n📆 Yearly: ${basePath}/consolidatedPeriods/yearly/periods`);
  const yearlySnapshot = await db.collection(`${basePath}/consolidatedPeriods/yearly/periods`)
    .orderBy('periodKey', 'asc')
    .get();

  if (yearlySnapshot.empty) {
    console.log('  ❌ NO EXISTEN documentos yearly consolidados');
  } else {
    console.log(`  ✅ ${yearlySnapshot.size} documentos yearly encontrados:\n`);
    yearlySnapshot.docs.forEach(doc => {
      const data = doc.data();
      const usd = data[CURRENCY] || {};
      console.log(
        `  ${data.periodKey || doc.id}: docsCount=${data.docsCount ?? 'UNDEF'}, ` +
        `${CURRENCY}.endFactor=${usd.endFactor?.toFixed(6) ?? 'N/A'}, ` +
        `${CURRENCY}.periodReturn=${usd.periodReturn?.toFixed(4) ?? 'N/A'}%`
      );
    });
  }

  return { monthlyCount: monthlySnapshot.size, yearlyCount: yearlySnapshot.size };
}

// ============================================================================
// DIAGNÓSTICO 2: Documentos Diarios — Conteos por Período
// ============================================================================

async function diagnoseDailyDocs() {
  separator('2. DOCUMENTOS DIARIOS — Conteos por Período Temporal');
  const basePath = buildBasePath(USER_ID, ACCOUNT_ID);

  const now = DateTime.now().setZone('America/New_York');
  const boundaries = {
    fiveYears: now.minus({ years: 5 }).toISODate(),
    twoYears: now.minus({ years: 2 }).toISODate(),
    oneYear: now.minus({ years: 1 }).toISODate(),
    sixMonths: now.minus({ months: 6 }).toISODate(),
    threeMonths: now.minus({ months: 3 }).toISODate(),
    oneMonth: now.minus({ months: 1 }).toISODate(),
    ytd: now.startOf('year').toISODate(),
    currentMonth: `${now.toFormat('yyyy-MM')}-01`
  };

  console.log(`\n📊 Daily docs: ${basePath}/dates`);
  console.log(`  Fecha actual (ET): ${now.toISODate()}\n`);

  // Conteo total
  const allDocs = await db.collection(`${basePath}/dates`)
    .orderBy('date', 'asc')
    .get();

  if (allDocs.empty) {
    console.log('  ❌ NO EXISTEN documentos diarios');
    return { totalDailyDocs: 0, docsByPeriod: {} };
  }

  const firstDate = allDocs.docs[0].data().date;
  const lastDate = allDocs.docs[allDocs.docs.length - 1].data().date;
  console.log(`  Total docs: ${allDocs.size}`);
  console.log(`  Rango: ${firstDate} → ${lastDate}`);

  // Verificar que los docs tienen datos de la currency
  let docsWithCurrency = 0;
  let docsWithAdjustedChange = 0;
  allDocs.docs.forEach(doc => {
    const data = doc.data();
    if (data[CURRENCY]) {
      docsWithCurrency++;
      if (data[CURRENCY].adjustedDailyChangePercentage !== undefined) {
        docsWithAdjustedChange++;
      }
    }
  });
  console.log(`  Docs con ${CURRENCY}: ${docsWithCurrency}/${allDocs.size}`);
  console.log(`  Docs con adjustedDailyChangePercentage: ${docsWithAdjustedChange}/${allDocs.size}`);

  // Conteos por período
  const docsByPeriod = {};
  console.log('\n  Conteos por período (docs diarios con fecha >= boundary):');
  console.log('  ' + '-'.repeat(80));
  console.log('  Período      | Boundary      | Daily Docs | Frontend Umbral | ¿Visible?');
  console.log('  ' + '-'.repeat(80));

  for (const [period, boundary] of Object.entries(boundaries)) {
    const count = allDocs.docs.filter(doc => doc.data().date >= boundary).length;
    docsByPeriod[period] = count;

    const threshold = FRONTEND_THRESHOLDS[period];
    const visible = threshold !== undefined ? (count >= threshold ? '✅ Sí' : '❌ NO') : 'N/A';

    console.log(
      `  ${period.padEnd(14)} | ${boundary.padEnd(13)} | ${String(count).padEnd(10)} | ` +
      `${threshold !== undefined ? '≥' + String(threshold).padEnd(14) : 'N/A'.padEnd(15)} | ${visible}`
    );
  }

  return { totalDailyDocs: allDocs.size, docsByPeriod, firstDate, lastDate };
}

// ============================================================================
// DIAGNÓSTICO 3: Snapshot Pre-Computado
// ============================================================================

async function diagnoseSnapshot() {
  separator('3. SNAPSHOT PRE-COMPUTADO (performanceSnapshots/)');

  // Snapshot overall
  const snapshotId = ACCOUNT_ID === 'overall'
    ? `${USER_ID}_${CURRENCY}`
    : `${USER_ID}_${ACCOUNT_ID}_${CURRENCY}`;

  console.log(`\n🗂️  Snapshot ID: ${snapshotId}`);

  const snapDoc = await db.doc(`performanceSnapshots/${snapshotId}`).get();

  if (!snapDoc.exists) {
    console.log('  ❌ NO EXISTE snapshot');
    return { snapshotExists: false };
  }

  const snap = snapDoc.data();
  console.log(`  ✅ Snapshot encontrado`);
  console.log(`  lastUpdated: ${snap.lastUpdated}`);
  console.log(`  schemaVersion: ${snap.schemaVersion}`);
  console.log(`  accountId: ${snap.accountId}`);
  console.log(`  currency: ${snap.currency}`);
  console.log(`  timeline points: ${snap.timeline?.length || 0}`);
  console.log(`  availableYears: ${JSON.stringify(snap.availableYears)}`);
  console.log(`  startDate: ${snap.startDate}`);

  // validDocsCountByPeriod
  console.log('\n  📈 validDocsCountByPeriod (del snapshot):');
  const vdoc = snap.validDocsCountByPeriod || {};
  console.log('  ' + '-'.repeat(65));
  console.log('  Período      | Count en Snapshot | Frontend Umbral | ¿Visible?');
  console.log('  ' + '-'.repeat(65));

  const periodMap = {
    oneMonth: 'oneMonth',
    threeMonths: 'threeMonths',
    sixMonths: 'sixMonths',
    ytd: 'ytd',
    oneYear: 'oneYear',
    twoYears: 'twoYears',
    fiveYears: 'fiveYears',
  };

  for (const [period, key] of Object.entries(periodMap)) {
    const count = vdoc[key] ?? 'UNDEF';
    const threshold = FRONTEND_THRESHOLDS[period];
    const visible = threshold !== undefined && typeof count === 'number'
      ? (count >= threshold ? '✅ Sí' : '❌ NO')
      : 'N/A';

    console.log(
      `  ${period.padEnd(14)} | ${String(count).padEnd(17)} | ` +
      `${threshold !== undefined ? '≥' + String(threshold).padEnd(14) : 'N/A'.padEnd(15)} | ${visible}`
    );
  }

  // returns flags
  console.log('\n  🏳️  Returns has*Data flags:');
  const returns = snap.returns || {};
  const hasFlags = [
    'hasYtdData', 'hasOneMonthData', 'hasThreeMonthData',
    'hasSixMonthData', 'hasOneYearData', 'hasTwoYearData', 'hasFiveYearData'
  ];
  hasFlags.forEach(flag => {
    console.log(`    ${flag}: ${returns[flag] ?? 'UNDEF'}`);
  });

  // returns values
  console.log('\n  💰 Returns TWR values:');
  const returnFields = [
    'ytdReturn', 'oneMonthReturn', 'threeMonthReturn',
    'sixMonthReturn', 'oneYearReturn', 'twoYearReturn', 'fiveYearReturn'
  ];
  returnFields.forEach(field => {
    const val = returns[field];
    console.log(`    ${field}: ${val !== undefined ? val.toFixed(4) + '%' : 'UNDEF'}`);
  });

  // Timeline first/last
  if (snap.timeline && snap.timeline.length > 0) {
    const first = snap.timeline[0];
    const last = snap.timeline[snap.timeline.length - 1];
    console.log(`\n  📉 Timeline: ${first.d} → ${last.d} (${snap.timeline.length} points)`);
    console.log(`    Primer punto: date=${first.d}, value=${first.v?.toFixed(2)}, change=${first.c?.toFixed(4)}%`);
    console.log(`    Último punto: date=${last.d}, value=${last.v?.toFixed(2)}, change=${last.c?.toFixed(4)}%`);
  }

  // _metadata
  if (snap._metadata) {
    console.log(`\n  🏷️  Metadata del snapshot:`);
    console.log(`    version: ${snap._metadata.version || 'N/A'}`);
    console.log(`    docsRead: ${snap._metadata.docsRead || 'N/A'}`);
    console.log(`    yearlyDocs: ${snap._metadata.yearlyDocs || 'N/A'}`);
    console.log(`    monthlyDocs: ${snap._metadata.monthlyDocs || 'N/A'}`);
    console.log(`    dailyDocs: ${snap._metadata.dailyDocs || 'N/A'}`);
  }

  // latestAssetPerformance
  const lap = snap.latestAssetPerformance || {};
  const assetKeys = Object.keys(lap);
  console.log(`\n  📊 latestAssetPerformance: ${assetKeys.length} assets`);
  if (assetKeys.length > 0 && assetKeys.length <= 10) {
    assetKeys.forEach(key => {
      console.log(`    ${key}: totalValue=${lap[key].totalValue?.toFixed(2)}`);
    });
  }

  return { snapshotExists: true, snapshot: snap };
}

// ============================================================================
// DIAGNÓSTICO 4: Performance Cache (Firestore)
// ============================================================================

async function diagnosePerformanceCache() {
  separator('4. PERFORMANCE CACHE (userData/{userId}/performanceCache/)');

  const cachePath = `userData/${USER_ID}/performanceCache`;
  const cacheSnapshot = await db.collection(cachePath).get();

  if (cacheSnapshot.empty) {
    console.log('  ❌ NO EXISTEN documentos de cache');
    return { cacheCount: 0 };
  }

  console.log(`  ✅ ${cacheSnapshot.size} documentos de cache encontrados:\n`);
  console.log('  Cache Key                  | validUntil          | Expired? | validDocsCount (ytd/1M/3M/6M/1Y)');
  console.log('  ' + '-'.repeat(110));
  
  const now = new Date();

  cacheSnapshot.docs.forEach(doc => {
    const data = doc.data();
    const validUntil = data.validUntil ? new Date(data.validUntil) : null;
    const isExpired = validUntil ? validUntil < now : 'N/A';
    
    const cached = data.data || {};
    const vdocs = cached.validDocsCountByPeriod || {};
    
    console.log(
      `  ${doc.id.padEnd(28)} | ` +
      `${(data.validUntil || 'N/A').substring(0, 19).padEnd(19)} | ` +
      `${String(isExpired).padEnd(8)} | ` +
      `ytd=${vdocs.ytd ?? '?'}, 1M=${vdocs.oneMonth ?? '?'}, 3M=${vdocs.threeMonths ?? '?'}, 6M=${vdocs.sixMonths ?? '?'}, 1Y=${vdocs.oneYear ?? '?'}`
    );
  });

  return { cacheCount: cacheSnapshot.size };
}

// ============================================================================
// DIAGNÓSTICO 5: lastSnapshotUpdate signal (PERF-SNAP-023)
// ============================================================================

async function diagnoseSnapshotSignal() {
  separator('5. lastSnapshotUpdate SIGNAL (PERF-SNAP-023)');

  const userPerfDoc = await db.doc(`portfolioPerformance/${USER_ID}`).get();

  if (!userPerfDoc.exists) {
    console.log('  ❌ Documento portfolioPerformance/{userId} NO EXISTE');
    return { lastSnapshotUpdate: null };
  }

  const data = userPerfDoc.data();
  const lsu = data.lastSnapshotUpdate || null;
  console.log(`  lastSnapshotUpdate: ${lsu || 'NO DEFINIDO'}`);
  
  if (lsu) {
    const lsuDate = new Date(lsu);
    const ageMs = Date.now() - lsuDate.getTime();
    const ageHours = (ageMs / 3600000).toFixed(1);
    console.log(`  Antigüedad: ${ageHours} horas`);
  }

  return { lastSnapshotUpdate: lsu };
}

// ============================================================================
// DIAGNÓSTICO 6: Simulación V2 vs V1 docsCount
// ============================================================================

async function diagnoseV2VsV1DocsCount(consolidatedResult) {
  separator('6. SIMULACIÓN V2 vs V1 — validDocsCountByPeriod');

  const now = DateTime.now().setZone('America/New_York');
  const basePath = buildBasePath(USER_ID, ACCOUNT_ID);

  // Simular qué haría V2
  const twoYearsAgo = now.minus({ years: 2 });
  const fiveYearsAgo = now.minus({ years: 5 });
  const lastCompleteYearForYearly = (twoYearsAgo.year - 1).toString();
  const monthsStartKey = `${twoYearsAgo.year}-01`;
  const currentMonth = now.toFormat('yyyy-MM');

  console.log(`\n🔄 Ranges que V2 leería:`);
  console.log(`  Yearly: ${fiveYearsAgo.year} → ${lastCompleteYearForYearly}`);
  console.log(`  Monthly: ${monthsStartKey} → ${currentMonth} (exclusive)`);
  console.log(`  Daily: ${currentMonth}-01 → hoy\n`);

  const [yearlySnap, monthlySnap, dailySnap] = await Promise.all([
    db.collection(`${basePath}/consolidatedPeriods/yearly/periods`)
      .where('periodKey', '>=', fiveYearsAgo.year.toString())
      .where('periodKey', '<=', lastCompleteYearForYearly)
      .orderBy('periodKey', 'asc')
      .get(),
    db.collection(`${basePath}/consolidatedPeriods/monthly/periods`)
      .where('periodKey', '>=', monthsStartKey)
      .where('periodKey', '<', currentMonth)
      .orderBy('periodKey', 'asc')
      .get(),
    db.collection(`${basePath}/dates`)
      .where('date', '>=', `${currentMonth}-01`)
      .orderBy('date', 'asc')
      .get()
  ]);

  const totalV2Reads = yearlySnap.size + monthlySnap.size + dailySnap.size;
  const hasConsolidated = yearlySnap.size > 0 || monthlySnap.size > 0;

  console.log(`  V2 reads: yearly=${yearlySnap.size}, monthly=${monthlySnap.size}, daily=${dailySnap.size}, total=${totalV2Reads}`);
  console.log(`  hasConsolidatedData: ${hasConsolidated}`);
  console.log(`  Resultado: ${hasConsolidated ? '→ V2 chainFactorsForPeriods() (NO fallback a V1)' : '→ V1 fallback (lee TODOS daily docs)'}`);

  // Simular docsCount que produciría chainFactorsForPeriods
  if (hasConsolidated) {
    console.log('\n  📊 Simulación de docsCount con datos V2:');
    
    const boundaries = {
      fiveYears: fiveYearsAgo.toISODate(),
      twoYears: now.minus({ years: 2 }).toISODate(),
      oneYear: now.minus({ years: 1 }).toISODate(),
      sixMonths: now.minus({ months: 6 }).toISODate(),
      threeMonths: now.minus({ months: 3 }).toISODate(),
      oneMonth: now.minus({ months: 1 }).toISODate(),
      ytd: now.startOf('year').toISODate()
    };

    for (const [period, boundary] of Object.entries(boundaries)) {
      let count = 0;

      // Contar yearly docs que contribuyen
      yearlySnap.docs.forEach(doc => {
        const data = doc.data();
        if (data.endDate >= boundary) {
          count += data.docsCount || 1;
        }
      });

      // Contar monthly docs que contribuyen
      monthlySnap.docs.forEach(doc => {
        const data = doc.data();
        if (data.endDate >= boundary) {
          count += data.docsCount || 1;
        }
      });

      // Contar daily docs que contribuyen
      dailySnap.docs.forEach(doc => {
        const data = doc.data();
        if (data.date >= boundary) {
          count++;
        }
      });

      const threshold = FRONTEND_THRESHOLDS[period];
      const visible = threshold !== undefined ? (count >= threshold ? '✅' : '❌') : '  ';

      console.log(
        `    ${period.padEnd(12)}: docsCount=${String(count).padEnd(5)} (boundary=${boundary}) ${visible}`
      );
    }

    // Detalle: ¿qué monthly docs contribuyen y con qué docsCount?
    console.log('\n  📋 Detalle de monthly docs y sus docsCount:');
    monthlySnap.docs.forEach(doc => {
      const data = doc.data();
      console.log(
        `    ${data.periodKey}: docsCount=${data.docsCount ?? 'UNDEFINED'}, ` +
        `endDate=${data.endDate}, ` +
        `${CURRENCY} exists=${!!data[CURRENCY]}, ` +
        `${CURRENCY}.validDocsCount=${data[CURRENCY]?.validDocsCount ?? 'UNDEF'}`
      );
    });
  }
}

// ============================================================================
// DIAGNÓSTICO 7: Cuentas del usuario
// ============================================================================

async function diagnoseUserAccounts() {
  separator('7. CUENTAS DEL USUARIO');

  const accountsSnap = await db.collection('portfolioAccounts')
    .where('userId', '==', USER_ID)
    .get();

  if (accountsSnap.empty) {
    console.log('  ❌ NO se encontraron cuentas');
    return [];
  }

  console.log(`  ✅ ${accountsSnap.size} cuentas encontradas:\n`);
  const accounts = [];
  accountsSnap.docs.forEach(doc => {
    const data = doc.data();
    console.log(`    ${doc.id}: name="${data.name || 'N/A'}", isActive=${data.isActive}`);
    accounts.push({ id: doc.id, name: data.name, isActive: data.isActive });
  });

  // Verificar snapshots per-account
  console.log('\n  🗂️  Snapshots per-account:');
  for (const acc of accounts.filter(a => a.isActive)) {
    const snapId = `${USER_ID}_${acc.id}_${CURRENCY}`;
    const snapDoc = await db.doc(`performanceSnapshots/${snapId}`).get();
    const exists = snapDoc.exists;
    const snapData = exists ? snapDoc.data() : null;
    console.log(
      `    ${acc.id} (${acc.name}): snapshot=${exists ? '✅' : '❌'}` +
      (exists ? `, timeline=${snapData.timeline?.length || 0} points, lastUpdated=${snapData.lastUpdated}` : '')
    );
  }

  return accounts;
}

// ============================================================================
// DIAGNÓSTICO 8: Listado de TODOS los snapshots del usuario
// ============================================================================

async function diagnoseAllSnapshots() {
  separator('8. TODOS LOS SNAPSHOTS DEL USUARIO');

  // Buscar todos los snapshots que empiecen con el userId
  const allSnaps = await db.collection('performanceSnapshots')
    .where('userId', '==', USER_ID)
    .get();

  if (allSnaps.empty) {
    console.log('  ❌ NO se encontraron snapshots para este usuario');
    return;
  }

  console.log(`  ✅ ${allSnaps.size} snapshots encontrados:\n`);
  console.log('  Doc ID                              | currency | accountId    | timeline | lastUpdated         | schema');
  console.log('  ' + '-'.repeat(110));

  allSnaps.docs.forEach(doc => {
    const data = doc.data();
    console.log(
      `  ${doc.id.padEnd(37)} | ${(data.currency || 'N/A').padEnd(8)} | ` +
      `${(data.accountId || 'N/A').padEnd(12)} | ${String(data.timeline?.length || 0).padEnd(8)} | ` +
      `${(data.lastUpdated || 'N/A').substring(0, 19).padEnd(19)} | ` +
      `v${data.schemaVersion ?? '?'}`
    );
  });
}

// ============================================================================
// RESUMEN FINAL
// ============================================================================

function printDiagnosisSummary(results) {
  separator('RESUMEN Y CONCLUSIONES');

  const { consolidated, daily, snapshot, cache, signal } = results;

  console.log('\n  📋 Estado de datos:');
  console.log(`    Monthly consolidated docs: ${consolidated.monthlyCount}`);
  console.log(`    Yearly consolidated docs:  ${consolidated.yearlyCount}`);
  console.log(`    Daily docs total:          ${daily.totalDailyDocs}`);
  console.log(`    Snapshot exists:           ${snapshot.snapshotExists}`);
  console.log(`    Performance cache count:   ${cache.cacheCount}`);
  console.log(`    lastSnapshotUpdate:        ${signal.lastSnapshotUpdate || 'NO DEFINIDO'}`);

  console.log('\n  🔍 Diagnóstico de causa raíz:');

  if (consolidated.monthlyCount === 0 && consolidated.yearlyCount === 0) {
    console.log('    → NO hay datos consolidados → V2 caería a V1 fallback');
    if (snapshot.snapshotExists) {
      const vdocs = snapshot.snapshot?.validDocsCountByPeriod || {};
      if ((vdocs.oneMonth || 0) < 20 || (vdocs.ytd || 0) < 30) {
        console.log('    → ⚠️  PERO el snapshot tiene docsCount bajos!');
        console.log('    → El snapshot fue generado durante una ventana donde V2 tenía cobertura parcial');
        console.log('    → O el snapshot fue generado antes del backfill y nunca se regeneró');
      }
    }
  } else {
    const hasGap = consolidated.monthlyCount < 12;
    if (hasGap) {
      console.log(`    → Solo ${consolidated.monthlyCount} monthly docs (menos de 12 meses completos)`);
      console.log('    → V2 procede SIN fallback a V1 (hasConsolidatedData=true)');
      console.log('    → Pero los meses cubren solo una parte del historial');
      console.log('    → docsCount por período será el count de los monthly + daily del mes actual');
    }
  }

  if (daily.totalDailyDocs > 200 && snapshot.snapshotExists) {
    const vdocs = snapshot.snapshot?.validDocsCountByPeriod || {};
    if ((vdocs.oneYear || 0) < 60) {
      console.log('    → ⚠️  DISCREPANCIA: Hay suficientes daily docs pero el snapshot reporta pocos');
      console.log('    → Esto confirma que el snapshot fue generado con V2 de cobertura parcial');
    }
  }

  console.log('\n  ✅ Acción recomendada:');
  console.log('    1. Si monthly docs son insuficientes → Ejecutar backfill de consolidación mensual');
  console.log('    2. Si snapshot tiene docsCount bajos → Regenerar snapshot (forceRefresh)');
  console.log('    3. Si cache performance está stale → Limpiar userData/.../performanceCache');
  console.log('    4. Corregir BUG-1 (IndexedDB cache sin lastSnapshotUpdate)');
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  console.log('\n🔬 BUG-PERF-SNAP-001 — Diagnóstico Completo');
  console.log(`   Fecha: ${new Date().toISOString()}`);
  console.log(`   Usuario: ${USER_ID}`);
  console.log(`   Currency: ${CURRENCY}`);
  console.log(`   Account: ${ACCOUNT_ID}`);

  try {
    const consolidated = await diagnoseConsolidatedDocs();
    const daily = await diagnoseDailyDocs();
    const snapshot = await diagnoseSnapshot();
    const cache = await diagnosePerformanceCache();
    const signal = await diagnoseSnapshotSignal();
    await diagnoseV2VsV1DocsCount(consolidated);
    await diagnoseUserAccounts();
    await diagnoseAllSnapshots();

    printDiagnosisSummary({ consolidated, daily, snapshot, cache, signal });

  } catch (error) {
    console.error('\n❌ Error durante diagnóstico:', error);
  }

  process.exit(0);
}

main();
