/**
 * Script para CORREGIR documentos específicos de OVERALL que tienen bug
 * 
 * PROBLEMA IDENTIFICADO:
 * - 2025-01-02: adjustedDailyChangePercentage = 0% (debería ser ~1.47%)
 * - Otros posibles días con gaps después de feriados
 * 
 * ESTRATEGIA:
 * 1. Identificar días donde OVERALL tiene error
 * 2. Recalcular el valor correcto usando datos de cuentas individuales
 * 3. Actualizar solo esos documentos específicos
 * 
 * USO:
 *   node fixOverallAdjustedChange.js --analyze     # Solo identificar problemas
 *   node fixOverallAdjustedChange.js --dry-run    # Ver cambios sin aplicar
 *   node fixOverallAdjustedChange.js --fix        # Aplicar correcciones
 */

const admin = require('firebase-admin');

const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  CURRENCY: 'USD'
};

// ============================================================================
// UTILIDADES
// ============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  let mode = 'analyze';
  
  if (args.includes('--fix')) mode = 'fix';
  else if (args.includes('--dry-run')) mode = 'dry-run';
  
  return { mode };
}

async function getOverallDocs() {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/dates`;
  const snapshot = await db.collection(path).orderBy('date', 'asc').get();
  return snapshot.docs.map(doc => ({ id: doc.id, ref: doc.ref, ...doc.data() }));
}

async function getAccountsDocs() {
  // Obtener cuentas activas
  const accountsSnap = await db.collection('portfolioAccounts')
    .where('userId', '==', CONFIG.USER_ID)
    .where('isActive', '==', true)
    .get();
  
  const accounts = accountsSnap.docs.map(d => ({ id: d.id, ...d.data() }));
  
  // Obtener docs de cada cuenta
  const allAccountDocs = new Map();
  
  for (const account of accounts) {
    const path = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${account.id}/dates`;
    const snapshot = await db.collection(path).orderBy('date', 'asc').get();
    const docs = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    allAccountDocs.set(account.id, { account, docs });
  }
  
  return allAccountDocs;
}

/**
 * Calcular el adjustedDailyChangePercentage correcto para un día
 * usando los datos de las cuentas individuales con el método de valor pre-cambio
 */
function calculateCorrectAdjustedChange(accountsDocs, date, currency) {
  let totalPreChange = 0;
  let weightedChange = 0;
  let accountsWithData = 0;
  
  for (const [accountId, { docs }] of accountsDocs) {
    const dayDoc = docs.find(d => d.date === date);
    if (!dayDoc || !dayDoc[currency]) continue;
    
    const value = dayDoc[currency].totalValue || 0;
    const change = dayDoc[currency].adjustedDailyChangePercentage || 0;
    
    if (value <= 0) continue;
    
    // Calcular valor pre-cambio
    const preChange = change !== 0 ? value / (1 + change / 100) : value;
    
    totalPreChange += preChange;
    weightedChange += preChange * change;
    accountsWithData++;
  }
  
  if (totalPreChange <= 0) return null;
  
  return {
    adjustedDailyChangePercentage: weightedChange / totalPreChange,
    accountsUsed: accountsWithData
  };
}

/**
 * Identificar días donde OVERALL tiene error comparado con cuentas individuales
 */
function findProblematicDays(overallDocs, accountsDocs, currency) {
  const problems = [];
  
  // Crear mapa de OVERALL por fecha
  const overallByDate = new Map(overallDocs.map(d => [d.date, d]));
  
  // Obtener todas las fechas únicas de todas las cuentas
  const allDates = new Set();
  for (const [, { docs }] of accountsDocs) {
    docs.forEach(d => allDates.add(d.date));
  }
  
  // Verificar cada fecha
  for (const date of [...allDates].sort()) {
    const overall = overallByDate.get(date);
    if (!overall || !overall[currency]) continue;
    
    const overallChange = overall[currency].adjustedDailyChangePercentage || 0;
    const calculated = calculateCorrectAdjustedChange(accountsDocs, date, currency);
    
    if (!calculated) continue;
    
    const diff = Math.abs(overallChange - calculated.adjustedDailyChangePercentage);
    
    // Si la diferencia es significativa (> 0.1%), es un problema
    if (diff > 0.1) {
      problems.push({
        date,
        docRef: overall.ref,
        overallCurrent: overallChange,
        calculatedCorrect: calculated.adjustedDailyChangePercentage,
        diff,
        accountsUsed: calculated.accountsUsed
      });
    }
  }
  
  return problems;
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const { mode } = parseArgs();
  
  console.log('');
  console.log('═'.repeat(90));
  console.log('  CORRECCIÓN DE OVERALL adjustedDailyChangePercentage');
  console.log('═'.repeat(90));
  console.log('');
  console.log(`  Modo: ${mode}`);
  console.log('');

  // 1. Obtener datos
  console.log('📥 Obteniendo datos de OVERALL...');
  const overallDocs = await getOverallDocs();
  console.log(`   ${overallDocs.length} documentos en OVERALL`);
  
  console.log('📥 Obteniendo datos de cuentas individuales...');
  const accountsDocs = await getAccountsDocs();
  console.log(`   ${accountsDocs.size} cuentas encontradas`);
  
  // 2. Identificar problemas
  console.log('');
  console.log('🔍 Buscando días con error...');
  const problems = findProblematicDays(overallDocs, accountsDocs, CONFIG.CURRENCY);
  
  if (problems.length === 0) {
    console.log('');
    console.log('✅ No se encontraron días con error en OVERALL');
    process.exit(0);
  }
  
  console.log(`   Encontrados ${problems.length} días con error`);
  
  // 3. Mostrar problemas encontrados
  console.log('');
  console.log('━'.repeat(90));
  console.log('  DÍAS CON ERROR');
  console.log('━'.repeat(90));
  console.log('');
  console.log('  Fecha       | OVERALL   | Correcto  | Diferencia | Cuentas');
  console.log('  ' + '-'.repeat(70));
  
  let totalDiff = 0;
  problems.forEach(p => {
    console.log(
      `  ${p.date} | ` +
      `${p.overallCurrent.toFixed(2).padStart(8)}% | ` +
      `${p.calculatedCorrect.toFixed(2).padStart(8)}% | ` +
      `${p.diff.toFixed(2).padStart(9)}% | ` +
      `${p.accountsUsed}`
    );
    totalDiff += p.diff;
  });
  
  console.log('  ' + '-'.repeat(70));
  console.log(`  Total diferencia acumulada: ${totalDiff.toFixed(2)}%`);
  
  // 4. Si es modo analyze, terminar aquí
  if (mode === 'analyze') {
    console.log('');
    console.log('📋 Modo análisis completo. Use --dry-run o --fix para ver/aplicar correcciones.');
    process.exit(0);
  }
  
  // 5. Preparar correcciones
  console.log('');
  console.log('━'.repeat(90));
  console.log('  CORRECCIONES A APLICAR');
  console.log('━'.repeat(90));
  console.log('');
  
  const corrections = problems.map(p => ({
    date: p.date,
    docRef: p.docRef,
    oldValue: p.overallCurrent,
    newValue: p.calculatedCorrect,
    path: `USD.adjustedDailyChangePercentage`
  }));
  
  corrections.forEach(c => {
    console.log(`  ${c.date}: ${c.oldValue.toFixed(2)}% → ${c.newValue.toFixed(2)}%`);
  });
  
  // 6. Si es dry-run, terminar aquí
  if (mode === 'dry-run') {
    console.log('');
    console.log('📋 Modo dry-run completo. Use --fix para aplicar correcciones.');
    process.exit(0);
  }
  
  // 7. Aplicar correcciones (modo fix)
  console.log('');
  console.log('🔧 Aplicando correcciones...');
  
  const batch = db.batch();
  let batchCount = 0;
  
  for (const c of corrections) {
    const update = {
      [`${CONFIG.CURRENCY}.adjustedDailyChangePercentage`]: c.newValue
    };
    
    batch.update(c.docRef, update);
    batchCount++;
    
    // Firestore limita a 500 operaciones por batch
    if (batchCount >= 400) {
      await batch.commit();
      console.log(`   Batch de ${batchCount} correcciones aplicado`);
      batchCount = 0;
    }
  }
  
  // Commit final
  if (batchCount > 0) {
    await batch.commit();
    console.log(`   Batch final de ${batchCount} correcciones aplicado`);
  }
  
  console.log('');
  console.log('═'.repeat(90));
  console.log(`  ✅ ${corrections.length} correcciones aplicadas exitosamente`);
  console.log('═'.repeat(90));
  
  // 8. Invalidar caché
  console.log('');
  console.log('🗑️ Invalidando caché de rendimientos...');
  
  const cacheRef = db.collection(`portfolioPerformance/${CONFIG.USER_ID}/cache`);
  const cacheSnap = await cacheRef.get();
  
  if (!cacheSnap.empty) {
    const deleteBatch = db.batch();
    cacheSnap.docs.forEach(doc => deleteBatch.delete(doc.ref));
    await deleteBatch.commit();
    console.log(`   ${cacheSnap.size} entradas de caché eliminadas`);
  }
  
  console.log('');
  console.log('✅ Proceso completado');
  
  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
