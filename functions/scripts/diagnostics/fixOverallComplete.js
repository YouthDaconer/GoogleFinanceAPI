/**
 * Script COMPLETO para corregir documentos de OVERALL
 * 
 * ESTRATEGIA:
 * Este script usa los datos de las cuentas individuales (que están correctos)
 * para recalcular los valores agregados de OVERALL usando el método de 
 * "valor pre-cambio" para ponderar correctamente los rendimientos.
 * 
 * Corrige:
 * - adjustedDailyChangePercentage para TODAS las monedas
 * - rawDailyChangePercentage para TODAS las monedas
 * - assetPerformance.*.adjustedDailyChangePercentage
 * 
 * USO:
 *   node fixOverallComplete.js --analyze     # Solo identificar problemas
 *   node fixOverallComplete.js --dry-run    # Ver cambios sin aplicar
 *   node fixOverallComplete.js --fix        # Aplicar correcciones
 * 
 * NOTA: Este script es diferente de backfillPortfolioPerformance.js:
 * - backfillPortfolioPerformance.js: Reconstruye datos desde transacciones + precios API
 *   Usar cuando: faltan documentos completos o cuentas individuales tienen errores
 * - fixOverallComplete.js: Corrige OVERALL usando datos existentes de cuentas
 *   Usar cuando: cuentas individuales están bien, solo OVERALL tiene errores de agregación
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
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
  TOLERANCE: 0.1 // 0.1% de tolerancia
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
  const accountsSnap = await db.collection('portfolioAccounts')
    .where('userId', '==', CONFIG.USER_ID)
    .where('isActive', '==', true)
    .get();
  
  const accounts = accountsSnap.docs.map(d => ({ id: d.id, ...d.data() }));
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
 * Calcular valores correctos para una moneda en un día
 * Retorna: { adjustedDailyChangePercentage, rawDailyChangePercentage, totalValue, totalInvestment }
 */
function calculateCorrectCurrencyValues(accountsDocs, date, currency) {
  let totalPreChange = 0;
  let weightedAdjustedChange = 0;
  let weightedRawChange = 0;
  let accountsWithData = 0;
  
  for (const [accountId, { docs }] of accountsDocs) {
    const dayDoc = docs.find(d => d.date === date);
    if (!dayDoc || !dayDoc[currency]) continue;
    
    const currData = dayDoc[currency];
    const value = currData.totalValue || 0;
    const adjChange = currData.adjustedDailyChangePercentage || 0;
    const rawChange = currData.rawDailyChangePercentage || currData.dailyChangePercentage || 0;
    
    if (value <= 0) continue;
    
    // Calcular valor pre-cambio para ponderación correcta
    const preChange = adjChange !== 0 ? value / (1 + adjChange / 100) : value;
    
    totalPreChange += preChange;
    weightedAdjustedChange += preChange * adjChange;
    weightedRawChange += preChange * rawChange;
    accountsWithData++;
  }
  
  if (totalPreChange <= 0) return null;
  
  return {
    adjustedDailyChangePercentage: weightedAdjustedChange / totalPreChange,
    rawDailyChangePercentage: weightedRawChange / totalPreChange,
    accountsUsed: accountsWithData
  };
}

/**
 * Calcular valores correctos para un asset específico en un día
 */
function calculateCorrectAssetValues(accountsDocs, date, currency, assetKey) {
  let totalPreChange = 0;
  let weightedAdjustedChange = 0;
  let accountsWithData = 0;
  
  for (const [accountId, { docs }] of accountsDocs) {
    const dayDoc = docs.find(d => d.date === date);
    if (!dayDoc || !dayDoc[currency]?.assetPerformance?.[assetKey]) continue;
    
    const assetData = dayDoc[currency].assetPerformance[assetKey];
    const value = assetData.totalValue || 0;
    const adjChange = assetData.adjustedDailyChangePercentage || 0;
    
    if (value <= 0) continue;
    
    const preChange = adjChange !== 0 ? value / (1 + adjChange / 100) : value;
    
    totalPreChange += preChange;
    weightedAdjustedChange += preChange * adjChange;
    accountsWithData++;
  }
  
  if (totalPreChange <= 0) return null;
  
  return {
    adjustedDailyChangePercentage: weightedAdjustedChange / totalPreChange,
    accountsUsed: accountsWithData
  };
}

/**
 * Encontrar todos los problemas en un documento
 */
function findDocumentProblems(overallDoc, accountsDocs) {
  const problems = {
    date: overallDoc.date,
    docRef: overallDoc.ref,
    currencyProblems: [],
    assetProblems: [],
    hasProblems: false
  };
  
  for (const currency of CONFIG.CURRENCIES) {
    const overallCurr = overallDoc[currency];
    if (!overallCurr) continue;
    
    const calculated = calculateCorrectCurrencyValues(accountsDocs, overallDoc.date, currency);
    if (!calculated) continue;
    
    const overallAdj = overallCurr.adjustedDailyChangePercentage || 0;
    const overallRaw = overallCurr.rawDailyChangePercentage || 0;
    
    const diffAdj = Math.abs(overallAdj - calculated.adjustedDailyChangePercentage);
    const diffRaw = Math.abs(overallRaw - calculated.rawDailyChangePercentage);
    
    if (diffAdj > CONFIG.TOLERANCE) {
      problems.currencyProblems.push({
        currency,
        field: 'adjustedDailyChangePercentage',
        current: overallAdj,
        correct: calculated.adjustedDailyChangePercentage,
        diff: diffAdj
      });
      problems.hasProblems = true;
    }
    
    if (diffRaw > CONFIG.TOLERANCE) {
      problems.currencyProblems.push({
        currency,
        field: 'rawDailyChangePercentage',
        current: overallRaw,
        correct: calculated.rawDailyChangePercentage,
        diff: diffRaw
      });
      problems.hasProblems = true;
    }
    
    // Verificar assets
    if (overallCurr.assetPerformance) {
      for (const [assetKey, assetData] of Object.entries(overallCurr.assetPerformance)) {
        const calculatedAsset = calculateCorrectAssetValues(accountsDocs, overallDoc.date, currency, assetKey);
        if (!calculatedAsset) continue;
        
        const assetAdj = assetData.adjustedDailyChangePercentage || 0;
        const diffAsset = Math.abs(assetAdj - calculatedAsset.adjustedDailyChangePercentage);
        
        if (diffAsset > CONFIG.TOLERANCE) {
          problems.assetProblems.push({
            currency,
            assetKey,
            current: assetAdj,
            correct: calculatedAsset.adjustedDailyChangePercentage,
            diff: diffAsset
          });
          problems.hasProblems = true;
        }
      }
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
  console.log('═'.repeat(100));
  console.log('  CORRECCIÓN COMPLETA DE OVERALL (todas las monedas y assets)');
  console.log('═'.repeat(100));
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
  console.log('🔍 Buscando documentos con error...');
  
  const allProblems = [];
  let totalCurrencyProblems = 0;
  let totalAssetProblems = 0;
  
  for (const doc of overallDocs) {
    const problems = findDocumentProblems(doc, accountsDocs);
    if (problems.hasProblems) {
      allProblems.push(problems);
      totalCurrencyProblems += problems.currencyProblems.length;
      totalAssetProblems += problems.assetProblems.length;
    }
  }
  
  if (allProblems.length === 0) {
    console.log('');
    console.log('✅ No se encontraron documentos con error en OVERALL');
    process.exit(0);
  }
  
  console.log(`   ${allProblems.length} documentos con error`);
  console.log(`   ${totalCurrencyProblems} problemas a nivel de moneda`);
  console.log(`   ${totalAssetProblems} problemas a nivel de asset`);
  
  // 3. Mostrar problemas
  console.log('');
  console.log('━'.repeat(100));
  console.log('  DOCUMENTOS CON ERROR');
  console.log('━'.repeat(100));
  
  for (const prob of allProblems) {
    console.log(`\n  📅 ${prob.date}:`);
    
    if (prob.currencyProblems.length > 0) {
      console.log('    Monedas:');
      prob.currencyProblems.forEach(cp => {
        console.log(`      ${cp.currency}.${cp.field}: ${cp.current.toFixed(2)}% → ${cp.correct.toFixed(2)}% (diff: ${cp.diff.toFixed(2)}%)`);
      });
    }
    
    if (prob.assetProblems.length > 0) {
      console.log(`    Assets: ${prob.assetProblems.length} errores`);
      // Solo mostrar primeros 5
      prob.assetProblems.slice(0, 5).forEach(ap => {
        console.log(`      ${ap.currency}.${ap.assetKey}: ${ap.current.toFixed(2)}% → ${ap.correct.toFixed(2)}%`);
      });
      if (prob.assetProblems.length > 5) {
        console.log(`      ... y ${prob.assetProblems.length - 5} más`);
      }
    }
  }
  
  // 4. Si es modo analyze, terminar aquí
  if (mode === 'analyze') {
    console.log('');
    console.log('📋 Modo análisis completo. Use --dry-run o --fix para ver/aplicar correcciones.');
    process.exit(0);
  }
  
  // 5. Si es dry-run, solo mostrar resumen
  if (mode === 'dry-run') {
    console.log('');
    console.log('━'.repeat(100));
    console.log('  RESUMEN DE CORRECCIONES');
    console.log('━'.repeat(100));
    console.log(`  Total de documentos a corregir: ${allProblems.length}`);
    console.log(`  Total de campos de moneda: ${totalCurrencyProblems}`);
    console.log(`  Total de campos de asset: ${totalAssetProblems}`);
    console.log('');
    console.log('📋 Modo dry-run completo. Use --fix para aplicar correcciones.');
    process.exit(0);
  }
  
  // 6. Aplicar correcciones (modo fix)
  console.log('');
  console.log('🔧 Aplicando correcciones...');
  
  let docsFixed = 0;
  let fieldsFixed = 0;
  
  // Procesar en batches de 10 documentos para evitar timeouts
  const batchSize = 10;
  
  for (let i = 0; i < allProblems.length; i += batchSize) {
    const batch = db.batch();
    const batchProblems = allProblems.slice(i, i + batchSize);
    
    for (const prob of batchProblems) {
      const updates = {};
      
      // Agregar correcciones de moneda
      for (const cp of prob.currencyProblems) {
        updates[`${cp.currency}.${cp.field}`] = cp.correct;
        fieldsFixed++;
      }
      
      // Agregar correcciones de assets
      for (const ap of prob.assetProblems) {
        updates[`${ap.currency}.assetPerformance.${ap.assetKey}.adjustedDailyChangePercentage`] = ap.correct;
        fieldsFixed++;
      }
      
      batch.update(prob.docRef, updates);
      docsFixed++;
    }
    
    await batch.commit();
    console.log(`   Batch ${Math.floor(i / batchSize) + 1}: ${batchProblems.length} documentos actualizados`);
  }
  
  console.log('');
  console.log('═'.repeat(100));
  console.log(`  ✅ ${docsFixed} documentos corregidos, ${fieldsFixed} campos actualizados`);
  console.log('═'.repeat(100));
  
  // 7. Invalidar caché
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
