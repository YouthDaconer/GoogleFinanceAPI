/**
 * Script para diagnosticar y corregir TODOS los activos del usuario
 * Detecta inconsistencias de cashflow implícito en todos los assets
 */

const admin = require('firebase-admin');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const CURRENCIES = ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'];

function recalculateAdjustedChange(startValue, endValue, implicitCashFlow) {
  if (startValue === 0) return 0;
  const pureReturn = (endValue - startValue + implicitCashFlow) / startValue;
  return pureReturn * 100;
}

async function diagnoseAllAssets() {
  console.log('='.repeat(100));
  console.log('DIAGNÓSTICO COMPLETO DE TODOS LOS ACTIVOS');
  console.log('='.repeat(100));
  console.log();

  // 1. Obtener todos los activos únicos del usuario
  const assetsSnapshot = await db.collection('assets')
    .where('isActive', '==', true)
    .get();

  // Agrupar por ticker_assetType
  const assetKeys = new Map();
  const accountsPerAsset = new Map();
  
  for (const doc of assetsSnapshot.docs) {
    const asset = doc.data();
    // Verificar que pertenece al usuario
    const accountDoc = await db.doc(`portfolioAccounts/${asset.portfolioAccount}`).get();
    if (!accountDoc.exists || accountDoc.data().userId !== USER_ID) continue;
    
    const key = `${asset.name}_${asset.assetType}`;
    if (!assetKeys.has(key)) {
      assetKeys.set(key, {
        name: asset.name,
        assetType: asset.assetType,
        market: asset.market,
        totalUnits: 0,
        totalInvestment: 0
      });
      accountsPerAsset.set(key, new Set());
    }
    
    const assetInfo = assetKeys.get(key);
    assetInfo.totalUnits += asset.units;
    assetInfo.totalInvestment += asset.unitValue * asset.units;
    accountsPerAsset.get(key).add(asset.portfolioAccount);
  }

  console.log(`📊 Total activos únicos encontrados: ${assetKeys.size}`);
  console.log();

  // Mostrar resumen de activos
  console.log('ACTIVOS DEL USUARIO:');
  console.log('-'.repeat(80));
  for (const [key, info] of assetKeys) {
    console.log(`   ${key}: ${info.totalUnits.toFixed(4)} units, $${info.totalInvestment.toFixed(2)} inversión`);
  }
  console.log();

  // 2. Analizar datos a nivel overall para cada asset
  console.log('='.repeat(100));
  console.log('ANÁLISIS DE INCONSISTENCIAS');
  console.log('='.repeat(100));
  console.log();

  const overallSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/dates`)
    .orderBy('date', 'asc')
    .get();

  console.log(`📂 Total documentos de performance: ${overallSnapshot.docs.length}`);
  console.log();

  const allProblems = [];
  const problemsByAsset = new Map();

  // Para cada asset, buscar problemas
  for (const [assetKey, info] of assetKeys) {
    let previousDoc = null;
    const assetProblems = [];

    for (const doc of overallSnapshot.docs) {
      const data = doc.data();
      const assetData = data.USD?.assetPerformance?.[assetKey];
      
      if (!assetData) {
        continue;
      }

      if (!previousDoc) {
        previousDoc = doc;
        continue;
      }

      const previousData = previousDoc.data();
      const previousAssetData = previousData.USD?.assetPerformance?.[assetKey];

      if (!previousAssetData) {
        previousDoc = doc;
        continue;
      }

      const currentUnits = assetData.units || 0;
      const previousUnits = previousAssetData.units || 0;
      const currentCashFlow = assetData.totalCashFlow || 0;
      const currentAdjustedChange = assetData.adjustedDailyChangePercentage || 0;
      const startValue = previousAssetData.totalValue || 0;
      const endValue = assetData.totalValue || 0;
      
      const unitsDifference = currentUnits - previousUnits;
      
      // Detectar problema: diferencia de unidades pero cashflow = 0
      if (Math.abs(unitsDifference) > 0.00000001 && Math.abs(currentCashFlow) < 0.01 && previousUnits > 0) {
        const impliedPrice = currentUnits > 0 ? endValue / currentUnits : 0;
        const implicitCashFlow = -unitsDifference * impliedPrice;
        const correctedAdjustedChange = recalculateAdjustedChange(startValue, endValue, implicitCashFlow);
        
        assetProblems.push({
          docRef: doc.ref,
          date: data.date,
          assetKey,
          previousUnits,
          currentUnits,
          unitsDifference,
          startValue,
          endValue,
          originalAdjustedChange: currentAdjustedChange,
          correctedAdjustedChange,
          implicitCashFlow,
          impact: Math.abs(currentAdjustedChange - correctedAdjustedChange)
        });
      }
      
      previousDoc = doc;
    }

    if (assetProblems.length > 0) {
      problemsByAsset.set(assetKey, assetProblems);
      allProblems.push(...assetProblems);
    }
  }

  // 3. Mostrar resultados
  console.log('='.repeat(100));
  console.log('RESUMEN DE PROBLEMAS ENCONTRADOS');
  console.log('='.repeat(100));
  console.log();

  if (allProblems.length === 0) {
    console.log('✅ ¡NO SE ENCONTRARON INCONSISTENCIAS EN NINGÚN ACTIVO!');
    console.log();
    console.log('Todos los activos tienen sus rendimientos históricos correctamente calculados.');
    process.exit(0);
    return;
  }

  console.log(`⚠️ Total de inconsistencias encontradas: ${allProblems.length}`);
  console.log();

  // Ordenar por impacto (mayor primero)
  allProblems.sort((a, b) => b.impact - a.impact);

  // Mostrar por asset
  for (const [assetKey, problems] of problemsByAsset) {
    console.log(`📈 ${assetKey}: ${problems.length} problema(s)`);
    for (const p of problems) {
      console.log(`   📅 ${p.date}:`);
      console.log(`      Units: ${p.previousUnits.toFixed(6)} → ${p.currentUnits.toFixed(6)} (diff: ${p.unitsDifference.toFixed(6)})`);
      console.log(`      AdjChange: ${p.originalAdjustedChange.toFixed(4)}% → ${p.correctedAdjustedChange.toFixed(4)}% (impacto: ${p.impact.toFixed(2)}pp)`);
    }
    console.log();
  }

  // 4. Preguntar si aplicar correcciones
  console.log('='.repeat(100));
  console.log('APLICANDO CORRECCIONES');
  console.log('='.repeat(100));
  console.log();

  // Aplicar correcciones
  const batchSize = 450;
  for (let i = 0; i < allProblems.length; i += batchSize) {
    const batch = db.batch();
    const batchProblems = allProblems.slice(i, i + batchSize);
    
    for (const problem of batchProblems) {
      const updateData = {};
      
      for (const currency of CURRENCIES) {
        updateData[`${currency}.assetPerformance.${problem.assetKey}.adjustedDailyChangePercentage`] = problem.correctedAdjustedChange;
        updateData[`${currency}.assetPerformance.${problem.assetKey}.totalCashFlow`] = problem.implicitCashFlow;
      }
      
      batch.update(problem.docRef, updateData);
      console.log(`   ✅ ${problem.assetKey} @ ${problem.date}`);
    }
    
    await batch.commit();
  }

  // 5. También corregir a nivel de cuenta
  console.log();
  console.log('Corrigiendo datos a nivel de cuenta...');
  
  // Obtener todas las cuentas del usuario
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', USER_ID)
    .get();

  for (const accountDoc of accountsSnapshot.docs) {
    const accountId = accountDoc.id;
    
    const accountPerformanceSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/accounts/${accountId}/dates`)
      .orderBy('date', 'asc')
      .get();

    if (accountPerformanceSnapshot.empty) continue;

    for (const [assetKey] of assetKeys) {
      let previousDoc = null;
      const accountProblems = [];

      for (const doc of accountPerformanceSnapshot.docs) {
        const data = doc.data();
        const assetData = data.USD?.assetPerformance?.[assetKey];
        
        if (!assetData) continue;

        if (!previousDoc) {
          previousDoc = doc;
          continue;
        }

        const previousData = previousDoc.data();
        const previousAssetData = previousData.USD?.assetPerformance?.[assetKey];

        if (!previousAssetData) {
          previousDoc = doc;
          continue;
        }

        const currentUnits = assetData.units || 0;
        const previousUnits = previousAssetData.units || 0;
        const currentCashFlow = assetData.totalCashFlow || 0;
        const startValue = previousAssetData.totalValue || 0;
        const endValue = assetData.totalValue || 0;
        
        const unitsDifference = currentUnits - previousUnits;
        
        if (Math.abs(unitsDifference) > 0.00000001 && Math.abs(currentCashFlow) < 0.01 && previousUnits > 0) {
          const impliedPrice = currentUnits > 0 ? endValue / currentUnits : 0;
          const implicitCashFlow = -unitsDifference * impliedPrice;
          const correctedAdjustedChange = recalculateAdjustedChange(startValue, endValue, implicitCashFlow);
          
          accountProblems.push({
            docRef: doc.ref,
            date: data.date,
            assetKey,
            correctedAdjustedChange,
            implicitCashFlow
          });
        }
        
        previousDoc = doc;
      }

      // Aplicar correcciones de cuenta
      if (accountProblems.length > 0) {
        const batch = db.batch();
        for (const problem of accountProblems) {
          const updateData = {};
          for (const currency of CURRENCIES) {
            updateData[`${currency}.assetPerformance.${problem.assetKey}.adjustedDailyChangePercentage`] = problem.correctedAdjustedChange;
            updateData[`${currency}.assetPerformance.${problem.assetKey}.totalCashFlow`] = problem.implicitCashFlow;
          }
          batch.update(problem.docRef, updateData);
        }
        await batch.commit();
        console.log(`   ✅ Cuenta ${accountId}: ${accountProblems.length} correcciones para ${assetKey}`);
      }
    }
  }

  console.log();
  console.log('='.repeat(100));
  console.log('✅ TODAS LAS CORRECCIONES APLICADAS EXITOSAMENTE');
  console.log('='.repeat(100));
  console.log();
  console.log('⚠️ Recuerda invalidar el cache ejecutando:');
  console.log('   node scripts/invalidatePerformanceCache.js');

  process.exit(0);
}

diagnoseAllAssets().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
