/**
 * Script de diagnóstico para verificar assetPerformance en documentos de backfill
 * Compara todos los campos de cada asset entre documentos del scheduler y backfill
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// Campos esperados en assetPerformance
const EXPECTED_ASSET_FIELDS = [
  'units',
  'totalValue',
  'totalInvestment',
  'totalCashFlow',
  'unrealizedProfitAndLoss',
  'doneProfitAndLoss',
  'totalROI',
  'dailyChangePercentage',
  'rawDailyChangePercentage',
  'adjustedDailyChangePercentage',
  'dailyReturn',
  'monthlyReturn',
  'annualReturn'
];

function formatValue(val) {
  if (val === undefined) return 'UNDEFINED';
  if (val === null) return 'NULL';
  if (typeof val === 'number') return val.toFixed(4);
  return String(val);
}

async function diagnose() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  const accountId = 'BZHvXz4QT2yqqqlFP22X'; // IBKR
  
  console.log('═'.repeat(80));
  console.log('  DIAGNÓSTICO DETALLADO DE assetPerformance EN BACKFILL');
  console.log('═'.repeat(80));
  console.log('');
  
  // ============================================================================
  // 1. COMPARAR ESTRUCTURA DE UN DOCUMENTO RECIENTE (scheduler) vs BACKFILL
  // ============================================================================
  console.log('=== COMPARACIÓN DE ESTRUCTURA DE ASSET ===\n');
  
  const recentDoc = await db.doc(`portfolioPerformance/${userId}/accounts/${accountId}/dates/2025-12-08`).get();
  const backfillDoc = await db.doc(`portfolioPerformance/${userId}/accounts/${accountId}/dates/2025-01-15`).get();
  
  if (recentDoc.exists && backfillDoc.exists) {
    const recentAssets = recentDoc.data().USD?.assetPerformance || {};
    const backfillAssets = backfillDoc.data().USD?.assetPerformance || {};
    
    // Tomar un asset común para comparar
    const commonAsset = Object.keys(backfillAssets)[0];
    
    if (commonAsset) {
      const recentAsset = recentAssets[commonAsset];
      const backfillAsset = backfillAssets[commonAsset];
      
      console.log(`Comparando asset: ${commonAsset}\n`);
      console.log('Campo'.padEnd(35), 'Scheduler (reciente)'.padEnd(20), 'Backfill'.padEnd(20), 'Estado');
      console.log('─'.repeat(90));
      
      EXPECTED_ASSET_FIELDS.forEach(field => {
        const schedulerVal = recentAsset?.[field];
        const backfillVal = backfillAsset?.[field];
        
        const hasScheduler = schedulerVal !== undefined;
        const hasBackfill = backfillVal !== undefined;
        
        let status = '';
        if (!hasScheduler && !hasBackfill) {
          status = '⚠️ Falta en ambos';
        } else if (!hasBackfill && hasScheduler) {
          status = '❌ FALTA EN BACKFILL';
        } else if (hasBackfill && !hasScheduler) {
          status = '➕ Solo en backfill';
        } else {
          status = '✅ OK';
        }
        
        console.log(
          field.padEnd(35),
          formatValue(schedulerVal).padEnd(20),
          formatValue(backfillVal).padEnd(20),
          status
        );
      });
    }
  }
  
  // ============================================================================
  // 2. VERIFICAR CONSISTENCIA DE VALORES EN UNA SECUENCIA DE DÍAS
  // ============================================================================
  console.log('\n');
  console.log('=== VERIFICACIÓN DE SECUENCIA DE DÍAS (VUAA.L_etf) ===\n');
  
  const dates = ['2025-01-13', '2025-01-14', '2025-01-15', '2025-01-16', '2025-01-17'];
  const assetToCheck = 'VUAA.L_etf';
  
  console.log('Fecha'.padEnd(15), 'Units'.padEnd(12), 'Value'.padEnd(12), 'Investment'.padEnd(12), 
              'Raw%'.padEnd(10), 'Adj%'.padEnd(10), 'CashFlow'.padEnd(12));
  console.log('─'.repeat(85));
  
  for (const date of dates) {
    const doc = await db.doc(`portfolioPerformance/${userId}/accounts/${accountId}/dates/${date}`).get();
    
    if (doc.exists) {
      const asset = doc.data().USD?.assetPerformance?.[assetToCheck];
      if (asset) {
        console.log(
          date.padEnd(15),
          (asset.units?.toFixed(4) || 'N/A').padEnd(12),
          (asset.totalValue?.toFixed(2) || 'N/A').padEnd(12),
          (asset.totalInvestment?.toFixed(2) || 'N/A').padEnd(12),
          (asset.rawDailyChangePercentage?.toFixed(4) || 'N/A').padEnd(10),
          (asset.adjustedDailyChangePercentage?.toFixed(4) || 'N/A').padEnd(10),
          (asset.totalCashFlow?.toFixed(2) || '0.00').padEnd(12)
        );
      } else {
        console.log(date.padEnd(15), 'Asset no encontrado en este documento');
      }
    } else {
      console.log(date.padEnd(15), 'Documento no existe');
    }
  }
  
  // ============================================================================
  // 3. VERIFICAR OVERALL - CAMPOS POR ASSET
  // ============================================================================
  console.log('\n');
  console.log('=== VERIFICACIÓN OVERALL - CAMPOS POR ASSET ===\n');
  
  const overallBackfill = await db.doc(`portfolioPerformance/${userId}/dates/2025-01-15`).get();
  
  if (overallBackfill.exists) {
    const overallAssets = overallBackfill.data().USD?.assetPerformance || {};
    const assetKeys = Object.keys(overallAssets);
    
    console.log(`Documentos OVERALL 2025-01-15 tiene ${assetKeys.length} assets:\n`);
    
    assetKeys.forEach(assetKey => {
      const asset = overallAssets[assetKey];
      const missingFields = EXPECTED_ASSET_FIELDS.filter(f => asset[f] === undefined);
      
      if (missingFields.length > 0) {
        console.log(`  ${assetKey}: ❌ Faltan campos: ${missingFields.join(', ')}`);
      } else {
        console.log(`  ${assetKey}: ✅ Todos los campos presentes`);
      }
    });
  }
  
  // ============================================================================
  // 4. COMPARAR VALORES CALCULADOS DEL BACKFILL CON TRANSACCIONES
  // ============================================================================
  console.log('\n');
  console.log('=== VALIDACIÓN DE VALORES CON TRANSACCIONES ===\n');
  
  // Obtener transacciones para validar
  const txSnapshot = await db.collection('transactions')
    .where('portfolioAccountId', '==', accountId)
    .where('date', '<=', '2025-01-15')
    .orderBy('date')
    .get();
  
  // Calcular totales esperados por asset
  const expectedHoldings = new Map();
  
  txSnapshot.docs.forEach(doc => {
    const tx = doc.data();
    if (!tx.assetName) return;
    
    const key = `${tx.assetName}_${tx.assetType || 'stock'}`;
    
    if (!expectedHoldings.has(key)) {
      expectedHoldings.set(key, { units: 0, totalInvestment: 0 });
    }
    
    const holding = expectedHoldings.get(key);
    
    if (tx.type === 'buy') {
      holding.units += tx.amount || 0;
      holding.totalInvestment += tx.totalCost || 0;
    } else if (tx.type === 'sell') {
      holding.units -= tx.amount || 0;
      // No reducir totalInvestment aquí - requiere lógica FIFO/LIFO
    }
  });
  
  // Comparar con documento backfill
  if (backfillDoc.exists) {
    const backfillAssets = backfillDoc.data().USD?.assetPerformance || {};
    
    console.log('Asset'.padEnd(20), 'Esperado'.padEnd(15), 'Backfill'.padEnd(15), 'Match');
    console.log('─'.repeat(65));
    
    expectedHoldings.forEach((expected, assetKey) => {
      if (expected.units < 0.0001) return; // Ignorar posiciones cerradas
      
      const backfill = backfillAssets[assetKey];
      const backfillUnits = backfill?.units || 0;
      const match = Math.abs(expected.units - backfillUnits) < 0.001;
      
      console.log(
        assetKey.padEnd(20),
        expected.units.toFixed(4).padEnd(15),
        backfillUnits.toFixed(4).padEnd(15),
        match ? '✅' : '❌'
      );
    });
  }
  
  // ============================================================================
  // 5. IDENTIFICAR DÍAS CON CAMBIOS ANORMALES
  // ============================================================================
  console.log('\n');
  console.log('=== DÍAS CON CAMBIOS ANORMALES (>5%) ===\n');
  
  const allDocs = await db.collection(`portfolioPerformance/${userId}/accounts/${accountId}/dates`)
    .where('date', '>=', '2025-01-01')
    .where('date', '<=', '2025-06-01')
    .orderBy('date')
    .get();
  
  let abnormalCount = 0;
  
  allDocs.docs.forEach(doc => {
    const data = doc.data();
    const adj = data.USD?.adjustedDailyChangePercentage || 0;
    
    if (Math.abs(adj) > 5) {
      abnormalCount++;
      console.log(`  ${data.date}: adjustedDailyChangePercentage = ${adj.toFixed(2)}%`);
    }
  });
  
  if (abnormalCount === 0) {
    console.log('  ✅ No se encontraron días con cambios anormales');
  } else {
    console.log(`\n  Total días con cambios anormales: ${abnormalCount}`);
  }
  
  console.log('\n');
  process.exit(0);
}

diagnose().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
