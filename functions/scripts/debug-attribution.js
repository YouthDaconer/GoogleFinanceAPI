/**
 * Debug Script: Attribution Diagnosis
 * 
 * Script para diagnosticar problemas de atribución para un usuario específico.
 * 
 * Uso: node scripts/debug-attribution.js [userId] [period] [currency]
 */

require('dotenv').config({ path: require('path').resolve(__dirname, '../.env') });

// Configurar token de servicio para autenticación
const { serviceTokenFromEnv } = require('./serviceTokenFromEnv');
process.env.CF_SERVICE_TOKEN = serviceTokenFromEnv();

const admin = require('../services/firebaseAdmin');
const db = admin.firestore();

const userId = process.argv[2] || 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const period = process.argv[3] || 'YTD';
const currency = process.argv[4] || 'USD';

console.log('='.repeat(80));
console.log('🔍 DIAGNÓSTICO DE ATRIBUCIÓN');
console.log('='.repeat(80));
console.log(`Usuario: ${userId}`);
console.log(`Período: ${period}`);
console.log(`Moneda: ${currency}`);
console.log(`Fecha actual: ${new Date().toISOString()}`);
console.log('');

function getPeriodStartDate(period) {
  const now = new Date();
  switch (period) {
    case '1M':
      return new Date(now.getFullYear(), now.getMonth() - 1, now.getDate());
    case '3M':
      return new Date(now.getFullYear(), now.getMonth() - 3, now.getDate());
    case '6M':
      return new Date(now.getFullYear(), now.getMonth() - 6, now.getDate());
    case 'YTD':
      return new Date(now.getFullYear(), 0, 1);
    case '1Y':
      return new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
    case '2Y':
      return new Date(now.getFullYear() - 2, now.getMonth(), now.getDate());
    case 'ALL':
      return new Date(2000, 0, 1);
    default:
      return new Date(now.getFullYear(), 0, 1);
  }
}

async function diagnose() {
  try {
    // ========================================================================
    // 1. VERIFICAR DATOS DE PORTFOLIOPERFORMANCE
    // ========================================================================
    console.log('\n📊 1. DATOS DE PORTFOLIOPERFORMANCE');
    console.log('-'.repeat(60));
    
    const periodStartDate = getPeriodStartDate(period);
    const periodStartStr = periodStartDate.toISOString().split('T')[0];
    
    console.log(`Inicio del período ${period}: ${periodStartStr}`);
    
    // Obtener documentos más recientes
    const latestSnapshot = await db.collection(`portfolioPerformance/${userId}/dates`)
      .orderBy('date', 'desc')
      .limit(5)
      .get();
    
    if (latestSnapshot.empty) {
      console.log('❌ No se encontraron documentos de performance');
      return;
    }
    
    console.log(`\nÚltimos 5 documentos de performance:`);
    for (const doc of latestSnapshot.docs) {
      const data = doc.data();
      const currencyData = data[currency] || {};
      console.log(`  ${doc.id}:`);
      console.log(`    totalValue: $${currencyData.totalValue?.toFixed(2) || 'N/A'}`);
      console.log(`    dailyChange%: ${currencyData.adjustedDailyChangePercentage?.toFixed(4) || 'N/A'}%`);
    }
    
    // Obtener primer documento del período
    const startSnapshot = await db.collection(`portfolioPerformance/${userId}/dates`)
      .where('date', '>=', periodStartStr)
      .orderBy('date', 'asc')
      .limit(1)
      .get();
    
    if (!startSnapshot.empty) {
      const startDoc = startSnapshot.docs[0];
      const startData = startDoc.data();
      const startCurrency = startData[currency] || {};
      console.log(`\nPrimer documento del período (${startDoc.id}):`);
      console.log(`  totalValue: $${startCurrency.totalValue?.toFixed(2) || 'N/A'}`);
    }
    
    // ========================================================================
    // 2. VERIFICAR DATOS DE ASSETPERFORMANCE
    // ========================================================================
    console.log('\n\n📈 2. DATOS DE ASSETPERFORMANCE (EN EL ÚLTIMO DOC)');
    console.log('-'.repeat(60));
    
    const latestDoc = latestSnapshot.docs[0];
    const latestData = latestDoc.data();
    const latestCurrency = latestData[currency] || {};
    const assetPerformance = latestCurrency.assetPerformance || {};
    
    console.log(`Documento: ${latestDoc.id}`);
    console.log(`Activos encontrados: ${Object.keys(assetPerformance).length}`);
    
    // Listar todos los activos con sus datos
    const assetsArray = Object.entries(assetPerformance).map(([key, data]) => ({
      key,
      ticker: key.split('_')[0],
      type: key.split('_')[1] || 'stock',
      totalValue: data.totalValue || 0,
      totalInvestment: data.totalInvestment || 0,
      totalROI: data.totalROI || 0,
      units: data.units || 0,
      unrealizedPnL: data.unrealizedProfitAndLoss || 0
    }));
    
    // Ordenar por valor
    assetsArray.sort((a, b) => b.totalValue - a.totalValue);
    
    console.log('\nDetalle de activos (ordenados por valor):');
    console.log('+' + '-'.repeat(15) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(10) + '+' + '-'.repeat(12) + '+');
    console.log('| Ticker        | Valor       | Inversión   | ROI (%)   | Unidades   |');
    console.log('+' + '-'.repeat(15) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(10) + '+' + '-'.repeat(12) + '+');
    
    let totalValue = 0;
    for (const asset of assetsArray) {
      totalValue += asset.totalValue;
      console.log(`| ${asset.ticker.padEnd(13)} | $${asset.totalValue.toFixed(2).padStart(9)} | $${asset.totalInvestment.toFixed(2).padStart(9)} | ${asset.totalROI.toFixed(2).padStart(7)}% | ${asset.units.toFixed(4).padStart(10)} |`);
    }
    console.log('+' + '-'.repeat(15) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(10) + '+' + '-'.repeat(12) + '+');
    console.log(`Total valor: $${totalValue.toFixed(2)}`);
    
    // ========================================================================
    // 3. COMPARAR INICIO VS FIN DEL PERÍODO
    // ========================================================================
    console.log('\n\n🔄 3. COMPARACIÓN INICIO VS FIN DEL PERÍODO');
    console.log('-'.repeat(60));
    
    if (!startSnapshot.empty) {
      const startDoc = startSnapshot.docs[0];
      const startData = startDoc.data();
      const startCurrency = startData[currency] || {};
      const startAssetPerformance = startCurrency.assetPerformance || {};
      
      console.log(`Inicio período: ${startDoc.id}`);
      console.log(`Fin período: ${latestDoc.id}`);
      console.log('');
      
      // Comparar assets
      const allTickers = new Set([
        ...Object.keys(assetPerformance),
        ...Object.keys(startAssetPerformance)
      ]);
      
      let sumContributions = 0;
      const contributions = [];
      const startTotalValue = startCurrency.totalValue || totalValue;
      
      console.log('Cálculo de contribución por activo:');
      console.log('+' + '-'.repeat(15) + '+' + '-'.repeat(14) + '+' + '-'.repeat(14) + '+' + '-'.repeat(14) + '+' + '-'.repeat(12) + '+');
      console.log('| Ticker        | Valor Inicio | Valor Fin    | Cambio       | Contrib (pp)|');
      console.log('+' + '-'.repeat(15) + '+' + '-'.repeat(14) + '+' + '-'.repeat(14) + '+' + '-'.repeat(14) + '+' + '-'.repeat(12) + '+');
      
      for (const key of allTickers) {
        const ticker = key.split('_')[0];
        const startAsset = startAssetPerformance[key] || { totalValue: 0, units: 0 };
        const endAsset = assetPerformance[key] || { totalValue: 0, units: 0 };
        
        const valueStart = startAsset.totalValue || 0;
        const valueEnd = endAsset.totalValue || 0;
        const unitsStart = startAsset.units || 0;
        const unitsEnd = endAsset.units || 0;
        
        // Calcular contribución usando la fórmula correcta
        let contribution = 0;
        
        if (valueStart > 0 && unitsStart > 0) {
          const priceStart = valueStart / unitsStart;
          const priceEnd = valueEnd > 0 && unitsEnd > 0 ? valueEnd / unitsEnd : 0;
          const priceChange = priceEnd - priceStart;
          const valueChangeFromPrice = priceChange * unitsStart;
          contribution = startTotalValue > 0 ? (valueChangeFromPrice / startTotalValue) * 100 : 0;
        } else if (valueEnd > 0) {
          // Activo nuevo
          const unrealizedPnL = (endAsset.unrealizedProfitAndLoss || 0);
          contribution = startTotalValue > 0 ? (unrealizedPnL / startTotalValue) * 100 : 0;
        }
        
        const change = valueEnd - valueStart;
        sumContributions += contribution;
        contributions.push({ ticker, contribution, change });
        
        console.log(`| ${ticker.padEnd(13)} | $${valueStart.toFixed(2).padStart(11)} | $${valueEnd.toFixed(2).padStart(11)} | $${change.toFixed(2).padStart(11)} | ${contribution.toFixed(4).padStart(10)} |`);
      }
      
      console.log('+' + '-'.repeat(15) + '+' + '-'.repeat(14) + '+' + '-'.repeat(14) + '+' + '-'.repeat(14) + '+' + '-'.repeat(12) + '+');
      console.log(`\nSuma de contribuciones: ${sumContributions.toFixed(4)} pp`);
      
      // Identificar problemas
      console.log('\n⚠️  DIAGNÓSTICO DE PROBLEMAS:');
      console.log('-'.repeat(60));
      
      const zeroContribs = contributions.filter(c => c.contribution === 0 && c.change !== 0);
      const zeroValues = contributions.filter(c => c.contribution === 0 && c.change === 0);
      
      if (zeroContribs.length > 0) {
        console.log(`\n❌ Activos con contribución 0 pero con cambio de valor:`);
        for (const c of zeroContribs) {
          console.log(`   ${c.ticker}: cambio=$${c.change.toFixed(2)}, contribución=${c.contribution.toFixed(4)}pp`);
        }
      }
      
      if (zeroValues.length > 0) {
        console.log(`\n⚪ Activos sin movimiento (esperado que tengan 0pp):`);
        for (const c of zeroValues) {
          console.log(`   ${c.ticker}`);
        }
      }
      
      // Comparar con TWR histórico
      console.log('\n📉 COMPARACIÓN CON TWR:');
      
      // Calcular TWR compuesto del período
      const periodDocs = await db.collection(`portfolioPerformance/${userId}/dates`)
        .where('date', '>=', periodStartStr)
        .orderBy('date', 'asc')
        .get();
      
      let twrFactor = 1.0;
      for (const doc of periodDocs.docs) {
        const data = doc.data();
        const currData = data[currency] || {};
        const dailyChange = currData.adjustedDailyChangePercentage || 0;
        if (dailyChange !== 0) {
          twrFactor *= (1 + dailyChange / 100);
        }
      }
      const twr = (twrFactor - 1) * 100;
      
      console.log(`TWR del período (${periodDocs.size} docs): ${twr.toFixed(4)}%`);
      console.log(`Suma de contribuciones: ${sumContributions.toFixed(4)}%`);
      console.log(`Discrepancia: ${Math.abs(twr - sumContributions).toFixed(4)}%`);
      
      if (Math.abs(twr - sumContributions) > 1) {
        console.log(`\n⚠️  DISCREPANCIA SIGNIFICATIVA: La suma de contribuciones debería aproximarse al TWR`);
      }
    }
    
    // ========================================================================
    // 4. VERIFICAR ACTIVOS ACTIVOS DEL USUARIO
    // ========================================================================
    console.log('\n\n📋 4. ACTIVOS ACTIVOS EN FIRESTORE');
    console.log('-'.repeat(60));
    
    // Obtener cuentas del usuario
    const accountsSnapshot = await db.collection('portfolioAccounts')
      .where('userId', '==', userId)
      .where('isActive', '==', true)
      .get();
    
    console.log(`Cuentas activas: ${accountsSnapshot.size}`);
    
    for (const accDoc of accountsSnapshot.docs) {
      const acc = accDoc.data();
      console.log(`  - ${accDoc.id}: ${acc.name}`);
      
      // Obtener activos de esta cuenta
      const assetsSnapshot = await db.collection('assets')
        .where('portfolioAccount', '==', accDoc.id)
        .where('isActive', '==', true)
        .get();
      
      console.log(`    Activos: ${assetsSnapshot.size}`);
      for (const assetDoc of assetsSnapshot.docs) {
        const asset = assetDoc.data();
        console.log(`      - ${asset.name} (${asset.assetType || 'stock'}): ${asset.units} unidades`);
      }
    }
    
    console.log('\n' + '='.repeat(80));
    console.log('🏁 DIAGNÓSTICO COMPLETADO');
    console.log('='.repeat(80));
    
  } catch (error) {
    console.error('❌ Error en diagnóstico:', error);
  } finally {
    process.exit(0);
  }
}

diagnose();
