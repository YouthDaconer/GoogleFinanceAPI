/**
 * Script de Diagnóstico: Verificar MWR Mensual en historicalReturnsService.js
 * 
 * Historia 25: Verificar que el cálculo de MWR mensual funciona correctamente
 * 
 * Este script simula la lógica del servicio para verificar los cálculos.
 * Es SOLO LECTURA - no modifica datos.
 * 
 * @usage node scripts/diagnostics/verifyMWRMonthly.js
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');
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

const CONFIG = {
  userId: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  currency: 'USD',
  accountName: 'XTB',
  accountId: 'Z3gnboYgRlTvSZNGSu8j' // XTB
};

// ============================================================================
// FUNCIONES
// ============================================================================

function calculateMWRSimple(startValue, endValue, totalCashFlow) {
  const netDeposits = -totalCashFlow; // cashflow negativo = depósitos
  
  if (startValue === 0 && netDeposits > 0) {
    return ((endValue - netDeposits) / netDeposits) * 100;
  } else if (startValue > 0) {
    const investmentBase = startValue + (netDeposits / 2);
    if (investmentBase > 0) {
      const gain = endValue - startValue - netDeposits;
      return (gain / investmentBase) * 100;
    }
  }
  return 0;
}

// ============================================================================
// ANÁLISIS PRINCIPAL
// ============================================================================

async function main() {
  console.log('='.repeat(80));
  console.log('🔍 VERIFICACIÓN DE MWR MENSUAL EN historicalReturnsService.js');
  console.log('   Historia 25 - Personal Return (MWR) Mensual');
  console.log('='.repeat(80));
  console.log('');
  
  try {
    // Obtener documentos de la cuenta XTB
    const snapshot = await db.collection('portfolioPerformance')
      .doc(CONFIG.userId)
      .collection('accounts')
      .doc(CONFIG.accountId)
      .collection('dates')
      .get();
    
    if (snapshot.empty) {
      console.log('❌ Sin documentos');
      process.exit(1);
    }
    
    const docs = snapshot.docs.map(doc => doc.data()).sort((a, b) => a.date.localeCompare(b.date));
    console.log(`📄 Documentos cargados: ${docs.length}`);
    console.log('');
    
    // Agrupar por mes
    const byMonth = {};
    docs.forEach(doc => {
      const [year, month] = doc.date.split('-');
      const key = `${year}-${month}`;
      if (!byMonth[key]) byMonth[key] = [];
      byMonth[key].push(doc);
    });
    
    // Simular el cálculo como lo haría historicalReturnsService.js
    console.log('📊 Simulación de cálculo como historicalReturnsService.js:');
    console.log('-'.repeat(80));
    console.log('| Mes     | startValue   | endValue     | cashFlow    | TWR      | MWR      | Diff    |');
    console.log('-'.repeat(80));
    
    const monthKeys = Object.keys(byMonth).sort().reverse().slice(0, 6);
    
    monthKeys.forEach(monthKey => {
      const monthDocs = byMonth[monthKey].sort((a, b) => a.date.localeCompare(b.date));
      
      if (monthDocs.length < 2) {
        console.log(`| ${monthKey}  | ⚠️  Insuficientes documentos (${monthDocs.length})`);
        return;
      }
      
      // Simular monthlyCompoundData como lo hace historicalReturnsService.js
      const firstDoc = monthDocs[0];
      const lastDoc = monthDocs[monthDocs.length - 1];
      
      const startTotalValue = firstDoc[CONFIG.currency]?.totalValue || 0;
      const endTotalValue = lastDoc[CONFIG.currency]?.totalValue || 0;
      
      // Sumar cashflows del mes
      let totalCashFlow = 0;
      monthDocs.forEach(doc => {
        totalCashFlow += doc[CONFIG.currency]?.totalCashFlow || 0;
      });
      
      // Calcular TWR (como lo hace actualmente)
      let twrFactor = 1;
      monthDocs.forEach(doc => {
        const dailyChange = doc[CONFIG.currency]?.adjustedDailyChangePercentage || 0;
        twrFactor *= (1 + dailyChange / 100);
      });
      const twr = (twrFactor - 1) * 100;
      
      // Calcular MWR (nuevo)
      const mwr = calculateMWRSimple(startTotalValue, endTotalValue, totalCashFlow);
      
      const diff = mwr - twr;
      
      console.log(`| ${monthKey}  | $${startTotalValue.toFixed(2).padStart(10)} | $${endTotalValue.toFixed(2).padStart(10)} | $${totalCashFlow.toFixed(2).padStart(9)} | ${twr.toFixed(2).padStart(7)}% | ${mwr.toFixed(2).padStart(7)}% | ${diff.toFixed(2).padStart(6)}% |`);
    });
    
    console.log('-'.repeat(80));
    console.log('');
    
    // Verificar estructura de respuesta esperada
    console.log('📋 ESTRUCTURA DE RESPUESTA ESPERADA:');
    console.log('');
    console.log('performanceByYear: {');
    console.log('  "2025": {');
    console.log('    months: { "0": 1.5, "1": -0.3, ... },  // TWR mensual (existente)');
    console.log('    total: 5.2,                            // TWR anual (existente)');
    console.log('    personalMonths: { "0": 2.1, "1": -0.1, ... },  // MWR mensual (NUEVO)');
    console.log('    personalTotal: 6.8                     // MWR anual (NUEVO)');
    console.log('  }');
    console.log('}');
    console.log('');
    
    console.log('monthlyCompoundData: {');
    console.log('  "2025": {');
    console.log('    "0": {');
    console.log('      returnPct: 1.5,          // TWR (existente)');
    console.log('      personalReturnPct: 2.1,  // MWR (NUEVO)');
    console.log('      startTotalValue: 1000,   // (existente)');
    console.log('      endTotalValue: 1021,     // (existente)');
    console.log('      totalCashFlow: -10,      // (existente)');
    console.log('      ...                      // otros campos existentes');
    console.log('    }');
    console.log('  }');
    console.log('}');
    console.log('');
    
    console.log('✅ Verificación completada');
    console.log('');
    console.log('📝 NOTA: Para ver los valores reales, ejecuta:');
    console.log('   1. Deploy: firebase deploy --only functions:getHistoricalReturns');
    console.log('   2. O prueba local: npm run serve (en functions/)');
    
    process.exit(0);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  }
}

main();
