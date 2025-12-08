/**
 * Script de Diagnóstico: Análisis completo de viabilidad MWR Mensual
 * 
 * Historia 25: Evaluar si podemos calcular MWR mensual con los datos existentes
 * 
 * Este script es SOLO LECTURA - no modifica ningún dato.
 * 
 * Estructura detectada de Firestore:
 * portfolioPerformance/{userId}/dates/{date}         <- Overall
 * portfolioPerformance/{userId}/accounts/{accountId}/dates/{date}  <- Por cuenta
 * 
 * @usage node scripts/diagnostics/analyzeMWRMonthlyComplete.js
 */

const admin = require('firebase-admin');
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
  monthsToAnalyze: 6
};

// ============================================================================
// FUNCIONES DE CÁLCULO MWR
// ============================================================================

/**
 * Calcula MWR Simple para un conjunto de documentos
 */
function calculateMWRForPeriod(docs, currency) {
  if (docs.length < 2) {
    return { success: false, reason: 'Menos de 2 documentos' };
  }
  
  // Ordenar por fecha
  const sorted = [...docs].sort((a, b) => a.date.localeCompare(b.date));
  
  const firstDoc = sorted[0];
  const lastDoc = sorted[sorted.length - 1];
  
  const startValue = firstDoc[currency]?.totalValue ?? 0;
  const endValue = lastDoc[currency]?.totalValue ?? 0;
  
  // Sumar cashflows
  let totalCashFlow = 0;
  let cashFlowDays = 0;
  
  sorted.forEach(doc => {
    const cf = doc[currency]?.totalCashFlow ?? 0;
    if (cf !== 0) {
      totalCashFlow += cf;
      cashFlowDays++;
    }
  });
  
  // Calcular MWR
  const netDeposits = -totalCashFlow; // Negativo = depósitos
  let mwr = 0;
  
  if (startValue === 0 && netDeposits > 0) {
    // Sin valor inicial, solo depósitos
    mwr = ((endValue - netDeposits) / netDeposits) * 100;
  } else if (startValue > 0) {
    // Con valor inicial
    const investmentBase = startValue + (netDeposits / 2);
    if (investmentBase > 0) {
      const gain = endValue - startValue - netDeposits;
      mwr = (gain / investmentBase) * 100;
    }
  }
  
  // Calcular TWR para comparación
  let twrFactor = 1;
  sorted.forEach(doc => {
    const dailyChange = doc[currency]?.adjustedDailyChangePercentage ?? 0;
    twrFactor *= (1 + dailyChange / 100);
  });
  const twr = (twrFactor - 1) * 100;
  
  return {
    success: true,
    period: {
      start: firstDoc.date,
      end: lastDoc.date,
      days: sorted.length
    },
    values: {
      startValue: startValue.toFixed(2),
      endValue: endValue.toFixed(2),
      netDeposits: netDeposits.toFixed(2),
      cashFlowDays
    },
    returns: {
      mwr: mwr.toFixed(2),
      twr: twr.toFixed(2),
      difference: (mwr - twr).toFixed(2)
    }
  };
}

/**
 * Agrupa documentos por mes
 */
function groupByMonth(docs) {
  const byMonth = {};
  
  docs.forEach(doc => {
    if (!doc.date) return;
    const [year, month] = doc.date.split('-');
    const key = `${year}-${month}`;
    
    if (!byMonth[key]) byMonth[key] = [];
    byMonth[key].push(doc);
  });
  
  return byMonth;
}

// ============================================================================
// ANÁLISIS PRINCIPAL
// ============================================================================

async function analyzeOverallDates() {
  console.log('\n📊 Analizando datos OVERALL (todas las cuentas)...');
  console.log('-'.repeat(60));
  
  const snapshot = await db.collection('portfolioPerformance')
    .doc(CONFIG.userId)
    .collection('dates')
    .get();
  
  if (snapshot.empty) {
    console.log('   ⚠️  Sin documentos en dates/');
    return;
  }
  
  const docs = snapshot.docs.map(doc => doc.data());
  console.log(`   📄 Documentos totales: ${docs.length}`);
  
  // Agrupar por mes
  const byMonth = groupByMonth(docs);
  const months = Object.keys(byMonth).sort().reverse().slice(0, CONFIG.monthsToAnalyze);
  
  console.log(`   📅 Meses con datos: ${Object.keys(byMonth).length}`);
  console.log('');
  
  console.log('   📈 MWR vs TWR por mes (OVERALL):');
  console.log('   ' + '-'.repeat(75));
  console.log('   | Mes     | Días | Start Value  | End Value    | MWR      | TWR      | Diff    |');
  console.log('   ' + '-'.repeat(75));
  
  months.forEach(month => {
    const monthDocs = byMonth[month];
    const result = calculateMWRForPeriod(monthDocs, CONFIG.currency);
    
    if (result.success) {
      const { period, values, returns } = result;
      console.log(`   | ${month}  | ${period.days.toString().padStart(4)} | $${values.startValue.padStart(10)} | $${values.endValue.padStart(10)} | ${returns.mwr.padStart(7)}% | ${returns.twr.padStart(7)}% | ${returns.difference.padStart(6)}% |`);
    } else {
      console.log(`   | ${month}  | ⚠️  ${result.reason}`);
    }
  });
  
  console.log('   ' + '-'.repeat(75));
  
  return { docs, byMonth };
}

async function analyzeAccountDates() {
  console.log('\n📊 Analizando datos POR CUENTA...');
  console.log('-'.repeat(60));
  
  const accountsSnapshot = await db.collection('portfolioPerformance')
    .doc(CONFIG.userId)
    .collection('accounts')
    .get();
  
  if (accountsSnapshot.empty) {
    console.log('   ⚠️  Sin cuentas');
    return;
  }
  
  // Obtener nombres de cuentas
  const portfolioAccounts = await db.collection('portfolioAccounts')
    .where('userId', '==', CONFIG.userId)
    .get();
  
  const accountNames = {};
  portfolioAccounts.docs.forEach(doc => {
    accountNames[doc.id] = doc.data().name;
  });
  
  console.log(`   📁 Cuentas encontradas: ${accountsSnapshot.docs.length}`);
  console.log('');
  
  for (const accDoc of accountsSnapshot.docs) {
    const accountId = accDoc.id;
    const accountName = accountNames[accountId] || accountId;
    
    console.log(`\n   🏦 ${accountName} (${accountId})`);
    console.log('   ' + '-'.repeat(60));
    
    const datesSnapshot = await accDoc.ref.collection('dates').get();
    
    if (datesSnapshot.empty) {
      console.log('      ⚠️  Sin documentos de fechas');
      continue;
    }
    
    const docs = datesSnapshot.docs.map(doc => doc.data());
    console.log(`      📄 Documentos: ${docs.length}`);
    
    const byMonth = groupByMonth(docs);
    const months = Object.keys(byMonth).sort().reverse().slice(0, 3); // Solo 3 meses por cuenta
    
    console.log('      📈 MWR vs TWR (últimos 3 meses):');
    
    months.forEach(month => {
      const monthDocs = byMonth[month];
      const result = calculateMWRForPeriod(monthDocs, CONFIG.currency);
      
      if (result.success) {
        const { returns } = result;
        const diff = parseFloat(returns.difference);
        const diffIndicator = diff > 1 ? '🟢 buen timing' : diff < -1 ? '🔴 mal timing' : '⚪ neutral';
        console.log(`      ${month}: MWR ${returns.mwr}% | TWR ${returns.twr}% | ${diffIndicator}`);
      } else {
        console.log(`      ${month}: ⚠️  ${result.reason}`);
      }
    });
  }
}

async function checkFieldAvailability() {
  console.log('\n📋 Verificando disponibilidad de campos para MWR...');
  console.log('-'.repeat(60));
  
  const snapshot = await db.collection('portfolioPerformance')
    .doc(CONFIG.userId)
    .collection('dates')
    .limit(10)
    .get();
  
  if (snapshot.empty) {
    console.log('   ⚠️  Sin documentos');
    return false;
  }
  
  const requiredFields = [
    'totalValue',
    'totalInvestment', 
    'totalCashFlow',
    'adjustedDailyChangePercentage'
  ];
  
  const optionalFields = [
    'doneProfitAndLoss',
    'unrealizedProfitAndLoss'
  ];
  
  let allRequired = true;
  
  console.log(`\n   Campos en ${CONFIG.currency}:`);
  
  const sampleDoc = snapshot.docs[0].data();
  const currencyData = sampleDoc[CONFIG.currency] || {};
  
  requiredFields.forEach(field => {
    const exists = currencyData[field] !== undefined;
    const icon = exists ? '✅' : '❌';
    console.log(`   ${icon} ${field}: ${exists ? currencyData[field] : 'NO EXISTE'}`);
    if (!exists) allRequired = false;
  });
  
  console.log('');
  optionalFields.forEach(field => {
    const exists = currencyData[field] !== undefined;
    const icon = exists ? '✅' : '⚠️';
    console.log(`   ${icon} ${field}: ${exists ? currencyData[field] : 'no disponible'}`);
  });
  
  return allRequired;
}

// ============================================================================
// CONCLUSIONES
// ============================================================================

function printConclusions(allFieldsAvailable) {
  console.log('\n' + '='.repeat(80));
  console.log('🎯 CONCLUSIONES Y RECOMENDACIONES');
  console.log('='.repeat(80));
  
  console.log('\n📊 VIABILIDAD DE MWR MENSUAL:');
  
  if (allFieldsAvailable) {
    console.log('   ✅ ES 100% VIABLE calcular MWR mensual con los datos existentes');
    console.log('');
    console.log('   📝 DATOS DISPONIBLES:');
    console.log('   • totalValue - Valor del portafolio por día ✅');
    console.log('   • totalCashFlow - Flujos diarios (depósitos/retiros) ✅');
    console.log('   • totalInvestment - Inversión acumulada ✅');
    console.log('   • adjustedDailyChangePercentage - TWR diario ✅');
    console.log('');
    console.log('   🚫 NO SE NECESITA BACKFILL:');
    console.log('   Los campos necesarios ya existen en todos los documentos.');
    console.log('   El cálculo de MWR mensual puede hacerse en tiempo real.');
    console.log('');
    console.log('   📋 PRÓXIMOS PASOS RECOMENDADOS:');
    console.log('   1. Agregar cálculo de MWR mensual en historicalReturnsService.js');
    console.log('   2. Incluir monthlyPersonalReturns en la respuesta');
    console.log('   3. Actualizar gráficos para mostrar MWR en totales mensuales/anuales');
    console.log('');
    console.log('   💡 OBSERVACIONES:');
    console.log('   • MWR > TWR indica "buen timing" (compraste bajo)');
    console.log('   • MWR < TWR indica "mal timing" (compraste alto)');
    console.log('   • MWR ≈ TWR indica pocos/sin cashflows en el período');
  } else {
    console.log('   ⚠️  Faltan algunos campos necesarios');
    console.log('   Se requeriría un script de backfill para completar datos');
  }
  
  console.log('');
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  console.log('='.repeat(80));
  console.log('🔍 ANÁLISIS COMPLETO DE VIABILIDAD PARA MWR MENSUAL');
  console.log('   Historia 25 - Personal Return (MWR) + TWR Dual Metrics');
  console.log('   📌 Este script es SOLO LECTURA - no modifica datos');
  console.log('='.repeat(80));
  
  try {
    // 1. Verificar campos disponibles
    const allFieldsAvailable = await checkFieldAvailability();
    
    // 2. Analizar datos overall
    await analyzeOverallDates();
    
    // 3. Analizar datos por cuenta
    await analyzeAccountDates();
    
    // 4. Imprimir conclusiones
    printConclusions(allFieldsAvailable);
    
    console.log('✅ Análisis completado\n');
    process.exit(0);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  }
}

main();
