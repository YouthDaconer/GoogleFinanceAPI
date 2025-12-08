/**
 * Script de Diagnóstico: Análisis de viabilidad para MWR Mensual
 * 
 * Historia 25: Evaluar si podemos calcular MWR mensual con los datos existentes
 * 
 * Este script es SOLO LECTURA - no modifica ningún dato.
 * 
 * Analiza:
 * 1. Estructura de campos disponibles en portfolioPerformance
 * 2. Disponibilidad de totalCashFlow por documento/día
 * 3. Cobertura de datos por mes
 * 4. Viabilidad de calcular MWR mensual
 * 
 * @usage node scripts/diagnostics/analyzeMWRMonthlyViability.js
 */

const admin = require('firebase-admin');
const path = require('path');

// Inicializar Firebase Admin (mismo patrón que otros scripts)
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
  // IDs de usuarios para analizar (usando los de otros diagnósticos)
  userIds: [
    'DDeR8P5hYgfuN8gcU4RsQfdTJqx2' // Usuario principal del diagnóstico
  ],
  // Meses a analizar (0 = Enero, 11 = Diciembre)
  monthsToAnalyze: 6, // Últimos 6 meses
  // Moneda principal
  currency: 'USD',
  // Límite de documentos por usuario
  maxDocsPerUser: 500
};

// ============================================================================
// FUNCIONES DE ANÁLISIS
// ============================================================================

/**
 * Analiza la estructura de campos de un documento
 */
function analyzeDocumentStructure(doc, currency) {
  const data = doc.data();
  const currencyData = data[currency] || {};
  
  return {
    date: data.date,
    hasDate: !!data.date,
    hasCurrencyData: !!data[currency],
    fields: {
      totalValue: currencyData.totalValue !== undefined,
      totalInvestment: currencyData.totalInvestment !== undefined,
      totalCashFlow: currencyData.totalCashFlow !== undefined,
      adjustedDailyChangePercentage: currencyData.adjustedDailyChangePercentage !== undefined,
      doneProfitAndLoss: currencyData.doneProfitAndLoss !== undefined,
      unrealizedProfitAndLoss: currencyData.unrealizedProfitAndLoss !== undefined,
    },
    values: {
      totalValue: currencyData.totalValue ?? null,
      totalInvestment: currencyData.totalInvestment ?? null,
      totalCashFlow: currencyData.totalCashFlow ?? null,
      adjustedDailyChangePercentage: currencyData.adjustedDailyChangePercentage ?? null,
    }
  };
}

/**
 * Agrupa documentos por mes
 */
function groupDocumentsByMonth(docs) {
  const byMonth = {};
  
  docs.forEach(doc => {
    const data = doc.data();
    if (!data.date) return;
    
    const [year, month] = data.date.split('-');
    const key = `${year}-${month}`;
    
    if (!byMonth[key]) {
      byMonth[key] = [];
    }
    byMonth[key].push(doc);
  });
  
  return byMonth;
}

/**
 * Calcula estadísticas de cobertura para un mes
 */
function calculateMonthCoverage(docs, currency) {
  if (docs.length === 0) {
    return { coverage: 0, hasCashFlows: false, cashFlowDays: 0, totalCashFlow: 0 };
  }
  
  // Obtener primer y último día del mes
  const firstDoc = docs[0].data();
  const [year, month] = firstDoc.date.split('-');
  const daysInMonth = new Date(parseInt(year), parseInt(month), 0).getDate();
  
  let cashFlowDays = 0;
  let totalCashFlow = 0;
  let daysWithData = 0;
  
  docs.forEach(doc => {
    const data = doc.data();
    const currencyData = data[currency] || {};
    
    daysWithData++;
    
    if (currencyData.totalCashFlow !== undefined && currencyData.totalCashFlow !== 0) {
      cashFlowDays++;
      totalCashFlow += currencyData.totalCashFlow;
    }
  });
  
  return {
    daysInMonth,
    daysWithData,
    coverage: (daysWithData / daysInMonth) * 100,
    hasCashFlows: cashFlowDays > 0,
    cashFlowDays,
    totalCashFlow
  };
}

/**
 * Intenta calcular MWR para un mes específico
 */
function tryCalculateMWRForMonth(docs, currency) {
  if (docs.length < 2) {
    return { success: false, reason: 'Insuficientes documentos (< 2)' };
  }
  
  // Ordenar por fecha
  const sorted = [...docs].sort((a, b) => 
    a.data().date.localeCompare(b.data().date)
  );
  
  const firstDoc = sorted[0].data();
  const lastDoc = sorted[sorted.length - 1].data();
  
  const startData = firstDoc[currency] || {};
  const endData = lastDoc[currency] || {};
  
  const startValue = startData.totalValue ?? 0;
  const endValue = endData.totalValue ?? 0;
  
  // Recolectar cashflows del mes
  const cashFlows = [];
  let totalCashFlow = 0;
  
  sorted.forEach(doc => {
    const data = doc.data();
    const currencyData = data[currency] || {};
    const cf = currencyData.totalCashFlow ?? 0;
    
    if (cf !== 0) {
      cashFlows.push({
        date: data.date,
        amount: cf
      });
      totalCashFlow += cf;
    }
  });
  
  // Calcular MWR simple
  const netDeposits = -totalCashFlow;
  let mwr = 0;
  
  if (startValue === 0 && netDeposits > 0) {
    mwr = ((endValue - netDeposits) / netDeposits) * 100;
  } else if (startValue > 0) {
    const investmentBase = startValue + (netDeposits / 2);
    if (investmentBase > 0) {
      const gain = endValue - startValue - netDeposits;
      mwr = (gain / investmentBase) * 100;
    }
  }
  
  // Calcular TWR para comparación
  let twrFactor = 1;
  sorted.forEach(doc => {
    const data = doc.data();
    const currencyData = data[currency] || {};
    const dailyChange = currencyData.adjustedDailyChangePercentage ?? 0;
    twrFactor *= (1 + dailyChange / 100);
  });
  const twr = (twrFactor - 1) * 100;
  
  return {
    success: true,
    startDate: firstDoc.date,
    endDate: lastDoc.date,
    startValue,
    endValue,
    totalCashFlow,
    cashFlowsCount: cashFlows.length,
    mwr: mwr.toFixed(2),
    twr: twr.toFixed(2),
    difference: (mwr - twr).toFixed(2)
  };
}

/**
 * Analiza datos por cuenta (overall + cuentas individuales)
 */
async function analyzeByAccount(userId, currency) {
  console.log('\n📂 Analizando datos por cuenta...\n');
  
  // Primero, obtener las cuentas del usuario
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  const accounts = [
    { id: 'overall', name: 'Overall (Agregado)' },
    ...accountsSnapshot.docs.map(doc => ({
      id: doc.id,
      name: doc.data().name
    }))
  ];
  
  console.log(`  Cuentas encontradas: ${accounts.length - 1} + overall`);
  
  const results = {};
  
  for (const account of accounts) {
    // Consultar documentos para esta cuenta (sin ordenar para evitar índice)
    // Solo usamos userId + portfolioAccount
    const snapshot = await db.collection('portfolioPerformance')
      .where('userId', '==', userId)
      .where('portfolioAccount', '==', account.id)
      .limit(CONFIG.maxDocsPerUser)
      .get();
    
    if (snapshot.empty) {
      results[account.id] = {
        name: account.name,
        docsCount: 0,
        hasData: false
      };
      continue;
    }
    
    // Ordenar manualmente los documentos por fecha
    const sortedDocs = snapshot.docs.sort((a, b) => 
      (b.data().date || '').localeCompare(a.data().date || '')
    );
    
    // Analizar primer documento para ver estructura
    const sampleDoc = sortedDocs[0];
    const structure = analyzeDocumentStructure(sampleDoc, currency);
    
    // Agrupar por mes
    const byMonth = groupDocumentsByMonth(sortedDocs);
    const monthlyStats = {};
    
    Object.keys(byMonth).sort().reverse().slice(0, CONFIG.monthsToAnalyze).forEach(monthKey => {
      const monthDocs = byMonth[monthKey];
      const coverage = calculateMonthCoverage(monthDocs, currency);
      const mwrCalc = tryCalculateMWRForMonth(monthDocs, currency);
      
      monthlyStats[monthKey] = {
        ...coverage,
        mwrCalculation: mwrCalc
      };
    });
    
    results[account.id] = {
      name: account.name,
      docsCount: snapshot.docs.length,
      hasData: true,
      fieldAvailability: structure.fields,
      monthlyStats
    };
  }
  
  return results;
}

/**
 * Genera reporte de viabilidad
 */
function generateViabilityReport(accountsAnalysis) {
  console.log('\n' + '='.repeat(80));
  console.log('📊 REPORTE DE VIABILIDAD PARA MWR MENSUAL');
  console.log('='.repeat(80));
  
  let allFieldsAvailable = true;
  let hasMonthlyData = true;
  let hasCashFlowData = true;
  
  Object.entries(accountsAnalysis).forEach(([accountId, data]) => {
    console.log(`\n📁 ${data.name} (${accountId})`);
    console.log('-'.repeat(50));
    
    if (!data.hasData) {
      console.log('  ⚠️  Sin datos');
      hasMonthlyData = false;
      return;
    }
    
    console.log(`  📄 Documentos: ${data.docsCount}`);
    
    // Campos disponibles
    console.log('\n  📋 Campos disponibles:');
    Object.entries(data.fieldAvailability).forEach(([field, available]) => {
      const icon = available ? '✅' : '❌';
      console.log(`     ${icon} ${field}`);
      if (!available && ['totalValue', 'totalCashFlow', 'totalInvestment'].includes(field)) {
        allFieldsAvailable = false;
      }
    });
    
    // Estadísticas mensuales
    console.log('\n  📅 Análisis mensual (últimos meses):');
    Object.entries(data.monthlyStats || {}).forEach(([month, stats]) => {
      console.log(`\n     ${month}:`);
      console.log(`       • Cobertura: ${stats.coverage.toFixed(1)}% (${stats.daysWithData}/${stats.daysInMonth} días)`);
      console.log(`       • CashFlows: ${stats.hasCashFlows ? `${stats.cashFlowDays} días con flujos` : 'Sin flujos'}`);
      
      if (!stats.hasCashFlows) {
        // No hay cashflows no significa error, solo que no hubo depósitos/retiros
      }
      
      if (stats.mwrCalculation.success) {
        console.log(`       • MWR Calculado: ${stats.mwrCalculation.mwr}%`);
        console.log(`       • TWR Calculado: ${stats.mwrCalculation.twr}%`);
        console.log(`       • Diferencia: ${stats.mwrCalculation.difference}%`);
      } else {
        console.log(`       • ⚠️  MWR: ${stats.mwrCalculation.reason}`);
      }
    });
  });
  
  // Conclusión
  console.log('\n' + '='.repeat(80));
  console.log('🎯 CONCLUSIÓN');
  console.log('='.repeat(80));
  
  const conclusions = [];
  
  if (allFieldsAvailable) {
    conclusions.push('✅ Todos los campos necesarios (totalValue, totalCashFlow, totalInvestment) están disponibles');
  } else {
    conclusions.push('❌ Faltan algunos campos necesarios para el cálculo de MWR');
  }
  
  if (hasMonthlyData) {
    conclusions.push('✅ Hay datos diarios suficientes para calcular MWR mensual');
  } else {
    conclusions.push('⚠️  Algunas cuentas no tienen datos suficientes');
  }
  
  conclusions.push('\n📝 RECOMENDACIÓN:');
  
  if (allFieldsAvailable && hasMonthlyData) {
    conclusions.push('   ✅ ES VIABLE calcular MWR mensual con los datos existentes');
    conclusions.push('   • No se necesita backfill de datos');
    conclusions.push('   • El cálculo puede hacerse en tiempo real usando los documentos diarios');
    conclusions.push('   • Podemos agregar MWR mensual en el backend (historicalReturnsService.js)');
  } else {
    conclusions.push('   ⚠️  Se requiere análisis adicional o backfill de datos');
  }
  
  conclusions.forEach(c => console.log(c));
  
  return {
    allFieldsAvailable,
    hasMonthlyData,
    isViable: allFieldsAvailable && hasMonthlyData
  };
}

// ============================================================================
// EJECUCIÓN PRINCIPAL
// ============================================================================

async function main() {
  console.log('🔍 Iniciando análisis de viabilidad para MWR Mensual...');
  console.log('📌 Este script es SOLO LECTURA - no modifica datos\n');
  
  try {
    for (const userId of CONFIG.userIds) {
      console.log(`\n👤 Analizando usuario: ${userId}`);
      console.log('='.repeat(80));
      
      const accountsAnalysis = await analyzeByAccount(userId, CONFIG.currency);
      const viability = generateViabilityReport(accountsAnalysis);
      
      console.log('\n');
    }
    
    console.log('✅ Análisis completado');
    process.exit(0);
    
  } catch (error) {
    console.error('❌ Error durante el análisis:', error);
    process.exit(1);
  }
}

main();
