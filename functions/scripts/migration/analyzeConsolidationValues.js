#!/usr/bin/env node
/**
 * Script de Análisis Pre-Migración: Valores de 2025
 * 
 * Muestra los valores que se calcularían para cada mes de 2025
 * y el total anual, para overall y cada cuenta.
 * 
 * @module analyzeConsolidationValues
 * @see docs/stories/64.story.md (COST-OPT-003)
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');
const path = require('path');

// Inicializar Firebase Admin
const serviceAccountPath = path.join(__dirname, '../../key.json');
const serviceAccount = require(serviceAccountPath);

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// Importar funciones de consolidación
const { consolidatePeriod, CONSOLIDATED_SCHEMA_VERSION } = require('../../utils/periodConsolidation');
const { consolidateMonthsToYear } = require('../../services/periodConsolidationScheduled');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

const userId = process.argv[2] || 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const targetYear = process.argv[3] || '2025';
const currencies = ['USD', 'COP', 'EUR'];

// ============================================================================
// FUNCIÓN PRINCIPAL
// ============================================================================

async function main() {
  console.log('\n' + '='.repeat(80));
  console.log('  ANÁLISIS DE VALORES DE CONSOLIDACIÓN - 2025');
  console.log('='.repeat(80) + '\n');
  
  console.log(`📋 Usuario: ${userId}`);
  console.log(`📅 Año objetivo: ${targetYear}`);
  console.log('');
  
  try {
    // 1. Analizar OVERALL
    console.log('\n' + '─'.repeat(80));
    console.log('  📊 OVERALL');
    console.log('─'.repeat(80));
    
    const basePath = `portfolioPerformance/${userId}`;
    await analyzeYearForPath(basePath, 'overall');
    
    // 2. Analizar cada CUENTA
    const accountsSnapshot = await db.collection(`${basePath}/accounts`).get();
    
    for (const accountDoc of accountsSnapshot.docs) {
      const accountId = accountDoc.id;
      const accountPath = `${basePath}/accounts/${accountId}`;
      
      console.log('\n' + '─'.repeat(80));
      console.log(`  👤 CUENTA: ${accountId}`);
      console.log('─'.repeat(80));
      
      await analyzeYearForPath(accountPath, accountId);
    }
    
    console.log('\n' + '='.repeat(80));
    console.log('  FIN DEL ANÁLISIS');
    console.log('='.repeat(80) + '\n');
    
  } catch (error) {
    console.error('❌ Error:', error);
    process.exit(1);
  }
}

// ============================================================================
// ANÁLISIS POR PATH
// ============================================================================

async function analyzeYearForPath(basePath, label) {
  const months = [];
  
  // Obtener todos los meses de 2025
  for (let month = 1; month <= 12; month++) {
    const monthKey = `${targetYear}-${month.toString().padStart(2, '0')}`;
    const monthData = await getMonthData(basePath, monthKey);
    
    if (monthData) {
      months.push(monthData);
    }
  }
  
  if (months.length === 0) {
    console.log(`   ⚠️  Sin datos para ${targetYear}`);
    return;
  }
  
  console.log(`\n   📅 Meses con datos: ${months.length}/12\n`);
  
  // Tabla de meses
  console.log('   ' + '─'.repeat(74));
  console.log('   │ Mes     │ Docs │ Factor TWR │ Return %  │ Valor Inicio  │ Valor Final   │');
  console.log('   ' + '─'.repeat(74));
  
  for (const month of months) {
    const usd = month.consolidated.USD || {};
    const factor = (usd.endFactor || 1).toFixed(6);
    const ret = (usd.periodReturn || 0).toFixed(2);
    const startVal = formatCurrency(usd.startTotalValue || 0);
    const endVal = formatCurrency(usd.endTotalValue || 0);
    
    console.log(`   │ ${month.periodKey} │ ${String(month.docsCount).padStart(4)} │ ${factor.padStart(10)} │ ${ret.padStart(8)}% │ ${startVal.padStart(13)} │ ${endVal.padStart(13)} │`);
  }
  
  console.log('   ' + '─'.repeat(74));
  
  // Consolidar año completo
  const mockMonthlyDocs = months.map(m => ({
    id: m.periodKey,
    data: () => m.consolidated
  }));
  
  const yearConsolidated = consolidateMonthsToYear(mockMonthlyDocs, targetYear);
  
  if (!yearConsolidated) {
    console.log(`\n   ⚠️  No se pudo consolidar el año`);
    return;
  }
  
  // Mostrar resumen anual por moneda
  console.log(`\n   📆 RESUMEN ANUAL ${targetYear}:`);
  console.log('   ' + '─'.repeat(74));
  
  for (const curr of currencies) {
    const data = yearConsolidated[curr];
    if (!data) continue;
    
    console.log(`\n   💰 ${curr}:`);
    console.log(`      Factor TWR Compuesto:   ${data.endFactor.toFixed(6)}`);
    console.log(`      Rendimiento Período:    ${data.periodReturn.toFixed(4)}%`);
    console.log(`      Personal Return (MWR):  ${(data.personalReturn || 0).toFixed(4)}%`);
    console.log(`      Valor Inicial:          ${formatCurrency(data.startTotalValue)}`);
    console.log(`      Valor Final:            ${formatCurrency(data.endTotalValue)}`);
    console.log(`      Total CashFlow:         ${formatCurrency(data.totalCashFlow)}`);
    console.log(`      Meses Válidos:          ${data.validDocsCount || months.length}`);
  }
  
  // Mostrar estructura del documento que se guardaría
  console.log(`\n   📄 DOCUMENTO A GUARDAR:`);
  console.log(`      Path: ${basePath}/consolidatedPeriods/yearly/periods/${targetYear}`);
  console.log(`      periodType: ${yearConsolidated.periodType}`);
  console.log(`      periodKey: ${yearConsolidated.periodKey}`);
  console.log(`      startDate: ${yearConsolidated.startDate}`);
  console.log(`      endDate: ${yearConsolidated.endDate}`);
  console.log(`      docsCount: ${yearConsolidated.docsCount}`);
  console.log(`      version: ${yearConsolidated.version}`);
}

// ============================================================================
// OBTENER DATOS DE UN MES
// ============================================================================

async function getMonthData(basePath, monthKey) {
  const [year, month] = monthKey.split('-').map(Number);
  const periodStart = DateTime.fromObject({ year, month, day: 1 }).startOf('month').toISODate();
  const periodEnd = DateTime.fromObject({ year, month, day: 1 }).endOf('month').toISODate();
  
  // Leer documentos diarios del mes
  const snapshot = await db.collection(`${basePath}/dates`)
    .where('date', '>=', periodStart)
    .where('date', '<=', periodEnd)
    .orderBy('date', 'asc')
    .get();
  
  if (snapshot.empty) {
    return null;
  }
  
  // Consolidar el mes
  const consolidated = consolidatePeriod(snapshot.docs, monthKey, 'month');
  
  if (!consolidated) {
    return null;
  }
  
  return {
    periodKey: monthKey,
    docsCount: snapshot.size,
    consolidated
  };
}

// ============================================================================
// UTILIDADES
// ============================================================================

function formatCurrency(value) {
  if (value === undefined || value === null) return 'N/A';
  if (Math.abs(value) >= 1000000) {
    return `$${(value / 1000000).toFixed(2)}M`;
  } else if (Math.abs(value) >= 1000) {
    return `$${(value / 1000).toFixed(2)}K`;
  }
  return `$${value.toFixed(2)}`;
}

// ============================================================================
// EJECUCIÓN
// ============================================================================

main().then(() => {
  process.exit(0);
}).catch((error) => {
  console.error('Error fatal:', error);
  process.exit(1);
});
