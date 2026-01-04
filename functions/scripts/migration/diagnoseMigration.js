#!/usr/bin/env node
/**
 * Script de Diagnóstico: Verificación Exhaustiva Post-Migración
 * 
 * COST-OPT-003: Diagnóstico profundo de la estructura de datos consolidados
 * para identificar problemas en la migración.
 * 
 * Verificaciones:
 * 1. Integridad estructural de documentos consolidados
 * 2. Coherencia de factores TWR (encadenamiento correcto)
 * 3. Consistencia de valores monetarios
 * 4. Cobertura temporal (sin gaps en meses/años)
 * 5. Coherencia entre overall y cuentas individuales
 * 
 * Uso:
 *   node diagnoseMigration.js
 *   node diagnoseMigration.js --user-id=abc123
 *   node diagnoseMigration.js --fix    # Intenta reparar problemas
 *   node diagnoseMigration.js --detailed
 * 
 * @module diagnoseMigration
 * @see docs/stories/64.story.md (COST-OPT-003)
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');
const { program } = require('commander');
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

// Importar constantes
const { CONSOLIDATED_SCHEMA_VERSION } = require('../../utils/periodConsolidation');

// ============================================================================
// CONFIGURACIÓN CLI
// ============================================================================

program
  .name('diagnoseMigration')
  .description('Diagnóstico exhaustivo de la migración de períodos consolidados')
  .option('--user-id <userId>', 'Diagnosticar usuario específico')
  .option('--sample-size <size>', 'Número de usuarios a diagnosticar', '10')
  .option('--all', 'Diagnosticar todos los usuarios')
  .option('--fix', 'Intentar reparar problemas encontrados')
  .option('--detailed', 'Mostrar información detallada de cada verificación')
  .option('--export', 'Exportar reporte JSON')
  .option('--currency <code>', 'Moneda a verificar', 'USD')
  .parse(process.argv);

const options = program.opts();
const currency = options.currency;

// ============================================================================
// TIPOS DE PROBLEMAS
// ============================================================================

const IssueType = {
  // Estructurales
  MISSING_FIELD: 'MISSING_FIELD',
  INVALID_VERSION: 'INVALID_VERSION',
  INVALID_DATE_FORMAT: 'INVALID_DATE_FORMAT',
  
  // Coherencia de datos
  NEGATIVE_FACTOR: 'NEGATIVE_FACTOR',
  ZERO_FACTOR: 'ZERO_FACTOR',
  EXTREME_RETURN: 'EXTREME_RETURN',
  INCONSISTENT_VALUES: 'INCONSISTENT_VALUES',
  
  // Cobertura temporal
  GAP_IN_MONTHS: 'GAP_IN_MONTHS',
  MISSING_YEAR: 'MISSING_YEAR',
  ORPHAN_MONTH: 'ORPHAN_MONTH',
  
  // Coherencia multi-nivel
  OVERALL_ACCOUNT_MISMATCH: 'OVERALL_ACCOUNT_MISMATCH',
  MONTHLY_YEARLY_MISMATCH: 'MONTHLY_YEARLY_MISMATCH',
  
  // Datos
  MISSING_CURRENCY: 'MISSING_CURRENCY',
  NAN_VALUE: 'NAN_VALUE',
  INFINITY_VALUE: 'INFINITY_VALUE'
};

const IssueSeverity = {
  ERROR: 'ERROR',     // Debe repararse
  WARNING: 'WARNING', // Revisar manualmente
  INFO: 'INFO'        // Informativo
};

// ============================================================================
// RESULTADOS GLOBALES
// ============================================================================

const results = {
  usersChecked: 0,
  usersWithIssues: 0,
  totalIssues: 0,
  issuesByType: {},
  issuesBySeverity: {
    [IssueSeverity.ERROR]: 0,
    [IssueSeverity.WARNING]: 0,
    [IssueSeverity.INFO]: 0
  },
  issuesFixed: 0,
  userDetails: [],
  startTime: Date.now()
};

// Inicializar contadores por tipo
Object.values(IssueType).forEach(type => {
  results.issuesByType[type] = 0;
});

// ============================================================================
// FUNCIÓN PRINCIPAL
// ============================================================================

async function main() {
  console.log('\n' + '='.repeat(60));
  console.log('  DIAGNÓSTICO POST-MIGRACIÓN - COST-OPT-003');
  console.log('='.repeat(60) + '\n');
  
  console.log(`📋 Configuración:`);
  console.log(`   Moneda: ${currency}`);
  console.log(`   Schema version esperada: ${CONSOLIDATED_SCHEMA_VERSION}`);
  console.log(`   Modo: ${options.fix ? '🔧 REPARAR' : '🔍 SOLO DIAGNÓSTICO'}`);
  console.log('');
  
  try {
    // Obtener usuarios
    const users = await getUsersToDiagnose();
    
    if (users.length === 0) {
      console.log('⚠️  No se encontraron usuarios para diagnosticar.');
      process.exit(0);
    }
    
    console.log(`\n🔍 Diagnosticando ${users.length} usuario(s)...\n`);
    console.log('-'.repeat(60) + '\n');
    
    // Diagnosticar cada usuario
    for (let i = 0; i < users.length; i++) {
      const userId = users[i];
      console.log(`[${i + 1}/${users.length}] 👤 ${userId}`);
      
      await diagnoseUser(userId);
    }
    
    // Imprimir resumen
    printSummary();
    
    // Exportar reporte
    if (options.export) {
      exportReport();
    }
    
  } catch (error) {
    console.error('❌ Error fatal:', error);
    process.exit(1);
  }
}

// ============================================================================
// OBTENCIÓN DE USUARIOS
// ============================================================================

async function getUsersToDiagnose() {
  if (options.userId) {
    return [options.userId];
  }
  
  const performanceSnapshot = await db.collection('portfolioPerformance').get();
  const allUsers = performanceSnapshot.docs.map(d => d.id);
  
  if (options.all) {
    return allUsers;
  }
  
  // Muestra aleatoria
  const sampleSize = Math.min(parseInt(options.sampleSize), allUsers.length);
  const shuffled = allUsers.sort(() => 0.5 - Math.random());
  return shuffled.slice(0, sampleSize);
}

// ============================================================================
// DIAGNÓSTICO DE USUARIO
// ============================================================================

async function diagnoseUser(userId) {
  const userResult = {
    userId,
    issues: [],
    checksPerformed: 0,
    monthlyDocs: 0,
    yearlyDocs: 0,
    accounts: []
  };
  
  try {
    const basePath = `portfolioPerformance/${userId}`;
    
    // 1. Verificar estructura de documentos mensuales
    await checkMonthlyStructure(userId, basePath, userResult);
    
    // 2. Verificar estructura de documentos anuales
    await checkYearlyStructure(userId, basePath, userResult);
    
    // 3. Verificar coherencia de factores
    await checkFactorCoherence(userId, basePath, userResult);
    
    // 4. Verificar cobertura temporal
    await checkTemporalCoverage(userId, basePath, userResult);
    
    // 5. Verificar coherencia mensual vs anual
    await checkMonthlyYearlyCoherence(userId, basePath, userResult);
    
    // 6. Verificar cuentas individuales
    await checkAccountsConsistency(userId, basePath, userResult);
    
    // Actualizar métricas
    results.usersChecked++;
    
    if (userResult.issues.length > 0) {
      results.usersWithIssues++;
      results.totalIssues += userResult.issues.length;
      
      // Contar por tipo y severidad
      userResult.issues.forEach(issue => {
        results.issuesByType[issue.type]++;
        results.issuesBySeverity[issue.severity]++;
      });
      
      // Mostrar resumen de issues
      const errorCount = userResult.issues.filter(i => i.severity === IssueSeverity.ERROR).length;
      const warningCount = userResult.issues.filter(i => i.severity === IssueSeverity.WARNING).length;
      
      if (errorCount > 0) {
        console.log(`   ❌ ${errorCount} errores, ${warningCount} warnings`);
      } else if (warningCount > 0) {
        console.log(`   ⚠️  ${warningCount} warnings`);
      }
      
      if (options.detailed) {
        userResult.issues.slice(0, 5).forEach(issue => {
          const icon = issue.severity === IssueSeverity.ERROR ? '❌' : '⚠️';
          console.log(`      ${icon} ${issue.type}: ${issue.message}`);
        });
        if (userResult.issues.length > 5) {
          console.log(`      ... y ${userResult.issues.length - 5} issues más`);
        }
      }
      
      // Intentar reparar si está habilitado
      if (options.fix) {
        await attemptRepairs(userId, basePath, userResult);
      }
      
    } else {
      console.log(`   ✅ Sin problemas (${userResult.checksPerformed} verificaciones)`);
    }
    
    results.userDetails.push(userResult);
    
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    userResult.issues.push({
      type: 'DIAGNOSTIC_ERROR',
      severity: IssueSeverity.ERROR,
      message: error.message
    });
    results.userDetails.push(userResult);
  }
}

// ============================================================================
// VERIFICACIONES ESPECÍFICAS
// ============================================================================

/**
 * 1. Verificar estructura de documentos mensuales
 */
async function checkMonthlyStructure(userId, basePath, userResult) {
  const monthlyPath = `${basePath}/consolidatedPeriods/monthly/periods`;
  const snapshot = await db.collection(monthlyPath).get();
  
  userResult.monthlyDocs = snapshot.size;
  userResult.checksPerformed++;
  
  for (const doc of snapshot.docs) {
    const data = doc.data();
    const periodKey = doc.id;
    
    // Verificar campos requeridos
    const requiredFields = ['periodType', 'periodKey', 'startDate', 'endDate', 'docsCount', 'version'];
    for (const field of requiredFields) {
      if (data[field] === undefined) {
        userResult.issues.push({
          type: IssueType.MISSING_FIELD,
          severity: IssueSeverity.ERROR,
          message: `Documento mensual ${periodKey} sin campo '${field}'`,
          doc: periodKey,
          field
        });
      }
    }
    
    // Verificar versión
    if (data.version !== CONSOLIDATED_SCHEMA_VERSION) {
      userResult.issues.push({
        type: IssueType.INVALID_VERSION,
        severity: IssueSeverity.WARNING,
        message: `Documento ${periodKey} con versión ${data.version}, esperada ${CONSOLIDATED_SCHEMA_VERSION}`,
        doc: periodKey
      });
    }
    
    // Verificar datos de moneda
    const currencyData = data[currency];
    if (currencyData) {
      checkCurrencyData(currencyData, periodKey, 'monthly', userResult);
    } else {
      userResult.issues.push({
        type: IssueType.MISSING_CURRENCY,
        severity: IssueSeverity.WARNING,
        message: `Documento ${periodKey} sin datos para ${currency}`,
        doc: periodKey
      });
    }
    
    userResult.checksPerformed++;
  }
}

/**
 * 2. Verificar estructura de documentos anuales
 */
async function checkYearlyStructure(userId, basePath, userResult) {
  const yearlyPath = `${basePath}/consolidatedPeriods/yearly/periods`;
  const snapshot = await db.collection(yearlyPath).get();
  
  userResult.yearlyDocs = snapshot.size;
  userResult.checksPerformed++;
  
  for (const doc of snapshot.docs) {
    const data = doc.data();
    const yearKey = doc.id;
    
    // Verificar campos requeridos
    const requiredFields = ['periodType', 'periodKey', 'startDate', 'endDate', 'docsCount', 'version'];
    for (const field of requiredFields) {
      if (data[field] === undefined) {
        userResult.issues.push({
          type: IssueType.MISSING_FIELD,
          severity: IssueSeverity.ERROR,
          message: `Documento anual ${yearKey} sin campo '${field}'`,
          doc: yearKey,
          field
        });
      }
    }
    
    // Verificar datos de moneda
    const currencyData = data[currency];
    if (currencyData) {
      checkCurrencyData(currencyData, yearKey, 'yearly', userResult);
    }
    
    userResult.checksPerformed++;
  }
}

/**
 * Verificar datos de moneda en documento consolidado
 */
function checkCurrencyData(currencyData, periodKey, periodType, userResult) {
  // Verificar factor no negativo
  if (currencyData.endFactor < 0) {
    userResult.issues.push({
      type: IssueType.NEGATIVE_FACTOR,
      severity: IssueSeverity.ERROR,
      message: `${periodType} ${periodKey}: endFactor negativo (${currencyData.endFactor})`,
      doc: periodKey
    });
  }
  
  // Verificar factor no cero
  if (currencyData.endFactor === 0) {
    userResult.issues.push({
      type: IssueType.ZERO_FACTOR,
      severity: IssueSeverity.ERROR,
      message: `${periodType} ${periodKey}: endFactor es 0`,
      doc: periodKey
    });
  }
  
  // Verificar rendimiento extremo (> 500% o < -90%)
  const periodReturn = currencyData.periodReturn || 0;
  if (periodReturn > 500 || periodReturn < -90) {
    userResult.issues.push({
      type: IssueType.EXTREME_RETURN,
      severity: IssueSeverity.WARNING,
      message: `${periodType} ${periodKey}: rendimiento extremo (${periodReturn.toFixed(2)}%)`,
      doc: periodKey
    });
  }
  
  // Verificar NaN
  const fieldsToCheck = ['startFactor', 'endFactor', 'periodReturn', 'startTotalValue', 'endTotalValue'];
  for (const field of fieldsToCheck) {
    if (currencyData[field] !== undefined && isNaN(currencyData[field])) {
      userResult.issues.push({
        type: IssueType.NAN_VALUE,
        severity: IssueSeverity.ERROR,
        message: `${periodType} ${periodKey}: ${field} es NaN`,
        doc: periodKey,
        field
      });
    }
    if (currencyData[field] === Infinity || currencyData[field] === -Infinity) {
      userResult.issues.push({
        type: IssueType.INFINITY_VALUE,
        severity: IssueSeverity.ERROR,
        message: `${periodType} ${periodKey}: ${field} es Infinity`,
        doc: periodKey,
        field
      });
    }
  }
  
  userResult.checksPerformed++;
}

/**
 * 3. Verificar coherencia de factores encadenados
 */
async function checkFactorCoherence(userId, basePath, userResult) {
  const monthlyPath = `${basePath}/consolidatedPeriods/monthly/periods`;
  const snapshot = await db.collection(monthlyPath)
    .orderBy('periodKey', 'asc')
    .get();
  
  if (snapshot.size < 2) return;
  
  let prevDoc = null;
  
  for (const doc of snapshot.docs) {
    const data = doc.data();
    const currencyData = data[currency];
    
    if (!currencyData) continue;
    
    if (prevDoc) {
      const prevData = prevDoc.data()[currency];
      
      // Verificar que endTotalValue del mes anterior ≈ startTotalValue del mes actual
      // (con tolerancia para cashflows)
      if (prevData && prevData.endTotalValue && currencyData.startTotalValue) {
        const diff = Math.abs(prevData.endTotalValue - currencyData.startTotalValue);
        const pctDiff = prevData.endTotalValue > 0 
          ? (diff / prevData.endTotalValue) * 100 
          : 0;
        
        // Solo warning si la diferencia es muy grande (> 50%)
        if (pctDiff > 50 && Math.abs(currencyData.totalCashFlow || 0) < diff * 0.5) {
          userResult.issues.push({
            type: IssueType.INCONSISTENT_VALUES,
            severity: IssueSeverity.WARNING,
            message: `Salto de valor entre ${prevDoc.id} y ${doc.id}: ${pctDiff.toFixed(1)}%`,
            doc: doc.id
          });
        }
      }
    }
    
    prevDoc = doc;
    userResult.checksPerformed++;
  }
}

/**
 * 4. Verificar cobertura temporal (sin gaps)
 */
async function checkTemporalCoverage(userId, basePath, userResult) {
  const monthlyPath = `${basePath}/consolidatedPeriods/monthly/periods`;
  const snapshot = await db.collection(monthlyPath)
    .orderBy('periodKey', 'asc')
    .get();
  
  if (snapshot.size === 0) return;
  
  const months = snapshot.docs.map(d => d.id);
  const now = DateTime.now().setZone('America/New_York');
  const currentMonth = now.toFormat('yyyy-MM');
  
  // Verificar gaps entre meses consecutivos
  for (let i = 1; i < months.length; i++) {
    const prevMonth = months[i - 1];
    const currMonth = months[i];
    
    const prevDt = DateTime.fromFormat(prevMonth, 'yyyy-MM');
    const expectedNext = prevDt.plus({ months: 1 }).toFormat('yyyy-MM');
    
    if (currMonth !== expectedNext && expectedNext < currentMonth) {
      userResult.issues.push({
        type: IssueType.GAP_IN_MONTHS,
        severity: IssueSeverity.WARNING,
        message: `Gap entre ${prevMonth} y ${currMonth}, falta ${expectedNext}`,
        missingMonth: expectedNext
      });
    }
    
    userResult.checksPerformed++;
  }
}

/**
 * 5. Verificar coherencia mensual vs anual
 */
async function checkMonthlyYearlyCoherence(userId, basePath, userResult) {
  const yearlyPath = `${basePath}/consolidatedPeriods/yearly/periods`;
  const yearlySnapshot = await db.collection(yearlyPath).get();
  
  for (const yearDoc of yearlySnapshot.docs) {
    const yearKey = yearDoc.id;
    const yearData = yearDoc.data();
    const yearCurrencyData = yearData[currency];
    
    if (!yearCurrencyData) continue;
    
    // Obtener meses del año
    const monthlyPath = `${basePath}/consolidatedPeriods/monthly/periods`;
    const monthlySnapshot = await db.collection(monthlyPath)
      .where('periodKey', '>=', `${yearKey}-01`)
      .where('periodKey', '<=', `${yearKey}-12`)
      .get();
    
    if (monthlySnapshot.empty) continue;
    
    // Calcular factor compuesto de los meses
    let compoundFactor = 1;
    monthlySnapshot.docs.forEach(monthDoc => {
      const monthData = monthDoc.data()[currency];
      if (monthData && monthData.endFactor && monthData.startFactor) {
        compoundFactor *= (monthData.endFactor / monthData.startFactor);
      }
    });
    
    // Comparar con factor anual
    const yearFactor = yearCurrencyData.endFactor || 1;
    const diff = Math.abs(compoundFactor - yearFactor);
    const pctDiff = yearFactor > 0 ? (diff / yearFactor) * 100 : 0;
    
    if (pctDiff > 1) { // Tolerancia de 1%
      userResult.issues.push({
        type: IssueType.MONTHLY_YEARLY_MISMATCH,
        severity: IssueSeverity.WARNING,
        message: `Año ${yearKey}: factor mensual compuesto (${compoundFactor.toFixed(4)}) vs anual (${yearFactor.toFixed(4)}), diff=${pctDiff.toFixed(2)}%`,
        year: yearKey
      });
    }
    
    userResult.checksPerformed++;
  }
}

/**
 * 6. Verificar cuentas individuales
 */
async function checkAccountsConsistency(userId, basePath, userResult) {
  const accountsSnapshot = await db.collection(`${basePath}/accounts`).get();
  userResult.accounts = accountsSnapshot.docs.map(d => d.id);
  
  for (const accountDoc of accountsSnapshot.docs) {
    const accountId = accountDoc.id;
    const accountPath = `${basePath}/accounts/${accountId}`;
    
    // Verificar que tiene consolidados
    const monthlySnapshot = await db.collection(`${accountPath}/consolidatedPeriods/monthly/periods`)
      .limit(1)
      .get();
    
    if (monthlySnapshot.empty) {
      userResult.issues.push({
        type: IssueType.ORPHAN_MONTH,
        severity: IssueSeverity.INFO,
        message: `Cuenta ${accountId} sin datos consolidados`,
        account: accountId
      });
    }
    
    userResult.checksPerformed++;
  }
}

// ============================================================================
// REPARACIONES
// ============================================================================

async function attemptRepairs(userId, basePath, userResult) {
  const repairableIssues = userResult.issues.filter(i => 
    i.severity === IssueSeverity.ERROR && 
    [IssueType.MISSING_FIELD, IssueType.INVALID_VERSION].includes(i.type)
  );
  
  if (repairableIssues.length === 0) {
    return;
  }
  
  console.log(`   🔧 Intentando reparar ${repairableIssues.length} issues...`);
  
  // Por ahora solo marcamos - la reparación real requeriría re-consolidar
  // los documentos afectados usando migrateHistoricalPeriods.js --execute
  
  console.log(`   💡 Ejecute: node migrateHistoricalPeriods.js --execute --user-id=${userId}`);
}

// ============================================================================
// RESUMEN Y REPORTE
// ============================================================================

function printSummary() {
  const duration = ((Date.now() - results.startTime) / 1000).toFixed(1);
  
  console.log('\n' + '='.repeat(60));
  console.log('  RESUMEN DE DIAGNÓSTICO');
  console.log('='.repeat(60) + '\n');
  
  console.log(`  📊 RESULTADOS:`);
  console.log(`     Usuarios verificados: ${results.usersChecked}`);
  console.log(`     Usuarios con issues:  ${results.usersWithIssues}`);
  console.log(`     Total de issues:      ${results.totalIssues}`);
  console.log(`     Duración:             ${duration}s`);
  console.log('');
  
  console.log(`  🔴 POR SEVERIDAD:`);
  console.log(`     ❌ Errores:   ${results.issuesBySeverity[IssueSeverity.ERROR]}`);
  console.log(`     ⚠️  Warnings:  ${results.issuesBySeverity[IssueSeverity.WARNING]}`);
  console.log(`     ℹ️  Info:      ${results.issuesBySeverity[IssueSeverity.INFO]}`);
  console.log('');
  
  // Top issues por tipo
  const sortedTypes = Object.entries(results.issuesByType)
    .filter(([_, count]) => count > 0)
    .sort((a, b) => b[1] - a[1]);
  
  if (sortedTypes.length > 0) {
    console.log(`  📋 POR TIPO:`);
    sortedTypes.slice(0, 10).forEach(([type, count]) => {
      console.log(`     ${type.padEnd(30)} ${count}`);
    });
  }
  
  // Recomendaciones
  console.log('\n' + '-'.repeat(60));
  
  if (results.issuesBySeverity[IssueSeverity.ERROR] > 0) {
    console.log('\n  ❌ ACCIÓN REQUERIDA:');
    console.log('     Hay errores que deben ser corregidos.');
    console.log('     Ejecute: node migrateHistoricalPeriods.js --execute [--user-id=xxx]');
  } else if (results.totalIssues === 0) {
    console.log('\n  ✅ ESTADO ÓPTIMO');
    console.log('     Todos los datos consolidados están correctos.');
  } else {
    console.log('\n  ⚠️  REVISAR MANUALMENTE');
    console.log('     Hay warnings que pueden requerir atención.');
  }
  
  console.log('\n' + '='.repeat(60) + '\n');
}

function exportReport() {
  const reportPath = path.join(__dirname, `diagnostic-report-${Date.now()}.json`);
  
  const report = {
    timestamp: new Date().toISOString(),
    config: {
      currency,
      fix: options.fix,
      userId: options.userId
    },
    summary: {
      usersChecked: results.usersChecked,
      usersWithIssues: results.usersWithIssues,
      totalIssues: results.totalIssues
    },
    issuesBySeverity: results.issuesBySeverity,
    issuesByType: results.issuesByType,
    userDetails: results.userDetails
  };
  
  require('fs').writeFileSync(reportPath, JSON.stringify(report, null, 2));
  console.log(`  📄 Reporte exportado: ${reportPath}\n`);
}

// ============================================================================
// EJECUCIÓN
// ============================================================================

main().then(() => {
  const hasErrors = results.issuesBySeverity[IssueSeverity.ERROR] > 0;
  process.exit(hasErrors ? 1 : 0);
}).catch((error) => {
  console.error('Error fatal:', error);
  process.exit(1);
});
