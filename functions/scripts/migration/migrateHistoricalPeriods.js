#!/usr/bin/env node
/**
 * OBSOLETO (HU 6.13): los consolidados de meses y años (`consolidatedPeriods`)
 * dejaron de escribirse y de leerse con `RETIRE_CONSOLIDATED_PERIODS` encendido.
 * Este script solo existe para ellos y no se debe usar. Para volver atrás se usa
 * `node scripts/ruta-antigua-consolidados.js rehacer` (raíz del repositorio).
 * @see platform-docs/stories/6.13-retirar-ruta-antigua-consolidados/refinamiento.md (T4)
 *
 * Script de Migración: Consolidación de Períodos Históricos
 * 
 * COST-OPT-003: Migra los datos existentes en portfolioPerformance/{userId}/dates
 * a la nueva estructura de consolidatedPeriods/monthly y yearly.
 * 
 * Uso:
 *   node migrateHistoricalPeriods.js --dry-run
 *   node migrateHistoricalPeriods.js --execute
 *   node migrateHistoricalPeriods.js --execute --user-id=abc123
 *   node migrateHistoricalPeriods.js --execute --start-year=2020 --end-year=2025
 * 
 * Principios aplicados:
 * - SRP: Solo migración, validación en script separado
 * - DRY: Reutiliza funciones de COST-OPT-001 y COST-OPT-002
 * - KISS: Procesamiento secuencial con progress visible
 * 
 * @module migrateHistoricalPeriods
 * @see docs/stories/64.story.md (COST-OPT-003)
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');
const { program } = require('commander');
const path = require('path');

// Inicializar Firebase Admin (usar credenciales de servicio)
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
// CONFIGURACIÓN CLI
// ============================================================================

program
  .name('migrateHistoricalPeriods')
  .description('Migra datos históricos a estructura de períodos consolidados')
  .option('--dry-run', 'Simula la migración sin escribir datos (default)', false)
  .option('--execute', 'Ejecuta la migración real escribiendo datos')
  .option('--user-id <userId>', 'Migrar solo un usuario específico')
  .option('--start-year <year>', 'Año de inicio de migración', '2020')
  .option('--end-year <year>', 'Año de fin (año actual por defecto)')
  .option('--skip-monthly', 'Omitir consolidación mensual')
  .option('--skip-yearly', 'Omitir consolidación anual')
  .option('--batch-size <size>', 'Tamaño de batch para escrituras', '500')
  .option('--verbose', 'Muestra logs detallados', false)
  .option('--include-accounts', 'Incluir cuentas individuales además de overall', true)
  .option('--throttle <ms>', 'Delay entre usuarios para evitar rate limits', '100')
  .parse(process.argv);

const options = program.opts();
const isDryRun = !options.execute;
const currentYear = DateTime.now().setZone('America/New_York').year;
const startYear = parseInt(options.startYear);
const endYear = parseInt(options.endYear || currentYear);
const batchSize = parseInt(options.batchSize);
const throttleMs = parseInt(options.throttle || '100');

// ============================================================================
// MÉTRICAS GLOBALES
// ============================================================================

const metrics = {
  usersAnalyzed: 0,
  usersProcessed: 0,
  usersSkipped: 0,
  accountsProcessed: 0,
  monthsConsolidated: 0,
  monthsSkipped: 0,
  yearsConsolidated: 0,
  yearsSkipped: 0,
  documentsWritten: 0,
  documentsRead: 0,
  errors: [],
  warnings: [],
  startTime: Date.now(),
  userDetails: []
};

// ============================================================================
// FUNCIÓN PRINCIPAL
// ============================================================================

/**
 * Función principal de migración
 */
async function main() {
  console.log('\n' + '='.repeat(60));
  console.log('  MIGRACIÓN DE PERÍODOS HISTÓRICOS - COST-OPT-003');
  console.log('='.repeat(60) + '\n');
  
  console.log(`📋 Configuración:`);
  console.log(`   Modo: ${isDryRun ? '🔍 DRY-RUN (simulación)' : '⚡ EXECUTE (escritura real)'}`);
  console.log(`   Rango de años: ${startYear} - ${endYear - 1}`);
  console.log(`   Batch size: ${batchSize}`);
  console.log(`   Throttle: ${throttleMs}ms`);
  console.log(`   Schema version: ${CONSOLIDATED_SCHEMA_VERSION}`);
  
  if (options.userId) {
    console.log(`   Usuario específico: ${options.userId}`);
  }
  if (options.skipMonthly) {
    console.log(`   ⏭️  Omitiendo consolidación mensual`);
  }
  if (options.skipYearly) {
    console.log(`   ⏭️  Omitiendo consolidación anual`);
  }
  
  console.log('\n' + '-'.repeat(60) + '\n');
  
  try {
    // FASE 1: Análisis
    console.log('📊 FASE 1: Analizando usuarios...\n');
    const users = await analyzeUsers();
    
    if (users.length === 0) {
      console.log('⚠️  No se encontraron usuarios para migrar.');
      return;
    }
    
    // Confirmar antes de ejecutar
    if (!isDryRun) {
      console.log('\n⚠️  ATENCIÓN: Se escribirán datos en Firestore.');
      console.log('   Presiona Ctrl+C en los próximos 5 segundos para cancelar...\n');
      await sleep(5000);
    }
    
    // FASE 2: Consolidación mensual
    if (!options.skipMonthly) {
      console.log('\n📅 FASE 2: Consolidando meses...\n');
      await processMonthlyConsolidation(users);
    }
    
    // FASE 3: Consolidación anual
    if (!options.skipYearly) {
      console.log('\n📆 FASE 3: Consolidando años...\n');
      await processYearlyConsolidation(users);
    }
    
    // Resumen final
    printSummary();
    
  } catch (error) {
    console.error('\n❌ Error fatal:', error.message);
    if (options.verbose) {
      console.error(error.stack);
    }
    metrics.errors.push({ phase: 'main', error: error.message });
    printSummary();
    process.exit(1);
  }
}

// ============================================================================
// FASE 1: ANÁLISIS
// ============================================================================

/**
 * Analiza usuarios y detecta rango de datos
 * 
 * @returns {Promise<Array>} Lista de usuarios con metadata
 */
async function analyzeUsers() {
  const users = [];
  
  if (options.userId) {
    // Verificar que el usuario existe
    const userDoc = await db.doc(`portfolioPerformance/${options.userId}`).get();
    if (!userDoc.exists) {
      console.log(`❌ Usuario ${options.userId} no encontrado`);
      return [];
    }
    
    const userData = await analyzeUser(options.userId);
    if (userData) {
      users.push(userData);
    }
  } else {
    // Obtener todos los usuarios
    const usersSnapshot = await db.collection('portfolioPerformance').get();
    console.log(`   Encontrados ${usersSnapshot.size} usuarios en portfolioPerformance`);
    
    let analyzed = 0;
    for (const userDoc of usersSnapshot.docs) {
      const userId = userDoc.id;
      
      try {
        const userData = await analyzeUser(userId);
        if (userData) {
          users.push(userData);
        }
        analyzed++;
        
        // Progress cada 10 usuarios
        if (analyzed % 10 === 0) {
          process.stdout.write(`   Analizados: ${analyzed}/${usersSnapshot.size}\r`);
        }
      } catch (error) {
        metrics.warnings.push({ userId, phase: 'analyze', error: error.message });
        if (options.verbose) {
          console.log(`   ⚠️  Error analizando ${userId}: ${error.message}`);
        }
      }
    }
    console.log('');
  }
  
  // Estadísticas del análisis
  metrics.usersAnalyzed = users.length;
  
  const totalMonths = users.reduce((sum, u) => sum + u.closedMonths.length, 0);
  const totalYears = users.reduce((sum, u) => sum + u.closedYears.length, 0);
  const totalAccounts = users.reduce((sum, u) => sum + u.accounts.length, 0);
  
  console.log(`\n   ✅ ${users.length} usuarios con datos válidos`);
  console.log(`   📅 ${totalMonths} meses cerrados a consolidar (overall)`);
  console.log(`   📆 ${totalYears} años cerrados a consolidar (overall)`);
  console.log(`   👤 ${totalAccounts} cuentas individuales detectadas`);
  
  // Mostrar rango de fechas
  const allFirstDates = users.map(u => u.firstDate).filter(Boolean).sort();
  const allLastDates = users.map(u => u.lastDate).filter(Boolean).sort();
  
  if (allFirstDates.length > 0) {
    console.log(`   📊 Rango de datos: ${allFirstDates[0]} a ${allLastDates[allLastDates.length - 1]}`);
  }
  
  return users;
}

/**
 * Analiza un usuario específico
 * 
 * @param {string} userId - ID del usuario
 * @returns {Promise<Object|null>} Metadata del usuario o null si no hay datos
 */
async function analyzeUser(userId) {
  const basePath = `portfolioPerformance/${userId}`;
  
  // Obtener primera y última fecha de datos
  const [firstDocSnapshot, lastDocSnapshot] = await Promise.all([
    db.collection(`${basePath}/dates`).orderBy('date', 'asc').limit(1).get(),
    db.collection(`${basePath}/dates`).orderBy('date', 'desc').limit(1).get()
  ]);
  
  metrics.documentsRead += 2;
  
  if (firstDocSnapshot.empty || lastDocSnapshot.empty) {
    if (options.verbose) {
      console.log(`   ⏭️  ${userId}: Sin documentos en dates/`);
    }
    metrics.usersSkipped++;
    return null;
  }
  
  const firstDate = firstDocSnapshot.docs[0].data().date;
  const lastDate = lastDocSnapshot.docs[0].data().date;
  
  // Obtener cuentas del usuario
  const accountsSnapshot = await db.collection(`${basePath}/accounts`).get();
  const accounts = accountsSnapshot.docs.map(doc => doc.id);
  metrics.documentsRead++;
  
  // Verificar qué meses/años ya están consolidados
  const [existingMonthlySnapshot, existingYearlySnapshot] = await Promise.all([
    db.collection(`${basePath}/consolidatedPeriods/monthly/periods`).get(),
    db.collection(`${basePath}/consolidatedPeriods/yearly/periods`).get()
  ]);
  
  const existingMonths = new Set(existingMonthlySnapshot.docs.map(d => d.id));
  const existingYears = new Set(existingYearlySnapshot.docs.map(d => d.id));
  metrics.documentsRead += 2;
  
  // Calcular meses y años cerrados dentro del rango especificado
  const now = DateTime.now().setZone('America/New_York');
  const currentMonth = now.toFormat('yyyy-MM');
  const closedMonths = [];
  const closedYears = [];
  
  const dataStart = DateTime.fromISO(firstDate);
  const effectiveStartYear = Math.max(dataStart.year, startYear);
  
  // Iterar por meses cerrados
  for (let year = effectiveStartYear; year < endYear; year++) {
    const monthStart = year === dataStart.year ? dataStart.month : 1;
    
    for (let month = monthStart; month <= 12; month++) {
      const monthKey = `${year}-${month.toString().padStart(2, '0')}`;
      
      // Solo incluir si está cerrado y no existe ya
      if (monthKey < currentMonth) {
        if (!existingMonths.has(monthKey)) {
          closedMonths.push(monthKey);
        } else {
          metrics.monthsSkipped++;
        }
      }
    }
  }
  
  // Iterar por años cerrados
  for (let year = effectiveStartYear; year < endYear && year < currentYear; year++) {
    const yearKey = year.toString();
    if (!existingYears.has(yearKey)) {
      closedYears.push(yearKey);
    } else {
      metrics.yearsSkipped++;
    }
  }
  
  if (options.verbose) {
    console.log(`   📊 ${userId}: ${firstDate} → ${lastDate}, ${closedMonths.length} meses, ${closedYears.length} años, ${accounts.length} cuentas`);
  }
  
  return {
    userId,
    firstDate,
    lastDate,
    accounts,
    closedMonths,
    closedYears,
    existingMonths: existingMonths.size,
    existingYears: existingYears.size
  };
}

// ============================================================================
// FASE 2: CONSOLIDACIÓN MENSUAL
// ============================================================================

/**
 * Procesa consolidación mensual para todos los usuarios
 * 
 * @param {Array} users - Lista de usuarios analizados
 */
async function processMonthlyConsolidation(users) {
  let processedUsers = 0;
  
  for (const user of users) {
    processedUsers++;
    const prefix = `[${processedUsers}/${users.length}]`;
    
    if (user.closedMonths.length === 0) {
      if (options.verbose) {
        console.log(`${prefix} ${user.userId}: Sin meses nuevos a consolidar`);
      }
      continue;
    }
    
    console.log(`${prefix} ${user.userId}: ${user.closedMonths.length} meses...`);
    
    try {
      // Consolidar overall
      const basePath = `portfolioPerformance/${user.userId}`;
      const overallResult = await consolidateMonthsForPath(
        user.userId, 
        null, 
        user.closedMonths, 
        basePath
      );
      
      metrics.userDetails.push({
        userId: user.userId,
        type: 'overall',
        monthsConsolidated: overallResult.consolidated,
        monthsSkipped: overallResult.skipped
      });
      
      // Consolidar cada cuenta si está habilitado
      if (options.includeAccounts && user.accounts.length > 0) {
        for (const accountId of user.accounts) {
          try {
            const accountPath = `${basePath}/accounts/${accountId}`;
            const accountResult = await consolidateMonthsForPath(
              user.userId,
              accountId,
              user.closedMonths,
              accountPath
            );
            
            metrics.accountsProcessed++;
            metrics.userDetails.push({
              userId: user.userId,
              accountId,
              type: 'account',
              monthsConsolidated: accountResult.consolidated,
              monthsSkipped: accountResult.skipped
            });
          } catch (accountError) {
            metrics.warnings.push({
              userId: user.userId,
              accountId,
              phase: 'monthly',
              error: accountError.message
            });
          }
        }
      }
      
      metrics.usersProcessed++;
      
      // Throttle para evitar rate limits
      if (throttleMs > 0) {
        await sleep(throttleMs);
      }
      
    } catch (userError) {
      console.log(`   ❌ Error: ${userError.message}`);
      metrics.errors.push({
        userId: user.userId,
        phase: 'monthly',
        error: userError.message
      });
    }
  }
}

/**
 * Consolida meses para un path específico (overall o cuenta)
 * 
 * @param {string} userId - ID del usuario
 * @param {string|null} accountId - ID de cuenta o null para overall
 * @param {Array} months - Lista de meses a consolidar
 * @param {string} basePath - Path base en Firestore
 * @returns {Object} Resultado de la consolidación
 */
async function consolidateMonthsForPath(userId, accountId, months, basePath) {
  const label = accountId ? `cuenta:${accountId}` : 'overall';
  const result = { consolidated: 0, skipped: 0, errors: 0 };
  
  for (const monthKey of months) {
    try {
      const [year, month] = monthKey.split('-').map(Number);
      const periodStart = DateTime.fromObject({ year, month, day: 1 }).startOf('month').toISODate();
      const periodEnd = DateTime.fromObject({ year, month, day: 1 }).endOf('month').toISODate();
      
      // Leer documentos diarios del mes
      const dailySnapshot = await db.collection(`${basePath}/dates`)
        .where('date', '>=', periodStart)
        .where('date', '<=', periodEnd)
        .orderBy('date', 'asc')
        .get();
      
      metrics.documentsRead += dailySnapshot.size;
      
      if (dailySnapshot.empty) {
        result.skipped++;
        continue;
      }
      
      // Consolidar usando función de COST-OPT-001
      const consolidated = consolidatePeriod(
        dailySnapshot.docs,
        monthKey,
        'month'
      );
      
      if (!consolidated) {
        result.skipped++;
        continue;
      }
      
      // Guardar documento consolidado
      const consolidatedPath = `${basePath}/consolidatedPeriods/monthly/periods/${monthKey}`;
      
      if (!isDryRun) {
        await db.doc(consolidatedPath).set(consolidated);
        metrics.documentsWritten++;
      }
      
      result.consolidated++;
      metrics.monthsConsolidated++;
      
      if (options.verbose) {
        const currencies = Object.keys(consolidated).filter(k => 
          !['periodType', 'periodKey', 'startDate', 'endDate', 'docsCount', 'version', 'lastUpdated'].includes(k)
        );
        console.log(`      ✅ ${monthKey} (${label}): ${dailySnapshot.size} docs → ${currencies.join(', ')}`);
      }
      
    } catch (monthError) {
      result.errors++;
      metrics.warnings.push({
        userId,
        accountId,
        monthKey,
        phase: 'monthly-consolidate',
        error: monthError.message
      });
    }
  }
  
  return result;
}

// ============================================================================
// FASE 3: CONSOLIDACIÓN ANUAL
// ============================================================================

/**
 * Procesa consolidación anual para todos los usuarios
 * 
 * @param {Array} users - Lista de usuarios analizados
 */
async function processYearlyConsolidation(users) {
  let processedUsers = 0;
  
  for (const user of users) {
    processedUsers++;
    const prefix = `[${processedUsers}/${users.length}]`;
    
    if (user.closedYears.length === 0) {
      if (options.verbose) {
        console.log(`${prefix} ${user.userId}: Sin años nuevos a consolidar`);
      }
      continue;
    }
    
    console.log(`${prefix} ${user.userId}: ${user.closedYears.length} años...`);
    
    try {
      // Consolidar overall
      const basePath = `portfolioPerformance/${user.userId}`;
      const overallResult = await consolidateYearsForPath(
        user.userId,
        null,
        user.closedYears,
        basePath
      );
      
      // Consolidar cada cuenta
      if (options.includeAccounts && user.accounts.length > 0) {
        for (const accountId of user.accounts) {
          try {
            const accountPath = `${basePath}/accounts/${accountId}`;
            await consolidateYearsForPath(
              user.userId,
              accountId,
              user.closedYears,
              accountPath
            );
          } catch (accountError) {
            metrics.warnings.push({
              userId: user.userId,
              accountId,
              phase: 'yearly',
              error: accountError.message
            });
          }
        }
      }
      
      // Throttle
      if (throttleMs > 0) {
        await sleep(throttleMs);
      }
      
    } catch (userError) {
      console.log(`   ❌ Error: ${userError.message}`);
      metrics.errors.push({
        userId: user.userId,
        phase: 'yearly',
        error: userError.message
      });
    }
  }
}

/**
 * Consolida años para un path específico
 * 
 * @param {string} userId - ID del usuario
 * @param {string|null} accountId - ID de cuenta o null para overall
 * @param {Array} years - Lista de años a consolidar
 * @param {string} basePath - Path base en Firestore
 * @returns {Object} Resultado de la consolidación
 */
async function consolidateYearsForPath(userId, accountId, years, basePath) {
  const label = accountId ? `cuenta:${accountId}` : 'overall';
  const result = { consolidated: 0, skipped: 0, errors: 0 };
  
  for (const yearKey of years) {
    try {
      // Leer documentos mensuales del año (ya consolidados en FASE 2)
      const monthlySnapshot = await db.collection(`${basePath}/consolidatedPeriods/monthly/periods`)
        .where('periodKey', '>=', `${yearKey}-01`)
        .where('periodKey', '<=', `${yearKey}-12`)
        .orderBy('periodKey', 'asc')
        .get();
      
      metrics.documentsRead += monthlySnapshot.size;
      
      if (monthlySnapshot.empty) {
        if (options.verbose) {
          console.log(`      ⏭️  ${yearKey} (${label}): Sin meses consolidados`);
        }
        result.skipped++;
        continue;
      }
      
      // Encadenar meses para formar el año
      const consolidated = consolidateMonthsToYear(monthlySnapshot.docs, yearKey);
      
      if (!consolidated) {
        result.skipped++;
        continue;
      }
      
      // Guardar documento consolidado anual
      const yearlyPath = `${basePath}/consolidatedPeriods/yearly/periods/${yearKey}`;
      
      if (!isDryRun) {
        await db.doc(yearlyPath).set(consolidated);
        metrics.documentsWritten++;
      }
      
      result.consolidated++;
      metrics.yearsConsolidated++;
      
      if (options.verbose) {
        const currencies = Object.keys(consolidated).filter(k => 
          !['periodType', 'periodKey', 'startDate', 'endDate', 'docsCount', 'version', 'lastUpdated'].includes(k)
        );
        console.log(`      ✅ ${yearKey} (${label}): ${monthlySnapshot.size} meses → ${currencies.join(', ')}`);
      }
      
    } catch (yearError) {
      result.errors++;
      metrics.warnings.push({
        userId,
        accountId,
        yearKey,
        phase: 'yearly-consolidate',
        error: yearError.message
      });
    }
  }
  
  return result;
}

// ============================================================================
// UTILIDADES
// ============================================================================

/**
 * Sleep helper
 * @param {number} ms - Milisegundos
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Imprime resumen de la migración
 */
function printSummary() {
  const duration = ((Date.now() - metrics.startTime) / 1000).toFixed(1);
  
  console.log('\n' + '='.repeat(60));
  console.log('  RESUMEN DE MIGRACIÓN');
  console.log('='.repeat(60) + '\n');
  
  console.log(`  📋 Modo: ${isDryRun ? '🔍 DRY-RUN (ningún dato escrito)' : '⚡ EJECUTADO'}`);
  console.log(`  ⏱️  Duración: ${duration}s`);
  console.log('');
  
  console.log('  📊 ESTADÍSTICAS:');
  console.log(`     Usuarios analizados: ${metrics.usersAnalyzed}`);
  console.log(`     Usuarios procesados: ${metrics.usersProcessed}`);
  console.log(`     Usuarios omitidos:   ${metrics.usersSkipped}`);
  console.log(`     Cuentas procesadas:  ${metrics.accountsProcessed}`);
  console.log('');
  console.log(`     Meses consolidados:  ${metrics.monthsConsolidated}`);
  console.log(`     Meses ya existentes: ${metrics.monthsSkipped}`);
  console.log(`     Años consolidados:   ${metrics.yearsConsolidated}`);
  console.log(`     Años ya existentes:  ${metrics.yearsSkipped}`);
  console.log('');
  console.log(`     Documentos leídos:   ${metrics.documentsRead}`);
  console.log(`     Documentos escritos: ${metrics.documentsWritten}`);
  
  if (metrics.errors.length > 0) {
    console.log('\n  ❌ ERRORES:');
    metrics.errors.slice(0, 10).forEach((err, i) => {
      console.log(`     ${i + 1}. [${err.userId || 'global'}] ${err.phase}: ${err.error}`);
    });
    if (metrics.errors.length > 10) {
      console.log(`     ... y ${metrics.errors.length - 10} errores más`);
    }
  }
  
  if (metrics.warnings.length > 0 && options.verbose) {
    console.log('\n  ⚠️  ADVERTENCIAS:');
    metrics.warnings.slice(0, 5).forEach((warn, i) => {
      console.log(`     ${i + 1}. [${warn.userId}${warn.accountId ? '/' + warn.accountId : ''}] ${warn.phase}: ${warn.error}`);
    });
    if (metrics.warnings.length > 5) {
      console.log(`     ... y ${metrics.warnings.length - 5} advertencias más`);
    }
  }
  
  if (isDryRun) {
    console.log('\n  ℹ️  Para ejecutar la migración real, use: --execute');
    console.log('     Ejemplo: node migrateHistoricalPeriods.js --execute');
  } else {
    console.log('\n  ✅ Migración completada.');
    console.log('     Ejecute validateMigration.js para verificar los resultados.');
  }
  
  console.log('\n' + '='.repeat(60) + '\n');
  
  // Guardar reporte JSON
  if (!isDryRun || options.verbose) {
    const reportPath = path.join(__dirname, `migration-report-${Date.now()}.json`);
    const report = {
      timestamp: new Date().toISOString(),
      mode: isDryRun ? 'dry-run' : 'execute',
      config: {
        startYear,
        endYear,
        batchSize,
        userId: options.userId || 'all'
      },
      metrics: {
        ...metrics,
        durationSeconds: parseFloat(duration)
      }
    };
    
    require('fs').writeFileSync(reportPath, JSON.stringify(report, null, 2));
    console.log(`  📄 Reporte guardado en: ${reportPath}\n`);
  }
}

// ============================================================================
// EJECUCIÓN
// ============================================================================

main().then(() => {
  process.exit(metrics.errors.length > 0 ? 1 : 0);
}).catch((error) => {
  console.error('Error fatal no capturado:', error);
  process.exit(1);
});
