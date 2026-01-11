/**
 * Script de Reconstrucción del Overall desde Cuentas Individuales
 * 
 * PROPÓSITO:
 * Recalcular TODOS los campos del overall (portfolioPerformance/{userId}/dates)
 * basándose en los datos de las cuentas individuales. Esto corrige:
 * 
 * 1. totalCashFlow - Suma de cashflows de todas las cuentas
 * 2. adjustedDailyChangePercentage - Calculado con la fórmula correcta
 * 3. rawDailyChangePercentage - Sin ajuste de cashflow
 * 4. dailyChangePercentage - Igual a rawDailyChangePercentage
 * 
 * PROBLEMA QUE RESUELVE:
 * Se detectó que el overall tiene cashflows que no coinciden con la suma de
 * las cuentas individuales, causando diferencias en el TWR acumulado.
 * 
 * FÓRMULA UTILIZADA:
 *   prevValue = Σ(prevValue_i) donde prevValue_i = (currValue_i + cashFlow_i) / (1 + change_i/100)
 *   adjustedChange = (currValue_total - prevValue_total + cashFlow_total) / prevValue_total × 100
 * 
 * USO:
 *   node rebuildOverallFromAccounts.js --analyze     # Identificar discrepancias
 *   node rebuildOverallFromAccounts.js --dry-run    # Ver cambios sin aplicar
 *   node rebuildOverallFromAccounts.js --fix        # Aplicar correcciones
 * 
 * OPCIONES:
 *   --user=<userId>        # Usuario específico
 *   --start=YYYY-MM-DD     # Fecha inicio
 *   --end=YYYY-MM-DD       # Fecha fin
 *   --threshold=<number>   # Umbral de discrepancia en % (default: 0.01)
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

// Inicializar Firebase Admin
const serviceAccount = require('../../../key.json');

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
  DEFAULT_USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  DEFAULT_START_DATE: '2024-08-01',
  DEFAULT_THRESHOLD: 0.01,
  BATCH_SIZE: 400,
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
};

// ============================================================================
// UTILIDADES
// ============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    mode: 'analyze',
    userId: CONFIG.DEFAULT_USER_ID,
    startDate: CONFIG.DEFAULT_START_DATE,
    endDate: DateTime.now().setZone('America/New_York').toISODate(),
    threshold: CONFIG.DEFAULT_THRESHOLD,
  };

  args.forEach(arg => {
    if (arg === '--analyze') options.mode = 'analyze';
    else if (arg === '--dry-run') options.mode = 'dry-run';
    else if (arg === '--fix') options.mode = 'fix';
    else if (arg.startsWith('--user=')) options.userId = arg.split('=')[1];
    else if (arg.startsWith('--start=')) options.startDate = arg.split('=')[1];
    else if (arg.startsWith('--end=')) options.endDate = arg.split('=')[1];
    else if (arg.startsWith('--threshold=')) options.threshold = parseFloat(arg.split('=')[1]);
  });

  return options;
}

function log(level, message, data = null) {
  const prefix = {
    'INFO': '📋',
    'SUCCESS': '✅',
    'WARNING': '⚠️',
    'ERROR': '❌',
    'DEBUG': '🔍',
    'CHANGE': '🔄',
    'SKIP': '⏭️',
  }[level] || '•';
  
  console.log(`${prefix} ${message}`);
  if (data) console.log('   ', JSON.stringify(data, null, 2).split('\n').join('\n    '));
}

// ============================================================================
// OBTENCIÓN DE DATOS
// ============================================================================

async function getUserAccounts(userId) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

async function getOverallPerformance(userId, startDate, endDate) {
  const snapshot = await db.collection(`portfolioPerformance/${userId}/dates`)
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs;
}

async function getAccountPerformance(userId, accountId, startDate, endDate) {
  const snapshot = await db.collection(`portfolioPerformance/${userId}/accounts/${accountId}/dates`)
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .orderBy('date', 'asc')
    .get();
  
  const data = {};
  snapshot.docs.forEach(doc => {
    data[doc.data().date] = doc.data();
  });
  
  return data;
}

// ============================================================================
// CÁLCULO DE OVERALL DESDE CUENTAS
// ============================================================================

/**
 * Calcular el overall para una fecha basándose en las cuentas individuales
 */
function calculateOverallFromAccounts(accountsData, date, previousOverallValue, currency = 'USD') {
  const contributions = [];
  
  // Recopilar datos de cada cuenta para esta fecha
  Object.entries(accountsData).forEach(([accountId, data]) => {
    const dateData = data[date];
    if (!dateData || !dateData[currency]) return;
    
    const currencyData = dateData[currency];
    contributions.push({
      accountId,
      totalValue: currencyData.totalValue || 0,
      totalInvestment: currencyData.totalInvestment || 0,
      adjustedDailyChangePercentage: currencyData.adjustedDailyChangePercentage || 0,
      rawDailyChangePercentage: currencyData.rawDailyChangePercentage || currencyData.dailyChangePercentage || 0,
      totalCashFlow: currencyData.totalCashFlow || 0,
      unrealizedPnL: currencyData.unrealizedPnL || 0,
      doneProfitAndLoss: currencyData.doneProfitAndLoss || 0,
    });
  });
  
  if (contributions.length === 0) {
    return null;
  }
  
  // Sumar valores de todas las cuentas
  const totalValue = contributions.reduce((sum, c) => sum + c.totalValue, 0);
  const totalInvestment = contributions.reduce((sum, c) => sum + c.totalInvestment, 0);
  const totalCashFlow = contributions.reduce((sum, c) => sum + c.totalCashFlow, 0);
  const unrealizedPnL = contributions.reduce((sum, c) => sum + c.unrealizedPnL, 0);
  const doneProfitAndLoss = contributions.reduce((sum, c) => sum + c.doneProfitAndLoss, 0);
  
  // Calcular prevValue usando la fórmula corregida
  // Excluir cuentas nuevas (primer día)
  const existingAccounts = contributions.filter(acc => {
    const isNewAccount = acc.adjustedDailyChangePercentage === 0 && 
                         acc.totalCashFlow < 0 && 
                         acc.totalValue > 0;
    return !isNewAccount;
  });
  
  // Calcular preChangeValue para cada cuenta existente
  const withPreValue = existingAccounts.map(acc => {
    const change = acc.adjustedDailyChangePercentage || 0;
    const currentValue = acc.totalValue || 0;
    const cashFlow = acc.totalCashFlow || 0;
    
    // FIX: Incluir cashFlow en el cálculo del prevValue
    const preChangeValue = change !== 0 
      ? (currentValue + cashFlow) / (1 + change / 100) 
      : currentValue + cashFlow;
    
    return { ...acc, preChangeValue: Math.max(0, preChangeValue) };
  });
  
  const prevValueCalculated = withPreValue.reduce((sum, acc) => sum + acc.preChangeValue, 0);
  
  // Usar el prevValue calculado o el anterior si no hay cuentas existentes
  const prevValue = prevValueCalculated > 0 ? prevValueCalculated : previousOverallValue;
  
  // Calcular cambios
  let adjustedDailyChangePercentage = 0;
  let rawDailyChangePercentage = 0;
  
  if (prevValue > 0) {
    // Fórmula correcta
    adjustedDailyChangePercentage = ((totalValue - prevValue + totalCashFlow) / prevValue) * 100;
    rawDailyChangePercentage = ((totalValue - prevValue) / prevValue) * 100;
  }
  
  // ROI
  const totalROI = totalInvestment > 0 
    ? ((totalValue - totalInvestment) / totalInvestment) * 100 
    : 0;
  
  return {
    totalValue,
    totalInvestment,
    totalCashFlow,
    unrealizedPnL,
    doneProfitAndLoss,
    totalROI,
    adjustedDailyChangePercentage,
    rawDailyChangePercentage,
    dailyChangePercentage: rawDailyChangePercentage,
    dailyReturn: adjustedDailyChangePercentage / 100,
    prevValueUsed: prevValue,
  };
}

// ============================================================================
// ANÁLISIS DE DISCREPANCIAS
// ============================================================================

async function analyzeDiscrepancies(overallDocs, accountsData, threshold) {
  const discrepancies = [];
  let previousOverallValue = 0;
  
  for (const doc of overallDocs) {
    const data = doc.data();
    const date = data.date;
    const storedUSD = data.USD || {};
    
    // Calcular el overall desde las cuentas
    const calculated = calculateOverallFromAccounts(accountsData, date, previousOverallValue, 'USD');
    
    if (!calculated) {
      previousOverallValue = storedUSD.totalValue || 0;
      continue;
    }
    
    // Comparar valores
    const diffCashFlow = Math.abs((storedUSD.totalCashFlow || 0) - calculated.totalCashFlow);
    const diffAdjustedChange = Math.abs((storedUSD.adjustedDailyChangePercentage || 0) - calculated.adjustedDailyChangePercentage);
    const diffRawChange = Math.abs((storedUSD.rawDailyChangePercentage || storedUSD.dailyChangePercentage || 0) - calculated.rawDailyChangePercentage);
    
    // Si hay discrepancia significativa
    if (diffCashFlow > 0.01 || diffAdjustedChange > threshold || diffRawChange > threshold) {
      discrepancies.push({
        date,
        docRef: doc.ref,
        stored: {
          totalCashFlow: storedUSD.totalCashFlow || 0,
          adjustedDailyChangePercentage: storedUSD.adjustedDailyChangePercentage || 0,
          rawDailyChangePercentage: storedUSD.rawDailyChangePercentage || storedUSD.dailyChangePercentage || 0,
        },
        calculated: {
          totalCashFlow: calculated.totalCashFlow,
          adjustedDailyChangePercentage: calculated.adjustedDailyChangePercentage,
          rawDailyChangePercentage: calculated.rawDailyChangePercentage,
          totalValue: calculated.totalValue,
          totalInvestment: calculated.totalInvestment,
        },
        diff: {
          cashFlow: diffCashFlow,
          adjustedChange: diffAdjustedChange,
          rawChange: diffRawChange,
        },
      });
    }
    
    // Actualizar prevValue para el siguiente día
    previousOverallValue = storedUSD.totalValue || 0;
  }
  
  return discrepancies;
}

/**
 * Calcular impacto en TWR
 */
function calculateTWRImpact(overallDocs, discrepancies) {
  let factorStored = 1;
  let factorCorrected = 1;
  
  const correctionsMap = new Map();
  discrepancies.forEach(d => {
    correctionsMap.set(d.date, d.calculated.adjustedDailyChangePercentage);
  });
  
  overallDocs.forEach(doc => {
    const data = doc.data();
    const date = data.date;
    const storedChange = data.USD?.adjustedDailyChangePercentage || 0;
    
    const correctedChange = correctionsMap.has(date) 
      ? correctionsMap.get(date) 
      : storedChange;
    
    factorStored *= (1 + storedChange / 100);
    factorCorrected *= (1 + correctedChange / 100);
  });
  
  return {
    twrStored: (factorStored - 1) * 100,
    twrCorrected: (factorCorrected - 1) * 100,
    difference: ((factorCorrected - 1) - (factorStored - 1)) * 100,
  };
}

// ============================================================================
// APLICACIÓN DE CORRECCIONES
// ============================================================================

async function applyCorrections(discrepancies, mode) {
  if (discrepancies.length === 0) {
    log('INFO', 'No hay correcciones que aplicar');
    return 0;
  }
  
  const batches = [];
  let currentBatch = db.batch();
  let operationsInBatch = 0;
  
  for (const d of discrepancies) {
    // Construir el update para todas las monedas
    const update = {};
    
    CONFIG.CURRENCIES.forEach(curr => {
      // CashFlow
      update[`${curr}.totalCashFlow`] = d.calculated.totalCashFlow;
      
      // Cambios porcentuales (iguales para todas las monedas)
      update[`${curr}.adjustedDailyChangePercentage`] = d.calculated.adjustedDailyChangePercentage;
      update[`${curr}.rawDailyChangePercentage`] = d.calculated.rawDailyChangePercentage;
      update[`${curr}.dailyChangePercentage`] = d.calculated.rawDailyChangePercentage;
      update[`${curr}.dailyReturn`] = d.calculated.adjustedDailyChangePercentage / 100;
    });
    
    if (mode === 'fix') {
      currentBatch.update(d.docRef, update);
      operationsInBatch++;
      
      if (operationsInBatch >= CONFIG.BATCH_SIZE) {
        batches.push(currentBatch);
        currentBatch = db.batch();
        operationsInBatch = 0;
      }
    }
    
    if (mode === 'dry-run') {
      const cfChange = d.diff.cashFlow > 0.01 ? ` CF: ${d.stored.totalCashFlow.toFixed(2)} → ${d.calculated.totalCashFlow.toFixed(2)}` : '';
      log('CHANGE', `${d.date}: ${d.stored.adjustedDailyChangePercentage.toFixed(4)}% → ${d.calculated.adjustedDailyChangePercentage.toFixed(4)}%${cfChange}`);
    }
  }
  
  if (operationsInBatch > 0) {
    batches.push(currentBatch);
  }
  
  if (mode === 'fix') {
    log('INFO', `Aplicando ${discrepancies.length} correcciones en ${batches.length} batches...`);
    
    for (let i = 0; i < batches.length; i++) {
      await batches[i].commit();
      log('SUCCESS', `Batch ${i + 1}/${batches.length} completado`);
    }
  }
  
  return discrepancies.length;
}

// ============================================================================
// PROCESO PRINCIPAL
// ============================================================================

async function main() {
  const options = parseArgs();
  
  console.log('');
  console.log('═'.repeat(100));
  console.log('  RECONSTRUCCIÓN DE OVERALL DESDE CUENTAS INDIVIDUALES');
  console.log('═'.repeat(100));
  console.log('');
  console.log(`Modo: ${options.mode.toUpperCase()}`);
  console.log(`Usuario: ${options.userId}`);
  console.log(`Período: ${options.startDate} a ${options.endDate}`);
  console.log(`Umbral de discrepancia: ${options.threshold}%`);
  console.log('');
  
  // 1. Obtener cuentas del usuario
  log('INFO', 'Obteniendo cuentas del usuario...');
  const accounts = await getUserAccounts(options.userId);
  log('SUCCESS', `Encontradas ${accounts.length} cuentas activas`);
  
  accounts.forEach(acc => {
    console.log(`   • ${acc.name} (${acc.id})`);
  });
  console.log('');
  
  // 2. Obtener datos de performance de cada cuenta
  log('INFO', 'Obteniendo datos de performance de cada cuenta...');
  const accountsData = {};
  
  for (const account of accounts) {
    accountsData[account.id] = await getAccountPerformance(
      options.userId, 
      account.id, 
      options.startDate, 
      options.endDate
    );
    const days = Object.keys(accountsData[account.id]).length;
    console.log(`   • ${account.name}: ${days} días`);
  }
  console.log('');
  
  // 3. Obtener documentos de overall
  log('INFO', 'Obteniendo documentos de overall...');
  const overallDocs = await getOverallPerformance(options.userId, options.startDate, options.endDate);
  log('SUCCESS', `Encontrados ${overallDocs.length} documentos de overall`);
  console.log('');
  
  // 4. Analizar discrepancias
  log('INFO', 'Analizando discrepancias...');
  const discrepancies = await analyzeDiscrepancies(overallDocs, accountsData, options.threshold);
  
  console.log('');
  console.log('─'.repeat(100));
  console.log('  DISCREPANCIAS ENCONTRADAS');
  console.log('─'.repeat(100));
  console.log('');
  
  if (discrepancies.length === 0) {
    log('SUCCESS', '¡No se encontraron discrepancias! Los datos están correctos.');
    process.exit(0);
  }
  
  log('WARNING', `Se encontraron ${discrepancies.length} documentos con discrepancias`);
  console.log('');
  
  // Clasificar discrepancias
  const cashFlowErrors = discrepancies.filter(d => d.diff.cashFlow > 0.01);
  const changeErrors = discrepancies.filter(d => d.diff.adjustedChange > options.threshold);
  
  console.log(`   • CashFlow incorrecto: ${cashFlowErrors.length} días`);
  console.log(`   • Cambio % incorrecto: ${changeErrors.length} días`);
  console.log('');
  
  // Mostrar top 15 discrepancias
  const topDiscrepancies = [...discrepancies]
    .sort((a, b) => (b.diff.cashFlow + b.diff.adjustedChange) - (a.diff.cashFlow + a.diff.adjustedChange))
    .slice(0, 15);
  
  console.log('Top discrepancias:');
  console.log('');
  console.log('Fecha       | CF Stored | CF Calc  | Change Stored | Change Calc');
  console.log('------------|-----------|----------|---------------|------------');
  
  topDiscrepancies.forEach(d => {
    console.log(
      `${d.date} | ` +
      `${d.stored.totalCashFlow.toFixed(2).padStart(9)} | ` +
      `${d.calculated.totalCashFlow.toFixed(2).padStart(8)} | ` +
      `${d.stored.adjustedDailyChangePercentage.toFixed(4).padStart(12)}% | ` +
      `${d.calculated.adjustedDailyChangePercentage.toFixed(4).padStart(10)}%`
    );
  });
  
  if (discrepancies.length > 15) {
    console.log(`... y ${discrepancies.length - 15} más`);
  }
  console.log('');
  
  // 5. Calcular impacto en TWR
  console.log('─'.repeat(100));
  console.log('  IMPACTO EN TWR ACUMULADO');
  console.log('─'.repeat(100));
  console.log('');
  
  const impact = calculateTWRImpact(overallDocs, discrepancies);
  
  console.log(`TWR con datos almacenados: ${impact.twrStored.toFixed(4)}%`);
  console.log(`TWR con datos corregidos:  ${impact.twrCorrected.toFixed(4)}%`);
  console.log(`Diferencia:                ${impact.difference.toFixed(4)}%`);
  console.log('');
  
  // 6. Ejecutar según el modo
  if (options.mode === 'analyze') {
    console.log('─'.repeat(100));
    log('INFO', 'Modo ANALYZE: Solo se muestran las discrepancias.');
    log('INFO', 'Ejecuta con --dry-run para ver los cambios propuestos.');
    log('INFO', 'Ejecuta con --fix para aplicar las correcciones.');
    console.log('');
    process.exit(0);
  }
  
  if (options.mode === 'dry-run') {
    console.log('─'.repeat(100));
    console.log('  CAMBIOS PROPUESTOS (DRY-RUN)');
    console.log('─'.repeat(100));
    console.log('');
    
    await applyCorrections(discrepancies, 'dry-run');
    
    console.log('');
    log('INFO', 'Modo DRY-RUN: No se aplicaron cambios.');
    log('INFO', 'Ejecuta con --fix para aplicar las correcciones.');
    console.log('');
    process.exit(0);
  }
  
  if (options.mode === 'fix') {
    console.log('─'.repeat(100));
    console.log('  APLICANDO CORRECCIONES');
    console.log('─'.repeat(100));
    console.log('');
    
    const count = await applyCorrections(discrepancies, 'fix');
    
    console.log('');
    log('SUCCESS', `¡${count} documentos corregidos exitosamente!`);
    console.log('');
    
    // Invalidar cache
    log('INFO', 'Invalidando cache de performance...');
    const cacheCollection = db.collection(`userData/${options.userId}/performanceCache`);
    const cacheSnapshot = await cacheCollection.get();
    
    if (!cacheSnapshot.empty) {
      const batch = db.batch();
      cacheSnapshot.docs.forEach(doc => batch.delete(doc.ref));
      await batch.commit();
      log('SUCCESS', `Cache invalidado (${cacheSnapshot.docs.length} documentos eliminados)`);
    } else {
      log('INFO', 'No hay cache que invalidar');
    }
    
    console.log('');
    process.exit(0);
  }
}

main().catch(err => {
  log('ERROR', 'Error fatal:', { message: err.message, stack: err.stack });
  process.exit(1);
});
