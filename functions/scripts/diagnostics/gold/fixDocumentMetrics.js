/**
 * Script de Corrección de Métricas de Documento de Performance
 * 
 * PROPÓSITO:
 * Recalcular las métricas agregadas del documento (totalValue, totalInvestment, 
 * totalCashFlow, adjustedDailyChangePercentage, etc.) basándose en la SUMA de 
 * los valores ya almacenados en assetPerformance.
 * 
 * DIFERENCIA CON fixAssetPerformance.js:
 * - fixAssetPerformance.js: Corrige un asset específico usando precios históricos
 * - fixDocumentMetrics.js: Corrige las métricas agregadas del documento usando
 *   los valores de assetPerformance ya existentes
 * 
 * ALCANCE:
 * - Actualiza OVERALL (portfolioPerformance/{userId}/dates)
 * - Actualiza las cuentas específicas (portfolioPerformance/{userId}/accounts/{accountId}/dates)
 * - Recalcula para todas las monedas activas (USD, COP, EUR, etc.)
 * 
 * USO:
 *   node fixDocumentMetrics.js --analyze                # Solo analiza discrepancias
 *   node fixDocumentMetrics.js --dry-run               # Muestra cambios sin aplicar
 *   node fixDocumentMetrics.js --fix                   # Aplica los cambios
 * 
 * OPCIONES:
 *   --user=<userId>        # Usuario específico (default: DDeR8P5hYgfuN8gcU4RsQfdTJqx2)
 *   --start=YYYY-MM-DD     # Fecha inicio
 *   --end=YYYY-MM-DD       # Fecha fin
 *   --threshold=<number>   # Umbral de discrepancia en % para considerar corrección (default: 0.1)
 *   --account=<accountId>  # Solo procesar una cuenta específica (o 'overall' para solo OVERALL)
 * 
 * MÉTRICAS QUE RECALCULA:
 *   - totalValue: Suma de todos los assetPerformance[*].totalValue
 *   - totalInvestment: Suma de todos los assetPerformance[*].totalInvestment
 *   - totalCashFlow: Suma de todos los assetPerformance[*].totalCashFlow
 *   - unrealizedProfitAndLoss: Suma de todos los assetPerformance[*].unrealizedProfitAndLoss
 *   - doneProfitAndLoss: Suma de todos los assetPerformance[*].doneProfitAndLoss
 *   - totalROI: Recalculado como (unrealizedPnL / totalInvestment) * 100
 *   - adjustedDailyChangePercentage: TWR calculado con fórmula estándar
 *   - dailyChangePercentage: Cambio porcentual del valor total
 * 
 * ESTRUCTURA DE DATOS:
 * portfolioPerformance/
 *   {userId}/
 *     dates/                         <- OVERALL
 *       {date}/
 *         date: "2024-08-16"
 *         USD: { 
 *           totalValue,              <- RECALCULADO
 *           totalInvestment,         <- RECALCULADO
 *           totalCashFlow,           <- RECALCULADO
 *           adjustedDailyChangePercentage,  <- RECALCULADO
 *           assetPerformance: { SPYG_etf: {...}, AMZN_stock: {...} }  <- FUENTE
 *         }
 *     accounts/
 *       {accountId}/
 *         dates/
 *           {date}/
 *             ... (misma estructura)
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
  
  // Monedas soportadas
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
  
  // Umbral de discrepancia para considerar corrección (%)
  DEFAULT_THRESHOLD: 0.1,
  
  // Batch size para escrituras
  BATCH_SIZE: 400,
};

// ============================================================================
// UTILIDADES
// ============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    mode: 'analyze', // analyze, dry-run, fix
    userId: CONFIG.DEFAULT_USER_ID,
    startDate: '2024-01-01',
    endDate: DateTime.now().setZone('America/New_York').toISODate(),
    threshold: CONFIG.DEFAULT_THRESHOLD,
    accountFilter: null, // null = todos, 'overall' = solo overall, accountId = solo esa cuenta
  };

  args.forEach(arg => {
    if (arg === '--analyze') options.mode = 'analyze';
    else if (arg === '--dry-run') options.mode = 'dry-run';
    else if (arg === '--fix') options.mode = 'fix';
    else if (arg.startsWith('--user=')) options.userId = arg.split('=')[1];
    else if (arg.startsWith('--start=')) options.startDate = arg.split('=')[1];
    else if (arg.startsWith('--end=')) options.endDate = arg.split('=')[1];
    else if (arg.startsWith('--threshold=')) options.threshold = parseFloat(arg.split('=')[1]);
    else if (arg.startsWith('--account=')) options.accountFilter = arg.split('=')[1];
  });

  return options;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
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

/**
 * Obtener todas las cuentas del usuario
 */
async function getUserAccounts(userId) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .get();
  
  return snapshot.docs.map(doc => ({
    id: doc.id,
    ...doc.data()
  }));
}

/**
 * Obtener documentos de performance existentes
 */
async function getPerformanceDocuments(userId, accountId, startDate, endDate) {
  const path = accountId 
    ? `portfolioPerformance/${userId}/accounts/${accountId}/dates`
    : `portfolioPerformance/${userId}/dates`;
  
  const snapshot = await db.collection(path)
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs;
}

// ============================================================================
// CÁLCULO DE MÉTRICAS DESDE ASSET PERFORMANCE
// ============================================================================

/**
 * Calcular métricas agregadas desde assetPerformance para una moneda específica
 * 
 * IMPORTANTE: 
 * - totalValue, totalInvestment, unrealizedPnL -> se SUMAN de los assets
 * - totalCashFlow -> se PRESERVA del documento (viene de transacciones, NO de assets)
 * - TWR -> se RECALCULA usando totalValue corregido + totalCashFlow preservado
 * 
 * @param {Object} currencyData - Datos de la moneda (USD, COP, etc.)
 * @param {number|null} previousTotalValue - Valor total del documento anterior (para TWR)
 * @returns {Object} Métricas calculadas
 */
function calculateMetricsFromAssetPerformance(currencyData, previousTotalValue = null) {
  const assetPerformance = currencyData?.assetPerformance || {};
  
  // Sumar valores de todos los assets
  let totalValue = 0;
  let totalInvestment = 0;
  let unrealizedProfitAndLoss = 0;
  let doneProfitAndLoss = 0;
  let assetCount = 0;
  
  Object.entries(assetPerformance).forEach(([assetKey, assetData]) => {
    if (!assetData || typeof assetData !== 'object') return;
    
    totalValue += assetData.totalValue || 0;
    totalInvestment += assetData.totalInvestment || 0;
    unrealizedProfitAndLoss += assetData.unrealizedProfitAndLoss || 0;
    doneProfitAndLoss += assetData.doneProfitAndLoss || 0;
    assetCount++;
  });
  
  // IMPORTANTE: totalCashFlow viene del documento, NO de los assets
  // Los assets tienen totalCashFlow = 0 en la mayoría de los casos
  // El cashflow real del portfolio se calcula desde las transacciones
  const totalCashFlow = currencyData?.totalCashFlow || 0;
  
  // Calcular métricas derivadas
  const totalROI = totalInvestment > 0 
    ? (unrealizedProfitAndLoss / totalInvestment) * 100 
    : 0;
  
  // Calcular cambios diarios usando el totalCashFlow PRESERVADO del documento
  let dailyChangePercentage = 0;
  let adjustedDailyChangePercentage = 0;
  
  if (previousTotalValue !== null && previousTotalValue > 0) {
    // Cambio simple (sin ajuste por cashflow)
    dailyChangePercentage = ((totalValue - previousTotalValue) / previousTotalValue) * 100;
    
    // TWR: ajustado por cashflow (usando cashflow del documento)
    // Fórmula: (V_final - V_inicial + CashFlow) / V_inicial
    adjustedDailyChangePercentage = ((totalValue - previousTotalValue + totalCashFlow) / previousTotalValue) * 100;
  }
  
  return {
    totalValue,
    totalInvestment,
    totalCashFlow,  // Preservado del documento
    unrealizedProfitAndLoss,
    doneProfitAndLoss,
    totalROI,
    dailyChangePercentage,
    adjustedDailyChangePercentage,
    rawDailyChangePercentage: dailyChangePercentage,
    assetCount,
  };
}

/**
 * Comparar métricas calculadas vs almacenadas y detectar discrepancias
 * 
 * NOTA: NO comparamos totalCashFlow porque ese valor viene de las transacciones
 * y ya está correcto. Solo comparamos métricas derivadas de assetPerformance.
 */
function compareMetrics(calculated, stored, threshold) {
  const discrepancies = [];
  
  // Campos a comparar con umbral porcentual
  // NOTA: NO incluimos totalCashFlow - viene de transacciones, no de assets
  const fieldsToCompare = [
    { key: 'totalValue', label: 'Total Value' },
    { key: 'totalInvestment', label: 'Total Investment' },
    // { key: 'totalCashFlow', label: 'Total CashFlow' },  // PRESERVAR - no comparar
    { key: 'unrealizedProfitAndLoss', label: 'Unrealized P&L' },
    { key: 'totalROI', label: 'Total ROI', isPercentage: true },
    { key: 'adjustedDailyChangePercentage', label: 'TWR', isPercentage: true },
  ];
  
  fieldsToCompare.forEach(field => {
    const calcValue = calculated[field.key] || 0;
    const storedValue = stored[field.key] || 0;
    
    let diff;
    if (field.isPercentage) {
      // Para porcentajes, comparar directamente la diferencia absoluta
      diff = Math.abs(calcValue - storedValue);
    } else {
      // Para valores absolutos, calcular diferencia porcentual
      if (Math.abs(storedValue) > 0.01) {
        diff = Math.abs((calcValue - storedValue) / storedValue) * 100;
      } else if (Math.abs(calcValue) > 0.01) {
        diff = 100; // Si stored es 0 pero calc no, es 100% diferente
      } else {
        diff = 0; // Ambos son ~0
      }
    }
    
    if (diff > threshold) {
      discrepancies.push({
        field: field.key,
        label: field.label,
        calculated: calcValue,
        stored: storedValue,
        diff,
        isPercentage: field.isPercentage,
      });
    }
  });
  
  return discrepancies;
}

// ============================================================================
// ANÁLISIS Y CORRECCIÓN
// ============================================================================

/**
 * Analizar discrepancias en un conjunto de documentos
 * 
 * @param {Array} docs - Documentos de Firestore
 * @param {string} level - 'overall' o accountId
 * @param {number} threshold - Umbral de discrepancia
 * @returns {Array} Lista de discrepancias encontradas
 */
async function analyzeDocumentDiscrepancies(docs, level, threshold) {
  const discrepancies = [];
  
  // Cache de valores anteriores por moneda
  const previousValues = new Map();
  
  // Cache de valores corregidos (para usar en cálculos de TWR posteriores)
  const correctedValuesCache = new Map();
  
  for (let i = 0; i < docs.length; i++) {
    const doc = docs[i];
    const docData = doc.data();
    const date = docData.date;
    
    const docDiscrepancies = {
      docRef: doc.ref,
      date,
      level,
      currencies: {},
      hasDiscrepancy: false,
    };
    
    // Analizar cada moneda
    for (const currency of CONFIG.CURRENCIES) {
      const currencyData = docData[currency];
      if (!currencyData || !currencyData.assetPerformance) continue;
      
      // Obtener valor anterior (corregido si existe, o del documento anterior)
      const cacheKey = `${currency}`;
      let previousTotalValue = null;
      
      if (correctedValuesCache.has(cacheKey)) {
        previousTotalValue = correctedValuesCache.get(cacheKey);
      } else if (previousValues.has(cacheKey)) {
        previousTotalValue = previousValues.get(cacheKey);
      }
      
      // Calcular métricas desde assetPerformance
      const calculated = calculateMetricsFromAssetPerformance(currencyData, previousTotalValue);
      
      // Comparar con valores almacenados
      const currencyDiscrepancies = compareMetrics(calculated, currencyData, threshold);
      
      if (currencyDiscrepancies.length > 0) {
        docDiscrepancies.currencies[currency] = {
          calculated,
          stored: {
            totalValue: currencyData.totalValue,
            totalInvestment: currencyData.totalInvestment,
            totalCashFlow: currencyData.totalCashFlow,
            unrealizedProfitAndLoss: currencyData.unrealizedProfitAndLoss,
            doneProfitAndLoss: currencyData.doneProfitAndLoss,
            totalROI: currencyData.totalROI,
            adjustedDailyChangePercentage: currencyData.adjustedDailyChangePercentage,
          },
          discrepancies: currencyDiscrepancies,
        };
        docDiscrepancies.hasDiscrepancy = true;
        
        // Guardar valor corregido para el siguiente documento
        correctedValuesCache.set(cacheKey, calculated.totalValue);
      } else {
        // Sin discrepancia, usar valor almacenado
        correctedValuesCache.set(cacheKey, currencyData.totalValue);
      }
      
      // Guardar valor para referencia del siguiente documento
      previousValues.set(cacheKey, currencyData.totalValue);
    }
    
    if (docDiscrepancies.hasDiscrepancy) {
      discrepancies.push(docDiscrepancies);
    }
  }
  
  return discrepancies;
}

/**
 * Generar actualizaciones para Firestore
 * 
 * NOTA: NO actualizamos totalCashFlow porque viene de las transacciones
 * y ya está correcto en el documento. Solo actualizamos las métricas
 * que se derivan de la suma de assetPerformance.
 */
function generateUpdates(discrepancies) {
  const updates = [];
  
  discrepancies.forEach(d => {
    const updateData = {};
    
    Object.entries(d.currencies).forEach(([currency, data]) => {
      const calc = data.calculated;
      
      // Actualizar métricas del nivel de moneda (no de assetPerformance)
      // IMPORTANTE: NO actualizamos totalCashFlow - viene de transacciones y está correcto
      updateData[`${currency}.totalValue`] = calc.totalValue;
      updateData[`${currency}.totalInvestment`] = calc.totalInvestment;
      // updateData[`${currency}.totalCashFlow`] = calc.totalCashFlow;  // PRESERVAR - no modificar
      updateData[`${currency}.unrealizedProfitAndLoss`] = calc.unrealizedProfitAndLoss;
      updateData[`${currency}.doneProfitAndLoss`] = calc.doneProfitAndLoss;
      updateData[`${currency}.totalROI`] = calc.totalROI;
      updateData[`${currency}.dailyChangePercentage`] = calc.dailyChangePercentage;
      updateData[`${currency}.adjustedDailyChangePercentage`] = calc.adjustedDailyChangePercentage;
      updateData[`${currency}.rawDailyChangePercentage`] = calc.rawDailyChangePercentage;
    });
    
    if (Object.keys(updateData).length > 0) {
      updates.push({
        ref: d.docRef,
        date: d.date,
        level: d.level,
        data: updateData,
        currenciesAffected: Object.keys(d.currencies),
      });
    }
  });
  
  return updates;
}

/**
 * Aplicar actualizaciones a Firestore
 */
async function applyUpdates(updates) {
  const batches = [];
  let currentBatch = db.batch();
  let operationsInBatch = 0;
  
  for (const update of updates) {
    currentBatch.update(update.ref, update.data);
    operationsInBatch++;
    
    if (operationsInBatch >= CONFIG.BATCH_SIZE) {
      batches.push(currentBatch);
      currentBatch = db.batch();
      operationsInBatch = 0;
    }
  }
  
  if (operationsInBatch > 0) {
    batches.push(currentBatch);
  }
  
  log('INFO', `Aplicando ${updates.length} actualizaciones en ${batches.length} batches...`);
  
  for (let i = 0; i < batches.length; i++) {
    await batches[i].commit();
    log('SUCCESS', `Batch ${i + 1}/${batches.length} completado`);
    if (i < batches.length - 1) {
      await sleep(100);
    }
  }
  
  return updates.length;
}

// ============================================================================
// PROCESO PRINCIPAL
// ============================================================================

async function main() {
  const options = parseArgs();
  
  console.log('');
  console.log('='.repeat(100));
  console.log('CORRECCIÓN DE MÉTRICAS DE DOCUMENTO DE PERFORMANCE');
  console.log('='.repeat(100));
  console.log(`Modo: ${options.mode.toUpperCase()}`);
  console.log(`Usuario: ${options.userId}`);
  console.log(`Período: ${options.startDate} a ${options.endDate}`);
  console.log(`Umbral de discrepancia: ${options.threshold}%`);
  console.log(`Filtro de cuenta: ${options.accountFilter || 'Todas'}`);
  console.log('');

  // Obtener cuentas del usuario
  const accounts = await getUserAccounts(options.userId);
  log('INFO', `Encontradas ${accounts.length} cuentas para el usuario`);

  const allDiscrepancies = [];
  const summaryByLevel = new Map();

  // 1. Analizar OVERALL (si no hay filtro o el filtro es 'overall')
  if (!options.accountFilter || options.accountFilter === 'overall') {
    console.log('');
    console.log('-'.repeat(100));
    log('INFO', 'Analizando OVERALL...');
    
    const overallDocs = await getPerformanceDocuments(
      options.userId, 
      null, 
      options.startDate, 
      options.endDate
    );
    log('INFO', `Encontrados ${overallDocs.length} documentos OVERALL`);
    
    const overallDiscrepancies = await analyzeDocumentDiscrepancies(
      overallDocs, 
      'overall', 
      options.threshold
    );
    
    log('INFO', `Discrepancias encontradas en OVERALL: ${overallDiscrepancies.length}`);
    allDiscrepancies.push(...overallDiscrepancies);
    summaryByLevel.set('overall', overallDiscrepancies.length);
  }

  // 2. Analizar cada cuenta (si no hay filtro o el filtro es una cuenta específica)
  if (!options.accountFilter || (options.accountFilter !== 'overall')) {
    for (const account of accounts) {
      // Si hay filtro de cuenta y no coincide, saltar
      if (options.accountFilter && options.accountFilter !== account.id) {
        continue;
      }
      
      console.log('');
      log('INFO', `Analizando cuenta ${account.id} (${account.name || 'Sin nombre'})...`);
      
      const accountDocs = await getPerformanceDocuments(
        options.userId,
        account.id,
        options.startDate,
        options.endDate
      );
      
      if (accountDocs.length === 0) {
        log('SKIP', `No hay documentos para cuenta ${account.id}`);
        continue;
      }
      
      log('INFO', `Encontrados ${accountDocs.length} documentos para cuenta ${account.id}`);
      
      const accountDiscrepancies = await analyzeDocumentDiscrepancies(
        accountDocs,
        account.id,
        options.threshold
      );
      
      if (accountDiscrepancies.length > 0) {
        log('INFO', `Discrepancias encontradas en cuenta ${account.id}: ${accountDiscrepancies.length}`);
        allDiscrepancies.push(...accountDiscrepancies);
        summaryByLevel.set(account.id, accountDiscrepancies.length);
      } else {
        log('SUCCESS', `Sin discrepancias en cuenta ${account.id}`);
      }
    }
  }

  // 3. Resumen
  console.log('');
  console.log('='.repeat(100));
  console.log('RESUMEN DE DISCREPANCIAS');
  console.log('='.repeat(100));
  
  summaryByLevel.forEach((count, level) => {
    console.log(`${level === 'overall' ? 'OVERALL' : `Cuenta ${level}`}: ${count} documentos con discrepancias`);
  });
  console.log(`TOTAL: ${allDiscrepancies.length} documentos a corregir`);
  console.log('');

  if (allDiscrepancies.length === 0) {
    log('SUCCESS', 'No se encontraron discrepancias significativas');
    process.exit(0);
  }

  // 4. Modo Analyze - solo mostrar
  if (options.mode === 'analyze') {
    log('INFO', 'Modo ANALYZE: Solo se muestran las discrepancias');
    
    // Mostrar algunas discrepancias de ejemplo
    console.log('');
    console.log('Ejemplo de discrepancias (primeras 5):');
    allDiscrepancies.slice(0, 5).forEach(d => {
      console.log(`  ${d.date} (${d.level}):`);
      Object.entries(d.currencies).forEach(([currency, data]) => {
        console.log(`    ${currency}:`);
        data.discrepancies.forEach(disc => {
          const calcStr = disc.isPercentage 
            ? `${disc.calculated.toFixed(4)}%` 
            : `$${disc.calculated.toFixed(2)}`;
          const storedStr = disc.isPercentage 
            ? `${disc.stored.toFixed(4)}%` 
            : `$${disc.stored.toFixed(2)}`;
          console.log(`      ${disc.label}: calc=${calcStr} stored=${storedStr} (diff=${disc.diff.toFixed(2)}%)`);
        });
      });
    });
    
    console.log('');
    log('INFO', 'Ejecuta con --dry-run para ver los cambios propuestos');
    log('INFO', 'Ejecuta con --fix para aplicar las correcciones');
    process.exit(0);
  }

  // 5. Generar actualizaciones
  log('INFO', 'Generando actualizaciones...');
  const allUpdates = generateUpdates(allDiscrepancies);
  log('INFO', `Total de actualizaciones generadas: ${allUpdates.length}`);

  // 6. Modo Dry-run - mostrar cambios sin aplicar
  if (options.mode === 'dry-run') {
    console.log('');
    console.log('='.repeat(100));
    console.log('MODO DRY-RUN: Cambios propuestos (no aplicados)');
    console.log('='.repeat(100));
    
    const samplesToShow = Math.min(10, allUpdates.length);
    console.log(`\nMostrando ${samplesToShow} de ${allUpdates.length} actualizaciones:\n`);
    
    allUpdates.slice(0, samplesToShow).forEach((update, idx) => {
      console.log(`${idx + 1}. ${update.ref.path} (${update.date})`);
      console.log(`   Monedas afectadas: ${update.currenciesAffected.join(', ')}`);
      
      // Mostrar algunos campos
      Object.entries(update.data).slice(0, 6).forEach(([key, value]) => {
        if (typeof value === 'number') {
          console.log(`   ${key}: ${value.toFixed(4)}`);
        }
      });
      console.log('');
    });
    
    if (allUpdates.length > samplesToShow) {
      console.log(`... y ${allUpdates.length - samplesToShow} actualizaciones más`);
    }
    
    console.log('');
    log('WARNING', 'Modo DRY-RUN: No se aplicaron cambios');
    log('INFO', 'Ejecuta con --fix para aplicar las correcciones');
    process.exit(0);
  }

  // 7. Modo Fix - aplicar cambios
  if (options.mode === 'fix') {
    console.log('');
    console.log('='.repeat(100));
    console.log('MODO FIX: Aplicando correcciones');
    console.log('='.repeat(100));
    
    console.log('');
    log('WARNING', `Se van a modificar ${allUpdates.length} documentos`);
    log('WARNING', 'Esta operación actualiza las métricas agregadas del documento');
    log('WARNING', 'Los datos de assetPerformance NO serán modificados');
    console.log('');
    
    const applied = await applyUpdates(allUpdates);
    
    console.log('');
    log('SUCCESS', `✅ Se aplicaron ${applied} correcciones exitosamente`);
    
    // Resumen final
    console.log('');
    console.log('='.repeat(100));
    console.log('RESUMEN DE CORRECCIONES APLICADAS');
    console.log('='.repeat(100));
    console.log(`Usuario: ${options.userId}`);
    console.log(`Período: ${options.startDate} a ${options.endDate}`);
    console.log(`Documentos corregidos: ${applied}`);
    summaryByLevel.forEach((count, level) => {
      console.log(`  - ${level === 'overall' ? 'OVERALL' : `Cuenta ${level}`}: ${count}`);
    });
  }

  process.exit(0);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
