/**
 * Script de Corrección de Overall Performance TWR
 * 
 * PROPÓSITO:
 * Corregir inconsistencias en el adjustedDailyChangePercentage del OVERALL
 * en portfolioPerformance/{userId}/dates donde el valor almacenado no coincide
 * con la fórmula correcta:
 * 
 *   adjustedDailyChange = (currValue - prevValue + cashFlow) / prevValue × 100
 * 
 * PROBLEMA QUE RESUELVE:
 * Se detectaron días donde el adjustedDailyChangePercentage almacenado difiere
 * del cálculo correcto, causando una diferencia acumulada de ~1% en el TWR anual.
 * 
 * USO:
 *   node fixOverallPerformance.js --analyze          # Identificar inconsistencias
 *   node fixOverallPerformance.js --dry-run          # Ver cambios sin aplicar
 *   node fixOverallPerformance.js --fix              # Aplicar correcciones
 * 
 * OPCIONES:
 *   --user=<userId>        # Usuario específico (default: DDeR8P5hYgfuN8gcU4RsQfdTJqx2)
 *   --start=YYYY-MM-DD     # Fecha inicio (default: 2024-08-01)
 *   --end=YYYY-MM-DD       # Fecha fin (default: hoy)
 *   --threshold=<number>   # Umbral de discrepancia en % (default: 0.01)
 *   --currency=<code>      # Moneda a analizar (default: USD)
 * 
 * @see getMultiAccountHistoricalReturns (Cloud Function corregida)
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
  DEFAULT_THRESHOLD: 0.01, // 0.01% de diferencia
  BATCH_SIZE: 400,
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
};

// ============================================================================
// UTILIDADES
// ============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    mode: 'analyze', // analyze, dry-run, fix
    userId: CONFIG.DEFAULT_USER_ID,
    startDate: CONFIG.DEFAULT_START_DATE,
    endDate: DateTime.now().setZone('America/New_York').toISODate(),
    threshold: CONFIG.DEFAULT_THRESHOLD,
    currency: 'USD',
  };

  args.forEach(arg => {
    if (arg === '--analyze') options.mode = 'analyze';
    else if (arg === '--dry-run') options.mode = 'dry-run';
    else if (arg === '--fix') options.mode = 'fix';
    else if (arg.startsWith('--user=')) options.userId = arg.split('=')[1];
    else if (arg.startsWith('--start=')) options.startDate = arg.split('=')[1];
    else if (arg.startsWith('--end=')) options.endDate = arg.split('=')[1];
    else if (arg.startsWith('--threshold=')) options.threshold = parseFloat(arg.split('=')[1]);
    else if (arg.startsWith('--currency=')) options.currency = arg.split('=')[1];
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

/**
 * Obtener todos los documentos de overall performance
 */
async function getOverallPerformance(userId, startDate, endDate) {
  const snapshot = await db.collection(`portfolioPerformance/${userId}/dates`)
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs;
}

// ============================================================================
// ANÁLISIS DE DISCREPANCIAS
// ============================================================================

/**
 * Analizar discrepancias en el overall
 * 
 * La fórmula correcta es:
 *   adjustedChange = (currValue - prevValue + cashFlow) / prevValue × 100
 * 
 * Donde:
 *   - currValue = totalValue del día actual
 *   - prevValue = totalValue del día anterior
 *   - cashFlow = totalCashFlow del día actual
 */
function analyzeDiscrepancies(docs, currency, threshold) {
  const discrepancies = [];
  let previousDoc = null;
  
  for (let i = 0; i < docs.length; i++) {
    const doc = docs[i];
    const data = doc.data();
    const currencyData = data[currency];
    
    if (!currencyData) continue;
    
    const date = data.date;
    const currValue = currencyData.totalValue || 0;
    const cashFlow = currencyData.totalCashFlow || 0;
    const storedAdjustedChange = currencyData.adjustedDailyChangePercentage || 0;
    const storedRawChange = currencyData.rawDailyChangePercentage || currencyData.dailyChangePercentage || 0;
    
    // Necesitamos el documento anterior para calcular
    if (!previousDoc) {
      previousDoc = doc;
      continue;
    }
    
    const previousData = previousDoc.data();
    const previousCurrencyData = previousData[currency];
    
    if (!previousCurrencyData) {
      previousDoc = doc;
      continue;
    }
    
    const prevValue = previousCurrencyData.totalValue || 0;
    
    // Calcular el valor esperado
    let expectedAdjustedChange = 0;
    let expectedRawChange = 0;
    
    if (prevValue > 0) {
      // Fórmula correcta: (currValue - prevValue + cashFlow) / prevValue × 100
      expectedAdjustedChange = ((currValue - prevValue + cashFlow) / prevValue) * 100;
      expectedRawChange = ((currValue - prevValue) / prevValue) * 100;
    }
    
    // Calcular diferencia
    const diffAdjusted = Math.abs(storedAdjustedChange - expectedAdjustedChange);
    const diffRaw = Math.abs(storedRawChange - expectedRawChange);
    
    // Si supera el umbral, registrar como discrepancia
    if (diffAdjusted > threshold || diffRaw > threshold) {
      discrepancies.push({
        date,
        docRef: doc.ref,
        currency,
        prevValue,
        currValue,
        cashFlow,
        stored: {
          adjustedDailyChangePercentage: storedAdjustedChange,
          rawDailyChangePercentage: storedRawChange,
        },
        expected: {
          adjustedDailyChangePercentage: expectedAdjustedChange,
          rawDailyChangePercentage: expectedRawChange,
        },
        diff: {
          adjusted: diffAdjusted,
          raw: diffRaw,
        },
      });
    }
    
    previousDoc = doc;
  }
  
  return discrepancies;
}

/**
 * Calcular impacto acumulado en TWR
 */
function calculateTWRImpact(docs, discrepancies, currency) {
  // TWR con valores almacenados
  let factorStored = 1;
  // TWR con valores corregidos
  let factorCorrected = 1;
  
  // Crear mapa de correcciones por fecha
  const correctionsMap = new Map();
  discrepancies.forEach(d => {
    correctionsMap.set(d.date, d.expected.adjustedDailyChangePercentage);
  });
  
  docs.forEach(doc => {
    const data = doc.data();
    const currencyData = data[currency];
    if (!currencyData) return;
    
    const date = data.date;
    const storedChange = currencyData.adjustedDailyChangePercentage || 0;
    
    // Usar corrección si existe, sino usar el valor almacenado
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

/**
 * Aplicar correcciones a Firestore
 */
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
    // El cambio porcentual es el mismo para todas las monedas (es un %)
    const update = {};
    
    CONFIG.CURRENCIES.forEach(curr => {
      update[`${curr}.adjustedDailyChangePercentage`] = d.expected.adjustedDailyChangePercentage;
      update[`${curr}.rawDailyChangePercentage`] = d.expected.rawDailyChangePercentage;
      update[`${curr}.dailyChangePercentage`] = d.expected.rawDailyChangePercentage;
      update[`${curr}.dailyReturn`] = d.expected.adjustedDailyChangePercentage / 100;
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
      log('CHANGE', `${d.date}: ${d.stored.adjustedDailyChangePercentage.toFixed(4)}% → ${d.expected.adjustedDailyChangePercentage.toFixed(4)}% (diff: ${d.diff.adjusted.toFixed(4)}%)`);
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
  console.log('  CORRECCIÓN DE OVERALL PERFORMANCE TWR');
  console.log('═'.repeat(100));
  console.log('');
  console.log(`Modo: ${options.mode.toUpperCase()}`);
  console.log(`Usuario: ${options.userId}`);
  console.log(`Período: ${options.startDate} a ${options.endDate}`);
  console.log(`Moneda: ${options.currency}`);
  console.log(`Umbral de discrepancia: ${options.threshold}%`);
  console.log('');
  
  // 1. Obtener documentos de overall
  log('INFO', 'Obteniendo documentos de overall performance...');
  const docs = await getOverallPerformance(options.userId, options.startDate, options.endDate);
  log('SUCCESS', `Encontrados ${docs.length} documentos`);
  
  if (docs.length === 0) {
    log('WARNING', 'No hay documentos para analizar');
    process.exit(0);
  }
  
  // 2. Analizar discrepancias
  log('INFO', 'Analizando discrepancias...');
  const discrepancies = analyzeDiscrepancies(docs, options.currency, options.threshold);
  
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
  
  // Mostrar las 10 mayores discrepancias
  const topDiscrepancies = [...discrepancies]
    .sort((a, b) => b.diff.adjusted - a.diff.adjusted)
    .slice(0, 10);
  
  console.log('Top 10 discrepancias por magnitud:');
  console.log('');
  console.log('Fecha       | Almacenado  | Esperado    | Diferencia  | CashFlow');
  console.log('------------|-------------|-------------|-------------|----------');
  
  topDiscrepancies.forEach(d => {
    console.log(
      `${d.date} | ` +
      `${d.stored.adjustedDailyChangePercentage.toFixed(4).padStart(10)}% | ` +
      `${d.expected.adjustedDailyChangePercentage.toFixed(4).padStart(10)}% | ` +
      `${d.diff.adjusted.toFixed(4).padStart(10)}% | ` +
      `${d.cashFlow.toFixed(2)}`
    );
  });
  
  console.log('');
  
  // 3. Calcular impacto en TWR
  console.log('─'.repeat(100));
  console.log('  IMPACTO EN TWR ACUMULADO');
  console.log('─'.repeat(100));
  console.log('');
  
  const impact = calculateTWRImpact(docs, discrepancies, options.currency);
  
  console.log(`TWR con datos almacenados: ${impact.twrStored.toFixed(4)}%`);
  console.log(`TWR con datos corregidos:  ${impact.twrCorrected.toFixed(4)}%`);
  console.log(`Diferencia:                ${impact.difference.toFixed(4)}%`);
  console.log('');
  
  // 4. Ejecutar según el modo
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
    }
    
    console.log('');
    process.exit(0);
  }
}

main().catch(err => {
  log('ERROR', 'Error fatal:', { message: err.message, stack: err.stack });
  process.exit(1);
});
