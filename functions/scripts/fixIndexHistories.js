#!/usr/bin/env node
/**
 * fixIndexHistories.js - Corrige datos inconsistentes en indexHistories
 * 
 * PROBLEMAS QUE CORRIGE:
 * 1. Datos INTRADAY: scores capturados antes del cierre (apertura/media mañana)
 *    que nunca se sobreescribieron con el precio de cierre.
 * 2. Datos en FESTIVOS NYSE: documentos que no deberían existir.
 * 3. change/percentChange inconsistentes: recalcula basándose en score día anterior.
 * 
 * FUENTE DE VERDAD:
 * Usa el endpoint /v1/historical de finance-query (Yahoo Finance) que devuelve
 * precios OHLCV de cierre definitivos.
 * 
 * USO:
 *   node fixIndexHistories.js --dry-run                    # Ver cambios sin aplicar
 *   node fixIndexHistories.js --fix                        # Aplicar correcciones
 *   node fixIndexHistories.js --dry-run --indices=GSPC     # Solo S&P 500
 *   node fixIndexHistories.js --fix --indices=GSPC,DJI     # S&P 500 y Dow Jones
 *   node fixIndexHistories.js --dry-run --start=2026-01-01 # Desde una fecha específica
 *   node fixIndexHistories.js --fix --delete-holidays      # Además elimina docs en festivos
 * 
 * @see docs/architecture/firebase-cost-analysis-detailed.md
 */

const admin = require('firebase-admin');
const axios = require('axios');

// ============================================================================
// INICIALIZACIÓN FIREBASE
// ============================================================================
const serviceAccount = require('../key.json');

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
  // API para precios históricos de cierre (Yahoo Finance via finance-query)
  HISTORICAL_API_BASE: 'https://api.portastock.net/v1',
  
  API_HEADERS: {
    'x-service-token': '26ca00231ead1b5fbd63c6bba10a16e2f619b56809013ab3b3bcbbfb029aff10',
    'origin': 'https://portastock.net',
    'referer': 'https://portastock.net'
  },

  // Mapeo de códigos internos a símbolos Yahoo Finance
  INDEX_SYMBOLS: {
    'GSPC': '^GSPC',      // S&P 500
    'DJI': '^DJI',        // Dow Jones
    'IXIC': '^IXIC',      // NASDAQ
    'RUT': '^RUT',        // Russell 2000
    'VIX': '^VIX',        // VIX
  },

  // Festivos NYSE (para detección y eliminación)
  NYSE_HOLIDAYS: new Set([
    // 2025
    '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18', '2025-05-26',
    '2025-06-19', '2025-07-04', '2025-09-01', '2025-11-27', '2025-12-25',
    // 2026
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25',
    '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25',
    // 2027
    '2027-01-01', '2027-01-18', '2027-02-15', '2027-03-26', '2027-05-31',
    '2027-06-18', '2027-07-05', '2027-09-06', '2027-11-25', '2027-12-24',
  ]),

  // Hora de cierre NYSE en UTC: 21:00 UTC (16:00 ET en winter) / 20:00 UTC (EDT)
  // Usamos un umbral conservador: cualquier captura antes de las 20:00 UTC se considera intraday
  NYSE_CLOSE_THRESHOLD_UTC_HOUR: 20,

  // Rate limiting
  API_DELAY_MS: 500,
  BATCH_SIZE: 400,
};

// ============================================================================
// PARSING DE ARGUMENTOS
// ============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    mode: 'dry-run',
    indices: Object.keys(CONFIG.INDEX_SYMBOLS), // todos los US por defecto
    startDate: '2025-12-30', // incluir cierre de 2025 para referencia
    endDate: new Date().toISOString().split('T')[0], // hoy
    deleteHolidays: false,
  };

  args.forEach(arg => {
    if (arg === '--dry-run') options.mode = 'dry-run';
    else if (arg === '--fix') options.mode = 'fix';
    else if (arg === '--delete-holidays') options.deleteHolidays = true;
    else if (arg.startsWith('--indices=')) options.indices = arg.split('=')[1].split(',');
    else if (arg.startsWith('--start=')) options.startDate = arg.split('=')[1];
    else if (arg.startsWith('--end=')) options.endDate = arg.split('=')[1];
  });

  return options;
}

// ============================================================================
// FUNCIONES DE API
// ============================================================================

/**
 * Obtiene datos históricos OHLCV de cierre para un símbolo
 * @param {string} yahooSymbol - Símbolo Yahoo Finance (ej: ^GSPC)
 * @returns {Promise<Object>} Mapa de { fecha: { open, high, low, close, volume } }
 */
async function fetchHistoricalClosePrices(yahooSymbol) {
  const url = `${CONFIG.HISTORICAL_API_BASE}/historical?symbol=${encodeURIComponent(yahooSymbol)}&range=6mo&interval=1d`;
  
  try {
    const response = await axios.get(url, {
      headers: CONFIG.API_HEADERS,
      timeout: 30000,
    });
    return response.data; // { "2026-01-02": { open, high, low, close, volume }, ... }
  } catch (error) {
    console.error(`  ❌ Error fetching ${yahooSymbol}: ${error.message}`);
    return null;
  }
}

// ============================================================================
// FUNCIONES DE DIAGNÓSTICO
// ============================================================================

function isWeekend(dateStr) {
  const d = new Date(dateStr + 'T12:00:00Z');
  const day = d.getUTCDay();
  return day === 0 || day === 6;
}

function isHoliday(dateStr) {
  return CONFIG.NYSE_HOLIDAYS.has(dateStr);
}

/**
 * Determina si una captura fue intraday (antes del cierre del mercado)
 */
function isIntradayCapture(timestamp) {
  if (!timestamp) return false;
  const ts = new Date(timestamp);
  const hourUTC = ts.getUTCHours();
  // Si fue capturado entre 13:00-20:00 UTC, es durante horario NYSE (intraday)
  return hourUTC >= 13 && hourUTC < CONFIG.NYSE_CLOSE_THRESHOLD_UTC_HOUR;
}

// ============================================================================
// LÓGICA PRINCIPAL
// ============================================================================

async function fixIndex(indexCode, options) {
  const yahooSymbol = CONFIG.INDEX_SYMBOLS[indexCode];
  if (!yahooSymbol) {
    console.log(`  ⚠️ No hay mapeo para ${indexCode}, saltando`);
    return { fixed: 0, deleted: 0, errors: 0 };
  }

  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  📊 ${indexCode} (${yahooSymbol})`);
  console.log(`${'═'.repeat(70)}`);

  // 1. Obtener precios históricos REALES de cierre
  console.log(`  🔄 Obteniendo precios de cierre reales...`);
  const historicalData = await fetchHistoricalClosePrices(yahooSymbol);
  
  if (!historicalData || Object.keys(historicalData).length === 0) {
    console.log(`  ❌ No se obtuvieron datos históricos`);
    return { fixed: 0, deleted: 0, errors: 0 };
  }
  
  const historicalDates = Object.keys(historicalData).sort();
  console.log(`  ✅ ${historicalDates.length} días de precios de cierre (${historicalDates[0]} → ${historicalDates[historicalDates.length - 1]})`);

  // 2. Obtener datos actuales en Firestore
  console.log(`  🔄 Leyendo datos actuales de Firestore...`);
  const datesRef = db.collection('indexHistories').doc(indexCode).collection('dates');
  const snapshot = await datesRef
    .where('date', '>=', options.startDate)
    .orderBy('date', 'asc')
    .get();
  
  console.log(`  📋 ${snapshot.size} documentos en Firestore desde ${options.startDate}`);

  // 3. Clasificar problemas
  const problems = {
    intraday: [],      // score capturado antes del cierre
    holiday: [],       // documento en día festivo
    calcMismatch: [],  // change/percentChange incorrectos
    missing: [],       // día hábil sin documento pero con precio disponible
  };

  const firestoreData = new Map();
  snapshot.docs.forEach(doc => {
    firestoreData.set(doc.id, doc.data());
  });

  // Iterar documentos existentes para detectar problemas
  firestoreData.forEach((data, date) => {
    // a) Check festivo
    if (isHoliday(date) || isWeekend(date)) {
      problems.holiday.push({ date, data, reason: isWeekend(date) ? 'weekend' : 'holiday' });
      return;
    }
    
    // b) Check intraday
    if (isIntradayCapture(data.timestamp)) {
      const closePrice = historicalData[date]?.close;
      problems.intraday.push({
        date,
        storedScore: data.score,
        closePrice: closePrice || null,
        timestamp: new Date(data.timestamp).toISOString(),
        hasHistorical: !!closePrice,
      });
    }
  });

  // Buscar días faltantes (hábiles con precio disponible pero sin doc en Firestore)
  historicalDates.forEach(date => {
    if (date < options.startDate || date > options.endDate) return;
    if (isHoliday(date) || isWeekend(date)) return;
    if (!firestoreData.has(date)) {
      problems.missing.push({ date, closePrice: historicalData[date].close });
    }
  });

  // Verificar cálculos de change/percentChange para documentos que no son intraday ni festivo
  const sortedDates = [...firestoreData.keys()].sort();
  for (let i = 1; i < sortedDates.length; i++) {
    const date = sortedDates[i];
    const prevDate = sortedDates[i - 1];
    const data = firestoreData.get(date);
    const prevData = firestoreData.get(prevDate);
    
    if (isHoliday(date) || isWeekend(date)) continue;
    // Skip si ya está marcado como intraday (se corregirá completamente)
    if (problems.intraday.some(p => p.date === date)) continue;
    
    const expectedChange = data.score - prevData.score;
    const expectedPct = (expectedChange / prevData.score) * 100;
    const storedPct = data.percentChange || 0;
    
    if (Math.abs(expectedPct - storedPct) > 0.05) {
      problems.calcMismatch.push({
        date,
        score: data.score,
        prevScore: prevData.score,
        expectedChange: parseFloat(expectedChange.toFixed(2)),
        storedChange: data.change,
        expectedPct: parseFloat(expectedPct.toFixed(2)),
        storedPct,
      });
    }
  }

  // 4. Reportar problemas
  console.log(`\n  📊 PROBLEMAS ENCONTRADOS:`);
  console.log(`     🔴 Datos INTRADAY: ${problems.intraday.length}`);
  problems.intraday.forEach(p => {
    const diff = p.closePrice ? ` (diff: ${(p.storedScore - p.closePrice).toFixed(2)})` : ' (sin precio de cierre)';
    console.log(`        ${p.date}: stored=${p.storedScore}, close=${p.closePrice || 'N/A'}${diff}`);
  });
  
  console.log(`     ⚠️ Datos en FESTIVOS/FINES DE SEMANA: ${problems.holiday.length}`);
  problems.holiday.forEach(p => {
    console.log(`        ${p.date}: ${p.reason}, score=${p.data.score}`);
  });
  
  console.log(`     🟠 Cálculos incorrectos: ${problems.calcMismatch.length}`);
  problems.calcMismatch.forEach(p => {
    console.log(`        ${p.date}: pct stored=${p.storedPct}% vs expected=${p.expectedPct}%`);
  });
  
  console.log(`     🔵 Días faltantes (con precio disponible): ${problems.missing.length}`);
  problems.missing.forEach(p => {
    console.log(`        ${p.date}: close=${p.closePrice}`);
  });

  const totalProblems = problems.intraday.length + problems.holiday.length + 
                        problems.calcMismatch.length + problems.missing.length;
  
  if (totalProblems === 0) {
    console.log(`\n  ✅ No se encontraron problemas en ${indexCode}`);
    return { fixed: 0, deleted: 0, errors: 0 };
  }

  console.log(`\n  📋 Total problemas: ${totalProblems}`);

  if (options.mode === 'dry-run') {
    console.log(`  ⚠️ DRY-RUN: No se aplicaron cambios`);
    return { fixed: 0, deleted: 0, errors: 0, totalProblems };
  }

  // 5. APLICAR CORRECCIONES (modo --fix)
  console.log(`\n  🔧 APLICANDO CORRECCIONES...`);
  let fixedCount = 0;
  let deletedCount = 0;
  let errorCount = 0;

  // 5a. Corregir datos intraday → sobreescribir con precio de cierre real
  for (const problem of problems.intraday) {
    if (!problem.hasHistorical) {
      console.log(`     ⏭️ ${problem.date}: sin precio de cierre disponible, saltando`);
      errorCount++;
      continue;
    }

    try {
      const closePrice = historicalData[problem.date].close;
      
      // Buscar score del día anterior para calcular change
      const prevDateKey = historicalDates[historicalDates.indexOf(problem.date) - 1];
      const prevClose = prevDateKey ? historicalData[prevDateKey].close : null;
      
      const change = prevClose ? parseFloat((closePrice - prevClose).toFixed(2)) : 0;
      const percentChange = prevClose ? parseFloat(((change / prevClose) * 100).toFixed(2)) : 0;

      const docRef = datesRef.doc(problem.date);
      await docRef.set({
        score: closePrice,
        change: change,
        percentChange: percentChange,
        date: problem.date,
        timestamp: Date.now(),
        captureType: 'fix-close',
        previousScore: problem.storedScore,
        fixedAt: new Date().toISOString(),
      }, { merge: false }); // merge: false para limpiar campos viejos

      console.log(`     ✅ ${problem.date}: ${problem.storedScore} → ${closePrice} (change: ${change}, pct: ${percentChange}%)`);
      fixedCount++;
    } catch (error) {
      console.log(`     ❌ ${problem.date}: ${error.message}`);
      errorCount++;
    }
  }

  // 5b. Eliminar documentos en festivos (si --delete-holidays)
  if (options.deleteHolidays) {
    for (const problem of problems.holiday) {
      try {
        await datesRef.doc(problem.date).delete();
        console.log(`     🗑️ ${problem.date}: eliminado (${problem.reason})`);
        deletedCount++;
      } catch (error) {
        console.log(`     ❌ ${problem.date}: error al eliminar: ${error.message}`);
        errorCount++;
      }
    }
  } else if (problems.holiday.length > 0) {
    console.log(`     ℹ️ ${problems.holiday.length} docs en festivos NO eliminados (uso --delete-holidays para eliminarlos)`);
  }

  // 5c. Corregir cálculos incorrectos de change/percentChange
  for (const problem of problems.calcMismatch) {
    try {
      const docRef = datesRef.doc(problem.date);
      await docRef.update({
        change: problem.expectedChange,
        percentChange: problem.expectedPct,
        fixedAt: new Date().toISOString(),
        fixType: 'calc-correction',
      });
      console.log(`     ✅ ${problem.date}: pct ${problem.storedPct}% → ${problem.expectedPct}%`);
      fixedCount++;
    } catch (error) {
      console.log(`     ❌ ${problem.date}: ${error.message}`);
      errorCount++;
    }
  }

  // 5d. Insertar días faltantes
  for (const problem of problems.missing) {
    try {
      const closePrice = historicalData[problem.date].close;
      const prevDateKey = historicalDates[historicalDates.indexOf(problem.date) - 1];
      const prevClose = prevDateKey ? historicalData[prevDateKey].close : null;
      
      const change = prevClose ? parseFloat((closePrice - prevClose).toFixed(2)) : 0;
      const percentChange = prevClose ? parseFloat(((change / prevClose) * 100).toFixed(2)) : 0;

      const docRef = datesRef.doc(problem.date);
      await docRef.set({
        score: closePrice,
        change: change,
        percentChange: percentChange,
        date: problem.date,
        timestamp: Date.now(),
        captureType: 'backfill',
      });
      console.log(`     ✅ ${problem.date}: insertado score=${closePrice}, pct=${percentChange}%`);
      fixedCount++;
    } catch (error) {
      console.log(`     ❌ ${problem.date}: ${error.message}`);
      errorCount++;
    }
  }

  return { fixed: fixedCount, deleted: deletedCount, errors: errorCount, totalProblems };
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const options = parseArgs();

  console.log('═'.repeat(70));
  console.log('  FIX INDEX HISTORIES - Corrección de datos inconsistentes');
  console.log('═'.repeat(70));
  console.log(`\n📋 Configuración:`);
  console.log(`   Modo: ${options.mode}`);
  console.log(`   Índices: ${options.indices.join(', ')}`);
  console.log(`   Rango: ${options.startDate} → ${options.endDate}`);
  console.log(`   Eliminar festivos: ${options.deleteHolidays}`);

  let totalFixed = 0;
  let totalDeleted = 0;
  let totalErrors = 0;
  let totalProblems = 0;

  for (const indexCode of options.indices) {
    const result = await fixIndex(indexCode, options);
    totalFixed += result.fixed;
    totalDeleted += result.deleted;
    totalErrors += result.errors;
    totalProblems += result.totalProblems || 0;

    // Rate limiting entre índices
    if (options.indices.length > 1) {
      await new Promise(r => setTimeout(r, CONFIG.API_DELAY_MS));
    }
  }

  console.log(`\n${'═'.repeat(70)}`);
  console.log('  RESUMEN');
  console.log('═'.repeat(70));
  console.log(`  📊 Problemas detectados: ${totalProblems}`);
  console.log(`  ✅ Documentos corregidos: ${totalFixed}`);
  console.log(`  🗑️ Documentos eliminados: ${totalDeleted}`);
  console.log(`  ❌ Errores: ${totalErrors}`);
  
  if (options.mode === 'dry-run') {
    console.log(`\n  ⚠️ Este fue un DRY-RUN. Para aplicar los cambios, ejecuta:`);
    console.log(`     node fixIndexHistories.js --fix --indices=${options.indices.join(',')}`);
    if (totalProblems > 0 && !options.deleteHolidays) {
      console.log(`     Agrega --delete-holidays para eliminar docs en festivos`);
    }
  }

  process.exit(0);
}

main().catch(err => {
  console.error('Error fatal:', err);
  process.exit(1);
});
