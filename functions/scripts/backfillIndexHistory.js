#!/usr/bin/env node
/**
 * Backfill Index History Script
 * 
 * Este script recupera datos históricos de índices de mercado desde el API
 * /v1/historical y los guarda en Firestore para llenar gaps de datos.
 * 
 * Uso:
 *   node scripts/backfillIndexHistory.js [--dry-run] [--days=7] [--indices=GSPC,DJI]
 * 
 * Opciones:
 *   --dry-run     No escribe en Firestore, solo muestra lo que haría
 *   --days=N      Número de días hacia atrás a verificar (default: 7)
 *   --indices=X   Lista de índices separados por coma (default: todos)
 * 
 * @see docs/architecture/OPT-009-index-history-backfill.md
 */

require('dotenv').config();
const admin = require('firebase-admin');
const axios = require('axios');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

const FINANCE_QUERY_API_URL = process.env.FINANCE_QUERY_API_URL || 'https://ws.portastock.top/v1';
const SERVICE_TOKEN = process.env.CF_SERVICE_TOKEN || '';

// Mapeo de códigos internos a símbolos de Yahoo Finance
const INDEX_SYMBOL_MAP = {
  'GSPC': '^GSPC',      // S&P 500
  'DJI': '^DJI',        // Dow Jones
  'IXIC': '^IXIC',      // NASDAQ
  'RUT': '^RUT',        // Russell 2000
  'VIX': '^VIX',        // CBOE Volatility
  'FTSE': '^FTSE',      // FTSE 100
  'GDAXI': '^GDAXI',    // DAX
  'FCHI': '^FCHI',      // CAC 40
  'STOXX50E': '^STOXX50E', // Euro Stoxx 50
  'IBEX': '^IBEX',      // IBEX 35
  'FTSEMIB': '^FTSEMIB', // FTSE MIB
  'SSMI': '^SSMI',      // SMI
  'N225': '^N225',      // Nikkei 225
  'HSI': '^HSI',        // Hang Seng
  'SHANGHAI': '000001.SS', // Shanghai Composite
  'SZSE': '399001.SZ',  // Shenzhen Index
  'KS11': '^KS11',      // KOSPI
  'TWII': '^TWII',      // TWSE
  'BSESN': '^BSESN',    // BSE Sensex
  'BVSP': '^BVSP',      // Bovespa
  'MXX': '^MXX',        // IPC Mexico
  'MERV': '^MERV',      // MERVAL
  'IPSA': '^IPSA',      // IPSA Chile
};

// ============================================================================
// INICIALIZACIÓN FIREBASE
// ============================================================================

// Inicializar Firebase Admin (usa las credenciales del entorno)
if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.applicationDefault(),
  });
}

const db = admin.firestore();

// ============================================================================
// FUNCIONES AUXILIARES
// ============================================================================

/**
 * Obtiene los headers para llamadas al API
 */
function getServiceHeaders() {
  return {
    'Content-Type': 'application/json',
    'User-Agent': 'backfill-script/1.0 portafolio-inversiones',
    'Referer': 'https://us-central1-portafolio-inversiones.cloudfunctions.net',
    ...(SERVICE_TOKEN ? { 'x-service-token': SERVICE_TOKEN } : {}),
  };
}

/**
 * Obtiene datos históricos de un índice desde el API
 * @param {string} symbol - Símbolo de Yahoo Finance (ej: ^GSPC)
 * @returns {Promise<Object>} Datos históricos
 */
async function fetchHistoricalData(symbol) {
  const url = `${FINANCE_QUERY_API_URL}/historical`;
  const params = {
    symbol,
    range: '1mo',
    interval: '1d',
  };

  try {
    const response = await axios.get(url, {
      params,
      headers: getServiceHeaders(),
      timeout: 30000,
    });
    return response.data;
  } catch (error) {
    if (error.response?.status === 404) {
      console.warn(`  ⚠️ Símbolo ${symbol} no encontrado`);
      return null;
    }
    throw error;
  }
}

/**
 * Obtiene las fechas que ya existen en Firestore para un índice
 * @param {string} code - Código del índice
 * @returns {Promise<Set<string>>} Set de fechas existentes
 */
async function getExistingDates(code) {
  const datesRef = db.collection('indexHistories').doc(code).collection('dates');
  const snapshot = await datesRef.select('date').get();
  
  const dates = new Set();
  snapshot.docs.forEach(doc => {
    const data = doc.data();
    if (data.date) {
      dates.add(data.date);
    }
  });
  
  return dates;
}

/**
 * Calcula el cambio porcentual entre dos valores
 */
function calculatePercentChange(current, previous) {
  if (!previous || previous === 0) return 0;
  return ((current - previous) / previous) * 100;
}

/**
 * Verifica si una fecha es un día de mercado (lunes a viernes)
 */
function isMarketDay(dateStr) {
  const date = new Date(dateStr + 'T12:00:00Z');
  const day = date.getUTCDay();
  return day !== 0 && day !== 6; // No domingo ni sábado
}

/**
 * Genera lista de fechas de los últimos N días hábiles
 */
function getLastNMarketDays(n) {
  const dates = [];
  const today = new Date();
  let current = new Date(today);
  
  while (dates.length < n) {
    current.setDate(current.getDate() - 1);
    const dateStr = current.toISOString().split('T')[0];
    if (isMarketDay(dateStr)) {
      dates.push(dateStr);
    }
  }
  
  return dates;
}

// ============================================================================
// BACKFILL LOGIC
// ============================================================================

/**
 * Ejecuta el backfill para un índice específico
 * @param {string} code - Código del índice
 * @param {boolean} dryRun - Si es true, no escribe en Firestore
 * @param {number} daysBack - Número de días hacia atrás a verificar
 * @returns {Promise<Object>} Resultados del backfill
 */
async function backfillIndex(code, dryRun = false, daysBack = 7) {
  const symbol = INDEX_SYMBOL_MAP[code];
  if (!symbol) {
    return { code, status: 'skipped', reason: 'No symbol mapping' };
  }

  console.log(`\n📊 Procesando ${code} (${symbol})...`);

  try {
    // 1. Obtener fechas existentes
    const existingDates = await getExistingDates(code);
    console.log(`  📅 Fechas existentes en Firestore: ${existingDates.size}`);

    // 2. Obtener datos históricos del API
    const historicalData = await fetchHistoricalData(symbol);
    if (!historicalData || Object.keys(historicalData).length === 0) {
      return { code, status: 'skipped', reason: 'No historical data available' };
    }

    // 3. Identificar fechas faltantes
    const targetDates = getLastNMarketDays(daysBack);
    const apiDates = Object.keys(historicalData).sort();
    
    console.log(`  📈 Fechas del API: ${apiDates.length} (${apiDates[apiDates.length - 1]} a ${apiDates[0]})`);

    const missingDates = targetDates.filter(date => 
      !existingDates.has(date) && historicalData[date]
    );

    if (missingDates.length === 0) {
      console.log(`  ✅ No hay fechas faltantes`);
      return { code, status: 'ok', inserted: 0 };
    }

    console.log(`  🔍 Fechas faltantes: ${missingDates.join(', ')}`);

    // 4. Preparar datos para insertar
    const batch = db.batch();
    let insertCount = 0;

    for (const date of missingDates) {
      const dayData = historicalData[date];
      if (!dayData || !dayData.close) continue;

      // Buscar el día anterior para calcular el cambio
      const sortedDates = apiDates.sort();
      const dateIndex = sortedDates.indexOf(date);
      const previousDate = dateIndex > 0 ? sortedDates[dateIndex - 1] : null;
      const previousData = previousDate ? historicalData[previousDate] : null;

      const percentChange = previousData 
        ? calculatePercentChange(dayData.close, previousData.close)
        : 0;

      const docRef = db
        .collection('indexHistories')
        .doc(code)
        .collection('dates')
        .doc(date);

      const docData = {
        date,
        score: dayData.close,
        change: previousData 
          ? (dayData.close - previousData.close).toFixed(2)
          : '0.00',
        percentChange: Math.round(percentChange * 100) / 100,
        timestamp: Date.now(),
        captureType: 'backfill',
        source: 'historical-api',
      };

      if (dryRun) {
        console.log(`    [DRY-RUN] Insertaría ${date}: score=${docData.score}, change=${docData.percentChange}%`);
      } else {
        batch.set(docRef, docData, { merge: true });
      }

      insertCount++;
    }

    // 5. Ejecutar batch
    if (!dryRun && insertCount > 0) {
      await batch.commit();
      console.log(`  ✅ Insertados ${insertCount} documentos`);
    }

    return { code, status: 'ok', inserted: insertCount };

  } catch (error) {
    console.error(`  ❌ Error: ${error.message}`);
    return { code, status: 'error', error: error.message };
  }
}

/**
 * Invalida el cache de índices después del backfill
 */
async function invalidateIndexCaches() {
  console.log('\n🗑️ Invalidando caches de índices...');
  
  const snapshot = await db.collection('indexCache').get();
  
  if (snapshot.empty) {
    console.log('  No hay caches para invalidar');
    return 0;
  }
  
  const batch = db.batch();
  snapshot.docs.forEach(doc => batch.delete(doc.ref));
  await batch.commit();
  
  console.log(`  ✅ ${snapshot.size} caches invalidados`);
  return snapshot.size;
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║          BACKFILL INDEX HISTORY SCRIPT                     ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log();

  // Parse arguments
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  
  const daysArg = args.find(a => a.startsWith('--days='));
  const daysBack = daysArg ? parseInt(daysArg.split('=')[1], 10) : 7;
  
  const indicesArg = args.find(a => a.startsWith('--indices='));
  const selectedIndices = indicesArg 
    ? indicesArg.split('=')[1].split(',')
    : Object.keys(INDEX_SYMBOL_MAP);

  console.log(`📋 Configuración:`);
  console.log(`   Dry Run: ${dryRun ? 'SÍ (no se escribirá en Firestore)' : 'NO'}`);
  console.log(`   Días hacia atrás: ${daysBack}`);
  console.log(`   Índices: ${selectedIndices.length}`);
  console.log(`   API URL: ${FINANCE_QUERY_API_URL}`);
  console.log(`   Token configurado: ${SERVICE_TOKEN ? 'Sí' : 'No'}`);

  if (!SERVICE_TOKEN) {
    console.warn('\n⚠️  ADVERTENCIA: CF_SERVICE_TOKEN no está configurado.');
    console.warn('   Configúralo en .env o ejecuta:');
    console.warn('   export CF_SERVICE_TOKEN=$(firebase functions:secrets:access CF_SERVICE_TOKEN)');
  }

  const results = {
    processed: 0,
    inserted: 0,
    skipped: 0,
    errors: 0,
  };

  // Procesar cada índice
  for (const code of selectedIndices) {
    const result = await backfillIndex(code, dryRun, daysBack);
    
    results.processed++;
    if (result.status === 'ok') {
      results.inserted += result.inserted || 0;
    } else if (result.status === 'skipped') {
      results.skipped++;
    } else if (result.status === 'error') {
      results.errors++;
    }

    // Pequeña pausa para no sobrecargar el API
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  // Invalidar caches si se insertaron datos
  if (!dryRun && results.inserted > 0) {
    await invalidateIndexCaches();
  }

  // Resumen
  console.log('\n╔════════════════════════════════════════════════════════════╗');
  console.log('║                        RESUMEN                              ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log(`   Índices procesados: ${results.processed}`);
  console.log(`   Documentos insertados: ${results.inserted}`);
  console.log(`   Índices omitidos: ${results.skipped}`);
  console.log(`   Errores: ${results.errors}`);
  
  if (dryRun) {
    console.log('\n🔄 Este fue un DRY RUN. Ejecuta sin --dry-run para aplicar cambios.');
  }

  process.exit(results.errors > 0 ? 1 : 0);
}

main().catch(error => {
  console.error('Error fatal:', error);
  process.exit(1);
});
