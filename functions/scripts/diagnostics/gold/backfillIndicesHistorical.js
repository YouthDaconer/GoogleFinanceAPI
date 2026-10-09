/**
 * Script de Backfill CORRECTO para Índices usando Precios Históricos
 * 
 * FIX-INDEX-002: Usa el endpoint /historical para obtener precios de cierre reales
 * NO usa /quotes (que devuelve precios en tiempo real)
 * 
 * USO:
 *   node backfillIndicesHistorical.js --dates=2026-03-05,2026-03-06
 *   node backfillIndicesHistorical.js --dry-run --dates=2026-03-05
 */

const admin = require('firebase-admin');
const axios = require('axios');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// Configuración API
const FINANCE_QUERY_API_URL = 'https://api.portastock.net/v1';
const API_HEADERS = {
  'x-service-token': '26ca00231ead1b5fbd63c6bba10a16e2f619b56809013ab3b3bcbbfb029aff10',
  'origin': 'https://portastock.net',
  'referer': 'https://portastock.net'
};

// Índices clave con símbolos Yahoo Finance
const KEY_INDICES = [
  { symbol: '^GSPC', code: 'GSPC', name: 'S&P 500', region: 'US' },
  { symbol: '^DJI', code: 'DJI', name: 'Dow Jones Industrial Average', region: 'US' },
  { symbol: '^IXIC', code: 'IXIC', name: 'NASDAQ Composite', region: 'US' },
  { symbol: '^RUT', code: 'RUT', name: 'Russell 2000', region: 'US' },
  { symbol: '^VIX', code: 'VIX', name: 'CBOE Volatility Index', region: 'US' },
  { symbol: '^NYA', code: 'NYA', name: 'NYSE Composite', region: 'US' },
];

// ============================================================================
// FUNCIONES
// ============================================================================

/**
 * Obtiene datos históricos de un índice para el último mes
 */
async function fetchHistoricalData(symbol) {
  try {
    const response = await axios.get(
      `${FINANCE_QUERY_API_URL}/historical`,
      { 
        headers: API_HEADERS,
        params: {
          symbol: symbol,
          range: '1mo',
          interval: '1d'
        },
        timeout: 15000
      }
    );
    
    // La respuesta tiene formato: { "2026-03-06": {...}, "2026-03-05": {...} }
    // Las keys son las fechas directamente
    const data = response.data;
    
    // Verificar que hay fechas en la respuesta
    const dateKeys = Object.keys(data).filter(k => /^\d{4}-\d{2}-\d{2}$/.test(k));
    
    if (dateKeys.length === 0) {
      console.log(`    DEBUG: formato inesperado: ${Object.keys(data).slice(0, 3).join(', ')}`);
      return null;
    }
    
    // Convertir al formato esperado: { dates: { "2026-03-06": {...} } }
    return { dates: data };
  } catch (err) {
    console.log(`  ⚠️ Error obteniendo histórico de ${symbol}: ${err.message}`);
    return null;
  }
}

/**
 * Calcula el cambio porcentual entre dos días
 */
function calculatePercentChange(currentClose, previousClose) {
  if (!previousClose || previousClose === 0) return 0;
  return ((currentClose - previousClose) / previousClose) * 100;
}

/**
 * Hace backfill para fechas específicas
 */
async function backfillIndicesForDates(targetDates, dryRun = false) {
  console.log('='.repeat(80));
  console.log(`BACKFILL HISTÓRICO DE ÍNDICES`);
  console.log(`Fechas: ${targetDates.join(', ')}`);
  console.log(`Modo: ${dryRun ? 'DRY-RUN (sin cambios)' : 'APLICAR CAMBIOS'}`);
  console.log('='.repeat(80));
  
  // Recolectar datos históricos de cada índice
  const historicalDataByIndex = new Map();
  
  console.log('\n📡 Obteniendo datos históricos de índices...');
  for (const index of KEY_INDICES) {
    console.log(`  Descargando ${index.code} (${index.symbol})...`);
    const data = await fetchHistoricalData(index.symbol);
    
    if (data && data.dates) {
      historicalDataByIndex.set(index.code, {
        ...index,
        dates: data.dates
      });
      console.log(`    ✅ ${Object.keys(data.dates).length} días disponibles`);
    } else {
      console.log(`    ❌ Sin datos`);
    }
  }
  
  // Procesar cada fecha objetivo
  const batch = db.batch();
  let totalUpdates = 0;
  
  for (const targetDate of targetDates) {
    console.log(`\n📅 Procesando ${targetDate}...`);
    
    for (const [code, indexData] of historicalDataByIndex) {
      const dateData = indexData.dates[targetDate];
      
      if (!dateData) {
        console.log(`  ⚠️ ${code}: No hay datos para ${targetDate}`);
        continue;
      }
      
      // Buscar día anterior para calcular cambio
      const allDates = Object.keys(indexData.dates).sort();
      const targetIdx = allDates.indexOf(targetDate);
      const prevDate = targetIdx > 0 ? allDates[targetIdx - 1] : null;
      const prevData = prevDate ? indexData.dates[prevDate] : null;
      
      const closePrice = dateData.close;
      const prevClose = prevData ? prevData.close : null;
      const percentChange = calculatePercentChange(closePrice, prevClose);
      const absoluteChange = prevClose ? closePrice - prevClose : 0;
      
      console.log(`  ${code}: close=${closePrice.toFixed(2)}, change=${percentChange.toFixed(2)}%`);
      
      if (!dryRun) {
        // Documento principal
        const generalDocRef = db.collection('indexHistories').doc(code);
        batch.set(generalDocRef, {
          name: indexData.name,
          code: code,
          region: indexData.region,
          lastUpdated: Date.now()
        }, { merge: true });
        
        // Documento de fecha con datos históricos reales
        const dateDocRef = generalDocRef.collection('dates').doc(targetDate);
        batch.set(dateDocRef, {
          score: closePrice,
          change: percentChange,  // % change
          percentChange: percentChange,
          date: targetDate,
          timestamp: Date.now(),
          captureType: 'backfill-historical',
          historicalData: {
            open: dateData.open,
            high: dateData.high,
            low: dateData.low,
            close: dateData.close,
            volume: dateData.volume
          },
          backfillDate: new Date().toISOString()
        }, { merge: true });
        
        totalUpdates++;
      }
    }
  }
  
  if (!dryRun && totalUpdates > 0) {
    console.log(`\n💾 Guardando ${totalUpdates} documentos...`);
    await batch.commit();
    console.log('✅ Guardado completado');
    
    // Invalidar cache
    console.log('\n🔄 Invalidando indexCache...');
    const cacheSnap = await db.collection('indexCache').get();
    if (!cacheSnap.empty) {
      const deleteBatch = db.batch();
      cacheSnap.docs.forEach(doc => {
        deleteBatch.delete(doc.ref);
      });
      await deleteBatch.commit();
      console.log(`✅ Eliminados ${cacheSnap.size} documentos de cache`);
    }
  }
  
  // Verificación
  if (!dryRun) {
    console.log('\n' + '='.repeat(80));
    console.log('VERIFICACIÓN POST-BACKFILL');
    console.log('='.repeat(80));
    
    for (const targetDate of targetDates) {
      console.log(`\n📅 ${targetDate}:`);
      for (const index of KEY_INDICES) {
        const doc = await db.collection('indexHistories')
          .doc(index.code)
          .collection('dates')
          .doc(targetDate)
          .get();
        
        if (doc.exists) {
          const data = doc.data();
          console.log(`  ✅ ${index.code}: score=${data.score}, change=${data.percentChange?.toFixed(2)}%`);
        } else {
          console.log(`  ❌ ${index.code}: documento no encontrado`);
        }
      }
    }
  }
  
  console.log('\n✅ BACKFILL COMPLETADO');
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const datesArg = args.find(a => a.startsWith('--dates='));
  
  if (!datesArg) {
    console.log('USO: node backfillIndicesHistorical.js --dates=2026-03-05,2026-03-06 [--dry-run]');
    process.exit(1);
  }
  
  const dates = datesArg.split('=')[1].split(',').map(d => d.trim());
  
  await backfillIndicesForDates(dates, dryRun);
  
  process.exit(0);
}

main().catch(err => {
  console.error('Error fatal:', err);
  process.exit(1);
});
