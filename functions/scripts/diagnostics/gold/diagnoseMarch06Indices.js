/**
 * Script de diagnóstico: Problema saveIndicesHistoryData - 6 marzo 2026
 * 
 * Verifica:
 * 1. Si existen datos para el 6 de marzo en indexHistories
 * 2. Estado de los últimos días de índices clave
 * 3. Si la fecha 2026-03-06 es día de trading válido
 * 4. Hace backfill si se pasa --fix
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

// FIX-INDEX-002: Índices clave con símbolos para /quotes
const KEY_INDEX_SYMBOLS = [
  { symbol: '^GSPC', code: 'GSPC', name: 'S&P 500', region: 'US' },
  { symbol: '^DJI', code: 'DJI', name: 'Dow Jones Industrial Average', region: 'US' },
  { symbol: '^IXIC', code: 'IXIC', name: 'NASDAQ Composite', region: 'US' },
  { symbol: '^RUT', code: 'RUT', name: 'Russell 2000', region: 'US' },
  { symbol: '^VIX', code: 'VIX', name: 'CBOE Volatility Index', region: 'US' },
  { symbol: '^NYA', code: 'NYA', name: 'NYSE Composite', region: 'US' },
  { symbol: '^FTSE', code: 'FTSE', name: 'FTSE 100', region: 'UK' },
  { symbol: '^GDAXI', code: 'GDAXI', name: 'DAX Performance Index', region: 'DE' },
  { symbol: '^FCHI', code: 'FCHI', name: 'CAC 40', region: 'FR' },
  { symbol: '^N225', code: 'N225', name: 'Nikkei 225', region: 'JP' },
  { symbol: '^HSI', code: 'HSI', name: 'Hang Seng Index', region: 'HK' },
];

// Festivos NYSE 2026
const NYSE_HOLIDAYS_2026 = new Set([
  '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',
  '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07',
  '2026-11-26', '2026-12-25',
]);

// Índices principales a verificar
const KEY_INDICES = ['GSPC', 'DJI', 'IXIC', 'RUT', 'VIX'];

// Fecha objetivo
const TARGET_DATE = '2026-03-06';

// ============================================================================
// UTILIDADES
// ============================================================================

function isValidTradingDay(dateStr) {
  const d = new Date(dateStr + 'T12:00:00Z');
  const dayOfWeek = d.getUTCDay();
  
  // Fin de semana
  if (dayOfWeek === 0 || dayOfWeek === 6) {
    return { isValid: false, reason: dayOfWeek === 0 ? 'sunday' : 'saturday' };
  }
  
  // Festivo
  if (NYSE_HOLIDAYS_2026.has(dateStr)) {
    return { isValid: false, reason: 'holiday' };
  }
  
  return { isValid: true, reason: 'trading-day' };
}

const normalizeNumber = (value) => {
  if (!value) return null;
  return parseFloat(value.replace(/[%,+]/g, ''));
};

// ============================================================================
// DIAGNÓSTICO
// ============================================================================

async function diagnoseIndexData() {
  console.log('='.repeat(80));
  console.log('DIAGNÓSTICO: saveIndicesHistoryData - 6 marzo 2026');
  console.log('='.repeat(80));
  console.log(`Fecha objetivo: ${TARGET_DATE}`);
  console.log(`Fecha actual: ${new Date().toISOString()}`);
  
  // 1. Verificar si es día de trading válido
  const tradingCheck = isValidTradingDay(TARGET_DATE);
  console.log(`\n📅 ¿${TARGET_DATE} es día de trading válido?`);
  console.log(`   Resultado: ${tradingCheck.isValid ? '✅ Sí' : '❌ No'} (${tradingCheck.reason})`);
  
  if (!tradingCheck.isValid) {
    console.log(`\n⚠️ ${TARGET_DATE} NO es día de trading, no debería haber datos.`);
    return;
  }
  
  // 2. Verificar marketHolidays/US en Firestore
  console.log('\n🔍 Verificando marketHolidays/US en Firestore...');
  try {
    const holidaysDoc = await db.collection('marketHolidays').doc('US').get();
    if (holidaysDoc.exists) {
      const holidaysData = holidaysDoc.data();
      if (holidaysData.holidays && holidaysData.holidays[TARGET_DATE]) {
        console.log(`   ⚠️ PROBLEMA ENCONTRADO: ${TARGET_DATE} está marcado como festivo en Firestore!`);
        console.log(`   Valor: ${holidaysData.holidays[TARGET_DATE]}`);
        console.log('   ESTO ES LA CAUSA DEL PROBLEMA');
      } else {
        console.log(`   ✅ ${TARGET_DATE} NO está marcado como festivo en marketHolidays/US`);
      }
      
      // Mostrar festivos cercanos
      const sortedHolidays = Object.keys(holidaysData.holidays || {}).sort();
      const marchHolidays = sortedHolidays.filter(d => d.startsWith('2026-03'));
      if (marchHolidays.length > 0) {
        console.log(`   Festivos de marzo 2026: ${marchHolidays.join(', ')}`);
      } else {
        console.log('   No hay festivos en marzo 2026');
      }
    } else {
      console.log('   ⚠️ Documento marketHolidays/US no existe (usará fallback estático)');
    }
  } catch (err) {
    console.log(`   ❌ Error consultando marketHolidays: ${err.message}`);
  }
  
  // 3. Verificar datos de índices para los últimos 5 días
  console.log('\n📊 Verificando datos de índices (últimos 5 días de trading):');
  
  for (const code of KEY_INDICES) {
    console.log(`\n  📈 Índice: ${code}`);
    const datesRef = db.collection('indexHistories').doc(code).collection('dates');
    
    try {
      const recentDocs = await datesRef
        .orderBy('date', 'desc')
        .limit(7)
        .get();
      
      if (recentDocs.empty) {
        console.log('     ❌ Sin documentos recientes');
        continue;
      }
      
      const dates = [];
      recentDocs.docs.forEach(doc => {
        const data = doc.data();
        dates.push({
          date: doc.id,
          score: data.score,
          percentChange: data.percentChange,
          captureType: data.captureType || 'unknown'
        });
      });
      
      // Ordenar ascendente para mostrar
      dates.sort((a, b) => a.date.localeCompare(b.date));
      dates.forEach(d => {
        const isMissing = d.date === TARGET_DATE ? '' : '';
        const icon = d.date === TARGET_DATE ? '🎯' : '  ';
        console.log(`     ${icon} ${d.date}: score=${d.score}, change=${d.percentChange}%, type=${d.captureType}`);
      });
      
      // Verificar si falta TARGET_DATE
      const hasTargetDate = dates.some(d => d.date === TARGET_DATE);
      if (!hasTargetDate) {
        console.log(`     ❌ FALTA: ${TARGET_DATE} no encontrado para ${code}`);
      } else {
        console.log(`     ✅ ${TARGET_DATE} presente`);
      }
      
    } catch (err) {
      console.log(`     ❌ Error: ${err.message}`);
    }
  }
  
  // 4. Verificar cache de índices
  console.log('\n🔄 Verificando indexCache...');
  const cacheSnap = await db.collection('indexCache')
    .orderBy('lastUpdated', 'desc')
    .limit(5)
    .get();
  
  if (!cacheSnap.empty) {
    cacheSnap.docs.forEach(doc => {
      const data = doc.data();
      const lastDate = data.chartData?.length > 0 
        ? data.chartData[data.chartData.length - 1].date 
        : 'N/A';
      console.log(`  ${doc.id}: lastUpdated=${new Date(data.lastUpdated).toISOString()}, lastDataDate=${lastDate}`);
    });
  } else {
    console.log('  Sin documentos en indexCache');
  }
  
  console.log('\n' + '='.repeat(80));
}

// ============================================================================
// BACKFILL
// ============================================================================

/**
 * FIX-INDEX-002: Obtener índices via /quotes (más estable que /indices)
 */
async function fetchIndicesViaQuotes() {
  const symbols = KEY_INDEX_SYMBOLS.map(i => i.symbol).join(',');
  
  const response = await axios.get(
    `${FINANCE_QUERY_API_URL}/quotes`,
    { 
      headers: API_HEADERS,
      params: { symbols }
    }
  );
  
  const quotes = response.data || [];
  
  // Transformar formato de /quotes al formato de /indices
  return quotes.map(quote => {
    const indexInfo = KEY_INDEX_SYMBOLS.find(i => i.symbol === quote.symbol) || {};
    
    const parseValue = (val) => {
      if (!val) return 0;
      const str = String(val).replace(/[,%$+]/g, '');
      return parseFloat(str) || 0;
    };
    
    return {
      code: indexInfo.code || quote.symbol.replace('^', ''),
      name: indexInfo.name || quote.name || quote.symbol,
      region: indexInfo.region || 'US',
      value: parseValue(quote.price),
      change: parseValue(quote.change),
      percentChange: quote.percentChange || '0%',
    };
  });
}

async function backfillIndicesForDate(targetDate) {
  console.log('\n' + '='.repeat(80));
  console.log(`BACKFILL: Guardando datos de índices para ${targetDate}`);
  console.log('='.repeat(80));
  
  // Verificar que es día de trading
  const tradingCheck = isValidTradingDay(targetDate);
  if (!tradingCheck.isValid) {
    console.log(`❌ ${targetDate} no es día de trading (${tradingCheck.reason}). Abortando.`);
    return;
  }
  
  // FIX-INDEX-002: Intentar /indices primero, fallback a /quotes
  console.log('\n📡 Obteniendo datos de índices desde API...');
  let indices = null;
  
  try {
    // Intentar endpoint /indices primero
    const response = await axios.get(
      `${FINANCE_QUERY_API_URL}/indices`,
      { headers: API_HEADERS, timeout: 10000 }
    );
    indices = response.data;
    console.log(`✅ Obtenidos ${indices.length} índices via /indices`);
  } catch (err) {
    console.log(`⚠️ /indices falló (${err.message}), usando fallback /quotes...`);
    
    try {
      indices = await fetchIndicesViaQuotes();
      console.log(`✅ Obtenidos ${indices.length} índices via /quotes (fallback)`);
    } catch (fallbackErr) {
      console.log(`❌ Ambos endpoints fallaron: ${fallbackErr.message}`);
      return;
    }
  }
  
  if (!indices || indices.length === 0) {
    console.log('❌ No se obtuvieron índices de la API');
    return;
  }
  
  try {
    const batch = db.batch();
    let count = 0;
    
    indices.forEach(index => {
      // Documento principal
      const generalDocRef = db.collection('indexHistories').doc(index.code);
      batch.set(generalDocRef, {
        name: index.name,
        code: index.code,
        region: index.region,
        lastUpdated: Date.now()
      }, { merge: true });
      
      // Documento de fecha
      const dateDocRef = generalDocRef.collection('dates').doc(targetDate);
      batch.set(dateDocRef, {
        score: index.value,
        change: index.change,
        percentChange: normalizeNumber(index.percentChange),
        date: targetDate,
        timestamp: Date.now(),
        captureType: 'backfill',
        backfillDate: new Date().toISOString()
      }, { merge: true });
      
      count++;
    });
    
    await batch.commit();
    console.log(`\n✅ BACKFILL COMPLETADO: ${count} índices guardados para ${targetDate}`);
    
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
    
    // Mostrar índices guardados (KEY_INDICES)
    console.log('\n📊 Índices clave guardados:');
    for (const index of indices.filter(i => KEY_INDICES.includes(i.code))) {
      console.log(`  ${index.code}: ${index.name} = ${index.value} (${index.percentChange})`);
    }
  } catch (err) {
    console.log(`❌ Error guardando en Firestore: ${err.message}`);
  }
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const args = process.argv.slice(2);
  const doFix = args.includes('--fix');
  const customDate = args.find(a => a.startsWith('--date='))?.split('=')[1];
  const targetDate = customDate || TARGET_DATE;
  
  console.log(`Modo: ${doFix ? 'FIX (backfill)' : 'DIAGNÓSTICO SOLO'}`);
  if (customDate) {
    console.log(`Fecha personalizada: ${customDate}`);
  }
  
  await diagnoseIndexData();
  
  if (doFix) {
    await backfillIndicesForDate(targetDate);
    
    // Re-verificar después del backfill
    console.log('\n' + '='.repeat(80));
    console.log('VERIFICACIÓN POST-BACKFILL');
    console.log('='.repeat(80));
    
    for (const code of KEY_INDICES) {
      const doc = await db.collection('indexHistories')
        .doc(code)
        .collection('dates')
        .doc(targetDate)
        .get();
      
      if (doc.exists) {
        const data = doc.data();
        console.log(`✅ ${code}: score=${data.score}, change=${data.percentChange}%`);
      } else {
        console.log(`❌ ${code}: documento no encontrado`);
      }
    }
  } else {
    console.log('\n💡 Para hacer backfill, ejecuta: node diagnoseMarch06Indices.js --fix');
    console.log('   También puedes especificar otra fecha: node diagnoseMarch06Indices.js --fix --date=2026-03-05');
  }
  
  process.exit(0);
}

main().catch(err => {
  console.error('Error fatal:', err);
  process.exit(1);
});
