/**
 * Script para rellenar datos históricos de índices faltantes en Firestore
 * Usa el endpoint /v1/historical para obtener datos OHLCV y calcular cambios
 * 
 * Ejecutar con: node backfillIndexHistories.js
 */

const admin = require('firebase-admin');
const axios = require('axios');

// Inicializar Firebase Admin (ajustar path según ubicación)
const serviceAccount = require('../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

// Mapeo de códigos de índices a símbolos de Yahoo Finance
const INDEX_SYMBOLS = {
  'GSPC': '^GSPC',      // S&P 500
  'DJI': '^DJI',        // Dow Jones
  'IXIC': '^IXIC',      // NASDAQ
  'RUT': '^RUT',        // Russell 2000
  'VIX': '^VIX',        // VIX
  'BVSP': '^BVSP',      // IBOVESPA
  'MXX': '^MXX',        // IPC Mexico
  'IPSA': '^IPSA',      // S&P IPSA
  'MERV': '^MERV',      // MERVAL
  'FTSE': '^FTSE',      // FTSE 100
  'GDAXI': '^GDAXI',    // DAX
  'FCHI': '^FCHI',      // CAC 40
  'STOXX50E': '^STOXX50E', // EURO STOXX 50
  'IBEX': '^IBEX',      // IBEX 35
  'FTSEMIB': 'FTSEMIB.MI', // FTSE MIB
  'SSMI': '^SSMI',      // SMI
  'HSI': '^HSI',        // Hang Seng
  'BSESN': '^BSESN',    // SENSEX
  'KS11': '^KS11',      // KOSPI
  'TWII': '^TWII',      // Taiwan
  'N225': '^N225',      // Nikkei
  'SHANGHAI': '000001.SS', // Shanghai (SSE)
  'SZSE': '399001.SZ'   // Shenzhen
};

// Información de región para cada índice
const INDEX_REGIONS = {
  'GSPC': 'US', 'DJI': 'US', 'IXIC': 'US', 'RUT': 'US', 'VIX': 'US',
  'BVSP': 'SA', 'MXX': 'SA', 'IPSA': 'SA', 'MERV': 'SA',
  'FTSE': 'EU', 'GDAXI': 'EU', 'FCHI': 'EU', 'STOXX50E': 'EU', 'IBEX': 'EU', 'FTSEMIB': 'EU', 'SSMI': 'EU',
  'HSI': 'AS', 'BSESN': 'AS', 'KS11': 'AS', 'TWII': 'AS', 'N225': 'AS', 'SHANGHAI': 'AS', 'SZSE': 'AS'
};

// Nombres de los índices
const INDEX_NAMES = {
  'GSPC': 'S&P 500',
  'DJI': 'Dow Jones Industrial Average',
  'IXIC': 'NASDAQ Composite',
  'RUT': 'Russell 2000 Index',
  'VIX': 'CBOE Volatility Index',
  'BVSP': 'IBOVESPA',
  'MXX': 'IPC MEXICO',
  'IPSA': 'S&P IPSA',
  'MERV': 'MERVAL',
  'FTSE': 'FTSE 100',
  'GDAXI': 'DAX Performance Index',
  'FCHI': 'CAC 40',
  'STOXX50E': 'EURO STOXX 50',
  'IBEX': 'IBEX 35',
  'FTSEMIB': 'FTSE MIB Index',
  'SSMI': 'SMI PR',
  'HSI': 'HANG SENG INDEX',
  'BSESN': 'S&P BSE SENSEX',
  'KS11': 'KOSPI Composite Index',
  'TWII': 'TWSE Capitalization Weighted Stock Index',
  'N225': 'Nikkei 225',
  'SHANGHAI': 'SSE Composite Index',
  'SZSE': 'Shenzhen Index'
};

const API_BASE_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws';

async function getHistoricalData(symbol) {
  try {
    const url = `${API_BASE_URL}/v1/historical?symbol=${encodeURIComponent(symbol)}&range=6mo&interval=1d`;
    const response = await axios.get(url, { timeout: 60000 });
    return response.data;
  } catch (error) {
    console.error(`Error fetching historical data for ${symbol}:`, error.message);
    return null;
  }
}

async function getExistingDates(indexCode) {
  const datesRef = db.collection('indexHistories').doc(indexCode).collection('dates');
  const snapshot = await datesRef.get();
  const existingDates = new Set();
  snapshot.forEach(doc => existingDates.add(doc.id));
  return existingDates;
}

async function backfillIndex(indexCode, yahooSymbol) {
  console.log(`\n📊 Procesando ${indexCode} (${yahooSymbol})...`);
  
  // Obtener datos históricos
  const historicalData = await getHistoricalData(yahooSymbol);
  if (!historicalData) {
    console.log(`  ❌ No se pudieron obtener datos históricos`);
    return { added: 0, skipped: 0 };
  }

  // Obtener fechas existentes en Firestore
  const existingDates = await getExistingDates(indexCode);
  console.log(`  📅 Fechas existentes en Firestore: ${existingDates.size}`);

  // Convertir datos históricos a array ordenado
  const dates = Object.keys(historicalData).sort();
  console.log(`  📅 Fechas en datos históricos: ${dates.length}`);

  let addedCount = 0;
  let skippedCount = 0;
  const batch = db.batch();
  let batchCount = 0;

  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    
    // Saltar si ya existe
    if (existingDates.has(date)) {
      skippedCount++;
      continue;
    }

    const dayData = historicalData[date];
    const closePrice = dayData.close;
    
    // Calcular cambio respecto al día anterior
    let change = '0.00';
    let percentChange = 0;
    
    if (i > 0) {
      const prevDate = dates[i - 1];
      const prevClose = historicalData[prevDate].close;
      const changeValue = closePrice - prevClose;
      percentChange = ((changeValue / prevClose) * 100);
      change = changeValue >= 0 ? `${changeValue.toFixed(2)}` : `${changeValue.toFixed(2)}`;
    }

    // Preparar documento
    const indexData = {
      score: closePrice,
      change: change,
      percentChange: parseFloat(percentChange.toFixed(2)),
      date: date,
      timestamp: new Date(date).getTime()
    };

    // Referencia al documento
    const docRef = db
      .collection('indexHistories')
      .doc(indexCode)
      .collection('dates')
      .doc(date);

    batch.set(docRef, indexData, { merge: true });
    batchCount++;
    addedCount++;

    // Firestore tiene un límite de 500 operaciones por batch
    if (batchCount >= 450) {
      await batch.commit();
      console.log(`  💾 Guardados ${addedCount} registros...`);
      batchCount = 0;
    }
  }

  // Commit final si quedan operaciones
  if (batchCount > 0) {
    await batch.commit();
  }

  // Asegurar que el documento principal existe
  const generalData = {
    name: INDEX_NAMES[indexCode] || indexCode,
    code: indexCode,
    region: INDEX_REGIONS[indexCode] || 'US'
  };
  await db.collection('indexHistories').doc(indexCode).set(generalData, { merge: true });

  console.log(`  ✅ Añadidos: ${addedCount}, Omitidos (ya existían): ${skippedCount}`);
  return { added: addedCount, skipped: skippedCount };
}

async function main() {
  console.log('🚀 Iniciando backfill de datos históricos de índices...\n');
  console.log('📆 Período: últimos 6 meses');
  console.log('=' .repeat(60));

  let totalAdded = 0;
  let totalSkipped = 0;
  const errors = [];

  for (const [indexCode, yahooSymbol] of Object.entries(INDEX_SYMBOLS)) {
    try {
      const result = await backfillIndex(indexCode, yahooSymbol);
      totalAdded += result.added;
      totalSkipped += result.skipped;
      
      // Pequeña pausa para no sobrecargar el API
      await new Promise(resolve => setTimeout(resolve, 1000));
    } catch (error) {
      console.error(`  ❌ Error procesando ${indexCode}:`, error.message);
      errors.push(indexCode);
    }
  }

  console.log('\n' + '=' .repeat(60));
  console.log('📊 RESUMEN:');
  console.log(`  ✅ Total registros añadidos: ${totalAdded}`);
  console.log(`  ⏭️  Total registros omitidos: ${totalSkipped}`);
  if (errors.length > 0) {
    console.log(`  ❌ Índices con errores: ${errors.join(', ')}`);
  }
  console.log('\n✨ Backfill completado!');
  
  process.exit(0);
}

main().catch(error => {
  console.error('Error fatal:', error);
  process.exit(1);
});
