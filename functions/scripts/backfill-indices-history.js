/**
 * Script para rellenar datos de índices históricos faltantes
 * 
 * FIX-SECRET-001: Los datos de índices no se guardaron desde 2026-01-30
 * debido a un problema con el token de servicio (caracteres inválidos).
 * 
 * Este script consulta el API de finanzas y guarda los datos históricos
 * para los índices faltantes usando el endpoint /historical.
 * 
 * USO:
 *   cd src/GoogleFinanceAPI/functions
 *   node scripts/backfill-indices-history.js
 *   node scripts/backfill-indices-history.js --from 2026-01-31 --to 2026-02-04
 * 
 * NOTA: Debe ejecutarse desde el directorio functions para encontrar las dependencias
 */

const admin = require('firebase-admin');
const axios = require('axios');
const path = require('path');

// Cargar variables de entorno
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

// Inicializar Firebase Admin
// Intentar cargar credenciales desde key.json o Application Default Credentials
let credential;
try {
  const serviceAccount = require('../key.json');
  credential = admin.credential.cert(serviceAccount);
} catch (e) {
  // Usar Application Default Credentials
  credential = admin.credential.applicationDefault();
}

admin.initializeApp({ credential });

const db = admin.firestore();

// Configuración del API
const FINANCE_QUERY_API_URL = 'https://ws.portastock.top/v1';
const SERVICE_TOKEN = '26ca00231ead1b5fbd63c6bba10a16e2f619b56809013ab3b3bcbbfb029aff10';

// Headers necesarios para Cloudflare WAF
const API_HEADERS = {
  'Content-Type': 'application/json',
  'User-Agent': 'google-cloud-functions/1.0 portafolio-inversiones',
  'Referer': 'https://us-central1-portafolio-inversiones.cloudfunctions.net',
  'x-service-token': SERVICE_TOKEN
};

// Mapeo de códigos de índices a símbolos de Yahoo Finance
// Basado en los índices existentes en la colección indexHistories de Firestore
const INDEX_SYMBOLS = {
  'GSPC': '^GSPC',      // S&P 500
  'DJI': '^DJI',        // Dow Jones Industrial Average
  'IXIC': '^IXIC',      // NASDAQ Composite
  'RUT': '^RUT',        // Russell 2000 Index
  'VIX': '^VIX',        // CBOE Volatility Index
  'BVSP': '^BVSP',      // IBOVESPA
  'MXX': '^MXX',        // IPC MEXICO
  'IPSA': '^IPSA',      // S&P IPSA Chile
  'MERV': '^MERV',      // MERVAL Argentina
  'FTSE': '^FTSE',      // FTSE 100
  'GDAXI': '^GDAXI',    // DAX Performance Index
  'FCHI': '^FCHI',      // CAC 40
  'STOXX50E': '^STOXX50E', // EURO STOXX 50
  'IBEX': '^IBEX',      // IBEX 35
  'FTSEMIB': 'FTSEMIB.MI', // FTSE MIB Index
  'SSMI': '^SSMI',      // SMI PR
  'HSI': '^HSI',        // HANG SENG INDEX
  'BSESN': '^BSESN',    // S&P BSE SENSEX
  'KS11': '^KS11',      // KOSPI Composite Index
  'TWII': '^TWII',      // TWSE Capitalization Weighted Stock Index
  'N225': '^N225',      // Nikkei 225
  'SHANGHAI': '000001.SS', // SSE Composite Index (Shanghai)
  'SZSE': '399001.SZ'   // Shenzhen Index
};

// Info de índices para los documentos principales
const INDEX_INFO = {
  'GSPC': { name: 'S&P 500', region: 'US' },
  'DJI': { name: 'Dow Jones Industrial Average', region: 'US' },
  'IXIC': { name: 'NASDAQ Composite', region: 'US' },
  'RUT': { name: 'Russell 2000 Index', region: 'US' },
  'VIX': { name: 'CBOE Volatility Index', region: 'US' },
  'BVSP': { name: 'IBOVESPA', region: 'SA' },
  'MXX': { name: 'IPC MEXICO', region: 'SA' },
  'IPSA': { name: 'S&P IPSA', region: 'SA' },
  'MERV': { name: 'MERVAL', region: 'SA' },
  'FTSE': { name: 'FTSE 100', region: 'EU' },
  'GDAXI': { name: 'DAX Performance Index', region: 'EU' },
  'FCHI': { name: 'CAC 40', region: 'EU' },
  'STOXX50E': { name: 'EURO STOXX 50', region: 'EU' },
  'IBEX': { name: 'IBEX 35', region: 'EU' },
  'FTSEMIB': { name: 'FTSE MIB Index', region: 'EU' },
  'SSMI': { name: 'SMI PR', region: 'EU' },
  'HSI': { name: 'HANG SENG INDEX', region: 'AS' },
  'BSESN': { name: 'S&P BSE SENSEX', region: 'AS' },
  'KS11': { name: 'KOSPI Composite Index', region: 'AS' },
  'TWII': { name: 'TWSE Capitalization Weighted Stock Index', region: 'AS' },
  'N225': { name: 'Nikkei 225', region: 'AS' },
  'SHANGHAI': { name: 'SSE Composite Index', region: 'AS' },
  'SZSE': { name: 'Shenzhen Index', region: 'AS' }
};

/**
 * Normaliza un valor numérico desde string con formato (%, +, etc.)
 */
const normalizeNumber = (value) => {
  if (!value) return null;
  if (typeof value === 'number') return value;
  return parseFloat(String(value).replace(/[%,+]/g, ''));
};

/**
 * Obtiene datos históricos de un índice específico
 * @param {string} symbol - Símbolo del índice (ej: ^GSPC)
 * @param {string} range - Rango de tiempo (ej: 1mo, 5d)
 */
async function fetchHistoricalData(symbol, range = '1mo') {
  try {
    const response = await axios.get(
      `${FINANCE_QUERY_API_URL}/historical`,
      { 
        headers: API_HEADERS,
        params: {
          symbol: symbol,
          range: range,
          interval: '1d'
        }
      }
    );
    return response.data;
  } catch (error) {
    console.error(`  ⚠ Error obteniendo histórico de ${symbol}: ${error.message}`);
    return null;
  }
}

/**
 * Obtiene los índices actuales del API (para datos de hoy)
 */
async function fetchCurrentIndices() {
  const response = await axios.get(
    `${FINANCE_QUERY_API_URL}/indices`,
    { headers: API_HEADERS }
  );
  return response.data;
}

/**
 * Parsea los argumentos de la línea de comandos
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const result = {
    from: null,
    to: null,
    today: false
  };
  
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--from' && args[i + 1]) {
      result.from = args[i + 1];
      i++;
    } else if (args[i] === '--to' && args[i + 1]) {
      result.to = args[i + 1];
      i++;
    } else if (args[i] === '--today') {
      result.today = true;
    }
  }
  
  return result;
}

/**
 * Obtiene el día de la semana de una fecha string YYYY-MM-DD
 * Retorna 0=Domingo, 1=Lunes, ..., 6=Sábado
 * Esta función evita problemas de timezone al calcular manualmente
 */
function getDayOfWeekFromDateStr(dateStr) {
  // Parsear la fecha manualmente para evitar problemas de timezone
  const [year, month, day] = dateStr.split('-').map(Number);
  // Usar UTC explícitamente para evitar offset de timezone
  const date = new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
  return date.getUTCDay();
}

/**
 * Incrementa una fecha string YYYY-MM-DD en un día
 */
function addDaysToDateStr(dateStr, days = 1) {
  const [year, month, day] = dateStr.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day + days, 12, 0, 0));
  return date.toISOString().split('T')[0];
}

/**
 * Obtiene las fechas faltantes en el rango especificado
 */
async function getMissingDates(indexCode, fromDate, toDate) {
  const missing = [];
  let currentDateStr = fromDate;
  
  while (currentDateStr <= toDate) {
    const dayOfWeek = getDayOfWeekFromDateStr(currentDateStr);
    
    // Solo días hábiles (lunes a viernes) - 0=Dom, 6=Sáb
    if (dayOfWeek !== 0 && dayOfWeek !== 6) {
      // Verificar si existe en Firestore
      const docRef = db.collection('indexHistories').doc(indexCode).collection('dates').doc(currentDateStr);
      const doc = await docRef.get();
      
      if (!doc.exists) {
        missing.push(currentDateStr);
      }
    }
    
    currentDateStr = addDaysToDateStr(currentDateStr, 1);
  }
  
  return missing;
}

/**
 * Guarda datos históricos de un índice para múltiples fechas
 * El API retorna datos en formato: { "2026-02-04": { open, high, low, close, ... }, ... }
 */
async function saveHistoricalDataForIndex(indexCode, historicalData, targetDates) {
  if (!historicalData || typeof historicalData !== 'object') {
    return 0;
  }
  
  // El API retorna un objeto con fechas como claves
  const dates = Object.keys(historicalData);
  if (dates.length === 0) {
    return 0;
  }
  
  const batch = db.batch();
  let count = 0;
  const info = INDEX_INFO[indexCode] || { name: indexCode, region: 'Unknown' };
  
  // Documento principal con info general
  const generalDocRef = db.collection('indexHistories').doc(indexCode);
  batch.set(generalDocRef, {
    name: info.name,
    code: indexCode,
    region: info.region,
    lastUpdated: Date.now()
  }, { merge: true });
  
  // Crear un mapa de fechas objetivo para búsqueda rápida
  const targetDateSet = new Set(targetDates);
  
  // Procesar cada punto de datos histórico
  for (const [dateStr, quote] of Object.entries(historicalData)) {
    // Solo guardar si es una fecha objetivo
    if (!targetDateSet.has(dateStr)) continue;
    
    // Calcular cambio porcentual si tenemos open y close
    let percentChange = null;
    if (quote.open && quote.close) {
      percentChange = ((quote.close - quote.open) / quote.open) * 100;
    }
    
    // Calcular cambio absoluto
    let change = null;
    if (quote.open && quote.close) {
      change = (quote.close - quote.open).toFixed(2);
    }
    
    const dateDocRef = generalDocRef.collection('dates').doc(dateStr);
    batch.set(dateDocRef, {
      score: quote.close || quote.adjClose,
      change: change,
      percentChange: percentChange ? parseFloat(percentChange.toFixed(2)) : null,
      date: dateStr,
      timestamp: Date.now(),
      captureType: 'backfill',
      source: 'historical-api',
      open: quote.open,
      high: quote.high,
      low: quote.low,
      volume: quote.volume
    }, { merge: true });
    
    count++;
  }
  
  if (count > 0) {
    await batch.commit();
  }
  
  return count;
}

/**
 * Ejecuta el backfill
 */
async function main() {
  const args = parseArgs();
  
  console.log('='.repeat(60));
  console.log('BACKFILL DE ÍNDICES HISTÓRICOS');
  console.log('='.repeat(60));
  console.log(`API URL: ${FINANCE_QUERY_API_URL}`);
  console.log(`Fecha actual: ${new Date().toISOString()}`);
  console.log('');
  
  // Determinar rango de fechas
  let fromDate, toDate;
  
  if (args.from && args.to) {
    fromDate = args.from;
    toDate = args.to;
    console.log(`Rango especificado: ${fromDate} a ${toDate}`);
  } else {
    // Por defecto: fechas faltantes conocidas
    fromDate = '2026-01-31';
    toDate = '2026-02-04';
    console.log(`Usando rango por defecto: ${fromDate} a ${toDate}`);
  }
  console.log('');

  try {
    const totalSaved = {};
    const errors = [];
    
    // Procesar cada índice
    console.log('Procesando índices...');
    console.log('-'.repeat(60));
    
    for (const [indexCode, yahooSymbol] of Object.entries(INDEX_SYMBOLS)) {
      process.stdout.write(`${indexCode.padEnd(12)} `);
      
      // Obtener fechas faltantes
      const missingDates = await getMissingDates(indexCode, fromDate, toDate);
      
      if (missingDates.length === 0) {
        console.log('✓ Sin fechas faltantes');
        continue;
      }
      
      process.stdout.write(`(${missingDates.length} fechas) `);
      
      // Obtener datos históricos
      const historical = await fetchHistoricalData(yahooSymbol, '1mo');
      
      if (!historical) {
        console.log('⚠ Error obteniendo datos');
        errors.push(indexCode);
        continue;
      }
      
      // Guardar datos
      const saved = await saveHistoricalDataForIndex(indexCode, historical, missingDates);
      totalSaved[indexCode] = saved;
      
      if (saved > 0) {
        console.log(`✓ ${saved} registros guardados`);
      } else {
        console.log('⚠ No hay datos para las fechas');
      }
      
      // Pequeña pausa para no saturar el API
      await new Promise(r => setTimeout(r, 200));
    }
    
    // Resumen
    console.log('');
    console.log('='.repeat(60));
    console.log('RESUMEN');
    console.log('='.repeat(60));
    
    const totalRecords = Object.values(totalSaved).reduce((a, b) => a + b, 0);
    console.log(`Total de registros guardados: ${totalRecords}`);
    console.log(`Índices procesados: ${Object.keys(totalSaved).length}`);
    
    if (errors.length > 0) {
      console.log(`Índices con errores: ${errors.join(', ')}`);
    }
    
    console.log('');
    console.log('Detalle por índice:');
    for (const [code, count] of Object.entries(totalSaved)) {
      if (count > 0) {
        console.log(`  - ${code}: ${count} registros`);
      }
    }

  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }

  console.log('');
  console.log('✓ Backfill completado exitosamente');
  process.exit(0);
}

main();
