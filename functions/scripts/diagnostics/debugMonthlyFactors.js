/**
 * Script de Diagnóstico: Verificar cálculo de factores mensuales
 * 
 * Este script simula exactamente lo que hace calculateHistoricalReturns
 * para identificar dónde está el bug.
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  ACCOUNTS: {
    IBKR: 'BZHvXz4QT2yqqqlFP22X',
    XTB: 'Z3gnboYgRlTvSZNGSu8j'
  },
  CURRENCY: 'USD'
};

async function getAccountDocs(accountId) {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`;
  const snapshot = await db.collection(path).orderBy('date', 'asc').get();
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

/**
 * Simular aggregación multi-cuenta (como en getMultiAccountHistoricalReturns)
 */
function aggregateAccounts(ibkrDocs, xtbDocs, currency) {
  const byDate = new Map();
  
  // Agregar docs de IBKR
  ibkrDocs.forEach(doc => {
    if (!byDate.has(doc.date)) {
      byDate.set(doc.date, { date: doc.date, currencies: {} });
    }
    const entry = byDate.get(doc.date);
    const currencyData = doc[currency];
    
    if (currencyData) {
      if (!entry.currencies[currency]) {
        entry.currencies[currency] = {
          totalValue: 0,
          totalInvestment: 0,
          totalCashFlow: 0,
          _accountContributions: []
        };
      }
      entry.currencies[currency].totalValue += currencyData.totalValue || 0;
      entry.currencies[currency].totalInvestment += currencyData.totalInvestment || 0;
      entry.currencies[currency].totalCashFlow += currencyData.totalCashFlow || 0;
      entry.currencies[currency]._accountContributions.push({
        totalValue: currencyData.totalValue || 0,
        adjustedDailyChangePercentage: currencyData.adjustedDailyChangePercentage || 0
      });
    }
  });
  
  // Agregar docs de XTB
  xtbDocs.forEach(doc => {
    if (!byDate.has(doc.date)) {
      byDate.set(doc.date, { date: doc.date, currencies: {} });
    }
    const entry = byDate.get(doc.date);
    const currencyData = doc[currency];
    
    if (currencyData) {
      if (!entry.currencies[currency]) {
        entry.currencies[currency] = {
          totalValue: 0,
          totalInvestment: 0,
          totalCashFlow: 0,
          _accountContributions: []
        };
      }
      entry.currencies[currency].totalValue += currencyData.totalValue || 0;
      entry.currencies[currency].totalInvestment += currencyData.totalInvestment || 0;
      entry.currencies[currency].totalCashFlow += currencyData.totalCashFlow || 0;
      entry.currencies[currency]._accountContributions.push({
        totalValue: currencyData.totalValue || 0,
        adjustedDailyChangePercentage: currencyData.adjustedDailyChangePercentage || 0
      });
    }
  });
  
  // Calcular cambio ponderado
  const sortedDates = [...byDate.keys()].sort();
  
  sortedDates.forEach(date => {
    const entry = byDate.get(date);
    const c = entry.currencies[currency];
    if (!c) return;
    
    const contributions = c._accountContributions || [];
    const totalWeight = contributions.reduce((sum, acc) => sum + (acc.totalValue || 0), 0);
    
    if (totalWeight > 0 && contributions.length > 0) {
      c.adjustedDailyChangePercentage = contributions.reduce((sum, acc) => {
        const weight = (acc.totalValue || 0) / totalWeight;
        return sum + (acc.adjustedDailyChangePercentage || 0) * weight;
      }, 0);
    } else {
      c.adjustedDailyChangePercentage = 0;
    }
    
    delete c._accountContributions;
  });
  
  // Convertir a formato de docs para calculateHistoricalReturns
  return sortedDates.map(date => {
    const entry = byDate.get(date);
    return {
      data: () => ({
        date: entry.date,
        ...entry.currencies
      })
    };
  });
}

/**
 * Simular calculateHistoricalReturns para un mes específico
 */
function simulateMonthlyCalculation(docs, currency, year, month) {
  const monthStr = month.toString();
  
  // Ordenar documentos por fecha
  const documents = docs.sort((a, b) => {
    const dateA = a.data ? a.data().date : a.date;
    const dateB = b.data ? b.data().date : b.date;
    return dateA.localeCompare(dateB);
  });
  
  // Agrupar fechas por mes
  const datesByMonth = {};
  const lastDaysByMonth = {};
  
  documents.forEach((doc) => {
    const data = doc.data ? doc.data() : doc;
    const date = DateTime.fromISO(data.date);
    const y = date.year.toString();
    const m = (date.month - 1).toString();
    
    if (!datesByMonth[y]) datesByMonth[y] = {};
    if (!datesByMonth[y][m]) datesByMonth[y][m] = [];
    datesByMonth[y][m].push(data.date);
  });
  
  // Obtener último día de cada mes
  Object.keys(datesByMonth).forEach(y => {
    lastDaysByMonth[y] = {};
    Object.keys(datesByMonth[y]).forEach(m => {
      const sortedDates = [...datesByMonth[y][m]].sort((a, b) => b.localeCompare(a));
      lastDaysByMonth[y][m] = sortedDates[0];
    });
  });
  
  // Inicializar estructuras
  const monthlyStartFactors = {};
  const monthlyEndFactors = {};
  
  Object.keys(datesByMonth).forEach(y => {
    monthlyStartFactors[y] = {};
    monthlyEndFactors[y] = {};
  });
  
  // Procesar documentos
  let currentFactor = 1;
  const dailyLog = [];
  
  documents.forEach((doc) => {
    const data = doc.data ? doc.data() : doc;
    const currencyData = data[currency];
    
    if (!currencyData) return;
    
    const adjustedDailyChange = currencyData.adjustedDailyChangePercentage || 0;
    
    // Actualizar factor
    currentFactor = currentFactor * (1 + adjustedDailyChange / 100);
    
    const date = DateTime.fromISO(data.date);
    const y = date.year.toString();
    const m = (date.month - 1).toString();
    
    // Solo loguear el mes que nos interesa
    if (y === year && m === monthStr) {
      dailyLog.push({
        date: data.date,
        change: adjustedDailyChange,
        factorBefore: currentFactor / (1 + adjustedDailyChange / 100),
        factorAfter: currentFactor,
        isFirstOfMonth: !monthlyStartFactors[y][m],
        totalValue: currencyData.totalValue
      });
    }
    
    // Guardar factores (AQUÍ ESTÁ EL POTENCIAL BUG)
    if (!monthlyStartFactors[y][m]) {
      monthlyStartFactors[y][m] = currentFactor;  // ← Se guarda DESPUÉS del cambio
    }
    monthlyEndFactors[y][m] = currentFactor;
  });
  
  // Calcular rendimiento mensual
  const startFactor = monthlyStartFactors[year]?.[monthStr] || 1;
  const endFactor = monthlyEndFactors[year]?.[monthStr] || 1;
  const monthReturn = (endFactor / startFactor - 1) * 100;
  
  return {
    startFactor,
    endFactor,
    monthReturn,
    dailyLog
  };
}

async function main() {
  console.log('');
  console.log('═'.repeat(90));
  console.log('  DIAGNÓSTICO: Cálculo de Factores Mensuales');
  console.log('═'.repeat(90));
  console.log('');

  // Obtener datos
  console.log('📊 Obteniendo datos de Firestore...');
  const [ibkrDocs, xtbDocs] = await Promise.all([
    getAccountDocs(CONFIG.ACCOUNTS.IBKR),
    getAccountDocs(CONFIG.ACCOUNTS.XTB)
  ]);
  
  // Filtrar marzo 2025
  const marchIbkr = ibkrDocs.filter(d => d.date.startsWith('2025-03'));
  const marchXtb = xtbDocs.filter(d => d.date.startsWith('2025-03'));
  
  console.log(`   IBKR marzo: ${marchIbkr.length} docs`);
  console.log(`   XTB marzo: ${marchXtb.length} docs`);
  
  // Simular cálculo para IBKR solo
  console.log('');
  console.log('━'.repeat(90));
  console.log('  SIMULACIÓN: IBKR SOLO (Marzo 2025)');
  console.log('━'.repeat(90));
  
  const ibkrOnlyDocs = ibkrDocs.map(d => ({ data: () => d }));
  const ibkrResult = simulateMonthlyCalculation(ibkrOnlyDocs, CONFIG.CURRENCY, '2025', 2);
  
  console.log('');
  console.log(`   Start Factor: ${ibkrResult.startFactor.toFixed(6)}`);
  console.log(`   End Factor: ${ibkrResult.endFactor.toFixed(6)}`);
  console.log(`   Rendimiento Marzo: ${ibkrResult.monthReturn.toFixed(2)}%`);
  
  console.log('');
  console.log('   Días del mes:');
  ibkrResult.dailyLog.forEach(day => {
    const isFirst = day.isFirstOfMonth ? '← START' : '';
    console.log(`   ${day.date} | Change: ${day.change.toFixed(4)}% | Factor: ${day.factorAfter.toFixed(6)} ${isFirst}`);
  });
  
  // Simular cálculo para XTB solo
  console.log('');
  console.log('━'.repeat(90));
  console.log('  SIMULACIÓN: XTB SOLO (Marzo 2025)');
  console.log('━'.repeat(90));
  
  const xtbOnlyDocs = xtbDocs.map(d => ({ data: () => d }));
  const xtbResult = simulateMonthlyCalculation(xtbOnlyDocs, CONFIG.CURRENCY, '2025', 2);
  
  console.log('');
  console.log(`   Start Factor: ${xtbResult.startFactor.toFixed(6)}`);
  console.log(`   End Factor: ${xtbResult.endFactor.toFixed(6)}`);
  console.log(`   Rendimiento Marzo: ${xtbResult.monthReturn.toFixed(2)}%`);
  
  console.log('');
  console.log('   Días del mes:');
  xtbResult.dailyLog.forEach(day => {
    const isFirst = day.isFirstOfMonth ? '← START' : '';
    console.log(`   ${day.date} | Change: ${day.change.toFixed(4)}% | Factor: ${day.factorAfter.toFixed(6)} ${isFirst}`);
  });
  
  // Simular cálculo para IBKR + XTB agregados
  console.log('');
  console.log('━'.repeat(90));
  console.log('  SIMULACIÓN: IBKR + XTB AGREGADOS (Marzo 2025)');
  console.log('━'.repeat(90));
  
  const aggregatedDocs = aggregateAccounts(ibkrDocs, xtbDocs, CONFIG.CURRENCY);
  const aggregatedResult = simulateMonthlyCalculation(aggregatedDocs, CONFIG.CURRENCY, '2025', 2);
  
  console.log('');
  console.log(`   Start Factor: ${aggregatedResult.startFactor.toFixed(6)}`);
  console.log(`   End Factor: ${aggregatedResult.endFactor.toFixed(6)}`);
  console.log(`   Rendimiento Marzo: ${aggregatedResult.monthReturn.toFixed(2)}%`);
  
  console.log('');
  console.log('   Días del mes:');
  aggregatedResult.dailyLog.forEach(day => {
    const isFirst = day.isFirstOfMonth ? '← START' : '';
    console.log(`   ${day.date} | Change: ${day.change.toFixed(4)}% | Value: $${day.totalValue?.toFixed(2)} | Factor: ${day.factorAfter.toFixed(6)} ${isFirst}`);
  });
  
  // VERIFICAR: ¿Por qué el caché muestra +3.37%?
  console.log('');
  console.log('━'.repeat(90));
  console.log('  COMPARACIÓN CON CACHÉ');
  console.log('━'.repeat(90));
  console.log('');
  console.log(`   IBKR calculado: ${ibkrResult.monthReturn.toFixed(2)}%`);
  console.log(`   XTB calculado: ${xtbResult.monthReturn.toFixed(2)}%`);
  console.log(`   Agregado calculado: ${aggregatedResult.monthReturn.toFixed(2)}%`);
  console.log('');
  console.log(`   CACHÉ muestra para IBKR: -8.87%`);
  console.log(`   CACHÉ muestra para XTB: -5.13%`);
  console.log(`   CACHÉ muestra para Multi: +3.37%  ← ANOMALÍA!`);
  
  // Explicar la discrepancia
  console.log('');
  console.log('━'.repeat(90));
  console.log('  ANÁLISIS DE LA DISCREPANCIA');
  console.log('━'.repeat(90));
  console.log('');
  console.log('   El factor INICIO para IBKR incluye TODA la historia desde agosto 2024.');
  console.log('   El factor INICIO para XTB solo incluye historia desde enero 2025.');
  console.log('');
  console.log('   Cuando se agregan, los factores de inicio son diferentes porque');
  console.log('   IBKR tiene más historia que afecta el "currentFactor" acumulado.');
  
  // Verificar cuánto es el currentFactor al inicio de marzo para cada cuenta
  console.log('');
  console.log('   Verificando factores acumulados al 1 de marzo:');
  
  // Para IBKR
  let ibkrFactorToMarch = 1;
  ibkrDocs.filter(d => d.date < '2025-03-01').forEach(d => {
    const change = d.USD?.adjustedDailyChangePercentage || 0;
    ibkrFactorToMarch *= (1 + change / 100);
  });
  console.log(`   IBKR factor al 28 Feb: ${ibkrFactorToMarch.toFixed(6)}`);
  
  // Para XTB
  let xtbFactorToMarch = 1;
  xtbDocs.filter(d => d.date < '2025-03-01').forEach(d => {
    const change = d.USD?.adjustedDailyChangePercentage || 0;
    xtbFactorToMarch *= (1 + change / 100);
  });
  console.log(`   XTB factor al 28 Feb: ${xtbFactorToMarch.toFixed(6)}`);
  
  // Para agregado
  const allDocsBeforeMarch = aggregatedDocs.filter(d => {
    const data = d.data();
    return data.date < '2025-03-01';
  });
  let aggregatedFactorToMarch = 1;
  allDocsBeforeMarch.forEach(d => {
    const data = d.data();
    const change = data.USD?.adjustedDailyChangePercentage || 0;
    aggregatedFactorToMarch *= (1 + change / 100);
  });
  console.log(`   Agregado factor al 28 Feb: ${aggregatedFactorToMarch.toFixed(6)}`);
  
  console.log('');
  console.log('═'.repeat(90));
  console.log('  ✅ DIAGNÓSTICO COMPLETO');
  console.log('═'.repeat(90));
  
  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
