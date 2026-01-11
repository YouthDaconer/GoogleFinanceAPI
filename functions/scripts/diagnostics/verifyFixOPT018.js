/**
 * Script de Verificación Final: Validar Fix OPT-018
 * 
 * Datos del UI después del fix:
 * 
 * 2 Cuentas (IBKR+XTB):
 *   - YTD: 36.03%
 *   - 1Y: 32.77%
 *   - Marzo 2025: -5.80%
 *   - Ene: 3.18%, Feb: -0.19%, Mar: -5.80%, Abr: 9.01%, May: 8.09%
 *   - Jun: 2.54%, Jul: 2.08%, Ago: 3.56%, Sep: 5.26%, Oct: 3.84%
 *   - Nov: 0.69%, Dic: -0.29%
 *   - Total 2025: 36.02%
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
 * Calcular rendimiento mensual CORRECTO (con el fix aplicado)
 * El factor de inicio del mes se guarda ANTES de aplicar el cambio del primer día
 */
function calculateMonthlyReturns(docs, currency) {
  // Agrupar por año/mes
  const byYearMonth = {};
  
  docs.forEach(doc => {
    const date = DateTime.fromISO(doc.date);
    const year = date.year.toString();
    const month = (date.month - 1).toString();
    const key = `${year}-${month}`;
    
    if (!byYearMonth[key]) {
      byYearMonth[key] = { year, month, docs: [] };
    }
    byYearMonth[key].docs.push(doc);
  });
  
  // Calcular factor compuesto global y por mes
  let currentFactor = 1;
  const monthlyStartFactors = {};
  const monthlyEndFactors = {};
  
  // Ordenar docs globalmente por fecha
  const sortedDocs = [...docs].sort((a, b) => a.date.localeCompare(b.date));
  
  sortedDocs.forEach(doc => {
    const currencyData = doc[currency];
    if (!currencyData) return;
    
    const change = currencyData.adjustedDailyChangePercentage || 0;
    
    const date = DateTime.fromISO(doc.date);
    const year = date.year.toString();
    const month = (date.month - 1).toString();
    
    // FIX OPT-018: Guardar inicio ANTES del cambio
    if (!monthlyStartFactors[year]) monthlyStartFactors[year] = {};
    if (!monthlyStartFactors[year][month]) {
      monthlyStartFactors[year][month] = currentFactor;
    }
    
    // Aplicar cambio
    currentFactor *= (1 + change / 100);
    
    // Guardar final después del cambio
    if (!monthlyEndFactors[year]) monthlyEndFactors[year] = {};
    monthlyEndFactors[year][month] = currentFactor;
  });
  
  // Calcular rendimientos mensuales
  const results = {};
  
  Object.keys(monthlyStartFactors).forEach(year => {
    results[year] = { months: {}, total: 0 };
    let yearCompound = 1;
    
    Object.keys(monthlyStartFactors[year]).forEach(month => {
      const start = monthlyStartFactors[year][month];
      const end = monthlyEndFactors[year][month];
      const monthReturn = ((end / start) - 1) * 100;
      results[year].months[month] = monthReturn;
      yearCompound *= (1 + monthReturn / 100);
    });
    
    results[year].total = (yearCompound - 1) * 100;
  });
  
  return results;
}

/**
 * Agregar datos de múltiples cuentas y calcular rendimientos
 */
function aggregateAndCalculate(ibkrDocs, xtbDocs, currency) {
  // Crear mapa por fecha
  const byDate = new Map();
  
  const addDocs = (docs, accountName) => {
    docs.forEach(doc => {
      if (!byDate.has(doc.date)) {
        byDate.set(doc.date, { date: doc.date, accounts: {} });
      }
      byDate.get(doc.date).accounts[accountName] = doc[currency] || {};
    });
  };
  
  addDocs(ibkrDocs, 'IBKR');
  addDocs(xtbDocs, 'XTB');
  
  // Calcular cambio ponderado por fecha
  const sortedDates = [...byDate.keys()].sort();
  const aggregatedDocs = [];
  
  sortedDates.forEach(date => {
    const entry = byDate.get(date);
    const accounts = entry.accounts;
    
    const ibkr = accounts.IBKR || {};
    const xtb = accounts.XTB || {};
    
    const ibkrVal = ibkr.totalValue || 0;
    const xtbVal = xtb.totalValue || 0;
    const totalVal = ibkrVal + xtbVal;
    
    const ibkrChange = ibkr.adjustedDailyChangePercentage || 0;
    const xtbChange = xtb.adjustedDailyChangePercentage || 0;
    
    let weightedChange = 0;
    if (totalVal > 0) {
      weightedChange = (ibkrVal * ibkrChange + xtbVal * xtbChange) / totalVal;
    }
    
    aggregatedDocs.push({
      date,
      [currency]: {
        adjustedDailyChangePercentage: weightedChange,
        totalValue: totalVal
      }
    });
  });
  
  return calculateMonthlyReturns(aggregatedDocs, currency);
}

async function main() {
  console.log('');
  console.log('═'.repeat(90));
  console.log('  VERIFICACIÓN FINAL: Fix OPT-018 Aplicado');
  console.log('═'.repeat(90));
  console.log('');

  const [ibkrDocs, xtbDocs] = await Promise.all([
    getAccountDocs(CONFIG.ACCOUNTS.IBKR),
    getAccountDocs(CONFIG.ACCOUNTS.XTB)
  ]);

  console.log(`Documentos IBKR: ${ibkrDocs.length}`);
  console.log(`Documentos XTB: ${xtbDocs.length}`);

  // Calcular rendimientos agregados IBKR+XTB
  const aggregatedReturns = aggregateAndCalculate(ibkrDocs, xtbDocs, CONFIG.CURRENCY);

  console.log('');
  console.log('━'.repeat(90));
  console.log('  COMPARACIÓN: UI vs CÁLCULO VERIFICADO');
  console.log('━'.repeat(90));
  console.log('');

  // Datos del UI según la imagen
  const uiData = {
    '2025': {
      '0': 3.18,   // Ene
      '1': -0.19,  // Feb
      '2': -5.80,  // Mar
      '3': 9.01,   // Abr
      '4': 8.09,   // May
      '5': 2.54,   // Jun
      '6': 2.08,   // Jul
      '7': 3.56,   // Ago
      '8': 5.26,   // Sep
      '9': 3.84,   // Oct
      '10': 0.69,  // Nov
      '11': -0.29, // Dic
      'total': 36.02
    },
    '2024': {
      '6': -8.71,  // Jul
      '7': 13.83,  // Ago
      '8': -5.41,  // Sep
      '9': 8.21,   // Oct
      '10': -0.65, // Nov
      'total': 5.66
    }
  };

  const monthNames = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'];

  console.log('  2025:');
  console.log('  Mes      | UI        | Calculado | Diff    | Match');
  console.log('  ' + '-'.repeat(55));

  let allMatch = true;
  
  if (aggregatedReturns['2025']) {
    for (let m = 0; m <= 11; m++) {
      const month = m.toString();
      const uiVal = uiData['2025'][month];
      const calcVal = aggregatedReturns['2025'].months[month];
      
      if (uiVal !== undefined && calcVal !== undefined) {
        const diff = Math.abs(uiVal - calcVal);
        const match = diff < 0.1;
        if (!match) allMatch = false;
        
        console.log(
          `  ${monthNames[m].padEnd(8)} | ` +
          `${uiVal.toFixed(2).padStart(8)}% | ` +
          `${calcVal.toFixed(2).padStart(8)}% | ` +
          `${diff.toFixed(2).padStart(6)}% | ` +
          `${match ? '✅' : '❌'}`
        );
      }
    }
    
    // Total del año
    const uiTotal = uiData['2025'].total;
    const calcTotal = aggregatedReturns['2025'].total;
    const totalDiff = Math.abs(uiTotal - calcTotal);
    const totalMatch = totalDiff < 0.1;
    if (!totalMatch) allMatch = false;
    
    console.log('  ' + '-'.repeat(55));
    console.log(
      `  ${'TOTAL'.padEnd(8)} | ` +
      `${uiTotal.toFixed(2).padStart(8)}% | ` +
      `${calcTotal.toFixed(2).padStart(8)}% | ` +
      `${totalDiff.toFixed(2).padStart(6)}% | ` +
      `${totalMatch ? '✅' : '❌'}`
    );
  }

  // Verificar 2024
  console.log('');
  console.log('  2024:');
  console.log('  Mes      | UI        | Calculado | Diff    | Match');
  console.log('  ' + '-'.repeat(55));

  if (aggregatedReturns['2024']) {
    for (let m = 6; m <= 10; m++) {
      const month = m.toString();
      const uiVal = uiData['2024'][month];
      const calcVal = aggregatedReturns['2024'].months[month];
      
      if (uiVal !== undefined && calcVal !== undefined) {
        const diff = Math.abs(uiVal - calcVal);
        const match = diff < 0.1;
        if (!match) allMatch = false;
        
        console.log(
          `  ${monthNames[m].padEnd(8)} | ` +
          `${uiVal.toFixed(2).padStart(8)}% | ` +
          `${calcVal.toFixed(2).padStart(8)}% | ` +
          `${diff.toFixed(2).padStart(6)}% | ` +
          `${match ? '✅' : '❌'}`
        );
      }
    }
    
    // Total del año
    const uiTotal = uiData['2024'].total;
    const calcTotal = aggregatedReturns['2024'].total;
    const totalDiff = Math.abs(uiTotal - calcTotal);
    const totalMatch = totalDiff < 0.1;
    if (!totalMatch) allMatch = false;
    
    console.log('  ' + '-'.repeat(55));
    console.log(
      `  ${'TOTAL'.padEnd(8)} | ` +
      `${uiTotal.toFixed(2).padStart(8)}% | ` +
      `${calcTotal.toFixed(2).padStart(8)}% | ` +
      `${totalDiff.toFixed(2).padStart(6)}% | ` +
      `${totalMatch ? '✅' : '❌'}`
    );
  }

  // Verificación específica de marzo (el mes problemático)
  console.log('');
  console.log('━'.repeat(90));
  console.log('  VERIFICACIÓN ESPECÍFICA: MARZO 2025 (mes del bug original)');
  console.log('━'.repeat(90));
  console.log('');
  
  const marchUI = -5.80;
  const marchCalc = aggregatedReturns['2025']?.months['2'] || 0;
  const marchMatch = Math.abs(marchUI - marchCalc) < 0.1;
  
  console.log(`  Valor en UI:       ${marchUI.toFixed(2)}%`);
  console.log(`  Valor calculado:   ${marchCalc.toFixed(2)}%`);
  console.log(`  Diferencia:        ${Math.abs(marchUI - marchCalc).toFixed(2)}%`);
  console.log(`  Valor ANTES del fix: -4.25% (incorrecto, perdía primer día)`);
  console.log(`  ¿Correcto?         ${marchMatch ? '✅ SÍ' : '❌ NO'}`);

  console.log('');
  console.log('═'.repeat(90));
  if (allMatch) {
    console.log('  ✅ VERIFICACIÓN EXITOSA: Todos los valores del UI coinciden con los cálculos');
  } else {
    console.log('  ⚠️ VERIFICACIÓN PARCIAL: Algunos valores no coinciden exactamente');
  }
  console.log('═'.repeat(90));

  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
