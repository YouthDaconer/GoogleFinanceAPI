/**
 * Script para recalcular cashflows y adj% de XTB usando las transacciones reales
 * 
 * Este script:
 * 1. Calcula el cashflow diario desde las transacciones reales
 * 2. Propaga cashflows de días sin documento al siguiente día disponible
 * 3. Recalcula el adjustedDailyChangePercentage
 * 
 * USO:
 *   node fixXTBCashFlows.js --dry-run
 *   node fixXTBCashFlows.js --fix
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  XTB_ACCOUNT: 'Z3gnboYgRlTvSZNGSu8j',
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
  START_DATE: '2025-01-01',
  END_DATE: '2025-12-31',
};

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    mode: args.includes('--fix') ? 'fix' : 'dry-run'
  };
}

function log(level, message) {
  const colors = {
    INFO: '\x1b[36m',
    SUCCESS: '\x1b[32m',
    WARN: '\x1b[33m',
    PROGRESS: '\x1b[35m',
  };
  const reset = '\x1b[0m';
  console.log(`${colors[level] || ''}[${level}]${reset} ${message}`);
}

async function main() {
  const options = parseArgs();
  
  console.log('═'.repeat(80));
  console.log('  CORRECCIÓN DE CASHFLOWS XTB DESDE TRANSACCIONES');
  console.log('═'.repeat(80));
  console.log('');
  log('INFO', `Modo: ${options.mode.toUpperCase()}`);
  console.log('');
  
  // 1. Obtener todas las transacciones
  log('PROGRESS', 'Cargando transacciones...');
  const txSnapshot = await db.collection('transactions')
    .where('portfolioAccountId', '==', CONFIG.XTB_ACCOUNT)
    .where('date', '>=', CONFIG.START_DATE)
    .where('date', '<=', CONFIG.END_DATE)
    .orderBy('date')
    .get();
  
  // Calcular cashflow por día
  const dailyCashFlows = {};
  
  txSnapshot.docs.forEach(doc => {
    const tx = doc.data();
    const date = tx.date;
    const amount = tx.amount || 0;
    const price = tx.price || 0;
    const total = amount * price;
    
    if (!dailyCashFlows[date]) dailyCashFlows[date] = 0;
    
    if (tx.type === 'buy') {
      dailyCashFlows[date] -= total;
    } else if (tx.type === 'sell') {
      dailyCashFlows[date] += total;
    } else if (tx.type === 'cash_income') {
      dailyCashFlows[date] -= amount; // cash_income es inyección de capital
    } else if (tx.type === 'cash_outcome') {
      dailyCashFlows[date] += amount;
    }
  });
  
  log('INFO', `Transacciones: ${txSnapshot.docs.length}, Días con transacciones: ${Object.keys(dailyCashFlows).length}`);
  
  // 2. Obtener todos los documentos
  log('PROGRESS', 'Cargando documentos de portfolioPerformance...');
  const docsSnapshot = await db.collection(`portfolioPerformance/${CONFIG.USER_ID}/accounts/${CONFIG.XTB_ACCOUNT}/dates`)
    .where('date', '>=', CONFIG.START_DATE)
    .where('date', '<=', CONFIG.END_DATE)
    .orderBy('date', 'asc')
    .get();
  
  const docsByDate = new Map();
  docsSnapshot.docs.forEach(doc => {
    const data = doc.data();
    docsByDate.set(data.date, { ref: doc.ref, data });
  });
  
  const existingDates = [...docsByDate.keys()].sort();
  log('INFO', `Documentos: ${existingDates.length}`);
  
  // 3. Propagar cashflows de días sin documento al siguiente día disponible
  log('PROGRESS', 'Propagando cashflows de días sin documento...');
  const finalCashFlows = {};
  
  // Inicializar con cashflows de días que tienen documento
  existingDates.forEach(date => {
    if (dailyCashFlows[date] !== undefined) {
      finalCashFlows[date] = dailyCashFlows[date];
    } else {
      finalCashFlows[date] = 0;
    }
  });
  
  // Propagar cashflows de días sin documento
  Object.keys(dailyCashFlows).sort().forEach(txDate => {
    if (!docsByDate.has(txDate)) {
      // Encontrar siguiente día disponible
      const nextDate = existingDates.find(d => d > txDate);
      if (nextDate) {
        finalCashFlows[nextDate] = (finalCashFlows[nextDate] || 0) + dailyCashFlows[txDate];
        log('INFO', `  Propagando ${txDate} -> ${nextDate}: ${dailyCashFlows[txDate].toFixed(2)}`);
      }
    }
  });
  
  // 4. Aplicar correcciones
  log('PROGRESS', 'Aplicando correcciones...');
  const documentsToFix = [];
  
  for (let i = 0; i < existingDates.length; i++) {
    const date = existingDates[i];
    const { ref, data } = docsByDate.get(date);
    
    // Obtener día anterior
    const prevData = i > 0 ? docsByDate.get(existingDates[i - 1])?.data : null;
    
    const cashFlowUSD = finalCashFlows[date] || 0;
    const currentCashFlow = data.USD?.totalCashFlow || 0;
    
    // Solo corregir si hay diferencia significativa
    if (Math.abs(cashFlowUSD - currentCashFlow) < 0.5) continue;
    
    const fixes = {};
    
    CONFIG.CURRENCIES.forEach(currency => {
      const currencyData = data[currency];
      if (!currencyData) return;
      
      let exchangeRate = 1;
      if (currency !== 'USD' && currencyData.totalValue && data.USD?.totalValue) {
        exchangeRate = currencyData.totalValue / data.USD.totalValue;
      }
      
      const cf = cashFlowUSD * exchangeRate;
      const prevTotalValue = prevData?.[currency]?.totalValue || 0;
      const currentTotalValue = currencyData.totalValue || 0;
      
      let adj = 0;
      if (prevTotalValue > 0) {
        adj = ((currentTotalValue - prevTotalValue + cf) / prevTotalValue) * 100;
      }
      
      fixes[currency] = {
        ...currencyData,
        totalCashFlow: cf,
        adjustedDailyChangePercentage: adj,
        dailyReturn: adj / 100,
      };
    });
    
    documentsToFix.push({ ref, date, fixes, cashFlowUSD, oldCashFlow: currentCashFlow });
  }
  
  log('INFO', `Documentos a corregir: ${documentsToFix.length}`);
  
  // 5. Mostrar cambios
  if (documentsToFix.length <= 20) {
    console.log('');
    documentsToFix.forEach(({ date, fixes, cashFlowUSD, oldCashFlow }) => {
      const adj = fixes.USD?.adjustedDailyChangePercentage || 0;
      log('INFO', `  ${date}: cf ${oldCashFlow.toFixed(0)} -> ${cashFlowUSD.toFixed(0)}, adj=${adj.toFixed(2)}%`);
    });
  }
  
  // 6. Aplicar cambios
  if (options.mode === 'fix' && documentsToFix.length > 0) {
    log('PROGRESS', 'Guardando cambios...');
    
    for (let i = 0; i < documentsToFix.length; i += 15) {
      const batch = db.batch();
      const chunk = documentsToFix.slice(i, i + 15);
      chunk.forEach(({ ref, fixes }) => batch.update(ref, fixes));
      await batch.commit();
    }
    
    log('SUCCESS', '✅ Correcciones aplicadas');
    
    // Invalidar cache
    const cache = await db.collection(`userData/${CONFIG.USER_ID}/performanceCache`).get();
    if (cache.docs.length > 0) {
      const batch = db.batch();
      cache.docs.forEach(doc => batch.delete(doc.ref));
      await batch.commit();
      log('SUCCESS', `Cache invalidado: ${cache.docs.length} docs`);
    }
  } else if (options.mode === 'dry-run') {
    log('WARN', 'Modo dry-run: No se aplicaron cambios');
    log('INFO', 'Ejecuta con --fix para aplicar');
  }
  
  // 7. Mostrar TWR mensual
  console.log('');
  log('PROGRESS', 'Calculando TWR mensual...');
  
  const monthlyTWR = {};
  let ytdProduct = 1;
  
  existingDates.forEach(date => {
    const month = date.substring(0, 7);
    const docEntry = docsByDate.get(date);
    
    // Usar el adj% corregido si está en documentsToFix, sino el existente
    let adj;
    const fix = documentsToFix.find(f => f.date === date);
    if (fix && options.mode === 'fix') {
      adj = fix.fixes.USD?.adjustedDailyChangePercentage || 0;
    } else if (fix) {
      adj = fix.fixes.USD?.adjustedDailyChangePercentage || 0; // Para dry-run mostrar el corregido
    } else {
      adj = docEntry?.data?.USD?.adjustedDailyChangePercentage || 0;
    }
    
    if (!monthlyTWR[month]) monthlyTWR[month] = 1;
    monthlyTWR[month] *= (1 + adj / 100);
    ytdProduct *= (1 + adj / 100);
  });
  
  console.log('');
  console.log('Mes         TWR%');
  console.log('-'.repeat(25));
  Object.keys(monthlyTWR).sort().forEach(month => {
    const twr = (monthlyTWR[month] - 1) * 100;
    console.log(`${month}   ${twr.toFixed(2).padStart(8)}%`);
  });
  console.log('-'.repeat(25));
  console.log(`YTD TWR: ${((ytdProduct - 1) * 100).toFixed(2)}%`);
  
  process.exit(0);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
