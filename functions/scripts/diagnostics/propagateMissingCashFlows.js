/**
 * Script para propagar cashflows de días faltantes al siguiente día disponible
 * 
 * Cuando hay transacciones en días sin documento de portfolioPerformance,
 * el cashflow de ese día debe sumarse al cashflow del siguiente día disponible.
 * 
 * USO:
 *   node propagateMissingCashFlows.js --dry-run
 *   node propagateMissingCashFlows.js --fix
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
  ACCOUNTS: ['BZHvXz4QT2yqqqlFP22X', 'Z3gnboYgRlTvSZNGSu8j'],
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
  START_DATE: '2025-01-01',
  END_DATE: '2025-06-01',
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

async function processAccount(accountId, options) {
  log('INFO', `Procesando cuenta ${accountId}...`);
  
  // 1. Obtener documentos existentes
  const docs = await db.collection(`portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`)
    .where('date', '>=', CONFIG.START_DATE)
    .where('date', '<=', CONFIG.END_DATE)
    .orderBy('date', 'asc')
    .get();
  
  const docsByDate = new Map();
  docs.docs.forEach(doc => {
    const data = doc.data();
    docsByDate.set(data.date, { ref: doc.ref, data });
  });
  
  const existingDates = [...docsByDate.keys()].sort();
  
  // 2. Obtener transacciones
  const txSnapshot = await db.collection('transactions')
    .where('portfolioAccountId', '==', accountId)
    .where('date', '>=', CONFIG.START_DATE)
    .where('date', '<=', CONFIG.END_DATE)
    .orderBy('date', 'asc')
    .get();
  
  // 3. Agrupar transacciones por fecha
  const txByDate = {};
  txSnapshot.docs.forEach(doc => {
    const tx = doc.data();
    if (!txByDate[tx.date]) txByDate[tx.date] = [];
    txByDate[tx.date].push(tx);
  });
  
  // 4. Calcular cashflows de días faltantes
  const missingCashFlows = {};
  
  Object.keys(txByDate).sort().forEach(date => {
    if (!docsByDate.has(date)) {
      let cashFlow = 0;
      txByDate[date].forEach(tx => {
        const total = (tx.amount || 0) * (tx.price || 0);
        if (tx.type === 'buy') cashFlow -= total;
        else if (tx.type === 'sell') cashFlow += total;
        else if (tx.type === 'cash_income') cashFlow += tx.amount;
        else if (tx.type === 'cash_outcome') cashFlow -= tx.amount;
      });
      
      if (Math.abs(cashFlow) > 0.01) {
        // Encontrar siguiente día disponible
        const nextDate = existingDates.find(d => d > date);
        if (nextDate) {
          if (!missingCashFlows[nextDate]) {
            missingCashFlows[nextDate] = { total: 0, sources: [] };
          }
          missingCashFlows[nextDate].total += cashFlow;
          missingCashFlows[nextDate].sources.push({ date, cashFlow });
        }
      }
    }
  });
  
  // 5. Aplicar correcciones
  const documentsToFix = [];
  
  for (const [targetDate, missing] of Object.entries(missingCashFlows)) {
    const docEntry = docsByDate.get(targetDate);
    if (!docEntry) continue;
    
    const { ref, data } = docEntry;
    
    // Encontrar día anterior
    const dateIndex = existingDates.indexOf(targetDate);
    const prevDate = dateIndex > 0 ? existingDates[dateIndex - 1] : null;
    const prevData = prevDate ? docsByDate.get(prevDate)?.data : null;
    
    const fixes = {};
    
    CONFIG.CURRENCIES.forEach(currency => {
      const currencyData = data[currency];
      if (!currencyData) return;
      
      // Calcular tipo de cambio
      let exchangeRate = 1;
      if (currency !== 'USD' && currencyData.totalValue && data.USD?.totalValue) {
        exchangeRate = currencyData.totalValue / data.USD.totalValue;
      }
      
      // Nuevo cashflow
      const additionalCashFlow = missing.total * exchangeRate;
      const newTotalCashFlow = (currencyData.totalCashFlow || 0) + additionalCashFlow;
      
      // Recalcular adj%
      const prevTotalValue = prevData?.[currency]?.totalValue || 0;
      const currentTotalValue = currencyData.totalValue || 0;
      
      let newAdj = currencyData.adjustedDailyChangePercentage || 0;
      if (prevTotalValue > 0) {
        newAdj = ((currentTotalValue - prevTotalValue + newTotalCashFlow) / prevTotalValue) * 100;
      }
      
      fixes[currency] = {
        ...currencyData,
        totalCashFlow: newTotalCashFlow,
        adjustedDailyChangePercentage: newAdj,
        dailyReturn: newAdj / 100,
      };
    });
    
    documentsToFix.push({ 
      ref, 
      targetDate, 
      fixes,
      sources: missing.sources,
      additionalCashFlow: missing.total 
    });
    
    log('INFO', `  ${targetDate}: +cf=${missing.total.toFixed(0)} (de ${missing.sources.map(s => s.date).join(', ')})`);
  }
  
  // 6. Guardar cambios
  if (options.mode === 'fix' && documentsToFix.length > 0) {
    const batch = db.batch();
    documentsToFix.forEach(({ ref, fixes }) => {
      batch.update(ref, fixes);
    });
    await batch.commit();
  }
  
  return documentsToFix.length;
}

async function main() {
  const options = parseArgs();
  
  console.log('═'.repeat(80));
  console.log('  PROPAGACIÓN DE CASHFLOWS DE DÍAS FALTANTES');
  console.log('═'.repeat(80));
  console.log('');
  log('INFO', `Modo: ${options.mode.toUpperCase()}`);
  console.log('');
  
  let totalFixed = 0;
  
  for (const accountId of CONFIG.ACCOUNTS) {
    const fixed = await processAccount(accountId, options);
    log('INFO', `  ${accountId}: ${fixed} documentos actualizados`);
    totalFixed += fixed;
  }
  
  console.log('');
  console.log('═'.repeat(80));
  log('INFO', `Total documentos actualizados: ${totalFixed}`);
  
  if (options.mode === 'fix') {
    log('SUCCESS', '✅ Corrección completada');
    
    // Invalidar cache
    const cache = await db.collection(`userData/${CONFIG.USER_ID}/performanceCache`).get();
    if (cache.docs.length > 0) {
      const batch = db.batch();
      cache.docs.forEach(doc => batch.delete(doc.ref));
      await batch.commit();
      log('SUCCESS', `Cache invalidado: ${cache.docs.length} docs`);
    }
  } else {
    log('WARN', 'Modo dry-run: No se aplicaron cambios');
    log('INFO', 'Ejecuta con --fix para aplicar');
  }
  
  process.exit(0);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
