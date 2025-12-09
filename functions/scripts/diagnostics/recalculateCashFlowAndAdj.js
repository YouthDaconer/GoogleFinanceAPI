/**
 * Script para recalcular totalCashFlow y adjustedDailyChangePercentage
 * basándose en las transacciones reales de cada día.
 * 
 * El problema: los documentos del backfill tienen totalCashFlow=0 cuando
 * debería reflejar las compras/ventas del día.
 * 
 * USO:
 *   node recalculateCashFlowAndAdj.js --dry-run    # Ver cambios sin aplicar
 *   node recalculateCashFlowAndAdj.js --fix        # Aplicar cambios
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
  BATCH_SIZE: 15,
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
    ERROR: '\x1b[31m',
  };
  const reset = '\x1b[0m';
  console.log(`${colors[level] || ''}[${level}]${reset} ${message}`);
}

/**
 * Obtener todas las transacciones de una cuenta
 */
async function getTransactions(accountId) {
  const snapshot = await db.collection('transactions')
    .where('portfolioAccountId', '==', accountId)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

/**
 * Calcular el cashflow neto de un día basado en transacciones
 * Cashflow es NEGATIVO para compras (dinero que sale) y POSITIVO para ventas
 */
function calculateDailyCashFlow(transactions, date) {
  return transactions
    .filter(tx => tx.date === date)
    .reduce((sum, tx) => {
      const amount = tx.amount || 0;
      const price = tx.price || 0;
      
      if (tx.type === 'buy') {
        // Compra: dinero sale -> cashflow negativo
        return sum - (amount * price);
      } else if (tx.type === 'sell') {
        // Venta: dinero entra -> cashflow positivo
        return sum + (amount * price);
      } else if (tx.type === 'cash_income') {
        // Ingreso de efectivo
        return sum + amount;
      } else if (tx.type === 'cash_outcome') {
        // Retiro de efectivo
        return sum - amount;
      }
      return sum;
    }, 0);
}

/**
 * Calcular cashflow por asset para un día
 */
function calculateAssetCashFlows(transactions, date) {
  const assetCashFlows = {};
  
  transactions
    .filter(tx => tx.date === date && tx.assetName)
    .forEach(tx => {
      const assetKey = `${tx.assetName}_${tx.assetType || 'stock'}`;
      const amount = tx.amount || 0;
      const price = tx.price || 0;
      
      if (!assetCashFlows[assetKey]) {
        assetCashFlows[assetKey] = 0;
      }
      
      if (tx.type === 'buy') {
        assetCashFlows[assetKey] -= (amount * price);
      } else if (tx.type === 'sell') {
        assetCashFlows[assetKey] += (amount * price);
      }
    });
  
  return assetCashFlows;
}

async function recalculateAccount(accountId, transactions, options) {
  log('INFO', `Procesando cuenta ${accountId}...`);
  
  // Obtener todos los documentos de la cuenta
  const allDocs = await db.collection(`portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`)
    .where('date', '>=', CONFIG.START_DATE)
    .where('date', '<=', CONFIG.END_DATE)
    .orderBy('date', 'asc')
    .get();
  
  const docsByDate = new Map();
  allDocs.docs.forEach(doc => {
    const data = doc.data();
    docsByDate.set(data.date, { ref: doc.ref, data });
  });
  
  const allDates = [...docsByDate.keys()].sort();
  const documentsToFix = [];
  
  for (let i = 0; i < allDates.length; i++) {
    const date = allDates[i];
    const { ref, data } = docsByDate.get(date);
    const previousData = i > 0 ? docsByDate.get(allDates[i - 1])?.data : null;
    
    // Calcular cashflow real del día
    const dailyCashFlowUSD = calculateDailyCashFlow(transactions, date);
    const assetCashFlows = calculateAssetCashFlows(transactions, date);
    
    const fixes = {};
    let hasChanges = false;
    
    CONFIG.CURRENCIES.forEach(currency => {
      const currencyData = data[currency];
      if (!currencyData) return;
      
      const prevCurrencyData = previousData?.[currency];
      const prevTotalValue = prevCurrencyData?.totalValue || 0;
      const currentTotalValue = currencyData.totalValue || 0;
      
      // Obtener tipo de cambio para esta moneda
      // Asumimos que el tipo de cambio está implícito en los valores
      let exchangeRate = 1;
      if (currency !== 'USD' && currencyData.totalValue && data.USD?.totalValue) {
        exchangeRate = currencyData.totalValue / data.USD.totalValue;
      }
      
      // Calcular cashflow en esta moneda
      const totalCashFlow = dailyCashFlowUSD * exchangeRate;
      
      // Calcular adj% correcto
      let adjustedDailyChangePercentage = 0;
      if (prevTotalValue > 0) {
        adjustedDailyChangePercentage = ((currentTotalValue - prevTotalValue + totalCashFlow) / prevTotalValue) * 100;
      }
      
      // Verificar si hay cambio significativo
      const oldCashFlow = currencyData.totalCashFlow || 0;
      const oldAdj = currencyData.adjustedDailyChangePercentage || 0;
      
      if (Math.abs(oldCashFlow - totalCashFlow) > 1 || Math.abs(oldAdj - adjustedDailyChangePercentage) > 0.1) {
        // Recalcular assetPerformance con cashflows correctos
        const assetPerformance = { ...currencyData.assetPerformance };
        
        Object.entries(assetPerformance).forEach(([assetKey, asset]) => {
          const assetCashFlowUSD = assetCashFlows[assetKey] || 0;
          const assetCashFlow = assetCashFlowUSD * exchangeRate;
          
          const prevAsset = prevCurrencyData?.assetPerformance?.[assetKey];
          const prevAssetValue = prevAsset?.totalValue || 0;
          const currentAssetValue = asset.totalValue || 0;
          
          // Calcular adj% del asset
          let assetAdj = 0;
          if (prevAssetValue > 0) {
            assetAdj = ((currentAssetValue - prevAssetValue + assetCashFlow) / prevAssetValue) * 100;
          }
          
          assetPerformance[assetKey] = {
            ...asset,
            totalCashFlow: assetCashFlow,
            adjustedDailyChangePercentage: assetAdj,
            dailyReturn: assetAdj / 100,
          };
        });
        
        fixes[currency] = {
          ...currencyData,
          totalCashFlow,
          adjustedDailyChangePercentage,
          dailyReturn: adjustedDailyChangePercentage / 100,
          assetPerformance,
        };
        hasChanges = true;
      }
    });
    
    if (hasChanges) {
      documentsToFix.push({ ref, date, fixes });
      
      if (options.mode === 'dry-run' && documentsToFix.length <= 5) {
        log('INFO', `  ${date}: cf=${fixes.USD?.totalCashFlow?.toFixed(0) || 0}, adj=${fixes.USD?.adjustedDailyChangePercentage?.toFixed(2) || 0}%`);
      }
    }
  }
  
  // Aplicar correcciones
  if (options.mode === 'fix' && documentsToFix.length > 0) {
    for (let i = 0; i < documentsToFix.length; i += CONFIG.BATCH_SIZE) {
      const batch = db.batch();
      const chunk = documentsToFix.slice(i, i + CONFIG.BATCH_SIZE);
      chunk.forEach(({ ref, fixes }) => batch.update(ref, fixes));
      await batch.commit();
    }
  }
  
  return documentsToFix.length;
}

async function recalculateOverall(options) {
  log('PROGRESS', 'Recalculando OVERALL...');
  
  // Obtener todos los documentos OVERALL
  const overallDocs = await db.collection(`portfolioPerformance/${CONFIG.USER_ID}/dates`)
    .where('date', '>=', CONFIG.START_DATE)
    .where('date', '<=', CONFIG.END_DATE)
    .orderBy('date', 'asc')
    .get();
  
  // Obtener documentos de cada cuenta para agregar
  const accountDocs = {};
  for (const accountId of CONFIG.ACCOUNTS) {
    const docs = await db.collection(`portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`)
      .where('date', '>=', CONFIG.START_DATE)
      .where('date', '<=', CONFIG.END_DATE)
      .get();
    
    accountDocs[accountId] = new Map();
    docs.docs.forEach(doc => {
      const data = doc.data();
      accountDocs[accountId].set(data.date, data);
    });
  }
  
  const overallByDate = new Map();
  overallDocs.docs.forEach(doc => {
    const data = doc.data();
    overallByDate.set(data.date, { ref: doc.ref, data });
  });
  
  const allDates = [...overallByDate.keys()].sort();
  const documentsToFix = [];
  
  for (let i = 0; i < allDates.length; i++) {
    const date = allDates[i];
    const { ref, data } = overallByDate.get(date);
    const previousData = i > 0 ? overallByDate.get(allDates[i - 1])?.data : null;
    
    const fixes = {};
    let hasChanges = false;
    
    CONFIG.CURRENCIES.forEach(currency => {
      const currencyData = data[currency];
      if (!currencyData) return;
      
      // Sumar cashflows de todas las cuentas
      let totalCashFlow = 0;
      let totalValue = 0;
      
      CONFIG.ACCOUNTS.forEach(accountId => {
        const accountData = accountDocs[accountId].get(date);
        if (accountData?.[currency]) {
          totalCashFlow += accountData[currency].totalCashFlow || 0;
          totalValue += accountData[currency].totalValue || 0;
        }
      });
      
      // También incluir Binance si existe
      // (No lo procesamos pero está en OVERALL)
      
      const prevCurrencyData = previousData?.[currency];
      const prevTotalValue = prevCurrencyData?.totalValue || 0;
      const currentTotalValue = currencyData.totalValue || 0;
      
      // Calcular adj% correcto
      let adjustedDailyChangePercentage = 0;
      if (prevTotalValue > 0) {
        adjustedDailyChangePercentage = ((currentTotalValue - prevTotalValue + totalCashFlow) / prevTotalValue) * 100;
      }
      
      const oldCashFlow = currencyData.totalCashFlow || 0;
      const oldAdj = currencyData.adjustedDailyChangePercentage || 0;
      
      if (Math.abs(oldCashFlow - totalCashFlow) > 1 || Math.abs(oldAdj - adjustedDailyChangePercentage) > 0.1) {
        fixes[currency] = {
          ...currencyData,
          totalCashFlow,
          adjustedDailyChangePercentage,
          dailyReturn: adjustedDailyChangePercentage / 100,
        };
        hasChanges = true;
      }
    });
    
    if (hasChanges) {
      documentsToFix.push({ ref, date, fixes });
    }
  }
  
  // Aplicar correcciones
  if (options.mode === 'fix' && documentsToFix.length > 0) {
    for (let i = 0; i < documentsToFix.length; i += CONFIG.BATCH_SIZE) {
      const batch = db.batch();
      const chunk = documentsToFix.slice(i, i + CONFIG.BATCH_SIZE);
      chunk.forEach(({ ref, fixes }) => batch.update(ref, fixes));
      await batch.commit();
    }
  }
  
  return documentsToFix.length;
}

async function main() {
  const options = parseArgs();
  
  console.log('═'.repeat(80));
  console.log('  RECÁLCULO DE CASHFLOW Y ADJ% DESDE TRANSACCIONES');
  console.log('═'.repeat(80));
  console.log('');
  log('INFO', `Modo: ${options.mode.toUpperCase()}`);
  console.log('');
  
  let totalFixed = 0;
  
  // Procesar cada cuenta
  for (const accountId of CONFIG.ACCOUNTS) {
    const transactions = await getTransactions(accountId);
    log('INFO', `  Transacciones cargadas: ${transactions.length}`);
    
    const fixed = await recalculateAccount(accountId, transactions, options);
    log('INFO', `  ${accountId}: ${fixed} docs a corregir`);
    totalFixed += fixed;
  }
  
  // Procesar OVERALL
  const overallFixed = await recalculateOverall(options);
  log('INFO', `  OVERALL: ${overallFixed} docs a corregir`);
  totalFixed += overallFixed;
  
  // Resumen
  console.log('');
  console.log('═'.repeat(80));
  log('INFO', `Total documentos a corregir: ${totalFixed}`);
  
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
