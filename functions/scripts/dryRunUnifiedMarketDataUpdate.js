/**
 * DRY-RUN: Simula la ejecución de unifiedMarketDataUpdate sin escribir a Firestore
 * 
 * Este script ejecuta la lógica de cálculo de rendimiento del portafolio
 * exactamente como lo haría la función scheduled, pero:
 * - NO escribe a Firestore (dry-run)
 * - Muestra los resultados que SE ESCRIBIRÍAN
 * - Permite verificar que los cálculos son correctos antes de ejecutar en producción
 * 
 * Uso:
 *   node scripts/dryRunUnifiedMarketDataUpdate.js [userId] [date]
 * 
 * Ejemplos:
 *   node scripts/dryRunUnifiedMarketDataUpdate.js
 *   node scripts/dryRunUnifiedMarketDataUpdate.js DDeR8P5hYgfuN8gcU4RsQfdTJqx2
 *   node scripts/dryRunUnifiedMarketDataUpdate.js DDeR8P5hYgfuN8gcU4RsQfdTJqx2 2026-01-28
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');
const { calculateAccountPerformance, convertCurrency } = require('../utils/portfolioCalculations');
const { getPricesFromApi, getCurrencyRatesFromApi } = require('../services/marketDataHelper');

// Inicializar Firebase
const serviceAccount = require('../key.json');
if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}
const db = admin.firestore();

// Colores para la consola
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m'
};

function log(color, ...args) {
  console.log(color, ...args, colors.reset);
}

/**
 * Clase de caché para datos históricos (copiada de unifiedMarketDataUpdate.js)
 */
class PerformanceDataCache {
  constructor() {
    this.userLastPerformance = new Map();
    this.accountLastPerformance = new Map();
    this.buyTransactionsByAsset = new Map();
  }

  async preloadHistoricalData(db, formattedDate, userPortfolios, sellTransactions) {
    log(colors.cyan, '📁 Precargando datos históricos...');
    
    const allUserIds = Object.keys(userPortfolios);
    const allAccountIds = Object.values(userPortfolios).flat().map(acc => acc.id);
    const assetIdsWithSells = [...new Set(sellTransactions.map(t => t.assetId).filter(id => id))];

    const [userPerformanceResults, accountPerformanceResults, buyTransactionsResults] = await Promise.all([
      Promise.all(allUserIds.map(async (userId) => {
        const snapshot = await db.collection('portfolioPerformance')
          .doc(userId)
          .collection('dates')
          .where('date', '<', formattedDate)
          .orderBy('date', 'desc')
          .limit(1)
          .get();
        return { userId, data: snapshot.empty ? null : snapshot.docs[0].data() };
      })),
      
      Promise.all(allAccountIds.map(async (accountId) => {
        const accountDoc = await db.collection('portfolioAccounts').doc(accountId).get();
        if (!accountDoc.exists) return { accountId, data: null };
        const userId = accountDoc.data().userId;
        
        const snapshot = await db.collection('portfolioPerformance')
          .doc(userId)
          .collection('accounts')
          .doc(accountId)
          .collection('dates')
          .where('date', '<', formattedDate)
          .orderBy('date', 'desc')
          .limit(1)
          .get();
        return { accountId, data: snapshot.empty ? null : snapshot.docs[0].data() };
      })),
      
      assetIdsWithSells.length > 0 ? 
        db.collection('transactions')
          .where('type', '==', 'buy')
          .where('assetId', 'in', assetIdsWithSells)
          .get() : 
        { docs: [] }
    ]);

    userPerformanceResults.forEach(({ userId, data }) => {
      if (data) this.userLastPerformance.set(userId, data);
    });

    accountPerformanceResults.forEach(({ accountId, data }) => {
      if (data) this.accountLastPerformance.set(accountId, data);
    });

    if (buyTransactionsResults.docs) {
      const buyTransactions = buyTransactionsResults.docs.map(doc => ({ id: doc.id, ...doc.data() }));
      buyTransactions.forEach(tx => {
        if (!this.buyTransactionsByAsset.has(tx.assetId)) {
          this.buyTransactionsByAsset.set(tx.assetId, []);
        }
        this.buyTransactionsByAsset.get(tx.assetId).push(tx);
      });
    }

    log(colors.green, `✅ Caché precargado: ${this.userLastPerformance.size} usuarios, ${this.accountLastPerformance.size} cuentas`);
  }

  getUserLastPerformance(userId, currencies) {
    const data = this.userLastPerformance.get(userId);
    if (!data) {
      return currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: { totalValue: 0 } }), {});
    }
    
    return Object.entries(data).reduce((acc, [currency, currencyData]) => {
      if (currency !== 'date') {
        acc[currency] = {
          totalValue: currencyData.totalValue || 0,
          ...Object.entries(currencyData.assetPerformance || {}).reduce((assetAcc, [assetName, assetData]) => {
            assetAcc[assetName] = {
              totalValue: assetData.totalValue || 0,
              units: assetData.units || 0,
            };
            return assetAcc;
          }, {})
        };
      }
      return acc;
    }, {});
  }

  getAccountLastPerformance(accountId, currencies) {
    const data = this.accountLastPerformance.get(accountId);
    if (!data) {
      return currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: { totalValue: 0 } }), {});
    }
    
    return Object.entries(data).reduce((acc, [currency, currencyData]) => {
      if (currency !== 'date') {
        acc[currency] = {
          totalValue: currencyData.totalValue || 0,
          ...Object.entries(currencyData.assetPerformance || {}).reduce((assetAcc, [assetName, assetData]) => {
            assetAcc[assetName] = {
              totalValue: assetData.totalValue || 0,
              units: assetData.units || 0,
            };
            return assetAcc;
          }, {})
        };
      }
      return acc;
    }, {});
  }

  getBuyTransactionsForAsset(assetId) {
    return this.buyTransactionsByAsset.get(assetId) || [];
  }
}

/**
 * Calcula P&L realizado de transacciones de venta
 */
function calculateDoneProfitAndLoss(sellTransactions, allAssets, currencies, cache) {
  const donePnLByCurrency = {};
  
  for (const currency of currencies) {
    let totalDonePnL = 0;
    const assetDonePnL = {};
    
    sellTransactions.forEach(sellTx => {
      if (sellTx.assetId) {
        const asset = allAssets.find(a => a.id === sellTx.assetId);
        if (asset) {
          const assetKey = `${asset.name}_${asset.assetType}`;
          if (!assetDonePnL[assetKey]) assetDonePnL[assetKey] = 0;
          
          let profitAndLoss = 0;
          
          if (sellTx.valuePnL !== undefined && sellTx.valuePnL !== null) {
            profitAndLoss = convertCurrency(
              sellTx.valuePnL,
              sellTx.currency || asset.currency,
              currency.code,
              currencies,
              sellTx.dollarPriceToDate,
              sellTx.defaultCurrencyForAdquisitionDollar
            );
          } else {
            const sellValue = sellTx.amount * sellTx.price;
            const buyTransactions = cache.getBuyTransactionsForAsset(sellTx.assetId);
            
            let remainingUnits = sellTx.amount;
            let totalCost = 0;
            
            const sortedBuys = [...buyTransactions].sort((a, b) => 
              new Date(a.date).getTime() - new Date(b.date).getTime()
            );
            
            for (const buyTx of sortedBuys) {
              if (remainingUnits <= 0) break;
              const unitsFromThisBuy = Math.min(remainingUnits, buyTx.amount);
              totalCost += unitsFromThisBuy * buyTx.price;
              remainingUnits -= unitsFromThisBuy;
            }
            
            if (remainingUnits > 0 && asset.unitValue) {
              totalCost += remainingUnits * asset.unitValue;
            }
            
            profitAndLoss = convertCurrency(
              sellValue - totalCost,
              sellTx.currency || asset.currency,
              currency.code,
              currencies,
              sellTx.dollarPriceToDate,
              sellTx.defaultCurrencyForAdquisitionDollar
            );
          }
          
          assetDonePnL[assetKey] += profitAndLoss;
          totalDonePnL += profitAndLoss;
        }
      }
    });
    
    donePnLByCurrency[currency.code] = {
      doneProfitAndLoss: totalDonePnL,
      assetDoneProfitAndLoss: assetDonePnL
    };
  }
  
  return donePnLByCurrency;
}

/**
 * Ejecuta el cálculo de rendimiento en modo dry-run
 */
async function dryRunCalculation(targetUserId = null, targetDate = null) {
  log(colors.bright + colors.blue, '\n' + '='.repeat(80));
  log(colors.bright + colors.blue, '🧪 DRY-RUN: Simulación de unifiedMarketDataUpdate');
  log(colors.bright + colors.blue, '='.repeat(80) + '\n');
  
  // Determinar fecha de cálculo
  const now = DateTime.now().setZone('America/New_York');
  let calcDate;
  
  if (targetDate) {
    calcDate = DateTime.fromISO(targetDate);
    log(colors.yellow, `📅 Usando fecha especificada: ${targetDate}`);
  } else {
    // Por defecto, calcular para el día anterior (como lo haría la función scheduled)
    calcDate = now.minus({ days: 1 });
    log(colors.yellow, `📅 Calculando para día anterior: ${calcDate.toISODate()}`);
  }
  
  const formattedDate = calcDate.toISODate();
  const dateRangeStart = `${formattedDate}T00:00:00.000Z`;
  const dateRangeEnd = `${formattedDate}T23:59:59.999Z`;
  
  log(colors.cyan, `\n📊 Fecha de cálculo: ${formattedDate}`);
  log(colors.cyan, `🕐 Hora actual (NY): ${now.toISO()}\n`);
  
  // 1. Obtener datos de Firestore
  log(colors.blue, '📡 Obteniendo datos de Firestore...');
  
  const [transactionsSnapshot, activeAssetsSnapshot, portfolioAccountsSnapshot] = await Promise.all([
    db.collection('transactions')
      .where('date', '>=', dateRangeStart)
      .where('date', '<=', dateRangeEnd)
      .get(),
    db.collection('assets').where('isActive', '==', true).get(),
    db.collection('portfolioAccounts').where('isActive', '==', true).get()
  ]);
  
  const todaysTransactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const sellTransactions = todaysTransactions.filter(t => t.type === 'sell');
  const assetIdsInSellTransactions = [...new Set(sellTransactions.map(t => t.assetId).filter(id => id))];
  
  log(colors.green, `   ✓ Transacciones del ${formattedDate}: ${todaysTransactions.length} (${sellTransactions.length} ventas)`);
  
  const activeAssets = activeAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  log(colors.green, `   ✓ Assets activos: ${activeAssets.length}`);
  
  const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  log(colors.green, `   ✓ Cuentas activas: ${portfolioAccounts.length}`);
  
  // Filtrar por usuario si se especificó
  let filteredAccounts = portfolioAccounts;
  if (targetUserId) {
    filteredAccounts = portfolioAccounts.filter(acc => acc.userId === targetUserId);
    log(colors.yellow, `   ⚠ Filtrando por usuario: ${targetUserId} (${filteredAccounts.length} cuentas)`);
  }
  
  // Obtener activos inactivos involucrados en ventas
  let inactiveAssets = [];
  if (assetIdsInSellTransactions.length > 0) {
    const inactiveAssetsSnapshot = await db.collection('assets')
      .where('isActive', '==', false)
      .get();
    
    inactiveAssets = inactiveAssetsSnapshot.docs
      .map(doc => ({ id: doc.id, ...doc.data() }))
      .filter(asset => assetIdsInSellTransactions.includes(asset.id));
    
    log(colors.green, `   ✓ Assets inactivos (ventas): ${inactiveAssets.length}`);
  }
  
  const allAssets = [...activeAssets, ...inactiveAssets];
  
  // 2. Obtener precios y currencies del API
  log(colors.blue, '\n📡 Obteniendo precios del API Lambda...');
  
  const symbols = [...new Set(activeAssets.map(a => a.name).filter(Boolean))];
  log(colors.cyan, `   Símbolos únicos: ${symbols.length}`);
  
  const [currentPrices, currencies] = await Promise.all([
    getPricesFromApi(symbols),
    getCurrencyRatesFromApi()
  ]);
  
  log(colors.green, `   ✓ Precios obtenidos: ${currentPrices.length}`);
  log(colors.green, `   ✓ Currencies obtenidos: ${currencies.length}`);
  
  // 3. Agrupar por usuario
  const userPortfolios = filteredAccounts.reduce((acc, account) => {
    if (!acc[account.userId]) acc[account.userId] = [];
    acc[account.userId].push(account);
    return acc;
  }, {});
  
  log(colors.blue, `\n👥 Usuarios a procesar: ${Object.keys(userPortfolios).length}`);
  
  // 4. Precargar datos históricos
  const cache = new PerformanceDataCache();
  await cache.preloadHistoricalData(db, formattedDate, userPortfolios, sellTransactions);
  
  // 5. Calcular rendimiento para cada usuario
  const results = [];
  
  for (const [userId, accounts] of Object.entries(userPortfolios)) {
    log(colors.magenta, `\n${'─'.repeat(60)}`);
    log(colors.bright + colors.magenta, `👤 Usuario: ${userId}`);
    log(colors.magenta, `   Cuentas: ${accounts.map(a => a.name).join(', ')}`);
    
    const lastOverallTotalValue = cache.getUserLastPerformance(userId, currencies);
    
    const allUserAssets = allAssets.filter(asset => 
      accounts.some(account => account.id === asset.portfolioAccount)
    );
    
    const userTransactions = todaysTransactions.filter(t => 
      accounts.some(account => account.id === t.portfolioAccountId)
    );
    
    log(colors.cyan, `   Assets del usuario: ${allUserAssets.length}`);
    log(colors.cyan, `   Transacciones del día: ${userTransactions.length}`);
    
    // Calcular performance overall
    const overallPerformance = calculateAccountPerformance(
      allUserAssets,
      currentPrices,
      currencies,
      lastOverallTotalValue,
      userTransactions
    );
    
    // Calcular doneProfitAndLoss
    const userSellTransactions = sellTransactions.filter(t => 
      accounts.some(account => account.id === t.portfolioAccountId)
    );
    
    const userDonePnL = calculateDoneProfitAndLoss(userSellTransactions, allAssets, currencies, cache);
    
    // Agregar doneProfitAndLoss a overallPerformance
    for (const [currencyCode, data] of Object.entries(userDonePnL)) {
      if (overallPerformance[currencyCode]) {
        overallPerformance[currencyCode].doneProfitAndLoss = data.doneProfitAndLoss;
        overallPerformance[currencyCode].unrealizedProfitAndLoss = 
          overallPerformance[currencyCode].totalValue - overallPerformance[currencyCode].totalInvestment;
      }
    }
    
    // Mostrar resultados USD
    const usd = overallPerformance.USD || {};
    log(colors.bright + colors.green, `\n   📊 OVERALL PERFORMANCE (USD):`);
    log(colors.green, `      Total Value:        $${(usd.totalValue || 0).toFixed(2)}`);
    log(colors.green, `      Total Investment:   $${(usd.totalInvestment || 0).toFixed(2)}`);
    log(colors.green, `      Total CashFlow:     $${(usd.totalCashFlow || 0).toFixed(2)}`);
    log(colors.green, `      Done P&L:           $${(usd.doneProfitAndLoss || 0).toFixed(2)}`);
    log(colors.green, `      Unrealized P&L:     $${(usd.unrealizedProfitAndLoss || 0).toFixed(2)}`);
    log(colors.green, `      Daily Change %:     ${(usd.adjustedDailyChangePercent || 0).toFixed(2)}%`);
    
    // Mostrar assets principales
    const assetPerf = usd.assetPerformance || {};
    const topAssets = Object.entries(assetPerf)
      .sort((a, b) => (b[1].totalValue || 0) - (a[1].totalValue || 0))
      .slice(0, 5);
    
    if (topAssets.length > 0) {
      log(colors.cyan, `\n   📈 Top 5 Assets:`);
      topAssets.forEach(([name, data]) => {
        log(colors.cyan, `      ${name}: ${data.units?.toFixed(4)} units @ $${(data.totalValue || 0).toFixed(2)}`);
      });
    }
    
    // Verificar SPYG específicamente
    if (assetPerf['SPYG_etf']) {
      const spyg = assetPerf['SPYG_etf'];
      log(colors.yellow, `\n   🔍 SPYG Verificación:`);
      log(colors.yellow, `      Units en cálculo: ${spyg.units?.toFixed(4)}`);
      
      // Comparar con assets reales
      const realSpygUnits = allUserAssets
        .filter(a => a.name === 'SPYG' && a.isActive)
        .reduce((sum, a) => sum + a.units, 0);
      log(colors.yellow, `      Units en assets:  ${realSpygUnits.toFixed(4)}`);
      
      if (Math.abs(spyg.units - realSpygUnits) > 0.001) {
        log(colors.red, `      ⚠️ DIFERENCIA: ${(spyg.units - realSpygUnits).toFixed(4)}`);
      } else {
        log(colors.green, `      ✅ Unidades coinciden`);
      }
    }
    
    // Calcular por cuenta
    log(colors.cyan, `\n   📋 Por Cuenta:`);
    for (const account of accounts) {
      const accountAssets = allUserAssets.filter(a => a.portfolioAccount === account.id);
      const lastAccountTotalValue = cache.getAccountLastPerformance(account.id, currencies);
      const accountTransactions = userTransactions.filter(t => t.portfolioAccountId === account.id);
      
      const accountPerformance = calculateAccountPerformance(
        accountAssets,
        currentPrices,
        currencies,
        lastAccountTotalValue,
        accountTransactions
      );
      
      const accUsd = accountPerformance.USD || {};
      log(colors.cyan, `      ${account.name}: $${(accUsd.totalValue || 0).toFixed(2)} (${accountAssets.length} assets)`);
    }
    
    results.push({
      userId,
      date: formattedDate,
      overallPerformance,
      accounts: accounts.map(a => a.name)
    });
  }
  
  // 6. Comparar con documento existente
  log(colors.bright + colors.blue, `\n${'='.repeat(80)}`);
  log(colors.bright + colors.blue, '📋 COMPARACIÓN CON DOCUMENTO EXISTENTE');
  log(colors.bright + colors.blue, '='.repeat(80) + '\n');
  
  for (const result of results) {
    const existingDoc = await db.collection('portfolioPerformance')
      .doc(result.userId)
      .collection('dates')
      .doc(formattedDate)
      .get();
    
    if (existingDoc.exists) {
      const existing = existingDoc.data();
      const existingUsd = existing.USD || {};
      const calcUsd = result.overallPerformance.USD || {};
      
      log(colors.yellow, `👤 Usuario: ${result.userId}`);
      log(colors.cyan, `   Campo                     Existente       Calculado       Diferencia`);
      log(colors.cyan, `   ${'-'.repeat(70)}`);
      
      const fields = ['totalValue', 'totalInvestment', 'totalCashFlow', 'doneProfitAndLoss'];
      fields.forEach(field => {
        const existVal = existingUsd[field] || 0;
        const calcVal = calcUsd[field] || 0;
        const diff = calcVal - existVal;
        const diffColor = Math.abs(diff) > 0.01 ? colors.red : colors.green;
        const fieldPadded = field.padEnd(25);
        const existPadded = existVal.toFixed(2).padEnd(15);
        const calcPadded = calcVal.toFixed(2).padEnd(15);
        log(diffColor, `   ${fieldPadded} ${existPadded} ${calcPadded} ${diff.toFixed(2)}`);
      });
      
      // Comparar unidades de SPYG
      const existSpyg = existingUsd.assetPerformance?.['SPYG_etf']?.units || 0;
      const calcSpyg = calcUsd.assetPerformance?.['SPYG_etf']?.units || 0;
      if (existSpyg > 0 || calcSpyg > 0) {
        const diff = calcSpyg - existSpyg;
        const diffColor = Math.abs(diff) > 0.001 ? colors.red : colors.green;
        const fieldPadded = 'SPYG units'.padEnd(25);
        const existPadded = existSpyg.toFixed(4).padEnd(15);
        const calcPadded = calcSpyg.toFixed(4).padEnd(15);
        log(diffColor, `   ${fieldPadded} ${existPadded} ${calcPadded} ${diff.toFixed(4)}`);
      }
    } else {
      log(colors.yellow, `👤 Usuario: ${result.userId} - No existe documento para ${formattedDate}`);
    }
  }
  
  log(colors.bright + colors.blue, `\n${'='.repeat(80)}`);
  log(colors.bright + colors.green, '✅ DRY-RUN COMPLETADO - No se escribieron datos a Firestore');
  log(colors.bright + colors.blue, '='.repeat(80) + '\n');
  
  return results;
}

// Ejecutar
const args = process.argv.slice(2);
const userId = args[0] || null;
const date = args[1] || null;

dryRunCalculation(userId, date)
  .then(() => process.exit(0))
  .catch(err => {
    console.error('Error:', err);
    process.exit(1);
  });
