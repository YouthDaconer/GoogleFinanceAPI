const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require('firebase-admin');
const axios = require('axios');
const { calculateAccountPerformance, convertCurrency } = require('../utils/portfolioCalculations');
const { calculatePortfolioRisk } = require('./calculatePortfolioRisk');
const { DateTime } = require('luxon');

const API_BASE_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

// Horarios estáticos para NYSE (en UTC)
const NYSE_OPEN_HOUR = 13.5;  // 9:30 AM EST
const NYSE_CLOSE_HOUR = 20;   // 4:00 PM EST

// 🚀 OPTIMIZACIÓN: Control de logging para reducir costos
const LOG_LEVEL = process.env.LOG_LEVEL || 'INFO'; // 'DEBUG', 'INFO', 'WARN', 'ERROR'
const ENABLE_DETAILED_LOGS = process.env.ENABLE_DETAILED_LOGS === 'true';

function logDebug(...args) {
  if (LOG_LEVEL === 'DEBUG') console.log(...args);
}

function logInfo(...args) {
  if (['DEBUG', 'INFO'].includes(LOG_LEVEL)) console.log(...args);
}

function logWarn(...args) {
  if (['DEBUG', 'INFO', 'WARN'].includes(LOG_LEVEL)) console.warn(...args);
}

function logError(...args) {
  console.error(...args); // Siempre loguear errores
}

function isNYSEMarketOpen() {
  const now = new Date();
  const utcHour = now.getUTCHours() + now.getUTCMinutes() / 60;
  return utcHour >= NYSE_OPEN_HOUR && utcHour < NYSE_CLOSE_HOUR;
}

/**
 * 🚀 OPTIMIZACIÓN: Función unificada que obtiene todos los datos de mercado en una sola llamada
 * Combina monedas y símbolos de activos para minimizar llamadas a la API Lambda
 */
async function getAllMarketDataBatch(currencyCodes, assetSymbols) {
  try {
    // Preparar símbolos de monedas (agregar %3DX para codificación URL)
    const currencySymbols = currencyCodes.map(code => `${code}%3DX`);
    
    // Combinar todos los símbolos en una sola consulta
    const allSymbols = [...currencySymbols, ...assetSymbols];
    
    // Dividir en lotes más grandes (100 símbolos por llamada para optimizar)
    const batchSize = 100;
    const results = {
      currencies: {},
      assets: new Map()
    };
    
    logInfo(`📡 Consultando ${allSymbols.length} símbolos en ${Math.ceil(allSymbols.length / batchSize)} lotes optimizados`);
    
    for (let i = 0; i < allSymbols.length; i += batchSize) {
      const symbolBatch = allSymbols.slice(i, i + batchSize);
      const symbolsParam = symbolBatch.join(',');
      
      const url = `${API_BASE_URL}/market-quotes?symbols=${symbolsParam}`;
      logDebug(`🔄 Lote ${Math.floor(i/batchSize) + 1}: ${symbolBatch.length} símbolos`);
      
      const { data } = await axios.get(url);
      
      if (Array.isArray(data)) {
        data.forEach(item => {
          if (item.symbol && item.regularMarketPrice) {
            // Si es una moneda (termina en =X en la respuesta)
            if (item.symbol.includes('=X')) {
              const currencyCode = item.symbol.replace('=X', '');
              if (currencyCodes.includes(currencyCode)) {
                results.currencies[currencyCode] = item.regularMarketPrice;
              }
            } 
            // Si es un activo normal
            else if (assetSymbols.includes(item.symbol)) {
              results.assets.set(item.symbol, item);
            }
          }
        });
      }
    }
    
    logInfo(`✅ Datos obtenidos: ${Object.keys(results.currencies).length} monedas, ${results.assets.size} activos`);
    return results;
  } catch (error) {
    logError(`❌ Error al obtener datos de mercado en lote:`, error.message);
    return { currencies: {}, assets: new Map() };
  }
}

/**
 * Actualiza las tasas de cambio de monedas usando datos ya obtenidos
 */
async function updateCurrencyRates(db, currencyRates) {
  logDebug('🔄 Actualizando tasas de cambio...');
  
  const currenciesRef = db.collection('currencies');
  const snapshot = await currenciesRef.where('isActive', '==', true).get();
  const batch = db.batch();
  let updatesCount = 0;
  let invalidCount = 0;

  const activeCurrencies = snapshot.docs.map(doc => ({
    code: doc.data().code,
    ref: doc.ref,
    data: doc.data()
  }));
  
  activeCurrencies.forEach(currency => {
    const { code, ref, data } = currency;
    const newRate = currencyRates[code];
    
    if (newRate && !isNaN(newRate) && newRate > 0) {
      const updatedData = {
        code: code,
        name: data.name,
        symbol: data.symbol,
        exchangeRate: newRate,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
      };

      batch.update(ref, updatedData);
      updatesCount++;
      
      // 🚀 OPTIMIZACIÓN: Solo log detallado si está habilitado
      if (ENABLE_DETAILED_LOGS) {
        logDebug(`Actualizada tasa de cambio para USD:${code} a ${newRate}`);
      }
    } else {
      invalidCount++;
      logWarn(`Valor inválido para USD:${code}: ${newRate}`);
    }
  });

  if (updatesCount > 0) {
    await batch.commit();
    logInfo(`✅ ${updatesCount} tasas de cambio actualizadas${invalidCount > 0 ? ` (${invalidCount} inválidas)` : ''}`);
  }
  
  return updatesCount;
}

/**
 * Actualiza los precios actuales de los activos usando datos ya obtenidos
 */
async function updateCurrentPrices(db, assetQuotes) {
  logDebug('🔄 Actualizando precios actuales...');
  
  const currentPricesRef = db.collection('currentPrices');
  const snapshot = await currentPricesRef.get();
  const batch = db.batch();
  let updatesCount = 0;
  let failedUpdates = 0;

  snapshot.docs.forEach(doc => {
    const docData = doc.data();
    const symbol = docData.symbol;
    const quote = assetQuotes.get(symbol);
    
    if (quote && quote.regularMarketPrice) {
      const updatedData = {
        symbol: symbol,
        price: quote.regularMarketPrice,
        lastUpdated: Date.now(),
        change: quote.regularMarketChange,
        percentChange: quote.regularMarketChangePercent,
        previousClose: quote.regularMarketPreviousClose,
        currency: quote.currency,
        marketState: quote.marketState,
        quoteType: quote.quoteType,
        exchange: quote.exchange,
        fullExchangeName: quote.fullExchangeName
      };
      
      // Mantener campos existentes
      if (docData.name) updatedData.name = docData.name;
      if (docData.isin) updatedData.isin = docData.isin;
      if (docData.type) updatedData.type = docData.type;
      
      batch.update(doc.ref, updatedData);
      updatesCount++;
      
      // 🚀 OPTIMIZACIÓN: Solo log detallado si está habilitado
      if (ENABLE_DETAILED_LOGS) {
        logDebug(`Actualizado precio para ${symbol}: ${quote.regularMarketPrice} ${quote.currency}`);
      }
    } else {
      failedUpdates++;
    }
  });

  if (updatesCount > 0) {
    await batch.commit();
    logInfo(`✅ ${updatesCount} precios actualizados${failedUpdates > 0 ? ` (${failedUpdates} fallidos)` : ''}`);
  }
  
  return updatesCount;
}

/**
 * 🚀 OPTIMIZACIÓN: Sistema de caché para datos históricos
 */
class PerformanceDataCache {
  constructor() {
    this.userLastPerformance = new Map();
    this.accountLastPerformance = new Map();
    this.buyTransactionsByAsset = new Map();
  }

  async preloadHistoricalData(db, formattedDate, userPortfolios, sellTransactions) {
    logDebug('📁 Precargando datos históricos (OPTIMIZACIÓN)...');
    
    const allUserIds = Object.keys(userPortfolios);
    const allAccountIds = Object.values(userPortfolios).flat().map(acc => acc.id);
    const assetIdsWithSells = [...new Set(sellTransactions.map(t => t.assetId).filter(id => id))];

    // ✨ OPTIMIZACIÓN: Consultas paralelas masivas
    const [userPerformanceResults, accountPerformanceResults, buyTransactionsResults] = await Promise.all([
      Promise.all(allUserIds.map(async (userId) => {
        const userRef = db.collection('portfolioPerformance').doc(userId);
        const query = await userRef.collection('dates')
          .where('date', '<', formattedDate)
          .orderBy('date', 'desc')
          .limit(1)
          .get();
        return { userId, data: query.empty ? null : query.docs[0].data() };
      })),
      
      Promise.all(allAccountIds.map(async (accountId) => {
        const userId = Object.keys(userPortfolios).find(uid => 
          userPortfolios[uid].some(acc => acc.id === accountId)
        );
        const accountRef = db.collection('portfolioPerformance')
          .doc(userId)
          .collection('accounts')
          .doc(accountId);
        const query = await accountRef.collection('dates')
          .where('date', '<', formattedDate)
          .orderBy('date', 'desc')
          .limit(1)
          .get();
        return { accountId, data: query.empty ? null : query.docs[0].data() };
      })),
      
      assetIdsWithSells.length > 0 ? 
        db.collection('transactions')
          .where('type', '==', 'buy')
          .where('assetId', 'in', assetIdsWithSells)
          .get() : 
        { docs: [] }
    ]);

    // Procesar resultados
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

    logInfo(`✅ Caché precargado: ${this.userLastPerformance.size} usuarios, ${this.accountLastPerformance.size} cuentas`);
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
              units: assetData.units || 0
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
              units: assetData.units || 0
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
 * 🚀 OPTIMIZACIÓN: Calcula el rendimiento diario del portafolio con caché
 */
async function calculateDailyPortfolioPerformance(db) {
  logInfo('🔄 Calculando rendimiento diario del portafolio (OPTIMIZADO)...');
  
  const now = DateTime.now().setZone('America/New_York');
  const formattedDate = now.toISODate();
  let calculationsCount = 0;
  
  logDebug(`📅 Fecha de cálculo (NY): ${formattedDate}`);
  
  // ✨ OPTIMIZACIÓN: Todas las consultas iniciales en paralelo
  const [
    transactionsSnapshot,
    activeAssetsSnapshot,
    currenciesSnapshot,
    portfolioAccountsSnapshot,
    currentPricesSnapshot
  ] = await Promise.all([
    db.collection('transactions').where('date', '==', formattedDate).get(),
    db.collection('assets').where('isActive', '==', true).get(),
    db.collection('currencies').where('isActive', '==', true).get(),
    db.collection('portfolioAccounts').where('isActive', '==', true).get(),
    db.collection('currentPrices').get()
  ]);
  
  const todaysTransactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const sellTransactions = todaysTransactions.filter(t => t.type === 'sell');
  const assetIdsInSellTransactions = [...new Set(sellTransactions.map(t => t.assetId).filter(id => id))];
  
  // 🚀 OPTIMIZACIÓN: Log consolidado de transacciones
  logInfo(`📊 Transacciones para ${formattedDate}: ${todaysTransactions.length} total (${sellTransactions.length} ventas)`);
  
  const activeAssets = activeAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const currentPrices = currentPricesSnapshot.docs.map(doc => ({ symbol: doc.id.split(':')[0], ...doc.data() }));
  
  // Obtener activos inactivos involucrados en ventas
  let inactiveAssets = [];
  if (assetIdsInSellTransactions.length > 0) {
    const inactiveAssetsSnapshot = await db.collection('assets')
      .where('isActive', '==', false)
      .get();
    
    inactiveAssets = inactiveAssetsSnapshot.docs
      .map(doc => ({ id: doc.id, ...doc.data() }))
      .filter(asset => assetIdsInSellTransactions.includes(asset.id));
    
    logDebug(`Obtenidos ${inactiveAssets.length} activos inactivos involucrados en ventas`);
  }
  
  const allAssets = [...activeAssets, ...inactiveAssets];
  
  // ✨ OPTIMIZACIÓN: Las transacciones de compra ya están en el caché
  
  // Agrupar transacciones de venta por cuenta
  const sellTransactionsByAccount = sellTransactions.reduce((acc, transaction) => {
    if (!acc[transaction.portfolioAccountId]) {
      acc[transaction.portfolioAccountId] = [];
    }
    acc[transaction.portfolioAccountId].push(transaction);
    return acc;
  }, {});

  // Identificar activos inactivos con ventas
  const inactiveAssetsWithSellTransactions = new Set(
    inactiveAssets.filter(asset => 
      sellTransactions.some(tx => tx.assetId === asset.id)
    ).map(asset => asset.id)
  );
  
  const assetsToInclude = [
    ...activeAssets,
    ...inactiveAssets.filter(asset => inactiveAssetsWithSellTransactions.has(asset.id))
  ];

  const userPortfolios = portfolioAccounts.reduce((acc, account) => {
    if (!acc[account.userId]) acc[account.userId] = [];
    acc[account.userId].push(account);
    return acc;
  }, {});

  // ✨ OPTIMIZACIÓN: Sistema de caché para datos históricos
  const cache = new PerformanceDataCache();
  await cache.preloadHistoricalData(db, formattedDate, userPortfolios, sellTransactions);

  // ✨ OPTIMIZACIÓN: Batch único para todas las operaciones
  const BATCH_SIZE = 450;
  let batch = db.batch();
  let batchCount = 0;
  let totalBatchesCommitted = 0;

  // 🚀 OPTIMIZACIÓN: Log consolidado de procesamiento
  const userCount = Object.keys(userPortfolios).length;
  const totalAccounts = Object.values(userPortfolios).flat().length;
  logInfo(`👥 Procesando ${userCount} usuarios con ${totalAccounts} cuentas activas`);
  
  for (const [userId, accounts] of Object.entries(userPortfolios)) {
    logDebug(`👤 Procesando usuario ${userId} con ${accounts.length} cuentas`);
    // ✨ OPTIMIZACIÓN: Usar datos del caché en lugar de consultas individuales
    const lastOverallTotalValue = cache.getUserLastPerformance(userId, currencies);

    const allUserAssets = assetsToInclude.filter(asset => 
      accounts.some(account => account.id === asset.portfolioAccount)
    );
    
    const userTransactions = todaysTransactions.filter(t => 
      accounts.some(account => account.id === t.portfolioAccountId)
    );

    const overallPerformance = calculateAccountPerformance(
      allUserAssets,
      currentPrices,
      currencies,
      lastOverallTotalValue,
      userTransactions
    );

    // Calcular doneProfitAndLoss para cada moneda
    const userDoneProfitAndLossByCurrency = {};
    const userSellTransactions = sellTransactions.filter(t => 
      accounts.some(account => account.id === t.portfolioAccountId)
    );
    
    for (const currency of currencies) {
      let totalDoneProfitAndLoss = 0;
      const assetDoneProfitAndLoss = {};
      
      userSellTransactions.forEach(sellTx => {
        if (sellTx.assetId) {
          const asset = allAssets.find(a => a.id === sellTx.assetId);
          if (asset) {
            const assetKey = `${asset.name}_${asset.assetType}`;
            if (!assetDoneProfitAndLoss[assetKey]) {
              assetDoneProfitAndLoss[assetKey] = 0;
            }
            
            let profitAndLoss = 0;
            
            // ✨ OPTIMIZACIÓN: Usar valuePnL si está disponible
            if (sellTx.valuePnL !== undefined && sellTx.valuePnL !== null) {
              // Usar PnL precalculada y convertir a moneda objetivo
              profitAndLoss = convertCurrency(
                sellTx.valuePnL,
                sellTx.currency,
                currency.code,
                currencies,
                sellTx.defaultCurrencyForAdquisitionDollar,
                parseFloat(sellTx.dollarPriceToDate.toString())
              );
            } else {
              // Fallback: calcular PnL manualmente (método anterior)
              const sellAmountConverted = convertCurrency(
                sellTx.amount * sellTx.price,
                sellTx.currency,
                currency.code,
                currencies,
                sellTx.defaultCurrencyForAdquisitionDollar,
                parseFloat(sellTx.dollarPriceToDate.toString())
              );
              
              const buyTxsForAsset = cache.getBuyTransactionsForAsset(sellTx.assetId);
              
              if (buyTxsForAsset.length > 0) {
                let totalBuyCost = 0;
                let totalBuyUnits = 0;
                
                buyTxsForAsset.forEach(buyTx => {
                  totalBuyCost += buyTx.amount * buyTx.price;
                  totalBuyUnits += buyTx.amount;
                });
                
                const avgCostPerUnit = totalBuyCost / totalBuyUnits;
                const costOfSoldUnits = sellTx.amount * avgCostPerUnit;
                
                const costOfSoldUnitsConverted = convertCurrency(
                  costOfSoldUnits,
                  sellTx.currency,
                  currency.code,
                  currencies,
                  sellTx.defaultCurrencyForAdquisitionDollar,
                  parseFloat(sellTx.dollarPriceToDate.toString())
                );
                
                profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
              }
            }
            
            assetDoneProfitAndLoss[assetKey] += profitAndLoss;
            totalDoneProfitAndLoss += profitAndLoss;
          }
        }
      });
      
      userDoneProfitAndLossByCurrency[currency.code] = { 
        doneProfitAndLoss: totalDoneProfitAndLoss,
        assetDoneProfitAndLoss
      };
    }

    // Agregar doneProfitAndLoss a overallPerformance
    for (const [currencyCode, data] of Object.entries(userDoneProfitAndLossByCurrency)) {
      if (overallPerformance[currencyCode]) {
        overallPerformance[currencyCode].doneProfitAndLoss = data.doneProfitAndLoss;
        
        const unrealizedProfitAndLoss = overallPerformance[currencyCode].totalValue - overallPerformance[currencyCode].totalInvestment;
        overallPerformance[currencyCode].unrealizedProfitAndLoss = unrealizedProfitAndLoss;
        
        if (overallPerformance[currencyCode].assetPerformance) {
          for (const [assetKey, pnl] of Object.entries(data.assetDoneProfitAndLoss)) {
            if (overallPerformance[currencyCode].assetPerformance[assetKey]) {
              overallPerformance[currencyCode].assetPerformance[assetKey].doneProfitAndLoss = pnl;
            }
          }
          
          for (const [assetKey, assetData] of Object.entries(overallPerformance[currencyCode].assetPerformance)) {
            const assetTotalValue = assetData.totalValue || 0;
            const assetTotalInvestment = assetData.totalInvestment || 0;
            const assetUnrealizedPnL = assetTotalValue - assetTotalInvestment;
            
            overallPerformance[currencyCode].assetPerformance[assetKey].unrealizedProfitAndLoss = assetUnrealizedPnL;
          }
        }
      }
    }

    // ✨ OPTIMIZACIÓN: Asegurar documento de usuario (idempotente)
    const userPerformanceRef = db.collection('portfolioPerformance').doc(userId);
    batch.set(userPerformanceRef, { userId }, { merge: true });
    batchCount++;

    // Guardar rendimiento general del usuario
    const userOverallPerformanceRef = userPerformanceRef.collection('dates').doc(formattedDate);
    batch.set(userOverallPerformanceRef, {
      date: formattedDate,
      ...overallPerformance
    });
    batchCount++;

    // Procesar cada cuenta del usuario
    for (const account of accounts) {
      const accountSellTransactions = sellTransactionsByAccount[account.id] || [];
      const inactiveAccountAssetsWithSells = inactiveAssets.filter(asset => 
        asset.portfolioAccount === account.id && 
        accountSellTransactions.some(t => t.assetId === asset.id)
      );
      
      const accountAssets = [
        ...activeAssets.filter(asset => asset.portfolioAccount === account.id),
        ...inactiveAccountAssetsWithSells
      ];

      // ✨ OPTIMIZACIÓN: Usar datos del caché para la cuenta
      const lastAccountTotalValue = cache.getAccountLastPerformance(account.id, currencies);

      const accountTransactions = userTransactions.filter(t => t.portfolioAccountId === account.id);
      const accountPerformance = calculateAccountPerformance(
        accountAssets,
        currentPrices,
        currencies,
        lastAccountTotalValue,
        accountTransactions
      );

      // Calcular doneProfitAndLoss para la cuenta (similar al usuario)
      const accountDoneProfitAndLossByCurrency = {};
      
      for (const currency of currencies) {
        let accountDoneProfitAndLoss = 0;
        const accountAssetDoneProfitAndLoss = {};
        
        accountSellTransactions.forEach(sellTx => {
          if (sellTx.assetId) {
            const asset = allAssets.find(a => a.id === sellTx.assetId);
            if (asset) {
              const assetKey = `${asset.name}_${asset.assetType}`;
              if (!accountAssetDoneProfitAndLoss[assetKey]) {
                accountAssetDoneProfitAndLoss[assetKey] = 0;
              }
              
              let profitAndLoss = 0;
              
              // ✨ OPTIMIZACIÓN: Usar valuePnL si está disponible
              if (sellTx.valuePnL !== undefined && sellTx.valuePnL !== null) {
                // Usar PnL precalculada y convertir a moneda objetivo
                profitAndLoss = convertCurrency(
                  sellTx.valuePnL,
                  sellTx.currency,
                  currency.code,
                  currencies,
                  sellTx.defaultCurrencyForAdquisitionDollar,
                  parseFloat(sellTx.dollarPriceToDate.toString())
                );
              } else {
                // Fallback: calcular PnL manualmente (método anterior)
                const sellAmountConverted = convertCurrency(
                  sellTx.amount * sellTx.price,
                  sellTx.currency,
                  currency.code,
                  currencies,
                  sellTx.defaultCurrencyForAdquisitionDollar,
                  parseFloat(sellTx.dollarPriceToDate.toString())
                );
                
                const buyTxsForAsset = cache.getBuyTransactionsForAsset(sellTx.assetId);
                
                if (buyTxsForAsset.length > 0) {
                  let totalBuyCost = 0;
                  let totalBuyUnits = 0;
                  
                  buyTxsForAsset.forEach(buyTx => {
                    totalBuyCost += buyTx.amount * buyTx.price;
                    totalBuyUnits += buyTx.amount;
                  });
                  
                  const avgCostPerUnit = totalBuyCost / totalBuyUnits;
                  const costOfSoldUnits = sellTx.amount * avgCostPerUnit;
                  
                  const costOfSoldUnitsConverted = convertCurrency(
                    costOfSoldUnits,
                    sellTx.currency,
                    currency.code,
                    currencies,
                    sellTx.defaultCurrencyForAdquisitionDollar,
                    parseFloat(sellTx.dollarPriceToDate.toString())
                  );
                  
                  profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
                }
              }
              
              accountAssetDoneProfitAndLoss[assetKey] += profitAndLoss;
              accountDoneProfitAndLoss += profitAndLoss;
            }
          }
        });
        
        accountDoneProfitAndLossByCurrency[currency.code] = { 
          doneProfitAndLoss: accountDoneProfitAndLoss,
          assetDoneProfitAndLoss: accountAssetDoneProfitAndLoss
        };
      }

      // Agregar doneProfitAndLoss a accountPerformance
      for (const [currencyCode, data] of Object.entries(accountDoneProfitAndLossByCurrency)) {
        if (accountPerformance[currencyCode]) {
          accountPerformance[currencyCode].doneProfitAndLoss = data.doneProfitAndLoss;
          
          const accountUnrealizedPnL = accountPerformance[currencyCode].totalValue - accountPerformance[currencyCode].totalInvestment;
          accountPerformance[currencyCode].unrealizedProfitAndLoss = accountUnrealizedPnL;
          
          if (accountPerformance[currencyCode].assetPerformance) {
            for (const [assetKey, pnl] of Object.entries(data.assetDoneProfitAndLoss)) {
              if (accountPerformance[currencyCode].assetPerformance[assetKey]) {
                accountPerformance[currencyCode].assetPerformance[assetKey].doneProfitAndLoss = pnl;
              }
            }
            
            for (const [assetKey, assetData] of Object.entries(accountPerformance[currencyCode].assetPerformance)) {
              const accountAssetTotalValue = assetData.totalValue || 0;
              const accountAssetTotalInvestment = assetData.totalInvestment || 0;
              const accountAssetUnrealizedPnL = accountAssetTotalValue - accountAssetTotalInvestment;
              
              accountPerformance[currencyCode].assetPerformance[assetKey].unrealizedProfitAndLoss = accountAssetUnrealizedPnL;
            }
          }
        }
      }

      // ✨ OPTIMIZACIÓN: Asegurar documento de cuenta (idempotente)
      const accountRef = userPerformanceRef.collection('accounts').doc(account.id);
      batch.set(accountRef, { accountId: account.id }, { merge: true });
      batchCount++;

      // Guardar rendimiento de la cuenta
      const accountPerformanceRef = accountRef.collection('dates').doc(formattedDate);
      batch.set(accountPerformanceRef, {
        date: formattedDate,
        ...accountPerformance
      });
      batchCount++;

      // ✨ Commit batch si se acerca al límite
      if (batchCount >= BATCH_SIZE) {
        await batch.commit();
        totalBatchesCommitted++;
        logDebug(`📦 Batch ${totalBatchesCommitted} de ${batchCount} operaciones completado`);
        batch = db.batch();
        batchCount = 0;
      }
    }

    calculationsCount++;
  }

  // ✨ Commit final del batch
  if (batchCount > 0) {
    await batch.commit();
    totalBatchesCommitted++;
    logDebug(`📦 Batch final ${totalBatchesCommitted} de ${batchCount} operaciones completado`);
  }

  logInfo(`✅ Rendimiento calculado para ${calculationsCount} usuarios (${totalBatchesCommitted} batches)`);
  return calculationsCount;
}

/**
 * Función principal unificada que ejecuta todas las actualizaciones
 */
exports.unifiedMarketDataUpdate = onSchedule({
  schedule: '*/2 9-17 * * 1-5',
  timeZone: 'America/New_York',
  retryCount: 3,
}, async (event) => {
  if (!isNYSEMarketOpen()) {
    logInfo('🔴 El mercado NYSE está cerrado. Omitiendo actualizaciones.');
    return null;
  }

  logInfo('🚀 Iniciando actualización unificada de datos de mercado...');
  
  const db = admin.firestore();
  const startTime = Date.now();
  
      try {
    // Paso 1: Obtener códigos de monedas y símbolos de activos dinámicamente
    const [currenciesSnapshot, currentPricesSnapshot] = await Promise.all([
      db.collection('currencies').where('isActive', '==', true).get(),
      db.collection('currentPrices').get()
    ]);
    
    const currencyCodes = currenciesSnapshot.docs.map(doc => doc.data().code);
    const assetSymbols = currentPricesSnapshot.docs.map(doc => doc.data().symbol);
    
    logInfo(`📊 Obteniendo datos para ${currencyCodes.length} monedas y ${assetSymbols.length} activos`);
    
    // Paso 2: Obtener TODOS los datos de mercado en llamadas optimizadas
    const marketData = await getAllMarketDataBatch(currencyCodes, assetSymbols);
    
    // Paso 3: Actualizar tasas de cambio con datos ya obtenidos
    const currencyUpdates = await updateCurrencyRates(db, marketData.currencies);
    
    // Paso 4: Actualizar precios actuales con datos ya obtenidos
    const priceUpdates = await updateCurrentPrices(db, marketData.assets);
    
    // Paso 5: Calcular rendimiento del portafolio
    const portfolioCalculations = await calculateDailyPortfolioPerformance(db);
    
    // Paso 6: Calcular riesgo del portafolio (usando datos actualizados)
    logDebug('🔄 Calculando riesgo del portafolio...');
    await calculatePortfolioRisk();
    logDebug('✅ Riesgo del portafolio calculado');
    
    const endTime = Date.now();
    const executionTime = (endTime - startTime) / 1000;
    
    logInfo(`🎉 Actualización unificada completada en ${executionTime}s:`);
    logInfo(`   - ${currencyUpdates} tasas de cambio actualizadas`);
    logInfo(`   - ${priceUpdates} precios actualizados`);
    logInfo(`   - ${portfolioCalculations} portafolios calculados`);
    logInfo(`   - ✅ Riesgo de portafolios calculado`);
    
    return null;
  } catch (error) {
    logError('❌ Error en actualización unificada:', error);
    return null;
  }
}); 