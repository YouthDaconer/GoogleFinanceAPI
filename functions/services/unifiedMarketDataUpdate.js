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
    
    console.log(`📡 Consultando ${allSymbols.length} símbolos en ${Math.ceil(allSymbols.length / batchSize)} lotes optimizados`);
    
    for (let i = 0; i < allSymbols.length; i += batchSize) {
      const symbolBatch = allSymbols.slice(i, i + batchSize);
      const symbolsParam = symbolBatch.join(',');
      
      const url = `${API_BASE_URL}/market-quotes?symbols=${symbolsParam}`;
      console.log(`🔄 Lote ${Math.floor(i/batchSize) + 1}: ${symbolBatch.length} símbolos`);
      
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
    
    console.log(`✅ Datos obtenidos: ${Object.keys(results.currencies).length} monedas, ${results.assets.size} activos`);
    return results;
  } catch (error) {
    console.error(`❌ Error al obtener datos de mercado en lote:`, error.message);
    return { currencies: {}, assets: new Map() };
  }
}

/**
 * Actualiza las tasas de cambio de monedas usando datos ya obtenidos
 */
async function updateCurrencyRates(db, currencyRates) {
  console.log('🔄 Actualizando tasas de cambio...');
  
  const currenciesRef = db.collection('currencies');
  const snapshot = await currenciesRef.where('isActive', '==', true).get();
  const batch = db.batch();
  let updatesCount = 0;

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
      console.log(`Actualizada tasa de cambio para USD:${code} a ${newRate}`);
    } else {
      console.warn(`Valor inválido para USD:${code}: ${newRate}`);
    }
  });

  if (updatesCount > 0) {
    await batch.commit();
    console.log(`✅ ${updatesCount} tasas de cambio actualizadas`);
  }
  
  return updatesCount;
}

/**
 * Actualiza los precios actuales de los activos usando datos ya obtenidos
 */
async function updateCurrentPrices(db, assetQuotes) {
  console.log('🔄 Actualizando precios actuales...');
  
  const currentPricesRef = db.collection('currentPrices');
  const snapshot = await currentPricesRef.get();
  const batch = db.batch();
  let updatesCount = 0;

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
      console.log(`Actualizado precio para ${symbol}: ${quote.regularMarketPrice} ${quote.currency}`);
    }
  });

  if (updatesCount > 0) {
    await batch.commit();
    console.log(`✅ ${updatesCount} precios actualizados`);
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
    console.log('📁 Precargando datos históricos (OPTIMIZACIÓN)...');
    
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

    console.log(`✅ Caché precargado: ${this.userLastPerformance.size} usuarios, ${this.accountLastPerformance.size} cuentas`);
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
  console.log('🔄 Calculando rendimiento diario del portafolio (OPTIMIZADO)...');
  
  const now = DateTime.now().setZone('America/New_York');
  const formattedDate = now.toISODate();
  let calculationsCount = 0;
  
  console.log(`📅 Fecha de cálculo (NY): ${formattedDate}`);
  
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
  
  console.log(`📊 Transacciones encontradas para ${formattedDate}: ${todaysTransactions.length} (${sellTransactions.length} ventas)`);
  
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
    
    console.log(`Obtenidos ${inactiveAssets.length} activos inactivos involucrados en ventas`);
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

  // Procesar cada usuario con datos precargados
  console.log(`👥 Procesando ${Object.keys(userPortfolios).length} usuarios con cuentas activas`);
  
  for (const [userId, accounts] of Object.entries(userPortfolios)) {
    console.log(`👤 Procesando usuario ${userId} con ${accounts.length} cuentas`);
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
        const sellAmountConverted = convertCurrency(
          sellTx.amount * sellTx.price,
          sellTx.currency,
          currency.code,
          currencies,
          sellTx.defaultCurrencyForAdquisitionDollar,
          parseFloat(sellTx.dollarPriceToDate.toString())
        );
        
        if (sellTx.assetId) {
          const asset = allAssets.find(a => a.id === sellTx.assetId);
          if (asset) {
            const assetKey = `${asset.name}_${asset.assetType}`;
            if (!assetDoneProfitAndLoss[assetKey]) {
              assetDoneProfitAndLoss[assetKey] = 0;
            }
            
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
              
              const profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
              
              assetDoneProfitAndLoss[assetKey] += profitAndLoss;
              totalDoneProfitAndLoss += profitAndLoss;
            }
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
          const sellAmountConverted = convertCurrency(
            sellTx.amount * sellTx.price,
            sellTx.currency,
            currency.code,
            currencies,
            sellTx.defaultCurrencyForAdquisitionDollar,
            parseFloat(sellTx.dollarPriceToDate.toString())
          );
          
          if (sellTx.assetId) {
            const asset = allAssets.find(a => a.id === sellTx.assetId);
            if (asset) {
              const assetKey = `${asset.name}_${asset.assetType}`;
              if (!accountAssetDoneProfitAndLoss[assetKey]) {
                accountAssetDoneProfitAndLoss[assetKey] = 0;
              }
              
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
                
                const profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
                
                accountAssetDoneProfitAndLoss[assetKey] += profitAndLoss;
                accountDoneProfitAndLoss += profitAndLoss;
              }
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
        console.log(`📦 Batch de ${batchCount} operaciones completado y guardado en Firestore`);
        batch = db.batch();
        batchCount = 0;
      }
    }

    calculationsCount++;
  }

  // ✨ Commit final del batch
  if (batchCount > 0) {
    await batch.commit();
    console.log(`📦 Batch final de ${batchCount} operaciones completado y guardado en Firestore`);
  } else {
    console.log(`ℹ️ No hay operaciones pendientes para guardar`);
  }

  console.log(`✅ Rendimiento calculado para ${calculationsCount} usuarios (OPTIMIZADO)`);
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
    console.log('🔴 El mercado NYSE está cerrado. Omitiendo actualizaciones.');
    return null;
  }

  console.log('🚀 Iniciando actualización unificada de datos de mercado...');
  
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
    
    console.log(`📊 Obteniendo datos para ${currencyCodes.length} monedas y ${assetSymbols.length} activos`);
    
    // Paso 2: Obtener TODOS los datos de mercado en llamadas optimizadas
    const marketData = await getAllMarketDataBatch(currencyCodes, assetSymbols);
    
    // Paso 3: Actualizar tasas de cambio con datos ya obtenidos
    const currencyUpdates = await updateCurrencyRates(db, marketData.currencies);
    
    // Paso 4: Actualizar precios actuales con datos ya obtenidos
    const priceUpdates = await updateCurrentPrices(db, marketData.assets);
    
    // Paso 5: Calcular rendimiento del portafolio
    const portfolioCalculations = await calculateDailyPortfolioPerformance(db);
    
    // Paso 6: Calcular riesgo del portafolio (usando datos actualizados)
    console.log('🔄 Calculando riesgo del portafolio...');
    await calculatePortfolioRisk();
    console.log('✅ Riesgo del portafolio calculado');
    
    const endTime = Date.now();
    const executionTime = (endTime - startTime) / 1000;
    
    console.log(`🎉 Actualización unificada completada en ${executionTime}s:`);
    console.log(`   - ${currencyUpdates} tasas de cambio actualizadas`);
    console.log(`   - ${priceUpdates} precios actualizados`);
    console.log(`   - ${portfolioCalculations} portafolios calculados`);
    console.log(`   - ✅ Riesgo de portafolios calculado`);
    
    return null;
  } catch (error) {
    console.error('❌ Error en actualización unificada:', error);
    return null;
  }
}); 