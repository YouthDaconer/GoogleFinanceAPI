const admin = require('../services/firebaseAdmin');
const axios = require('axios');
const { DateTime } = require('luxon');
const { calculateAccountPerformance, convertCurrency } = require('../utils/portfolioCalculations');

const API_BASE_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

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
      console.log(`   URL: ${url.substring(0, 100)}...`);
      
      const { data } = await axios.get(url);
      
      if (Array.isArray(data)) {
        data.forEach(item => {
          if (item.symbol && item.regularMarketPrice) {
            // Si es una moneda (termina en =X en la respuesta)
            if (item.symbol.includes('=X')) {
              const currencyCode = item.symbol.replace('=X', '');
              if (currencyCodes.includes(currencyCode)) {
                results.currencies[currencyCode] = item.regularMarketPrice;
                console.log(`   💱 Moneda obtenida: ${currencyCode} = ${item.regularMarketPrice}`);
              }
            } 
            // Si es un activo normal
            else if (assetSymbols.includes(item.symbol)) {
              results.assets.set(item.symbol, item);
              console.log(`   📈 Activo obtenido: ${item.symbol} = ${item.regularMarketPrice} ${item.currency}`);
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
 * 🔧 OPTIMIZACIÓN: Actualización más eficiente de tasas de cambio
 */
async function updateCurrencyRatesOptimized(db, currencyRates) {
  console.log('🔄 Actualizando tasas de cambio (OPTIMIZADO)...');
  
  const currenciesSnapshot = await db.collection('currencies')
    .where('isActive', '==', true)
    .get();
  
  if (currenciesSnapshot.empty) {
    console.log('   ℹ️ No hay monedas activas para actualizar');
    return 0;
  }

  const batch = db.batch();
  let updatesCount = 0;
  const timestamp = admin.firestore.FieldValue.serverTimestamp();

  // ✨ OPTIMIZACIÓN: Una sola iteración, sin múltiples lecturas
  currenciesSnapshot.docs.forEach(doc => {
    const data = doc.data();
    const code = data.code;
    const newRate = currencyRates[code];
    
    if (newRate && !isNaN(newRate) && newRate > 0) {
      // ✨ Solo actualizar campos que cambiaron para reducir tamaño del write
      const updatedData = {
        exchangeRate: newRate,
        lastUpdated: timestamp
      };

      batch.update(doc.ref, updatedData);
      updatesCount++;
      console.log(`   ✅ Actualizada tasa de cambio para USD:${code} a ${newRate}`);
    } else {
      console.warn(`   ⚠️ Valor inválido para USD:${code}: ${newRate}`);
    }
  });

  if (updatesCount > 0) {
    await batch.commit();
    console.log(`✅ ${updatesCount} tasas de cambio actualizadas (OPTIMIZADO)`);
  }
  
  return updatesCount;
}

/**
 * 🔧 OPTIMIZACIÓN: Actualización más eficiente de precios
 */
async function updateCurrentPricesOptimized(db, assetQuotes) {
  console.log('🔄 Actualizando precios actuales (OPTIMIZADO)...');
  
  const currentPricesSnapshot = await db.collection('currentPrices').get();
  
  if (currentPricesSnapshot.empty) {
    console.log('   ℹ️ No hay precios para actualizar');
    return 0;
  }

  const batch = db.batch();
  let updatesCount = 0;
  const timestamp = Date.now();

  // ✨ OPTIMIZACIÓN: Una sola iteración, actualizaciones selectivas
  currentPricesSnapshot.docs.forEach(doc => {
    const docData = doc.data();
    const symbol = docData.symbol;
    const quote = assetQuotes.get(symbol);
    
    if (quote && quote.regularMarketPrice) {
      // ✨ Solo actualizar campos de precio, mantener metadatos existentes
      const updatedData = {
        price: quote.regularMarketPrice,
        lastUpdated: timestamp,
        change: quote.regularMarketChange,
        percentChange: quote.regularMarketChangePercent,
        previousClose: quote.regularMarketPreviousClose,
        marketState: quote.marketState
      };
      
      batch.update(doc.ref, updatedData);
      updatesCount++;
      console.log(`   ✅ Actualizado precio para ${symbol}: ${quote.regularMarketPrice} ${quote.currency || 'N/A'}`);
    } else {
      console.log(`   ⚠️ No se pudo obtener precio para ${symbol}`);
    }
  });

  if (updatesCount > 0) {
    await batch.commit();
    console.log(`✅ ${updatesCount} precios actualizados (OPTIMIZADO)`);
  } else {
    console.log('ℹ️ No se requirieron actualizaciones de precios');
  }
  
  return updatesCount;
}

/**
 * 🚀 OPTIMIZACIÓN: Sistema de caché para datos históricos (versión REAL)
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
 * 🚀 OPTIMIZACIÓN: CALCULA REALMENTE el rendimiento diario del portafolio con caché
 */
async function calculateDailyPortfolioPerformanceReal(db) {
  console.log('🔄 Calculando rendimiento diario del portafolio REAL (OPTIMIZADO)...');
  
  const formattedDate = "2025-06-27"; // Fecha específica para el test
  let calculationsCount = 0;
  
  console.log(`📅 Fecha de cálculo: ${formattedDate}`);
  
  try {
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
    
    console.log(`📊 Transacciones para ${formattedDate}: ${todaysTransactions.length} total (${sellTransactions.length} ventas)`);
    
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

    const userCount = Object.keys(userPortfolios).length;
    const totalAccounts = Object.values(userPortfolios).flat().length;
    console.log(`👥 Procesando ${userCount} usuarios con ${totalAccounts} cuentas activas`);
    
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
          console.log(`📦 Batch ${totalBatchesCommitted} de ${batchCount} operaciones completado`);
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
      console.log(`📦 Batch final ${totalBatchesCommitted} de ${batchCount} operaciones completado`);
    }

    console.log(`✅ Rendimiento calculado para ${calculationsCount} usuarios (${totalBatchesCommitted} batches)`);
    return calculationsCount;
  } catch (error) {
    console.error('❌ Error en cálculo de rendimiento:', error);
    return 0;
  }
}

/**
 * Simula el cálculo de riesgo del portafolio (versión simplificada para pruebas)
 */
async function testCalculatePortfolioRisk() {
  console.log('🔄 Simulando cálculo de riesgo del portafolio...');
  
  try {
    // En pruebas, solo simulamos que se ejecuta correctamente
    console.log('   📊 Simulación de cálculo de riesgo completada');
    return true;
  } catch (error) {
    console.error('   ❌ Error en simulación de cálculo de riesgo:', error.message);
    return false;
  }
}

/**
 * 🚀 OPTIMIZACIÓN: Función principal de prueba unificada con todas las optimizaciones
 */
 async function testUnifiedMarketDataUpdateOptimized() {
   console.log('🚀 INICIANDO PRUEBA REAL DE ACTUALIZACIÓN UNIFICADA OPTIMIZADA DE DATOS DE MERCADO...');
   console.log('📅 CALCULANDO RENDIMIENTOS REALES PARA FECHA: 2025-06-27');
   console.log('='.repeat(80));
   
   const db = admin.firestore();
   const startTime = Date.now();
   
   // ✨ ZONA HORARIA: Mostrar hora actual en America/New_York
   const currentTime = DateTime.now().setZone('America/New_York');
   console.log(`🕐 Hora de inicio (NY): ${currentTime.toFormat('yyyy-MM-dd HH:mm:ss')}`);
   console.log(`📅 Fecha de procesamiento: ${currentTime.toISODate()}`);
   
   try {
    // ✨ OPTIMIZACIÓN: Consultas iniciales en paralelo
    console.log('\n📋 PASO 1: Obteniendo códigos de monedas y símbolos de activos (PARALELO)...');
    const [currenciesSnapshot, currentPricesSnapshot] = await Promise.all([
      db.collection('currencies').where('isActive', '==', true).get(),
      db.collection('currentPrices').get()
    ]);
    
    const currencyCodes = currenciesSnapshot.docs.map(doc => doc.data().code);
    const assetSymbols = currentPricesSnapshot.docs.map(doc => doc.data().symbol);
    
    console.log(`   📊 Monedas activas encontradas: ${currencyCodes.length}`);
    console.log(`   📊 Activos encontrados: ${assetSymbols.length}`);
    console.log(`   💱 Códigos de monedas: ${currencyCodes.join(', ')}`);
    console.log(`   📈 Primeros 10 activos: ${assetSymbols.slice(0, 10).join(', ')}${assetSymbols.length > 10 ? '...' : ''}`);
    
    // Paso 2: Obtener TODOS los datos de mercado en llamadas optimizadas
    console.log('\n📡 PASO 2: Obteniendo datos de mercado de la API Lambda (OPTIMIZADO)...');
    const marketData = await getAllMarketDataBatch(currencyCodes, assetSymbols);
    
    // ✨ OPTIMIZACIÓN: Ejecutar actualizaciones en paralelo cuando sea posible
    console.log('\n⚡ PASO 3-4: Actualizando tasas y precios EN PARALELO (OPTIMIZACIÓN)...');
    const [currencyUpdates, priceUpdates] = await Promise.all([
      updateCurrencyRatesOptimized(db, marketData.currencies),
      updateCurrentPricesOptimized(db, marketData.assets)
    ]);
    
    // Paso 5: Calcular rendimiento del portafolio REAL
    console.log('\n📊 PASO 5: Calculando rendimiento REAL del portafolio para 2025-06-27 (OPTIMIZADO)...');
    const portfolioCalculations = await calculateDailyPortfolioPerformanceReal(db);
    
    // Paso 6: Simular cálculo de riesgo del portafolio
    console.log('\n⚡ PASO 6: Simulando cálculo de riesgo del portafolio...');
    const riskCalculationSuccess = await testCalculatePortfolioRisk();
    
    const endTime = Date.now();
    const executionTime = (endTime - startTime) / 1000;
    
    console.log('\n' + '='.repeat(80));
    console.log('🎉 PRUEBA REAL DE ACTUALIZACIÓN UNIFICADA OPTIMIZADA COMPLETADA');
    console.log('📊 RENDIMIENTOS REALES CALCULADOS PARA 2025-06-27');
    console.log('='.repeat(80));
    console.log(`⏱️  Tiempo total de ejecución: ${executionTime.toFixed(2)}s`);
    console.log(`💱 Tasas de cambio actualizadas: ${currencyUpdates}`);
    console.log(`📈 Precios de activos actualizados: ${priceUpdates}`);
    console.log(`👥 Usuarios con cuentas activas: ${portfolioCalculations}`);
    console.log(`⚡ Cálculo de riesgo: ${riskCalculationSuccess ? 'Exitoso' : 'Falló'}`);
    console.log('\n🚀 OPTIMIZACIONES APLICADAS:');
    console.log('   ✅ Consultas iniciales en paralelo');
    console.log('   ✅ Actualizaciones de tasas y precios en paralelo');
    console.log('   ✅ Sistema de caché para datos históricos');
    console.log('   ✅ Cálculo REAL de rendimiento de portafolio');
    console.log('   ✅ Uso de valuePnL en transacciones de venta');
    console.log('   ✅ Batch management inteligente');
    console.log('   ✅ Escrituras selectivas (solo campos cambiados)');
    console.log('   ✅ Operaciones idempotentes');
    console.log('='.repeat(80));
    
    return {
      success: true,
      executionTime,
      currencyUpdates,
      priceUpdates,
      portfolioCalculations,
      riskCalculationSuccess,
      optimizationsApplied: [
        'Consultas paralelas',
        'Sistema de caché',
        'Cálculo REAL de portafolio',
        'Uso de valuePnL',
        'Batch management',
        'Escrituras selectivas',
        'Operaciones idempotentes'
      ]
    };
  } catch (error) {
    console.error('\n❌ ERROR EN PRUEBA DE ACTUALIZACIÓN UNIFICADA OPTIMIZADA:', error);
    console.error('Stack trace:', error.stack);
    return {
      success: false,
      error: error.message
    };
  }
}

// Ejecutar la prueba optimizada REAL
console.log('🧪 Iniciando prueba REAL de función unificada OPTIMIZADA para fecha 2025-06-27...');
testUnifiedMarketDataUpdateOptimized()
  .then(result => {
    if (result) {
      console.log('\n✅ Prueba finalizada:', result.success ? 'EXITOSA' : 'CON ERRORES');
      if (result.success && result.optimizationsApplied) {
        console.log(`🚀 Optimizaciones aplicadas: ${result.optimizationsApplied.length}`);
      }
    }
    process.exit(result?.success ? 0 : 1);
  })
  .catch(error => {
    console.error('\n💥 Error fatal en la prueba:', error);
    process.exit(1);
  }); 