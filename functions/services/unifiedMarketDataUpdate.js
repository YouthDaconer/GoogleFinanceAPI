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
 * Obtiene las tasas de cambio actuales de múltiples monedas en una sola petición
 */
async function getCurrencyRatesBatch(currencyCodes) {
  try {
    const symbolsParam = currencyCodes.map(code => `${code}%3DX`).join(',');
    const url = `${API_BASE_URL}/market-quotes?symbols=${symbolsParam}`;
    
    console.log(`Consultando tasas para múltiples monedas: ${url}`);
    
    const { data } = await axios.get(url);
    const rates = {};
    
    if (Array.isArray(data)) {
      data.forEach(currencyData => {
        const code = currencyData.symbol.replace('%3DX', '');
        if (currencyData.regularMarketPrice && !isNaN(currencyData.regularMarketPrice)) {
          rates[code] = currencyData.regularMarketPrice;
        }
      });
      return rates;
    }
    
    console.warn('Formato de respuesta inesperado para tasas de cambio:', data);
    return null;
  } catch (error) {
    console.error(`Error al obtener tasas de cambio en lote:`, error.message);
    return null;
  }
}

/**
 * Actualiza las tasas de cambio de monedas
 */
async function updateCurrencyRates(db) {
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
  
  const currencyCodes = activeCurrencies.map(currency => currency.code);
  const exchangeRates = await getCurrencyRatesBatch(currencyCodes);
  
  if (exchangeRates) {
    activeCurrencies.forEach(currency => {
      const { code, ref, data } = currency;
      const newRate = exchangeRates[code];
      
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
  }
  
  return updatesCount;
}

/**
 * Actualiza los precios actuales de los activos
 */
async function updateCurrentPrices(db) {
  console.log('🔄 Actualizando precios actuales...');
  
  const currentPricesRef = db.collection('currentPrices');
  const snapshot = await currentPricesRef.get();
  const batch = db.batch();
  let updatesCount = 0;

  const symbols = snapshot.docs.map(doc => doc.data().symbol);
  const batchSize = 50;
  
  for (let i = 0; i < symbols.length; i += batchSize) {
    const symbolBatch = symbols.slice(i, i + batchSize);
    const symbolsParam = symbolBatch.join(',');
    
    try {
      const response = await axios.get(`${API_BASE_URL}/market-quotes?symbols=${symbolsParam}`);
      
      if (Array.isArray(response.data)) {
        const quotesMap = new Map(response.data.map(quote => [quote.symbol, quote]));
        
        for (const symbol of symbolBatch) {
          const quote = quotesMap.get(symbol);
          
          if (quote && quote.regularMarketPrice) {
            const matchingDocs = snapshot.docs.filter(doc => doc.data().symbol === symbol);
            
            if (matchingDocs.length > 0) {
              const doc = matchingDocs[0];
              const docData = doc.data();
              
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
          }
        }
      }
    } catch (error) {
      console.error(`Error al obtener cotizaciones para el lote:`, error.message);
    }
  }

  if (updatesCount > 0) {
    await batch.commit();
    console.log(`✅ ${updatesCount} precios actualizados`);
  }
  
  return updatesCount;
}

/**
 * Calcula el rendimiento diario del portafolio
 */
async function calculateDailyPortfolioPerformance(db) {
  console.log('🔄 Calculando rendimiento diario del portafolio...');
  
  const now = DateTime.now().setZone('America/New_York');
  const formattedDate = now.toISODate();
  let calculationsCount = 0;
  
  // Obtener transacciones para el día actual
  const transactionsSnapshot = await db.collection('transactions')
    .where('date', '==', formattedDate)
    .get();
  
  const todaysTransactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const sellTransactions = todaysTransactions.filter(t => t.type === 'sell');
  const assetIdsInSellTransactions = [...new Set(sellTransactions.map(t => t.assetId).filter(id => id))];
  
  // Obtener activos activos e inactivos necesarios
  const [activeAssetsSnapshot, currenciesSnapshot, portfolioAccountsSnapshot] = await Promise.all([
    db.collection('assets').where('isActive', '==', true).get(),
    db.collection('currencies').where('isActive', '==', true).get(),
    db.collection('portfolioAccounts').where('isActive', '==', true).get()
  ]);
  
  const activeAssets = activeAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  
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
  
  // Obtener datos adicionales necesarios
  const [currentPricesSnapshot] = await Promise.all([
    db.collection('currentPrices').get()
  ]);
  
  const currentPrices = currentPricesSnapshot.docs.map(doc => ({ symbol: doc.id.split(':')[0], ...doc.data() }));
  const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  
  // Obtener transacciones de compra históricas para activos vendidos
  const assetIdsWithSells = new Set(assetIdsInSellTransactions);
  let buyTransactionsByAssetId = {};
  
  if (assetIdsWithSells.size > 0) {
    const historicalBuyTransactionsSnapshot = await db.collection('transactions')
      .where('type', '==', 'buy')
      .where('assetId', 'in', [...assetIdsWithSells])
      .get();
    
    const historicalBuyTransactions = historicalBuyTransactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    
    buyTransactionsByAssetId = historicalBuyTransactions.reduce((acc, t) => {
      if (!acc[t.assetId]) acc[t.assetId] = [];
      acc[t.assetId].push(t);
      return acc;
    }, {});
  }
  
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

  // Procesar cada usuario
  for (const [userId, accounts] of Object.entries(userPortfolios)) {
    const batch = db.batch();

    // Asegurar que el documento de usuario existe
    const userPerformanceRef = db.collection('portfolioPerformance').doc(userId);
    const userPerformanceDoc = await userPerformanceRef.get();
    if (!userPerformanceDoc.exists) {
      batch.set(userPerformanceRef, { userId });
    }

    // Encontrar la fecha más reciente con datos de rendimiento
    const lastPerformanceQuery = await userPerformanceRef
      .collection('dates')
      .where('date', '<', formattedDate)
      .orderBy('date', 'desc')
      .limit(1)
      .get();

    let lastOverallTotalValue = currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: { totalValue: 0 } }), {});

    if (!lastPerformanceQuery.empty) {
      const lastPerformanceDoc = lastPerformanceQuery.docs[0];
      lastOverallTotalValue = Object.entries(lastPerformanceDoc.data() || {}).reduce((acc, [currency, data]) => {
        if (currency !== 'date') {
          acc[currency] = {
            totalValue: data.totalValue || 0,
            ...Object.entries(data.assetPerformance || {}).reduce((assetAcc, [assetName, assetData]) => {
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
            
            const buyTxsForAsset = buyTransactionsByAssetId[sellTx.assetId] || [];
            
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

    // Guardar rendimiento general del usuario
    const userOverallPerformanceRef = userPerformanceRef
      .collection('dates')
      .doc(formattedDate);
    batch.set(userOverallPerformanceRef, {
      date: formattedDate,
      ...overallPerformance
    }, { merge: true });

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

      const accountRef = userPerformanceRef.collection('accounts').doc(account.id);
      const accountDoc = await accountRef.get();
      if (!accountDoc.exists) {
        batch.set(accountRef, { accountId: account.id });
      }

      const lastAccountPerformanceQuery = await accountRef
        .collection('dates')
        .where('date', '<', formattedDate)
        .orderBy('date', 'desc')
        .limit(1)
        .get();

      let lastAccountTotalValue = currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: { totalValue: 0 } }), {});

      if (!lastAccountPerformanceQuery.empty) {
        const lastAccountPerformanceDoc = lastAccountPerformanceQuery.docs[0];
        lastAccountTotalValue = Object.entries(lastAccountPerformanceDoc.data() || {}).reduce((acc, [currency, data]) => {
          if (currency !== 'date') {
            acc[currency] = {
              totalValue: data.totalValue || 0,
              ...Object.entries(data.assetPerformance || {}).reduce((assetAcc, [assetName, assetData]) => {
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
              
              const buyTxsForAsset = buyTransactionsByAssetId[sellTx.assetId] || [];
              
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

      // Guardar rendimiento de la cuenta
      const accountPerformanceRef = accountRef
        .collection('dates')
        .doc(formattedDate);
      batch.set(accountPerformanceRef, {
        date: formattedDate,
        ...accountPerformance
      }, { merge: true });
    }

    await batch.commit();
    calculationsCount++;
    console.log(`Rendimiento calculado para usuario ${userId}`);
  }

  console.log(`✅ Rendimiento calculado para ${calculationsCount} usuarios`);
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
    // Paso 1: Actualizar tasas de cambio
    const currencyUpdates = await updateCurrencyRates(db);
    
    // Paso 2: Actualizar precios actuales
    const priceUpdates = await updateCurrentPrices(db);
    
    // Paso 3: Calcular rendimiento del portafolio
    const portfolioCalculations = await calculateDailyPortfolioPerformance(db);
    
    // Paso 4: Calcular riesgo del portafolio (usando datos actualizados)
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