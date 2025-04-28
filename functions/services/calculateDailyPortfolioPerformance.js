const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require('firebase-admin');
const { calculateAccountPerformance, convertCurrency } = require('../utils/portfolioCalculations');
const { DateTime } = require('luxon');

exports.calcDailyPortfolioPerf = onSchedule({
  schedule: '*/3 9-17 * * 1-5',
  timeZone: 'America/New_York',
  retryCount: 3,
}, async (event) => {
  const db = admin.firestore();
  const now = DateTime.now().setZone('America/New_York');
  const formattedDate = now.toISODate();
  
  try {
    // Obtener transacciones para el día actual primero
    const transactionsSnapshot = await db.collection('transactions')
      .where('date', '==', formattedDate)
      .get();
    
    const todaysTransactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    
    // Filtrar transacciones por tipo
    const sellTransactions = todaysTransactions.filter(t => t.type === 'sell');
    
    // Extraer IDs de activos involucrados en ventas hoy
    const assetIdsInSellTransactions = [...new Set(sellTransactions.map(t => t.assetId).filter(id => id))];
    
    // Obtener activos activos
    const activeAssetsSnapshot = await db.collection('assets')
      .where('isActive', '==', true)
      .get();
    
    const activeAssets = activeAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    
    // Obtener activos inactivos específicos involucrados en ventas (solo si hay ventas)
    let inactiveAssets = [];
    if (assetIdsInSellTransactions.length > 0) {
      // Firestore no permite usar .where('id', 'in', [...]) directamente, así que hacemos una consulta más amplia
      const inactiveAssetsSnapshot = await db.collection('assets')
        .where('isActive', '==', false)
        .get();
      
      // Filtrar manualmente los activos inactivos que están en transacciones de venta
      inactiveAssets = inactiveAssetsSnapshot.docs
        .map(doc => ({ id: doc.id, ...doc.data() }))
        .filter(asset => assetIdsInSellTransactions.includes(asset.id));
      
      console.log(`Obtenidos ${inactiveAssets.length} activos inactivos involucrados en ventas hoy`);
    } else {
      console.log('No hay activos inactivos involucrados en ventas hoy');
    }
    
    // Ahora tenemos activeAssets y inactiveAssets, que es el equivalente a allAssets filtrado
    const allAssets = [...activeAssets, ...inactiveAssets];
    
    // Consultar el resto de datos necesarios
    const [currentPricesSnapshot, currenciesSnapshot, portfolioAccountsSnapshot] = await Promise.all([
      db.collection('currentPrices').get(),
      db.collection('currencies').where('isActive', '==', true).get(),
      db.collection('portfolioAccounts').where('isActive', '==', true).get()
    ]);
    
    const currentPrices = currentPricesSnapshot.docs.map(doc => ({ symbol: doc.id.split(':')[0], ...doc.data() }));
    const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    
    // Obtener todas las transacciones de compra para los activos vendidos hoy
    const assetIdsWithSells = new Set(assetIdsInSellTransactions);
    
    // Variables para almacenar las transacciones de compra históricas
    let historicalBuyTransactions = [];
    let buyTransactionsByAssetId = {};
    
    // Solo realizar la consulta si hay assetIds con ventas
    if (assetIdsWithSells.size > 0) {
      console.log(`Obteniendo transacciones de compra históricas para ${assetIdsWithSells.size} activos vendidos`);
      // Obtener todas las transacciones de compra históricas para estos activos
      const historicalBuyTransactionsSnapshot = await db.collection('transactions')
        .where('type', '==', 'buy')
        .where('assetId', 'in', [...assetIdsWithSells])
        .get();
      
      historicalBuyTransactions = historicalBuyTransactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
      
      // Agrupar transacciones de compra por assetId
      buyTransactionsByAssetId = historicalBuyTransactions.reduce((acc, t) => {
        if (!acc[t.assetId]) {
          acc[t.assetId] = [];
        }
        acc[t.assetId].push(t);
        return acc;
      }, {});
    } else {
      console.log('No hay transacciones de venta con assetId válido para hoy');
    }
    
    // Agrupar transacciones de venta por portfolioAccountId y assetId
    const sellTransactionsByAccount = sellTransactions.reduce((acc, transaction) => {
      if (!acc[transaction.portfolioAccountId]) {
        acc[transaction.portfolioAccountId] = [];
      }
      acc[transaction.portfolioAccountId].push(transaction);
      return acc;
    }, {});

    // Identificar assets inactivos que tuvieron ventas hoy
    const inactiveAssetsWithSellTransactions = new Set(
      inactiveAssets.filter(asset => 
        sellTransactions.some(tx => tx.assetId === asset.id)
      ).map(asset => asset.id)
    );
    
    // Incluir assets inactivos que tuvieron ventas hoy
    const assetsToInclude = [
      ...activeAssets,
      ...inactiveAssets.filter(asset => inactiveAssetsWithSellTransactions.has(asset.id))
    ];
    
    console.log(`Incluyendo ${inactiveAssetsWithSellTransactions.size} assets inactivos con transacciones de venta para hoy`);

    const userPortfolios = portfolioAccounts.reduce((acc, account) => {
      if (!acc[account.userId]) acc[account.userId] = [];
      acc[account.userId].push(account);
      return acc;
    }, {});

    for (const [userId, accounts] of Object.entries(userPortfolios)) {
      const batch = db.batch();

      // Ensure user document exists
      const userPerformanceRef = db.collection('portfolioPerformance').doc(userId);
      const userPerformanceDoc = await userPerformanceRef.get();
      if (!userPerformanceDoc.exists) {
        batch.set(userPerformanceRef, { userId });
      }

      // Find the most recent date with performance data
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

      // Incluir tanto activos como inactivos que tuvieron transacciones de venta
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

      // Calcular doneProfitAndLoss para cada moneda a nivel de usuario
      const userDoneProfitAndLossByCurrency = {};
      
      // Filtrar transacciones de venta del usuario
      const userSellTransactions = sellTransactions.filter(t => 
        accounts.some(account => account.id === t.portfolioAccountId)
      );
      
      // Para cada moneda, calcular doneProfitAndLoss
      for (const currency of currencies) {
        let totalDoneProfitAndLoss = 0;
        const assetDoneProfitAndLoss = {};
        
        // Procesar ventas y calcular doneProfitAndLoss
        userSellTransactions.forEach(sellTx => {
          const sellAmountConverted = convertCurrency(
            sellTx.amount * sellTx.price,
            sellTx.currency,
            currency.code,
            currencies,
            sellTx.defaultCurrencyForAdquisitionDollar,
            parseFloat(sellTx.dollarPriceToDate.toString())
          );
          
          // Si hay assetId, calcular doneProfitAndLoss
          if (sellTx.assetId) {
            // Encontrar el activo correspondiente (activo o inactivo)
            const asset = allAssets.find(a => a.id === sellTx.assetId);
            if (asset) {
              const assetKey = `${asset.name}_${asset.assetType}`;
              if (!assetDoneProfitAndLoss[assetKey]) {
                assetDoneProfitAndLoss[assetKey] = 0;
              }
              
              // Calcular doneProfitAndLoss (ganancias o pérdidas realizadas)
              const buyTxsForAsset = buyTransactionsByAssetId[sellTx.assetId] || [];
              
              if (buyTxsForAsset.length > 0) {
                // Calcular el costo promedio de adquisición para esas unidades
                let totalBuyCost = 0;
                let totalBuyUnits = 0;
                
                buyTxsForAsset.forEach(buyTx => {
                  totalBuyCost += buyTx.amount * buyTx.price;
                  totalBuyUnits += buyTx.amount;
                });
                
                const avgCostPerUnit = totalBuyCost / totalBuyUnits;
                const costOfSoldUnits = sellTx.amount * avgCostPerUnit;
                
                // Convertir el costo de adquisición a la moneda actual
                const costOfSoldUnitsConverted = convertCurrency(
                  costOfSoldUnits,
                  sellTx.currency,
                  currency.code,
                  currencies,
                  sellTx.defaultCurrencyForAdquisitionDollar,
                  parseFloat(sellTx.dollarPriceToDate.toString())
                );
                
                // El PnL es la diferencia entre el valor de venta y el costo de adquisición
                const profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
                
                // Acumular para el asset específico
                assetDoneProfitAndLoss[assetKey] += profitAndLoss;
                totalDoneProfitAndLoss += profitAndLoss;
                
                console.log(`Calculado P&L para venta de ${sellTx.amount} unidades de ${assetKey}: ${profitAndLoss} ${currency.code}`);
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
          
          // Calcular y añadir unrealizedProfitAndLoss
          const unrealizedProfitAndLoss = overallPerformance[currencyCode].totalValue - overallPerformance[currencyCode].totalInvestment;
          overallPerformance[currencyCode].unrealizedProfitAndLoss = unrealizedProfitAndLoss;
          console.log(`Calculado unrealizedProfitAndLoss para ${currencyCode} a nivel de usuario: ${unrealizedProfitAndLoss}`);
          
          // Agregar doneProfitAndLoss a nivel de activo para los que tuvieron ventas
          if (overallPerformance[currencyCode].assetPerformance) {
            // Primero: Asignar doneProfitAndLoss a los activos que tuvieron ventas
            for (const [assetKey, pnl] of Object.entries(data.assetDoneProfitAndLoss)) {
              if (overallPerformance[currencyCode].assetPerformance[assetKey]) {
                overallPerformance[currencyCode].assetPerformance[assetKey].doneProfitAndLoss = pnl;
              }
            }
            
            // Segundo: Calcular unrealizedProfitAndLoss para TODOS los activos
            console.log(`Calculando unrealizedProfitAndLoss para todos los activos en ${currencyCode}`);
            for (const [assetKey, assetData] of Object.entries(overallPerformance[currencyCode].assetPerformance)) {
              const assetTotalValue = assetData.totalValue || 0;
              const assetTotalInvestment = assetData.totalInvestment || 0;
              const assetUnrealizedPnL = assetTotalValue - assetTotalInvestment;
              
              // Asignar unrealizedProfitAndLoss para este activo
              overallPerformance[currencyCode].assetPerformance[assetKey].unrealizedProfitAndLoss = assetUnrealizedPnL;
              console.log(`Calculado unrealizedProfitAndLoss para ${assetKey} en ${currencyCode}: ${assetUnrealizedPnL}`);
            }
          }
        }
      }

      // Save overall user performance (overwrite existing data)
      const userOverallPerformanceRef = userPerformanceRef
        .collection('dates')
        .doc(formattedDate);
      batch.set(userOverallPerformanceRef, {
        date: formattedDate,
        ...overallPerformance
      }, { merge: true }); // Usar merge:true para preservar otros campos existentes

      for (const account of accounts) {
        // Incluir activos inactivos que tuvieron ventas para esta cuenta
        const accountSellTransactions = sellTransactionsByAccount[account.id] || [];
        const inactiveAccountAssetsWithSells = inactiveAssets.filter(asset => 
          asset.portfolioAccount === account.id && 
          accountSellTransactions.some(t => t.assetId === asset.id)
        );
        
        // Combinar activos activos e inactivos con ventas hoy para esta cuenta
        const accountAssets = [
          ...activeAssets.filter(asset => asset.portfolioAccount === account.id),
          ...inactiveAccountAssetsWithSells
        ];

        // Ensure account document exists
        const accountRef = userPerformanceRef.collection('accounts').doc(account.id);
        const accountDoc = await accountRef.get();
        if (!accountDoc.exists) {
          batch.set(accountRef, { accountId: account.id });
        }

        // Find the most recent date with performance data for this account
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

        // Calcular doneProfitAndLoss para cada moneda a nivel de cuenta
        const accountDoneProfitAndLossByCurrency = {};
        
        // Para cada moneda, calcular doneProfitAndLoss a nivel de cuenta
        for (const currency of currencies) {
          let accountDoneProfitAndLoss = 0;
          const accountAssetDoneProfitAndLoss = {};
          
          // Procesar ventas y calcular doneProfitAndLoss
          accountSellTransactions.forEach(sellTx => {
            const sellAmountConverted = convertCurrency(
              sellTx.amount * sellTx.price,
              sellTx.currency,
              currency.code,
              currencies,
              sellTx.defaultCurrencyForAdquisitionDollar,
              parseFloat(sellTx.dollarPriceToDate.toString())
            );
            
            // Si hay assetId, calcular doneProfitAndLoss
            if (sellTx.assetId) {
              // Encontrar el activo correspondiente (activo o inactivo)
              const asset = allAssets.find(a => a.id === sellTx.assetId);
              if (asset) {
                const assetKey = `${asset.name}_${asset.assetType}`;
                if (!accountAssetDoneProfitAndLoss[assetKey]) {
                  accountAssetDoneProfitAndLoss[assetKey] = 0;
                }
                
                // Calcular doneProfitAndLoss (ganancias o pérdidas realizadas)
                const buyTxsForAsset = buyTransactionsByAssetId[sellTx.assetId] || [];
                
                if (buyTxsForAsset.length > 0) {
                  // Calcular el costo promedio de adquisición para esas unidades
                  let totalBuyCost = 0;
                  let totalBuyUnits = 0;
                  
                  buyTxsForAsset.forEach(buyTx => {
                    totalBuyCost += buyTx.amount * buyTx.price;
                    totalBuyUnits += buyTx.amount;
                  });
                  
                  const avgCostPerUnit = totalBuyCost / totalBuyUnits;
                  const costOfSoldUnits = sellTx.amount * avgCostPerUnit;
                  
                  // Convertir el costo de adquisición a la moneda actual
                  const costOfSoldUnitsConverted = convertCurrency(
                    costOfSoldUnits,
                    sellTx.currency,
                    currency.code,
                    currencies,
                    sellTx.defaultCurrencyForAdquisitionDollar,
                    parseFloat(sellTx.dollarPriceToDate.toString())
                  );
                  
                  // El PnL es la diferencia entre el valor de venta y el costo de adquisición
                  const profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
                  
                  // Acumular para el asset específico
                  accountAssetDoneProfitAndLoss[assetKey] += profitAndLoss;
                  accountDoneProfitAndLoss += profitAndLoss;
                  
                  console.log(`Calculado P&L para venta de ${sellTx.amount} unidades de ${assetKey} en cuenta ${account.id}: ${profitAndLoss} ${currency.code}`);
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
            
            // Calcular y añadir unrealizedProfitAndLoss
            const accountUnrealizedPnL = accountPerformance[currencyCode].totalValue - accountPerformance[currencyCode].totalInvestment;
            accountPerformance[currencyCode].unrealizedProfitAndLoss = accountUnrealizedPnL;
            console.log(`Calculado unrealizedProfitAndLoss para cuenta ${account.id} en ${currencyCode}: ${accountUnrealizedPnL}`);
            
            // Agregar doneProfitAndLoss a nivel de activo
            if (accountPerformance[currencyCode].assetPerformance) {
              // Primero: Asignar doneProfitAndLoss a los activos que tuvieron ventas
              for (const [assetKey, pnl] of Object.entries(data.assetDoneProfitAndLoss)) {
                if (accountPerformance[currencyCode].assetPerformance[assetKey]) {
                  accountPerformance[currencyCode].assetPerformance[assetKey].doneProfitAndLoss = pnl;
                }
              }
              
              // Segundo: Calcular unrealizedProfitAndLoss para TODOS los activos
              console.log(`Calculando unrealizedProfitAndLoss para todos los activos en cuenta ${account.id}, moneda ${currencyCode}`);
              for (const [assetKey, assetData] of Object.entries(accountPerformance[currencyCode].assetPerformance)) {
                const accountAssetTotalValue = assetData.totalValue || 0;
                const accountAssetTotalInvestment = assetData.totalInvestment || 0;
                const accountAssetUnrealizedPnL = accountAssetTotalValue - accountAssetTotalInvestment;
                
                // Asignar unrealizedProfitAndLoss para este activo
                accountPerformance[currencyCode].assetPerformance[assetKey].unrealizedProfitAndLoss = accountAssetUnrealizedPnL;
                console.log(`Calculado unrealizedProfitAndLoss para ${assetKey} en cuenta ${account.id}, moneda ${currencyCode}: ${accountAssetUnrealizedPnL}`);
              }
            }
          }
        }

        // Save account performance (overwrite existing data)
        const accountPerformanceRef = accountRef
          .collection('dates')
          .doc(formattedDate);
        batch.set(accountPerformanceRef, {
          date: formattedDate,
          ...accountPerformance
        }, { merge: true }); // Usar merge:true para preservar otros campos existentes
      }

      await batch.commit();
      console.log(`Datos de rendimiento de la cartera calculados y actualizados para el usuario ${userId} en ${formattedDate}`);
    }

    console.log(`Cálculo del rendimiento diario de la cartera completado para ${formattedDate}`);
    return null;
  } catch (error) {
    console.error('Error al calcular el rendimiento diario de la cartera:', error);
    return null;
  }
});