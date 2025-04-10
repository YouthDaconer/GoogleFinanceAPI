const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require('firebase-admin');
const { calculateAccountPerformance } = require('../utils/portfolioCalculations');
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
    // Obtener activos activos e inactivos por separado
    const [activeAssetsSnapshot, allAssetsSnapshot, currentPricesSnapshot, currenciesSnapshot, portfolioAccountsSnapshot, transactionsSnapshot] = await Promise.all([
      db.collection('assets').where('isActive', '==', true).get(), // Solo assets activos
      db.collection('assets').get(), // Todos los assets (activos e inactivos)
      db.collection('currentPrices').get(),
      db.collection('currencies').where('isActive', '==', true).get(),
      db.collection('portfolioAccounts').where('isActive', '==', true).get(),
      db.collection('transactions').where('date', '==', formattedDate).get()
    ]);

    const activeAssets = activeAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    const allAssets = allAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    
    // Obtener assets inactivos
    const inactiveAssets = allAssets.filter(asset => !asset.isActive);
    
    const currentPrices = currentPricesSnapshot.docs.map(doc => ({ symbol: doc.id.split(':')[0], ...doc.data() }));
    const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    const todaysTransactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

    // Filtrar transacciones de venta de activos inactivos
    const sellTransactions = todaysTransactions.filter(t => t.type === 'sell');
    
    // Agrupar transacciones de venta por portfolioAccountId y assetId
    const sellTransactionsByAccount = sellTransactions.reduce((acc, transaction) => {
      if (!acc[transaction.portfolioAccountId]) {
        acc[transaction.portfolioAccountId] = [];
      }
      acc[transaction.portfolioAccountId].push(transaction);
      return acc;
    }, {});

    // Identificar assets inactivos que tuvieron ventas hoy
    const inactiveAssetsWithSellTransactions = new Set();
    sellTransactions.forEach(transaction => {
      if (transaction.assetId) {
        const asset = inactiveAssets.find(a => a.id === transaction.assetId);
        if (asset) {
          inactiveAssetsWithSellTransactions.add(asset.id);
        }
      }
    });
    
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
                assetAcc[assetName] = { totalValue: assetData.totalValue || 0 };
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

      // Save overall user performance (overwrite existing data)
      const userOverallPerformanceRef = userPerformanceRef
        .collection('dates')
        .doc(formattedDate);
      batch.set(userOverallPerformanceRef, {
        date: formattedDate,
        ...overallPerformance
      }, { merge: false });

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
                  assetAcc[assetName] = { totalValue: assetData.totalValue || 0 };
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

        // Save account performance (overwrite existing data)
        const accountPerformanceRef = accountRef
          .collection('dates')
          .doc(formattedDate);
        batch.set(accountPerformanceRef, {
          date: formattedDate,
          ...accountPerformance
        }, { merge: false });
      }

      await batch.commit();
      console.log(`Datos de rendimiento de la cartera calculados y sobrescritos para el usuario ${userId} en ${formattedDate}`);
    }

    console.log(`Cálculo del rendimiento diario de la cartera completado y sobrescrito para ${formattedDate}`);
    return null;
  } catch (error) {
    console.error('Error al calcular el rendimiento diario de la cartera:', error);
    return null;
  }
});