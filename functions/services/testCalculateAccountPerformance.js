const admin = require('./firebaseAdmin');
const { calculateAccountPerformance } = require('../utils/portfolioCalculations');
const { DateTime } = require('luxon');

async function calculatePortfolioPerformance() {
    const db = admin.firestore();
    const now = DateTime.now().setZone('America/New_York');
    //const formattedDate = now.toISODate();
    const formattedDate = '2025-02-21';

    try {
      const [assetsSnapshot, currentPricesSnapshot, currenciesSnapshot, portfolioAccountsSnapshot, transactionsSnapshot] = await Promise.all([
        db.collection('assets').where('isActive', '==', true).get(),
        db.collection('currentPrices').get(),
        db.collection('currencies').where('isActive', '==', true).get(),
        db.collection('portfolioAccounts').where('isActive', '==', true).get(),
        db.collection('transactions').where('date', '==', formattedDate).get()
      ]);

      const assets = assetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
      const currentPrices = currentPricesSnapshot.docs.map(doc => ({ symbol: doc.id.split(':')[0], ...doc.data() }));
      const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
      const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
      const todaysTransactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

      // Intenta obtener los dividendos, pero no falla si la colección no existe
      let todaysDividends = [];
      try {
        const dividendsSnapshot = await db.collection('dividends').where('date', '==', formattedDate).get();
        todaysDividends = dividendsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
      } catch (error) {
        console.log('La colección de dividendos no se encontró o hubo un error al obtener los dividendos:', error);
      }

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

        const allUserAssets = assets.filter(asset => accounts.some(account => account.id === asset.portfolioAccount));
        const userTransactions = todaysTransactions.filter(t => accounts.some(account => account.id === t.portfolioAccountId));
        const userDividends = todaysDividends.filter(d => allUserAssets.some(asset => asset.id === d.assetId));

        const overallPerformance = calculateAccountPerformance(
          allUserAssets,
          currentPrices,
          currencies,
          lastOverallTotalValue,
          userTransactions,
          userDividends
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
          const accountAssets = assets.filter(asset => asset.portfolioAccount === account.id);

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
          const accountDividends = userDividends.filter(d => accountAssets.some(asset => asset.id === d.assetId));

          const accountPerformance = calculateAccountPerformance(
            accountAssets,
            currentPrices,
            currencies,
            lastAccountTotalValue,
            accountTransactions,
            accountDividends
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
}

// Llamar a la función
calculatePortfolioPerformance();