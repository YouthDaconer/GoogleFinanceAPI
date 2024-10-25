const admin = require('firebase-admin');
const { calculateAccountPerformance } = require('../utils/portfolioCalculations');
const { DateTime } = require('luxon');

async function calculatePortfolioPerformance() {
    const db = admin.firestore();
    const now = DateTime.now().setZone('America/New_York');
    const formattedDate = now.toISODate();
    const yesterday = now.minus({ days: 1 });
    const formattedYesterday = yesterday.toISODate();

    try {
      const [assetsSnapshot, currentPricesSnapshot, currenciesSnapshot, portfolioAccountsSnapshot] = await Promise.all([
        db.collection('assets').where('isActive', '==', true).get(),
        db.collection('currentPrices').get(),
        db.collection('currencies').where('isActive', '==', true).get(),
        db.collection('portfolioAccounts').where('isActive', '==', true).get()
      ]);

      const assets = assetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
      const currentPrices = currentPricesSnapshot.docs.map(doc => ({ symbol: doc.id.split(':')[0], ...doc.data() }));
      const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
      const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

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

        const yesterdayOverallPerformanceDoc = await userPerformanceRef
          .collection('dates')
          .doc(formattedYesterday)
          .get();

        const yesterdayOverallTotalValue = yesterdayOverallPerformanceDoc.exists
          ? Object.entries(yesterdayOverallPerformanceDoc.data()).reduce((acc, [currency, data]) => {
              acc[currency] = {
                totalValue: data.totalValue || 0,
                ...Object.entries(data.assetPerformance || {}).reduce((assetAcc, [assetName, assetData]) => {
                  assetAcc[assetName] = { totalValue: assetData.totalValue || 0 };
                  return assetAcc;
                }, {})
              };
              return acc;
            }, {})
          : currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: { totalValue: 0 } }), {});

        const allUserAssets = assets.filter(asset => accounts.some(account => account.id === asset.portfolioAccount));

        const overallPerformance = calculateAccountPerformance(
          allUserAssets,
          currentPrices,
          currencies,
          yesterdayOverallTotalValue
        );

        // Save overall user performance (overwrite existing data)
        const userOverallPerformanceRef = userPerformanceRef
          .collection('dates')
          .doc(formattedDate);
        batch.set(userOverallPerformanceRef, {
          date: formattedDate,
          ...overallPerformance
        }, { merge: false }); // Use merge: false to overwrite existing data

        for (const account of accounts) {
          const accountAssets = assets.filter(asset => asset.portfolioAccount === account.id);

          // Ensure account document exists
          const accountRef = userPerformanceRef.collection('accounts').doc(account.id);
          const accountDoc = await accountRef.get();
          if (!accountDoc.exists) {
            batch.set(accountRef, { accountId: account.id });
          }

          const yesterdayPerformanceDoc = await accountRef
            .collection('dates')
            .doc(formattedYesterday)
            .get();

          const yesterdayTotalValue = yesterdayPerformanceDoc.exists
            ? Object.entries(yesterdayPerformanceDoc.data()).reduce((acc, [currency, data]) => {
                acc[currency] = {
                  totalValue: data.totalValue || 0,
                  ...Object.entries(data.assetPerformance || {}).reduce((assetAcc, [assetName, assetData]) => {
                    assetAcc[assetName] = { totalValue: assetData.totalValue || 0 };
                    return assetAcc;
                  }, {})
                };
                return acc;
              }, {})
            : currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: { totalValue: 0 } }), {});

          const accountPerformance = calculateAccountPerformance(
            accountAssets,
            currentPrices,
            currencies,
            yesterdayTotalValue
          );

          // Save account performance (overwrite existing data)
          const accountPerformanceRef = accountRef
            .collection('dates')
            .doc(formattedDate);
          batch.set(accountPerformanceRef, {
            date: formattedDate,
            ...accountPerformance
          }, { merge: false }); // Use merge: false to overwrite existing data
        }

        await batch.commit();
        console.log(`Datos de rendimiento de la cartera calculados y sobrescritos para el usuario ${userId} en ${formattedDate}`);
      }

      console.log(`Cálculo diario del rendimiento de la cartera completado y sobrescrito para ${formattedDate}`);
      return null;
    } catch (error) {
      console.error('Error al calcular el rendimiento diario de la cartera:', error);
      return null;
    }
}

// Llamar a la función
calculatePortfolioPerformance();