const functions = require('firebase-functions');
const admin = require('firebase-admin');
const { calculateAccountPerformance } = require('../utils/portfolioCalculations');

exports.calcDailyPortfolioPerf = functions.pubsub
    .schedule('0 17 * * 1-5')
    .timeZone('America/New_York')
    .onRun(async (context) => {
        const db = admin.firestore();
        const now = new Date();
        const formattedDate = now.toISOString().split('T')[0];
        const yesterday = new Date(now.setDate(now.getDate() - 1));
        const formattedYesterday = yesterday.toISOString().split('T')[0];

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

            const batch = db.batch();

            for (const [userId, accounts] of Object.entries(userPortfolios)) {
                const userPerformanceData = { accounts: {}, overall: {} };

                for (const account of accounts) {
                    const accountAssets = assets.filter(asset => asset.portfolioAccount === account.id);

                    const yesterdayPerformanceDoc = await db.collection('portfolioPerformance')
                        .doc(`${formattedYesterday}`)
                        .collection('users')
                        .doc(userId)
                        .collection('accounts')
                        .doc(account.id)
                        .get();

                    const yesterdayTotalValue = yesterdayPerformanceDoc.exists
                        ? yesterdayPerformanceDoc.data()?.totalValue || {}
                        : currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: 0 }), {});

                    const accountPerformance = calculateAccountPerformance(
                        accountAssets,
                        currentPrices,
                        currencies,
                        yesterdayTotalValue
                    );

                    userPerformanceData.accounts[account.id] = accountPerformance;

                    // Guardar rendimiento de la cuenta
                    const accountPerformanceRef = db.collection('portfolioPerformance')
                        .doc(formattedDate)
                        .collection('users')
                        .doc(userId)
                        .collection('accounts')
                        .doc(account.id);
                    batch.set(accountPerformanceRef, accountPerformance);
                }

                // Calcular rendimiento general del usuario
                const allUserAssets = assets.filter(asset => accounts.some(account => account.id === asset.portfolioAccount));

                const yesterdayOverallPerformanceDoc = await db.collection('portfolioPerformance')
                    .doc(formattedYesterday)
                    .collection('users')
                    .doc(userId)
                    .get();

                const yesterdayOverallTotalValue = yesterdayOverallPerformanceDoc.exists
                    ? yesterdayOverallPerformanceDoc.data()?.totalValue || {}
                    : currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: 0 }), {});

                const overallPerformance = calculateAccountPerformance(
                    allUserAssets,
                    currentPrices,
                    currencies,
                    yesterdayOverallTotalValue
                );

                userPerformanceData.overall = overallPerformance;

                // Guardar rendimiento general del usuario
                const userOverallPerformanceRef = db.collection('portfolioPerformance')
                    .doc(formattedDate)
                    .collection('users')
                    .doc(userId);
                batch.set(userOverallPerformanceRef, overallPerformance);

                console.log(`Datos de rendimiento del portafolio calculados para el usuario ${userId} en ${formattedDate}`);
            }

            await batch.commit();
            console.log(`Cálculo de rendimiento diario del portafolio completado para ${formattedDate}`);
            return null;
        } catch (error) {
            console.error('Error al calcular el rendimiento diario del portafolio:', error);
            return null;
        }
    });