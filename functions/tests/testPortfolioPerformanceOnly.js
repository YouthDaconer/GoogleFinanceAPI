const admin = require('../services/firebaseAdmin');
const { DateTime } = require('luxon');
const { calculateAccountPerformance, convertCurrency } = require('../utils/portfolioCalculations');

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
 * 🧪 PRUEBA: Función para probar solo calculateDailyPortfolioPerformance
 */
async function testCalculateDailyPortfolioPerformanceOnly() {
  console.log('🧪 PROBANDO SOLO calculateDailyPortfolioPerformance...');
  console.log('='.repeat(80));
  
  const db = admin.firestore();
  const startTime = Date.now();
  
  const now = DateTime.now().setZone('America/New_York');
  const formattedDate = now.toISODate();
  let calculationsCount = 0;
  
  console.log(`📅 Fecha de cálculo (NY): ${formattedDate}`);
  console.log(`🕐 Hora actual (NY): ${now.toFormat('yyyy-MM-dd HH:mm:ss')}`);

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
    
    console.log(`📊 Transacciones encontradas para ${formattedDate}: ${todaysTransactions.length} (${sellTransactions.length} ventas)`);
    
    const activeAssets = activeAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    const currentPrices = currentPricesSnapshot.docs.map(doc => ({ symbol: doc.id.split(':')[0], ...doc.data() }));
    
    console.log(`📊 Datos obtenidos: ${activeAssets.length} activos, ${currencies.length} monedas, ${portfolioAccounts.length} cuentas, ${currentPrices.length} precios`);
    
    // Obtener activos inactivos involucrados en ventas
    let inactiveAssets = [];
    if (assetIdsInSellTransactions.length > 0) {
      const inactiveAssetsSnapshot = await db.collection('assets')
        .where('isActive', '==', false)
        .get();
      
      inactiveAssets = inactiveAssetsSnapshot.docs
        .map(doc => ({ id: doc.id, ...doc.data() }))
        .filter(asset => assetIdsInSellTransactions.includes(asset.id));
      
      console.log(`📊 Obtenidos ${inactiveAssets.length} activos inactivos involucrados en ventas`);
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

      console.log(`   📝 Agregadas ${2} operaciones al batch para usuario ${userId}`);

      // Procesar cada cuenta del usuario
      for (const account of accounts) {
        const accountAssets = activeAssets.filter(asset => asset.portfolioAccount === account.id);
        const lastAccountTotalValue = cache.getAccountLastPerformance(account.id, currencies);

        const accountTransactions = userTransactions.filter(t => t.portfolioAccountId === account.id);
        const accountPerformance = calculateAccountPerformance(
          accountAssets,
          currentPrices,
          currencies,
          lastAccountTotalValue,
          accountTransactions
        );

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

        console.log(`   📝 Agregadas ${2} operaciones al batch para cuenta ${account.id}`);

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

    const endTime = Date.now();
    const executionTime = (endTime - startTime) / 1000;

    console.log('\n' + '='.repeat(80));
    console.log('🎉 PRUEBA DE calculateDailyPortfolioPerformance COMPLETADA');
    console.log('='.repeat(80));
    console.log(`⏱️  Tiempo de ejecución: ${executionTime.toFixed(2)}s`);
    console.log(`👥 Usuarios procesados: ${calculationsCount}`);
    console.log(`📅 Fecha procesada: ${formattedDate}`);
    console.log(`💾 Datos guardados en Firestore: SÍ`);
    console.log('='.repeat(80));

    return {
      success: true,
      executionTime,
      calculationsCount,
      formattedDate,
      dataCommitted: true
    };

  } catch (error) {
    console.error('\n❌ ERROR EN PRUEBA:', error);
    console.error('Stack trace:', error.stack);
    return {
      success: false,
      error: error.message
    };
  }
}

// Ejecutar la prueba
console.log('🧪 Iniciando prueba específica de calculateDailyPortfolioPerformance...');
testCalculateDailyPortfolioPerformanceOnly()
  .then(result => {
    if (result) {
      console.log('\n✅ Prueba finalizada:', result.success ? 'EXITOSA' : 'CON ERRORES');
      if (result.success) {
        console.log(`📊 Datos guardados para la fecha: ${result.formattedDate}`);
      }
    }
    process.exit(result?.success ? 0 : 1);
  })
  .catch(error => {
    console.error('\n💥 Error fatal en la prueba:', error);
    process.exit(1);
  }); 