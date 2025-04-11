const admin = require('./firebaseAdmin');
const { DateTime } = require('luxon');

/**
 * Calcula el riesgo del portafolio basado en el beta de los activos
 * @returns {Promise<null>}
 */
async function calculatePortfolioRisk() {
  const db = admin.firestore();
  const calculationDate = DateTime.now().setZone('America/New_York').toISODate();
  
  try {
    // Obtener todos los usuarios con portfolioPerformance
    const portfolioPerformanceSnapshot = await db.collection('portfolioPerformance').get();
    const userIds = portfolioPerformanceSnapshot.docs.map(doc => doc.id);
    
    // Obtener todas las cotizaciones actuales con betas
    const currentPricesSnapshot = await db.collection('currentPrices').get();
    const currentPrices = {};
    
    // Crear mapa de símbolos a betas
    currentPricesSnapshot.docs.forEach(doc => {
      const data = doc.data();
      // Usar 1 como beta por defecto si no está definido (neutral)
      currentPrices[data.symbol] = {
        beta: data.beta !== undefined ? data.beta : 1.0,
        price: data.price || 0
      };
    });
    
    // Obtener monedas activas para conversiones
    const currenciesSnapshot = await db.collection('currencies').where('isActive', '==', true).get();
    const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    
    // Para cada usuario
    for (const userId of userIds) {
      console.log(`Calculando riesgo para usuario: ${userId}`);
      
      // Obtener cuentas del usuario
      const accountsSnapshot = await db.collection('portfolioAccounts')
        .where('userId', '==', userId)
        .where('isActive', '==', true)
        .get();
      
      const accounts = accountsSnapshot.docs.map(doc => ({
        id: doc.id,
        ...doc.data()
      }));
      
      if (accounts.length === 0) {
        console.log(`No hay cuentas activas para el usuario ${userId}`);
        continue;
      }
      
      // Obtener los IDs de las cuentas del usuario
      const accountIds = accounts.map(account => account.id);
      
      // Obtener activos activos relacionados con las cuentas del usuario
      const assetsSnapshot = await db.collection('assets')
        .where('portfolioAccount', 'in', accountIds)
        .where('isActive', '==', true)
        .get();
      
      const assets = assetsSnapshot.docs.map(doc => ({
        id: doc.id,
        ...doc.data()
      }));
      
      if (assets.length === 0) {
        console.log(`No hay activos activos para el usuario ${userId}`);
        continue;
      }
      
      // Agrupar activos por cuenta
      const assetsByAccount = {};
      accounts.forEach(account => {
        assetsByAccount[account.id] = assets.filter(asset => asset.portfolioAccount === account.id);
      });
      
      // Crear batch para guardar resultados
      const batch = db.batch();
      
      // Crear/actualizar documento de métricas generales
      const userMetricsRef = db.collection('portfolioMetrics').doc(userId);
      const riskMetricsRef = userMetricsRef.collection('riskMetrics').doc('latest');
      
      // Calcular riesgo general (todas las cuentas)
      const overallRisk = calculateBetaForAssets(assets, currentPrices, currencies);
      
      // Preparar datos para escritura general
      const riskData = {
        calculationDate,
        metrics: {
          portfolioBeta: overallRisk.portfolioBeta,
          riskCategory: getRiskCategory(overallRisk.portfolioBeta),
          assetCount: assets.length,
          weightedBetas: overallRisk.weightedBetas,
          totalValue: overallRisk.totalValue,
          includedValue: overallRisk.includedValue,
          portfolioCoverage: overallRisk.portfolioCoverage,
          excludedAssetCount: overallRisk.excludedAssets?.length || 0
        }
      };
      
      // Agregar a batch
      batch.set(riskMetricsRef, riskData);
      
      // Calcular y guardar riesgo por cuenta
      for (const [accountId, accountAssets] of Object.entries(assetsByAccount)) {
        if (accountAssets.length === 0) continue;
        
        // Calcular beta para esta cuenta
        const accountRisk = calculateBetaForAssets(accountAssets, currentPrices, currencies);
        
        // Crear referencia para la cuenta
        const accountRiskMetricsRef = userMetricsRef
          .collection('accounts')
          .doc(accountId)
          .collection('riskMetrics')
          .doc('latest');
        
        // Preparar datos para escritura de la cuenta
        const accountRiskData = {
          calculationDate,
          metrics: {
            portfolioBeta: accountRisk.portfolioBeta,
            riskCategory: getRiskCategory(accountRisk.portfolioBeta),
            assetCount: accountAssets.length,
            weightedBetas: accountRisk.weightedBetas,
            totalValue: accountRisk.totalValue,
            includedValue: accountRisk.includedValue,
            portfolioCoverage: accountRisk.portfolioCoverage,
            excludedAssetCount: accountRisk.excludedAssets?.length || 0
          }
        };
        
        // Agregar a batch
        batch.set(accountRiskMetricsRef, accountRiskData);
      }
      
      // Guardar todos los cambios
      await batch.commit();
      console.log(`Datos de riesgo calculados y actualizados para el usuario ${userId}`);
    }
    
    console.log('Cálculo de riesgo del portafolio completado');
    return null;
  } catch (error) {
    console.error('Error al calcular riesgo del portafolio:', error);
    return null;
  }
}

/**
 * Calcula el beta de un conjunto de activos
 * @param {Array} assets - Lista de activos
 * @param {Object} currentPrices - Mapa de símbolos a precios y betas
 * @param {Array} currencies - Lista de monedas para conversión
 * @returns {Object} - Beta del portafolio y datos complementarios
 */
function calculateBetaForAssets(assets, currentPrices, currencies) {
  // Calcular valor total del portafolio en USD
  let totalPortfolioValue = 0;
  let totalIncludedValue = 0;
  
  // Agrupar activos por símbolo (name)
  const symbolGroups = {};
  const excludedAssets = [];
  
  // Lista de tipos de activos que típicamente no tienen beta
  const assetTypesWithoutBeta = ['cryptocurrency', 'crypto', 'commodity', 'physical'];
  
  // Primero agrupamos los activos por símbolo y calculamos sus valores
  assets.forEach(asset => {
    const symbol = asset.name;
    const priceData = currentPrices[symbol];
    
    if (priceData && priceData.price) {
      // Calcular valor del activo en USD
      const assetValueUSD = priceData.price * asset.units;
      totalPortfolioValue += assetValueUSD;
      
      // Verificar si el activo tiene un beta válido
      const betaUndefined = priceData.beta === undefined;
      const betaIsNull = priceData.beta === null;
      const isExcludedType = asset.assetType && assetTypesWithoutBeta.some(type => 
        asset.assetType.toLowerCase().includes(type.toLowerCase())
      );
      
      // Si el activo no tiene beta válido o es de un tipo excluido, omitirlo del cálculo
      if (betaUndefined || betaIsNull || isExcludedType) {
        excludedAssets.push({
          symbol,
          value: assetValueUSD,
          type: asset.assetType || 'unknown',
          reason: betaUndefined || betaIsNull ? 'no_beta' : 'excluded_type'
        });
        return;
      }
      
      // Activo tiene beta válido, incluirlo en el cálculo
      totalIncludedValue += assetValueUSD;
      
      // Agrupar por símbolo
      if (!symbolGroups[symbol]) {
        symbolGroups[symbol] = {
          symbol,
          beta: priceData.beta,
          totalValue: 0,
          totalUnits: 0,
          assetType: asset.assetType || 'unknown'
        };
      }
      
      // Acumular valor y unidades
      symbolGroups[symbol].totalValue += assetValueUSD;
      symbolGroups[symbol].totalUnits += asset.units;
    }
  });
  
  // Calcular porcentaje del portafolio incluido
  const portfolioCoverage = totalPortfolioValue > 0 
    ? (totalIncludedValue / totalPortfolioValue) * 100 
    : 0;
  
  // Si no hay valor incluido, no podemos calcular beta
  if (totalIncludedValue === 0) {
    return {
      portfolioBeta: 1.0,  // Valor neutral por defecto
      weightedBetas: {},
      totalValue: totalPortfolioValue,
      includedValue: 0,
      portfolioCoverage: 0,
      excludedAssets
    };
  }
  
  // Calcular beta ponderado de cada grupo de símbolos
  let portfolioBeta = 0;
  const weightedBetas = {};
  
  for (const [symbol, groupData] of Object.entries(symbolGroups)) {
    // Calcular peso del símbolo en el portafolio incluido en el cálculo
    const weight = groupData.totalValue / totalIncludedValue;
    
    // Calcular beta ponderado
    const weightedBeta = weight * groupData.beta;
    
    // Guardar beta ponderado
    weightedBetas[symbol] = {
      symbol,
      weight,
      beta: groupData.beta,
      weightedBeta,
      value: groupData.totalValue,
      units: groupData.totalUnits,
      assetType: groupData.assetType
    };
    
    // Sumar al beta del portafolio
    portfolioBeta += weightedBeta;
  }
  
  return {
    portfolioBeta,
    weightedBetas,
    totalValue: totalPortfolioValue,
    includedValue: totalIncludedValue,
    portfolioCoverage,
    excludedAssets
  };
}

/**
 * Determina la categoría de riesgo basada en el beta
 * @param {number} beta - Beta del portafolio
 * @returns {string} - Categoría de riesgo
 */
function getRiskCategory(beta) {
  if (beta < 0.5) return "Muy bajo";
  if (beta < 0.8) return "Bajo";
  if (beta < 1.2) return "Moderado";
  if (beta < 1.5) return "Alto";
  return "Muy alto";
}

// Exportar para uso desde Cloud Functions
module.exports = { calculatePortfolioRisk };

// Si se ejecuta directamente
if (require.main === module) {
  // Si Firebase Admin no está inicializado, inicializarlo
  if (!admin.apps.length) {
    admin.initializeApp();
  }
  
  calculatePortfolioRisk()
    .then(() => {
      console.log('Proceso completado');
      process.exit(0);
    })
    .catch(error => {
      console.error('Error en el proceso principal:', error);
      process.exit(1);
    });
} 