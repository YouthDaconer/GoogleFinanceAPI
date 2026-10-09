const admin = require('firebase-admin');
const { DateTime } = require('luxon');
// OPT-DEMAND-CLEANUP: Importar helper para obtener precios y currencies del API Lambda
const { getPricesFromApi, getCurrencyRatesFromApi } = require('./marketDataHelper');

// 🚀 OPTIMIZACIÓN: Control de logging para reducir costos
const LOG_LEVEL = process.env.LOG_LEVEL || 'INFO'; // 'DEBUG', 'INFO', 'WARN', 'ERROR'
const ENABLE_DETAILED_LOGS = process.env.ENABLE_DETAILED_LOGS === 'true';

function logDebug(...args) {
  if (LOG_LEVEL === 'DEBUG') console.log(...args);
}

function logInfo(...args) {
  if (['DEBUG', 'INFO'].includes(LOG_LEVEL)) console.log(...args);
}

function logWarn(...args) {
  if (['DEBUG', 'INFO', 'WARN'].includes(LOG_LEVEL)) console.warn(...args);
}

function logError(...args) {
  console.error(...args); // Siempre loguear errores
}

/**
 * Calcula el riesgo del portafolio basado en el beta de los activos.
 * 
 * OPT-SNAP-INCR Fase 2: Acepta datos inyectados desde el pipeline EOD para
 * evitar re-leer Firestore y re-llamar al API Lambda.
 * Si no recibe datos (ejecución standalone/debugging), hace self-fetch.
 * 
 * @param {Object|null} injectedData - Datos pre-cargados del pipeline (null = self-fetch)
 * @param {Array} injectedData.allAssets - Assets activos [{id, name, units, portfolioAccount, assetType, ...}]
 * @param {Object} injectedData.userPortfolios - Mapa {userId: [{id, userId, isActive, ...}]}
 * @param {string[]} injectedData.userIds - IDs de usuarios a procesar
 * @param {Object} injectedData.currentPricesMap - Mapa {symbol: {beta, price}}
 * @param {Array} injectedData.currencies - Array de currencies para conversión
 * @param {string} injectedData.calculationDate - Fecha ISO del trading day (YYYY-MM-DD)
 * @returns {Promise<null>}
 */
async function calculatePortfolioRisk(injectedData = null) {
  const db = admin.firestore();
  
  let allAssets, userPortfolios, userIds, currentPricesMap, currencies, calculationDate;
  
  if (injectedData) {
    // Path optimizado: datos inyectados desde EOD pipeline (0 reads, 0 HTTP)
    // Guard: validar campos críticos para evitar silent failures (beta=1.0 para todos)
    if (!injectedData.currentPricesMap || !injectedData.allAssets || !injectedData.userIds) {
      logError('❌ [INJECTED] injectedData incompleto — fallback a standalone', {
        hasCurrentPricesMap: !!injectedData.currentPricesMap,
        hasAllAssets: !!injectedData.allAssets,
        hasUserIds: !!injectedData.userIds
      });
      ({ allAssets, userPortfolios, userIds, currentPricesMap, currencies, calculationDate } = await fetchRiskDataFromSources(db));
    } else {
      ({ allAssets, userPortfolios, userIds, currentPricesMap, currencies, calculationDate } = injectedData);
      logInfo(`📊 [INJECTED] Iniciando cálculo de riesgo - ${userIds.length} usuarios, datos pre-cargados`);
    }
  } else {
    // Path standalone: self-fetch para debugging/ejecución manual
    // NOTA: calculationDate será "hoy" (DateTime.now()), no "yesterday" como en el pipeline EOD.
    // Esto es intencional — standalone se usa para debugging adhoc, no para el ciclo EOD.
    ({ allAssets, userPortfolios, userIds, currentPricesMap, currencies, calculationDate } = await fetchRiskDataFromSources(db));
  }
  
  try {
    let processedUsers = 0;
    let usersWithData = 0;
    let totalAccounts = 0;
    let totalAssetsProcessed = 0;
    
    for (const userId of userIds) {
      logDebug(`Calculando riesgo para usuario: ${userId}`);
      
      const accounts = (userPortfolios[userId] || []).filter(a => a.isActive !== false);
      
      if (accounts.length === 0) {
        logDebug(`No hay cuentas activas para el usuario ${userId}`);
        processedUsers++;
        continue;
      }
      
      const accountIds = accounts.map(a => a.id);
      const userAssets = allAssets.filter(a => accountIds.includes(a.portfolioAccount));
      
      if (userAssets.length === 0) {
        logDebug(`No hay activos activos para el usuario ${userId}`);
        processedUsers++;
        continue;
      }
      
      const assetsByAccount = {};
      accounts.forEach(account => {
        assetsByAccount[account.id] = userAssets.filter(asset => asset.portfolioAccount === account.id);
      });
      
      const batch = db.batch();
      const userMetricsRef = db.collection('portfolioMetrics').doc(userId);
      const riskMetricsRef = userMetricsRef.collection('riskMetrics').doc('latest');
      
      const overallRisk = calculateBetaForAssets(userAssets, currentPricesMap, currencies);
      
      batch.set(riskMetricsRef, {
        calculationDate,
        metrics: {
          portfolioBeta: overallRisk.portfolioBeta,
          riskCategory: getRiskCategory(overallRisk.portfolioBeta),
          assetCount: userAssets.length,
          weightedBetas: overallRisk.weightedBetas,
          totalValue: overallRisk.totalValue,
          includedValue: overallRisk.includedValue,
          portfolioCoverage: overallRisk.portfolioCoverage,
          excludedAssetCount: overallRisk.excludedAssets?.length || 0
        }
      });
      
      for (const [accountId, accountAssets] of Object.entries(assetsByAccount)) {
        if (accountAssets.length === 0) continue;
        
        const accountRisk = calculateBetaForAssets(accountAssets, currentPricesMap, currencies);
        const accountRiskMetricsRef = userMetricsRef
          .collection('accounts').doc(accountId)
          .collection('riskMetrics').doc('latest');
        
        batch.set(accountRiskMetricsRef, {
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
        });
      }
      
      await batch.commit();
      
      processedUsers++;
      usersWithData++;
      totalAccounts += accounts.length;
      totalAssetsProcessed += userAssets.length;
      
      if (ENABLE_DETAILED_LOGS) {
        logDebug(`Datos de riesgo calculados para usuario ${userId} (${accounts.length} cuentas, ${userAssets.length} activos)`);
      }
    }
    
    logInfo(`✅ Cálculo de riesgo completado: ${usersWithData}/${processedUsers} usuarios procesados, ${totalAccounts} cuentas, ${totalAssetsProcessed} activos`);
    return null;
  } catch (error) {
    logError('❌ Error al calcular riesgo del portafolio:', error);
    return null;
  }
}

/**
 * Self-fetch de datos para ejecución standalone (debugging/manual).
 * Aísla la adquisición de datos del core logic.
 * 
 * @param {FirebaseFirestore.Firestore} db
 * @returns {Promise<Object>} Datos en el mismo formato que injectedData
 */
async function fetchRiskDataFromSources(db) {
  const calculationDate = DateTime.now().setZone('America/New_York').toISODate();
  
  const [perfSnap, assetsSnap, accountsSnap] = await Promise.all([
    db.collection('portfolioPerformance').get(),
    db.collection('assets').where('isActive', '==', true).get(),
    db.collection('portfolioAccounts').where('isActive', '==', true).get()
  ]);
  
  const userIds = perfSnap.docs.map(doc => doc.id);
  const allAssets = assetsSnap.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  
  const accounts = accountsSnap.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const userPortfolios = accounts.reduce((acc, a) => {
    if (!acc[a.userId]) acc[a.userId] = [];
    acc[a.userId].push(a);
    return acc;
  }, {});
  
  const symbols = [...new Set(allAssets.map(a => a.name).filter(Boolean))];
  const [pricesArray, currenciesArr] = await Promise.all([
    getPricesFromApi(symbols),
    getCurrencyRatesFromApi()
  ]);
  
  const currentPricesMap = {};
  pricesArray.forEach(q => {
    currentPricesMap[q.symbol] = { beta: q.beta ?? 1.0, price: q.price || 0 };
  });
  
  logInfo(`📊 [STANDALONE] Iniciando cálculo de riesgo - ${userIds.length} usuarios, ${symbols.length} símbolos del API Lambda`);
  
  return { allAssets, userPortfolios, userIds, currentPricesMap, currencies: currenciesArr, calculationDate };
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
      logInfo('✅ Proceso completado');
      process.exit(0);
    })
    .catch(error => {
      logError('❌ Error en el proceso principal:', error);
      process.exit(1);
    });
} 