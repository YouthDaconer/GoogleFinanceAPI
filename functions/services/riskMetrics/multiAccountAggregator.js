/**
 * Multi-Account Aggregator
 * 
 * Implementa la lógica de agregación de métricas de riesgo
 * cuando se seleccionan múltiples cuentas.
 * 
 * Replica la lógica del frontend:
 * - useMultiAccountHistoricalReturns.ts
 * - useDashboardAccountSelection (store)
 * 
 * @module services/riskMetrics/multiAccountAggregator
 * @see docs/stories/36.story.md
 */

const admin = require('../firebaseAdmin');
const db = admin.firestore();

/**
 * Determina la estrategia de fetch según los accountIds
 * 
 * @param {string[]} accountIds - Lista de IDs de cuenta
 * @returns {'overall' | 'single' | 'multi'} Estrategia a usar
 */
function determineStrategy(accountIds) {
  if (!accountIds || accountIds.length === 0) return 'overall';
  if (accountIds.includes('overall')) return 'overall';
  if (accountIds.length === 1) return 'single';
  return 'multi';
}

/**
 * Divide un array en chunks de tamaño máximo
 * Útil para queries Firestore 'in' que tienen límite de 10
 * 
 * @param {Array} array - Array a dividir
 * @param {number} size - Tamaño máximo de cada chunk
 * @returns {Array<Array>} Array de chunks
 */
function chunkArray(array, size = 10) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

/**
 * Obtiene datos de performance de una cuenta específica
 * 
 * @param {string} userId - ID del usuario
 * @param {string} accountId - ID de la cuenta
 * @param {string} startDate - Fecha inicio
 * @param {string} endDate - Fecha fin
 * @param {string} currency - Moneda
 * @returns {Promise<Object|null>} Datos de la cuenta o null si no hay datos
 */
async function fetchAccountPerformanceData(userId, accountId, startDate, endDate, currency) {
  try {
    const performancePath = `portfolioPerformance/${userId}/accounts/${accountId}/dates`;
    const performanceRef = db.collection(performancePath);
    
    const snapshot = await performanceRef
      .where('date', '>=', startDate)
      .where('date', '<=', endDate)
      .orderBy('date', 'asc')
      .get();
    
    if (snapshot.empty) {
      console.log(`[multiAccountAggregator] No data for account ${accountId}`);
      return null;
    }
    
    const dailyData = [];
    let currentValue = 0;
    
    snapshot.docs.forEach(doc => {
      const data = doc.data();
      const currencyData = data[currency] || data.USD || {};
      
      if (currencyData.totalValue !== undefined) {
        currentValue = currencyData.totalValue;
        dailyData.push({
          date: doc.id,
          return: (currencyData.adjustedDailyChangePercentage ?? currencyData.dailyChangePercentage ?? 0) / 100,
          value: currencyData.totalValue || 0
        });
      }
    });
    
    return {
      accountId,
      currentValue,
      dailyData,
      dataPoints: dailyData.length
    };
  } catch (error) {
    console.error(`[multiAccountAggregator] Error fetching account ${accountId}:`, error);
    return null;
  }
}

/**
 * Obtiene datos de performance agregados (overall)
 * 
 * @param {string} userId - ID del usuario
 * @param {string} startDate - Fecha inicio
 * @param {string} endDate - Fecha fin
 * @param {string} currency - Moneda
 * @returns {Promise<Object|null>} Datos agregados
 */
async function fetchOverallPerformanceData(userId, startDate, endDate, currency) {
  try {
    const performancePath = `portfolioPerformance/${userId}/dates`;
    const performanceRef = db.collection(performancePath);
    
    const snapshot = await performanceRef
      .where('date', '>=', startDate)
      .where('date', '<=', endDate)
      .orderBy('date', 'asc')
      .get();
    
    if (snapshot.empty) {
      console.log('[multiAccountAggregator] No overall data found');
      return null;
    }
    
    const dailyData = [];
    let currentValue = 0;
    
    snapshot.docs.forEach(doc => {
      const data = doc.data();
      const currencyData = data[currency] || data.USD || {};
      
      if (currencyData.totalValue !== undefined) {
        currentValue = currencyData.totalValue;
        dailyData.push({
          date: doc.id,
          return: (currencyData.adjustedDailyChangePercentage ?? currencyData.dailyChangePercentage ?? 0) / 100,
          value: currencyData.totalValue || 0
        });
      }
    });
    
    return {
      accountId: 'overall',
      currentValue,
      dailyData,
      dataPoints: dailyData.length
    };
  } catch (error) {
    console.error('[multiAccountAggregator] Error fetching overall data:', error);
    return null;
  }
}

/**
 * Consolida retornos diarios de múltiples cuentas
 * Agrupa por fecha y calcula el retorno ponderado por valor
 * 
 * @param {Array<Object>} accountsData - Datos de cada cuenta
 * @returns {Array<{date: string, return: number, value: number}>}
 */
function consolidateDailyReturns(accountsData) {
  const byDate = new Map();
  
  for (const account of accountsData) {
    if (!account || !account.dailyData) continue;
    
    for (const day of account.dailyData) {
      const existing = byDate.get(day.date) || { 
        totalValue: 0, 
        weightedReturn: 0 
      };
      
      existing.totalValue += day.value;
      existing.weightedReturn += day.return * day.value;
      byDate.set(day.date, existing);
    }
  }
  
  const consolidated = [];
  for (const [date, data] of byDate) {
    if (data.totalValue > 0) {
      consolidated.push({
        date,
        return: data.weightedReturn / data.totalValue,
        value: data.totalValue
      });
    }
  }
  
  return consolidated.sort((a, b) => a.date.localeCompare(b.date));
}

/**
 * Obtiene y agrega datos de performance para múltiples cuentas
 * 
 * @param {string} userId - ID del usuario
 * @param {string[]} accountIds - IDs de las cuentas
 * @param {string} startDate - Fecha inicio
 * @param {string} endDate - Fecha fin
 * @param {string} currency - Moneda
 * @returns {Promise<Object>} Datos consolidados con metadata
 */
async function aggregateMultiAccountData(userId, accountIds, startDate, endDate, currency) {
  const strategy = determineStrategy(accountIds);
  
  if (strategy === 'overall') {
    const overallData = await fetchOverallPerformanceData(userId, startDate, endDate, currency);
    
    if (!overallData) {
      throw new Error('INSUFFICIENT_DATA: No overall data found');
    }
    
    return {
      strategy: 'overall',
      accountsProcessed: 1,
      accountsRequested: 0,
      dailyReturns: overallData.dailyData.map(d => d.return),
      dailyData: overallData.dailyData,
      totalValue: overallData.currentValue,
      metadata: {
        accountsIncluded: ['overall'],
        aggregationMethod: 'pre-aggregated'
      }
    };
  }
  
  if (strategy === 'single') {
    const singleData = await fetchAccountPerformanceData(
      userId, 
      accountIds[0], 
      startDate, 
      endDate, 
      currency
    );
    
    if (!singleData) {
      throw new Error(`INSUFFICIENT_DATA: No data for account ${accountIds[0]}`);
    }
    
    return {
      strategy: 'single',
      accountsProcessed: 1,
      accountsRequested: 1,
      dailyReturns: singleData.dailyData.map(d => d.return),
      dailyData: singleData.dailyData,
      totalValue: singleData.currentValue,
      metadata: {
        accountsIncluded: [accountIds[0]],
        aggregationMethod: 'single-account'
      }
    };
  }
  
  // strategy === 'multi'
  const batches = chunkArray(accountIds, 10);
  const allAccountsData = [];
  
  for (const batch of batches) {
    const batchResults = await Promise.all(
      batch.map(id => fetchAccountPerformanceData(userId, id, startDate, endDate, currency))
    );
    allAccountsData.push(...batchResults.filter(d => d !== null));
  }
  
  if (allAccountsData.length === 0) {
    throw new Error('INSUFFICIENT_DATA: No data found for selected accounts');
  }
  
  const consolidatedData = consolidateDailyReturns(allAccountsData);
  const totalValue = allAccountsData.reduce((sum, acc) => sum + acc.currentValue, 0);
  
  return {
    strategy: 'multi',
    accountsProcessed: allAccountsData.length,
    accountsRequested: accountIds.length,
    dailyReturns: consolidatedData.map(d => d.return),
    dailyData: consolidatedData,
    totalValue,
    metadata: {
      accountsIncluded: allAccountsData.map(a => a.accountId),
      aggregationMethod: 'value-weighted',
      accountsMissing: accountIds.filter(
        id => !allAccountsData.some(a => a.accountId === id)
      )
    }
  };
}

module.exports = {
  determineStrategy,
  chunkArray,
  fetchAccountPerformanceData,
  fetchOverallPerformanceData,
  consolidateDailyReturns,
  aggregateMultiAccountData
};
