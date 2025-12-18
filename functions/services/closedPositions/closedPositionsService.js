/**
 * Closed Positions Service
 * 
 * Servicio principal que orquesta la obtención y procesamiento
 * de posiciones cerradas del portafolio.
 * 
 * @module services/closedPositions/closedPositionsService
 * @see docs/stories/36.story.md
 */

const { getClosedPositionsFromTransactions } = require('./transactionProcessor');
const { 
  calculateFullSummary, 
  calculateCumulativePnL,
  calculateTradeDistribution,
  calculateHoldingPeriodAnalysis 
} = require('./summaryCalculator');
const { DEFAULTS } = require('./types');

const admin = require('../firebaseAdmin');
const db = admin.firestore();

/**
 * Obtiene las cuentas del usuario
 * 
 * @param {string} userId - ID del usuario
 * @returns {Promise<Map>} Mapa de accountId -> nombre
 */
async function getUserAccounts(userId) {
  const accountNames = new Map();
  
  try {
    const accountsSnapshot = await db.collection('portfolioAccounts')
      .where('userId', '==', userId)
      .get();
    
    accountsSnapshot.docs.forEach(doc => {
      const data = doc.data();
      accountNames.set(doc.id, data.name || 'Sin nombre');
    });
  } catch (error) {
    console.error('[closedPositionsService] Error fetching accounts:', error);
  }
  
  return accountNames;
}

/**
 * Filtra posiciones según opciones
 * 
 * @param {Array} positions - Posiciones a filtrar
 * @param {Object} options - Opciones de filtrado
 * @returns {Array} Posiciones filtradas
 */
function filterPositions(positions, options) {
  let filtered = [...positions];
  
  // Filtrar por cuentas
  if (options.accountIds && options.accountIds.length > 0) {
    filtered = filtered.filter(p => 
      options.accountIds.includes(p.portfolioAccountId)
    );
  }
  
  // Filtrar por fecha inicio
  if (options.startDate) {
    const start = new Date(options.startDate);
    filtered = filtered.filter(p => 
      new Date(p.sellDate) >= start
    );
  }
  
  // Filtrar por fecha fin
  if (options.endDate) {
    const end = new Date(options.endDate);
    filtered = filtered.filter(p => 
      new Date(p.sellDate) <= end
    );
  }
  
  // Filtrar por tickers
  if (options.tickers && options.tickers.length > 0) {
    filtered = filtered.filter(p => 
      options.tickers.includes(p.ticker)
    );
  }
  
  return filtered;
}

/**
 * Ordena posiciones
 * 
 * @param {Array} positions - Posiciones a ordenar
 * @param {string} sortBy - Campo de ordenamiento
 * @param {string} sortOrder - 'asc' o 'desc'
 * @returns {Array} Posiciones ordenadas
 */
function sortPositions(positions, sortBy, sortOrder) {
  const sorted = [...positions];
  
  sorted.sort((a, b) => {
    let aVal = a[sortBy];
    let bVal = b[sortBy];
    
    // Manejar fechas
    if (sortBy === 'sellDate' || sortBy === 'buyDate') {
      aVal = new Date(aVal).getTime();
      bVal = new Date(bVal).getTime();
    }
    
    // Manejar strings
    if (typeof aVal === 'string') {
      aVal = aVal.toLowerCase();
      bVal = bVal.toLowerCase();
    }
    
    if (aVal < bVal) return sortOrder === 'asc' ? -1 : 1;
    if (aVal > bVal) return sortOrder === 'asc' ? 1 : -1;
    return 0;
  });
  
  return sorted;
}

/**
 * Pagina posiciones
 * 
 * @param {Array} positions - Posiciones a paginar
 * @param {number} page - Página (1-indexed)
 * @param {number} pageSize - Tamaño de página
 * @returns {{positions: Array, pagination: Object}}
 */
function paginatePositions(positions, page, pageSize) {
  const totalItems = positions.length;
  const totalPages = Math.ceil(totalItems / pageSize);
  const validPage = Math.max(1, Math.min(page, totalPages || 1));
  
  const startIndex = (validPage - 1) * pageSize;
  const paginatedPositions = positions.slice(startIndex, startIndex + pageSize);
  
  return {
    positions: paginatedPositions,
    pagination: {
      page: validPage,
      pageSize,
      totalItems,
      totalPages,
      hasNext: validPage < totalPages,
      hasPrev: validPage > 1
    }
  };
}

/**
 * Obtiene posiciones cerradas con filtros, ordenamiento y paginación
 * 
 * @param {string} userId - ID del usuario
 * @param {Object} options - Opciones de consulta
 * @returns {Promise<Object>} Resultado con posiciones, resumen y metadata
 */
async function getClosedPositions(userId, options = {}) {
  const {
    accountIds = [],
    startDate,
    endDate,
    tickers = [],
    currency = DEFAULTS.CURRENCY,
    page = 1,
    pageSize = DEFAULTS.PAGE_SIZE,
    sortBy = DEFAULTS.SORT_BY,
    sortOrder = DEFAULTS.SORT_ORDER,
    requestId = 'unknown'
  } = options;
  
  const startTime = Date.now();
  console.log(`[closedPositionsService] Starting`, {
    requestId,
    userId,
    accountIds: accountIds.length || 'all',
    page,
    pageSize
  });
  
  try {
    // 1. Obtener cuentas del usuario
    const accountNames = await getUserAccounts(userId);
    
    if (accountNames.size === 0) {
      return {
        success: false,
        error: 'NO_ACCOUNTS',
        message: 'No accounts found for user',
        metadata: { requestId, durationMs: Date.now() - startTime }
      };
    }
    
    // 2. Determinar qué cuentas consultar
    const targetAccountIds = accountIds.length > 0 
      ? accountIds.filter(id => accountNames.has(id))
      : Array.from(accountNames.keys());
    
    if (targetAccountIds.length === 0) {
      return {
        success: false,
        error: 'INVALID_ACCOUNTS',
        message: 'None of the specified accounts belong to user',
        metadata: { requestId, durationMs: Date.now() - startTime }
      };
    }
    
    // 3. Obtener posiciones cerradas
    const allPositions = await getClosedPositionsFromTransactions(
      userId, 
      targetAccountIds, 
      accountNames
    );
    
    if (allPositions.length === 0) {
      return {
        success: true,
        positions: [],
        summary: {
          totalRealizedPnL: 0,
          totalTrades: 0,
          winningTrades: 0,
          losingTrades: 0,
          winRate: 0,
          avgHoldingPeriod: 0,
          byAccount: []
        },
        cumulativeData: [],
        distribution: [],
        holdingAnalysis: {},
        pagination: {
          page: 1,
          pageSize,
          totalItems: 0,
          totalPages: 0,
          hasNext: false,
          hasPrev: false
        },
        metadata: {
          requestId,
          accountsQueried: targetAccountIds.length,
          durationMs: Date.now() - startTime
        }
      };
    }
    
    // 4. Aplicar filtros
    const filteredPositions = filterPositions(allPositions, {
      accountIds: accountIds.length > 0 ? accountIds : null,
      startDate,
      endDate,
      tickers
    });
    
    // 5. Calcular métricas sobre datos filtrados
    const summary = calculateFullSummary(filteredPositions);
    const cumulativeData = calculateCumulativePnL(filteredPositions);
    const distribution = calculateTradeDistribution(filteredPositions);
    const holdingAnalysis = calculateHoldingPeriodAnalysis(filteredPositions);
    
    // 6. Ordenar
    const sortedPositions = sortPositions(filteredPositions, sortBy, sortOrder);
    
    // 7. Paginar
    const { positions: paginatedPositions, pagination } = paginatePositions(
      sortedPositions, 
      page, 
      Math.min(pageSize, DEFAULTS.MAX_PAGE_SIZE)
    );
    
    // 8. Obtener tickers únicos para filtros
    const availableTickers = [...new Set(allPositions.map(p => p.ticker))].sort();
    
    const result = {
      success: true,
      positions: paginatedPositions,
      summary,
      cumulativeData,
      distribution,
      holdingAnalysis,
      availableTickers,
      pagination,
      metadata: {
        requestId,
        currency,
        accountsQueried: targetAccountIds.length,
        totalPositions: allPositions.length,
        filteredPositions: filteredPositions.length,
        durationMs: Date.now() - startTime
      }
    };
    
    console.log(`[closedPositionsService] Complete`, {
      requestId,
      positions: paginatedPositions.length,
      totalFiltered: filteredPositions.length,
      durationMs: result.metadata.durationMs
    });
    
    return result;
    
  } catch (error) {
    console.error(`[closedPositionsService] Error:`, {
      requestId,
      error: error.message,
      stack: error.stack
    });
    
    return {
      success: false,
      error: 'PROCESSING_ERROR',
      message: error.message,
      metadata: {
        requestId,
        durationMs: Date.now() - startTime
      }
    };
  }
}

module.exports = {
  getClosedPositions,
  getUserAccounts,
  filterPositions,
  sortPositions,
  paginatePositions
};
