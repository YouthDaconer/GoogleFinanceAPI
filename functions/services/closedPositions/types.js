/**
 * Closed Positions Types
 * 
 * Tipos y constantes para el sistema de posiciones cerradas del portafolio.
 * 
 * @module services/closedPositions/types
 * @see docs/stories/36.story.md
 */

/**
 * Posición cerrada procesada
 * @typedef {Object} ClosedPosition
 * @property {string} ticker - Símbolo del activo
 * @property {string} name - Nombre del activo
 * @property {string} type - Tipo de activo (stock, etf, crypto)
 * @property {string} portfolioAccountId - ID de la cuenta
 * @property {string} accountName - Nombre de la cuenta
 * @property {number} shares - Cantidad de acciones vendidas
 * @property {number} buyPrice - Precio promedio de compra
 * @property {number} sellPrice - Precio de venta
 * @property {number} buyValue - Valor total de compra
 * @property {number} sellValue - Valor total de venta
 * @property {number} realizedPnL - Ganancia/pérdida realizada
 * @property {number} realizedPnLPercent - P&L como porcentaje
 * @property {string} buyDate - Fecha de compra más antigua
 * @property {string} sellDate - Fecha de venta
 * @property {number} holdingPeriodDays - Días de holding
 * @property {string} currency - Moneda de la transacción
 */

/**
 * Resumen de posiciones cerradas
 * @typedef {Object} ClosedPositionsSummary
 * @property {number} totalRealizedPnL - P&L total realizado
 * @property {number} totalTrades - Número total de trades
 * @property {number} winningTrades - Trades ganadores
 * @property {number} losingTrades - Trades perdedores
 * @property {number} winRate - Tasa de acierto (0-100)
 * @property {number} avgHoldingPeriod - Período promedio de holding en días
 * @property {number} avgPnLPercent - P&L promedio por trade
 * @property {number} bestTrade - Mejor trade (P&L)
 * @property {number} worstTrade - Peor trade (P&L)
 * @property {AccountBreakdown[]} byAccount - Desglose por cuenta
 */

/**
 * Desglose por cuenta
 * @typedef {Object} AccountBreakdown
 * @property {string} accountId - ID de la cuenta
 * @property {string} accountName - Nombre de la cuenta
 * @property {number} trades - Número de trades
 * @property {number} pnl - P&L de la cuenta
 * @property {number} winRate - Tasa de acierto de la cuenta
 */

/**
 * Punto de P&L acumulado para gráfico
 * @typedef {Object} CumulativePnLDataPoint
 * @property {string} date - Fecha de la venta
 * @property {string} ticker - Ticker vendido
 * @property {number} pnl - P&L del trade
 * @property {number} cumulativePnL - P&L acumulado hasta esta fecha
 */

/**
 * Opciones para consulta de posiciones cerradas
 * @typedef {Object} ClosedPositionsOptions
 * @property {string[]} [accountIds] - IDs de cuentas a incluir (vacío = todas)
 * @property {string} [startDate] - Fecha inicio (ISO)
 * @property {string} [endDate] - Fecha fin (ISO)
 * @property {string[]} [tickers] - Filtrar por tickers específicos
 * @property {string} [currency] - Moneda para conversión (default: USD)
 * @property {number} [page] - Página (1-indexed)
 * @property {number} [pageSize] - Tamaño de página (default: 50)
 * @property {string} [sortBy] - Campo para ordenar (sellDate, realizedPnL, ticker)
 * @property {'asc' | 'desc'} [sortOrder] - Orden (default: desc)
 */

/**
 * Respuesta paginada de posiciones cerradas
 * @typedef {Object} ClosedPositionsResponse
 * @property {boolean} success - Si la consulta fue exitosa
 * @property {ClosedPosition[]} positions - Lista de posiciones
 * @property {ClosedPositionsSummary} summary - Resumen agregado
 * @property {CumulativePnLDataPoint[]} cumulativeData - Datos para gráfico
 * @property {PaginationInfo} pagination - Información de paginación
 */

/**
 * Información de paginación
 * @typedef {Object} PaginationInfo
 * @property {number} page - Página actual
 * @property {number} pageSize - Tamaño de página
 * @property {number} totalItems - Total de items
 * @property {number} totalPages - Total de páginas
 * @property {boolean} hasNext - Si hay página siguiente
 * @property {boolean} hasPrev - Si hay página anterior
 */

/** Configuración por defecto */
const DEFAULTS = {
  PAGE_SIZE: 50,
  MAX_PAGE_SIZE: 200,
  SORT_BY: 'sellDate',
  SORT_ORDER: 'desc',
  CURRENCY: 'USD'
};

module.exports = {
  DEFAULTS
};
