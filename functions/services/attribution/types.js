/**
 * Attribution Types
 * 
 * Tipos compartidos para el sistema de atribución de rendimiento del portafolio.
 * Estos tipos son la fuente de verdad para backend y frontend.
 * 
 * @module services/attribution/types
 * @see docs/architecture/portfolio-attribution-coherence-analysis.md
 */

/**
 * Períodos de atribución soportados
 * @typedef {'1M' | '3M' | '6M' | 'YTD' | '1Y' | '2Y' | 'ALL'} AttributionPeriod
 */

/**
 * Estado de un activo en el portafolio
 * @typedef {'active' | 'sold'} AssetStatus
 */

/**
 * Atribución individual de un activo
 * @typedef {Object} AssetAttribution
 * @property {string} assetKey - Clave única del activo (ticker_type)
 * @property {string} ticker - Símbolo del activo
 * @property {string} name - Nombre completo del activo
 * @property {string} sector - Sector del activo
 * @property {'stock' | 'etf' | 'crypto'} type - Tipo de activo
 * @property {'active' | 'sold'} status - Estado de la posición
 * @property {number} weightStart - Peso al inicio del período (0-1)
 * @property {number} weightEnd - Peso al final del período (0-1)
 * @property {number} weightAverage - Peso promedio durante el período
 * @property {number} returnPercent - Retorno del activo en el período (%)
 * @property {number} contribution - Contribución al retorno del portafolio (pp)
 * @property {number} contributionAbsolute - Contribución en valor absoluto (moneda)
 * @property {number} valueStart - Valor al inicio del período
 * @property {number} valueEnd - Valor al final del período
 * @property {number} valueChange - Cambio de valor durante el período
 * @property {boolean} [hasPartialSales] - Si hubo ventas parciales durante el período
 * @property {number} [partialSalesCount] - Número de ventas parciales
 * @property {number} [partialSalesPnL] - P&L de ventas parciales
 */

/**
 * Punto de datos para gráfico waterfall
 * @typedef {Object} WaterfallDataPoint
 * @property {string} name - Nombre a mostrar (ticker o label)
 * @property {number} value - Valor de la barra
 * @property {number} contributionPP - Contribución en puntos porcentuales
 * @property {'start' | 'end' | 'positive' | 'negative'} type - Tipo de barra
 * @property {number} runningTotal - Total acumulado hasta este punto
 * @property {string} color - Color de la barra
 * @property {number} invisible - Parte invisible (para barras flotantes)
 * @property {Array<{ticker: string, contributionPP: number, value: number}>} [groupedAssets] - Assets agrupados
 */

/**
 * Resumen de atribución del portafolio
 * @typedef {Object} AttributionSummary
 * @property {string} periodStart - Fecha de inicio del período (ISO)
 * @property {string} periodEnd - Fecha de fin del período (ISO)
 * @property {string} periodLabel - Label legible del período
 * @property {number} portfolioReturn - Retorno del portafolio (%)
 * @property {number} portfolioReturnAbsolute - Retorno absoluto (moneda)
 * @property {number} benchmarkReturn - Retorno del benchmark (%)
 * @property {number} alpha - Alpha vs benchmark (pp)
 * @property {boolean} beatingBenchmark - Si supera al benchmark
 * @property {{ticker: string, contribution: number}} topContributor - Mejor contribuyente
 * @property {{ticker: string, contribution: number}} worstContributor - Peor contribuyente
 * @property {string} bestSector - Mejor sector
 * @property {string} worstSector - Peor sector
 * @property {number} totalAssets - Total de activos analizados
 * @property {number} positiveContributors - Activos con contribución positiva
 * @property {number} negativeContributors - Activos con contribución negativa
 * @property {number} portfolioValueStart - Valor inicial del portafolio
 * @property {number} portfolioValueEnd - Valor final del portafolio
 */

/**
 * Respuesta completa del servicio de atribución
 * @typedef {Object} AttributionResponse
 * @property {AssetAttribution[]} assetAttributions - Atribuciones por activo
 * @property {WaterfallDataPoint[]} waterfallData - Datos para gráfico waterfall
 * @property {AttributionSummary} summary - Resumen de atribución
 * @property {Object} metadata - Metadatos de la respuesta
 * @property {string} metadata.calculatedAt - Timestamp de cálculo
 * @property {string} metadata.dataSource - Fuente de datos usada
 * @property {string} metadata.portfolioDate - Fecha del portafolio usado
 * @property {string} metadata.period - Período calculado
 * @property {string} metadata.currency - Moneda usada
 */

/**
 * Colores para contribuciones
 */
const CONTRIBUTION_COLORS = {
  strongPositive: '#22c55e',    // green-500
  positive: '#4ade80',          // green-400
  slightlyPositive: '#86efac',  // green-300
  neutral: '#9ca3af',           // gray-400
  slightlyNegative: '#fca5a5',  // red-300
  negative: '#f87171',          // red-400
  strongNegative: '#ef4444',    // red-500
  start: '#60a5fa',             // blue-400
  end: '#818cf8'                // indigo-400
};

/**
 * Obtiene el color basado en la contribución
 * @param {number} contribution - Contribución en pp
 * @returns {string} Color hex
 */
function getContributionColor(contribution) {
  if (contribution > 2) return CONTRIBUTION_COLORS.strongPositive;
  if (contribution > 0.5) return CONTRIBUTION_COLORS.positive;
  if (contribution > 0) return CONTRIBUTION_COLORS.slightlyPositive;
  if (contribution === 0) return CONTRIBUTION_COLORS.neutral;
  if (contribution > -0.5) return CONTRIBUTION_COLORS.slightlyNegative;
  if (contribution > -2) return CONTRIBUTION_COLORS.negative;
  return CONTRIBUTION_COLORS.strongNegative;
}

/**
 * Labels para períodos
 */
const PERIOD_LABELS = {
  '1M': '1 Mes',
  '3M': '3 Meses',
  '6M': '6 Meses',
  'YTD': 'Año actual (YTD)',
  '1Y': '1 Año',
  '2Y': '2 Años',
  'ALL': 'Todo'
};

/**
 * Obtiene el label del período
 * @param {string} period - Código del período
 * @returns {string} Label legible
 */
function getPeriodLabel(period) {
  return PERIOD_LABELS[period] || period;
}

/**
 * Obtiene la fecha de inicio del período
 * @param {string} period - Período a calcular
 * @returns {Date} Fecha de inicio
 */
function getPeriodStartDate(period) {
  const now = new Date();
  const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  
  switch (period) {
    case '1M':
      return new Date(new Date(startOfDay).setMonth(startOfDay.getMonth() - 1));
    case '3M':
      return new Date(new Date(startOfDay).setMonth(startOfDay.getMonth() - 3));
    case '6M':
      return new Date(new Date(startOfDay).setMonth(startOfDay.getMonth() - 6));
    case 'YTD':
      return new Date(now.getFullYear(), 0, 1);
    case '1Y':
      return new Date(new Date(startOfDay).setFullYear(startOfDay.getFullYear() - 1));
    case '2Y':
      return new Date(new Date(startOfDay).setFullYear(startOfDay.getFullYear() - 2));
    case 'ALL':
      return new Date(2020, 0, 1);
    default:
      return new Date(now.getFullYear(), 0, 1);
  }
}

module.exports = {
  CONTRIBUTION_COLORS,
  PERIOD_LABELS,
  getContributionColor,
  getPeriodLabel,
  getPeriodStartDate
};
