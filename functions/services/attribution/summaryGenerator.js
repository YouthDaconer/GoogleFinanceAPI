/**
 * Summary Generator Service
 * 
 * SRP: Responsabilidad única de generar resúmenes de atribución.
 * Calcula estadísticas agregadas del análisis de atribución.
 * 
 * @module services/attribution/summaryGenerator
 * @see docs/architecture/portfolio-attribution-coherence-analysis.md
 */

const { getPeriodLabel, getPeriodStartDate } = require('./types');

/**
 * Genera un resumen completo del análisis de atribución
 * 
 * @param {Object} contributionResult - Resultado de calculateContributions
 * @param {string} period - Período del análisis
 * @param {Object} options - Opciones adicionales
 * @param {number} options.benchmarkReturn - Retorno del benchmark para comparar
 * @returns {Object} AttributionSummary
 */
function generateSummary(contributionResult, period, options = {}) {
  const {
    attributions = [],
    totalPortfolioValue = 0,
    totalPortfolioInvestment = 0,
    portfolioReturn = 0,
    latestDate,
    periodStartDate: startDateStr
  } = contributionResult || {};
  
  const { benchmarkReturn = 0 } = options;
  
  // Asegurar que portfolioReturn sea un número válido
  const safePortfolioReturn = typeof portfolioReturn === 'number' && !isNaN(portfolioReturn) 
    ? portfolioReturn 
    : 0;
  
  // =========================================================================
  // TOP Y WORST CONTRIBUTORS
  // =========================================================================
  const sorted = [...attributions];
  const topContributor = sorted[0] || { ticker: 'N/A', contribution: 0 };
  const worstContributor = sorted[sorted.length - 1] || { ticker: 'N/A', contribution: 0 };
  
  // =========================================================================
  // ANÁLISIS POR SECTOR
  // =========================================================================
  const sectorContributions = new Map();
  for (const attr of attributions) {
    const sector = attr.sector || 'Unknown';
    const current = sectorContributions.get(sector) || { contribution: 0, count: 0 };
    current.contribution += attr.contribution;
    current.count += 1;
    sectorContributions.set(sector, current);
  }
  
  let bestSector = 'N/A';
  let worstSector = 'N/A';
  let maxSectorContrib = -Infinity;
  let minSectorContrib = Infinity;
  
  for (const [sector, data] of sectorContributions) {
    if (data.contribution > maxSectorContrib) {
      maxSectorContrib = data.contribution;
      bestSector = sector;
    }
    if (data.contribution < minSectorContrib) {
      minSectorContrib = data.contribution;
      worstSector = sector;
    }
  }
  
  // =========================================================================
  // CONTEO DE CONTRIBUYENTES
  // =========================================================================
  const positiveContributors = attributions.filter(a => a.contribution > 0).length;
  const negativeContributors = attributions.filter(a => a.contribution < 0).length;
  const neutralContributors = attributions.filter(a => a.contribution === 0).length;
  
  // =========================================================================
  // CÁLCULO DE ALPHA
  // =========================================================================
  const alpha = safePortfolioReturn - benchmarkReturn;
  const beatingBenchmark = safePortfolioReturn > benchmarkReturn;
  
  // =========================================================================
  // VALOR AL INICIO DEL PERÍODO
  // =========================================================================
  // Calcular valor inicial basado en el retorno
  const portfolioValueStart = safePortfolioReturn !== 0 
    ? totalPortfolioValue / (1 + safePortfolioReturn / 100)
    : totalPortfolioValue;
  const portfolioReturnAbsolute = totalPortfolioValue - portfolioValueStart;
  
  // =========================================================================
  // ANÁLISIS POR TIPO DE ACTIVO
  // =========================================================================
  const typeContributions = new Map();
  for (const attr of attributions) {
    const type = attr.type || 'stock';
    const current = typeContributions.get(type) || { contribution: 0, count: 0, value: 0 };
    current.contribution += attr.contribution;
    current.count += 1;
    current.value += attr.valueEnd || 0;
    typeContributions.set(type, current);
  }
  
  // Convertir a objeto para serialización
  const contributionByType = {};
  for (const [type, data] of typeContributions) {
    contributionByType[type] = {
      contribution: data.contribution,
      count: data.count,
      value: data.value,
      weight: totalPortfolioValue > 0 ? (data.value / totalPortfolioValue) * 100 : 0
    };
  }
  
  // =========================================================================
  // ESTADÍSTICAS ADICIONALES
  // =========================================================================
  const contributions = attributions.map(a => a.contribution);
  const avgContribution = contributions.length > 0 
    ? contributions.reduce((a, b) => a + b, 0) / contributions.length 
    : 0;
  
  const maxContribution = contributions.length > 0 ? Math.max(...contributions) : 0;
  const minContribution = contributions.length > 0 ? Math.min(...contributions) : 0;
  
  // Concentración: qué porcentaje del retorno viene de los top 5
  const top5Contribution = sorted.slice(0, 5).reduce((sum, a) => sum + a.contribution, 0);
  const concentrationRatio = safePortfolioReturn !== 0 
    ? (top5Contribution / safePortfolioReturn) * 100 
    : 0;
  
  return {
    // Fechas
    periodStart: startDateStr || getPeriodStartDate(period).toISOString().split('T')[0],
    periodEnd: latestDate || new Date().toISOString().split('T')[0],
    periodLabel: getPeriodLabel(period),
    
    // Retornos
    portfolioReturn: safePortfolioReturn,
    portfolioReturnAbsolute,
    benchmarkReturn,
    alpha,
    beatingBenchmark,
    
    // Top/Worst
    topContributor: {
      ticker: topContributor.ticker,
      contribution: topContributor.contribution,
      returnPercent: topContributor.returnPercent || 0
    },
    worstContributor: {
      ticker: worstContributor.ticker,
      contribution: worstContributor.contribution,
      returnPercent: worstContributor.returnPercent || 0
    },
    
    // Sectores
    bestSector,
    worstSector,
    sectorBreakdown: Object.fromEntries(sectorContributions),
    
    // Conteos
    totalAssets: attributions.length,
    positiveContributors,
    negativeContributors,
    neutralContributors,
    
    // Valores
    portfolioValueStart,
    portfolioValueEnd: totalPortfolioValue,
    totalInvestment: totalPortfolioInvestment,
    
    // Breakdown por tipo
    contributionByType,
    
    // Estadísticas adicionales
    statistics: {
      avgContribution,
      maxContribution,
      minContribution,
      concentrationRatio, // % del retorno de top 5
      diversificationScore: 100 - Math.abs(concentrationRatio) // 100 = muy diversificado
    }
  };
}

/**
 * Genera un resumen compacto para respuestas ligeras
 * 
 * @param {Object} fullSummary - Resumen completo
 * @returns {Object} Resumen compacto
 */
function generateCompactSummary(fullSummary) {
  return {
    periodLabel: fullSummary.periodLabel,
    portfolioReturn: fullSummary.portfolioReturn,
    topContributor: fullSummary.topContributor,
    worstContributor: fullSummary.worstContributor,
    positiveContributors: fullSummary.positiveContributors,
    negativeContributors: fullSummary.negativeContributors,
    totalAssets: fullSummary.totalAssets
  };
}

module.exports = {
  generateSummary,
  generateCompactSummary
};
