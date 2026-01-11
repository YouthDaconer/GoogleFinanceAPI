/**
 * Waterfall Generator Service
 * 
 * SRP: Responsabilidad única de generar datos para gráficos waterfall.
 * Transforma las atribuciones en puntos de datos visualizables.
 * 
 * @module services/attribution/waterfallGenerator
 * @see docs/architecture/portfolio-attribution-coherence-analysis.md
 */

const { CONTRIBUTION_COLORS, getContributionColor } = require('./types');

/**
 * Genera datos para un gráfico waterfall de atribución
 * 
 * El gráfico muestra cómo cada activo contribuye al cambio de valor del portafolio,
 * empezando desde el valor inicial y llegando al valor final.
 * 
 * @param {Array} attributions - Array de atribuciones ordenadas por contribución
 * @param {number} valueStart - Valor inicial del portafolio
 * @param {number} valueEnd - Valor final del portafolio
 * @param {Object} options - Opciones de configuración
 * @param {number} options.maxBars - Máximo de barras individuales (default: 8)
 * @param {number} options.minContributionThreshold - Mínima contribución para mostrar (default: 0.1)
 * @returns {Array} Array de WaterfallDataPoint
 */
function generateWaterfallData(attributions, valueStart, valueEnd, options = {}) {
  const {
    maxBars = 8,
    minContributionThreshold = 0.1
  } = options;
  
  const data = [];
  
  // =========================================================================
  // BARRA DE INICIO
  // =========================================================================
  data.push({
    name: 'Inicio',
    value: valueStart,
    contributionPP: 0,
    type: 'start',
    runningTotal: valueStart,
    color: CONTRIBUTION_COLORS.start,
    invisible: 0
  });
  
  // =========================================================================
  // SEPARAR POSITIVOS Y NEGATIVOS
  // =========================================================================
  const positives = attributions.filter(a => a.contribution > minContributionThreshold);
  const negatives = attributions.filter(a => a.contribution < -minContributionThreshold);
  
  // Calcular cuántas barras mostrar de cada tipo
  const maxPositives = Math.min(positives.length, Math.ceil((maxBars - 2) * 0.7));
  const maxNegatives = Math.min(negatives.length, Math.floor((maxBars - 2) * 0.3));
  
  let runningTotal = valueStart;
  
  // =========================================================================
  // BARRAS POSITIVAS (Top Contributors)
  // =========================================================================
  const topPositives = positives.slice(0, maxPositives);
  for (const attr of topPositives) {
    const invisible = runningTotal;
    const barValue = Math.abs(attr.contributionAbsolute);
    runningTotal += barValue;
    
    data.push({
      name: attr.ticker,
      value: barValue,
      contributionPP: attr.contribution,
      type: 'positive',
      runningTotal,
      color: getContributionColor(attr.contribution),
      invisible,
      // Metadata adicional
      ticker: attr.ticker,
      sector: attr.sector,
      returnPercent: attr.returnPercent
    });
  }
  
  // Agrupar otros positivos si hay más
  const otherPositives = positives.slice(maxPositives);
  if (otherPositives.length > 0) {
    const otherValue = otherPositives.reduce((sum, a) => sum + Math.abs(a.contributionAbsolute), 0);
    const otherPP = otherPositives.reduce((sum, a) => sum + a.contribution, 0);
    const invisible = runningTotal;
    runningTotal += otherValue;
    
    data.push({
      name: `+${otherPositives.length} más`,
      value: otherValue,
      contributionPP: otherPP,
      type: 'positive',
      runningTotal,
      color: CONTRIBUTION_COLORS.slightlyPositive,
      invisible,
      // Detalle de los assets agrupados
      groupedAssets: otherPositives.map(a => ({
        ticker: a.ticker,
        contributionPP: a.contribution,
        value: Math.abs(a.contributionAbsolute),
        returnPercent: a.returnPercent
      }))
    });
  }
  
  // =========================================================================
  // BARRAS NEGATIVAS (Bottom Contributors)
  // =========================================================================
  const topNegatives = negatives.slice(0, maxNegatives);
  for (const attr of topNegatives) {
    const contribution = Math.abs(attr.contributionAbsolute);
    const invisible = runningTotal - contribution;
    runningTotal -= contribution;
    
    data.push({
      name: attr.ticker,
      value: contribution,
      contributionPP: attr.contribution,
      type: 'negative',
      runningTotal,
      color: getContributionColor(attr.contribution),
      invisible,
      // Metadata adicional
      ticker: attr.ticker,
      sector: attr.sector,
      returnPercent: attr.returnPercent
    });
  }
  
  // Agrupar otros negativos si hay más
  const otherNegatives = negatives.slice(maxNegatives);
  if (otherNegatives.length > 0) {
    const otherValue = otherNegatives.reduce((sum, a) => sum + Math.abs(a.contributionAbsolute), 0);
    const otherPP = otherNegatives.reduce((sum, a) => sum + a.contribution, 0);
    const invisible = runningTotal - otherValue;
    runningTotal -= otherValue;
    
    data.push({
      name: `+${otherNegatives.length} más`,
      value: otherValue,
      contributionPP: otherPP,
      type: 'negative',
      runningTotal,
      color: CONTRIBUTION_COLORS.slightlyNegative,
      invisible,
      // Detalle de los assets agrupados
      groupedAssets: otherNegatives.map(a => ({
        ticker: a.ticker,
        contributionPP: a.contribution,
        value: Math.abs(a.contributionAbsolute),
        returnPercent: a.returnPercent
      }))
    });
  }
  
  // =========================================================================
  // BARRA FINAL
  // =========================================================================
  data.push({
    name: 'Final',
    value: valueEnd,
    contributionPP: 0,
    type: 'end',
    runningTotal: valueEnd,
    color: CONTRIBUTION_COLORS.end,
    invisible: 0
  });
  
  return data;
}

/**
 * Calcula el valor inicial basado en el retorno del portafolio
 * 
 * Formula: valorInicial = valorFinal / (1 + retorno/100)
 * 
 * @param {number} valueEnd - Valor final del portafolio
 * @param {number} returnPercent - Retorno en porcentaje
 * @returns {number} Valor inicial calculado
 */
function calculateValueStart(valueEnd, returnPercent) {
  if (returnPercent === -100) return 0; // Edge case: pérdida total
  return valueEnd / (1 + returnPercent / 100);
}

/**
 * Genera datos de waterfall a partir de contribuciones
 * 
 * @param {Object} contributionResult - Resultado de calculateContributions
 * @param {Object} options - Opciones de configuración
 * @returns {Array} Array de WaterfallDataPoint
 */
function generateWaterfallFromContributions(contributionResult, options = {}) {
  const { attributions, totalPortfolioValue, portfolioReturn } = contributionResult;
  
  // Calcular valor inicial basado en el retorno
  const valueStart = calculateValueStart(totalPortfolioValue, portfolioReturn);
  const valueEnd = totalPortfolioValue;
  
  return generateWaterfallData(attributions, valueStart, valueEnd, options);
}

module.exports = {
  generateWaterfallData,
  generateWaterfallFromContributions,
  calculateValueStart
};
