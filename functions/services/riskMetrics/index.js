/**
 * Risk Metrics Module Index
 * 
 * Exporta todos los servicios de métricas de riesgo del portafolio.
 * 
 * @module services/riskMetrics
 * @see docs/stories/36.story.md
 */

const types = require('./types');
const mathCalculations = require('./mathCalculations');
const benchmarkCache = require('./benchmarkCache');
const multiAccountAggregator = require('./multiAccountAggregator');
const riskMetricsService = require('./riskMetricsService');

module.exports = {
  // Tipos y constantes
  types,
  ...types,
  
  // Servicio principal
  ...riskMetricsService,
  
  // Servicios individuales (para uso avanzado)
  mathCalculations,
  benchmarkCache,
  multiAccountAggregator
};
