/**
 * Attribution Module Index
 * 
 * Exporta todos los servicios de atribución del portafolio.
 * 
 * @module services/attribution
 */

const attributionService = require('./attributionService');
const contributionCalculator = require('./contributionCalculator');
const waterfallGenerator = require('./waterfallGenerator');
const summaryGenerator = require('./summaryGenerator');
const types = require('./types');

module.exports = {
  // Servicio principal
  ...attributionService,
  
  // Servicios individuales (para uso avanzado)
  contributionCalculator,
  waterfallGenerator,
  summaryGenerator,
  
  // Tipos y utilidades
  types,
  CONTRIBUTION_COLORS: types.CONTRIBUTION_COLORS,
  getContributionColor: types.getContributionColor,
  getPeriodLabel: types.getPeriodLabel,
  getPeriodStartDate: types.getPeriodStartDate
};
