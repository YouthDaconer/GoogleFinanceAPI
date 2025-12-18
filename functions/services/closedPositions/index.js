/**
 * Closed Positions Module Index
 * 
 * Exporta todos los servicios de posiciones cerradas del portafolio.
 * 
 * @module services/closedPositions
 * @see docs/stories/36.story.md
 */

const types = require('./types');
const transactionProcessor = require('./transactionProcessor');
const summaryCalculator = require('./summaryCalculator');
const closedPositionsService = require('./closedPositionsService');

module.exports = {
  // Tipos y constantes
  types,
  ...types,
  
  // Servicio principal
  ...closedPositionsService,
  
  // Servicios individuales
  transactionProcessor,
  summaryCalculator
};
