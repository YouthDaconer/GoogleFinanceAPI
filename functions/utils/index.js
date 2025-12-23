/**
 * Utils Module Index
 * 
 * Exporta todas las utilidades del backend.
 * 
 * @module utils
 */

const dateUtils = require('./dateUtils');
const mwrCalculations = require('./mwrCalculations');
const periodCalculations = require('./periodCalculations');
const portfolioCalculations = require('./portfolioCalculations');
const logger = require('./logger');
const observability = require('./observability');

module.exports = {
  ...dateUtils,
  ...mwrCalculations,
  ...periodCalculations,
  ...portfolioCalculations,
  ...logger,
  ...observability,
  
  // También exportar como módulos
  dateUtils,
  mwrCalculations,
  periodCalculations,
  portfolioCalculations,
  logger,
  observability
};
