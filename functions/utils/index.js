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

module.exports = {
  ...dateUtils,
  ...mwrCalculations,
  ...periodCalculations,
  ...portfolioCalculations,
  
  // También exportar como módulos
  dateUtils,
  mwrCalculations,
  periodCalculations,
  portfolioCalculations
};
