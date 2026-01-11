/**
 * STORY-036: Date Utilities Module (Backend)
 * 
 * Módulo centralizado para funciones de fecha relacionadas con períodos.
 * Compatible con la versión frontend en lib/dateUtils.ts
 * 
 * @module utils/dateUtils
 * @see docs/stories/36.story.md
 */

/**
 * Días de trading por año (estándar del mercado)
 */
const TRADING_DAYS_PER_YEAR = 252;

/**
 * Períodos válidos
 */
const VALID_PERIODS = ['YTD', '1M', '3M', '6M', '1Y', '2Y', 'ALL'];

/**
 * Calcula la fecha de inicio según el período
 * 
 * @param {string} period - Período de análisis (YTD, 1M, 3M, 6M, 1Y, 2Y, ALL)
 * @returns {Date} Fecha de inicio
 */
function getPeriodStartDate(period) {
  const now = new Date();
  
  switch (period) {
    case '1M':
      return new Date(now.getFullYear(), now.getMonth() - 1, now.getDate());
    case '3M':
      return new Date(now.getFullYear(), now.getMonth() - 3, now.getDate());
    case '6M':
      return new Date(now.getFullYear(), now.getMonth() - 6, now.getDate());
    case 'YTD':
      return new Date(now.getFullYear(), 0, 1);
    case '1Y':
      return new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
    case '2Y':
      return new Date(now.getFullYear() - 2, now.getMonth(), now.getDate());
    case 'ALL':
      return new Date(2000, 0, 1);
    default:
      return new Date(now.getFullYear(), 0, 1); // Default: YTD
  }
}

/**
 * Formatea una fecha a formato ISO (YYYY-MM-DD)
 * 
 * @param {Date} date - Fecha a formatear
 * @returns {string} String en formato YYYY-MM-DD
 */
function formatDateToISO(date) {
  return date.toISOString().split('T')[0];
}

/**
 * Obtiene el rango de fechas (inicio y fin) para un período
 * 
 * @param {string} period - Período de análisis
 * @returns {{start: Date, end: Date}} Objeto con fechas start y end
 */
function getDateRangeForPeriod(period) {
  return {
    start: getPeriodStartDate(period),
    end: new Date()
  };
}

/**
 * Obtiene etiqueta legible para un período
 * 
 * @param {string} period - Período
 * @param {string} [locale='es'] - Locale para traducción
 * @returns {string} Etiqueta legible
 */
function getPeriodLabel(period, locale = 'es') {
  const labels = {
    'YTD': { es: 'Año a la fecha', en: 'Year to Date' },
    '1M': { es: 'Último mes', en: 'Last Month' },
    '3M': { es: 'Últimos 3 meses', en: 'Last 3 Months' },
    '6M': { es: 'Últimos 6 meses', en: 'Last 6 Months' },
    '1Y': { es: 'Último año', en: 'Last Year' },
    '2Y': { es: 'Últimos 2 años', en: 'Last 2 Years' },
    'ALL': { es: 'Todo el historial', en: 'All Time' }
  };
  
  const lang = locale.startsWith('en') ? 'en' : 'es';
  return labels[period]?.[lang] || period;
}

/**
 * Calcula la diferencia en días entre dos fechas
 * 
 * @param {Date} startDate - Fecha inicio
 * @param {Date} endDate - Fecha fin
 * @returns {number} Número de días
 */
function daysBetween(startDate, endDate) {
  const msPerDay = 1000 * 60 * 60 * 24;
  return Math.floor((endDate.getTime() - startDate.getTime()) / msPerDay);
}

/**
 * Convierte días de calendario a días de trading aproximados
 * 
 * @param {number} calendarDays - Días de calendario
 * @returns {number} Días de trading estimados
 */
function calendarToTradingDays(calendarDays) {
  // Aproximación: 5 días de trading por 7 días de calendario
  return Math.floor(calendarDays * (5 / 7));
}

/**
 * Parsea una fecha string como fecha local (evita problemas de timezone)
 * 
 * @param {string} dateString - Fecha en formato YYYY-MM-DD
 * @returns {Date} Fecha parseada como local
 */
function parseAsLocalDate(dateString) {
  if (dateString.includes('T')) {
    return new Date(dateString);
  }
  return new Date(dateString + 'T12:00:00');
}

/**
 * Valida si un período es válido
 * 
 * @param {string} period - String a validar
 * @returns {boolean} true si es un período válido
 */
function isValidPeriod(period) {
  return VALID_PERIODS.includes(period);
}

module.exports = {
  TRADING_DAYS_PER_YEAR,
  VALID_PERIODS,
  getPeriodStartDate,
  formatDateToISO,
  getDateRangeForPeriod,
  getPeriodLabel,
  daysBetween,
  calendarToTradingDays,
  parseAsLocalDate,
  isValidPeriod
};
