/**
 * Utilidades compartidas para cálculos de períodos y extracción de datos
 * 
 * Este módulo extrae la lógica común entre TWR (historicalReturnsService.js)
 * y MWR (mwrCalculations.js) para cumplir con el principio DRY.
 * 
 * Historia 25: Implementación de Personal Return (MWR) + TWR Dual Metrics
 * 
 * @module periodCalculations
 * @see docs/stories/25.story.md
 */

const { DateTime } = require('luxon');

/**
 * Definición de períodos de análisis
 * Centraliza la configuración para evitar duplicación
 */
const PERIOD_DEFINITIONS = {
  ytd: { 
    name: 'YTD',
    getStartDate: (now) => now.startOf("year").toISODate()
  },
  oneMonth: { 
    name: '1M',
    getStartDate: (now) => now.minus({ months: 1 }).toISODate()
  },
  threeMonths: { 
    name: '3M',
    getStartDate: (now) => now.minus({ months: 3 }).toISODate()
  },
  sixMonths: { 
    name: '6M',
    getStartDate: (now) => now.minus({ months: 6 }).toISODate()
  },
  oneYear: { 
    name: '1Y',
    getStartDate: (now) => now.minus({ years: 1 }).toISODate()
  },
  twoYears: { 
    name: '2Y',
    getStartDate: (now) => now.minus({ years: 2 }).toISODate()
  },
  fiveYears: { 
    name: '5Y',
    getStartDate: (now) => now.minus({ years: 5 }).toISODate()
  }
};

/**
 * Umbrales mínimos de documentos por período para considerar datos suficientes
 */
const MIN_DOCS = {
  oneMonth: 21,
  threeMonths: 63,
  sixMonths: 126,
  ytd: 1,
  oneYear: 252,
  twoYears: 504,
  fiveYears: 1260
};

/**
 * Calcula días entre dos fechas ISO
 * 
 * @param {string} date1 - Fecha ISO (ej: "2025-01-01")
 * @param {string} date2 - Fecha ISO (ej: "2025-12-07")
 * @returns {number} Número de días entre las fechas
 */
function daysBetween(date1, date2) {
  const d1 = DateTime.fromISO(date1);
  const d2 = DateTime.fromISO(date2);
  return Math.abs(d2.diff(d1, 'days').days);
}

/**
 * Obtiene las fechas límite para cada período de análisis
 * 
 * @param {string} timezone - Zona horaria (default: "America/New_York")
 * @returns {Object} Objeto con fechas límite por período
 */
function getPeriodBoundaries(timezone = "America/New_York") {
  const now = DateTime.now().setZone(timezone);
  
  const boundaries = {
    now,
    todayISO: now.toISODate(),
    periods: {}
  };
  
  Object.entries(PERIOD_DEFINITIONS).forEach(([key, definition]) => {
    boundaries.periods[key] = {
      startDate: definition.getStartDate(now) || "",
      name: definition.name
    };
  });
  
  return boundaries;
}

/**
 * Ordena documentos de Firestore por fecha
 * Soporta tanto documentos crudos como snapshots de Firestore
 * 
 * @param {Array} docs - Documentos a ordenar
 * @returns {Array} Documentos ordenados por fecha ascendente
 */
function sortDocumentsByDate(docs) {
  return [...docs].sort((a, b) => {
    const dateA = a.data ? a.data().date : a.date;
    const dateB = b.data ? b.data().date : b.date;
    return dateA.localeCompare(dateB);
  });
}

/**
 * Extrae datos de un documento según la moneda y asset específico
 * 
 * @param {Object} doc - Documento de Firestore (snapshot o crudo)
 * @param {string} currency - Código de moneda (USD, COP, etc.)
 * @param {string|null} ticker - Ticker específico (opcional)
 * @param {string|null} assetType - Tipo de asset (opcional)
 * @returns {Object|null} Datos extraídos o null si no hay datos
 */
function extractDocumentData(doc, currency, ticker = null, assetType = null) {
  const data = doc.data ? doc.data() : doc;
  const currencyData = data[currency];
  
  if (!currencyData) return null;
  
  const result = {
    date: data.date,
    hasData: false
  };
  
  if (ticker && assetType) {
    // Datos de un asset específico
    const assetKey = `${ticker}_${assetType}`;
    const assetData = currencyData.assetPerformance?.[assetKey];
    
    if (assetData) {
      result.totalValue = assetData.totalValue || 0;
      result.totalInvestment = assetData.totalInvestment || 0;
      result.totalCashFlow = assetData.totalCashFlow || 0;
      result.doneProfitAndLoss = assetData.doneProfitAndLoss || 0;
      result.unrealizedProfitAndLoss = assetData.unrealizedProfitAndLoss || 0;
      result.adjustedDailyChangePercentage = assetData.adjustedDailyChangePercentage;
      result.dailyChangePercentage = assetData.dailyChangePercentage || 0;
      result.hasData = assetData.adjustedDailyChangePercentage !== undefined;
    }
  } else {
    // Datos del portafolio completo
    result.totalValue = currencyData.totalValue || 0;
    result.totalInvestment = currencyData.totalInvestment || 0;
    result.totalCashFlow = currencyData.totalCashFlow || 0;
    result.doneProfitAndLoss = currencyData.doneProfitAndLoss || 0;
    result.unrealizedProfitAndLoss = currencyData.unrealizedProfitAndLoss || 0;
    result.adjustedDailyChangePercentage = currencyData.adjustedDailyChangePercentage;
    result.dailyChangePercentage = currencyData.dailyChangePercentage || 0;
    result.hasData = currencyData.adjustedDailyChangePercentage !== undefined;
  }
  
  return result;
}

/**
 * Inicializa la estructura de períodos para acumular datos
 * 
 * @param {Object} periodBoundaries - Fechas límite de cada período
 * @param {Object} options - Opciones de inicialización
 * @param {boolean} options.includeTWR - Incluir campos para TWR
 * @param {boolean} options.includeMWR - Incluir campos para MWR
 * @returns {Object} Estructura de períodos inicializada
 */
function initializePeriods(periodBoundaries, options = { includeTWR: true, includeMWR: true }) {
  const periods = {};
  
  Object.entries(periodBoundaries.periods).forEach(([key, config]) => {
    periods[key] = {
      startDate: config.startDate,
      name: config.name,
      found: false,
      docsCount: 0,
      validDocsCount: 0
    };
    
    if (options.includeTWR) {
      periods[key].startFactor = 1;
    }
    
    if (options.includeMWR) {
      periods[key].startValue = null;
      periods[key].endValue = null;
      periods[key].cashFlows = [];
      periods[key].totalCashFlow = 0;
    }
  });
  
  return periods;
}

/**
 * Verifica si hay suficientes documentos para un período
 * 
 * @param {string} periodKey - Clave del período
 * @param {number} docsCount - Cantidad de documentos
 * @param {DateTime} now - Fecha actual (para cálculo dinámico de YTD)
 * @returns {boolean} true si hay suficientes documentos
 */
function hasEnoughDocuments(periodKey, docsCount, now) {
  if (periodKey === 'ytd') {
    // YTD tiene mínimo dinámico basado en el mes actual
    const currentMonth = now.month;
    const minDocsForYtd = Math.max(Math.ceil(currentMonth * 4 / 12), MIN_DOCS.ytd);
    return docsCount >= minDocsForYtd;
  }
  
  return docsCount >= (MIN_DOCS[periodKey] || 1);
}

/**
 * Normaliza la clave de período para la API de respuesta
 * Convierte de camelCase a formato de API consistente
 * 
 * @param {string} periodKey - Clave del período (ej: "threeMonths")
 * @param {string} suffix - Sufijo a agregar (ej: "Return", "PersonalReturn")
 * @returns {string} Clave normalizada (ej: "threeMonthReturn")
 */
function normalizeApiKey(periodKey, suffix) {
  // Mapeo de nombres internos a nombres de API
  const keyMap = {
    'ytd': 'ytd',
    'oneMonth': 'oneMonth',
    'threeMonths': 'threeMonth',
    'sixMonths': 'sixMonth',
    'oneYear': 'oneYear',
    'twoYears': 'twoYear',
    'fiveYears': 'fiveYear'
  };
  
  return `${keyMap[periodKey] || periodKey}${suffix}`;
}

/**
 * Capitaliza la primera letra de una cadena
 * 
 * @param {string} str - Cadena a capitalizar
 * @returns {string} Cadena con primera letra en mayúscula
 */
function capitalize(str) {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

module.exports = {
  // Constantes
  PERIOD_DEFINITIONS,
  MIN_DOCS,
  
  // Funciones de utilidad
  daysBetween,
  getPeriodBoundaries,
  sortDocumentsByDate,
  extractDocumentData,
  initializePeriods,
  hasEnoughDocuments,
  normalizeApiKey,
  capitalize
};
