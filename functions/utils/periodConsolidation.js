/**
 * Utilidades para consolidación de períodos de rendimiento
 * 
 * COST-OPT-001: Pre-calcula y persiste "checkpoints" de rendimiento
 * para períodos cerrados (meses, años), reduciendo lecturas de Firestore
 * de ~1,825 docs a ~40 docs para consultas de 5 años.
 * 
 * Principios aplicados:
 * - SRP: Solo lógica de consolidación, sin I/O de Firestore
 * - DRY: Reutiliza periodCalculations.js
 * - KISS: Funciones pequeñas y composables
 * 
 * @module periodConsolidation
 * @see docs/stories/62.story.md (COST-OPT-001)
 * @see docs/architecture/cost-optimization-architectural-proposal.md
 */

const { DateTime } = require('luxon');
const { 
  sortDocumentsByDate, 
  extractDocumentData,
  PERIOD_DEFINITIONS 
} = require('./periodCalculations');

// ============================================================================
// CONSTANTES
// ============================================================================

/**
 * Versión del schema de documentos consolidados
 * Incrementar cuando cambie la estructura
 */
const CONSOLIDATED_SCHEMA_VERSION = 1;

/**
 * Campos que no son monedas en un documento consolidado
 */
const NON_CURRENCY_FIELDS = [
  'periodType', 'periodKey', 'startDate', 'endDate', 
  'docsCount', 'version', 'lastUpdated'
];

// ============================================================================
// FUNCIONES DE CONSOLIDACIÓN
// ============================================================================

/**
 * Consolida documentos diarios en un único documento de período
 * 
 * @param {Array} dailyDocs - Documentos diarios de Firestore (snapshots o crudos)
 * @param {string} periodKey - Clave del período (ej: "2024-12" o "2024")
 * @param {string} periodType - Tipo: 'month' o 'year'
 * @returns {Object|null} Documento consolidado listo para guardar, o null si no hay datos
 * 
 * @example
 * const consolidated = consolidatePeriod(dailyDocs, '2025-12', 'month');
 * // { periodType: 'month', periodKey: '2025-12', USD: { startFactor: 1, endFactor: 1.05, ... } }
 */
function consolidatePeriod(dailyDocs, periodKey, periodType) {
  if (!dailyDocs || dailyDocs.length === 0) {
    return null;
  }
  
  const sortedDocs = sortDocumentsByDate(dailyDocs);
  const firstDoc = sortedDocs[0];
  const lastDoc = sortedDocs[sortedDocs.length - 1];
  
  const firstData = firstDoc.data ? firstDoc.data() : firstDoc;
  const lastData = lastDoc.data ? lastDoc.data() : lastDoc;
  
  // Obtener todas las monedas disponibles en los documentos
  const currencies = extractCurrenciesFromDocs(sortedDocs);
  
  if (currencies.size === 0) {
    return null;
  }
  
  const consolidated = {
    periodType,
    periodKey,
    startDate: firstData.date,
    endDate: lastData.date,
    docsCount: sortedDocs.length,
    version: CONSOLIDATED_SCHEMA_VERSION,
    lastUpdated: new Date().toISOString()
  };
  
  // Procesar cada moneda
  currencies.forEach(currencyCode => {
    const currencyConsolidation = consolidateCurrencyData(sortedDocs, currencyCode);
    if (currencyConsolidation) {
      consolidated[currencyCode] = currencyConsolidation;
    }
  });
  
  return consolidated;
}

/**
 * Extrae todas las monedas disponibles de un conjunto de documentos
 * 
 * @param {Array} docs - Documentos a analizar
 * @returns {Set<string>} Set de códigos de moneda encontrados
 */
function extractCurrenciesFromDocs(docs) {
  const currencies = new Set();
  
  docs.forEach(doc => {
    const data = doc.data ? doc.data() : doc;
    Object.keys(data).forEach(key => {
      if (key !== 'date' && typeof data[key] === 'object' && data[key] !== null) {
        // Verificar que tiene campos de moneda (totalValue, etc.)
        if (data[key].totalValue !== undefined || data[key].adjustedDailyChangePercentage !== undefined) {
          currencies.add(key);
        }
      }
    });
  });
  
  return currencies;
}

/**
 * Consolida datos de una moneda específica desde documentos diarios
 * 
 * @param {Array} sortedDocs - Documentos ordenados por fecha
 * @param {string} currencyCode - Código de moneda (USD, COP, etc.)
 * @returns {Object|null} Datos consolidados para la moneda
 */
function consolidateCurrencyData(sortedDocs, currencyCode) {
  let startFactor = 1;
  let currentFactor = 1;
  let startTotalValue = 0;
  let startTotalInvestment = 0;
  let endTotalValue = 0;
  let endTotalInvestment = 0;
  let totalCashFlow = 0;
  let foundStart = false;
  let validDocsCount = 0;
  
  // Estructuras para MWR
  const cashFlows = [];
  
  // Estructuras para assets
  const assetFactors = {};
  
  sortedDocs.forEach((doc) => {
    const data = doc.data ? doc.data() : doc;
    const currencyData = data[currencyCode];
    
    if (!currencyData) return;
    
    // Guardar valores iniciales del primer documento con datos
    if (!foundStart && currencyData.totalValue !== undefined) {
      startTotalValue = currencyData.totalValue || 0;
      startTotalInvestment = currencyData.totalInvestment || 0;
      startFactor = 1; // Factor inicial siempre es 1 para el período
      foundStart = true;
    }
    
    // Actualizar valores finales (se sobrescriben con cada documento)
    if (currencyData.totalValue !== undefined) {
      endTotalValue = currencyData.totalValue;
      endTotalInvestment = currencyData.totalInvestment || 0;
    }
    
    // Acumular cashflow del día
    const dailyCashFlow = currencyData.totalCashFlow || 0;
    if (dailyCashFlow !== 0) {
      totalCashFlow += dailyCashFlow;
      cashFlows.push({ date: data.date, amount: dailyCashFlow });
    }
    
    // Actualizar factor compuesto (TWR)
    const adjustedDailyChange = currencyData.adjustedDailyChangePercentage;
    if (adjustedDailyChange !== undefined && adjustedDailyChange !== null) {
      currentFactor = currentFactor * (1 + adjustedDailyChange / 100);
      validDocsCount++;
    }
    
    // Procesar assets individuales
    if (currencyData.assetPerformance) {
      Object.entries(currencyData.assetPerformance).forEach(([assetKey, assetData]) => {
        if (!assetFactors[assetKey]) {
          assetFactors[assetKey] = {
            startFactor: 1,
            currentFactor: 1,
            startTotalValue: assetData.totalValue || 0,
            endTotalValue: 0,
            validDocsCount: 0
          };
        }
        
        const af = assetFactors[assetKey];
        af.endTotalValue = assetData.totalValue || 0;
        
        const assetChange = assetData.adjustedDailyChangePercentage;
        if (assetChange !== undefined && assetChange !== null) {
          af.currentFactor = af.currentFactor * (1 + assetChange / 100);
          af.validDocsCount++;
        }
      });
    }
  });
  
  // Si no encontramos datos válidos, retornar null
  if (!foundStart || validDocsCount === 0) {
    return null;
  }
  
  // Calcular rendimiento del período (TWR)
  const periodReturn = (currentFactor / startFactor - 1) * 100;
  
  // Calcular MWR (Personal Return) usando Modified Dietz simplificado
  const personalReturn = calculateModifiedDietzSimple(
    startTotalValue,
    endTotalValue,
    totalCashFlow,
    cashFlows,
    sortedDocs.length
  );
  
  // Construir resultado
  const result = {
    // Factores para encadenamiento (TWR)
    startFactor,
    endFactor: currentFactor,
    periodReturn,
    
    // Valores absolutos
    startTotalValue,
    endTotalValue,
    startTotalInvestment,
    endTotalInvestment,
    
    // Datos para MWR
    totalCashFlow,
    personalReturn,
    
    // Metadata
    validDocsCount,
    
    // Assets consolidados
    assetPerformance: {}
  };
  
  // Agregar assets consolidados
  Object.entries(assetFactors).forEach(([assetKey, af]) => {
    if (af.validDocsCount > 0) {
      result.assetPerformance[assetKey] = {
        startFactor: af.startFactor,
        endFactor: af.currentFactor,
        periodReturn: (af.currentFactor / af.startFactor - 1) * 100,
        startTotalValue: af.startTotalValue,
        endTotalValue: af.endTotalValue
      };
    }
  });
  
  return result;
}

/**
 * Calcula Modified Dietz Return simplificado para un período
 * 
 * @param {number} startValue - Valor al inicio del período
 * @param {number} endValue - Valor al final del período
 * @param {number} totalCashFlow - Suma total de cashflows (negativo = depósitos)
 * @param {Array} cashFlows - Array de { date, amount }
 * @param {number} totalDays - Días totales del período
 * @returns {number} Personal return en porcentaje
 */
function calculateModifiedDietzSimple(startValue, endValue, totalCashFlow, cashFlows, totalDays) {
  // Si no hay valor inicial ni cashflow, retornar 0
  if (startValue === 0 && totalCashFlow === 0) {
    return 0;
  }
  
  // netDeposits = -totalCashFlow porque cashflow negativo significa depósito
  const netDeposits = -totalCashFlow;
  
  // Modified Dietz simplificado: asume cashflows a mitad de período
  const investmentBase = startValue + (netDeposits / 2);
  
  if (investmentBase <= 0) {
    return 0;
  }
  
  const gain = endValue - startValue - netDeposits;
  return (gain / investmentBase) * 100;
}

// ============================================================================
// FUNCIONES DE ENCADENAMIENTO DE FACTORES
// ============================================================================

/**
 * Encadena factores de períodos consolidados + días actuales
 * para calcular rendimientos por período (YTD, 1Y, 5Y, etc.)
 * 
 * Esta función es el corazón de la optimización COST-OPT-001.
 * En lugar de leer ~1,825 documentos diarios, lee:
 * - ~5 documentos de años consolidados
 * - ~11 documentos de meses consolidados
 * - ~20 documentos diarios del mes actual
 * 
 * @param {Array} yearlyDocs - Documentos de años consolidados
 * @param {Array} monthlyDocs - Documentos de meses consolidados  
 * @param {Array} dailyDocs - Documentos diarios del mes actual
 * @param {string} currency - Código de moneda (USD, COP, etc.)
 * @param {string|null} ticker - Ticker específico (opcional)
 * @param {string|null} assetType - Tipo de asset (opcional)
 * @param {DateTime} now - Fecha actual para cálculo de límites de período
 * @returns {Object} Rendimientos calculados en formato compatible con V1
 */
function chainFactorsForPeriods(yearlyDocs, monthlyDocs, dailyDocs, currency, ticker, assetType, now) {
  // Calcular límites de cada período de análisis
  const periodBoundaries = calculatePeriodBoundaries(now);
  
  // Inicializar factores por período
  const periodFactors = initializePeriodFactors();
  
  // Arrays para datos de gráficos
  const totalValueDates = [];
  const totalValueValues = [];
  const percentChanges = [];
  
  // Variables para datos adicionales
  let firstDate = null;
  let lastValue = 0;
  let firstValue = 0;
  
  // FIX-V2-001: Estructura para construir performanceByYear
  // Recopila rendimientos mensuales para cada año
  const monthlyReturns = {}; // { "2024": { "1": 0.5, "2": -0.3, ... } }
  const yearlyReturns = {}; // { "2024": 5.2, "2025": 10.1 }
  
  // Procesar años consolidados
  yearlyDocs.forEach(doc => {
    const data = doc.data ? doc.data() : doc;
    const periodStart = data.startDate;
    const periodEnd = data.endDate;
    const periodKey = data.periodKey; // "2024"
    const currencyData = extractCurrencyOrAssetData(data, currency, ticker, assetType);
    
    if (!currencyData) return;
    
    processConsolidatedPeriod(periodFactors, periodBoundaries, currencyData, periodStart, periodEnd, data.docsCount || 1);
    
    // FIX-V2-001: Guardar rendimiento anual total
    if (periodKey) {
      yearlyReturns[periodKey] = currencyData.periodReturn || 0;
    }
    
    // Agregar punto para gráfico (fin del año)
    if (currencyData.endTotalValue !== undefined) {
      totalValueDates.push(periodEnd);
      totalValueValues.push(currencyData.endTotalValue);
      percentChanges.push(currencyData.periodReturn || 0);
      
      if (!firstDate) {
        firstDate = periodStart;
        firstValue = currencyData.startTotalValue || 0;
      }
      lastValue = currencyData.endTotalValue;
    }
  });
  
  // Procesar meses consolidados
  monthlyDocs.forEach(doc => {
    const data = doc.data ? doc.data() : doc;
    const periodStart = data.startDate;
    const periodEnd = data.endDate;
    const periodKey = data.periodKey; // "2024-12"
    const currencyData = extractCurrencyOrAssetData(data, currency, ticker, assetType);
    
    if (!currencyData) return;
    
    processConsolidatedPeriod(periodFactors, periodBoundaries, currencyData, periodStart, periodEnd, data.docsCount || 1);
    
    // FIX-V2-001: Recopilar rendimiento mensual para performanceByYear
    if (periodKey) {
      const [yearStr, monthStr] = periodKey.split('-');
      const monthNum = parseInt(monthStr, 10).toString(); // "12" -> "12", "01" -> "1"
      
      if (!monthlyReturns[yearStr]) {
        monthlyReturns[yearStr] = {};
      }
      // periodReturn es el rendimiento compuesto del mes
      monthlyReturns[yearStr][monthNum] = currencyData.periodReturn || 0;
    }
    
    // Agregar punto para gráfico (fin del mes)
    if (currencyData.endTotalValue !== undefined) {
      totalValueDates.push(periodEnd);
      totalValueValues.push(currencyData.endTotalValue);
      percentChanges.push(currencyData.periodReturn || 0);
      
      if (!firstDate) {
        firstDate = periodStart;
        firstValue = currencyData.startTotalValue || 0;
      }
      lastValue = currencyData.endTotalValue;
    }
  });
  
  // Procesar días del mes actual
  // FIX-V2-001: Calcular rendimiento del mes actual en curso
  let currentMonthFactor = 1;
  const currentMonthKey = now.toFormat('yyyy-MM');
  const currentYear = now.year.toString();
  const currentMonth = now.month.toString();
  
  dailyDocs.forEach(doc => {
    const data = doc.data ? doc.data() : doc;
    const date = data.date;
    const currencyData = extractCurrencyOrAssetData(data, currency, ticker, assetType);
    
    if (!currencyData) return;
    
    processDailyDocument(periodFactors, periodBoundaries, currencyData, date);
    
    // FIX-V2-001: Acumular factor para mes actual
    const dailyChange = currencyData.adjustedDailyChangePercentage || currencyData.dailyChangePercentage || 0;
    currentMonthFactor *= (1 + dailyChange / 100);
    
    // Agregar punto para gráfico
    const value = ticker && assetType 
      ? currencyData.totalValue 
      : currencyData.totalValue;
      
    if (value !== undefined) {
      totalValueDates.push(date);
      totalValueValues.push(value);
      percentChanges.push(currencyData.dailyChangePercentage || 0);
      
      if (!firstDate) {
        firstDate = date;
        firstValue = value;
      }
      lastValue = value;
    }
  });
  
  // FIX-V2-001: Agregar rendimiento del mes actual si hay datos
  if (dailyDocs.length > 0) {
    if (!monthlyReturns[currentYear]) {
      monthlyReturns[currentYear] = {};
    }
    monthlyReturns[currentYear][currentMonth] = (currentMonthFactor - 1) * 100;
  }
  
  // Calcular rendimientos finales
  return buildReturnsResult(periodFactors, {
    totalValueDates,
    totalValueValues,
    percentChanges,
    firstDate,
    firstValue,
    lastValue,
    now,
    // FIX-V2-001: Pasar datos para construir performanceByYear
    monthlyReturns,
    yearlyReturns
  });
}

/**
 * Calcula los límites de fecha para cada período de análisis
 * 
 * @param {DateTime} now - Fecha actual
 * @returns {Object} Límites por período
 */
function calculatePeriodBoundaries(now) {
  return {
    fiveYears: now.minus({ years: 5 }).toISODate(),
    twoYears: now.minus({ years: 2 }).toISODate(),
    oneYear: now.minus({ years: 1 }).toISODate(),
    sixMonths: now.minus({ months: 6 }).toISODate(),
    threeMonths: now.minus({ months: 3 }).toISODate(),
    oneMonth: now.minus({ months: 1 }).toISODate(),
    ytd: now.startOf('year').toISODate()
  };
}

/**
 * Inicializa estructura de factores por período
 * 
 * @returns {Object} Factores inicializados
 */
function initializePeriodFactors() {
  const periods = ['fiveYears', 'twoYears', 'oneYear', 'sixMonths', 'threeMonths', 'oneMonth', 'ytd'];
  const factors = {};
  
  periods.forEach(period => {
    factors[period] = {
      startFactor: 1,
      currentFactor: 1,
      found: false,
      docsCount: 0,
      startValue: null,
      endValue: null,
      totalCashFlow: 0
    };
  });
  
  return factors;
}

/**
 * Extrae datos de moneda o asset específico de un documento consolidado
 * 
 * @param {Object} data - Datos del documento
 * @param {string} currency - Código de moneda
 * @param {string|null} ticker - Ticker específico
 * @param {string|null} assetType - Tipo de asset
 * @returns {Object|null} Datos extraídos
 */
function extractCurrencyOrAssetData(data, currency, ticker, assetType) {
  const currencyData = data[currency];
  if (!currencyData) return null;
  
  if (ticker && assetType) {
    const assetKey = `${ticker}_${assetType}`;
    return currencyData.assetPerformance?.[assetKey] || null;
  }
  
  return currencyData;
}

/**
 * Procesa un período consolidado (mes o año) actualizando los factores
 * 
 * COST-OPT-003 FIX: La lógica de inclusión ahora es:
 * - Un período consolidado contribuye si su fecha de FIN >= límite del período
 * - Se marca como "found" cuando el período consolidado intersecta con el límite
 * - Esto asegura que meses como diciembre 2025 cuenten para "1 mes atrás" aunque
 *   el límite sea 2025-12-04 y el mes empiece en 2025-12-01
 * 
 * @param {Object} periodFactors - Factores a actualizar
 * @param {Object} boundaries - Límites de cada período
 * @param {Object} currencyData - Datos de la moneda
 * @param {string} periodStart - Fecha de inicio del período consolidado
 * @param {string} periodEnd - Fecha de fin del período consolidado
 * @param {number} docsCount - Cantidad de documentos en el período
 */
function processConsolidatedPeriod(periodFactors, boundaries, currencyData, periodStart, periodEnd, docsCount) {
  Object.entries(boundaries).forEach(([periodKey, boundaryDate]) => {
    const pf = periodFactors[periodKey];
    
    // Un período consolidado contribuye si su fecha de fin >= límite del período
    // Esto significa que al menos parte del período está dentro del rango de análisis
    if (periodEnd >= boundaryDate) {
      // COST-OPT-003 FIX: Marcar como encontrado si el período consolidado 
      // tiene intersección con el período de análisis (termina después del límite)
      // No requerimos que empiece después del límite porque los meses son unidades
      // discretas que se incluyen completos si tienen intersección
      if (!pf.found) {
        pf.startFactor = pf.currentFactor;
        pf.startValue = currencyData.startTotalValue || 0;
        pf.found = true;
      }
      
      // Multiplicar el factor del período consolidado
      if (currencyData.endFactor && currencyData.startFactor) {
        const periodMultiplier = currencyData.endFactor / currencyData.startFactor;
        pf.currentFactor *= periodMultiplier;
      }
      
      pf.endValue = currencyData.endTotalValue || 0;
      pf.totalCashFlow += currencyData.totalCashFlow || 0;
      pf.docsCount += docsCount;
    }
  });
}

/**
 * Procesa un documento diario actualizando los factores
 * 
 * @param {Object} periodFactors - Factores a actualizar
 * @param {Object} boundaries - Límites de cada período
 * @param {Object} currencyData - Datos de la moneda
 * @param {string} date - Fecha del documento
 */
function processDailyDocument(periodFactors, boundaries, currencyData, date) {
  Object.entries(boundaries).forEach(([periodKey, boundaryDate]) => {
    // Solo procesar si la fecha está dentro del período
    if (date >= boundaryDate) {
      const pf = periodFactors[periodKey];
      
      if (!pf.found) {
        pf.startFactor = pf.currentFactor;
        pf.startValue = currencyData.totalValue || 0;
        pf.found = true;
      }
      
      // Aplicar cambio diario al factor
      const dailyChange = currencyData.adjustedDailyChangePercentage;
      if (dailyChange !== undefined && dailyChange !== null) {
        pf.currentFactor *= (1 + dailyChange / 100);
      }
      
      pf.endValue = currencyData.totalValue || 0;
      pf.totalCashFlow += currencyData.totalCashFlow || 0;
      pf.docsCount++;
    }
  });
}

/**
 * Construye el resultado final de rendimientos
 * 
 * @param {Object} periodFactors - Factores calculados
 * @param {Object} chartData - Datos para gráficos
 * @returns {Object} Resultado en formato compatible con V1
 */
function buildReturnsResult(periodFactors, chartData) {
  const { 
    totalValueDates, 
    totalValueValues, 
    percentChanges, 
    firstDate, 
    firstValue, 
    lastValue, 
    now,
    // FIX-V2-001: Datos para performanceByYear
    monthlyReturns = {},
    yearlyReturns = {}
  } = chartData;
  
  // Función auxiliar para calcular rendimiento
  const calculateReturn = (pf) => {
    if (!pf.found || pf.startFactor === 0) return 0;
    return (pf.currentFactor / pf.startFactor - 1) * 100;
  };
  
  // Función auxiliar para calcular MWR
  const calculatePersonalReturn = (pf) => {
    if (!pf.found || (pf.startValue === 0 && pf.totalCashFlow === 0)) return 0;
    const netDeposits = -pf.totalCashFlow;
    const investmentBase = pf.startValue + (netDeposits / 2);
    if (investmentBase <= 0) return 0;
    const gain = pf.endValue - pf.startValue - netDeposits;
    return (gain / investmentBase) * 100;
  };
  
  // Calcular cambio total
  const overallPercentChange = firstValue > 0 
    ? ((lastValue - firstValue) / firstValue) * 100 
    : 0;
  
  // FIX-V2-001: Construir performanceByYear desde monthlyReturns
  const performanceByYear = {};
  const yearsWithData = new Set();
  
  // Procesar años que tienen datos mensuales
  Object.keys(monthlyReturns).forEach(year => {
    performanceByYear[year] = {
      months: {},
      personalMonths: {},
      total: 0,
      personalTotal: 0
    };
    
    // Inicializar todos los meses con 0
    for (let i = 1; i <= 12; i++) {
      performanceByYear[year].months[i.toString()] = 0;
      performanceByYear[year].personalMonths[i.toString()] = 0;
    }
    
    // Llenar con datos reales
    let yearCompound = 1;
    Object.keys(monthlyReturns[year]).forEach(month => {
      const monthReturn = monthlyReturns[year][month];
      performanceByYear[year].months[month] = monthReturn;
      performanceByYear[year].personalMonths[month] = monthReturn; // Simplificación: usar mismo valor
      yearCompound *= (1 + monthReturn / 100);
      yearsWithData.add(year);
    });
    
    // Calcular total del año como compuesto de meses
    performanceByYear[year].total = (yearCompound - 1) * 100;
    performanceByYear[year].personalTotal = performanceByYear[year].total;
  });
  
  // Agregar años consolidados que no tenían meses detallados
  Object.keys(yearlyReturns).forEach(year => {
    if (!performanceByYear[year]) {
      performanceByYear[year] = {
        months: {},
        personalMonths: {},
        total: yearlyReturns[year],
        personalTotal: yearlyReturns[year]
      };
      // Inicializar meses vacíos
      for (let i = 1; i <= 12; i++) {
        performanceByYear[year].months[i.toString()] = 0;
        performanceByYear[year].personalMonths[i.toString()] = 0;
      }
      yearsWithData.add(year);
    }
  });
  
  // FIX-V2-001: Construir availableYears (ordenados descendentemente)
  const availableYears = Array.from(yearsWithData).sort((a, b) => parseInt(b) - parseInt(a));
  
  return {
    returns: {
      // TWR
      fiveYearReturn: calculateReturn(periodFactors.fiveYears),
      twoYearReturn: calculateReturn(periodFactors.twoYears),
      oneYearReturn: calculateReturn(periodFactors.oneYear),
      sixMonthReturn: calculateReturn(periodFactors.sixMonths),
      threeMonthReturn: calculateReturn(periodFactors.threeMonths),
      oneMonthReturn: calculateReturn(periodFactors.oneMonth),
      ytdReturn: calculateReturn(periodFactors.ytd),
      
      // MWR (Personal Return)
      fiveYearPersonalReturn: calculatePersonalReturn(periodFactors.fiveYears),
      twoYearPersonalReturn: calculatePersonalReturn(periodFactors.twoYears),
      oneYearPersonalReturn: calculatePersonalReturn(periodFactors.oneYear),
      sixMonthPersonalReturn: calculatePersonalReturn(periodFactors.sixMonths),
      threeMonthPersonalReturn: calculatePersonalReturn(periodFactors.threeMonths),
      oneMonthPersonalReturn: calculatePersonalReturn(periodFactors.oneMonth),
      ytdPersonalReturn: calculatePersonalReturn(periodFactors.ytd),
      
      // Flags de datos disponibles
      hasFiveYearData: periodFactors.fiveYears.found,
      hasTwoYearData: periodFactors.twoYears.found,
      hasOneYearData: periodFactors.oneYear.found,
      hasSixMonthData: periodFactors.sixMonths.found,
      hasThreeMonthData: periodFactors.threeMonths.found,
      hasOneMonthData: periodFactors.oneMonth.found,
      hasYtdData: periodFactors.ytd.found
    },
    validDocsCountByPeriod: {
      fiveYears: periodFactors.fiveYears.docsCount,
      twoYears: periodFactors.twoYears.docsCount,
      oneYear: periodFactors.oneYear.docsCount,
      sixMonths: periodFactors.sixMonths.docsCount,
      threeMonths: periodFactors.threeMonths.docsCount,
      oneMonth: periodFactors.oneMonth.docsCount,
      ytd: periodFactors.ytd.docsCount
    },
    totalValueData: {
      dates: totalValueDates,
      values: totalValueValues,
      percentChanges,
      overallPercentChange
    },
    // FIX-V2-001: Agregar performanceByYear y availableYears para gráficos
    performanceByYear,
    availableYears,
    startDate: firstDate || '',
    // monthlyCompoundData no disponible en V2, el frontend debe usar performanceByYear
    monthlyCompoundData: {},
    // Flag para indicar que se usó V2
    consolidatedVersion: true
  };
}

// ============================================================================
// FUNCIONES AUXILIARES
// ============================================================================

/**
 * Verifica si un mes ya está cerrado y puede ser consolidado
 * 
 * @param {string} monthKey - Clave del mes (ej: "2024-12")
 * @param {DateTime} now - Fecha actual
 * @returns {boolean} true si el mes ya terminó
 */
function isMonthClosed(monthKey, now) {
  const currentMonth = now.toFormat('yyyy-MM');
  return monthKey < currentMonth;
}

/**
 * Verifica si un año ya está cerrado y puede ser consolidado
 * 
 * @param {string} yearKey - Clave del año (ej: "2024")
 * @param {DateTime} now - Fecha actual
 * @returns {boolean} true si el año ya terminó
 */
function isYearClosed(yearKey, now) {
  return parseInt(yearKey) < now.year;
}

/**
 * Obtiene el siguiente mes en formato yyyy-MM
 * 
 * @param {string} monthKey - Mes actual (ej: "2024-12")
 * @returns {string} Siguiente mes (ej: "2025-01")
 */
function getNextMonth(monthKey) {
  const dt = DateTime.fromFormat(monthKey, 'yyyy-MM');
  return dt.plus({ months: 1 }).toFormat('yyyy-MM');
}

/**
 * Obtiene todos los meses entre dos fechas
 * 
 * @param {string} startDate - Fecha de inicio ISO
 * @param {string} endDate - Fecha de fin ISO
 * @returns {Array<string>} Array de claves de mes
 */
function getMonthsBetween(startDate, endDate) {
  const months = [];
  let current = DateTime.fromISO(startDate).startOf('month');
  const end = DateTime.fromISO(endDate).startOf('month');
  
  while (current <= end) {
    months.push(current.toFormat('yyyy-MM'));
    current = current.plus({ months: 1 });
  }
  
  return months;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Constantes
  CONSOLIDATED_SCHEMA_VERSION,
  NON_CURRENCY_FIELDS,
  
  // Consolidación
  consolidatePeriod,
  consolidateCurrencyData,
  extractCurrenciesFromDocs,
  
  // Encadenamiento de factores
  chainFactorsForPeriods,
  calculatePeriodBoundaries,
  initializePeriodFactors,
  processConsolidatedPeriod,
  processDailyDocument,
  buildReturnsResult,
  
  // Utilidades
  isMonthClosed,
  isYearClosed,
  getNextMonth,
  getMonthsBetween,
  calculateModifiedDietzSimple
};
