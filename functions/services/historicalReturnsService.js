/**
 * Cloud Function: getHistoricalReturns
 * 
 * Pre-calcula los rendimientos históricos del portafolio en el servidor
 * y los cachea para reducir lecturas de Firestore de ~200 a 1.
 * 
 * Actualizado en Historia 25 para incluir Personal Return (MWR) junto con TWR.
 * 
 * @module historicalReturnsService
 * @see docs/stories/7.story.md (OPT-002)
 * @see docs/stories/25.story.md (MWR Dual Metrics)
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require('./firebaseAdmin');
const { DateTime } = require('luxon');

// Importar utilidades compartidas y funciones MWR
const { 
  MIN_DOCS,
  sortDocumentsByDate,
  extractDocumentData,
  getPeriodBoundaries,
  hasEnoughDocuments
} = require('../utils/periodCalculations');

const {
  calculateSimplePersonalReturn,
  calculateModifiedDietzReturn
} = require('../utils/mwrCalculations');

// Importar rate limiter (SCALE-BE-004)
const { withRateLimit } = require('../utils/rateLimiter');

const db = admin.firestore();

/**
 * Configuración de la Cloud Function
 */
const callableConfig = {
  cors: true,
  enforceAppCheck: false,
  timeoutSeconds: 60,
  memory: "512MiB",
};

/**
 * Calcula los rendimientos históricos basados en factores compuestos
 * 
 * @param {Array} docs - Documentos de Firestore ordenados por fecha
 * @param {string} currency - Código de moneda (USD, COP, etc.)
 * @param {string|null} ticker - Ticker específico (opcional)
 * @param {string|null} assetType - Tipo de asset (opcional)
 * @returns {Object} Rendimientos calculados
 */
function calculateHistoricalReturns(docs, currency, ticker, assetType) {
  // Fechas límite para cada período
  const now = DateTime.now().setZone("America/New_York");
  const fiveYearsAgo = now.minus({ years: 5 }).toISODate() || "";
  const twoYearsAgo = now.minus({ years: 2 }).toISODate() || "";
  const oneYearAgo = now.minus({ years: 1 }).toISODate() || "";
  const sixMonthsAgo = now.minus({ months: 6 }).toISODate() || "";
  const startOfYear = now.startOf("year").toISODate() || "";
  const threeMonthsAgo = now.minus({ months: 3 }).toISODate() || "";
  const oneMonthAgo = now.minus({ months: 1 }).toISODate() || "";

  // Ordenar documentos por fecha
  const documents = docs.sort((a, b) => {
    const dateA = a.data ? a.data().date : a.date;
    const dateB = b.data ? b.data().date : b.date;
    return dateA.localeCompare(dateB);
  });

  // Factores iniciales para cada período
  let ytdStartFactor = 1;
  let oneMonthStartFactor = 1;
  let threeMonthStartFactor = 1;
  let sixMonthStartFactor = 1;
  let oneYearStartFactor = 1;
  let twoYearStartFactor = 1;
  let fiveYearStartFactor = 1;

  // Factor actual
  let currentFactor = 1;

  // Flags para cada período
  let foundYTDStart = false;
  let foundOneMonthStart = false;
  let foundThreeMonthStart = false;
  let foundSixMonthStart = false;
  let foundOneYearStart = false;
  let foundTwoYearStart = false;
  let foundFiveYearStart = false;

  // Contadores de documentos por período
  let ytdDocsCount = 0;
  let oneMonthDocsCount = 0;
  let threeMonthDocsCount = 0;
  let sixMonthDocsCount = 0;
  let oneYearDocsCount = 0;
  let twoYearDocsCount = 0;
  let fiveYearDocsCount = 0;

  // Contadores de documentos válidos
  let validYtdDocsCount = 0;
  let validOneMonthDocsCount = 0;
  let validThreeMonthDocsCount = 0;
  let validSixMonthDocsCount = 0;
  let validOneYearDocsCount = 0;
  let validTwoYearDocsCount = 0;
  let validFiveYearDocsCount = 0;

  // Datos para gráficos
  const totalValueDates = [];
  const totalValueValues = [];
  const percentChanges = [];
  // FEAT-CHART-001: Agregar cambios ajustados para TWR en gráficos multi-cuenta
  const adjustedPercentChanges = [];

  // Datos para rendimiento mensual
  const datesByMonth = {};
  const lastDaysByMonth = {};
  const monthlyStartFactors = {};
  const monthlyEndFactors = {};
  const monthlyCompoundData = {};

  // ============================================================
  // MWR (Personal Return) - Historia 25
  // Estructuras para acumular datos de MWR en paralelo con TWR
  // ============================================================
  const mwrPeriods = {
    ytd: { startDate: startOfYear, startValue: null, endValue: null, cashFlows: [], totalCashFlow: 0, found: false },
    oneMonth: { startDate: oneMonthAgo, startValue: null, endValue: null, cashFlows: [], totalCashFlow: 0, found: false },
    threeMonths: { startDate: threeMonthsAgo, startValue: null, endValue: null, cashFlows: [], totalCashFlow: 0, found: false },
    sixMonths: { startDate: sixMonthsAgo, startValue: null, endValue: null, cashFlows: [], totalCashFlow: 0, found: false },
    oneYear: { startDate: oneYearAgo, startValue: null, endValue: null, cashFlows: [], totalCashFlow: 0, found: false },
    twoYears: { startDate: twoYearsAgo, startValue: null, endValue: null, cashFlows: [], totalCashFlow: 0, found: false },
    fiveYears: { startDate: fiveYearsAgo, startValue: null, endValue: null, cashFlows: [], totalCashFlow: 0, found: false }
  };
  const todayISO = now.toISODate();

  // Primera pasada: agrupar fechas por mes
  documents.forEach((doc) => {
    const data = doc.data ? doc.data() : doc;
    const date = DateTime.fromISO(data.date);
    const year = date.year.toString();
    // FIX: Usar índice 1-based para meses (1=Ene, 12=Dic) para consistencia con V2
    const month = date.month.toString();

    if (!datesByMonth[year]) {
      datesByMonth[year] = {};
    }
    if (!datesByMonth[year][month]) {
      datesByMonth[year][month] = [];
    }
    datesByMonth[year][month].push(data.date);
  });

  // Obtener el último día de cada mes
  Object.keys(datesByMonth).forEach(year => {
    lastDaysByMonth[year] = {};
    Object.keys(datesByMonth[year]).forEach(month => {
      const sortedDates = [...datesByMonth[year][month]].sort((a, b) => b.localeCompare(a));
      lastDaysByMonth[year][month] = sortedDates[0];
    });
  });

  // Inicializar estructuras mensuales
  Object.keys(datesByMonth).forEach(year => {
    monthlyStartFactors[year] = {};
    monthlyEndFactors[year] = {};
    monthlyCompoundData[year] = {};
  });

  // Segunda pasada: procesar documentos
  documents.forEach((doc) => {
    const data = doc.data ? doc.data() : doc;
    const currencyData = data[currency];

    if (!currencyData) return;

    // Contabilizar documentos por período
    if (data.date >= startOfYear) ytdDocsCount++;
    if (data.date >= oneMonthAgo) oneMonthDocsCount++;
    if (data.date >= threeMonthsAgo) threeMonthDocsCount++;
    if (data.date >= sixMonthsAgo) sixMonthDocsCount++;
    if (data.date >= oneYearAgo) oneYearDocsCount++;
    if (data.date >= twoYearsAgo) twoYearDocsCount++;
    if (data.date >= fiveYearsAgo) fiveYearDocsCount++;

    // Guardar factor inicial para cada período
    if (!foundYTDStart && data.date >= startOfYear) {
      ytdStartFactor = currentFactor;
      foundYTDStart = true;
    }
    if (!foundOneMonthStart && data.date >= oneMonthAgo) {
      oneMonthStartFactor = currentFactor;
      foundOneMonthStart = true;
    }
    if (!foundThreeMonthStart && data.date >= threeMonthsAgo) {
      threeMonthStartFactor = currentFactor;
      foundThreeMonthStart = true;
    }
    if (!foundSixMonthStart && data.date >= sixMonthsAgo) {
      sixMonthStartFactor = currentFactor;
      foundSixMonthStart = true;
    }
    if (!foundOneYearStart && data.date >= oneYearAgo) {
      oneYearStartFactor = currentFactor;
      foundOneYearStart = true;
    }
    if (!foundTwoYearStart && data.date >= twoYearsAgo) {
      twoYearStartFactor = currentFactor;
      foundTwoYearStart = true;
    }
    if (!foundFiveYearStart && data.date >= fiveYearsAgo) {
      fiveYearStartFactor = currentFactor;
      foundFiveYearStart = true;
    }

    // Obtener cambio diario
    let adjustedDailyChange = 0;
    let hasData = false;
    let totalValue = 0;
    let totalInvestment = 0;
    let totalCashFlow = 0;
    let doneProfitAndLoss = 0;
    let unrealizedProfitAndLoss = 0;

    if (ticker && assetType) {
      const assetKey = `${ticker}_${assetType}`;
      const assetData = currencyData.assetPerformance?.[assetKey];
      if (assetData && assetData.adjustedDailyChangePercentage !== undefined) {
        adjustedDailyChange = assetData.adjustedDailyChangePercentage;
        hasData = true;

        totalValue = assetData.totalValue || 0;
        totalInvestment = assetData.totalInvestment || 0;
        totalCashFlow = assetData.totalCashFlow || 0;
        doneProfitAndLoss = assetData.doneProfitAndLoss || 0;
        unrealizedProfitAndLoss = assetData.unrealizedProfitAndLoss || 0;

        if (data.date >= startOfYear) validYtdDocsCount++;
        if (data.date >= oneMonthAgo) validOneMonthDocsCount++;
        if (data.date >= threeMonthsAgo) validThreeMonthDocsCount++;
        if (data.date >= sixMonthsAgo) validSixMonthDocsCount++;
        if (data.date >= oneYearAgo) validOneYearDocsCount++;
        if (data.date >= twoYearsAgo) validTwoYearDocsCount++;
        if (data.date >= fiveYearsAgo) validFiveYearDocsCount++;

        if (assetData.totalValue !== undefined) {
          totalValueDates.push(data.date);
          totalValueValues.push(assetData.totalValue);
          percentChanges.push(assetData.dailyChangePercentage || 0);
          // FEAT-CHART-001: Guardar cambios ajustados para TWR en gráficos
          adjustedPercentChanges.push(assetData.adjustedDailyChangePercentage || 0);
        }
      }
    } else if (currencyData.adjustedDailyChangePercentage !== undefined) {
      adjustedDailyChange = currencyData.adjustedDailyChangePercentage;
      hasData = true;

      totalValue = currencyData.totalValue || 0;
      totalInvestment = currencyData.totalInvestment || 0;
      totalCashFlow = currencyData.totalCashFlow || 0;
      doneProfitAndLoss = currencyData.doneProfitAndLoss || 0;
      unrealizedProfitAndLoss = currencyData.unrealizedProfitAndLoss || 0;

      if (data.date >= startOfYear) validYtdDocsCount++;
      if (data.date >= oneMonthAgo) validOneMonthDocsCount++;
      if (data.date >= threeMonthsAgo) validThreeMonthDocsCount++;
      if (data.date >= sixMonthsAgo) validSixMonthDocsCount++;
      if (data.date >= oneYearAgo) validOneYearDocsCount++;
      if (data.date >= twoYearsAgo) validTwoYearDocsCount++;
      if (data.date >= fiveYearsAgo) validFiveYearDocsCount++;

      if (currencyData.totalValue !== undefined) {
        totalValueDates.push(data.date);
        totalValueValues.push(currencyData.totalValue);
        percentChanges.push(currencyData.dailyChangePercentage || 0);
        // FEAT-CHART-001: Guardar cambios ajustados para TWR en gráficos
        adjustedPercentChanges.push(currencyData.adjustedDailyChangePercentage || 0);
      }
    }

    // ============================================================
    // MWR: Acumular datos para Personal Return (Historia 25)
    // ============================================================
    if (hasData) {
      Object.keys(mwrPeriods).forEach(periodKey => {
        const period = mwrPeriods[periodKey];
        
        if (data.date >= period.startDate) {
          // Marcar valor inicial del período
          if (!period.found) {
            period.startValue = totalValue;
            period.found = true;
          }
          
          // Actualizar valor final
          period.endValue = totalValue;
          
          // Acumular cashflows
          if (totalCashFlow !== 0) {
            period.cashFlows.push({ date: data.date, amount: totalCashFlow });
          }
          period.totalCashFlow += totalCashFlow;
        }
      });
    }

    // Actualizar factor compuesto
    if (hasData) {
      const date = DateTime.fromISO(data.date);
      const year = date.year.toString();
      // FIX: Usar índice 1-based para meses (1=Ene, 12=Dic) para consistencia con V2
      const month = date.month.toString();

      const isLastDayOfMonth = lastDaysByMonth[year]?.[month] === data.date;

      // FIX OPT-018: Guardar monthlyStartFactors ANTES de aplicar el cambio del día
      // para que el rendimiento del primer día del mes se incluya correctamente
      if (!monthlyStartFactors[year][month]) {
        monthlyStartFactors[year][month] = currentFactor;
      }

      // Ahora sí aplicamos el cambio del día
      currentFactor = currentFactor * (1 + adjustedDailyChange / 100);

      // Guardar el factor final después del cambio
      monthlyEndFactors[year][month] = currentFactor;

      if (!monthlyCompoundData[year][month]) {
        monthlyCompoundData[year][month] = {
          startFactor: monthlyStartFactors[year][month],
          endFactor: currentFactor,
          returnPct: 0,
          startTotalValue: totalValue,
          startTotalInvestment: totalInvestment,
          endTotalValue: totalValue,
          endTotalInvestment: totalInvestment,
          totalCashFlow: totalCashFlow,
          doneProfitAndLoss: doneProfitAndLoss,
          unrealizedProfitAndLoss: unrealizedProfitAndLoss,
          lastDayOfMonth: isLastDayOfMonth
        };
      } else {
        monthlyCompoundData[year][month].endFactor = currentFactor;
        monthlyCompoundData[year][month].endTotalValue = totalValue;
        monthlyCompoundData[year][month].endTotalInvestment = totalInvestment;
        monthlyCompoundData[year][month].lastDayOfMonth = isLastDayOfMonth;

        if (monthlyCompoundData[year][month].doneProfitAndLoss !== undefined) {
          monthlyCompoundData[year][month].doneProfitAndLoss += doneProfitAndLoss;
        } else {
          monthlyCompoundData[year][month].doneProfitAndLoss = doneProfitAndLoss;
        }

        monthlyCompoundData[year][month].unrealizedProfitAndLoss = unrealizedProfitAndLoss;

        if (monthlyCompoundData[year][month].totalCashFlow !== undefined) {
          monthlyCompoundData[year][month].totalCashFlow += totalCashFlow;
        } else {
          monthlyCompoundData[year][month].totalCashFlow = totalCashFlow;
        }
      }
    }
  });

  // Calcular rendimientos por período
  const ytdReturn = foundYTDStart ? (currentFactor / ytdStartFactor - 1) * 100 : 0;
  const oneMonthReturn = foundOneMonthStart ? (currentFactor / oneMonthStartFactor - 1) * 100 : 0;
  const threeMonthReturn = foundThreeMonthStart ? (currentFactor / threeMonthStartFactor - 1) * 100 : 0;
  const sixMonthReturn = foundSixMonthStart ? (currentFactor / sixMonthStartFactor - 1) * 100 : 0;
  const oneYearReturn = foundOneYearStart ? (currentFactor / oneYearStartFactor - 1) * 100 : 0;
  const twoYearReturn = foundTwoYearStart ? (currentFactor / twoYearStartFactor - 1) * 100 : 0;
  const fiveYearReturn = foundFiveYearStart ? (currentFactor / fiveYearStartFactor - 1) * 100 : 0;

  // ============================================================
  // Calcular rendimientos mensuales (TWR + MWR)
  // Historia 25: Agregar Personal Return mensual
  // ============================================================
  const performanceByYear = {};

  Object.keys(monthlyStartFactors).forEach(year => {
    performanceByYear[year] = { 
      months: {}, 
      total: 0,
      // Historia 25: Agregar estructura para MWR mensual
      personalMonths: {},
      personalTotal: 0
    };
    let compoundTotal = 1;
    let personalCompoundTotal = 1;

    // Inicializar meses con 0 (1-based: 1=Ene, 12=Dic)
    for (let i = 1; i <= 12; i++) {
      performanceByYear[year].months[i.toString()] = 0;
      performanceByYear[year].personalMonths[i.toString()] = 0;
    }

    Object.keys(monthlyStartFactors[year]).forEach(month => {
      const monthStart = monthlyStartFactors[year][month];
      const monthEnd = monthlyEndFactors[year][month];

      if (monthStart > 0) {
        const monthReturn = (monthEnd / monthStart - 1) * 100;

        performanceByYear[year].months[month] = monthReturn;
        
        if (monthlyCompoundData[year][month]) {
          monthlyCompoundData[year][month].returnPct = monthReturn;

          if (monthlyCompoundData[year][month].lastDayOfMonth &&
            monthlyCompoundData[year][month].doneProfitAndLoss !== undefined &&
            monthlyCompoundData[year][month].unrealizedProfitAndLoss !== undefined) {
            const unrealizedProfit = monthlyCompoundData[year][month].unrealizedProfitAndLoss;
            const doneProfit = monthlyCompoundData[year][month].doneProfitAndLoss;
            monthlyCompoundData[year][month].profit = doneProfit + unrealizedProfit;
          } else if (monthlyCompoundData[year][month].doneProfitAndLoss !== undefined) {
            monthlyCompoundData[year][month].profit = monthlyCompoundData[year][month].doneProfitAndLoss;
          }
          
          // ============================================================
          // Historia 25: Calcular MWR (Personal Return) mensual
          // Usando datos de monthlyCompoundData que ya tiene:
          // - startTotalValue, endTotalValue
          // - totalCashFlow (suma de cashflows del mes)
          // ============================================================
          const startValue = monthlyCompoundData[year][month].startTotalValue || 0;
          const endValue = monthlyCompoundData[year][month].endTotalValue || 0;
          const totalCashFlow = monthlyCompoundData[year][month].totalCashFlow || 0;
          
          let personalReturn = 0;
          
          // Calcular MWR usando fórmula Simple (Modified Dietz simplificado)
          const netDeposits = -totalCashFlow; // cashflow negativo = depósitos
          
          if (startValue === 0 && netDeposits > 0) {
            // Sin valor inicial, solo depósitos durante el mes
            personalReturn = ((endValue - netDeposits) / netDeposits) * 100;
          } else if (startValue > 0) {
            // Con valor inicial: usar inversión base ponderada
            const investmentBase = startValue + (netDeposits / 2);
            if (investmentBase > 0) {
              const gain = endValue - startValue - netDeposits;
              personalReturn = (gain / investmentBase) * 100;
            }
          }
          
          // Protección para valores extremos (>100% o <-100%)
          // En estos casos, usar el TWR como fallback
          if (Math.abs(personalReturn) > 100 && Math.abs(monthReturn) < Math.abs(personalReturn)) {
            personalReturn = monthReturn;
          }
          
          performanceByYear[year].personalMonths[month] = personalReturn;
          monthlyCompoundData[year][month].personalReturnPct = personalReturn;
          
          // Calcular compuesto para MWR anual
          personalCompoundTotal = personalCompoundTotal * (1 + personalReturn / 100);
        }

        compoundTotal = compoundTotal * (1 + monthReturn / 100);
      }
    });

    performanceByYear[year].total = (compoundTotal - 1) * 100;
    performanceByYear[year].personalTotal = (personalCompoundTotal - 1) * 100;
  });

  // Determinar años disponibles y fecha de inicio
  const yearsWithData = Object.keys(performanceByYear).filter(year => {
    return Object.keys(performanceByYear[year].months).some(month => {
      return performanceByYear[year].months[month] !== 0;
    });
  });

  const availableYears = yearsWithData.sort((a, b) => parseInt(b) - parseInt(a));
  let startDate = "";

  if (yearsWithData.length > 0) {
    const firstValidYear = yearsWithData[yearsWithData.length - 1];
    // FIX: Usar índice 1-based (1=Ene, 12=Dic)
    let firstValidMonth = "12";

    for (let month = 1; month <= 12; month++) {
      if (performanceByYear[firstValidYear].months[month.toString()] !== 0) {
        firstValidMonth = month.toString();
        break;
      }
    }

    const firstDate = DateTime.fromObject({
      year: parseInt(firstValidYear),
      month: parseInt(firstValidMonth), // Ya es 1-based, no necesita +1
      day: 1
    });

    startDate = firstDate.toFormat("dd/MM/yy");
  }

  // Calcular mínimo de documentos para YTD
  const currentMonth = now.month;
  const minDocsForYtd = Math.max(Math.ceil(currentMonth * 4 / 12), MIN_DOCS.ytd);

  // Seleccionar contadores
  const ytdCount = ticker ? validYtdDocsCount : ytdDocsCount;
  const oneMonthCount = ticker ? validOneMonthDocsCount : oneMonthDocsCount;
  const threeMonthCount = ticker ? validThreeMonthDocsCount : threeMonthDocsCount;
  const sixMonthCount = ticker ? validSixMonthDocsCount : sixMonthDocsCount;
  const oneYearCount = ticker ? validOneYearDocsCount : oneYearDocsCount;
  const twoYearCount = ticker ? validTwoYearDocsCount : twoYearDocsCount;
  const fiveYearCount = ticker ? validFiveYearDocsCount : fiveYearDocsCount;

  // Verificar suficientes documentos
  const hasEnoughYtdDocs = ytdCount >= minDocsForYtd;
  const hasEnoughOneMonthDocs = oneMonthCount >= MIN_DOCS.oneMonth;
  const hasEnoughThreeMonthDocs = threeMonthCount >= MIN_DOCS.threeMonths;
  const hasEnoughSixMonthDocs = sixMonthCount >= MIN_DOCS.sixMonths;
  const hasEnoughOneYearDocs = oneYearCount >= MIN_DOCS.oneYear;
  const hasEnoughTwoYearDocs = twoYearCount >= MIN_DOCS.twoYears;
  const hasEnoughFiveYearDocs = fiveYearCount >= MIN_DOCS.fiveYears;

  // Calcular cambio porcentual total
  let overallPercentChange = 0;
  if (totalValueValues.length >= 2) {
    const initialValue = totalValueValues[0];
    const finalValue = totalValueValues[totalValueValues.length - 1];
    overallPercentChange = ((finalValue - initialValue) / initialValue) * 100;
  }

  // ============================================================
  // MWR: Calcular Personal Returns (Historia 25)
  // ============================================================
  const personalReturns = {};
  
  // Mapeo de claves internas a claves de API
  const periodApiMap = {
    ytd: 'ytd',
    oneMonth: 'oneMonth',
    threeMonths: 'threeMonth',
    sixMonths: 'sixMonth',
    oneYear: 'oneYear',
    twoYears: 'twoYear',
    fiveYears: 'fiveYear'
  };

  Object.keys(mwrPeriods).forEach(periodKey => {
    const period = mwrPeriods[periodKey];
    const apiKey = periodApiMap[periodKey];
    
    if (period.found && period.endValue !== null) {
      // Usar Modified Dietz si hay cashflows, sino usar Simple
      if (period.cashFlows.length > 0) {
        personalReturns[`${apiKey}PersonalReturn`] = calculateModifiedDietzReturn(
          period.startValue,
          period.endValue,
          period.cashFlows,
          period.startDate,
          todayISO
        );
      } else {
        personalReturns[`${apiKey}PersonalReturn`] = calculateSimplePersonalReturn(
          period.startValue,
          period.endValue,
          period.totalCashFlow
        );
      }
    } else {
      personalReturns[`${apiKey}PersonalReturn`] = 0;
    }
    
    // Flag de datos disponibles para MWR
    personalReturns[`has${apiKey.charAt(0).toUpperCase() + apiKey.slice(1)}PersonalData`] = period.found;
  });

  return {
    returns: {
      // TWR (Time-Weighted Return) - existente
      ytdReturn,
      oneMonthReturn,
      threeMonthReturn,
      sixMonthReturn,
      oneYearReturn,
      twoYearReturn,
      fiveYearReturn,
      hasYtdData: foundYTDStart && hasEnoughYtdDocs,
      hasOneMonthData: foundOneMonthStart && hasEnoughOneMonthDocs,
      hasThreeMonthData: foundThreeMonthStart && hasEnoughThreeMonthDocs,
      hasSixMonthData: foundSixMonthStart && hasEnoughSixMonthDocs,
      hasOneYearData: foundOneYearStart && hasEnoughOneYearDocs,
      hasTwoYearData: foundTwoYearStart && hasEnoughTwoYearDocs,
      hasFiveYearData: foundFiveYearStart && hasEnoughFiveYearDocs,
      
      // MWR (Money-Weighted Return / Personal Return) - Historia 25
      ...personalReturns
    },
    validDocsCountByPeriod: {
      ytd: ytdCount,
      oneMonth: oneMonthCount,
      threeMonths: threeMonthCount,
      sixMonths: sixMonthCount,
      oneYear: oneYearCount,
      twoYears: twoYearCount,
      fiveYears: fiveYearCount
    },
    totalValueData: {
      dates: totalValueDates,
      values: totalValueValues,
      percentChanges: percentChanges,
      // FEAT-CHART-001: Cambios ajustados para TWR en gráficos multi-cuenta
      adjustedPercentChanges: adjustedPercentChanges,
      overallPercentChange: overallPercentChange
    },
    performanceByYear,
    availableYears,
    startDate,
    monthlyCompoundData
  };
}

/**
 * Cloud Function para obtener rendimientos históricos pre-calculados
 * 
 * @param {Object} request - Request con auth y data
 * @returns {Object} Rendimientos calculados o desde cache
 */
const getHistoricalReturns = onCall(callableConfig, withRateLimit('getHistoricalReturns')(async (request) => {
  const { auth, data } = request;

  // 1. Verificar autenticación
  if (!auth) {
    throw new HttpsError('unauthenticated', 'Debes iniciar sesión para acceder a los rendimientos');
  }
  const userId = auth.uid;

  const {
    currency = "USD",
    accountId = "overall",
    ticker = null,
    assetType = null,
    forceRefresh = false
  } = data || {};

  // 2. Generar clave de cache
  const cacheKey = `${currency}_${accountId}${ticker ? `_${ticker}` : ''}${assetType ? `_${assetType}` : ''}`;

  console.log(`[getHistoricalReturns] Usuario: ${userId}, Cache key: ${cacheKey}, Force: ${forceRefresh}`);

  // 3. Verificar cache (si no forceRefresh)
  if (!forceRefresh) {
    try {
      const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
      const cacheDoc = await cacheRef.get();

      if (cacheDoc.exists) {
        const cache = cacheDoc.data();
        const validUntil = new Date(cache.validUntil);

        if (validUntil > new Date()) {
          console.log(`[getHistoricalReturns] Cache HIT para ${userId}/${cacheKey}`);
          return {
            ...cache.data,
            cacheHit: true,
            lastCalculated: cache.lastCalculated,
            validUntil: cache.validUntil
          };
        } else {
          console.log(`[getHistoricalReturns] Cache EXPIRED para ${userId}/${cacheKey}`);
        }
      }
    } catch (cacheError) {
      console.warn(`[getHistoricalReturns] Error leyendo cache: ${cacheError.message}`);
    }
  }

  console.log(`[getHistoricalReturns] Cache MISS - Calculando para ${userId}/${cacheKey}`);

  // 4. Obtener documentos de performance
  const basePath = accountId === "overall"
    ? `portfolioPerformance/${userId}/dates`
    : `portfolioPerformance/${userId}/accounts/${accountId}/dates`;

  const performanceSnapshot = await db.collection(basePath)
    .orderBy("date", "asc")
    .get();

  if (performanceSnapshot.empty) {
    console.log(`[getHistoricalReturns] Sin datos de performance para ${userId}`);
    return {
      returns: {
        ytdReturn: 0, oneMonthReturn: 0, threeMonthReturn: 0, sixMonthReturn: 0,
        oneYearReturn: 0, twoYearReturn: 0, fiveYearReturn: 0,
        hasYtdData: false, hasOneMonthData: false, hasThreeMonthData: false,
        hasSixMonthData: false, hasOneYearData: false, hasTwoYearData: false,
        hasFiveYearData: false
      },
      validDocsCountByPeriod: {
        ytd: 0, oneMonth: 0, threeMonths: 0, sixMonths: 0,
        oneYear: 0, twoYears: 0, fiveYears: 0
      },
      totalValueData: {
        dates: [], values: [], percentChanges: [], overallPercentChange: 0
      },
      performanceByYear: {},
      availableYears: [],
      startDate: "",
      monthlyCompoundData: {},
      cacheHit: false,
      lastCalculated: new Date().toISOString()
    };
  }

  console.log(`[getHistoricalReturns] Procesando ${performanceSnapshot.docs.length} documentos`);

  // 5. Ejecutar cálculos
  const result = calculateHistoricalReturns(
    performanceSnapshot.docs,
    currency,
    ticker,
    assetType
  );

  // 6. Guardar en cache con TTL dinámico basado en horario de mercado
  const now = new Date();
  const validUntil = calculateDynamicTTL();

  const cacheData = {
    data: result,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };

  try {
    const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
    await cacheRef.set(cacheData);
    console.log(`[getHistoricalReturns] Cache guardado para ${userId}/${cacheKey}`);
  } catch (cacheWriteError) {
    console.error(`[getHistoricalReturns] Error guardando cache: ${cacheWriteError.message}`);
  }

  return {
    ...result,
    cacheHit: false,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };
}));

/**
 * Invalida el cache de rendimientos para un usuario
 * 
 * @param {string} userId - ID del usuario
 */
async function invalidatePerformanceCache(userId) {
  try {
    const cacheCollection = db.collection(`userData/${userId}/performanceCache`);
    const cacheSnapshot = await cacheCollection.get();

    if (cacheSnapshot.empty) {
      console.log(`[invalidatePerformanceCache] Sin cache para ${userId}`);
      return;
    }

    const batch = db.batch();
    cacheSnapshot.docs.forEach(doc => {
      batch.delete(doc.ref);
    });

    await batch.commit();
    console.log(`[invalidatePerformanceCache] Cache invalidado para ${userId} (${cacheSnapshot.size} documentos)`);
  } catch (error) {
    console.error(`[invalidatePerformanceCache] Error: ${error.message}`);
  }
}

/**
 * Calcula el TTL dinámico basado en el estado del mercado NYSE
 * - Durante horario de mercado (9:30-16:00 ET): 5 minutos
 * - Fuera de horario: hasta próxima apertura (9:30 AM ET)
 * - Fines de semana: hasta lunes 9:30 AM ET
 * 
 * @returns {Date} Fecha de expiración del cache
 */
function calculateDynamicTTL() {
  const now = DateTime.now().setZone('America/New_York');
  const hour = now.hour + now.minute / 60;
  
  const MARKET_OPEN = 9.5;  // 9:30 AM
  const MARKET_CLOSE = 16;   // 4:00 PM
  
  // Sábado: válido hasta lunes 9:30 AM
  if (now.weekday === 6) {
    return now.plus({ days: 2 }).set({ hour: 9, minute: 30, second: 0, millisecond: 0 }).toJSDate();
  }
  
  // Domingo: válido hasta lunes 9:30 AM
  if (now.weekday === 7) {
    return now.plus({ days: 1 }).set({ hour: 9, minute: 30, second: 0, millisecond: 0 }).toJSDate();
  }
  
  // Durante horario de mercado: TTL de 2 minutos
  // (calcDailyPortfolioPerf corre cada 3 minutos, así que 2 min asegura datos frescos)
  if (hour >= MARKET_OPEN && hour < MARKET_CLOSE) {
    return now.plus({ minutes: 2 }).toJSDate();
  }
  
  // Después del cierre
  if (hour >= MARKET_CLOSE) {
    // Viernes después del cierre: válido hasta lunes 9:30 AM
    if (now.weekday === 5) {
      return now.plus({ days: 3 }).set({ hour: 9, minute: 30, second: 0, millisecond: 0 }).toJSDate();
    }
    // Lunes-Jueves después del cierre: válido hasta mañana 9:30 AM
    return now.plus({ days: 1 }).set({ hour: 9, minute: 30, second: 0, millisecond: 0 }).toJSDate();
  }
  
  // Antes de apertura: válido hasta las 9:30 AM de hoy
  return now.set({ hour: 9, minute: 30, second: 0, millisecond: 0 }).toJSDate();
}

/**
 * Invalida el cache de rendimientos para múltiples usuarios en batch
 * Optimizado para minimizar lecturas y escrituras de Firestore
 * 
 * @param {string[]} userIds - Array de IDs de usuarios
 * @returns {Promise<{usersProcessed: number, cachesDeleted: number}>}
 */
async function invalidatePerformanceCacheBatch(userIds) {
  if (!userIds || userIds.length === 0) {
    return { usersProcessed: 0, cachesDeleted: 0 };
  }

  let totalDeleted = 0;

  // Consultar todos los caches en paralelo (sin límite de usuarios ya que son pocos en producción)
  const cachePromises = userIds.map(async (userId) => {
    const cacheCollection = db.collection(`userData/${userId}/performanceCache`);
    const snapshot = await cacheCollection.limit(20).get();
    return { userId, docs: snapshot.docs };
  });
  
  const results = await Promise.all(cachePromises);
  
  // Agrupar todas las eliminaciones en un solo batch
  const deleteBatch = db.batch();
  
  for (const { docs } of results) {
    for (const doc of docs) {
      deleteBatch.delete(doc.ref);
      totalDeleted++;
    }
  }
  
  // Solo commit si hay documentos que eliminar
  if (totalDeleted > 0) {
    await deleteBatch.commit();
  }

  console.log(`[invalidatePerformanceCacheBatch] Eliminados ${totalDeleted} caches para ${userIds.length} usuarios`);
  return { usersProcessed: userIds.length, cachesDeleted: totalDeleted };
}

/**
 * FEAT-CHART-001: Cloud Function para obtener rendimientos históricos de múltiples cuentas agregados
 * 
 * Esta función permite obtener rendimientos combinados de un subconjunto de cuentas,
 * agregando los datos diarios de cada cuenta seleccionada.
 * 
 * Optimizaciones implementadas:
 * 1. Si accountIds incluye "overall" o "all" → usa getHistoricalReturns existente
 * 2. Si accountIds.length === 1 → usa getHistoricalReturns con esa cuenta
 * 3. Si accountIds incluye TODAS las cuentas del usuario → usa "overall"
 * 4. Cache específico para combinaciones multi-cuenta
 * 
 * @param {Object} request - Request con auth y data
 * @param {string[]} request.data.accountIds - Array de IDs de cuentas a agregar
 * @param {string} request.data.currency - Código de moneda (USD, COP, etc.)
 * @param {string|null} request.data.ticker - Ticker específico (opcional)
 * @param {string|null} request.data.assetType - Tipo de activo (opcional)
 * @param {boolean} request.data.forceRefresh - Forzar recálculo ignorando cache
 * @returns {Object} Rendimientos agregados calculados
 * 
 * @see docs/stories/24.story.md
 */
const getMultiAccountHistoricalReturns = onCall(callableConfig, withRateLimit('getMultiAccountHistoricalReturns')(async (request) => {
  const { auth, data } = request;

  // 1. Verificar autenticación
  if (!auth) {
    throw new HttpsError('unauthenticated', 'Debes iniciar sesión para acceder a los rendimientos');
  }
  const userId = auth.uid;

  const {
    accountIds = [],
    currency = "USD",
    ticker = null,
    assetType = null,
    forceRefresh = false
  } = data || {};

  // 2. Validación de parámetros
  if (!Array.isArray(accountIds) || accountIds.length === 0) {
    throw new HttpsError('invalid-argument', 'Debes proporcionar al menos una cuenta en accountIds');
  }

  console.log(`[getMultiAccountHistoricalReturns] Usuario: ${userId}, Cuentas: ${accountIds.length}, Moneda: ${currency}`);

  // 3. Optimización: Si es "overall" o "all", delegar a función existente
  if (accountIds.includes("overall") || accountIds.includes("all")) {
    console.log(`[getMultiAccountHistoricalReturns] Delegando a getHistoricalReturns (overall)`);
    // Llamar directamente a la lógica interna (no como Cloud Function)
    return await getHistoricalReturnsInternal(userId, {
      currency,
      accountId: "overall",
      ticker,
      assetType,
      forceRefresh
    });
  }

  // 4. Optimización: Si es una sola cuenta, delegar a función existente
  if (accountIds.length === 1) {
    console.log(`[getMultiAccountHistoricalReturns] Delegando a getHistoricalReturns (cuenta única: ${accountIds[0]})`);
    return await getHistoricalReturnsInternal(userId, {
      currency,
      accountId: accountIds[0],
      ticker,
      assetType,
      forceRefresh
    });
  }

  // 5. Verificar si se seleccionaron TODAS las cuentas del usuario
  const userAccountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  const userAccountIds = userAccountsSnapshot.docs.map(doc => doc.id);
  const sortedRequestedIds = [...accountIds].sort();
  const sortedUserIds = [...userAccountIds].sort();
  
  // Si seleccionó todas las cuentas, usar "overall" para mejor performance
  if (sortedRequestedIds.length === sortedUserIds.length && 
      sortedRequestedIds.every((id, i) => id === sortedUserIds[i])) {
    console.log(`[getMultiAccountHistoricalReturns] Todas las cuentas seleccionadas, usando "overall"`);
    return await getHistoricalReturnsInternal(userId, {
      currency,
      accountId: "overall",
      ticker,
      assetType,
      forceRefresh
    });
  }

  // 6. Multi-cuenta: Generar clave de cache
  const sortedIds = [...accountIds].sort().join('_');
  const cacheKey = `multi_${currency}_${sortedIds}${ticker ? `_${ticker}` : ''}${assetType ? `_${assetType}` : ''}`;

  console.log(`[getMultiAccountHistoricalReturns] Multi-cuenta, cache key: ${cacheKey}`);

  // 7. Verificar cache (si no forceRefresh)
  if (!forceRefresh) {
    try {
      const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
      const cacheDoc = await cacheRef.get();

      if (cacheDoc.exists) {
        const cache = cacheDoc.data();
        const validUntil = new Date(cache.validUntil);

        if (validUntil > new Date()) {
          console.log(`[getMultiAccountHistoricalReturns] Cache HIT para ${userId}/${cacheKey}`);
          return {
            ...cache.data,
            cacheHit: true,
            lastCalculated: cache.lastCalculated,
            validUntil: cache.validUntil
          };
        } else {
          console.log(`[getMultiAccountHistoricalReturns] Cache EXPIRED para ${userId}/${cacheKey}`);
        }
      }
    } catch (cacheError) {
      console.warn(`[getMultiAccountHistoricalReturns] Error leyendo cache: ${cacheError.message}`);
    }
  }

  console.log(`[getMultiAccountHistoricalReturns] Cache MISS - Agregando ${accountIds.length} cuentas`);

  // 8. Leer datos de cada cuenta en paralelo
  const accountDataPromises = accountIds.map(accountId => 
    db.collection(`portfolioPerformance/${userId}/accounts/${accountId}/dates`)
      .orderBy("date", "asc")
      .get()
  );
  
  const accountSnapshots = await Promise.all(accountDataPromises);

  // Verificar si hay datos
  const totalDocs = accountSnapshots.reduce((sum, snap) => sum + snap.size, 0);
  if (totalDocs === 0) {
    console.log(`[getMultiAccountHistoricalReturns] Sin datos de performance para cuentas seleccionadas`);
    return {
      returns: {
        ytdReturn: 0, oneMonthReturn: 0, threeMonthReturn: 0, sixMonthReturn: 0,
        oneYearReturn: 0, twoYearReturn: 0, fiveYearReturn: 0,
        hasYtdData: false, hasOneMonthData: false, hasThreeMonthData: false,
        hasSixMonthData: false, hasOneYearData: false, hasTwoYearData: false,
        hasFiveYearData: false
      },
      validDocsCountByPeriod: {
        ytd: 0, oneMonth: 0, threeMonths: 0, sixMonths: 0,
        oneYear: 0, twoYears: 0, fiveYears: 0
      },
      totalValueData: {
        dates: [], values: [], percentChanges: [], overallPercentChange: 0
      },
      performanceByYear: {},
      availableYears: [],
      startDate: "",
      monthlyCompoundData: {},
      cacheHit: false,
      lastCalculated: new Date().toISOString()
    };
  }

  // 9. Agregar datos por fecha
  const aggregatedByDate = new Map();

  accountSnapshots.forEach(snapshot => {
    snapshot.docs.forEach(doc => {
      const data = doc.data();
      const date = data.date;
      
      if (!aggregatedByDate.has(date)) {
        aggregatedByDate.set(date, {
          date,
          currencies: {},
          // FEAT-CHART-001: Guardar datos por cuenta para ponderar correctamente
          accountData: {}
        });
      }
      
      const existing = aggregatedByDate.get(date);
      
      // Agregar métricas por moneda
      Object.keys(data).forEach(key => {
        if (key === 'date') return;
        
        const currencyCode = key;
        const currencyData = data[currencyCode];
        
        if (!currencyData || typeof currencyData !== 'object') return;
        
        if (!existing.currencies[currencyCode]) {
          existing.currencies[currencyCode] = {
            totalInvestment: 0,
            totalValue: 0,
            totalCashFlow: 0,
            unrealizedProfitAndLoss: 0,
            doneProfitAndLoss: 0,
            assetPerformance: {},
            // Para ponderar el cambio diario
            _accountContributions: []
          };
        }
        
        // Guardar contribución de esta cuenta para ponderar el cambio diario
        // El adjustedDailyChangePercentage ya viene pre-calculado correctamente en Firestore
        // FIX-MULTI-001: Agregar totalCashFlow para detectar cuentas nuevas (depósitos iniciales)
        existing.currencies[currencyCode]._accountContributions.push({
          totalValue: currencyData.totalValue || 0,
          adjustedDailyChangePercentage: currencyData.adjustedDailyChangePercentage || 0,
          rawDailyChangePercentage: currencyData.rawDailyChangePercentage || currencyData.dailyChangePercentage || 0,
          totalCashFlow: currencyData.totalCashFlow || 0
        });
        
        // Sumar métricas aditivas
        existing.currencies[currencyCode].totalInvestment += currencyData.totalInvestment || 0;
        existing.currencies[currencyCode].totalValue += currencyData.totalValue || 0;
        existing.currencies[currencyCode].totalCashFlow += currencyData.totalCashFlow || 0;
        existing.currencies[currencyCode].unrealizedProfitAndLoss += currencyData.unrealizedProfitAndLoss || 0;
        existing.currencies[currencyCode].doneProfitAndLoss += currencyData.doneProfitAndLoss || 0;
        
        // Agregar assets para detalle
        if (currencyData.assetPerformance) {
          Object.entries(currencyData.assetPerformance).forEach(([assetKey, assetData]) => {
            if (!existing.currencies[currencyCode].assetPerformance[assetKey]) {
              existing.currencies[currencyCode].assetPerformance[assetKey] = {
                totalInvestment: 0,
                totalValue: 0,
                totalCashFlow: 0,
                units: 0,
                unrealizedProfitAndLoss: 0,
                doneProfitAndLoss: 0
              };
            }
            
            const existingAsset = existing.currencies[currencyCode].assetPerformance[assetKey];
            existingAsset.totalInvestment += assetData.totalInvestment || 0;
            existingAsset.totalValue += assetData.totalValue || 0;
            existingAsset.totalCashFlow += assetData.totalCashFlow || 0;
            existingAsset.units += assetData.units || 0;
            existingAsset.unrealizedProfitAndLoss += assetData.unrealizedProfitAndLoss || 0;
            existingAsset.doneProfitAndLoss += assetData.doneProfitAndLoss || 0;
          });
        }
      });
    });
  });

  // 10. Calcular rendimiento ponderado por valor para cada día
  // 
  // FIX OPT-018: Usar el VALOR PRE-CAMBIO para ponderar
  // 
  // El problema anterior era que usábamos totalValue (que ya incluye el cambio del día)
  // para calcular los pesos. Esto causaba que las cuentas con mayores ganancias
  // tuvieran más peso, inflando el rendimiento combinado.
  //
  // Solución: Calcular el valor "pre-cambio" de cada cuenta:
  //   valorPreCambio = valorActual / (1 + cambio/100)
  //
  // Fórmula corregida de ponderación:
  // portfolioChange = Σ(cuenta_adjustedChange × cuenta_valorPreCambio) / Σ(cuenta_valorPreCambio)
  //
  const sortedDates = Array.from(aggregatedByDate.keys()).sort();
  
  sortedDates.forEach((date, index) => {
    const dateData = aggregatedByDate.get(date);
    
    Object.keys(dateData.currencies).forEach(currencyCode => {
      const c = dateData.currencies[currencyCode];
      
      // ROI total
      c.totalROI = c.totalInvestment > 0 
        ? ((c.totalValue - c.totalInvestment) / c.totalInvestment) * 100 
        : 0;
      
      // FIX OPT-018 + FIX-MULTI-001: Calcular rendimiento diario usando fórmula directa
      //
      // Problema original: El promedio ponderado por valor pre-cambio no era consistente
      // con el cálculo del overall cuando había cuentas nuevas (primer día con datos).
      //
      // Una cuenta nueva infla el denominador del promedio ponderado porque su
      // preChangeValue = currValue (ya que change=0%), pero en realidad no existía ayer.
      //
      // Solución: Usar la fórmula directa del overall:
      //   change = (currValue - prevValue + cashFlow) / prevValue × 100
      //
      // Donde prevValue = suma de valores pre-cambio de cuentas EXISTENTES
      // (excluyendo cuentas nuevas identificadas por: change=0% && cashFlow<0 && value>0)
      //
      // Esta fórmula es matemáticamente equivalente al promedio ponderado para días
      // normales, y correcta para días con cuentas nuevas.
      
      const contributions = c._accountContributions || [];
      
      // Identificar cuentas existentes (no nuevas) para calcular prevValue
      const existingAccountsData = contributions.filter(acc => {
        const isNewAccount = 
          acc.adjustedDailyChangePercentage === 0 && 
          acc.totalCashFlow < 0 &&  // Depósito inicial
          acc.totalValue > 0;
        return !isNewAccount;
      });
      
      // Calcular valor pre-cambio para cuentas existentes
      // FIX-MULTI-001: La fórmula correcta para calcular prevValue es:
      //   prevValue = (currValue + cashFlow) / (1 + change/100)
      // 
      // Esto es porque el adjustedDailyChangePercentage se calcula como:
      //   adjustedChange = (currValue - prevValue + cashFlow) / prevValue × 100
      //
      // Despejando:
      //   prevValue × (1 + adjustedChange/100) = currValue + cashFlow
      //   prevValue = (currValue + cashFlow) / (1 + adjustedChange/100)
      //
      // NOTA: cashFlow es negativo para depósitos, positivo para retiros
      const contributionsWithPreValue = existingAccountsData.map(acc => {
        const change = acc.adjustedDailyChangePercentage || 0;
        const currentValue = acc.totalValue || 0;
        const cashFlow = acc.totalCashFlow || 0;
        
        // FIX: Incluir cashFlow en el cálculo del prevValue
        const preChangeValue = change !== 0 
          ? (currentValue + cashFlow) / (1 + change / 100) 
          : currentValue + cashFlow;  // Si change=0, prevValue = currValue + cashFlow
        
        return { ...acc, preChangeValue: Math.max(0, preChangeValue) };  // Evitar valores negativos
      });
      
      // totalWeight = suma de valores pre-cambio de cuentas EXISTENTES (prevValue)
      const totalWeight = contributionsWithPreValue.reduce((sum, acc) => sum + acc.preChangeValue, 0);
      
      // FIX-MULTI-001: Calcular usando la fórmula directa para consistencia con overall
      // 
      // La fórmula del overall es:
      //   change = (currValue - prevValue + cashFlow) / prevValue × 100
      //
      // Para multi-cuenta, adaptamos esto como:
      //   change = (currValue_total - prevValue_existentes + cashFlow_total) / prevValue_existentes × 100
      //
      // Donde:
      //   - currValue_total = suma de valores de TODAS las cuentas (existentes + nuevas)
      //   - prevValue_existentes = suma de valores pre-cambio de cuentas EXISTENTES (totalWeight)
      //   - cashFlow_total = suma de cashflow de TODAS las cuentas
      //
      // Esto garantiza consistencia con el cálculo del overall en portfolioCalculations.js
      
      const totalCashFlow = contributions.reduce((sum, acc) => sum + (acc.totalCashFlow || 0), 0);
      const totalCurrentValue = contributions.reduce((sum, acc) => sum + (acc.totalValue || 0), 0);
      
      if (totalWeight > 0 && contributionsWithPreValue.length > 0) {
        // Fórmula directa: (currValue - prevValue + cashFlow) / prevValue × 100
        const adjustedDailyChange = ((totalCurrentValue - totalWeight + totalCashFlow) / totalWeight) * 100;
        
        // Para raw change, no incluimos el cashflow
        const rawDailyChange = ((totalCurrentValue - totalWeight) / totalWeight) * 100;
        
        c.dailyChangePercentage = rawDailyChange;
        c.rawDailyChangePercentage = rawDailyChange;
        c.adjustedDailyChangePercentage = adjustedDailyChange;
      } else {
        // Si no hay peso (primer día o sin datos), usar 0
        c.dailyChangePercentage = 0;
        c.rawDailyChangePercentage = 0;
        c.adjustedDailyChangePercentage = 0;
      }
      
      // Limpiar el campo temporal
      delete c._accountContributions;
      
      // Calcular ROI para cada asset
      Object.values(c.assetPerformance).forEach(assetData => {
        assetData.totalROI = assetData.totalInvestment > 0
          ? ((assetData.totalValue - assetData.totalInvestment) / assetData.totalInvestment) * 100
          : 0;
      });
    });
  });

  // 11. Convertir a formato de docs para usar calculateHistoricalReturns
  const aggregatedDocs = sortedDates.map(date => {
    const dateData = aggregatedByDate.get(date);
    return {
      data: () => ({
        date: dateData.date,
        ...dateData.currencies
      })
    };
  });

  // Logging de diagnóstico para los últimos días
  if (sortedDates.length > 0) {
    const lastDate = sortedDates[sortedDates.length - 1];
    const lastData = aggregatedByDate.get(lastDate);
    const currencyData = lastData.currencies[currency];
    
    console.log(`[getMultiAccountHistoricalReturns] Último día (${lastDate}):`, {
      totalValue: currencyData?.totalValue,
      totalInvestment: currencyData?.totalInvestment,
      adjustedDailyChangePercentage: currencyData?.adjustedDailyChangePercentage?.toFixed(4),
      rawDailyChangePercentage: currencyData?.rawDailyChangePercentage?.toFixed(4)
    });
    
    // Log de algunos días intermedios para verificar la ponderación
    if (sortedDates.length > 5) {
      const midDate = sortedDates[Math.floor(sortedDates.length / 2)];
      const midData = aggregatedByDate.get(midDate);
      const midCurrencyData = midData.currencies[currency];
      console.log(`[getMultiAccountHistoricalReturns] Día intermedio (${midDate}):`, {
        adjustedDailyChangePercentage: midCurrencyData?.adjustedDailyChangePercentage?.toFixed(4)
      });
    }
  }

  console.log(`[getMultiAccountHistoricalReturns] Procesando ${aggregatedDocs.length} fechas agregadas`);

  // 12. Usar función existente para calcular rendimientos
  const result = calculateHistoricalReturns(aggregatedDocs, currency, ticker, assetType);

  // 13. Guardar en cache
  const now = new Date();
  const validUntil = calculateDynamicTTL();

  const cacheData = {
    data: result,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };

  try {
    const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
    await cacheRef.set(cacheData);
    console.log(`[getMultiAccountHistoricalReturns] Cache guardado para ${userId}/${cacheKey}`);
  } catch (cacheWriteError) {
    console.error(`[getMultiAccountHistoricalReturns] Error guardando cache: ${cacheWriteError.message}`);
  }

  return {
    ...result,
    cacheHit: false,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };
}));

/**
 * Función interna para obtener rendimientos históricos (sin verificación de auth)
 * Usada para delegación desde getMultiAccountHistoricalReturns
 * 
 * @param {string} userId - ID del usuario
 * @param {Object} params - Parámetros de la consulta
 * @returns {Object} Rendimientos calculados
 */
async function getHistoricalReturnsInternal(userId, params) {
  const {
    currency = "USD",
    accountId = "overall",
    ticker = null,
    assetType = null,
    forceRefresh = false
  } = params;

  // Generar clave de cache
  const cacheKey = `${currency}_${accountId}${ticker ? `_${ticker}` : ''}${assetType ? `_${assetType}` : ''}`;

  console.log(`[getHistoricalReturnsInternal] Usuario: ${userId}, Cache key: ${cacheKey}`);

  // Verificar cache (si no forceRefresh)
  if (!forceRefresh) {
    try {
      const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
      const cacheDoc = await cacheRef.get();

      if (cacheDoc.exists) {
        const cache = cacheDoc.data();
        const validUntil = new Date(cache.validUntil);

        if (validUntil > new Date()) {
          console.log(`[getHistoricalReturnsInternal] Cache HIT para ${userId}/${cacheKey}`);
          return {
            ...cache.data,
            cacheHit: true,
            lastCalculated: cache.lastCalculated,
            validUntil: cache.validUntil
          };
        }
      }
    } catch (cacheError) {
      console.warn(`[getHistoricalReturnsInternal] Error leyendo cache: ${cacheError.message}`);
    }
  }

  // Obtener documentos de performance
  const basePath = accountId === "overall"
    ? `portfolioPerformance/${userId}/dates`
    : `portfolioPerformance/${userId}/accounts/${accountId}/dates`;

  const performanceSnapshot = await db.collection(basePath)
    .orderBy("date", "asc")
    .get();

  if (performanceSnapshot.empty) {
    return {
      returns: {
        ytdReturn: 0, oneMonthReturn: 0, threeMonthReturn: 0, sixMonthReturn: 0,
        oneYearReturn: 0, twoYearReturn: 0, fiveYearReturn: 0,
        hasYtdData: false, hasOneMonthData: false, hasThreeMonthData: false,
        hasSixMonthData: false, hasOneYearData: false, hasTwoYearData: false,
        hasFiveYearData: false
      },
      validDocsCountByPeriod: {
        ytd: 0, oneMonth: 0, threeMonths: 0, sixMonths: 0,
        oneYear: 0, twoYears: 0, fiveYears: 0
      },
      totalValueData: {
        dates: [], values: [], percentChanges: [], overallPercentChange: 0
      },
      performanceByYear: {},
      availableYears: [],
      startDate: "",
      monthlyCompoundData: {},
      cacheHit: false,
      lastCalculated: new Date().toISOString()
    };
  }

  // Ejecutar cálculos
  const result = calculateHistoricalReturns(
    performanceSnapshot.docs,
    currency,
    ticker,
    assetType
  );

  // Guardar en cache
  const now = new Date();
  const validUntil = calculateDynamicTTL();

  const cacheData = {
    data: result,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };

  try {
    const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
    await cacheRef.set(cacheData);
  } catch (cacheWriteError) {
    console.error(`[getHistoricalReturnsInternal] Error guardando cache: ${cacheWriteError.message}`);
  }

  return {
    ...result,
    cacheHit: false,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };
}

module.exports = {
  getHistoricalReturns,
  getMultiAccountHistoricalReturns,
  invalidatePerformanceCache,
  invalidatePerformanceCacheBatch,
  calculateDynamicTTL,
  // SCALE-CF-001: Exportar funciones de cálculo para handlers unificados
  calculateHistoricalReturns,
  getHistoricalReturnsInternal,
};
