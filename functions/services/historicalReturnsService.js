/**
 * Cloud Function: getHistoricalReturns
 * 
 * Pre-calcula los rendimientos históricos del portafolio en el servidor
 * y los cachea para reducir lecturas de Firestore de ~200 a 1.
 * 
 * @module historicalReturnsService
 * @see docs/stories/7.story.md (OPT-002)
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require('./firebaseAdmin');
const { DateTime } = require('luxon');

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
 * Umbrales mínimos de documentos por período para considerar datos suficientes
 */
const MIN_DOCS = {
  ONE_MONTH: 21,
  THREE_MONTHS: 63,
  SIX_MONTHS: 126,
  YTD: 1,
  ONE_YEAR: 252,
  TWO_YEARS: 504,
  FIVE_YEARS: 1260
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

  // Datos para rendimiento mensual
  const datesByMonth = {};
  const lastDaysByMonth = {};
  const monthlyStartFactors = {};
  const monthlyEndFactors = {};
  const monthlyCompoundData = {};

  // Primera pasada: agrupar fechas por mes
  documents.forEach((doc) => {
    const data = doc.data ? doc.data() : doc;
    const date = DateTime.fromISO(data.date);
    const year = date.year.toString();
    const month = (date.month - 1).toString();

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
      }
    }

    // Actualizar factor compuesto
    if (hasData) {
      currentFactor = currentFactor * (1 + adjustedDailyChange / 100);

      const date = DateTime.fromISO(data.date);
      const year = date.year.toString();
      const month = (date.month - 1).toString();

      const isLastDayOfMonth = lastDaysByMonth[year]?.[month] === data.date;

      if (!monthlyStartFactors[year][month]) {
        monthlyStartFactors[year][month] = currentFactor;
      }
      monthlyEndFactors[year][month] = currentFactor;

      if (!monthlyCompoundData[year][month]) {
        monthlyCompoundData[year][month] = {
          startFactor: currentFactor,
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

  // Calcular rendimientos mensuales
  const performanceByYear = {};

  Object.keys(monthlyStartFactors).forEach(year => {
    performanceByYear[year] = { months: {}, total: 0 };
    let compoundTotal = 1;

    // Inicializar meses con 0
    for (let i = 0; i < 12; i++) {
      performanceByYear[year].months[i.toString()] = 0;
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
        }

        compoundTotal = compoundTotal * (1 + monthReturn / 100);
      }
    });

    performanceByYear[year].total = (compoundTotal - 1) * 100;
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
    let firstValidMonth = "11";

    for (let month = 0; month < 12; month++) {
      if (performanceByYear[firstValidYear].months[month.toString()] !== 0) {
        firstValidMonth = month.toString();
        break;
      }
    }

    const firstDate = DateTime.fromObject({
      year: parseInt(firstValidYear),
      month: parseInt(firstValidMonth) + 1,
      day: 1
    });

    startDate = firstDate.toFormat("dd/MM/yy");
  }

  // Calcular mínimo de documentos para YTD
  const currentMonth = now.month;
  const minDocsForYtd = Math.max(Math.ceil(currentMonth * 4 / 12), MIN_DOCS.YTD);

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
  const hasEnoughOneMonthDocs = oneMonthCount >= MIN_DOCS.ONE_MONTH;
  const hasEnoughThreeMonthDocs = threeMonthCount >= MIN_DOCS.THREE_MONTHS;
  const hasEnoughSixMonthDocs = sixMonthCount >= MIN_DOCS.SIX_MONTHS;
  const hasEnoughOneYearDocs = oneYearCount >= MIN_DOCS.ONE_YEAR;
  const hasEnoughTwoYearDocs = twoYearCount >= MIN_DOCS.TWO_YEARS;
  const hasEnoughFiveYearDocs = fiveYearCount >= MIN_DOCS.FIVE_YEARS;

  // Calcular cambio porcentual total
  let overallPercentChange = 0;
  if (totalValueValues.length >= 2) {
    const initialValue = totalValueValues[0];
    const finalValue = totalValueValues[totalValueValues.length - 1];
    overallPercentChange = ((finalValue - initialValue) / initialValue) * 100;
  }

  return {
    returns: {
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
      hasFiveYearData: foundFiveYearStart && hasEnoughFiveYearDocs
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
const getHistoricalReturns = onCall(callableConfig, async (request) => {
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
});

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
  
  // Durante horario de mercado: TTL de 5 minutos
  if (hour >= MARKET_OPEN && hour < MARKET_CLOSE) {
    return now.plus({ minutes: 5 }).toJSDate();
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

module.exports = {
  getHistoricalReturns,
  invalidatePerformanceCache,
  invalidatePerformanceCacheBatch,
  calculateDynamicTTL
};
