/**
 * Query Handlers - Lógica de negocio para operaciones de consulta
 * 
 * SCALE-CF-001: Handlers para consolidación de Cloud Functions de consulta.
 * Incluye funciones migradas desde:
 * - historicalReturnsService.js
 * - userPricesService.js  
 * - indexHistoryService.js
 * - index.js (inline: getPortfolioDistribution, getAvailableSectors)
 * 
 * OPT-DEMAND-CLEANUP: getCurrentPricesForUser migrado para usar API Lambda
 * en lugar de Firestore collection('currentPrices').
 * 
 * @module handlers/queryHandlers
 * @see docs/stories/56.story.md
 * @see docs/architecture/OPT-DEMAND-CLEANUP-firestore-fallback-removal.md
 */

const { HttpsError } = require("firebase-functions/v2/https");
const admin = require('../firebaseAdmin');
const db = admin.firestore();

// ============================================================================
// IMPORTS DE SERVICIOS
// ============================================================================

// Servicios con lógica compleja que se reutilizan
const portfolioDistributionService = require('../portfolioDistributionService');
const { 
  calculateHistoricalReturns, 
  getHistoricalReturnsInternal 
} = require('../historicalReturnsService');
const { calculateDynamicTTL } = require('../cacheInvalidationService');
const { calculateIndexData } = require('../indexHistoryService');
const { DateTime } = require('luxon');

// COST-OPT-001: Importar servicio de rendimientos consolidados (V2)
const { 
  getHistoricalReturnsV2,
  checkConsolidatedDataStatus 
} = require('../consolidatedReturnsService');

// Importar utilidades MWR para cálculos de multi-account
const {
  calculateSimplePersonalReturn,
  calculateModifiedDietzReturn
} = require('../../utils/mwrCalculations');

// OPT-DEMAND-CLEANUP: Importar helper para obtener precios del API Lambda
const { getPricesFromApi } = require('../marketDataHelper');

// ============================================================================
// CONSTANTES
// ============================================================================

const VALID_INDEX_RANGES = ["1M", "3M", "6M", "YTD", "1Y", "5Y", "MAX"];
const INDEX_CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 horas

// ============================================================================
// HANDLERS
// ============================================================================

/**
 * Obtiene precios actuales filtrados por los símbolos que el usuario posee
 * 
 * OPT-DEMAND-CLEANUP: Migrado para usar API Lambda en lugar de Firestore.
 * Ya NO lee de collection('currentPrices').
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de consulta (vacío para este handler)
 * @returns {Promise<{prices: Array, symbols: Array, timestamp: number, source: string}>}
 */
async function getCurrentPricesForUser(context, payload) {
  const { auth } = context;
  const userId = auth.uid;

  console.log(`[queryHandlers][getCurrentPricesForUser] userId: ${userId}`);

  try {
    // 1. Obtener portfolioAccounts activas del usuario
    const accountsSnapshot = await db.collection('portfolioAccounts')
      .where('userId', '==', userId)
      .where('isActive', '==', true)
      .get();
    
    if (accountsSnapshot.empty) {
      console.log(`[queryHandlers][getCurrentPricesForUser] Usuario sin cuentas activas`);
      return { prices: [], symbols: [], timestamp: Date.now(), source: 'none' };
    }
    
    const accountIds = accountsSnapshot.docs.map(doc => doc.id);
    
    // 2. Obtener símbolos únicos de assets activos
    const symbolsSet = new Set();
    
    for (let i = 0; i < accountIds.length; i += 10) {
      const batchAccountIds = accountIds.slice(i, i + 10);
      
      const assetsSnapshot = await db.collection('assets')
        .where('portfolioAccount', 'in', batchAccountIds)
        .where('isActive', '==', true)
        .get();
      
      assetsSnapshot.docs.forEach(doc => {
        const name = doc.data().name;
        if (name) {
          symbolsSet.add(name);
        }
      });
    }
    
    const symbols = Array.from(symbolsSet);
    
    if (symbols.length === 0) {
      console.log(`[queryHandlers][getCurrentPricesForUser] Usuario sin assets activos`);
      return { prices: [], symbols: [], timestamp: Date.now(), source: 'none' };
    }
    
    // OPT-DEMAND-CLEANUP: Obtener precios del API Lambda en lugar de Firestore
    console.log(`[queryHandlers][getCurrentPricesForUser] Consultando API Lambda para ${symbols.length} símbolos`);
    
    const pricesFromApi = await getPricesFromApi(symbols);
    
    // Formatear respuesta para mantener compatibilidad con el frontend
    const prices = pricesFromApi.map(price => ({
      id: price.symbol,
      symbol: price.symbol,
      ...price
    }));
    
    console.log(`[queryHandlers][getCurrentPricesForUser] Éxito - ${prices.length} precios desde API Lambda`);
    
    return {
      prices,
      symbols,
      timestamp: Date.now(),
      source: 'api-lambda'  // OPT-DEMAND-CLEANUP: Indicar fuente de datos
    };

  } catch (error) {
    console.error(`[queryHandlers][getCurrentPricesForUser] Error:`, error);
    throw new HttpsError('internal', 'Error al obtener precios del usuario');
  }
}

/**
 * Obtiene rendimientos históricos del portafolio
 * 
 * COST-OPT-003: Ahora usa V2 (períodos consolidados) con fallback automático a V1
 * si no hay datos consolidados. Esto reduce lecturas de ~300 a ~20 documentos.
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de consulta
 * @returns {Promise<Object>} Rendimientos calculados
 */
async function getHistoricalReturns(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { 
    currency = "USD", 
    accountId = "overall", 
    ticker = null, 
    assetType = null, 
    forceRefresh = false 
  } = payload || {};

  // Generar clave de cache
  const cacheKey = `${currency}_${accountId}${ticker ? `_${ticker}` : ''}${assetType ? `_${assetType}` : ''}`;
  
  console.log(`[queryHandlers][getHistoricalReturns] userId: ${userId}, cacheKey: ${cacheKey}`);

  try {
    // 1. Verificar cache (si no forceRefresh)
    if (!forceRefresh) {
      const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
      const cacheDoc = await cacheRef.get();

      if (cacheDoc.exists) {
        const cacheData = cacheDoc.data();
        const validUntil = new Date(cacheData.validUntil);

        if (validUntil > new Date()) {
          console.log(`[queryHandlers][getHistoricalReturns] Cache hit`);
          return {
            ...cacheData.data,
            cacheHit: true,
            lastCalculated: cacheData.lastCalculated,
            validUntil: cacheData.validUntil
          };
        }
      }
    }

    // COST-OPT-003: Usar V2 (períodos consolidados) con fallback a V1
    // Esto reduce lecturas de ~300+ documentos a ~20 documentos
    const result = await getHistoricalReturnsV2(userId, {
      currency,
      accountId,
      ticker,
      assetType,
      forceRefresh,
      fallbackToV1: true  // Fallback automático si no hay datos consolidados
    });

    // 2. Guardar en cache
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
      console.error(`[queryHandlers][getHistoricalReturns] Error guardando cache:`, cacheWriteError);
    }

    const version = result._metadata?.version || 'v1';
    console.log(`[queryHandlers][getHistoricalReturns] Éxito - version: ${version}`);
    
    return {
      ...result,
      cacheHit: false,
      lastCalculated: now.toISOString(),
      validUntil: validUntil.toISOString()
    };

  } catch (error) {
    console.error(`[queryHandlers][getHistoricalReturns] Error:`, error);
    if (error instanceof HttpsError) throw error;
    throw new HttpsError('internal', 'Error calculando rendimientos históricos');
  }
}

/**
 * Obtiene rendimientos históricos multi-cuenta
 * 
 * SCALE-CF-001: Implementación completa de agregación multi-cuenta
 * Migrada desde historicalReturnsService.getMultiAccountHistoricalReturns
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de consulta
 * @returns {Promise<Object>} Rendimientos agregados de múltiples cuentas
 */
async function getMultiAccountHistoricalReturns(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { 
    accountIds = [], 
    currency = "USD", 
    ticker = null, 
    assetType = null, 
    forceRefresh = false 
  } = payload || {};

  console.log(`[queryHandlers][getMultiAccountHistoricalReturns] userId: ${userId}, accounts: ${accountIds.length}`);

  try {
    // Validación de parámetros
    if (!Array.isArray(accountIds) || accountIds.length === 0) {
      throw new HttpsError('invalid-argument', 'Debes proporcionar al menos una cuenta en accountIds');
    }

    // Si es "overall" o "all", delegar a getHistoricalReturnsInternal
    if (accountIds.includes("overall") || accountIds.includes("all")) {
      console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Delegando a internal (overall)`);
      return await getHistoricalReturnsInternal(userId, {
        currency,
        accountId: "overall",
        ticker,
        assetType,
        forceRefresh
      });
    }

    // Si es una sola cuenta, delegar
    if (accountIds.length === 1) {
      console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Delegando a internal (cuenta única)`);
      return await getHistoricalReturnsInternal(userId, {
        currency,
        accountId: accountIds[0],
        ticker,
        assetType,
        forceRefresh
      });
    }

    // Verificar si se seleccionaron TODAS las cuentas del usuario
    const userAccountsSnapshot = await db.collection('portfolioAccounts')
      .where('userId', '==', userId)
      .where('isActive', '==', true)
      .get();
    
    const userAccountIds = userAccountsSnapshot.docs.map(doc => doc.id);
    const sortedRequestedIds = [...accountIds].sort();
    const sortedUserIds = [...userAccountIds].sort();
    
    if (sortedRequestedIds.length === sortedUserIds.length && 
        sortedRequestedIds.every((id, i) => id === sortedUserIds[i])) {
      console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Todas las cuentas, usando overall`);
      return await getHistoricalReturnsInternal(userId, {
        currency,
        accountId: "overall",
        ticker,
        assetType,
        forceRefresh
      });
    }

    // ============================================================================
    // MULTI-CUENTA REAL: Agregar datos de múltiples cuentas
    // ============================================================================
    
    // Generar clave de cache
    const sortedIds = [...accountIds].sort().join('_');
    const cacheKey = `multi_${currency}_${sortedIds}${ticker ? `_${ticker}` : ''}${assetType ? `_${assetType}` : ''}`;

    console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Multi-cuenta, cache key: ${cacheKey}`);

    // Verificar cache (si no forceRefresh)
    if (!forceRefresh) {
      try {
        const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
        const cacheDoc = await cacheRef.get();

        if (cacheDoc.exists) {
          const cache = cacheDoc.data();
          const validUntil = new Date(cache.validUntil);

          if (validUntil > new Date()) {
            console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Cache HIT`);
            return {
              ...cache.data,
              cacheHit: true,
              lastCalculated: cache.lastCalculated,
              validUntil: cache.validUntil
            };
          }
        }
      } catch (cacheError) {
        console.warn(`[queryHandlers][getMultiAccountHistoricalReturns] Error leyendo cache:`, cacheError.message);
      }
    }

    console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Cache MISS - Agregando ${accountIds.length} cuentas`);

    // Leer datos de cada cuenta en paralelo
    const accountDataPromises = accountIds.map(accountId => 
      db.collection(`portfolioPerformance/${userId}/accounts/${accountId}/dates`)
        .orderBy("date", "asc")
        .get()
    );
    
    const accountSnapshots = await Promise.all(accountDataPromises);

    // Verificar si hay datos
    const totalDocs = accountSnapshots.reduce((sum, snap) => sum.size + snap.size, 0);
    if (totalDocs === 0) {
      console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Sin datos de performance`);
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

    // Agregar datos por fecha
    const aggregatedByDate = new Map();

    accountSnapshots.forEach(snapshot => {
      snapshot.docs.forEach(doc => {
        const data = doc.data();
        const date = data.date;
        
        if (!aggregatedByDate.has(date)) {
          aggregatedByDate.set(date, {
            date,
            currencies: {}
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
              _accountContributions: []
            };
          }
          
          // Guardar contribución de esta cuenta para ponderar el cambio diario
          existing.currencies[currencyCode]._accountContributions.push({
            totalValue: currencyData.totalValue || 0,
            adjustedDailyChangePercentage: currencyData.adjustedDailyChangePercentage || 0,
            rawDailyChangePercentage: currencyData.rawDailyChangePercentage || currencyData.dailyChangePercentage || 0
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

    // Calcular rendimiento ponderado por valor para cada día
    // Usar el VALOR PRE-CAMBIO para ponderar correctamente
    const sortedDates = Array.from(aggregatedByDate.keys()).sort();
    
    sortedDates.forEach(date => {
      const dateData = aggregatedByDate.get(date);
      
      Object.keys(dateData.currencies).forEach(currencyCode => {
        const c = dateData.currencies[currencyCode];
        
        // ROI total
        c.totalROI = c.totalInvestment > 0 
          ? ((c.totalValue - c.totalInvestment) / c.totalInvestment) * 100 
          : 0;
        
        // Calcular valor PRE-CAMBIO de cada cuenta para ponderar
        const contributions = c._accountContributions || [];
        
        const contributionsWithPreValue = contributions.map(acc => {
          const change = acc.adjustedDailyChangePercentage || 0;
          const currentValue = acc.totalValue || 0;
          const preChangeValue = change !== 0 ? currentValue / (1 + change / 100) : currentValue;
          return { ...acc, preChangeValue };
        });
        
        const totalWeight = contributionsWithPreValue.reduce((sum, acc) => sum + acc.preChangeValue, 0);
        
        if (totalWeight > 0 && contributionsWithPreValue.length > 0) {
          const weightedAdjustedChange = contributionsWithPreValue.reduce((sum, acc) => {
            const weight = acc.preChangeValue / totalWeight;
            return sum + (acc.adjustedDailyChangePercentage || 0) * weight;
          }, 0);
          
          const weightedRawChange = contributionsWithPreValue.reduce((sum, acc) => {
            const weight = acc.preChangeValue / totalWeight;
            return sum + (acc.rawDailyChangePercentage || 0) * weight;
          }, 0);
          
          c.dailyChangePercentage = weightedRawChange;
          c.rawDailyChangePercentage = weightedRawChange;
          c.adjustedDailyChangePercentage = weightedAdjustedChange;
        } else {
          c.dailyChangePercentage = 0;
          c.rawDailyChangePercentage = 0;
          c.adjustedDailyChangePercentage = 0;
        }
        
        delete c._accountContributions;
        
        // Calcular ROI para cada asset
        Object.values(c.assetPerformance).forEach(assetData => {
          assetData.totalROI = assetData.totalInvestment > 0
            ? ((assetData.totalValue - assetData.totalInvestment) / assetData.totalInvestment) * 100
            : 0;
        });
      });
    });

    // Convertir a formato de docs para usar calculateHistoricalReturns
    const aggregatedDocs = sortedDates.map(date => {
      const dateData = aggregatedByDate.get(date);
      return {
        data: () => ({
          date: dateData.date,
          ...dateData.currencies
        })
      };
    });

    console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Procesando ${aggregatedDocs.length} fechas agregadas`);

    // Usar función existente para calcular rendimientos
    const result = calculateHistoricalReturns(aggregatedDocs, currency, ticker, assetType);

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
      console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Cache guardado`);
    } catch (cacheWriteError) {
      console.error(`[queryHandlers][getMultiAccountHistoricalReturns] Error guardando cache:`, cacheWriteError.message);
    }

    return {
      ...result,
      cacheHit: false,
      lastCalculated: now.toISOString(),
      validUntil: validUntil.toISOString()
    };

  } catch (error) {
    console.error(`[queryHandlers][getMultiAccountHistoricalReturns] Error:`, error);
    if (error instanceof HttpsError) throw error;
    throw new HttpsError('internal', 'Error calculando rendimientos multi-cuenta');
  }
}

/**
 * Obtiene datos históricos de un índice de mercado
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de consulta
 * @returns {Promise<Object>} Datos del índice
 */
async function getIndexHistory(context, payload) {
  const { auth } = context;
  const { code, range } = payload || {};

  console.log(`[queryHandlers][getIndexHistory] userId: ${auth.uid}, code: ${code}, range: ${range}`);

  // Validación de parámetros
  if (!code || typeof code !== 'string') {
    throw new HttpsError("invalid-argument", "El parámetro 'code' es requerido");
  }

  if (!range || typeof range !== 'string') {
    throw new HttpsError("invalid-argument", "El parámetro 'range' es requerido");
  }

  if (!VALID_INDEX_RANGES.includes(range)) {
    throw new HttpsError(
      "invalid-argument", 
      `El parámetro 'range' debe ser uno de: ${VALID_INDEX_RANGES.join(", ")}`
    );
  }

  const cacheKey = `${code}_${range}`;
  const cacheRef = db.collection("indexCache").doc(cacheKey);

  try {
    // 1. Intentar obtener del cache
    const cacheDoc = await cacheRef.get();
    
    if (cacheDoc.exists) {
      const cacheData = cacheDoc.data();
      const cacheAge = Date.now() - (cacheData.lastUpdated || 0);

      if (cacheAge < INDEX_CACHE_TTL_MS) {
        console.log(`[queryHandlers][getIndexHistory] Cache hit para ${cacheKey}`);
        return {
          chartData: cacheData.chartData || [],
          overallChange: cacheData.overallChange || 0,
          latestValue: cacheData.latestValue || 0,
          indexInfo: cacheData.indexInfo || { name: code, region: "Unknown", code },
          cacheHit: true,
          cacheTimestamp: cacheData.lastUpdated,
        };
      }
    }

    // 2. Cache miss - calcular datos
    console.log(`[queryHandlers][getIndexHistory] Cache miss para ${cacheKey}, calculando...`);
    const result = await calculateIndexData(code, range);

    // 3. Guardar en cache
    await cacheRef.set({
      ...result,
      lastUpdated: Date.now(),
    });

    console.log(`[queryHandlers][getIndexHistory] Éxito - ${cacheKey}`);

    return {
      ...result,
      cacheHit: false,
      cacheTimestamp: Date.now(),
    };

  } catch (error) {
    console.error(`[queryHandlers][getIndexHistory] Error:`, error);
    if (error instanceof HttpsError) throw error;
    throw new HttpsError('internal', 'Error obteniendo datos del índice');
  }
}

/**
 * Obtiene distribución del portafolio (sectores, países, holdings)
 * Migrado desde función inline en index.js
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de distribución
 * @returns {Promise<Object>} Distribución del portafolio
 */
async function getPortfolioDistribution(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { accountIds, accountId, currency, includeHoldings } = payload || {};

  console.log(`[queryHandlers][getPortfolioDistribution] userId: ${userId}`);

  try {
    const result = await portfolioDistributionService.getPortfolioDistribution(
      userId,
      { 
        accountIds, 
        accountId, 
        currency: currency || 'USD', 
        includeHoldings: includeHoldings ?? true 
      }
    );

    console.log(`[queryHandlers][getPortfolioDistribution] Éxito - sectors: ${result.sectors?.length || 0}`);

    return result;
  } catch (error) {
    console.error(`[queryHandlers][getPortfolioDistribution] Error:`, error);
    throw new HttpsError('internal', 'Error calculando distribución del portafolio');
  }
}

/**
 * Obtiene sectores disponibles en el sistema
 * Migrado desde función inline en index.js
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Sin uso
 * @returns {Promise<{sectors: Array}>}
 */
async function getAvailableSectors(context, payload) {
  const { auth } = context;

  console.log(`[queryHandlers][getAvailableSectors] userId: ${auth.uid}`);

  try {
    const sectors = await portfolioDistributionService.getAvailableSectors();
    
    console.log(`[queryHandlers][getAvailableSectors] Éxito - ${sectors.length} sectores`);
    
    return { sectors };
  } catch (error) {
    console.error(`[queryHandlers][getAvailableSectors] Error:`, error);
    throw new HttpsError('internal', 'Error obteniendo sectores disponibles');
  }
}

/**
 * Obtiene rendimientos históricos usando períodos consolidados (V2)
 * 
 * COST-OPT-001: Versión optimizada que reduce lecturas de Firestore
 * de ~1,825 a ~40 documentos para consultas de 5 años.
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de consulta
 * @returns {Promise<Object>} Rendimientos calculados
 */
async function getHistoricalReturnsOptimized(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { 
    currency = "USD", 
    accountId = "overall", 
    ticker = null, 
    assetType = null, 
    forceRefresh = false,
    fallbackToV1 = true
  } = payload || {};

  // Generar clave de cache
  const cacheKey = `v2_${currency}_${accountId}${ticker ? `_${ticker}` : ''}${assetType ? `_${assetType}` : ''}`;
  
  console.log(`[queryHandlers][getHistoricalReturnsOptimized] userId: ${userId}, cacheKey: ${cacheKey}`);

  try {
    // 1. Verificar cache (si no forceRefresh)
    if (!forceRefresh) {
      const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
      const cacheDoc = await cacheRef.get();

      if (cacheDoc.exists) {
        const cacheData = cacheDoc.data();
        const validUntil = new Date(cacheData.validUntil);

        if (validUntil > new Date()) {
          console.log(`[queryHandlers][getHistoricalReturnsOptimized] Cache hit`);
          return {
            ...cacheData.data,
            cacheHit: true,
            lastCalculated: cacheData.lastCalculated,
            validUntil: cacheData.validUntil
          };
        }
      }
    }

    // 2. Ejecutar cálculos con V2 (períodos consolidados)
    const result = await getHistoricalReturnsV2(userId, {
      currency,
      accountId,
      ticker,
      assetType,
      forceRefresh,
      fallbackToV1
    });

    // 3. Guardar en cache
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
      console.error(`[queryHandlers][getHistoricalReturnsOptimized] Error guardando cache:`, cacheWriteError);
    }

    console.log(`[queryHandlers][getHistoricalReturnsOptimized] Éxito - version: ${result._metadata?.version || 'unknown'}`);
    
    return {
      ...result,
      cacheHit: false,
      lastCalculated: now.toISOString(),
      validUntil: validUntil.toISOString()
    };

  } catch (error) {
    console.error(`[queryHandlers][getHistoricalReturnsOptimized] Error:`, error);
    if (error instanceof HttpsError) throw error;
    throw new HttpsError('internal', 'Error calculando rendimientos históricos optimizados');
  }
}

/**
 * Verifica el estado de datos consolidados de un usuario
 * 
 * COST-OPT-001: Útil para diagnóstico y determinar si usar V2.
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de consulta
 * @returns {Promise<Object>} Estado de datos consolidados
 */
async function getConsolidatedDataStatus(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { accountId = "overall" } = payload || {};

  console.log(`[queryHandlers][getConsolidatedDataStatus] userId: ${userId}, accountId: ${accountId}`);

  try {
    const status = await checkConsolidatedDataStatus(userId, accountId);
    
    console.log(`[queryHandlers][getConsolidatedDataStatus] Éxito - canUseV2: ${status.canUseV2}`);
    
    return status;
  } catch (error) {
    console.error(`[queryHandlers][getConsolidatedDataStatus] Error:`, error);
    throw new HttpsError('internal', 'Error verificando estado de datos consolidados');
  }
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  getCurrentPricesForUser,
  getHistoricalReturns,
  getMultiAccountHistoricalReturns,
  getIndexHistory,
  getPortfolioDistribution,
  getAvailableSectors,
  // COST-OPT-001: Nuevos handlers para rendimientos optimizados
  getHistoricalReturnsOptimized,
  getConsolidatedDataStatus,
};
