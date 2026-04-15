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

// PERF-SNAP-007: Importar helper para construir ID de snapshot
// PERF-SNAP-009: Importar generatePerformanceSnapshot para on-demand generation
// PERF-SNAP-024: Importar generateAssetSnapshot para on-demand per-asset
const { buildSnapshotDocId, generatePerformanceSnapshot, generateAssetSnapshot } = require('../snapshotGenerator');

// PERF-SNAP-021: Importar funciones de mercado para TTL inteligente
const { isNYSEMarketOpen, calculateTTLUntilNextEOD, MARKET_CACHE_TTL_MS } = require('../riskMetrics/riskMetricsCache');

// ============================================================================
// CONSTANTES
// ============================================================================

const VALID_INDEX_RANGES = ["1M", "3M", "6M", "YTD", "1Y", "5Y", "MAX"];
const INDEX_CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 horas
const INDEX_INTRADAY_CACHE_TTL_MS = 5 * 60 * 1000; // 5 min (cuando falta punto de hoy)

// PERF-SNAP-021: In-memory cache para snapshots a nivel de módulo (sobrevive warm starts)
const SNAPSHOT_MEM_CACHE_MAX_SIZE = 100;
const snapshotMemCache = new Map();

function getSnapshotCacheTTL() {
  return isNYSEMarketOpen() ? MARKET_CACHE_TTL_MS : calculateTTLUntilNextEOD();
}

async function getSnapshotWithCache(snapshotId, userId) {
  // PERF-SNAP-023: Read lastSnapshotUpdate for invalidation signal
  let lastSnapshotUpdate = null;
  if (userId) {
    try {
      const userPerfDoc = await db.doc(`portfolioPerformance/${userId}`).get();
      lastSnapshotUpdate = userPerfDoc.exists ? userPerfDoc.data()?.lastSnapshotUpdate || null : null;
    } catch {
      // Graceful degradation — rely on TTL if read fails
    }
  }

  const cached = snapshotMemCache.get(snapshotId);
  if (cached && Date.now() - cached.cachedAt < getSnapshotCacheTTL()) {
    // PERF-SNAP-023: Invalidate if EOD generated newer data
    if (lastSnapshotUpdate && new Date(lastSnapshotUpdate).getTime() > cached.cachedAt) {
      snapshotMemCache.delete(snapshotId);
    } else {
      snapshotMemCache.delete(snapshotId);
      snapshotMemCache.set(snapshotId, cached);
      return { data: cached.data, lastSnapshotUpdate };
    }
  }

  const doc = await db.doc(`performanceSnapshots/${snapshotId}`).get();
  if (!doc.exists) return { data: null, lastSnapshotUpdate };

  const data = doc.data();
  snapshotMemCache.set(snapshotId, { data, cachedAt: Date.now() });

  if (snapshotMemCache.size > SNAPSHOT_MEM_CACHE_MAX_SIZE) {
    const oldestKey = snapshotMemCache.keys().next().value;
    snapshotMemCache.delete(oldestKey);
  }

  return { data, lastSnapshotUpdate };
}

function clearSnapshotMemCache() {
  snapshotMemCache.clear();
}

function getSnapshotMemCacheSize() {
  return snapshotMemCache.size;
}

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

// ============================================================================
// PERF-SNAP-008: Agregación de snapshots para multi-cuenta
// ============================================================================

/**
 * PERF-SNAP-008: Agrega N snapshots de cuentas individuales en un resultado
 * multi-cuenta unificado. Alinea timelines por fecha, pondera dailyChangePercentage
 * por valor pre-cambio (TWR multi-cuenta) y pasa "fake docs" a
 * calculateHistoricalReturns() para producir el formato final.
 *
 * @param {Array<Object>} snapshots - Snapshots individuales (cada uno con timeline, returns, etc.)
 * @param {string} currency - Código de moneda (e.g. 'USD')
 * @returns {Object} Resultado en formato idéntico al que produce la ruta legacy
 */
function aggregateSnapshotTimelines(snapshots, currency) {
  const dateMap = new Map();

  for (const snapshot of snapshots) {
    for (const entry of snapshot.timeline || []) {
      const { d: date, v: totalValue, c: dailyChangePercentage } = entry;

      if (!dateMap.has(date)) {
        dateMap.set(date, []);
      }
      dateMap.get(date).push({ totalValue, dailyChangePercentage });
    }
  }

  const sortedDates = Array.from(dateMap.keys()).sort();

  const aggregatedDocs = sortedDates.map(date => {
    const contributions = dateMap.get(date);

    const aggregatedTotalValue = contributions.reduce((sum, c) => sum + c.totalValue, 0);

    const contributionsWithPreValue = contributions.map(c => {
      const change = c.dailyChangePercentage || 0;
      const preChangeValue = change !== 0
        ? c.totalValue / (1 + change / 100)
        : c.totalValue;
      return { ...c, preChangeValue };
    });

    const totalWeight = contributionsWithPreValue.reduce((sum, c) => sum + c.preChangeValue, 0);

    let weightedChange = 0;
    if (totalWeight > 0) {
      weightedChange = contributionsWithPreValue.reduce((sum, c) => {
        return sum + (c.dailyChangePercentage || 0) * (c.preChangeValue / totalWeight);
      }, 0);
    }

    return {
      data: () => ({
        date,
        [currency]: {
          totalValue: aggregatedTotalValue,
          dailyChangePercentage: weightedChange,
          adjustedDailyChangePercentage: weightedChange,
        },
      }),
    };
  });

  return calculateHistoricalReturns(aggregatedDocs, currency);
}

/**
 * Aggregate monthlyCompound from multiple snapshots by summing P&L fields.
 * For percentage fields (returnPct), uses value-weighted average.
 */
function aggregateMonthlyCompounds(snapshots) {
  const result = {};

  for (const snapshot of snapshots) {
    const mc = snapshot.monthlyCompound || {};
    for (const [year, months] of Object.entries(mc)) {
      if (!result[year]) result[year] = {};
      for (const [month, data] of Object.entries(months)) {
        if (!result[year][month]) {
          result[year][month] = {
            returnPct: 0,
            startTotalValue: 0,
            startTotalInvestment: 0,
            endTotalValue: 0,
            endTotalInvestment: 0,
            totalCashFlow: 0,
            profit: 0,
            doneProfitAndLoss: 0,
            unrealizedProfitAndLoss: 0,
            lastDayOfMonth: data.lastDayOfMonth || false,
          };
        }
        const acc = result[year][month];
        acc.startTotalValue += data.startTotalValue || 0;
        acc.startTotalInvestment += data.startTotalInvestment || 0;
        acc.endTotalValue += data.endTotalValue || 0;
        acc.endTotalInvestment += data.endTotalInvestment || 0;
        acc.totalCashFlow += data.totalCashFlow || 0;
        acc.profit += data.profit || 0;
        acc.doneProfitAndLoss += data.doneProfitAndLoss || 0;
        acc.unrealizedProfitAndLoss += data.unrealizedProfitAndLoss || 0;
        if (data.lastDayOfMonth) acc.lastDayOfMonth = true;
      }
    }
  }

  return result;
}

/**
 * Aggregate performanceByYear from multiple snapshots.
 * Months get their TWR re-computed from the aggregated timeline (result.performanceByYear),
 * but P&L-related totals come from the snapshot compounds.
 */
function aggregatePerformanceByYear(snapshots, basePerformanceByYear) {
  const result = {};

  // Start from the base (computed from aggregated timeline — has correct TWR returns)
  for (const [year, data] of Object.entries(basePerformanceByYear || {})) {
    result[year] = { ...data };
  }

  // For each year, merge personalMonths/personalTotal from snapshots by summing
  for (const snapshot of snapshots) {
    const pby = snapshot.performanceByYear || {};
    for (const [year, data] of Object.entries(pby)) {
      if (!result[year]) {
        result[year] = { months: {}, personalMonths: {}, total: 0, personalTotal: 0 };
      }
    }
  }

  return result;
}

/**
 * PERF-SNAP-007: Transforma un snapshot pre-computado al formato de respuesta
 * que el frontend espera (idéntico a getHistoricalReturnsV2).
 */
function transformSnapshotToResponse(snapshot) {
  const dates = [];
  const values = [];
  const percentChanges = [];

  for (const entry of snapshot.timeline || []) {
    dates.push(entry.d);
    values.push(entry.v);
    percentChanges.push(entry.c);
  }

  const firstValue = values[0] || 0;
  const lastValue = values[values.length - 1] || 0;
  const overallPercentChange = firstValue > 0
    ? ((lastValue - firstValue) / firstValue) * 100
    : 0;

  const isDailyTimeline = snapshot.timelineGranularity === 'daily';

  // PERF-SNAP-025: Computar soldRanges y soldCompletelyDate desde timeline
  let soldCompletelyDate = null;
  const soldRanges = []; // Array de { from, to } — períodos sin posición
  if (isDailyTimeline) {
    const timeline = snapshot.timeline || [];
    const lastEntry = timeline[timeline.length - 1];

    // Detectar rangos vendidos: secuencias de entries con u<=0 seguidas de re-compra
    let soldStart = null;
    for (let i = 0; i < timeline.length; i++) {
      const e = timeline[i];
      if (e.u <= 0 && soldStart === null) {
        soldStart = e.d;
      } else if (e.u > 0 && soldStart !== null) {
        soldRanges.push({ from: soldStart, to: e.d });
        soldStart = null;
      }
    }
    // Si termina vendido, es venta permanente
    if (soldStart !== null) {
      soldCompletelyDate = soldStart;
    }
  }

  return {
    returns: snapshot.returns || {},
    validDocsCountByPeriod: snapshot.validDocsCountByPeriod || {},
    totalValueData: {
      dates,
      values,
      percentChanges,
      overallPercentChange,
      ...(isDailyTimeline && { timelineGranularity: 'daily' }),
      ...(isDailyTimeline && { soldCompletelyDate }),
      ...(isDailyTimeline && soldRanges.length > 0 && { soldRanges }),
    },
    performanceByYear: snapshot.performanceByYear || {},
    monthlyCompoundData: snapshot.monthlyCompound || {},
    availableYears: snapshot.availableYears || [],
    startDate: snapshot.startDate || '',
    latestAssetPerformance: snapshot.latestAssetPerformance || {},
    _metadata: {
      version: 'snapshot',
      schemaVersion: snapshot.schemaVersion,
      snapshotLastUpdated: snapshot.lastUpdated,
    },
  };
}

/**
 * Obtiene rendimientos históricos del portafolio
 * 
 * PERF-SNAP-007: Intenta leer snapshot pre-computado (1 read).
 * Si no existe o es request por ticker/assetType, cae a ruta legacy (V2/V1).
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

  console.log(`[queryHandlers][getHistoricalReturns] userId: ${userId}, currency: ${currency}, accountId: ${accountId}`);

  try {
    // PERF-SNAP-024: Per-asset snapshot read path (antes del bypass legacy)
    if (ticker && assetType && !forceRefresh) {
      const assetSnapshotId = buildSnapshotDocId(userId, accountId, currency, ticker, assetType);
      const assetSnapshotResult = await getSnapshotWithCache(assetSnapshotId, userId);
      const assetSnapshot = assetSnapshotResult.data;

      if (assetSnapshot) {
        const result = transformSnapshotToResponse(assetSnapshot);
        console.log(`[PERF] Asset snapshot hit - ${assetSnapshotId}`);

        const now = new Date();
        return {
          ...result,
          cacheHit: false,
          lastCalculated: now.toISOString(),
          validUntil: calculateDynamicTTL().toISOString(),
          lastSnapshotUpdate: assetSnapshotResult.lastSnapshotUpdate || null,
        };
      }

      console.log(`[PERF] Asset snapshot miss - ${assetSnapshotId}, falling back to legacy`);
      generateAssetSnapshot(db, userId, accountId, currency, ticker, assetType).catch(err =>
        console.warn(`[PERF] On-demand asset snapshot failed for ${assetSnapshotId}: ${err.message}`)
      );
    }

    // PERF-SNAP-007: Ticker/AssetType → ruta legacy
    if (ticker || assetType) {
      return await getHistoricalReturnsLegacy(context, payload);
    }

    // PERF-SNAP-007/021: Intentar leer snapshot (cache in-memory → Firestore)
    if (!forceRefresh) {
      const snapshotDocId = buildSnapshotDocId(userId, accountId, currency);
      const snapshotResult = await getSnapshotWithCache(snapshotDocId, userId);
      const snapshot = snapshotResult.data;

      if (snapshot) {
        const result = transformSnapshotToResponse(snapshot);

        console.log(`[queryHandlers][getHistoricalReturns] Snapshot hit - ${snapshotDocId}`);

        const now = new Date();
        return {
          ...result,
          cacheHit: false,
          lastCalculated: now.toISOString(),
          validUntil: calculateDynamicTTL().toISOString(),
          lastSnapshotUpdate: snapshotResult.lastSnapshotUpdate || null,
        };
      }

      // PERF-SNAP-009: Log estandarizado con snapshotId para monitoreo
      console.log(`[PERF] Snapshot not found for ${snapshotDocId}, falling back to legacy`);

      // PERF-SNAP-009: Generación on-demand fire-and-forget
      generatePerformanceSnapshot(db, userId, accountId, currency).catch(err =>
        console.warn(`[PERF] On-demand snapshot generation failed for ${snapshotDocId}: ${err.message}`)
      );
    }

    // Fallback a ruta legacy (V2/V1 + cache)
    return await getHistoricalReturnsLegacy(context, payload);

  } catch (error) {
    console.error(`[queryHandlers][getHistoricalReturns] Error:`, error);
    if (error instanceof HttpsError) throw error;
    throw new HttpsError('internal', 'Error calculando rendimientos históricos');
  }
}

/**
 * Ruta legacy de rendimientos históricos (V2 consolidados + cache Firestore).
 * Usada como fallback cuando no hay snapshot, para ticker/assetType, o forceRefresh.
 */
async function getHistoricalReturnsLegacy(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { 
    currency = "USD", 
    accountId = "overall", 
    ticker = null, 
    assetType = null, 
    forceRefresh = false 
  } = payload || {};

  const cacheKey = `${currency}_${accountId}${ticker ? `_${ticker}` : ''}${assetType ? `_${assetType}` : ''}`;

  // 1. Verificar cache (si no forceRefresh)
  if (!forceRefresh) {
    const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
    const cacheDoc = await cacheRef.get();

    if (cacheDoc.exists) {
      const cacheData = cacheDoc.data();
      const validUntil = new Date(cacheData.validUntil);

      if (validUntil > new Date()) {
        console.log(`[queryHandlers][getHistoricalReturnsLegacy] Cache hit`);
        return {
          ...cacheData.data,
          cacheHit: true,
          lastCalculated: cacheData.lastCalculated,
          validUntil: cacheData.validUntil
        };
      }
    }
  }

  const result = await getHistoricalReturnsV2(userId, {
    currency,
    accountId,
    ticker,
    assetType,
    forceRefresh,
    fallbackToV1: true
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
    console.error(`[queryHandlers][getHistoricalReturnsLegacy] Error guardando cache:`, cacheWriteError);
  }

  const version = result._metadata?.version || 'v1';
  console.log(`[queryHandlers][getHistoricalReturnsLegacy] Éxito - version: ${version}`);
  
  return {
    ...result,
    cacheHit: false,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };
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
    // PERF-SNAP-008: Snapshot path — N reads en vez de N full collection scans
    // Uses getSnapshotWithCache for in-memory cache + lastSnapshotUpdate invalidation
    // ============================================================================
    if (!forceRefresh && !ticker && !assetType) {
      const snapshotPromises = accountIds.map(accountId =>
        getSnapshotWithCache(buildSnapshotDocId(userId, accountId, currency), userId)
      );
      const snapshotResults = await Promise.all(snapshotPromises);

      if (snapshotResults.every(r => r.data !== null)) {
        const snapshots = snapshotResults.map(r => r.data);
        const result = aggregateSnapshotTimelines(snapshots, currency);

        // FIX-SNAP-008c: Los snapshots individuales tienen validDocsCountByPeriod correcto
        // (calculado por V2 chainFactorsForPeriods con docsCount de consolidated periods).
        // Pero aggregateSnapshotTimelines → calculateHistoricalReturns recalcula desde
        // "fake docs" del timeline, que tiene 1 punto por período consolidado = counts bajísimos.
        // Fix: usar el MAX de validDocsCountByPeriod de los snapshots individuales.
        const aggregatedValidDocs = snapshots.reduce((acc, snap) => {
          const vd = snap.validDocsCountByPeriod || {};
          return {
            ytd: Math.max(acc.ytd, vd.ytd || 0),
            oneMonth: Math.max(acc.oneMonth, vd.oneMonth || 0),
            threeMonths: Math.max(acc.threeMonths, vd.threeMonths || 0),
            sixMonths: Math.max(acc.sixMonths, vd.sixMonths || 0),
            oneYear: Math.max(acc.oneYear, vd.oneYear || 0),
            twoYears: Math.max(acc.twoYears, vd.twoYears || 0),
            fiveYears: Math.max(acc.fiveYears, vd.fiveYears || 0),
          };
        }, { ytd: 0, oneMonth: 0, threeMonths: 0, sixMonths: 0, oneYear: 0, twoYears: 0, fiveYears: 0 });

        // FIX-SNAP-008c: Los has*Data flags también necesitan reflejar los datos reales
        const aggregatedReturns = { ...result.returns };
        aggregatedReturns.hasYtdData = aggregatedValidDocs.ytd >= 1;
        aggregatedReturns.hasOneMonthData = aggregatedValidDocs.oneMonth >= 5;
        aggregatedReturns.hasThreeMonthData = aggregatedValidDocs.threeMonths >= 15;
        aggregatedReturns.hasSixMonthData = aggregatedValidDocs.sixMonths >= 30;
        aggregatedReturns.hasOneYearData = aggregatedValidDocs.oneYear >= 60;
        aggregatedReturns.hasTwoYearData = aggregatedValidDocs.twoYears >= 120;
        aggregatedReturns.hasFiveYearData = aggregatedValidDocs.fiveYears >= 250;

        console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Snapshot path - ${accountIds.length} snapshots agregados, validDocs: ytd=${aggregatedValidDocs.ytd}, 1M=${aggregatedValidDocs.oneMonth}, 3M=${aggregatedValidDocs.threeMonths}, 6M=${aggregatedValidDocs.sixMonths}, 1Y=${aggregatedValidDocs.oneYear}`);

        // FIX-SNAP-008b: Propagar lastSnapshotUpdate para invalidación de cache frontend
        const lastSnapshotUpdate = snapshotResults[0]?.lastSnapshotUpdate || null;

        // Aggregate monthlyCompound from individual snapshots (P&L fields)
        const aggregatedMonthlyCompound = aggregateMonthlyCompounds(snapshots);

        const now = new Date();
        return {
          ...result,
          returns: aggregatedReturns,
          validDocsCountByPeriod: aggregatedValidDocs,
          monthlyCompoundData: aggregatedMonthlyCompound,
          cacheHit: false,
          lastCalculated: now.toISOString(),
          validUntil: calculateDynamicTTL().toISOString(),
          lastSnapshotUpdate,
          _metadata: { ...(result._metadata || {}), version: 'snapshot-multi' },
        };
      }

      // PERF-SNAP-009: Log estandarizado con IDs de snapshots faltantes
      const missingIds = snapshotResults
        .map((r, i) => r.data !== null ? null : buildSnapshotDocId(userId, accountIds[i], currency))
        .filter(Boolean);
      console.log(`[PERF] Snapshot not found for [${missingIds.join(', ')}], falling back to legacy`);

      // PERF-SNAP-009: Generación on-demand fire-and-forget solo para cuentas sin snapshot
      const missingAccountIds = snapshotResults
        .map((r, i) => r.data !== null ? null : accountIds[i])
        .filter(Boolean);
      missingAccountIds.forEach(accountId => {
        generatePerformanceSnapshot(db, userId, accountId, currency).catch(err =>
          console.warn(`[PERF] On-demand snapshot generation failed for ${buildSnapshotDocId(userId, accountId, currency)}: ${err.message}`)
        );
      });
    }

    // Fallback a ruta legacy (full collection scan + cache)
    return await getMultiAccountHistoricalReturnsLegacy(context, payload);

  } catch (error) {
    console.error(`[queryHandlers][getMultiAccountHistoricalReturns] Error:`, error);
    if (error instanceof HttpsError) throw error;
    throw new HttpsError('internal', 'Error calculando rendimientos multi-cuenta');
  }
}

/**
 * PERF-SNAP-008: Ruta legacy de rendimientos históricos multi-cuenta.
 * Full collection scan por cada cuenta + agregación en memoria + cache Firestore.
 * Usada como fallback cuando no hay snapshots, para ticker/assetType, o forceRefresh.
 */
async function getMultiAccountHistoricalReturnsLegacy(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const {
    accountIds = [],
    currency = "USD",
    ticker = null,
    assetType = null,
    forceRefresh = false
  } = payload || {};

  // ============================================================================
  // MULTI-CUENTA REAL: Agregar datos de múltiples cuentas
  // ============================================================================
    
  // Generar clave de cache
  const sortedIds = [...accountIds].sort().join('_');
  const cacheKey = `multi_${currency}_${sortedIds}${ticker ? `_${ticker}` : ''}${assetType ? `_${assetType}` : ''}`;

  console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Multi-cuenta, cache key: ${cacheKey}`);

  // Verificar cache (si no forceRefresh)
  if (!forceRefresh) {
    try {
      const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
      const cacheDoc = await cacheRef.get();

      if (cacheDoc.exists) {
        const cache = cacheDoc.data();
        const validUntil = new Date(cache.validUntil);

        if (validUntil > new Date()) {
          console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Cache HIT`);
          return {
            ...cache.data,
            cacheHit: true,
            lastCalculated: cache.lastCalculated,
            validUntil: cache.validUntil
          };
        }
      }
    } catch (cacheError) {
      console.warn(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Error leyendo cache:`, cacheError.message);
    }
  }

  console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Cache MISS - Agregando ${accountIds.length} cuentas`);

  // Leer datos de cada cuenta en paralelo
  const accountDataPromises = accountIds.map(accountId => 
    db.collection(`portfolioPerformance/${userId}/accounts/${accountId}/dates`)
      .orderBy("date", "asc")
      .get()
  );
  
  const accountSnapshots = await Promise.all(accountDataPromises);

  const totalDocs = accountSnapshots.reduce((sum, snap) => sum + snap.size, 0);
  const docsDetail = accountSnapshots.map((snap, i) => `${accountIds[i].substring(0, 8)}...:${snap.size}`).join(', ');
  console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Docs por cuenta: [${docsDetail}], Total: ${totalDocs}`);

  if (totalDocs === 0) {
    console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Sin datos de performance`);
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
        
        existing.currencies[currencyCode]._accountContributions.push({
          totalValue: currencyData.totalValue || 0,
          adjustedDailyChangePercentage: currencyData.adjustedDailyChangePercentage || 0,
          rawDailyChangePercentage: currencyData.rawDailyChangePercentage || currencyData.dailyChangePercentage || 0
        });
        
        existing.currencies[currencyCode].totalInvestment += currencyData.totalInvestment || 0;
        existing.currencies[currencyCode].totalValue += currencyData.totalValue || 0;
        existing.currencies[currencyCode].totalCashFlow += currencyData.totalCashFlow || 0;
        existing.currencies[currencyCode].unrealizedProfitAndLoss += currencyData.unrealizedProfitAndLoss || 0;
        existing.currencies[currencyCode].doneProfitAndLoss += currencyData.doneProfitAndLoss || 0;
        
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

  const sortedDates = Array.from(aggregatedByDate.keys()).sort();
  
  sortedDates.forEach(date => {
    const dateData = aggregatedByDate.get(date);
    
    Object.keys(dateData.currencies).forEach(currencyCode => {
      const c = dateData.currencies[currencyCode];
      
      c.totalROI = c.totalInvestment > 0 
        ? ((c.totalValue - c.totalInvestment) / c.totalInvestment) * 100 
        : 0;
      
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
      
      Object.values(c.assetPerformance).forEach(assetData => {
        assetData.totalROI = assetData.totalInvestment > 0
          ? ((assetData.totalValue - assetData.totalInvestment) / assetData.totalInvestment) * 100
          : 0;
      });
    });
  });

  const aggregatedDocs = sortedDates.map(date => {
    const dateData = aggregatedByDate.get(date);
    return {
      data: () => ({
        date: dateData.date,
        ...dateData.currencies
      })
    };
  });

  console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Procesando ${aggregatedDocs.length} fechas agregadas`);

  const result = calculateHistoricalReturns(aggregatedDocs, currency, ticker, assetType);

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
    console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Cache guardado`);
  } catch (cacheWriteError) {
    console.error(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Error guardando cache:`, cacheWriteError.message);
  }

  return {
    ...result,
    cacheHit: false,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };
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

      // FIX-INDEX-INTRADAY: Use shorter TTL if cached data doesn't include today
      const todayStr = new Date().toISOString().split('T')[0];
      const lastCachedDate = (cacheData.chartData || []).length > 0
        ? cacheData.chartData[cacheData.chartData.length - 1].date
        : '';
      const effectiveTTL = lastCachedDate === todayStr ? INDEX_CACHE_TTL_MS : INDEX_INTRADAY_CACHE_TTL_MS;

      if (cacheAge < effectiveTTL) {
        console.log(`[queryHandlers][getIndexHistory] Cache hit para ${cacheKey} (TTL=${effectiveTTL/1000}s)`);
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
  const { accountIds, accountId, currency, includeHoldings, forceRefresh } = payload || {};

  console.log(`[queryHandlers][getPortfolioDistribution] userId: ${userId}, forceRefresh: ${forceRefresh}`);

  try {
    const result = await portfolioDistributionService.getPortfolioDistribution(
      userId,
      { 
        accountIds, 
        accountId, 
        currency: currency || 'USD', 
        includeHoldings: includeHoldings ?? true,
        forceRefresh: forceRefresh ?? false
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
  // PERF-SNAP-007: Funciones expuestas para testing
  transformSnapshotToResponse,
  getHistoricalReturnsLegacy,
  // PERF-SNAP-008: Funciones expuestas para testing
  aggregateSnapshotTimelines,
  getMultiAccountHistoricalReturnsLegacy,
  // PERF-SNAP-021: Cache in-memory helpers para testing
  clearSnapshotMemCache,
  getSnapshotMemCacheSize,
  getSnapshotWithCache,
  getSnapshotCacheTTL,
};
