/**
 * OPT-SNAP-INCR: Snapshot Incremental Append Service
 *
 * Actualiza performanceSnapshots incrementalmente (1 read + 1 write)
 * en lugar de regenerar desde cero (O(T) reads).
 *
 * Principios:
 * - SRP: Solo lógica de actualización incremental
 * - OCP: Extensible vía opciones, sin modificar internos
 * - DIP: Depende de abstracciones (periodConsolidation), no de Firestore directamente
 * - Bajo acoplamiento: No importa unifiedMarketDataUpdate ni snapshotGenerator internals
 *
 * @module snapshotIncrementalService
 * @see docs/architecture/EPIC-OPT-SNAPSHOT-INCREMENTAL.md — Fase 1
 */

const { DateTime } = require('luxon');
const {
  calculatePeriodBoundaries,
  initializePeriodFactors,
  processDailyDocument,
  buildReturnsResult,
} = require('../utils/periodConsolidation');

// ============================================================================
// CONSTANTS
// ============================================================================

const SCHEMA_VERSION_INCREMENTAL = 3;

/** Max calendar days gap before triggering full rebuild */
const MAX_GAP_CALENDAR_DAYS = 7;

// ============================================================================
// CORE: computeReturnsFromTimeline
// ============================================================================

/**
 * Computa returns desde un timeline en memoria.
 * Equivalente funcional a computePortfolioReturnsFromDailyDocs pero sin I/O.
 *
 * @param {Array<{d: string, v: number, c: number}>} timeline - Puntos compactos
 * @param {DateTime} now - Fecha actual (Luxon, timezone-aware)
 * @returns {{returns: Object, validDocsCountByPeriod: Object, performanceByYear: Object, availableYears: string[], startDate: string}|null}
 */
function computeReturnsFromTimeline(timeline, now) {
  if (!timeline || timeline.length === 0) return null;

  const boundaries = calculatePeriodBoundaries(now);
  const factors = initializePeriodFactors();
  const monthlyFactors = {};
  let firstDate = null;

  for (const point of timeline) {
    const date = point.d;
    const change = point.c || 0;

    const syntheticData = {
      adjustedDailyChangePercentage: change,
      dailyChangePercentage: change,
      totalValue: point.v,
      totalCashFlow: 0,
    };

    processDailyDocument(factors, boundaries, syntheticData, date);

    if (!firstDate) firstDate = date;

    const monthKey = date.substring(0, 7);
    if (!monthlyFactors[monthKey]) monthlyFactors[monthKey] = 1;
    monthlyFactors[monthKey] *= (1 + change / 100);
  }

  const monthlyReturns = {};
  for (const [monthKey, factor] of Object.entries(monthlyFactors)) {
    const [year, month] = monthKey.split('-');
    if (!monthlyReturns[year]) monthlyReturns[year] = {};
    monthlyReturns[year][parseInt(month, 10).toString()] = (factor - 1) * 100;
  }

  const lastPoint = timeline[timeline.length - 1];

  return buildReturnsResult(factors, {
    totalValueDates: timeline.map(p => p.d),
    totalValueValues: timeline.map(p => p.v),
    percentChanges: timeline.map(p => p.c),
    firstDate,
    firstValue: timeline[0].v || 0,
    lastValue: lastPoint.v || 0,
    now,
    monthlyReturns,
    yearlyReturns: {},
  });
}

// ============================================================================
// CORE: Incremental Update Functions
// ============================================================================

/**
 * Actualiza un portfolio snapshot incrementalmente.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} snapshotDocId - ID del documento snapshot
 * @param {Object} newDailyData - Datos del día recién calculado
 * @param {string} newDailyData.date
 * @param {number} newDailyData.totalValue
 * @param {number} newDailyData.totalInvestment
 * @param {number} newDailyData.adjustedDailyChangePercentage
 * @param {number} [newDailyData.dailyChangePercentage]
 * @param {number} [newDailyData.totalCashFlow]
 * @param {number} [newDailyData.doneProfitAndLoss]
 * @param {number} [newDailyData.unrealizedProfitAndLoss]
 * @param {Object} [newDailyData.assetPerformance]
 * @param {Object} [options]
 * @param {DateTime} [options.now] - Fecha actual inyectable (para determinismo en tests/retries)
 * @returns {Promise<{updated: boolean, method: 'incremental'|'full-rebuild'|'skipped', reason?: string}>}
 */
async function updateSnapshotIncremental(db, snapshotDocId, newDailyData, options = {}) {
  const existingDoc = await db.doc(`performanceSnapshots/${snapshotDocId}`).get();

  if (!existingDoc.exists || (existingDoc.data().schemaVersion || 0) < SCHEMA_VERSION_INCREMENTAL) {
    return { updated: false, method: 'full-rebuild', reason: 'missing-or-old-schema' };
  }

  const snapshot = existingDoc.data();

  // Idempotency: already has today's data
  if (snapshot.lastDateInTimeline === newDailyData.date) {
    return { updated: true, method: 'skipped', reason: 'already-current' };
  }

  // Gap detection
  if (snapshot.lastDateInTimeline) {
    const lastDate = DateTime.fromISO(snapshot.lastDateInTimeline);
    const newDate = DateTime.fromISO(newDailyData.date);
    const gap = newDate.diff(lastDate, 'days').days;

    if (gap > MAX_GAP_CALENDAR_DAYS) {
      return { updated: false, method: 'full-rebuild', reason: `gap-${Math.round(gap)}-days` };
    }
  }

  // Append new point (H1-FIX: push to copied array instead of spread-of-spread)
  const newPoint = {
    d: newDailyData.date,
    v: newDailyData.totalValue ?? 0,
    c: newDailyData.adjustedDailyChangePercentage ?? newDailyData.dailyChangePercentage ?? 0,
  };

  const updatedTimeline = snapshot.timeline ? snapshot.timeline.slice() : [];
  updatedTimeline.push(newPoint);

  // Recompute returns from in-memory timeline (M2-FIX: accept injected now)
  const now = options.now || DateTime.now().setZone('America/New_York');
  const computed = computeReturnsFromTimeline(updatedTimeline, now);

  if (!computed) {
    return { updated: false, method: 'full-rebuild', reason: 'compute-failed' };
  }

  // Incremental monthlyCompound
  const updatedMonthlyCompound = appendToMonthlyCompound(
    snapshot.monthlyCompound || {},
    newDailyData
  );

  // Incremental performanceByYear
  const updatedPerformanceByYear = appendToPerformanceByYear(
    snapshot.performanceByYear || {},
    newDailyData
  );

  // Update latestAssetPerformance if provided
  const updatedLatestAssetPerf = newDailyData.assetPerformance
    ? buildLatestAssetPerformance(newDailyData.assetPerformance)
    : (snapshot.latestAssetPerformance || {});

  const updatedSnapshot = {
    userId: snapshot.userId,
    currency: snapshot.currency,
    accountId: snapshot.accountId,
    timeline: updatedTimeline,
    returns: computed.returns,
    performanceByYear: updatedPerformanceByYear,
    monthlyCompound: updatedMonthlyCompound,
    validDocsCountByPeriod: computed.validDocsCountByPeriod,
    availableYears: computed.availableYears,
    startDate: computed.startDate || snapshot.startDate,
    latestAssetPerformance: updatedLatestAssetPerf,
    lastUpdated: new Date().toISOString(),
    lastDateInTimeline: newDailyData.date,
    schemaVersion: SCHEMA_VERSION_INCREMENTAL,
  };

  await db.doc(`performanceSnapshots/${snapshotDocId}`).set(updatedSnapshot);

  return { updated: true, method: 'incremental' };
}

/**
 * Actualiza un asset snapshot incrementalmente.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} snapshotDocId - ID del documento snapshot
 * @param {Object|null} newAssetData - Datos del activo (null = vendido)
 * @param {string} date - Fecha ISO YYYY-MM-DD
 * @returns {Promise<{updated: boolean, method: string, reason?: string}>}
 */
async function updateAssetSnapshotIncremental(db, snapshotDocId, newAssetData, date, options = {}) {
  const existingDoc = await db.doc(`performanceSnapshots/${snapshotDocId}`).get();

  if (!existingDoc.exists || (existingDoc.data().schemaVersion || 0) < SCHEMA_VERSION_INCREMENTAL) {
    return { updated: false, method: 'full-rebuild', reason: 'missing-or-old-schema' };
  }

  const snapshot = existingDoc.data();

  if (snapshot.lastDateInTimeline === date) {
    return { updated: true, method: 'skipped', reason: 'already-current' };
  }

  if (snapshot.lastDateInTimeline) {
    const gap = DateTime.fromISO(date).diff(DateTime.fromISO(snapshot.lastDateInTimeline), 'days').days;
    if (gap > MAX_GAP_CALENDAR_DAYS) {
      return { updated: false, method: 'full-rebuild', reason: `gap-${Math.round(gap)}-days` };
    }
  }

  const newPoint = newAssetData
    ? {
        d: date,
        v: newAssetData.totalValue ?? 0,
        c: newAssetData.adjustedDailyChangePercentage ?? newAssetData.dailyChangePercentage ?? 0,
        u: newAssetData.units ?? 0,
      }
    : { d: date, v: 0, c: 0, u: 0 };

  // H1-FIX: push to copied array instead of spread
  const updatedTimeline = snapshot.timeline ? snapshot.timeline.slice() : [];
  updatedTimeline.push(newPoint);

  // Compute returns only from points with active position
  const activePoints = updatedTimeline.filter(p => p.u > 0);
  const now = options.now || DateTime.now().setZone('America/New_York');
  const computed = activePoints.length > 0 ? computeReturnsFromTimeline(activePoints, now) : null;

  const updatedSnapshot = {
    userId: snapshot.userId,
    currency: snapshot.currency,
    accountId: snapshot.accountId,
    ticker: snapshot.ticker,
    assetType: snapshot.assetType,
    type: 'asset',
    timelineGranularity: 'daily',
    timeline: updatedTimeline,
    returns: computed?.returns || snapshot.returns,
    performanceByYear: computed?.performanceByYear || snapshot.performanceByYear,
    monthlyCompound: snapshot.monthlyCompound || {},
    validDocsCountByPeriod: computed?.validDocsCountByPeriod || snapshot.validDocsCountByPeriod,
    availableYears: computed?.availableYears || snapshot.availableYears,
    startDate: snapshot.startDate,
    lastUpdated: new Date().toISOString(),
    lastDateInTimeline: date,
    schemaVersion: SCHEMA_VERSION_INCREMENTAL,
  };

  await db.doc(`performanceSnapshots/${snapshotDocId}`).set(updatedSnapshot);

  return { updated: true, method: 'incremental' };
}

// ============================================================================
// HELPERS: Monthly & Yearly Compound
// ============================================================================

/**
 * Appends a new day to monthlyCompound. Meses cerrados no se tocan.
 *
 * @param {Object} existing - monthlyCompound actual
 * @param {Object} dailyData - Datos del nuevo día
 * @returns {Object} monthlyCompound actualizado
 */
function appendToMonthlyCompound(existing, dailyData) {
  const result = {};
  // Shallow-clone years, deep-clone only affected month
  for (const [y, months] of Object.entries(existing)) {
    result[y] = { ...months };
  }

  const date = dailyData.date;
  const yearStr = date.substring(0, 4);
  const monthNum = parseInt(date.substring(5, 7), 10).toString();

  if (!result[yearStr]) result[yearStr] = {};

  const change = dailyData.adjustedDailyChangePercentage ?? dailyData.dailyChangePercentage ?? 0;
  const prev = result[yearStr][monthNum];

  if (!prev) {
    // First day of month
    result[yearStr][monthNum] = {
      returnPct: change,
      startTotalValue: dailyData.totalValue ?? 0,
      startTotalInvestment: dailyData.totalInvestment ?? 0,
      endTotalValue: dailyData.totalValue ?? 0,
      endTotalInvestment: dailyData.totalInvestment ?? 0,
      totalCashFlow: dailyData.totalCashFlow ?? 0,
      profit: (dailyData.doneProfitAndLoss ?? 0) + (dailyData.unrealizedProfitAndLoss ?? 0),
      doneProfitAndLoss: dailyData.doneProfitAndLoss ?? 0,
      unrealizedProfitAndLoss: dailyData.unrealizedProfitAndLoss ?? 0,
      lastDayOfMonth: false,
    };
  } else {
    // Compound onto existing month
    const prevFactor = 1 + (prev.returnPct / 100);
    const newFactor = prevFactor * (1 + change / 100);

    result[yearStr][monthNum] = {
      ...prev,
      returnPct: (newFactor - 1) * 100,
      endTotalValue: dailyData.totalValue ?? 0,
      endTotalInvestment: dailyData.totalInvestment ?? 0,
      totalCashFlow: dailyData.totalCashFlow ?? 0,
      profit: (prev.doneProfitAndLoss + (dailyData.doneProfitAndLoss ?? 0))
              + (dailyData.unrealizedProfitAndLoss ?? 0),
      doneProfitAndLoss: prev.doneProfitAndLoss + (dailyData.doneProfitAndLoss ?? 0),
      unrealizedProfitAndLoss: dailyData.unrealizedProfitAndLoss ?? 0,
      lastDayOfMonth: false,
    };
  }

  // M4-FIX: Only mark the immediately preceding month as closed (if new month started)
  // Prior months are already closed from previous runs — no need to iterate all history
  const currentMonthKey = date.substring(0, 7);
  if (!prev) {
    // First day of a new month — mark the previous month as closed
    const prevMonthDt = DateTime.fromISO(`${currentMonthKey}-01`).minus({ months: 1 });
    const prevYearStr = prevMonthDt.toFormat('yyyy');
    const prevMonthNum = prevMonthDt.month.toString();
    if (result[prevYearStr]?.[prevMonthNum] && !result[prevYearStr][prevMonthNum].lastDayOfMonth) {
      result[prevYearStr][prevMonthNum] = { ...result[prevYearStr][prevMonthNum], lastDayOfMonth: true };
    }
  }

  return result;
}

/**
 * Appends a new day to performanceByYear. Only the current month/year is touched.
 *
 * @param {Object} existing - performanceByYear actual
 * @param {Object} dailyData - Datos del nuevo día
 * @returns {Object} performanceByYear actualizado
 */
function appendToPerformanceByYear(existing, dailyData) {
  const result = {};
  for (const [y, data] of Object.entries(existing)) {
    result[y] = { ...data, months: { ...data.months }, personalMonths: { ...data.personalMonths } };
  }

  const date = dailyData.date;
  const yearStr = date.substring(0, 4);
  const monthNum = parseInt(date.substring(5, 7), 10).toString();

  if (!result[yearStr]) {
    result[yearStr] = { months: {}, personalMonths: {}, total: 0, personalTotal: 0 };
  }

  const change = dailyData.adjustedDailyChangePercentage ?? dailyData.dailyChangePercentage ?? 0;

  const prevReturn = result[yearStr].months[monthNum] || 0;
  const prevFactor = 1 + (prevReturn / 100);
  const newFactor = prevFactor * (1 + change / 100);
  const newMonthReturn = (newFactor - 1) * 100;

  result[yearStr].months[monthNum] = newMonthReturn;
  result[yearStr].personalMonths[monthNum] = newMonthReturn;

  // Recompute year total as compound of all months
  let yearFactor = 1;
  for (const monthReturn of Object.values(result[yearStr].months)) {
    if (typeof monthReturn === 'number') {
      yearFactor *= (1 + monthReturn / 100);
    }
  }
  result[yearStr].total = (yearFactor - 1) * 100;
  result[yearStr].personalTotal = result[yearStr].total;

  return result;
}

/**
 * Builds latestAssetPerformance from raw assetPerformance data.
 * Extracts only the fields needed for the snapshot.
 *
 * @param {Object} assetPerformance - Raw assetPerformance keyed by ticker_assetType
 * @returns {Object} Compact latestAssetPerformance
 */
function buildLatestAssetPerformance(assetPerformance) {
  const result = {};
  for (const [assetKey, data] of Object.entries(assetPerformance)) {
    result[assetKey] = {
      totalValue: data.totalValue ?? 0,
      totalInvestment: data.totalInvestment ?? 0,
      units: data.units ?? 0,
      unrealizedPnL: data.unrealizedProfitAndLoss ?? 0,
      totalROI: data.totalROI ?? 0,
      dailyChangePercentage: data.dailyChangePercentage ?? 0,
    };
  }
  return result;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Core
  updateSnapshotIncremental,
  updateAssetSnapshotIncremental,
  computeReturnsFromTimeline,

  // Helpers (exported for testing)
  appendToMonthlyCompound,
  appendToPerformanceByYear,
  buildLatestAssetPerformance,

  // Constants
  SCHEMA_VERSION_INCREMENTAL,
  MAX_GAP_CALENDAR_DAYS,
};
