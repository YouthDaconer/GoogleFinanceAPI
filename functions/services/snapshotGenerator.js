/**
 * PERF-SNAP-003: Snapshot Generator — Write-Time Aggregation
 *
 * Pre-computa la respuesta completa de rendimientos históricos y la almacena
 * como un único documento en performanceSnapshots/. Los consumidores (Dashboard,
 * Attribution, Risk Metrics) leen 1 doc en vez de 40-1,500+.
 *
 * @module snapshotGenerator
 * @see docs/stories/PERF-SNAP-003.story.md
 * @see docs/architecture/AUDIT-PORTFOLIO-PERFORMANCE-ARCHITECTURE.md
 */

const { DateTime } = require('luxon');
const {
  calculatePeriodBoundaries,
  initializePeriodFactors,
  processDailyDocument,
  buildReturnsResult,
} = require('../utils/periodConsolidation');

const SCHEMA_VERSION = 2;
const ASSET_SCHEMA_VERSION = 3;

function buildSnapshotDocId(userId, accountId, currency, ticker, assetType) {
  if (ticker && assetType) {
    return accountId === 'overall'
      ? `${userId}_${ticker}_${assetType}_${currency}`
      : `${userId}_${ticker}_${assetType}_${accountId}_${currency}`;
  }
  if (accountId === 'overall') {
    return `${userId}_${currency}`;
  }
  return `${userId}_${accountId}_${currency}`;
}

function transformToCompactTimeline(totalValueData) {
  if (!totalValueData || !totalValueData.dates || totalValueData.dates.length === 0) {
    return [];
  }

  const { dates, values, percentChanges } = totalValueData;

  return dates.map((date, i) => ({
    d: date,
    v: values[i] ?? 0,
    c: percentChanges[i] ?? 0,
  }));
}

async function fetchAllDailyDocs(db, userId, accountId) {
  const basePath = accountId === 'overall'
    ? `portfolioPerformance/${userId}/dates`
    : `portfolioPerformance/${userId}/accounts/${accountId}/dates`;

  const snapshot = await db.collection(basePath)
    .orderBy('date', 'asc')
    .get();

  return snapshot.docs;
}

function buildDailyTimeline(docs, currency) {
  const timeline = [];
  for (const doc of docs) {
    const data = doc.data();
    const currencyData = data[currency];
    if (!currencyData) continue;
    timeline.push({
      d: data.date,
      v: currencyData.totalValue ?? 0,
      c: currencyData.adjustedDailyChangePercentage ?? currencyData.dailyChangePercentage ?? 0,
    });
  }
  return timeline;
}

function extractLatestAssetPerformanceFromDocs(docs, currency) {
  if (!docs || docs.length === 0) return {};
  const lastDoc = docs[docs.length - 1];
  const data = lastDoc.data();
  const currencyData = data[currency] || {};
  const assetPerf = currencyData.assetPerformance || {};
  return extractAssetPerformanceFields(assetPerf);
}

/**
 * Build monthlyCompoundData from daily docs, extracting P&L fields that
 * PortfolioPerformanceChartViewer tooltip needs.
 *
 * V2 consolidated returns left monthlyCompoundData as {} — this fills that gap
 * by reading doneProfitAndLoss and unrealizedProfitAndLoss from the last daily
 * doc of each month.
 *
 * @param {Array} docs - Daily docs (Firestore snapshots or plain objects), ordered by date asc
 * @param {string} currency - Currency code (e.g., 'USD')
 * @param {string} [ticker] - Asset ticker (for asset-level snapshots)
 * @param {string} [assetType] - Asset type (for asset-level snapshots)
 * @returns {Object} { "2026": { "1": { returnPct, profit, doneProfitAndLoss, ... } } }
 */
function buildMonthlyCompoundFromDailyDocs(docs, currency, ticker, assetType) {
  if (!docs || docs.length === 0) return {};

  const isAsset = !!(ticker && assetType);
  const assetKey = isAsset ? `${ticker}_${assetType}` : null;

  const now = DateTime.now().setZone('America/New_York');
  const currentMonthKey = now.toFormat('yyyy-MM');

  // Group daily docs by year-month, preserving insertion order
  const monthGroups = new Map();

  for (const doc of docs) {
    const data = doc.data ? doc.data() : doc;
    const date = data.date;
    if (!date) continue;

    const currencyData = data[currency];
    if (!currencyData) continue;

    const entryData = isAsset
      ? currencyData.assetPerformance?.[assetKey]
      : currencyData;
    if (!entryData) continue;

    const monthKey = date.substring(0, 7); // "2026-01"
    if (!monthGroups.has(monthKey)) monthGroups.set(monthKey, []);
    monthGroups.get(monthKey).push(entryData);
  }

  const result = {};

  for (const [monthKey, entries] of monthGroups.entries()) {
    const [yearStr, monthStr] = monthKey.split('-');
    const monthNum = parseInt(monthStr, 10).toString();

    if (!result[yearStr]) result[yearStr] = {};

    // Entries are already in chronological order (docs ordered by date asc)
    const first = entries[0];
    const last = entries[entries.length - 1];

    // Compose monthly TWR from daily changes
    let monthFactor = 1;
    for (const entry of entries) {
      const change = entry.adjustedDailyChangePercentage ?? entry.dailyChangePercentage ?? 0;
      monthFactor *= (1 + change / 100);
    }

    const isClosedMonth = monthKey < currentMonthKey;

    // doneProfitAndLoss in each daily doc = realized P&L from sells on THAT day only.
    // Must SUM across all days in the month to get monthly total.
    let monthlyDonePnL = 0;
    for (const entry of entries) {
      monthlyDonePnL += entry.doneProfitAndLoss ?? 0;
    }

    // unrealizedProfitAndLoss = totalValue - totalInvestment (point-in-time snapshot).
    // The LAST day of the month is the correct value for monthly display.
    const unrealizedPnL = last.unrealizedProfitAndLoss ?? 0;

    result[yearStr][monthNum] = {
      returnPct: (monthFactor - 1) * 100,
      startTotalValue: first.totalValue ?? 0,
      startTotalInvestment: first.totalInvestment ?? 0,
      endTotalValue: last.totalValue ?? 0,
      endTotalInvestment: last.totalInvestment ?? 0,
      totalCashFlow: last.totalCashFlow ?? 0,
      profit: monthlyDonePnL + unrealizedPnL,
      doneProfitAndLoss: monthlyDonePnL,
      unrealizedProfitAndLoss: unrealizedPnL,
      lastDayOfMonth: isClosedMonth,
    };
  }

  return result;
}

const ASSET_PERF_FIELDS = ['totalValue', 'totalInvestment', 'units', 'unrealizedPnL', 'totalROI', 'dailyChangePercentage'];

function extractAssetPerformanceFields(assetPerformance) {
  const result = {};
  for (const [assetKey, assetData] of Object.entries(assetPerformance)) {
    result[assetKey] = {
      totalValue: assetData.totalValue ?? 0,
      totalInvestment: assetData.totalInvestment ?? 0,
      units: assetData.units ?? 0,
      unrealizedPnL: assetData.unrealizedProfitAndLoss ?? 0,
      totalROI: assetData.totalROI ?? 0,
      dailyChangePercentage: assetData.dailyChangePercentage ?? 0,
    };
  }
  return result;
}

async function fetchLatestAssetPerformance(db, userId, accountId, currency) {
  const basePath = accountId === 'overall'
    ? `portfolioPerformance/${userId}/dates`
    : `portfolioPerformance/${userId}/accounts/${accountId}/dates`;

  const latestDoc = await db.collection(basePath)
    .orderBy('date', 'desc')
    .limit(1)
    .get();

  if (latestDoc.empty) {
    return {};
  }

  const data = latestDoc.docs[0].data();
  const currencyData = data[currency] || {};
  const assetPerf = currencyData.assetPerformance || {};

  return extractAssetPerformanceFields(assetPerf);
}

async function generatePerformanceSnapshot(db, userId, accountId, currency, options = {}) {
  // PERF-SNAP-030: Usar daily docs directamente (como PERF-SNAP-025 para assets)
  const dailyDocs = options.dailyDocs || await fetchAllDailyDocs(db, userId, accountId);

  const now = DateTime.now().setZone('America/New_York');
  const computedResult = computePortfolioReturnsFromDailyDocs(dailyDocs, currency, now);

  if (!computedResult || !computedResult.returns) {
    console.log(`[snapshotGenerator] No data for userId=${userId}, accountId=${accountId}, currency=${currency} — skipping`);
    return false;
  }

  const hasAnyData = computedResult.returns.hasYtdData ||
    computedResult.returns.hasOneMonthData ||
    computedResult.returns.hasThreeMonthData;

  if (!hasAnyData) {
    console.log(`[snapshotGenerator] Empty result for userId=${userId}, accountId=${accountId}, currency=${currency} — skipping`);
    return false;
  }

  const timeline = buildDailyTimeline(dailyDocs, currency);
  const latestAssetPerformance = extractLatestAssetPerformanceFromDocs(dailyDocs, currency);

  const snapshot = {
    userId,
    currency,
    accountId,
    lastUpdated: new Date().toISOString(),
    schemaVersion: SCHEMA_VERSION,

    returns: computedResult.returns,
    timeline,

    performanceByYear: computedResult.performanceByYear || {},
    monthlyCompound: buildMonthlyCompoundFromDailyDocs(dailyDocs, currency),

    validDocsCountByPeriod: computedResult.validDocsCountByPeriod || {},
    availableYears: computedResult.availableYears || [],
    startDate: computedResult.startDate || '',

    latestAssetPerformance,
  };

  const docId = buildSnapshotDocId(userId, accountId, currency);
  console.log(`[snapshotGenerator] Writing snapshot ${docId} — timeline: ${timeline.length} points`);
  await db.collection('performanceSnapshots').doc(docId).set(snapshot);
  // OPT-FIRESTORE-002 P2-A: Return snapshot object so callers can use it directly
  // without an additional Firestore read. Truthy, so `if (wrote)` checks still work.
  return snapshot;
}

// PERF-SNAP-025: Pre-filtrar docs que contienen data del activo específico
function filterDocsForAsset(docs, currency, ticker, assetType) {
  const assetKey = `${ticker}_${assetType}`;
  return docs.filter(doc => {
    const data = doc.data ? doc.data() : doc;
    return data[currency]?.assetPerformance?.[assetKey] !== undefined;
  });
}

// PERF-SNAP-025: Construir timeline diaria para un activo específico
function buildAssetDailyTimeline(docs, currency, ticker, assetType) {
  const assetKey = `${ticker}_${assetType}`;
  const timeline = [];
  let assetSeen = false;
  let soldMarkerPushed = false;

  for (const doc of docs) {
    const data = doc.data ? doc.data() : doc;
    const currencyData = data[currency];
    if (!currencyData) continue;

    const assetData = currencyData.assetPerformance?.[assetKey];

    if (assetData) {
      // Hallazgo 3: Si el activo reaparece después de venta (re-compra), reanudar
      if (soldMarkerPushed) {
        soldMarkerPushed = false;
      }
      assetSeen = true;
      timeline.push({
        d: data.date,
        v: assetData.totalValue ?? 0,
        c: assetData.adjustedDailyChangePercentage ?? assetData.dailyChangePercentage ?? 0,
        u: assetData.units ?? 0,
      });
    } else if (assetSeen && !soldMarkerPushed) {
      // Asset desapareció (vendido) — push UN solo marker, luego continue buscando re-entry
      timeline.push({
        d: data.date,
        v: 0,
        c: 0,
        u: 0,
      });
      soldMarkerPushed = true;
      // Hallazgo 3: NO break — continue para soportar re-compras
    }
  }

  return timeline;
}

// PERF-SNAP-025: Computar returns desde daily docs pre-filtrados (reemplaza V2 calls)
function computeAssetReturnsFromDailyDocs(docs, currency, ticker, assetType, now) {
  const assetKey = `${ticker}_${assetType}`;
  const effectiveNow = now || DateTime.now().setZone('America/New_York');
  const boundaries = calculatePeriodBoundaries(effectiveNow);
  const factors = initializePeriodFactors();

  const totalValueDates = [];
  const totalValueValues = [];
  const percentChanges = [];
  const monthlyReturns = {};
  const monthlyFactors = {};
  let firstDate = null;
  let firstValue = 0;
  let lastValue = 0;

  for (const doc of docs) {
    const data = doc.data ? doc.data() : doc;
    const date = data.date;
    const currencyData = data[currency];
    if (!currencyData) continue;

    const assetData = currencyData.assetPerformance?.[assetKey];
    if (!assetData) continue;

    // Period returns (TWR, MWR)
    processDailyDocument(factors, boundaries, assetData, date);

    // Chart data
    const value = assetData.totalValue ?? 0;
    const change = assetData.adjustedDailyChangePercentage ?? assetData.dailyChangePercentage ?? 0;

    totalValueDates.push(date);
    totalValueValues.push(value);
    percentChanges.push(change);

    if (!firstDate) { firstDate = date; firstValue = value; }
    lastValue = value;

    // Monthly grouping for performanceByYear
    const monthKey = date.substring(0, 7);
    if (!monthlyFactors[monthKey]) monthlyFactors[monthKey] = 1;
    monthlyFactors[monthKey] *= (1 + change / 100);
  }

  if (totalValueDates.length === 0) {
    return null;
  }

  // Convert monthly factors to monthlyReturns format
  for (const [monthKey, factor] of Object.entries(monthlyFactors)) {
    const [year, month] = monthKey.split('-');
    if (!monthlyReturns[year]) monthlyReturns[year] = {};
    monthlyReturns[year][parseInt(month, 10).toString()] = (factor - 1) * 100;
  }

  return buildReturnsResult(factors, {
    totalValueDates,
    totalValueValues,
    percentChanges,
    firstDate,
    firstValue,
    lastValue,
    now: effectiveNow,
    monthlyReturns,
    yearlyReturns: {},
  });
}

// PERF-SNAP-030: Computar returns de portfolio desde daily docs (elimina dependencia V2)
function computePortfolioReturnsFromDailyDocs(docs, currency, now) {
  const effectiveNow = now || DateTime.now().setZone('America/New_York');
  const boundaries = calculatePeriodBoundaries(effectiveNow);
  const factors = initializePeriodFactors();

  const totalValueDates = [];
  const totalValueValues = [];
  const percentChanges = [];
  const monthlyFactors = {};
  let firstDate = null;
  let firstValue = 0;
  let lastValue = 0;

  for (const doc of docs) {
    const data = doc.data ? doc.data() : doc;
    const date = data.date;
    const currencyData = data[currency];
    if (!currencyData) continue;

    processDailyDocument(factors, boundaries, currencyData, date);

    const value = currencyData.totalValue ?? 0;
    const change = currencyData.adjustedDailyChangePercentage
      ?? currencyData.dailyChangePercentage ?? 0;

    totalValueDates.push(date);
    totalValueValues.push(value);
    percentChanges.push(change);

    if (!firstDate) { firstDate = date; firstValue = value; }
    lastValue = value;

    const monthKey = date.substring(0, 7);
    if (!monthlyFactors[monthKey]) monthlyFactors[monthKey] = 1;
    monthlyFactors[monthKey] *= (1 + change / 100);
  }

  if (totalValueDates.length === 0) {
    return null;
  }

  const monthlyReturns = {};
  for (const [monthKey, factor] of Object.entries(monthlyFactors)) {
    const [year, month] = monthKey.split('-');
    if (!monthlyReturns[year]) monthlyReturns[year] = {};
    monthlyReturns[year][parseInt(month, 10).toString()] = (factor - 1) * 100;
  }

  return buildReturnsResult(factors, {
    totalValueDates,
    totalValueValues,
    percentChanges,
    firstDate,
    firstValue,
    lastValue,
    now: effectiveNow,
    monthlyReturns,
    yearlyReturns: {},
  });
}

async function generateAssetSnapshot(db, userId, accountId, currency, ticker, assetType, options = {}) {
  // PERF-SNAP-025: Usar daily docs en vez de V2
  const dailyDocs = options.dailyDocs || await fetchAllDailyDocs(db, userId, accountId);

  // Hallazgo 6: Pre-filtrar docs para este activo
  const assetDocs = filterDocsForAsset(dailyDocs, currency, ticker, assetType);

  // Computar returns desde daily docs pre-filtrados (0 calls a V2)
  const now = DateTime.now().setZone('America/New_York');
  const computedResult = computeAssetReturnsFromDailyDocs(assetDocs, currency, ticker, assetType, now);

  if (!computedResult || !computedResult.returns) {
    console.log(`[snapshotGenerator] No asset data for ${ticker}_${assetType} userId=${userId} — skipping`);
    return false;
  }

  const hasAnyData = computedResult.returns.hasYtdData ||
    computedResult.returns.hasOneMonthData ||
    computedResult.returns.hasThreeMonthData;

  if (!hasAnyData) {
    console.log(`[snapshotGenerator] Empty asset result for ${ticker}_${assetType} userId=${userId} — skipping`);
    return false;
  }

  // Timeline diaria completa (necesita todos los docs para detectar gaps/re-compras)
  const timeline = buildAssetDailyTimeline(dailyDocs, currency, ticker, assetType);

  const snapshot = {
    userId,
    currency,
    accountId,
    ticker,
    assetType,
    type: 'asset',
    lastUpdated: new Date().toISOString(),
    schemaVersion: ASSET_SCHEMA_VERSION,

    timelineGranularity: 'daily',

    returns: computedResult.returns,
    timeline,

    performanceByYear: computedResult.performanceByYear || {},
    monthlyCompound: buildMonthlyCompoundFromDailyDocs(assetDocs, currency, ticker, assetType),

    validDocsCountByPeriod: computedResult.validDocsCountByPeriod || {},
    availableYears: computedResult.availableYears || [],
    startDate: computedResult.startDate || '',
  };

  const docId = buildSnapshotDocId(userId, accountId, currency, ticker, assetType);
  console.log(`[snapshotGenerator] Writing asset snapshot ${docId} — timeline: ${timeline.length} points (daily)`);

  await db.collection('performanceSnapshots').doc(docId).set(snapshot);
  // OPT-FIRESTORE-002 P2-A: Return snapshot object (truthy, backward-compatible)
  return snapshot;
}

async function generateAllAssetSnapshots(db, userId, currency, latestAssetPerformance, options = {}) {
  const assetKeys = Object.keys(latestAssetPerformance || {});
  const total = assetKeys.length;
  let success = 0;
  let failed = 0;

  for (const assetKey of assetKeys) {
    const [ticker, assetType] = assetKey.split('_');
    if (!ticker || !assetType) continue;

    try {
      // PERF-SNAP-025: Pasar dailyDocs compartidos
      const wrote = await generateAssetSnapshot(db, userId, 'overall', currency, ticker, assetType, {
        dailyDocs: options.dailyDocs,
      });
      if (wrote) success++;
    } catch (error) {
      failed++;
      console.error(`[snapshotGenerator] Failed asset snapshot ${assetKey} for userId=${userId}:`, error.message);
    }
  }

  console.log(`[snapshotGenerator] Asset snapshots for ${userId}/${currency}: success=${success}, failed=${failed}, total=${total}`);
  return { success, failed, total };
}

async function generateAllSnapshots(db, userId, currencies, accountIds) {
  const allAccountIds = ['overall', ...accountIds];
  const total = allAccountIds.length * currencies.length;
  let success = 0;
  let failed = 0;
  // PERF-SNAP-025: Retornar dailyDocs por cuenta para reutilización en asset snapshots
  const dailyDocsByAccount = new Map();

  for (const accountId of allAccountIds) {
    // Pre-fetch daily docs once per account, reuse across all currencies
    const dailyDocs = await fetchAllDailyDocs(db, userId, accountId);
    dailyDocsByAccount.set(accountId, dailyDocs);

    for (const currency of currencies) {
      try {
        const wrote = await generatePerformanceSnapshot(db, userId, accountId, currency, { dailyDocs });
        if (wrote) {
          success++;
        }
      } catch (error) {
        failed++;
        console.error(`[snapshotGenerator] Failed snapshot for userId=${userId}, accountId=${accountId}, currency=${currency}:`, error.message);
      }
    }
  }

  console.log(`[snapshotGenerator] User ${userId} done — success: ${success}, failed: ${failed}, total: ${total}`);
  return { success, failed, total, dailyDocsByAccount };
}

module.exports = {
  generatePerformanceSnapshot,
  generateAllSnapshots,
  generateAssetSnapshot,
  generateAllAssetSnapshots,
  buildSnapshotDocId,
  transformToCompactTimeline,
  extractAssetPerformanceFields,
  fetchLatestAssetPerformance,
  fetchAllDailyDocs,
  buildDailyTimeline,
  extractLatestAssetPerformanceFromDocs,
  // PERF-SNAP-025
  filterDocsForAsset,
  buildAssetDailyTimeline,
  computeAssetReturnsFromDailyDocs,
  // PERF-SNAP-030
  computePortfolioReturnsFromDailyDocs,
  // P&L monthlyCompound fix
  buildMonthlyCompoundFromDailyDocs,
};
