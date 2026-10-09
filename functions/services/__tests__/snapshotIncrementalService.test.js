/**
 * Tests para snapshotIncrementalService.js
 *
 * Valida la lógica incremental de actualización de snapshots (Fase 1 OPT-SNAP-INCR).
 *
 * @see docs/architecture/EPIC-OPT-SNAPSHOT-INCREMENTAL.md
 * @module __tests__/services/snapshotIncrementalService.test
 */

// ============================================================================
// Mocks
// ============================================================================

jest.mock('firebase-admin', () => ({
  firestore: Object.assign(jest.fn(() => ({})), {
    FieldValue: { serverTimestamp: jest.fn(() => 'mock-server-timestamp') },
  }),
  initializeApp: jest.fn(),
}));

// Mock periodConsolidation — we test the integration with real logic
// but need the module to be resolvable
jest.mock('../../utils/periodConsolidation', () => {
  const { DateTime } = require('luxon');

  function calculatePeriodBoundaries(now) {
    return {
      fiveYears: now.minus({ years: 5 }).toISODate(),
      twoYears: now.minus({ years: 2 }).toISODate(),
      oneYear: now.minus({ years: 1 }).toISODate(),
      sixMonths: now.minus({ months: 6 }).toISODate(),
      threeMonths: now.minus({ months: 3 }).toISODate(),
      oneMonth: now.minus({ months: 1 }).toISODate(),
      ytd: now.startOf('year').toISODate(),
    };
  }

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
        totalCashFlow: 0,
      };
    });
    return factors;
  }

  function processDailyDocument(periodFactors, boundaries, currencyData, date) {
    Object.entries(boundaries).forEach(([periodKey, boundaryDate]) => {
      if (date >= boundaryDate) {
        const pf = periodFactors[periodKey];
        if (!pf.found) {
          pf.startFactor = pf.currentFactor;
          pf.startValue = currencyData.totalValue || 0;
          pf.found = true;
        }
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

  function buildReturnsResult(periodFactors, chartData) {
    const { firstDate, monthlyReturns = {} } = chartData;

    const calculateReturn = (pf) => {
      if (!pf.found || pf.startFactor === 0) return 0;
      return (pf.currentFactor / pf.startFactor - 1) * 100;
    };

    const performanceByYear = {};
    const yearsWithData = new Set();
    Object.keys(monthlyReturns).forEach(year => {
      performanceByYear[year] = { months: {}, personalMonths: {}, total: 0, personalTotal: 0 };
      let yearCompound = 1;
      Object.keys(monthlyReturns[year]).forEach(month => {
        const monthReturn = monthlyReturns[year][month];
        performanceByYear[year].months[month] = monthReturn;
        performanceByYear[year].personalMonths[month] = monthReturn;
        yearCompound *= (1 + monthReturn / 100);
        yearsWithData.add(year);
      });
      performanceByYear[year].total = (yearCompound - 1) * 100;
      performanceByYear[year].personalTotal = performanceByYear[year].total;
    });

    const availableYears = Array.from(yearsWithData).sort((a, b) => parseInt(b) - parseInt(a));

    return {
      returns: {
        fiveYearReturn: calculateReturn(periodFactors.fiveYears),
        twoYearReturn: calculateReturn(periodFactors.twoYears),
        oneYearReturn: calculateReturn(periodFactors.oneYear),
        sixMonthReturn: calculateReturn(periodFactors.sixMonths),
        threeMonthReturn: calculateReturn(periodFactors.threeMonths),
        oneMonthReturn: calculateReturn(periodFactors.oneMonth),
        ytdReturn: calculateReturn(periodFactors.ytd),
        fiveYearPersonalReturn: 0,
        twoYearPersonalReturn: 0,
        oneYearPersonalReturn: 0,
        sixMonthPersonalReturn: 0,
        threeMonthPersonalReturn: 0,
        oneMonthPersonalReturn: 0,
        ytdPersonalReturn: 0,
        hasFiveYearData: periodFactors.fiveYears.found,
        hasTwoYearData: periodFactors.twoYears.found,
        hasOneYearData: periodFactors.oneYear.found,
        hasSixMonthData: periodFactors.sixMonths.found,
        hasThreeMonthData: periodFactors.threeMonths.found,
        hasOneMonthData: periodFactors.oneMonth.found,
        hasYtdData: periodFactors.ytd.found,
      },
      validDocsCountByPeriod: {
        fiveYears: periodFactors.fiveYears.docsCount,
        twoYears: periodFactors.twoYears.docsCount,
        oneYear: periodFactors.oneYear.docsCount,
        sixMonths: periodFactors.sixMonths.docsCount,
        threeMonths: periodFactors.threeMonths.docsCount,
        oneMonth: periodFactors.oneMonth.docsCount,
        ytd: periodFactors.ytd.docsCount,
      },
      performanceByYear,
      availableYears,
      startDate: firstDate || '',
    };
  }

  return {
    calculatePeriodBoundaries,
    initializePeriodFactors,
    processDailyDocument,
    buildReturnsResult,
  };
});

// ============================================================================
// Imports
// ============================================================================

const { DateTime } = require('luxon');
const {
  updateSnapshotIncremental,
  updateAssetSnapshotIncremental,
  computeReturnsFromTimeline,
  appendToMonthlyCompound,
  appendToPerformanceByYear,
  buildLatestAssetPerformance,
  SCHEMA_VERSION_INCREMENTAL,
  MAX_GAP_CALENDAR_DAYS,
} = require('../snapshotIncrementalService');

// ============================================================================
// Test Helpers
// ============================================================================

/** Fixed "now" for deterministic tests: 2026-04-29 NY timezone */
const NOW = DateTime.fromISO('2026-04-29', { zone: 'America/New_York' });

/**
 * Creates a mock Firestore DB for incremental tests.
 * The doc().get() returns the snapshot provided.
 */
function createMockDb(snapshotData = null) {
  const mockSet = jest.fn().mockResolvedValue();
  const mockGet = jest.fn().mockResolvedValue({
    exists: snapshotData !== null,
    data: () => snapshotData,
  });
  const mockDoc = jest.fn(() => ({ get: mockGet, set: mockSet }));

  return {
    doc: mockDoc,
    _mockSet: mockSet,
    _mockGet: mockGet,
    _mockDoc: mockDoc,
  };
}

/** Builds a valid v3 snapshot with timeline data */
function buildExistingSnapshot(overrides = {}) {
  return {
    userId: 'user-1',
    currency: 'USD',
    accountId: 'overall',
    schemaVersion: SCHEMA_VERSION_INCREMENTAL,
    lastDateInTimeline: '2026-04-28',
    timeline: [
      { d: '2026-04-25', v: 10000, c: 0 },
      { d: '2026-04-28', v: 10100, c: 1.0 },
    ],
    returns: {
      ytdReturn: 5.0,
      oneMonthReturn: 1.0,
      threeMonthReturn: 3.0,
      sixMonthReturn: 4.0,
      oneYearReturn: 8.0,
      twoYearReturn: 12.0,
      fiveYearReturn: 20.0,
      hasYtdData: true,
      hasOneMonthData: true,
      hasThreeMonthData: true,
      hasSixMonthData: true,
      hasOneYearData: true,
      hasTwoYearData: true,
      hasFiveYearData: true,
    },
    performanceByYear: {
      '2026': { months: { '4': 1.0 }, personalMonths: { '4': 1.0 }, total: 1.0, personalTotal: 1.0 },
    },
    monthlyCompound: {
      '2026': {
        '4': {
          returnPct: 1.0,
          startTotalValue: 10000,
          startTotalInvestment: 9000,
          endTotalValue: 10100,
          endTotalInvestment: 9000,
          totalCashFlow: 0,
          profit: 100,
          doneProfitAndLoss: 0,
          unrealizedProfitAndLoss: 100,
          lastDayOfMonth: false,
        },
      },
    },
    validDocsCountByPeriod: { ytd: 2, oneMonth: 2, threeMonths: 2, sixMonths: 2, oneYear: 2, twoYears: 2, fiveYears: 2 },
    availableYears: ['2026'],
    startDate: '2026-04-25',
    latestAssetPerformance: {},
    lastUpdated: '2026-04-28T05:00:00.000Z',
    ...overrides,
  };
}

/** Builds daily data for a new day */
function buildNewDailyData(overrides = {}) {
  return {
    date: '2026-04-29',
    totalValue: 10200,
    totalInvestment: 9000,
    adjustedDailyChangePercentage: 0.99,
    dailyChangePercentage: 0.99,
    totalCashFlow: 0,
    doneProfitAndLoss: 0,
    unrealizedProfitAndLoss: 200,
    assetPerformance: {
      'AAPL_stock': {
        totalValue: 5100,
        totalInvestment: 4500,
        units: 10,
        unrealizedProfitAndLoss: 600,
        totalROI: 13.33,
        dailyChangePercentage: 1.2,
      },
    },
    ...overrides,
  };
}

// ============================================================================
// Tests: computeReturnsFromTimeline
// ============================================================================

describe('OPT-SNAP-INCR: snapshotIncrementalService', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  // ==========================================================================
  // computeReturnsFromTimeline
  // ==========================================================================

  describe('computeReturnsFromTimeline', () => {
    it('should return null for empty timeline', () => {
      expect(computeReturnsFromTimeline([], NOW)).toBeNull();
      expect(computeReturnsFromTimeline(null, NOW)).toBeNull();
    });

    it('should compute returns for a single point', () => {
      const timeline = [{ d: '2026-04-29', v: 10000, c: 1.5 }];
      const result = computeReturnsFromTimeline(timeline, NOW);

      expect(result).not.toBeNull();
      expect(result.returns).toBeDefined();
      expect(result.returns.ytdReturn).toBeCloseTo(1.5, 1);
      expect(result.returns.hasYtdData).toBe(true);
      expect(result.validDocsCountByPeriod.ytd).toBe(1);
      expect(result.startDate).toBe('2026-04-29');
    });

    it('should compound multiple days correctly (TWR)', () => {
      const timeline = [
        { d: '2026-04-25', v: 10000, c: 1.0 },  // +1%
        { d: '2026-04-28', v: 10100, c: 0.5 },  // +0.5%
        { d: '2026-04-29', v: 10200, c: 0.99 }, // ~+1%
      ];
      const result = computeReturnsFromTimeline(timeline, NOW);

      // Compounded: (1.01 * 1.005 * 1.0099) - 1 = ~2.508%
      expect(result.returns.ytdReturn).toBeCloseTo(2.508, 1);
      expect(result.validDocsCountByPeriod.ytd).toBe(3);
      expect(result.availableYears).toContain('2026');
    });

    it('should group into monthly returns (performanceByYear)', () => {
      const timeline = [
        { d: '2026-03-28', v: 9800, c: 0.5 },
        { d: '2026-04-01', v: 9850, c: 0.2 },
        { d: '2026-04-02', v: 9900, c: 0.3 },
      ];
      const result = computeReturnsFromTimeline(timeline, NOW);

      expect(result.performanceByYear['2026']).toBeDefined();
      // Month 3 = 0.5%
      expect(result.performanceByYear['2026'].months['3']).toBeCloseTo(0.5, 2);
      // Month 4 = compound of 0.2% and 0.3% = (1.002 * 1.003 - 1) * 100 ≈ 0.5006%
      expect(result.performanceByYear['2026'].months['4']).toBeCloseTo(0.5006, 2);
    });

    it('should handle zero-change days without error', () => {
      const timeline = [
        { d: '2026-04-28', v: 10000, c: 0 },
        { d: '2026-04-29', v: 10000, c: 0 },
      ];
      const result = computeReturnsFromTimeline(timeline, NOW);

      expect(result.returns.ytdReturn).toBe(0);
      expect(result.validDocsCountByPeriod.ytd).toBe(2);
    });

    it('should handle negative changes', () => {
      const timeline = [
        { d: '2026-04-28', v: 10000, c: -2.0 },
        { d: '2026-04-29', v: 9800, c: -1.0 },
      ];
      const result = computeReturnsFromTimeline(timeline, NOW);

      // (0.98 * 0.99 - 1) * 100 = -2.98%
      expect(result.returns.ytdReturn).toBeCloseTo(-2.98, 1);
    });
  });

  // ==========================================================================
  // updateSnapshotIncremental
  // ==========================================================================

  describe('updateSnapshotIncremental', () => {
    it('should return full-rebuild when snapshot does not exist', async () => {
      const db = createMockDb(null); // no doc exists

      const result = await updateSnapshotIncremental(db, 'user1_USD', buildNewDailyData(), { now: NOW });

      expect(result).toEqual({ updated: false, method: 'full-rebuild', reason: 'missing-or-old-schema' });
      expect(db._mockSet).not.toHaveBeenCalled();
    });

    it('should return full-rebuild when schemaVersion < 3', async () => {
      const db = createMockDb({ schemaVersion: 2, lastDateInTimeline: '2026-04-28' });

      const result = await updateSnapshotIncremental(db, 'user1_USD', buildNewDailyData(), { now: NOW });

      expect(result).toEqual({ updated: false, method: 'full-rebuild', reason: 'missing-or-old-schema' });
      expect(db._mockSet).not.toHaveBeenCalled();
    });

    it('should skip when day already processed (idempotency)', async () => {
      const snapshot = buildExistingSnapshot({ lastDateInTimeline: '2026-04-29' });
      const db = createMockDb(snapshot);

      const result = await updateSnapshotIncremental(
        db, 'user1_USD',
        buildNewDailyData({ date: '2026-04-29' }),
        { now: NOW }
      );

      expect(result).toEqual({ updated: true, method: 'skipped', reason: 'already-current' });
      expect(db._mockSet).not.toHaveBeenCalled();
    });

    it('should return full-rebuild when gap > 7 calendar days', async () => {
      const snapshot = buildExistingSnapshot({ lastDateInTimeline: '2026-04-15' });
      const db = createMockDb(snapshot);

      // Gap: April 15 → April 29 = 14 days > 7
      const result = await updateSnapshotIncremental(
        db, 'user1_USD',
        buildNewDailyData({ date: '2026-04-29' }),
        { now: NOW }
      );

      expect(result.method).toBe('full-rebuild');
      expect(result.reason).toMatch(/gap-14-days/);
      expect(db._mockSet).not.toHaveBeenCalled();
    });

    it('should allow gap <= 7 calendar days (weekend)', async () => {
      // Gap: April 22 → April 29 = 7 days = exactly the limit
      const snapshot = buildExistingSnapshot({ lastDateInTimeline: '2026-04-22' });
      const db = createMockDb(snapshot);

      const result = await updateSnapshotIncremental(
        db, 'user1_USD',
        buildNewDailyData({ date: '2026-04-29' }),
        { now: NOW }
      );

      expect(result.method).toBe('incremental');
      expect(result.updated).toBe(true);
      expect(db._mockSet).toHaveBeenCalledTimes(1);
    });

    it('should append new point and write updated snapshot (happy path)', async () => {
      const snapshot = buildExistingSnapshot();
      const db = createMockDb(snapshot);
      const dailyData = buildNewDailyData();

      const result = await updateSnapshotIncremental(db, 'user1_USD', dailyData, { now: NOW });

      expect(result).toEqual({ updated: true, method: 'incremental' });
      expect(db._mockSet).toHaveBeenCalledTimes(1);

      const written = db._mockSet.mock.calls[0][0];

      // Timeline should have 3 points now (2 existing + 1 new)
      expect(written.timeline).toHaveLength(3);
      expect(written.timeline[2]).toEqual({ d: '2026-04-29', v: 10200, c: 0.99 });

      // Metadata
      expect(written.lastDateInTimeline).toBe('2026-04-29');
      expect(written.schemaVersion).toBe(SCHEMA_VERSION_INCREMENTAL);
      expect(written.lastUpdated).toBeDefined();

      // Returns should be recomputed
      expect(written.returns).toBeDefined();
      expect(typeof written.returns.ytdReturn).toBe('number');
      expect(written.returns.hasYtdData).toBe(true);

      // validDocsCountByPeriod updated
      expect(written.validDocsCountByPeriod.ytd).toBe(3);
    });

    it('should update monthlyCompound incrementally', async () => {
      const snapshot = buildExistingSnapshot();
      const db = createMockDb(snapshot);

      const result = await updateSnapshotIncremental(db, 'user1_USD', buildNewDailyData(), { now: NOW });

      expect(result.method).toBe('incremental');
      const written = db._mockSet.mock.calls[0][0];

      // Month 4, 2026: should have compounded the new day onto existing
      const month4 = written.monthlyCompound['2026']['4'];
      expect(month4).toBeDefined();
      expect(month4.endTotalValue).toBe(10200);
      expect(month4.unrealizedProfitAndLoss).toBe(200);
      // returnPct = compound of 1.0% and 0.99% = (1.01 * 1.0099 - 1) * 100 ≈ 1.9999%
      expect(month4.returnPct).toBeCloseTo(1.9999, 1);
    });

    it('should update performanceByYear incrementally', async () => {
      const snapshot = buildExistingSnapshot();
      const db = createMockDb(snapshot);

      await updateSnapshotIncremental(db, 'user1_USD', buildNewDailyData(), { now: NOW });

      const written = db._mockSet.mock.calls[0][0];
      const year2026 = written.performanceByYear['2026'];
      expect(year2026).toBeDefined();
      // Month 4 compounded: (1.01 * 1.0099 - 1) * 100 ≈ 1.9999%
      expect(year2026.months['4']).toBeCloseTo(1.9999, 1);
      expect(year2026.total).toBeCloseTo(1.9999, 1);
    });

    it('should update latestAssetPerformance when provided', async () => {
      const snapshot = buildExistingSnapshot();
      const db = createMockDb(snapshot);

      await updateSnapshotIncremental(db, 'user1_USD', buildNewDailyData(), { now: NOW });

      const written = db._mockSet.mock.calls[0][0];
      expect(written.latestAssetPerformance['AAPL_stock']).toBeDefined();
      expect(written.latestAssetPerformance['AAPL_stock'].totalValue).toBe(5100);
      expect(written.latestAssetPerformance['AAPL_stock'].units).toBe(10);
    });

    it('should preserve existing latestAssetPerformance when not provided', async () => {
      const snapshot = buildExistingSnapshot({
        latestAssetPerformance: { 'MSFT_stock': { totalValue: 3000 } },
      });
      const db = createMockDb(snapshot);

      const dailyData = buildNewDailyData({ assetPerformance: null });
      await updateSnapshotIncremental(db, 'user1_USD', dailyData, { now: NOW });

      const written = db._mockSet.mock.calls[0][0];
      expect(written.latestAssetPerformance).toEqual({ 'MSFT_stock': { totalValue: 3000 } });
    });

    it('should preserve userId, currency, accountId from existing snapshot', async () => {
      const snapshot = buildExistingSnapshot({ userId: 'usr-x', currency: 'COP', accountId: 'acc-1' });
      const db = createMockDb(snapshot);

      await updateSnapshotIncremental(db, 'usr-x_acc-1_COP', buildNewDailyData(), { now: NOW });

      const written = db._mockSet.mock.calls[0][0];
      expect(written.userId).toBe('usr-x');
      expect(written.currency).toBe('COP');
      expect(written.accountId).toBe('acc-1');
    });
  });

  // ==========================================================================
  // updateAssetSnapshotIncremental
  // ==========================================================================

  describe('updateAssetSnapshotIncremental', () => {
    function buildAssetSnapshot(overrides = {}) {
      return {
        userId: 'user-1',
        currency: 'USD',
        accountId: 'overall',
        ticker: 'AAPL',
        assetType: 'stock',
        type: 'asset',
        timelineGranularity: 'daily',
        schemaVersion: SCHEMA_VERSION_INCREMENTAL,
        lastDateInTimeline: '2026-04-28',
        timeline: [
          { d: '2026-04-25', v: 5000, c: 0, u: 10 },
          { d: '2026-04-28', v: 5050, c: 1.0, u: 10 },
        ],
        returns: { ytdReturn: 1.0, hasYtdData: true },
        performanceByYear: {},
        monthlyCompound: {},
        validDocsCountByPeriod: { ytd: 2 },
        availableYears: ['2026'],
        startDate: '2026-04-25',
        lastUpdated: '2026-04-28T05:00:00.000Z',
        ...overrides,
      };
    }

    it('should return full-rebuild when asset snapshot does not exist', async () => {
      const db = createMockDb(null);
      const result = await updateAssetSnapshotIncremental(
        db, 'user1_AAPL_stock_USD',
        { totalValue: 5100, units: 10, adjustedDailyChangePercentage: 1.0 },
        '2026-04-29',
        { now: NOW }
      );
      expect(result.method).toBe('full-rebuild');
    });

    it('should return full-rebuild when schema is old', async () => {
      const db = createMockDb({ schemaVersion: 1, lastDateInTimeline: '2026-04-28' });
      const result = await updateAssetSnapshotIncremental(
        db, 'user1_AAPL_stock_USD',
        { totalValue: 5100, units: 10 },
        '2026-04-29',
        { now: NOW }
      );
      expect(result.method).toBe('full-rebuild');
    });

    it('should skip when date already in timeline (idempotency)', async () => {
      const snapshot = buildAssetSnapshot({ lastDateInTimeline: '2026-04-29' });
      const db = createMockDb(snapshot);

      const result = await updateAssetSnapshotIncremental(
        db, 'user1_AAPL_stock_USD',
        { totalValue: 5100, units: 10 },
        '2026-04-29',
        { now: NOW }
      );
      expect(result).toEqual({ updated: true, method: 'skipped', reason: 'already-current' });
    });

    it('should return full-rebuild on gap > 7 days', async () => {
      const snapshot = buildAssetSnapshot({ lastDateInTimeline: '2026-04-10' });
      const db = createMockDb(snapshot);

      const result = await updateAssetSnapshotIncremental(
        db, 'user1_AAPL_stock_USD',
        { totalValue: 5100, units: 10 },
        '2026-04-29',
        { now: NOW }
      );
      expect(result.method).toBe('full-rebuild');
      expect(result.reason).toMatch(/gap-19-days/);
    });

    it('should append point for active asset (happy path)', async () => {
      const snapshot = buildAssetSnapshot();
      const db = createMockDb(snapshot);

      const result = await updateAssetSnapshotIncremental(
        db, 'user1_AAPL_stock_USD',
        { totalValue: 5100, units: 10, adjustedDailyChangePercentage: 0.99 },
        '2026-04-29',
        { now: NOW }
      );

      expect(result).toEqual({ updated: true, method: 'incremental' });
      expect(db._mockSet).toHaveBeenCalledTimes(1);

      const written = db._mockSet.mock.calls[0][0];
      expect(written.timeline).toHaveLength(3);
      expect(written.timeline[2]).toEqual({ d: '2026-04-29', v: 5100, c: 0.99, u: 10 });
      expect(written.lastDateInTimeline).toBe('2026-04-29');
      expect(written.schemaVersion).toBe(SCHEMA_VERSION_INCREMENTAL);
      expect(written.type).toBe('asset');
    });

    it('should push sold marker when asset is null (sold)', async () => {
      const snapshot = buildAssetSnapshot();
      const db = createMockDb(snapshot);

      const result = await updateAssetSnapshotIncremental(
        db, 'user1_AAPL_stock_USD',
        null, // sold
        '2026-04-29',
        { now: NOW }
      );

      expect(result).toEqual({ updated: true, method: 'incremental' });

      const written = db._mockSet.mock.calls[0][0];
      expect(written.timeline[2]).toEqual({ d: '2026-04-29', v: 0, c: 0, u: 0 });
    });

    it('should compute returns only from active points (u > 0)', async () => {
      const snapshot = buildAssetSnapshot({
        timeline: [
          { d: '2026-04-22', v: 5000, c: 1.0, u: 10 },
          { d: '2026-04-23', v: 0, c: 0, u: 0 }, // sold
          { d: '2026-04-24', v: 4800, c: 0, u: 8 }, // re-bought
          { d: '2026-04-28', v: 4900, c: 2.08, u: 8 },
        ],
        lastDateInTimeline: '2026-04-28',
      });
      const db = createMockDb(snapshot);

      await updateAssetSnapshotIncremental(
        db, 'user1_AAPL_stock_USD',
        { totalValue: 5000, units: 8, adjustedDailyChangePercentage: 2.04 },
        '2026-04-29',
        { now: NOW }
      );

      const written = db._mockSet.mock.calls[0][0];
      // 5 points in timeline total
      expect(written.timeline).toHaveLength(5);
      // Returns computed from 4 active points (u > 0): days 22, 24, 28, 29
      expect(written.returns).toBeDefined();
      expect(written.validDocsCountByPeriod.ytd).toBe(4);
    });

    it('should preserve ticker, assetType, type fields', async () => {
      const snapshot = buildAssetSnapshot({ ticker: 'TSLA', assetType: 'stock' });
      const db = createMockDb(snapshot);

      await updateAssetSnapshotIncremental(
        db, 'user1_TSLA_stock_USD',
        { totalValue: 7000, units: 5, adjustedDailyChangePercentage: 1.5 },
        '2026-04-29',
        { now: NOW }
      );

      const written = db._mockSet.mock.calls[0][0];
      expect(written.ticker).toBe('TSLA');
      expect(written.assetType).toBe('stock');
      expect(written.type).toBe('asset');
      expect(written.timelineGranularity).toBe('daily');
    });
  });

  // ==========================================================================
  // appendToMonthlyCompound
  // ==========================================================================

  describe('appendToMonthlyCompound', () => {
    it('should create new month entry for first day of month', () => {
      const existing = {};
      const dailyData = {
        date: '2026-05-01',
        adjustedDailyChangePercentage: 1.5,
        totalValue: 10000,
        totalInvestment: 9000,
        totalCashFlow: 0,
        doneProfitAndLoss: 0,
        unrealizedProfitAndLoss: 1000,
      };

      const result = appendToMonthlyCompound(existing, dailyData);

      expect(result['2026']['5']).toBeDefined();
      expect(result['2026']['5'].returnPct).toBe(1.5);
      expect(result['2026']['5'].startTotalValue).toBe(10000);
      expect(result['2026']['5'].endTotalValue).toBe(10000);
      expect(result['2026']['5'].lastDayOfMonth).toBe(false);
    });

    it('should compound onto existing month', () => {
      const existing = {
        '2026': {
          '4': {
            returnPct: 1.0,
            startTotalValue: 10000,
            startTotalInvestment: 9000,
            endTotalValue: 10100,
            endTotalInvestment: 9000,
            totalCashFlow: 0,
            profit: 100,
            doneProfitAndLoss: 50,
            unrealizedProfitAndLoss: 50,
            lastDayOfMonth: false,
          },
        },
      };
      const dailyData = {
        date: '2026-04-29',
        adjustedDailyChangePercentage: 0.5,
        totalValue: 10200,
        totalInvestment: 9000,
        totalCashFlow: 0,
        doneProfitAndLoss: 10,
        unrealizedProfitAndLoss: 200,
      };

      const result = appendToMonthlyCompound(existing, dailyData);

      const month4 = result['2026']['4'];
      // Compound: (1 + 1/100) * (1 + 0.5/100) - 1 = 1.505%
      expect(month4.returnPct).toBeCloseTo(1.505, 2);
      expect(month4.endTotalValue).toBe(10200);
      expect(month4.doneProfitAndLoss).toBe(60); // 50 + 10
      expect(month4.unrealizedProfitAndLoss).toBe(200); // latest
      expect(month4.startTotalValue).toBe(10000); // unchanged
    });

    it('should mark previous month as closed when new month starts', () => {
      const existing = {
        '2026': {
          '3': {
            returnPct: 2.0,
            startTotalValue: 9500,
            startTotalInvestment: 9000,
            endTotalValue: 9690,
            endTotalInvestment: 9000,
            totalCashFlow: 0,
            profit: 190,
            doneProfitAndLoss: 0,
            unrealizedProfitAndLoss: 190,
            lastDayOfMonth: false,
          },
        },
      };
      const dailyData = {
        date: '2026-04-01',
        adjustedDailyChangePercentage: 0.3,
        totalValue: 9720,
        totalInvestment: 9000,
        totalCashFlow: 0,
        doneProfitAndLoss: 0,
        unrealizedProfitAndLoss: 720,
      };

      const result = appendToMonthlyCompound(existing, dailyData);

      // Month 3 should now be closed
      expect(result['2026']['3'].lastDayOfMonth).toBe(true);
      // Month 4 is new
      expect(result['2026']['4'].returnPct).toBe(0.3);
      expect(result['2026']['4'].lastDayOfMonth).toBe(false);
    });

    it('should not mutate the input object', () => {
      const existing = {
        '2026': { '4': { returnPct: 1.0, startTotalValue: 10000, startTotalInvestment: 9000, endTotalValue: 10100, endTotalInvestment: 9000, totalCashFlow: 0, profit: 100, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 100, lastDayOfMonth: false } },
      };
      const original = JSON.parse(JSON.stringify(existing));

      appendToMonthlyCompound(existing, {
        date: '2026-04-29',
        adjustedDailyChangePercentage: 0.5,
        totalValue: 10200,
        totalInvestment: 9000,
      });

      expect(existing).toEqual(original);
    });
  });

  // ==========================================================================
  // appendToPerformanceByYear
  // ==========================================================================

  describe('appendToPerformanceByYear', () => {
    it('should create new year entry when none exists', () => {
      const result = appendToPerformanceByYear({}, {
        date: '2026-04-29',
        adjustedDailyChangePercentage: 1.5,
      });

      expect(result['2026']).toBeDefined();
      expect(result['2026'].months['4']).toBeCloseTo(1.5, 4);
      expect(result['2026'].personalMonths['4']).toBeCloseTo(1.5, 4);
      expect(result['2026'].total).toBeCloseTo(1.5, 4);
      expect(result['2026'].personalTotal).toBeCloseTo(1.5, 4);
    });

    it('should compound onto existing month', () => {
      const existing = {
        '2026': {
          months: { '4': 1.0 },
          personalMonths: { '4': 1.0 },
          total: 1.0,
          personalTotal: 1.0,
        },
      };

      const result = appendToPerformanceByYear(existing, {
        date: '2026-04-29',
        adjustedDailyChangePercentage: 0.5,
      });

      // (1 + 1/100) * (1 + 0.5/100) - 1 = 1.505%
      expect(result['2026'].months['4']).toBeCloseTo(1.505, 2);
      expect(result['2026'].total).toBeCloseTo(1.505, 2);
    });

    it('should compute year total as compound of all months', () => {
      const existing = {
        '2026': {
          months: { '1': 2.0, '2': -1.0, '3': 1.5 },
          personalMonths: { '1': 2.0, '2': -1.0, '3': 1.5 },
          total: 0,
          personalTotal: 0,
        },
      };

      const result = appendToPerformanceByYear(existing, {
        date: '2026-04-01',
        adjustedDailyChangePercentage: 0.8,
      });

      // Year: 1.02 * 0.99 * 1.015 * 1.008 - 1 = 3.34% approx
      const expectedYear = ((1.02 * 0.99 * 1.015 * 1.008) - 1) * 100;
      expect(result['2026'].total).toBeCloseTo(expectedYear, 2);
    });

    it('should not mutate the input object', () => {
      const existing = {
        '2026': { months: { '4': 1.0 }, personalMonths: { '4': 1.0 }, total: 1.0, personalTotal: 1.0 },
      };
      const original = JSON.parse(JSON.stringify(existing));

      appendToPerformanceByYear(existing, {
        date: '2026-04-29',
        adjustedDailyChangePercentage: 0.5,
      });

      expect(existing).toEqual(original);
    });
  });

  // ==========================================================================
  // buildLatestAssetPerformance
  // ==========================================================================

  describe('buildLatestAssetPerformance', () => {
    it('should extract key fields from assetPerformance', () => {
      const input = {
        'AAPL_stock': {
          totalValue: 5000,
          totalInvestment: 4000,
          units: 10,
          unrealizedProfitAndLoss: 1000,
          totalROI: 25.0,
          dailyChangePercentage: 1.2,
          extraField: 'should-be-ignored',
        },
        'BTC_crypto': {
          totalValue: 30000,
          totalInvestment: 25000,
          units: 0.5,
          unrealizedProfitAndLoss: 5000,
          totalROI: 20.0,
          dailyChangePercentage: -0.5,
        },
      };

      const result = buildLatestAssetPerformance(input);

      expect(result['AAPL_stock']).toEqual({
        totalValue: 5000,
        totalInvestment: 4000,
        units: 10,
        unrealizedPnL: 1000,
        totalROI: 25.0,
        dailyChangePercentage: 1.2,
      });
      expect(result['BTC_crypto']).toEqual({
        totalValue: 30000,
        totalInvestment: 25000,
        units: 0.5,
        unrealizedPnL: 5000,
        totalROI: 20.0,
        dailyChangePercentage: -0.5,
      });
    });

    it('should default missing fields to 0', () => {
      const result = buildLatestAssetPerformance({ 'X_stock': {} });

      expect(result['X_stock']).toEqual({
        totalValue: 0,
        totalInvestment: 0,
        units: 0,
        unrealizedPnL: 0,
        totalROI: 0,
        dailyChangePercentage: 0,
      });
    });
  });

  // ==========================================================================
  // E2E: 5-day incremental pipeline simulation
  // ==========================================================================

  describe('E2E: 5-day incremental pipeline', () => {
    it('should correctly accumulate 5 consecutive days', async () => {
      const days = [
        { date: '2026-04-23', totalValue: 10000, adjustedDailyChangePercentage: 0, totalInvestment: 9000, totalCashFlow: 0, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 1000 },
        { date: '2026-04-24', totalValue: 10050, adjustedDailyChangePercentage: 0.5, totalInvestment: 9000, totalCashFlow: 0, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 1050 },
        { date: '2026-04-25', totalValue: 10100, adjustedDailyChangePercentage: 0.497, totalInvestment: 9000, totalCashFlow: 0, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 1100 },
        { date: '2026-04-28', totalValue: 10150, adjustedDailyChangePercentage: 0.495, totalInvestment: 9000, totalCashFlow: 0, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 1150 },
        { date: '2026-04-29', totalValue: 10200, adjustedDailyChangePercentage: 0.493, totalInvestment: 9000, totalCashFlow: 0, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 1200 },
      ];

      // Day 1: full rebuild path (no existing snapshot)
      const db1 = createMockDb(null);
      const result1 = await updateSnapshotIncremental(db1, 'user1_USD', days[0], { now: NOW });
      expect(result1.method).toBe('full-rebuild');

      // Simulate full rebuild by creating the initial snapshot
      let currentSnapshot = buildExistingSnapshot({
        timeline: [{ d: '2026-04-23', v: 10000, c: 0 }],
        lastDateInTimeline: '2026-04-23',
        monthlyCompound: {
          '2026': { '4': { returnPct: 0, startTotalValue: 10000, startTotalInvestment: 9000, endTotalValue: 10000, endTotalInvestment: 9000, totalCashFlow: 0, profit: 1000, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 1000, lastDayOfMonth: false } },
        },
        performanceByYear: { '2026': { months: { '4': 0 }, personalMonths: { '4': 0 }, total: 0, personalTotal: 0 } },
      });

      // Days 2-5: incremental
      for (let i = 1; i < days.length; i++) {
        const db = createMockDb(currentSnapshot);
        const result = await updateSnapshotIncremental(db, 'user1_USD', days[i], { now: NOW });

        expect(result.method).toBe('incremental');
        expect(result.updated).toBe(true);

        // Update currentSnapshot with what was written
        currentSnapshot = db._mockSet.mock.calls[0][0];
      }

      // Final state validation
      expect(currentSnapshot.timeline).toHaveLength(5);
      expect(currentSnapshot.lastDateInTimeline).toBe('2026-04-29');
      expect(currentSnapshot.schemaVersion).toBe(SCHEMA_VERSION_INCREMENTAL);

      // Returns should reflect compounding of all 5 days
      // 0% + 0.5% + 0.497% + 0.495% + 0.493% compounded
      const expectedCompound = ((1.0) * (1.005) * (1.00497) * (1.00495) * (1.00493) - 1) * 100;
      expect(currentSnapshot.returns.ytdReturn).toBeCloseTo(expectedCompound, 1);

      // Monthly compound for April
      const month4 = currentSnapshot.monthlyCompound['2026']['4'];
      expect(month4.returnPct).toBeCloseTo(expectedCompound, 1);
      expect(month4.endTotalValue).toBe(10200);

      // PerformanceByYear
      expect(currentSnapshot.performanceByYear['2026'].months['4']).toBeCloseTo(expectedCompound, 1);
    });
  });

  // ==========================================================================
  // Constants
  // ==========================================================================

  describe('Constants', () => {
    it('SCHEMA_VERSION_INCREMENTAL should be 3', () => {
      expect(SCHEMA_VERSION_INCREMENTAL).toBe(3);
    });

    it('MAX_GAP_CALENDAR_DAYS should be 7', () => {
      expect(MAX_GAP_CALENDAR_DAYS).toBe(7);
    });
  });
});
