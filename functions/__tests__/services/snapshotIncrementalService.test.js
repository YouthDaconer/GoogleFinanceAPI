/**
 * OPT-SNAP-INCR: Unit tests for snapshotIncrementalService
 * @see docs/architecture/EPIC-OPT-SNAPSHOT-INCREMENTAL.md — §3.9
 */

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
} = require('../../services/snapshotIncrementalService');

// ============================================================================
// Mock Firestore
// ============================================================================

function createMockDb(snapshotData = null) {
  const setMock = jest.fn().mockResolvedValue(undefined);
  const getMock = jest.fn().mockResolvedValue({
    exists: snapshotData !== null,
    data: () => snapshotData,
  });

  const docMock = jest.fn().mockReturnValue({
    get: getMock,
    set: setMock,
  });

  return { doc: docMock, _set: setMock, _get: getMock, _doc: docMock };
}

// ============================================================================
// Helpers
// ============================================================================

const NOW = DateTime.fromISO('2026-04-30', { zone: 'America/New_York' });

function makeBaseSnapshot(overrides = {}) {
  return {
    userId: 'user1',
    currency: 'USD',
    accountId: 'overall',
    schemaVersion: SCHEMA_VERSION_INCREMENTAL,
    lastDateInTimeline: '2026-04-29',
    timeline: [
      { d: '2026-04-28', v: 10000, c: 0.5 },
      { d: '2026-04-29', v: 10050, c: 0.5 },
    ],
    returns: { ytd: 2.5 },
    performanceByYear: { '2026': { months: { '4': 1.0 }, personalMonths: { '4': 1.0 }, total: 1.0, personalTotal: 1.0 } },
    monthlyCompound: { '2026': { '4': { returnPct: 1.0, startTotalValue: 9900, endTotalValue: 10050, startTotalInvestment: 9000, endTotalInvestment: 9000, totalCashFlow: 0, profit: 50, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 50, lastDayOfMonth: false } } },
    validDocsCountByPeriod: {},
    availableYears: ['2026'],
    startDate: '2026-04-28',
    latestAssetPerformance: {},
    lastUpdated: '2026-04-29T05:00:00.000Z',
    ...overrides,
  };
}

function makeDailyData(overrides = {}) {
  return {
    date: '2026-04-30',
    totalValue: 10100,
    totalInvestment: 9000,
    adjustedDailyChangePercentage: 0.5,
    totalCashFlow: 0,
    doneProfitAndLoss: 0,
    unrealizedProfitAndLoss: 100,
    assetPerformance: {},
    ...overrides,
  };
}

// ============================================================================
// updateSnapshotIncremental
// ============================================================================
describe('updateSnapshotIncremental', () => {
  test('happy path — appends 1 point and returns incremental', async () => {
    const snapshot = makeBaseSnapshot();
    const db = createMockDb(snapshot);
    const dailyData = makeDailyData();

    const result = await updateSnapshotIncremental(db, 'snap-1', dailyData, { now: NOW });

    expect(result).toEqual({ updated: true, method: 'incremental' });
    expect(db._set).toHaveBeenCalledTimes(1);

    const written = db._set.mock.calls[0][0];
    expect(written.timeline).toHaveLength(3);
    expect(written.timeline[2]).toEqual({ d: '2026-04-30', v: 10100, c: 0.5 });
    expect(written.lastDateInTimeline).toBe('2026-04-30');
    expect(written.schemaVersion).toBe(SCHEMA_VERSION_INCREMENTAL);
  });

  test('idempotency — same day returns skipped', async () => {
    const snapshot = makeBaseSnapshot({ lastDateInTimeline: '2026-04-30' });
    const db = createMockDb(snapshot);
    const dailyData = makeDailyData({ date: '2026-04-30' });

    const result = await updateSnapshotIncremental(db, 'snap-1', dailyData, { now: NOW });

    expect(result).toEqual({ updated: true, method: 'skipped', reason: 'already-current' });
    expect(db._set).not.toHaveBeenCalled();
  });

  test('gap detection — >7 calendar days triggers full-rebuild', async () => {
    const snapshot = makeBaseSnapshot({ lastDateInTimeline: '2026-04-15' });
    const db = createMockDb(snapshot);
    const dailyData = makeDailyData({ date: '2026-04-30' });

    const result = await updateSnapshotIncremental(db, 'snap-1', dailyData, { now: NOW });

    expect(result.updated).toBe(false);
    expect(result.method).toBe('full-rebuild');
    expect(result.reason).toMatch(/^gap-\d+-days$/);
    expect(db._set).not.toHaveBeenCalled();
  });

  test('gap within 7 days — proceeds with incremental', async () => {
    // 5 calendar days gap (weekend + 1): should still be incremental
    const snapshot = makeBaseSnapshot({ lastDateInTimeline: '2026-04-25' });
    const db = createMockDb(snapshot);
    const dailyData = makeDailyData({ date: '2026-04-30' });

    const result = await updateSnapshotIncremental(db, 'snap-1', dailyData, { now: NOW });

    expect(result.method).toBe('incremental');
    expect(result.updated).toBe(true);
  });

  test('schema migration — old schema triggers full-rebuild', async () => {
    const snapshot = makeBaseSnapshot({ schemaVersion: 2 });
    const db = createMockDb(snapshot);
    const dailyData = makeDailyData();

    const result = await updateSnapshotIncremental(db, 'snap-1', dailyData, { now: NOW });

    expect(result).toEqual({ updated: false, method: 'full-rebuild', reason: 'missing-or-old-schema' });
    expect(db._set).not.toHaveBeenCalled();
  });

  test('snapshot does not exist — triggers full-rebuild', async () => {
    const db = createMockDb(null);
    const dailyData = makeDailyData();

    const result = await updateSnapshotIncremental(db, 'snap-1', dailyData, { now: NOW });

    expect(result).toEqual({ updated: false, method: 'full-rebuild', reason: 'missing-or-old-schema' });
  });

  test('preserves userId, currency, accountId in written snapshot', async () => {
    const snapshot = makeBaseSnapshot({ userId: 'u99', currency: 'COP', accountId: 'acc-7' });
    const db = createMockDb(snapshot);
    const dailyData = makeDailyData();

    await updateSnapshotIncremental(db, 'snap-1', dailyData, { now: NOW });

    const written = db._set.mock.calls[0][0];
    expect(written.userId).toBe('u99');
    expect(written.currency).toBe('COP');
    expect(written.accountId).toBe('acc-7');
  });

  test('updates latestAssetPerformance when assetPerformance provided', async () => {
    const snapshot = makeBaseSnapshot();
    const db = createMockDb(snapshot);
    const dailyData = makeDailyData({
      assetPerformance: {
        'AAPL_stock': { totalValue: 5000, totalInvestment: 4000, units: 10, unrealizedProfitAndLoss: 1000, totalROI: 25, dailyChangePercentage: 0.8 },
      },
    });

    await updateSnapshotIncremental(db, 'snap-1', dailyData, { now: NOW });

    const written = db._set.mock.calls[0][0];
    expect(written.latestAssetPerformance['AAPL_stock']).toEqual({
      totalValue: 5000,
      totalInvestment: 4000,
      units: 10,
      unrealizedPnL: 1000,
      totalROI: 25,
      dailyChangePercentage: 0.8,
    });
  });
});

// ============================================================================
// updateAssetSnapshotIncremental
// ============================================================================
describe('updateAssetSnapshotIncremental', () => {
  function makeAssetSnapshot(overrides = {}) {
    return {
      userId: 'user1',
      currency: 'USD',
      accountId: 'overall',
      ticker: 'AAPL',
      assetType: 'stock',
      type: 'asset',
      timelineGranularity: 'daily',
      schemaVersion: SCHEMA_VERSION_INCREMENTAL,
      lastDateInTimeline: '2026-04-29',
      timeline: [
        { d: '2026-04-28', v: 5000, c: 1.0, u: 10 },
        { d: '2026-04-29', v: 5050, c: 1.0, u: 10 },
      ],
      returns: { ytd: 3.0 },
      performanceByYear: {},
      monthlyCompound: {},
      validDocsCountByPeriod: {},
      availableYears: ['2026'],
      startDate: '2026-04-28',
      lastUpdated: '2026-04-29T05:00:00.000Z',
      ...overrides,
    };
  }

  test('happy path — appends asset point with units', async () => {
    const snapshot = makeAssetSnapshot();
    const db = createMockDb(snapshot);
    const assetData = { totalValue: 5100, adjustedDailyChangePercentage: 1.0, units: 10 };

    const result = await updateAssetSnapshotIncremental(db, 'asset-snap-1', assetData, '2026-04-30', { now: NOW });

    expect(result).toEqual({ updated: true, method: 'incremental' });
    const written = db._set.mock.calls[0][0];
    expect(written.timeline).toHaveLength(3);
    expect(written.timeline[2]).toEqual({ d: '2026-04-30', v: 5100, c: 1.0, u: 10 });
    expect(written.lastDateInTimeline).toBe('2026-04-30');
  });

  test('sold marker — null asset data writes zero point', async () => {
    const snapshot = makeAssetSnapshot();
    const db = createMockDb(snapshot);

    const result = await updateAssetSnapshotIncremental(db, 'asset-snap-1', null, '2026-04-30', { now: NOW });

    expect(result).toEqual({ updated: true, method: 'incremental' });
    const written = db._set.mock.calls[0][0];
    const lastPoint = written.timeline[written.timeline.length - 1];
    expect(lastPoint).toEqual({ d: '2026-04-30', v: 0, c: 0, u: 0 });
  });

  test('idempotency — same date returns skipped', async () => {
    const snapshot = makeAssetSnapshot({ lastDateInTimeline: '2026-04-30' });
    const db = createMockDb(snapshot);

    const result = await updateAssetSnapshotIncremental(db, 'asset-snap-1', { totalValue: 5100, units: 10 }, '2026-04-30', { now: NOW });

    expect(result).toEqual({ updated: true, method: 'skipped', reason: 'already-current' });
    expect(db._set).not.toHaveBeenCalled();
  });

  test('gap detection — triggers full-rebuild', async () => {
    const snapshot = makeAssetSnapshot({ lastDateInTimeline: '2026-04-10' });
    const db = createMockDb(snapshot);

    const result = await updateAssetSnapshotIncremental(db, 'asset-snap-1', { totalValue: 5100, units: 10 }, '2026-04-30', { now: NOW });

    expect(result.method).toBe('full-rebuild');
    expect(result.reason).toMatch(/^gap-/);
  });

  test('missing schema — triggers full-rebuild', async () => {
    const snapshot = makeAssetSnapshot({ schemaVersion: 1 });
    const db = createMockDb(snapshot);

    const result = await updateAssetSnapshotIncremental(db, 'asset-snap-1', { totalValue: 5100, units: 10 }, '2026-04-30', { now: NOW });

    expect(result).toEqual({ updated: false, method: 'full-rebuild', reason: 'missing-or-old-schema' });
  });
});

// ============================================================================
// computeReturnsFromTimeline
// ============================================================================
describe('computeReturnsFromTimeline', () => {
  test('returns null for empty timeline', () => {
    expect(computeReturnsFromTimeline([], NOW)).toBeNull();
    expect(computeReturnsFromTimeline(null, NOW)).toBeNull();
  });

  test('computes returns from single point', () => {
    const timeline = [{ d: '2026-04-30', v: 10000, c: 1.5 }];
    const result = computeReturnsFromTimeline(timeline, NOW);

    expect(result).not.toBeNull();
    expect(result.startDate).toBe('2026-04-30');
    expect(result.availableYears).toContain('2026');
  });

  test('computes returns from multi-day timeline', () => {
    const timeline = [
      { d: '2026-04-28', v: 10000, c: 0.5 },
      { d: '2026-04-29', v: 10050, c: 0.5 },
      { d: '2026-04-30', v: 10100, c: 0.5 },
    ];
    const result = computeReturnsFromTimeline(timeline, NOW);

    expect(result).not.toBeNull();
    expect(result.returns).toBeDefined();
    expect(result.validDocsCountByPeriod).toBeDefined();
    expect(result.startDate).toBe('2026-04-28');
  });

  test('handles zero-change days correctly', () => {
    const timeline = [
      { d: '2026-04-28', v: 10000, c: 0 },
      { d: '2026-04-29', v: 10000, c: 0 },
      { d: '2026-04-30', v: 10000, c: 0 },
    ];
    const result = computeReturnsFromTimeline(timeline, NOW);

    expect(result).not.toBeNull();
    expect(result.startDate).toBe('2026-04-28');
  });

  test('groups monthly factors correctly across months', () => {
    const timeline = [
      { d: '2026-03-30', v: 9800, c: 1.0 },
      { d: '2026-03-31', v: 9900, c: 1.02 },
      { d: '2026-04-01', v: 9950, c: 0.5 },
      { d: '2026-04-02', v: 10000, c: 0.5 },
    ];
    const result = computeReturnsFromTimeline(timeline, NOW);

    expect(result).not.toBeNull();
    // Should have data across two months
    expect(result.availableYears).toContain('2026');
  });
});

// ============================================================================
// appendToMonthlyCompound
// ============================================================================
describe('appendToMonthlyCompound', () => {
  test('first day of month creates new entry', () => {
    const existing = {};
    const dailyData = {
      date: '2026-05-01',
      adjustedDailyChangePercentage: 1.5,
      totalValue: 10000,
      totalInvestment: 9000,
      totalCashFlow: 0,
      doneProfitAndLoss: 50,
      unrealizedProfitAndLoss: 950,
    };

    const result = appendToMonthlyCompound(existing, dailyData);

    expect(result['2026']['5']).toEqual({
      returnPct: 1.5,
      startTotalValue: 10000,
      startTotalInvestment: 9000,
      endTotalValue: 10000,
      endTotalInvestment: 9000,
      totalCashFlow: 0,
      profit: 1000,
      doneProfitAndLoss: 50,
      unrealizedProfitAndLoss: 950,
      lastDayOfMonth: false,
    });
  });

  test('subsequent days compound returnPct correctly', () => {
    const existing = {
      '2026': {
        '4': {
          returnPct: 1.0,
          startTotalValue: 9900,
          startTotalInvestment: 9000,
          endTotalValue: 10000,
          endTotalInvestment: 9000,
          totalCashFlow: 0,
          profit: 50,
          doneProfitAndLoss: 20,
          unrealizedProfitAndLoss: 30,
          lastDayOfMonth: false,
        },
      },
    };
    const dailyData = {
      date: '2026-04-30',
      adjustedDailyChangePercentage: 0.5,
      totalValue: 10100,
      totalInvestment: 9000,
      totalCashFlow: 0,
      doneProfitAndLoss: 10,
      unrealizedProfitAndLoss: 100,
    };

    const result = appendToMonthlyCompound(existing, dailyData);

    // Compound: (1 + 1/100) * (1 + 0.5/100) - 1 = 1.505%
    const expected = (1.01 * 1.005 - 1) * 100;
    expect(result['2026']['4'].returnPct).toBeCloseTo(expected, 10);
    expect(result['2026']['4'].endTotalValue).toBe(10100);
    expect(result['2026']['4'].doneProfitAndLoss).toBe(30); // 20 + 10
    expect(result['2026']['4'].unrealizedProfitAndLoss).toBe(100);
  });

  test('first day of new month marks previous month as closed', () => {
    const existing = {
      '2026': {
        '4': {
          returnPct: 2.0,
          startTotalValue: 9800,
          endTotalValue: 10000,
          startTotalInvestment: 9000,
          endTotalInvestment: 9000,
          totalCashFlow: 0,
          profit: 200,
          doneProfitAndLoss: 100,
          unrealizedProfitAndLoss: 100,
          lastDayOfMonth: false,
        },
      },
    };
    const dailyData = {
      date: '2026-05-01',
      adjustedDailyChangePercentage: 0.3,
      totalValue: 10030,
      totalInvestment: 9000,
      totalCashFlow: 0,
      doneProfitAndLoss: 0,
      unrealizedProfitAndLoss: 30,
    };

    const result = appendToMonthlyCompound(existing, dailyData);

    expect(result['2026']['4'].lastDayOfMonth).toBe(true);
    expect(result['2026']['5'].lastDayOfMonth).toBe(false);
  });

  test('does not mutate the original object', () => {
    const existing = { '2026': { '4': { returnPct: 1.0, startTotalValue: 9900, endTotalValue: 10000, startTotalInvestment: 9000, endTotalInvestment: 9000, totalCashFlow: 0, profit: 50, doneProfitAndLoss: 20, unrealizedProfitAndLoss: 30, lastDayOfMonth: false } } };
    const dailyData = { date: '2026-04-30', adjustedDailyChangePercentage: 0.5, totalValue: 10100, totalInvestment: 9000, totalCashFlow: 0, doneProfitAndLoss: 10, unrealizedProfitAndLoss: 100 };

    const result = appendToMonthlyCompound(existing, dailyData);

    expect(existing['2026']['4'].returnPct).toBe(1.0); // unchanged
    expect(result['2026']['4'].returnPct).not.toBe(1.0); // changed
  });
});

// ============================================================================
// appendToPerformanceByYear
// ============================================================================
describe('appendToPerformanceByYear', () => {
  test('creates new year entry if not present', () => {
    const existing = {};
    const dailyData = { date: '2026-04-30', adjustedDailyChangePercentage: 1.0 };

    const result = appendToPerformanceByYear(existing, dailyData);

    expect(result['2026'].months['4']).toBeCloseTo(1.0);
    expect(result['2026'].total).toBeCloseTo(1.0);
    expect(result['2026'].personalTotal).toBeCloseTo(1.0);
  });

  test('compounds within same month correctly', () => {
    const existing = {
      '2026': { months: { '4': 1.0 }, personalMonths: { '4': 1.0 }, total: 1.0, personalTotal: 1.0 },
    };
    const dailyData = { date: '2026-04-30', adjustedDailyChangePercentage: 0.5 };

    const result = appendToPerformanceByYear(existing, dailyData);

    const expected = (1.01 * 1.005 - 1) * 100;
    expect(result['2026'].months['4']).toBeCloseTo(expected, 10);
    expect(result['2026'].total).toBeCloseTo(expected, 10);
  });

  test('recomputes year total as compound of all months', () => {
    const existing = {
      '2026': { months: { '1': 2.0, '2': -0.5, '3': 1.5 }, personalMonths: { '1': 2.0, '2': -0.5, '3': 1.5 }, total: 3.0, personalTotal: 3.0 },
    };
    const dailyData = { date: '2026-04-01', adjustedDailyChangePercentage: 0.8 };

    const result = appendToPerformanceByYear(existing, dailyData);

    // Year total = compound of months 1,2,3,4
    const yearFactor = (1 + 2 / 100) * (1 - 0.5 / 100) * (1 + 1.5 / 100) * (1 + 0.8 / 100);
    const expectedTotal = (yearFactor - 1) * 100;
    expect(result['2026'].total).toBeCloseTo(expectedTotal, 8);
  });

  test('does not mutate the original object', () => {
    const existing = { '2026': { months: { '4': 1.0 }, personalMonths: { '4': 1.0 }, total: 1.0, personalTotal: 1.0 } };
    const dailyData = { date: '2026-04-30', adjustedDailyChangePercentage: 0.5 };

    appendToPerformanceByYear(existing, dailyData);

    expect(existing['2026'].months['4']).toBe(1.0);
  });
});

// ============================================================================
// buildLatestAssetPerformance
// ============================================================================
describe('buildLatestAssetPerformance', () => {
  test('extracts correct fields from asset data', () => {
    const assetPerf = {
      'AAPL_stock': {
        totalValue: 5000,
        totalInvestment: 4000,
        units: 10,
        unrealizedProfitAndLoss: 1000,
        totalROI: 25,
        dailyChangePercentage: 0.8,
        extraField: 'should-be-ignored',
      },
      'MSFT_stock': {
        totalValue: 3000,
        totalInvestment: 2800,
        units: 5,
        unrealizedProfitAndLoss: 200,
        totalROI: 7.1,
        dailyChangePercentage: -0.2,
      },
    };

    const result = buildLatestAssetPerformance(assetPerf);

    expect(result['AAPL_stock']).toEqual({
      totalValue: 5000,
      totalInvestment: 4000,
      units: 10,
      unrealizedPnL: 1000,
      totalROI: 25,
      dailyChangePercentage: 0.8,
    });
    expect(result['MSFT_stock']).toEqual({
      totalValue: 3000,
      totalInvestment: 2800,
      units: 5,
      unrealizedPnL: 200,
      totalROI: 7.1,
      dailyChangePercentage: -0.2,
    });
    // extraField should NOT be present
    expect(result['AAPL_stock'].extraField).toBeUndefined();
  });

  test('defaults missing fields to 0', () => {
    const assetPerf = { 'BTC_crypto': {} };
    const result = buildLatestAssetPerformance(assetPerf);

    expect(result['BTC_crypto']).toEqual({
      totalValue: 0,
      totalInvestment: 0,
      units: 0,
      unrealizedPnL: 0,
      totalROI: 0,
      dailyChangePercentage: 0,
    });
  });
});

// ============================================================================
// E2E: Pipeline simulation — 5 consecutive days
// ============================================================================
describe('E2E: 5 consecutive days incremental pipeline', () => {
  test('simulates 5 consecutive days building up a snapshot', async () => {
    const days = [
      { date: '2026-04-28', totalValue: 10000, adjustedDailyChangePercentage: 0.5, totalInvestment: 9500, totalCashFlow: 0, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 500 },
      { date: '2026-04-29', totalValue: 10050, adjustedDailyChangePercentage: 0.5, totalInvestment: 9500, totalCashFlow: 0, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 550 },
      { date: '2026-04-30', totalValue: 10100, adjustedDailyChangePercentage: 0.497, totalInvestment: 9500, totalCashFlow: 0, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 600 },
      { date: '2026-05-01', totalValue: 10080, adjustedDailyChangePercentage: -0.198, totalInvestment: 9500, totalCashFlow: 0, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 580 },
      { date: '2026-05-02', totalValue: 10150, adjustedDailyChangePercentage: 0.694, totalInvestment: 9500, totalCashFlow: 0, doneProfitAndLoss: 0, unrealizedProfitAndLoss: 650 },
    ];

    // Day 1: No snapshot exists → full-rebuild signal
    const db1 = createMockDb(null);
    const r1 = await updateSnapshotIncremental(db1, 'snap-e2e', days[0], { now: NOW });
    expect(r1.method).toBe('full-rebuild');

    // Simulate that the full-rebuild created a v3 snapshot with day 1
    let currentSnapshot = {
      userId: 'user1',
      currency: 'USD',
      accountId: 'overall',
      schemaVersion: SCHEMA_VERSION_INCREMENTAL,
      lastDateInTimeline: '2026-04-28',
      timeline: [{ d: '2026-04-28', v: 10000, c: 0.5 }],
      returns: {},
      performanceByYear: {},
      monthlyCompound: {},
      validDocsCountByPeriod: {},
      availableYears: ['2026'],
      startDate: '2026-04-28',
      latestAssetPerformance: {},
      lastUpdated: '2026-04-28T05:00:00.000Z',
    };

    // Days 2-5: Incremental
    for (let i = 1; i < days.length; i++) {
      const dayNow = DateTime.fromISO(days[i].date, { zone: 'America/New_York' });
      const db = createMockDb(currentSnapshot);
      const result = await updateSnapshotIncremental(db, 'snap-e2e', days[i], { now: dayNow });

      expect(result.method).toBe('incremental');
      expect(result.updated).toBe(true);

      // Capture written snapshot as "current" for next iteration
      currentSnapshot = db._set.mock.calls[0][0];
    }

    // Final snapshot validation
    expect(currentSnapshot.timeline).toHaveLength(5);
    expect(currentSnapshot.lastDateInTimeline).toBe('2026-05-02');
    expect(currentSnapshot.schemaVersion).toBe(SCHEMA_VERSION_INCREMENTAL);

    // Monthly compound should have April (closed) and May (open)
    expect(currentSnapshot.monthlyCompound['2026']['4']).toBeDefined();
    expect(currentSnapshot.monthlyCompound['2026']['4'].lastDayOfMonth).toBe(true);
    expect(currentSnapshot.monthlyCompound['2026']['5']).toBeDefined();
    expect(currentSnapshot.monthlyCompound['2026']['5'].lastDayOfMonth).toBe(false);

    // Performance by year should span both months
    expect(currentSnapshot.performanceByYear['2026'].months['4']).toBeDefined();
    expect(currentSnapshot.performanceByYear['2026'].months['5']).toBeDefined();

    // Returns should be computed
    expect(currentSnapshot.returns).toBeDefined();
  });
});
