/**
 * PERF-SNAP-025: Tests for transformSnapshotToResponse enrichment
 */

const { transformSnapshotToResponse } = require('../../services/handlers/queryHandlers');

describe('transformSnapshotToResponse — PERF-SNAP-025', () => {
  const baseDailySnapshot = {
    timelineGranularity: 'daily',
    schemaVersion: 3,
    lastUpdated: '2026-04-14T05:00:00.000Z',
    returns: { ytdReturn: 5.5, hasYtdData: true },
    validDocsCountByPeriod: { ytd: 72 },
    timeline: [
      { d: '2026-01-02', v: 10000, c: 0, u: 10 },
      { d: '2026-01-03', v: 10120, c: 1.2, u: 10 },
      { d: '2026-04-10', v: 10500, c: 0.5, u: 10 },
    ],
    performanceByYear: { '2026': { months: {} } },
    monthlyCompound: {},
    availableYears: ['2026'],
    startDate: '2026-01-02',
    latestAssetPerformance: {},
  };

  test('should include timelineGranularity and soldCompletelyDate for daily asset snapshots', () => {
    const result = transformSnapshotToResponse(baseDailySnapshot);
    expect(result.totalValueData.timelineGranularity).toBe('daily');
    expect(result.totalValueData.soldCompletelyDate).toBeNull();
  });

  test('should set soldCompletelyDate to the date of first u=0 when timeline ends with u=0', () => {
    const snapshot = {
      ...baseDailySnapshot,
      timeline: [
        { d: '2026-01-02', v: 10000, c: 0, u: 10 },
        { d: '2026-01-03', v: 10120, c: 1.2, u: 10 },
        { d: '2026-01-04', v: 0, c: 0, u: 0 },  // sold
      ],
    };
    const result = transformSnapshotToResponse(snapshot);
    expect(result.totalValueData.soldCompletelyDate).toBe('2026-01-04');
  });

  test('should set soldCompletelyDate to null when asset is active or re-purchased', () => {
    const snapshot = {
      ...baseDailySnapshot,
      timeline: [
        { d: '2026-01-02', v: 10000, c: 0, u: 10 },
        { d: '2026-01-03', v: 0, c: 0, u: 0 },      // sold
        { d: '2026-02-01', v: 5000, c: 0, u: 5 },     // re-purchased (u > 0)
      ],
    };
    const result = transformSnapshotToResponse(snapshot);
    // Timeline ends with u=5 > 0, so soldCompletelyDate = null
    expect(result.totalValueData.soldCompletelyDate).toBeNull();
    // Should emit soldRanges for the gap period
    expect(result.totalValueData.soldRanges).toEqual([
      { from: '2026-01-03', to: '2026-02-01' },
    ]);
  });

  test('should NOT include adjustedPercentChanges for any snapshot type', () => {
    const result = transformSnapshotToResponse(baseDailySnapshot);
    expect(result.totalValueData.adjustedPercentChanges).toBeUndefined();
  });

  test('should NOT include units[] for any snapshot type', () => {
    const result = transformSnapshotToResponse(baseDailySnapshot);
    expect(result.totalValueData.units).toBeUndefined();
  });

  test('should NOT include new fields for v2 asset snapshots (no timelineGranularity)', () => {
    const v2Snapshot = {
      schemaVersion: 2,
      lastUpdated: '2026-04-14T05:00:00.000Z',
      returns: { ytdReturn: 5.5, hasYtdData: true },
      validDocsCountByPeriod: { ytd: 72 },
      timeline: [
        { d: '2026-01-02', v: 10000, c: 0 },
        { d: '2026-04-10', v: 10500, c: 0.5 },
      ],
      performanceByYear: {},
      monthlyCompound: {},
      availableYears: ['2026'],
      startDate: '2026-01-02',
      latestAssetPerformance: {},
    };
    const result = transformSnapshotToResponse(v2Snapshot);
    expect(result.totalValueData.timelineGranularity).toBeUndefined();
    expect(result.totalValueData.soldCompletelyDate).toBeUndefined();
  });

  test('should correctly compute dates, values, percentChanges from timeline', () => {
    const result = transformSnapshotToResponse(baseDailySnapshot);
    expect(result.totalValueData.dates).toEqual(['2026-01-02', '2026-01-03', '2026-04-10']);
    expect(result.totalValueData.values).toEqual([10000, 10120, 10500]);
    expect(result.totalValueData.percentChanges).toEqual([0, 1.2, 0.5]);
  });

  test('should include _metadata with schemaVersion', () => {
    const result = transformSnapshotToResponse(baseDailySnapshot);
    expect(result._metadata.schemaVersion).toBe(3);
    expect(result._metadata.version).toBe('snapshot');
  });
});
