/**
 * Tests para OPT-SNAP-INCR Fase 3: Consolidación Pipeline Nocturno + Timeline Windowing
 *
 * Valida:
 * 1. compressToMonthly — compresión correcta de puntos diarios a mensuales
 * 2. applyTimelineWindowing — compactación cuando se excede MAX_DAILY_POINTS
 * 3. saveIndicesHistoryDataInternal — lógica core extraída
 * 4. refreshIndexCacheInternal — lógica core extraída
 *
 * @see docs/architecture/EPIC-OPT-SNAPSHOT-INCREMENTAL.md — Fase 3
 */

// ============================================================================
// Tests de Timeline Windowing (no requieren mocks de Firebase)
// ============================================================================

const {
  compressToMonthly,
  applyTimelineWindowing,
  MAX_DAILY_POINTS,
} = require('../snapshotIncrementalService');

describe('compressToMonthly', () => {
  it('returns empty array for empty input', () => {
    expect(compressToMonthly([])).toEqual([]);
  });

  it('compresses a single month into one point', () => {
    const points = [
      { d: '2025-01-02', v: 1000, c: 1.0 },
      { d: '2025-01-03', v: 1010, c: 0.5 },
      { d: '2025-01-06', v: 1020, c: -0.2 },
    ];

    const result = compressToMonthly(points);

    expect(result).toHaveLength(1);
    expect(result[0].d).toBe('2025-01-06'); // Last date of the group
    expect(result[0].v).toBe(1020); // Last value
    // Compounded: (1 + 1/100) * (1 + 0.5/100) * (1 + (-0.2)/100) - 1
    const expected = ((1.01) * (1.005) * (0.998) - 1) * 100;
    expect(result[0].c).toBeCloseTo(expected, 6);
  });

  it('compresses multiple months correctly', () => {
    const points = [
      { d: '2025-01-02', v: 1000, c: 2.0 },
      { d: '2025-01-03', v: 1020, c: 1.0 },
      { d: '2025-02-03', v: 1030, c: 0.5 },
      { d: '2025-02-04', v: 1035, c: -0.3 },
      { d: '2025-03-03', v: 1040, c: 0.8 },
    ];

    const result = compressToMonthly(points);

    expect(result).toHaveLength(3);
    expect(result[0].d).toBe('2025-01-03');
    expect(result[1].d).toBe('2025-02-04');
    expect(result[2].d).toBe('2025-03-03');
  });

  it('handles zero change correctly', () => {
    const points = [
      { d: '2025-05-01', v: 500, c: 0 },
      { d: '2025-05-02', v: 500, c: 0 },
    ];

    const result = compressToMonthly(points);

    expect(result).toHaveLength(1);
    expect(result[0].c).toBe(0);
  });

  it('handles null/undefined c values gracefully', () => {
    const points = [
      { d: '2025-06-01', v: 100, c: undefined },
      { d: '2025-06-02', v: 100, c: null },
      { d: '2025-06-03', v: 100 },
    ];

    const result = compressToMonthly(points);

    expect(result).toHaveLength(1);
    expect(result[0].c).toBe(0); // All factors are 1 → (1-1)*100 = 0
  });
});

describe('applyTimelineWindowing', () => {
  it('returns timeline unchanged when under threshold', () => {
    const timeline = Array.from({ length: 100 }, (_, i) => ({
      d: `2025-01-${String(i + 1).padStart(2, '0')}`,
      v: 1000 + i,
      c: 0.1,
    }));

    const result = applyTimelineWindowing(timeline);

    expect(result).toHaveLength(100);
    expect(result).toEqual(timeline);
  });

  it('returns timeline unchanged when exactly at threshold', () => {
    const timeline = Array.from({ length: MAX_DAILY_POINTS }, (_, i) => ({
      d: '2025-01-01',
      v: 1000,
      c: 0.1,
    }));

    const result = applyTimelineWindowing(timeline);

    expect(result).toHaveLength(MAX_DAILY_POINTS);
  });

  it('compresses old points when over threshold', () => {
    // Create timeline with MAX_DAILY_POINTS + 60 points (2 months excess)
    const excessDays = 60;
    const totalPoints = MAX_DAILY_POINTS + excessDays;

    const timeline = [];
    const startDate = new Date('2020-01-01');

    for (let i = 0; i < totalPoints; i++) {
      const date = new Date(startDate);
      date.setDate(date.getDate() + i);
      timeline.push({
        d: date.toISOString().split('T')[0],
        v: 1000 + i,
        c: 0.05,
      });
    }

    const result = applyTimelineWindowing(timeline);

    // Should be compressed: the 60 excess daily points → ~2 monthly points
    // Plus the MAX_DAILY_POINTS recent points
    expect(result.length).toBeLessThanOrEqual(MAX_DAILY_POINTS + 3); // monthly compressed + recent
    expect(result.length).toBeGreaterThan(MAX_DAILY_POINTS); // monthly points added
    expect(result.length).toBeLessThan(totalPoints); // definitely compressed

    // Last point should be preserved exactly
    expect(result[result.length - 1]).toEqual(timeline[timeline.length - 1]);
  });

  it('preserves recent points exactly (no data loss for <5yr data)', () => {
    const excessDays = 30;
    const totalPoints = MAX_DAILY_POINTS + excessDays;
    const timeline = [];

    for (let i = 0; i < totalPoints; i++) {
      timeline.push({
        d: `2020-01-${String((i % 28) + 1).padStart(2, '0')}`,
        v: 1000 + i * 10,
        c: 0.1 * i,
      });
    }

    const result = applyTimelineWindowing(timeline);

    // The last MAX_DAILY_POINTS points should be preserved exactly
    const recentOriginal = timeline.slice(excessDays);
    const recentResult = result.slice(result.length - MAX_DAILY_POINTS);

    expect(recentResult).toEqual(recentOriginal);
  });

  it('compressed months preserve TWR (compounded return)', () => {
    // 40 days excess, all in same month with c=1%
    const excessDays = 40;
    const totalPoints = MAX_DAILY_POINTS + excessDays;
    const timeline = [];

    for (let i = 0; i < excessDays; i++) {
      timeline.push({
        d: `2019-03-${String(i + 1).padStart(2, '0')}`,
        v: 1000 + i,
        c: 1.0, // 1% daily
      });
    }

    // Fill the rest with recent points
    for (let i = 0; i < MAX_DAILY_POINTS; i++) {
      timeline.push({
        d: `2020-01-${String((i % 28) + 1).padStart(2, '0')}`,
        v: 2000 + i,
        c: 0.5,
      });
    }

    const result = applyTimelineWindowing(timeline);

    // First point(s) should be the compressed month
    const compressedPoint = result[0];
    // 40 days at 1% = (1.01^40 - 1) * 100
    const expectedReturn = (Math.pow(1.01, 40) - 1) * 100;
    expect(compressedPoint.c).toBeCloseTo(expectedReturn, 4);
  });
});

describe('MAX_DAILY_POINTS constant', () => {
  it('is set to 1260 (5 years of trading days)', () => {
    expect(MAX_DAILY_POINTS).toBe(1260);
  });
});
