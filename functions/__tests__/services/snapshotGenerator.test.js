/**
 * PERF-SNAP-025: Tests for snapshotGenerator — Asset Daily Timeline & Returns from Daily Docs
 */

const {
  filterDocsForAsset,
  buildAssetDailyTimeline,
  computeAssetReturnsFromDailyDocs,
} = require('../../services/snapshotGenerator');

// Helper to create mock daily doc objects
function mockDoc(date, currencyData) {
  const data = { date, ...currencyData };
  return {
    data: () => data,
    id: date,
  };
}

function makeAssetEntry(ticker, assetType, { totalValue = 1000, adjustedDailyChangePercentage = 0, units = 10, dailyChangePercentage } = {}) {
  const assetKey = `${ticker}_${assetType}`;
  const entry = {
    totalValue,
    adjustedDailyChangePercentage,
    units,
  };
  if (dailyChangePercentage !== undefined) {
    entry.dailyChangePercentage = dailyChangePercentage;
  }
  return { assetPerformance: { [assetKey]: entry } };
}

// ============================================================================
// filterDocsForAsset
// ============================================================================
describe('filterDocsForAsset', () => {
  const ticker = 'AAPL';
  const assetType = 'stock';
  const currency = 'USD';

  test('should return only docs containing the specified asset', () => {
    const docs = [
      mockDoc('2026-01-02', { USD: makeAssetEntry('AAPL', 'stock') }),
      mockDoc('2026-01-03', { USD: makeAssetEntry('MSFT', 'stock') }),
      mockDoc('2026-01-04', { USD: makeAssetEntry('AAPL', 'stock') }),
    ];

    const result = filterDocsForAsset(docs, currency, ticker, assetType);
    expect(result).toHaveLength(2);
    expect(result[0].data().date).toBe('2026-01-02');
    expect(result[1].data().date).toBe('2026-01-04');
  });

  test('should return empty array if asset never exists', () => {
    const docs = [
      mockDoc('2026-01-02', { USD: makeAssetEntry('MSFT', 'stock') }),
    ];
    const result = filterDocsForAsset(docs, currency, ticker, assetType);
    expect(result).toHaveLength(0);
  });

  test('should handle docs without currency data gracefully', () => {
    const docs = [
      mockDoc('2026-01-02', { EUR: makeAssetEntry('AAPL', 'stock') }),
    ];
    const result = filterDocsForAsset(docs, currency, ticker, assetType);
    expect(result).toHaveLength(0);
  });
});

// ============================================================================
// buildAssetDailyTimeline
// ============================================================================
describe('buildAssetDailyTimeline', () => {
  const ticker = 'AAPL';
  const assetType = 'stock';
  const currency = 'USD';

  test('should build daily timeline from daily docs for specific asset', () => {
    const docs = [
      mockDoc('2026-01-02', { USD: makeAssetEntry('AAPL', 'stock', { totalValue: 1000, adjustedDailyChangePercentage: 0, units: 10 }) }),
      mockDoc('2026-01-03', { USD: makeAssetEntry('AAPL', 'stock', { totalValue: 1012, adjustedDailyChangePercentage: 1.2, units: 10 }) }),
      mockDoc('2026-01-06', { USD: makeAssetEntry('AAPL', 'stock', { totalValue: 1020, adjustedDailyChangePercentage: 0.79, units: 10 }) }),
    ];

    const timeline = buildAssetDailyTimeline(docs, currency, ticker, assetType);
    expect(timeline).toHaveLength(3);
    expect(timeline[0]).toEqual({ d: '2026-01-02', v: 1000, c: 0, u: 10 });
    expect(timeline[1]).toEqual({ d: '2026-01-03', v: 1012, c: 1.2, u: 10 });
    expect(timeline[2]).toEqual({ d: '2026-01-06', v: 1020, c: 0.79, u: 10 });
  });

  test('should include u (units) field for each entry', () => {
    const docs = [
      mockDoc('2026-01-02', { USD: makeAssetEntry('AAPL', 'stock', { units: 25 }) }),
    ];
    const timeline = buildAssetDailyTimeline(docs, currency, ticker, assetType);
    expect(timeline[0].u).toBe(25);
  });

  test('should use adjustedDailyChangePercentage for c field, falling back to dailyChangePercentage', () => {
    const docs = [
      mockDoc('2026-01-02', {
        USD: {
          assetPerformance: {
            AAPL_stock: {
              totalValue: 1000,
              dailyChangePercentage: 2.5,
              units: 10,
              // NO adjustedDailyChangePercentage
            },
          },
        },
      }),
    ];
    const timeline = buildAssetDailyTimeline(docs, currency, ticker, assetType);
    expect(timeline[0].c).toBe(2.5);
  });

  test('should push sold marker with u:0 and continue scanning (Hallazgo 3)', () => {
    const docs = [
      mockDoc('2026-01-02', { USD: makeAssetEntry('AAPL', 'stock', { totalValue: 1000, units: 10 }) }),
      mockDoc('2026-01-03', { USD: makeAssetEntry('AAPL', 'stock', { totalValue: 1010, units: 10 }) }),
      mockDoc('2026-01-04', { USD: makeAssetEntry('MSFT', 'stock') }),  // AAPL sold
      mockDoc('2026-01-05', { USD: makeAssetEntry('MSFT', 'stock') }),  // AAPL still gone
      mockDoc('2026-01-06', { USD: makeAssetEntry('MSFT', 'stock') }),  // AAPL still gone
    ];

    const timeline = buildAssetDailyTimeline(docs, currency, ticker, assetType);
    expect(timeline).toHaveLength(3);  // 2 normal + 1 sold marker
    expect(timeline[2]).toEqual({ d: '2026-01-04', v: 0, c: 0, u: 0 });
  });

  test('should push only ONE sold marker per gap period', () => {
    const docs = [
      mockDoc('2026-01-02', { USD: makeAssetEntry('AAPL', 'stock', { units: 10 }) }),
      mockDoc('2026-01-03', { USD: makeAssetEntry('MSFT', 'stock') }),  // sold
      mockDoc('2026-01-04', { USD: makeAssetEntry('MSFT', 'stock') }),  // still gone
      mockDoc('2026-01-05', { USD: makeAssetEntry('MSFT', 'stock') }),  // still gone
    ];

    const timeline = buildAssetDailyTimeline(docs, currency, ticker, assetType);
    const soldMarkers = timeline.filter(e => e.u === 0);
    expect(soldMarkers).toHaveLength(1);
  });

  test('should resume timeline after re-purchase (Hallazgo 3)', () => {
    const docs = [
      mockDoc('2026-01-02', { USD: makeAssetEntry('AAPL', 'stock', { totalValue: 1000, units: 10 }) }),
      mockDoc('2026-01-03', { USD: makeAssetEntry('MSFT', 'stock') }),  // AAPL sold
      mockDoc('2026-01-06', { USD: makeAssetEntry('AAPL', 'stock', { totalValue: 500, units: 5 }) }),  // re-purchased
    ];

    const timeline = buildAssetDailyTimeline(docs, currency, ticker, assetType);
    expect(timeline).toHaveLength(3);  // normal + sold marker + re-purchased
    expect(timeline[0]).toMatchObject({ d: '2026-01-02', u: 10 });
    expect(timeline[1]).toMatchObject({ d: '2026-01-03', u: 0 });  // sold marker
    expect(timeline[2]).toMatchObject({ d: '2026-01-06', v: 500, u: 5 });  // re-purchased
  });

  test('should return empty array if asset never exists in docs', () => {
    const docs = [
      mockDoc('2026-01-02', { USD: makeAssetEntry('MSFT', 'stock') }),
      mockDoc('2026-01-03', { USD: makeAssetEntry('MSFT', 'stock') }),
    ];
    const timeline = buildAssetDailyTimeline(docs, currency, ticker, assetType);
    expect(timeline).toHaveLength(0);
  });

  test('should handle asset appearing mid-history', () => {
    const docs = [
      mockDoc('2026-01-02', { USD: makeAssetEntry('MSFT', 'stock') }),  // no AAPL
      mockDoc('2026-01-03', { USD: makeAssetEntry('MSFT', 'stock') }),  // no AAPL
      mockDoc('2026-01-04', { USD: makeAssetEntry('AAPL', 'stock', { totalValue: 800, units: 8 }) }),  // AAPL appears
      mockDoc('2026-01-05', { USD: makeAssetEntry('AAPL', 'stock', { totalValue: 810, units: 8, adjustedDailyChangePercentage: 1.25 }) }),
    ];

    const timeline = buildAssetDailyTimeline(docs, currency, ticker, assetType);
    expect(timeline).toHaveLength(2);
    expect(timeline[0]).toMatchObject({ d: '2026-01-04', v: 800, u: 8 });
    expect(timeline[1]).toMatchObject({ d: '2026-01-05', v: 810, c: 1.25 });
  });
});

// ============================================================================
// computeAssetReturnsFromDailyDocs
// ============================================================================
describe('computeAssetReturnsFromDailyDocs', () => {
  const ticker = 'AAPL';
  const assetType = 'stock';
  const currency = 'USD';

  // Use a fixed "now" for deterministic boundaries
  const { DateTime } = require('luxon');
  const fixedNow = DateTime.fromISO('2026-04-14').setZone('America/New_York');

  test('should compute period returns from daily docs', () => {
    // Create 30 days of data within the last month
    const docs = [];
    for (let i = 30; i >= 1; i--) {
      const date = fixedNow.minus({ days: i }).toISODate();
      docs.push(mockDoc(date, {
        USD: makeAssetEntry('AAPL', 'stock', {
          totalValue: 1000 + (30 - i) * 10,
          adjustedDailyChangePercentage: 1,
          units: 10,
        }),
      }));
    }

    const result = computeAssetReturnsFromDailyDocs(docs, currency, ticker, assetType, fixedNow);
    expect(result).not.toBeNull();
    expect(result.returns).toBeDefined();
    expect(result.returns.hasOneMonthData).toBe(true);
  });

  test('should build monthlyReturns for performanceByYear', () => {
    const docs = [
      mockDoc('2026-03-15', { USD: makeAssetEntry('AAPL', 'stock', { adjustedDailyChangePercentage: 1.5 }) }),
      mockDoc('2026-03-16', { USD: makeAssetEntry('AAPL', 'stock', { adjustedDailyChangePercentage: -0.5 }) }),
      mockDoc('2026-04-01', { USD: makeAssetEntry('AAPL', 'stock', { adjustedDailyChangePercentage: 2.0 }) }),
    ];

    const result = computeAssetReturnsFromDailyDocs(docs, currency, ticker, assetType, fixedNow);
    expect(result).not.toBeNull();
    expect(result.performanceByYear).toBeDefined();
    expect(result.performanceByYear['2026']).toBeDefined();
  });

  test('should return validDocsCountByPeriod', () => {
    const docs = [
      mockDoc('2026-04-10', { USD: makeAssetEntry('AAPL', 'stock', { adjustedDailyChangePercentage: 0.5 }) }),
      mockDoc('2026-04-11', { USD: makeAssetEntry('AAPL', 'stock', { adjustedDailyChangePercentage: 0.3 }) }),
    ];

    const result = computeAssetReturnsFromDailyDocs(docs, currency, ticker, assetType, fixedNow);
    expect(result).not.toBeNull();
    expect(result.validDocsCountByPeriod).toBeDefined();
    expect(result.validDocsCountByPeriod.ytd).toBeGreaterThan(0);
  });

  test('should return null when asset has no data', () => {
    const docs = [
      mockDoc('2026-04-10', { USD: makeAssetEntry('MSFT', 'stock') }),
    ];

    const result = computeAssetReturnsFromDailyDocs(docs, currency, ticker, assetType, fixedNow);
    expect(result).toBeNull();
  });
});
