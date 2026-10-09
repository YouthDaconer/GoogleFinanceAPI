/**
 * FIX-ATTR-BACKFILL: Tests for backfill producing complete docs (assetPerformance)
 * and NOT clobbering good EOD data with zeros when holdings can't be priced.
 *
 * @see project memory: attribution-empty-treemap-fix-2026-07-01
 */

// The functions under test are pure; we only need the module to load without
// touching real Firestore. Mock the firestore accessors it requires at import time.
jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => ({}),
  FieldValue: { serverTimestamp: () => 'ts', increment: () => 1, delete: () => 'del' },
}));
jest.mock('../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({ collection: jest.fn(), doc: jest.fn() })),
}));

const { calculateAccountDayPerformance, aggregateOverallPerformance } = require('../backfillCoreModule');

const DATE = '2026-06-30';
// reconstructAssetStateForDate rebuilds holdings from transactions (keyed by assetName).
const txns = [
  { type: 'buy', assetName: 'AAPL', assetType: 'stock', date: '2026-01-05', amount: 2, price: 100,
    currency: 'USD', portfolioAccountId: 'acc1' },
];
// getPriceForDate expects a { 'YYYY-MM-DD': price } map per symbol.
const pricesBySymbol = { AAPL: { [DATE]: 150 } };
const rates = { USD: 1 };

describe('calculateAccountDayPerformance — FIX-ATTR-BACKFILL', () => {
  test('builds USD.assetPerformance for priced holdings', () => {
    const perf = calculateAccountDayPerformance(txns, DATE, rates, pricesBySymbol, null);
    expect(perf).not.toBeNull();
    expect(perf.USD.assetPerformance).toBeDefined();
    const ap = perf.USD.assetPerformance['AAPL_stock'];
    expect(ap).toBeDefined();
    expect(ap.units).toBeCloseTo(2, 4);
    expect(ap.totalValue).toBeCloseTo(300, 2); // 2 * 150
    expect(perf.USD.totalValue).toBeCloseTo(300, 2);
  });

  test('returns null (skips write) when holdings exist but none can be priced', () => {
    const perf = calculateAccountDayPerformance(txns, DATE, rates, /* no prices */ {}, null);
    expect(perf).toBeNull(); // must NOT return a zeroed doc that would clobber EOD data
  });
});

describe('aggregateOverallPerformance — FIX-ATTR-BACKFILL', () => {
  test('aggregates assetPerformance across accounts', () => {
    const map = new Map();
    map.set('acc1', { date: DATE, USD: { totalValue: 300, totalInvestment: 200, adjustedDailyChangePercentage: 0, rawDailyChangePercentage: 0,
      assetPerformance: { AAPL_stock: { units: 2, totalValue: 300, totalInvestment: 200, unrealizedProfitAndLoss: 100 } } } });
    map.set('acc2', { date: DATE, USD: { totalValue: 150, totalInvestment: 100, adjustedDailyChangePercentage: 0, rawDailyChangePercentage: 0,
      assetPerformance: { AAPL_stock: { units: 1, totalValue: 150, totalInvestment: 100, unrealizedProfitAndLoss: 50 } } } });

    const overall = aggregateOverallPerformance(map, DATE);
    const ap = overall.USD.assetPerformance['AAPL_stock'];
    expect(ap.units).toBeCloseTo(3, 4);
    expect(ap.totalValue).toBeCloseTo(450, 2);
    expect(ap.totalInvestment).toBeCloseTo(300, 2);
    expect(ap.totalROI).toBeCloseTo(50, 2); // (450-300)/300*100
    expect(overall.USD.totalValue).toBeCloseTo(450, 2);
  });
});
