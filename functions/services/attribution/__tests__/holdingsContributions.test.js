/**
 * Tests para services/attribution/holdingsContributions (FIX-ATTR-HOLDINGS-001)
 *
 * Firestore está falseado con un motor en memoria que entiende
 * where / orderBy / limit / select, para comprobar qué serie se lee, cómo se
 * arma la respuesta y la cache de la serie entre períodos.
 */

const {
  calculateHoldingsContributions,
  _seriesCache,
  _quoteCurrencyCache,
  _withTimeout,
} = require('../holdingsContributions');

// ============================================================================
// Firestore en memoria
// ============================================================================

const compare = (a, op, b) => {
  switch (op) {
    case '==': return a === b;
    case '<': return a < b;
    case '<=': return a <= b;
    case '>': return a > b;
    case '>=': return a >= b;
    case 'in': return b.includes(a);
    default: throw new Error(`op ${op} no soportado`);
  }
};

function fakeDb(collections) {
  const calls = [];
  const query = (path, filters = [], order = null, limit = null) => ({
    where: (f, op, v) => query(path, [...filters, [f, op, v]], order, limit),
    orderBy: (f, dir = 'asc') => query(path, filters, [f, dir], limit),
    limit: (n) => query(path, filters, order, n),
    select: () => query(path, filters, order, limit),
    get: async () => {
      calls.push({ path, filters });
      let docs = (collections[path] || []).filter((d) => filters.every(([f, op, v]) => compare(d.data[f], op, v)));
      if (order) {
        const [f, dir] = order;
        docs = docs.slice().sort((x, y) => (x.data[f] < y.data[f] ? -1 : x.data[f] > y.data[f] ? 1 : 0) * (dir === 'desc' ? -1 : 1));
      }
      if (limit !== null) docs = docs.slice(0, limit);
      return { empty: docs.length === 0, size: docs.length, docs: docs.map((d) => ({ id: d.id, data: () => d.data })) };
    },
  });
  return { calls, collection: (path) => query(path) };
}

// ============================================================================
// Fixtures
// ============================================================================

const UID = 'u1';
const perfDoc = (date, holdings) => ({
  id: date,
  data: {
    date,
    USD: {
      totalValue: Object.values(holdings).reduce((s, [v]) => s + v, 0),
      assetPerformance: Object.fromEntries(Object.entries(holdings).map(([k, [v, u]]) => [k, { totalValue: v, units: u, totalROI: 99 }])),
    },
  },
});

// A sube 10 % dos días seguidos; B cae 5 % y se vende el 07; el 08 entra un aporte a A.
const OVERALL = [
  perfDoc('2026-01-05', { A_stock: [600, 6], B_stock: [400, 4] }),
  perfDoc('2026-01-06', { A_stock: [660, 6], B_stock: [380, 4] }),
  perfDoc('2026-01-07', { A_stock: [726, 6] }),
  perfDoc('2026-01-08', { A_stock: [1452, 12] }),
];
const ACC1 = [
  perfDoc('2026-01-05', { A_stock: [600, 6] }),
  perfDoc('2026-01-06', { A_stock: [660, 6] }),
  perfDoc('2026-01-07', { A_stock: [726, 6] }),
  perfDoc('2026-01-08', { A_stock: [1452, 12] }),
];

function collections() {
  return {
    portfolioAccounts: [
      { id: 'acc-1', data: { userId: UID, isActive: true } },
      { id: 'acc-2', data: { userId: UID, isActive: true } },
    ],
    assets: [
      { id: 'a1', data: { portfolioAccount: 'acc-1', isActive: true, units: 12, name: 'A' } },
    ],
    transactions: [],
    [`portfolioPerformance/${UID}/dates`]: OVERALL,
    [`portfolioPerformance/${UID}/accounts/acc-1/dates`]: ACC1,
    [`portfolioPerformance/${UID}/accounts/acc-2/dates`]: [],
  };
}

function deps(db, overrides = {}) {
  return {
    db,
    getPricesFromApi: async () => [],
    getHistoricalPrices: async () => { throw new Error('no debería pedirse'); },
    fetchSeries: async () => null,
    mapLimit: async (items, _limit, fn) => Promise.all(items.map(fn)),
    chooseHistoryRange: () => '1y',
    fetchConcurrency: 6,
    getSellTransactionsInPeriod: async () => ({
      B_stock: { ticker: 'B', assetType: 'stock', totalRealizedPnL: 12.5, totalSold: 380, transactions: [{}, {}] },
    }),
    priceBudgetMs: 1000,
    ...overrides,
  };
}

const RANGE = { startDate: new Date('2026-01-06T00:00:00Z'), endDate: new Date('2026-01-08T00:00:00Z') };

beforeEach(() => {
  _seriesCache.clear();
  _quoteCurrencyCache.clear();
});

describe('calculateHoldingsContributions', () => {
  it('arma atribuciones por tenencias: suma = TWR, sin repartir residuos', async () => {
    const db = fakeDb(collections());
    const r = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', currency: 'USD', accountIds: ['overall'], dateRange: RANGE },
      deps(db)
    );
    const r2 = (660 / 1040) * 0.1;
    expect(r.method).toBe('holdings');
    expect(r.baseDate).toBe('2026-01-05');
    expect(r.periodStartDate).toBe('2026-01-06');
    expect(r.holdingsTWR).toBeCloseTo((1.04 * (1 + r2) - 1) * 100, 8);
    expect(r.sumOfContributions).toBeCloseTo(r.holdingsTWR, 8);
    expect(r.portfolioReturn).toBeCloseTo(r.holdingsTWR, 8);
    expect(r.normalized).toBe(false);
    expect(r.startTotalValue).toBe(1000);
    expect(r.totalPortfolioValue).toBe(1452);
  });

  it('marca vendido lo que ya no está en assets y le pone peso promedio, no peso de cierre', async () => {
    const db = fakeDb(collections());
    const r = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', currency: 'USD', accountIds: ['overall'], dateRange: RANGE },
      deps(db)
    );
    const a = r.attributions.find((x) => x.ticker === 'A');
    const b = r.attributions.find((x) => x.ticker === 'B');
    expect(a.status).toBe('active');
    expect(a.weightEnd).toBe(1);
    expect(a.hasPartialSales).toBe(false);
    expect(b.status).toBe('sold');
    expect(b.assetKey).toBe('B_stock');
    expect(b.weightEnd).toBe(0);
    expect(b.valueEnd).toBe(0);
    expect(b.weightStart).toBeCloseTo(0.4, 10);
    expect(b.weightAverage).toBeCloseTo((0.4 + 380 / 1040) / 2, 10);
    expect(b.returnPercent).toBeCloseTo(-5, 10);
    expect(b.realizedPnL).toBe(12.5);
    expect(b.partialSalesCount).toBe(2);
    expect(b.hasPartialSales).toBe(false); // la UI muestra "P&L Realizada" para vendidos
    expect(r.attributions[0].ticker).toBe('A'); // orden por contribución
    expect(r.realizedPnL).toEqual({ total: 12.5, assetsWithSales: 1, closedPositions: 1 });
  });

  it('usa la serie overall cuando la selección son todas las cuentas activas', async () => {
    const db = fakeDb(collections());
    await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', accountIds: ['acc-2', 'acc-1'], dateRange: RANGE },
      deps(db)
    );
    const perfPaths = new Set(db.calls.map((c) => c.path).filter((p) => p.startsWith('portfolioPerformance/')));
    expect([...perfPaths]).toEqual([`portfolioPerformance/${UID}/dates`]);
  });

  it('con una sola cuenta lee su serie y sólo considera sus activos', async () => {
    const db = fakeDb(collections());
    const r = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', accountIds: ['acc-1'], dateRange: RANGE },
      deps(db, { getSellTransactionsInPeriod: async () => ({}) })
    );
    const perfPaths = new Set(db.calls.map((c) => c.path).filter((p) => p.startsWith('portfolioPerformance/')));
    expect([...perfPaths]).toEqual([`portfolioPerformance/${UID}/accounts/acc-1/dates`]);
    expect(r.attributions.map((x) => x.ticker)).toEqual(['A']);
    expect(r.holdingsTWR).toBeCloseTo(21, 8);
  });

  it('ignora cuentas que no son del usuario', async () => {
    const db = fakeDb(collections());
    const r = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', accountIds: ['acc-ajena'], dateRange: RANGE },
      deps(db)
    );
    expect(r).toBeNull();
  });

  it('devuelve null con menos de dos días (el llamador cae a legacy)', async () => {
    const c = collections();
    c[`portfolioPerformance/${UID}/dates`] = [OVERALL[0]];
    const r = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', accountIds: ['overall'], dateRange: { startDate: new Date('2026-01-05T00:00:00Z'), endDate: RANGE.endDate } },
      deps(fakeDb(c))
    );
    expect(r).toBeNull();
  });

  it('usa precios de mercado y si se agota el presupuesto cae al precio implícito', async () => {
    const db = fakeDb(collections());
    const series = (closes) => ({ dates: Object.keys(closes), closes: Object.values(closes) });
    const withMarket = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', accountIds: ['overall'], dateRange: RANGE },
      deps(db, {
        getPricesFromApi: async (t) => t.map((symbol) => ({ symbol, currency: 'USD' })),
        fetchSeries: async (t) => (t === 'A'
          ? series({ '2026-01-05': 100, '2026-01-06': 101, '2026-01-07': 102.01, '2026-01-08': 102.01 })
          : series({ '2026-01-05': 100, '2026-01-06': 100, '2026-01-07': 100, '2026-01-08': 100 })),
      })
    );
    expect(withMarket.coverage.marketReturns).toBeGreaterThan(0);
    expect(withMarket.coverage.impliedReturns).toBe(0);
    expect(withMarket.attributions.find((x) => x.ticker === 'A').returnPercent).toBeCloseTo(2.01, 8);

    _seriesCache.clear();
    _quoteCurrencyCache.clear();
    const slow = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', accountIds: ['overall'], dateRange: RANGE },
      deps(fakeDb(collections()), { getPricesFromApi: () => new Promise(() => {}), priceBudgetMs: 10 })
    );
    expect(slow.coverage.priceBudgetExhausted).toBe(true);
    expect(slow.coverage.marketReturns).toBe(0);
    expect(slow.coverage.impliedReturns).toBeGreaterThan(0);
    // Con precio implícito: A +10 % dos días, B -5 % el primero
    const r2 = (660 / 1040) * 0.1;
    expect(slow.holdingsTWR).toBeCloseTo((1.04 * (1 + r2) - 1) * 100, 8);
  });

  it('si se agota el presupuesto usa los precios que ya llegaron y el implícito para el resto', async () => {
    const series = (closes) => ({ dates: Object.keys(closes), closes: Object.values(closes) });
    const r = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', accountIds: ['overall'], dateRange: RANGE },
      deps(fakeDb(collections()), {
        getPricesFromApi: async (t) => t.map((symbol) => ({ symbol, currency: 'USD' })),
        fetchSeries: (t) => (t === 'A'
          ? Promise.resolve(series({ '2026-01-05': 100, '2026-01-06': 101, '2026-01-07': 102.01, '2026-01-08': 102.01 }))
          : new Promise(() => {})), // B nunca llega
        priceBudgetMs: 30,
      })
    );
    expect(r.coverage.priceBudgetExhausted).toBe(true);
    expect(r.coverage.tickersWithMarketPrice).toBe(1);
    expect(r.attributions.find((x) => x.ticker === 'A').returnPercent).toBeCloseTo(2.01, 8); // mercado
    expect(r.attributions.find((x) => x.ticker === 'B').returnPercent).toBeCloseTo(-5, 8); // implícito
  });

  it('pide siempre al menos 5 años de históricos para compartir la cache entre períodos', async () => {
    const ranges = [];
    await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', accountIds: ['overall'], dateRange: RANGE },
      deps(fakeDb(collections()), {
        getPricesFromApi: async (t) => t.map((symbol) => ({ symbol, currency: 'USD' })),
        chooseHistoryRange: () => '1y',
        fetchSeries: async (_t, range) => { ranges.push(range); return null; },
      })
    );
    expect(new Set(ranges)).toEqual(new Set(['5y']));
  });

  it('recuerda la moneda de cotización y no vuelve a pedir cotizaciones', async () => {
    const getPricesFromApi = jest.fn(async (t) => t.map((symbol) => ({ symbol, currency: 'USD' })));
    const params = { userId: UID, period: 'YTD', accountIds: ['overall'], dateRange: RANGE };
    await calculateHoldingsContributions(params, deps(fakeDb(collections()), { getPricesFromApi }));
    await calculateHoldingsContributions(params, deps(fakeDb(collections()), { getPricesFromApi }));
    expect(getPricesFromApi).toHaveBeenCalledTimes(1);
  });

  it('cachea la serie: otro período sólo relee el último día', async () => {
    const db = fakeDb(collections());
    const params = { userId: UID, period: 'YTD', accountIds: ['overall'], dateRange: RANGE };
    const first = await calculateHoldingsContributions(params, deps(db));
    const readsAfterFirst = db.calls.filter((c) => c.path.startsWith('portfolioPerformance/')).length;
    const second = await calculateHoldingsContributions(params, deps(db));
    const tailReads = db.calls.filter((c) => c.path.startsWith('portfolioPerformance/')).slice(readsAfterFirst);
    expect(second.holdingsTWR).toBeCloseTo(first.holdingsTWR, 10);
    // relee base + rango desde el último día conocido, no todo el período
    expect(tailReads).toHaveLength(2);
    expect(tailReads.some((c) => c.filters.some(([f, op, v]) => f === 'date' && op === '>=' && v === '2026-01-08'))).toBe(true);
  });

  it('al pedir un período más largo extiende la cache hacia atrás', async () => {
    const db = fakeDb(collections());
    await calculateHoldingsContributions({ userId: UID, period: 'YTD', accountIds: ['overall'], dateRange: RANGE }, deps(db));
    const longer = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', accountIds: ['overall'], dateRange: { startDate: new Date('2026-01-05T00:00:00Z'), endDate: RANGE.endDate } },
      deps(db)
    );
    expect(longer.baseDate).toBe('2026-01-05');
    expect(longer.periodStartDate).toBe('2026-01-05');
  });
});

describe('withTimeout', () => {
  it('resuelve null si la promesa no termina a tiempo', async () => {
    await expect(_withTimeout(new Promise(() => {}), 5)).resolves.toBeNull();
    await expect(_withTimeout(Promise.resolve(7), 50)).resolves.toBe(7);
  });
});
