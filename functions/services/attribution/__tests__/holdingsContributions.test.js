/**
 * Tests para services/attribution/holdingsContributions (FIX-ATTR-HOLDINGS-001)
 *
 * Firestore está falseado con un motor en memoria que entiende
 * where / orderBy / limit / select, para comprobar qué serie se lee, cómo se
 * arma la respuesta y la cache de la serie entre períodos.
 */

const {
  calculateHoldingsContributions,
  _loadDividendTransactions,
  _seriesCache,
  _quoteCurrencyCache,
  _withTimeout,
} = require('../holdingsContributions');
const { MODOS, _reiniciarBloqueos } = require('../../../utils/scopedReads');

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

const RANGE_OPS = new Set(['<', '<=', '>', '>=']);
const byField = (f) => (x, y) => (x.data[f] < y.data[f] ? -1 : x.data[f] > y.data[f] ? 1 : 0);
const byId = (x, y) => (x.id < y.id ? -1 : x.id > y.id ? 1 : 0);

/**
 * @param {Object} collections
 * @param {Object} [options]
 * @param {(path: string, filters: Array, docs: Array) => Array} [options.onQuery] -
 *   HU 6.9.3: altera (o hace fallar) lo que devuelve una consulta, para preparar
 *   una diferencia o un fallo en la lectura acotada
 */
function fakeDb(collections, { onQuery = null } = {}) {
  const calls = [];
  const query = (path, filters = [], order = null, limit = null) => ({
    where: (f, op, v) => query(path, [...filters, [f, op, v]], order, limit),
    orderBy: (f, dir = 'asc') => query(path, filters, [f, dir], limit),
    limit: (n) => query(path, filters, order, n),
    select: () => query(path, filters, order, limit),
    get: async () => {
      let docs = (collections[path] || []).filter((d) => filters.every(([f, op, v]) => compare(d.data[f], op, v)));
      if (order) {
        const [f, dir] = order;
        docs = docs.slice().sort((x, y) => byField(f)(x, y) * (dir === 'desc' ? -1 : 1));
      } else {
        // Como Firestore: sin orderBy, por el campo del rango (si lo hay) y luego por identificador
        const range = filters.find(([, op]) => RANGE_OPS.has(op));
        docs = docs.slice().sort((x, y) => (range ? byField(range[0])(x, y) : 0) || byId(x, y));
      }
      if (limit !== null) docs = docs.slice(0, limit);
      if (onQuery) docs = onQuery(path, filters, docs);
      calls.push({ path, filters, ids: docs.map((d) => d.id) });
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

// ============================================================================
// HU 6.9.3 — La atribución lee sólo los dividendos del periodo consultado
// ============================================================================
//
// Datos de qa.md: INV-L con 310 dividendos desde 2019-01-15 en dos cuentas
// (CTA-US y CTA-CO) e INV-N con 11 desde 2026-07-15. Los identificadores NO van
// en orden de fecha y un tercio de las fechas trae hora (ISO), como en
// producción: así se comprueba que la lectura acotada entrega lo mismo y en el
// mismo orden que la de siempre.
//
// @see platform-docs/stories/6.9.3-solo-periodo-y-ventas-del-dia/qa.md (QA-05..QA-10)

const addDays = (fecha, n) => {
  const d = new Date(`${fecha}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
};

let secuenciaDividendo = 0;
const dividendo = (cuenta, fecha, overrides = {}) => {
  secuenciaDividendo += 1;
  const id = `div-${String((secuenciaDividendo * 7919) % 10007).padStart(5, '0')}`;
  return {
    id,
    data: {
      type: 'dividendPay',
      portfolioAccountId: cuenta,
      symbol: cuenta === 'cta-co' ? 'EC' : 'KO',
      date: secuenciaDividendo % 3 === 0 ? `${fecha}T14:30:00.000Z` : fecha,
      amount: 100,
      price: 0.485 + (secuenciaDividendo % 7) / 100,
      currency: 'USD',
      ...overrides,
    },
  };
};

const P_TRIM_FECHAS = ['2026-08-05', '2026-08-15', '2026-08-25', '2026-09-05', '2026-09-15', '2026-09-25', '2026-10-05', '2026-10-15', '2026-10-25'];

/** 276 antes de 2026 + 25 entre enero y julio de 2026 + 9 en P-TRIM (6 en CTA-US, 3 en CTA-CO) = 310. */
function dividendosDeInvL() {
  const lista = [];
  for (let i = 0; i < 276; i++) lista.push(dividendo(i % 2 ? 'cta-co' : 'cta-us', addDays('2019-01-15', i * 9)));
  for (let i = 0; i < 25; i++) lista.push(dividendo(i % 2 ? 'cta-co' : 'cta-us', addDays('2026-01-05', i * 8)));
  P_TRIM_FECHAS.forEach((fecha, i) => lista.push(dividendo(i < 6 ? 'cta-us' : 'cta-co', fecha)));
  return lista;
}

/** 2 en julio + 9 en P-TRIM = 11, todos en CTA-N. */
function dividendosDeInvN() {
  return ['2026-07-15', '2026-07-25', ...P_TRIM_FECHAS].map((fecha) => dividendo('cta-n', fecha));
}

const docDeL = (date, i) => perfDoc(date, { KO_stock: [5000 + i * 137.5, 100], EC_stock: [800 + i * 21.3, 50] });
const FECHAS_L = ['2019-01-14', '2022-06-30', '2025-12-31', '2026-01-02', '2026-03-31', '2026-07-31', '2026-08-31', '2026-09-30', '2026-10-30', '2026-11-06', '2026-11-12'];
const FECHAS_N = ['2026-07-14', '2026-07-31', '2026-08-31', '2026-09-30', '2026-10-30', '2026-11-12'];

function plataformaDeDividendos({ extraL = [] } = {}) {
  secuenciaDividendo = 0; // mismos identificadores en cada plataforma
  return {
    portfolioAccounts: [
      { id: 'cta-co', data: { userId: 'inv-l', isActive: true } },
      { id: 'cta-us', data: { userId: 'inv-l', isActive: true } },
      { id: 'cta-n', data: { userId: 'inv-n', isActive: true } },
    ],
    assets: [
      { id: 'ko-us', data: { portfolioAccount: 'cta-us', isActive: true, units: 100, name: 'KO' } },
      { id: 'ec-co', data: { portfolioAccount: 'cta-co', isActive: true, units: 50, name: 'EC' } },
      { id: 'ko-n', data: { portfolioAccount: 'cta-n', isActive: true, units: 100, name: 'KO' } },
    ],
    transactions: [...dividendosDeInvL(), ...extraL, ...dividendosDeInvN()],
    'portfolioPerformance/inv-l/dates': FECHAS_L.map(docDeL),
    'portfolioPerformance/inv-n/dates': FECHAS_N.map((date, i) => perfDoc(date, { KO_stock: [4000 + i * 90, 100] })),
  };
}

const PERIODOS = {
  'P-TRIM': { startDate: new Date('2026-08-01T00:00:00Z'), endDate: new Date('2026-10-31T00:00:00Z') },
  'P-ANIO': { startDate: new Date('2026-01-01T00:00:00Z'), endDate: new Date('2026-11-12T00:00:00Z') },
  'P-VACIO': { startDate: new Date('2026-11-01T00:00:00Z'), endDate: new Date('2026-11-12T00:00:00Z') },
  'P-TODO': { startDate: new Date('2019-01-01T00:00:00Z'), endDate: new Date('2026-11-12T00:00:00Z') },
};

const atribucion = (db, uid, periodo) => calculateHoldingsContributions(
  { userId: uid, period: 'YTD', currency: 'USD', accountIds: ['overall'], dateRange: PERIODOS[periodo] },
  deps(db, { getSellTransactionsInPeriod: async () => ({}) })
);

/** Dividendos que entregó Firestore (la MED de qa.md). */
const dividendosLeidos = (db) => db.calls
  .filter((c) => c.path === 'transactions' && c.filters.some(([f, , v]) => f === 'type' && v === 'dividendPay'))
  .flatMap((c) => c.ids);

/** El resultado sin lo que cambia por diseño (la medición) ni por reloj (los tiempos). */
const cifras = (r) => {
  // eslint-disable-next-line no-unused-vars
  const { timingsMs, dividendDocsRead, dividendReadMode, ...coverage } = r.coverage;
  return { ...r, coverage };
};

/** REF-AT de qa.md: la atribución con la lectura de siempre, en una instancia limpia. */
async function referencia(uid, periodo, collections = plataformaDeDividendos()) {
  process.env.SCOPED_READS_ENABLED = 'false';
  try {
    _seriesCache.clear();
    const db = fakeDb(collections);
    const r = await atribucion(db, uid, periodo);
    return { r, leidos: dividendosLeidos(db) };
  } finally {
    delete process.env.SCOPED_READS_ENABLED;
    _seriesCache.clear();
  }
}

const ENTORNO_ORIGINAL = { ...process.env };

describe('HU 6.9.3 — la atribución lee sólo los dividendos del periodo (Escenario 2, RN-6.9.3-K, RN-6.9-N)', () => {
  beforeEach(() => {
    delete process.env.SCOPED_READS_ENABLED;
    delete process.env.SCOPED_READS_VERIFY;
    _reiniciarBloqueos();
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = { ...ENTORNO_ORIGINAL };
    jest.restoreAllMocks();
  });

  it.each([
    ['P-TRIM', 9],
    ['P-ANIO', 34],
    ['P-VACIO', 0],
    ['P-TODO', 310],
  ])('QA-05 — INV-L en %s lee %i dividendos de sus dos cuentas, con el mismo resultado que hoy', async (periodo, esperados) => {
    const ref = await referencia('inv-l', periodo);
    const db = fakeDb(plataformaDeDividendos());

    const r = await atribucion(db, 'inv-l', periodo);

    expect(dividendosLeidos(db)).toHaveLength(esperados);
    expect(r.coverage.dividendDocsRead).toBe(esperados);
    expect(r.coverage.dividendReadMode).toBe(MODOS.ACOTADA);
    expect(cifras(r)).toEqual(cifras(ref.r));
    expect(ref.leidos).toHaveLength(310); // antes: todos, fuera cual fuera el periodo
  });

  it('QA-05 — en P-TRIM lee 6 de CTA-US y 3 de CTA-CO, y esos dividendos sí mueven la atribución', async () => {
    const db = fakeDb(plataformaDeDividendos());
    const r = await atribucion(db, 'inv-l', 'P-TRIM');

    const porCuenta = db.calls
      .filter((c) => c.path === 'transactions')
      .map((c) => [c.filters.find(([f]) => f === 'portfolioAccountId')[2], c.ids.length]);
    expect(Object.fromEntries(porCuenta)).toEqual({ 'cta-co': 3, 'cta-us': 6 });

    // Sin dividendos la contribución de KO sería otra: la comparación no pasa en vacío
    _seriesCache.clear();
    const sinDividendos = plataformaDeDividendos();
    sinDividendos.transactions = [];
    const r0 = await atribucion(fakeDb(sinDividendos), 'inv-l', 'P-TRIM');
    const ko = (x) => x.attributions.find((a) => a.ticker === 'KO').contribution;
    expect(ko(r)).not.toBe(ko(r0));
  });

  it('QA-06 — los extremos del periodo: lee los del 08-01 y del 10-31, no los del 07-31 ni del 11-01', async () => {
    const pago = (id, date, grossAmount) => ({
      id,
      data: { type: 'dividendPay', portfolioAccountId: 'cta-us', symbol: 'KO', date, grossAmount, amount: 100, price: grossAmount / 100, currency: 'USD' },
    });
    const extremos = () => [
      pago('ext-0731', '2026-07-31', 12.40),
      pago('ext-0801', '2026-08-01T10:00:00.000Z', 12.40),
      pago('ext-1031', '2026-10-31', 12.85),
      pago('ext-1101', '2026-11-01T09:00:00.000Z', 12.85),
    ];
    const ref = await referencia('inv-l', 'P-TRIM', plataformaDeDividendos({ extraL: extremos() }));
    const db = fakeDb(plataformaDeDividendos({ extraL: extremos() }));

    const r = await atribucion(db, 'inv-l', 'P-TRIM');

    const leidos = dividendosLeidos(db);
    expect(leidos).toHaveLength(11);
    expect(leidos).toEqual(expect.arrayContaining(['ext-0801', 'ext-1031']));
    expect(leidos).not.toContain('ext-0731');
    expect(leidos).not.toContain('ext-1101');
    expect(cifras(r)).toEqual(cifras(ref.r));
  });

  it('QA-07 — INV-L (310 desde 2019) e INV-N (11 desde julio) leen lo mismo en P-TRIM: 9 cada uno', async () => {
    const antesL = await referencia('inv-l', 'P-TRIM');
    const antesN = await referencia('inv-n', 'P-TRIM');
    expect([antesL.leidos.length, antesN.leidos.length]).toEqual([310, 11]);

    const db = fakeDb(plataformaDeDividendos());
    const l = await atribucion(db, 'inv-l', 'P-TRIM');
    const n = await atribucion(db, 'inv-n', 'P-TRIM');

    expect(l.coverage.dividendDocsRead).toBe(9);
    expect(n.coverage.dividendDocsRead).toBe(9);
    expect(cifras(l)).toEqual(cifras(antesL.r));
    expect(cifras(n)).toEqual(cifras(antesN.r));
  });

  it('sin fin de rango (los periodos hasta hoy) entrega exactamente los mismos dividendos y en el mismo orden', async () => {
    const leer = () => _loadDividendTransactions(fakeDb(plataformaDeDividendos()), ['cta-co', 'cta-us'], '2026-07-31', null);
    process.env.SCOPED_READS_ENABLED = 'false';
    const deSiempre = await leer();
    delete process.env.SCOPED_READS_ENABLED;
    const acotada = await leer();

    expect(deSiempre.lecturas).toBe(310);
    expect(acotada.lecturas).toBe(9);
    expect(acotada.txs).toHaveLength(9);
    // Mismos objetos y mismo orden: por cuenta y, dentro de cada una, por identificador (no por fecha)
    expect(acotada.txs).toEqual(deSiempre.txs);
  });

  it('QA-09 — una diferencia preparada en el método nuevo bloquea el cambio: lee como antes y da REF-AT', async () => {
    const ref = await referencia('inv-l', 'P-TRIM');
    process.env.SCOPED_READS_VERIFY = 'true';
    // A propósito: la lectura acotada pierde el dividendo del 09-15
    const conDiferencia = fakeDb(plataformaDeDividendos(), {
      onQuery: (path, filters, docs) => (path === 'transactions' && filters.some(([f, op]) => f === 'date' && op === '>')
        ? docs.filter((d) => !String(d.data.date).startsWith('2026-09-15'))
        : docs),
    });

    const r = await atribucion(conDiferencia, 'inv-l', 'P-TRIM');

    expect(r.coverage.dividendReadMode).toBe(MODOS.POR_DIFERENCIA);
    expect(r.coverage.dividendDocsRead).toBe(310);
    expect(cifras(r)).toEqual(cifras(ref.r));
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('[scopedReads] DIFERENCIA en dividendos'));

    // Bloqueado: la siguiente consulta sigue leyendo sus 310 dividendos
    delete process.env.SCOPED_READS_VERIFY;
    _seriesCache.clear();
    const siguiente = await atribucion(fakeDb(plataformaDeDividendos()), 'inv-l', 'P-TRIM');
    expect(siguiente.coverage.dividendReadMode).toBe(MODOS.BLOQUEADA);
    expect(siguiente.coverage.dividendDocsRead).toBe(310);
  });

  it('QA-09 — sin diferencias, la verificación usa la acotada e informa el antes y el después', async () => {
    process.env.SCOPED_READS_VERIFY = 'true';
    const r = await atribucion(fakeDb(plataformaDeDividendos()), 'inv-l', 'P-TRIM');

    expect(r.coverage.dividendReadMode).toBe(MODOS.VERIFICADA);
    expect(r.coverage.dividendDocsRead).toBe(9);
    expect(console.log).toHaveBeenCalledWith(expect.stringContaining('9 documentos leídos (acotada-verificada) — antes 310, después 9'));
  });

  it('QA-10 — revertido lee los 310; reaplicado vuelve a 9 con las mismas cifras', async () => {
    const revertido = await referencia('inv-l', 'P-TRIM');
    const reaplicado = await atribucion(fakeDb(plataformaDeDividendos()), 'inv-l', 'P-TRIM');

    expect(revertido.r.coverage.dividendReadMode).toBe(MODOS.COMPLETA);
    expect(revertido.r.coverage.dividendDocsRead).toBe(310);
    expect(reaplicado.coverage.dividendDocsRead).toBe(9);
    expect(cifras(reaplicado)).toEqual(cifras(revertido.r));
  });

  it('si la consulta acotada falla (índice aún sin crear) usa la de siempre y la atribución sale igual', async () => {
    const ref = await referencia('inv-l', 'P-TRIM');
    const sinIndice = fakeDb(plataformaDeDividendos(), {
      onQuery: (path, filters, docs) => {
        if (path === 'transactions' && filters.some(([f]) => f === 'date')) {
          throw new Error('9 FAILED_PRECONDITION: The query requires an index');
        }
        return docs;
      },
    });

    const r = await atribucion(sinIndice, 'inv-l', 'P-TRIM');

    expect(r.coverage.dividendReadMode).toBe(MODOS.POR_ERROR);
    expect(cifras(r)).toEqual(cifras(ref.r));
  });
});
