/**
 * LEDGER-LOTS y "todo o nada" en la reconciliación.
 *
 * Casos reales de DDeR8P que producían saltos de unidades sin transacción en
 * cada frontera entre un día del EOD (unidades de los lotes) y uno recalculado
 * (unidades del libro): SPYG con un lote de 0,33 sin compra, DPZ con un lote
 * de 0,05 cuya compra dice 0,03 y NVDA con una venta de un lote borrado.
 *
 * Y la reconciliación de noviembre: sin token, todo 401, 0 días valorables y
 * aun así `success: true`.
 */

// --- Firestore en memoria ----------------------------------------------------
const mockState = { collections: {}, writes: [], snapshotsSaved: [] };

const mockCompare = (a, op, b) => {
  switch (op) {
    case '==': return a === b;
    case '<': return a < b;
    case '<=': return a <= b;
    case '>': return a > b;
    case '>=': return a >= b;
    case 'in': return b.includes(a);
    default: throw new Error(`op ${op}`);
  }
};

function mockQuery(path, filters = [], order = null) {
  return {
    where: (f, op, v) => mockQuery(path, [...filters, [f, op, v]], order),
    orderBy: (f, dir = 'asc') => mockQuery(path, filters, [f, dir]),
    doc: (id) => mockDocRef(`${path}/${id}`),
    get: async () => {
      let docs = (mockState.collections[path] || []).filter((d) => filters.every(([f, op, v]) => mockCompare(d.data[f], op, v)));
      if (order) docs = docs.slice().sort((x, y) => String(x.data[order[0]]).localeCompare(String(y.data[order[0]])));
      const wrapped = docs.map((d) => ({ id: d.id, data: () => d.data }));
      return { empty: wrapped.length === 0, size: wrapped.length, docs: wrapped, forEach: (fn) => wrapped.forEach(fn) };
    },
  };
}

function mockDocRef(path) {
  return {
    path,
    collection: (name) => mockQuery(`${path}/${name}`),
    get: async () => ({ exists: false, data: () => undefined }),
    set: async (data) => { mockState.snapshotsSaved.push({ path, data }); },
  };
}

jest.mock('../firebaseAdmin', () => ({
  firestore: () => ({
    collection: (name) => mockQuery(name),
    batch: () => {
      const pending = [];
      return {
        set: (ref, data, opts) => pending.push({ path: ref.path, data, opts }),
        delete: (ref) => pending.push({ path: ref.path, deleted: true }),
        commit: async () => { mockState.writes.push(...pending); },
      };
    },
  }),
}));
jest.mock('firebase-admin/firestore', () => ({ FieldValue: { delete: () => '__delete__' } }));

const mockRates = jest.fn();
jest.mock('../historicalRateService', () => ({ getRatesForRange: (...args) => mockRates(...args) }));

const mockPrices = {};
const mockHttpStatus = { code: 200 };
jest.mock('node-fetch', () => {
  const fn = jest.fn(async (url) => {
    const symbol = decodeURIComponent(/symbol=([^&]+)/.exec(url)[1]);
    if (mockHttpStatus.code !== 200) return { ok: false, status: mockHttpStatus.code, json: async () => ({}) };
    const series = mockPrices[symbol];
    if (!series) return { ok: false, status: 404, json: async () => ({}) };
    return { ok: true, status: 200, json: async () => Object.fromEntries(Object.entries(series).map(([d, c]) => [d, { close: c }])) };
  });
  fn.default = fn;
  return fn;
});

jest.mock('../config', () => ({ getServiceTokenSecret: () => 'abc123' }));

const {
  alignLedgerWithLots,
  reconstructAssetStateForDate,
  calculateDailyCashFlow,
  calculateAccountDayPerformance,
  impliedPricesByDate,
  backfillUserPerformance,
  clearHistoricalPriceCache,
  CONFIG,
} = require('../backfillCoreModule');

CONFIG.API_DELAY_MS = 0;

// --- Fixtures ----------------------------------------------------------------
const lot = (id, name, units, overrides = {}) => ({
  id, name, assetType: 'etf', portfolioAccount: 'ibkr', isActive: units > 0, units,
  unitValue: 50, currency: 'USD', acquisitionDate: '2024-11-01T14:29:28.000Z', ...overrides,
});
const tx = (id, type, assetId, amount, date, overrides = {}) => ({
  id, type, assetId, assetName: 'SPYG', assetType: 'etf', amount, price: 100, currency: 'USD',
  date, portfolioAccountId: 'ibkr', ...overrides,
});

const byId = (lots) => new Map(lots.map((l) => [l.id, l]));
const unitsOf = (assets, name) => assets.filter((a) => a.name === name).reduce((s, a) => s + a.units, 0);

describe('alignLedgerWithLots', () => {
  // SPYG: un lote sin compra (sólo sus ventas) y otro que cuadra
  const LOTS = [
    lot('L1', 'SPYG', 0, { isActive: false }),
    lot('L2', 'SPYG', 3.6004, { acquisitionDate: '2025-01-28T14:39:45.000Z' }),
  ];
  const TXS = [
    tx('b2', 'buy', 'L2', 4, '2025-01-28T14:39:45.000Z'),
    tx('s1', 'sell', 'L1', 0.1313, '2026-01-28T21:13:12.000Z'),
    tx('s2', 'sell', 'L1', 0.1987, '2026-03-04T15:29:33.000Z'),
    tx('s3', 'sell', 'L2', 0.3996, '2026-03-04T15:29:33.000Z'),
  ];

  test('añade la compra que le falta a un lote, fechada en su adquisición', () => {
    const { transactions, corrections } = alignLedgerWithLots(TXS, byId(LOTS), 'ibkr');
    expect(corrections).toEqual([expect.objectContaining({ assetId: 'L1', date: '2024-11-01', applied: true })]);
    expect(corrections[0].gap).toBeCloseTo(0.33, 8);
    const synthetic = transactions.find((t) => t.synthetic);
    expect(synthetic).toMatchObject({ type: 'buy', assetId: 'L1', price: 50, date: '2024-11-01' });
  });

  test('las unidades reconstruidas son las de los lotes: antes y después de las ventas', () => {
    const { transactions } = alignLedgerWithLots(TXS, byId(LOTS), 'ibkr');
    // Hoy: lo mismo que suman los lotes (lo que ve el EOD)
    expect(unitsOf(reconstructAssetStateForDate(transactions, '2026-11-10'), 'SPYG')).toBeCloseTo(3.6004, 8);
    // Diciembre: lotes de entonces = 4 + 0,33 (el libro solo daba 4)
    expect(unitsOf(reconstructAssetStateForDate(transactions, '2025-12-31'), 'SPYG')).toBeCloseTo(4.33, 8);
    // Sin alinear, el libro se queda 0,33 por debajo para siempre
    expect(unitsOf(reconstructAssetStateForDate(TXS, '2026-11-10'), 'SPYG')).toBeCloseTo(3.2704, 8);
  });

  test('la corrección es un flujo el día de la adquisición: no cuenta como rendimiento', () => {
    const { transactions } = alignLedgerWithLots(TXS, byId(LOTS), 'ibkr');
    expect(calculateDailyCashFlow(transactions, '2024-11-01')).toBeCloseTo(-0.33 * 50, 8);
  });

  test('un lote con más unidades que su compra se corrige por la diferencia (DPZ 0,05 vs 0,03)', () => {
    const lots = [lot('D1', 'DPZ', 0.05, { assetType: 'stock', acquisitionDate: '2025-11-03T15:11:22.000Z' })];
    const txs = [tx('bd', 'buy', 'D1', 0.03, '2025-11-03T15:11:25.000Z', { assetName: 'DPZ', assetType: 'stock' })];
    const { transactions, corrections } = alignLedgerWithLots(txs, byId(lots), 'ibkr');
    expect(corrections[0].gap).toBeCloseTo(0.02, 8);
    expect(unitsOf(reconstructAssetStateForDate(transactions, '2025-12-31'), 'DPZ')).toBeCloseTo(0.05, 8);
  });

  test('un lote con menos unidades que su libro se corrige a la baja', () => {
    const lots = [lot('X', 'SPYG', 1, { acquisitionDate: '2025-01-10T10:00:00.000Z' })];
    const txs = [tx('bx', 'buy', 'X', 1.5, '2025-01-10T10:00:00.000Z')];
    const { transactions, corrections } = alignLedgerWithLots(txs, byId(lots), 'ibkr');
    expect(corrections[0]).toMatchObject({ gap: -0.5, date: '2025-01-10' });
    expect(unitsOf(reconstructAssetStateForDate(transactions, '2025-06-01'), 'SPYG')).toBeCloseTo(1, 8);
  });

  test('las transacciones de un lote que ya no existe no cuentan (NVDA 0,3)', () => {
    const lots = [lot('N1', 'NVDA', 1, { assetType: 'stock' })];
    const txs = [
      tx('bn', 'buy', 'N1', 1, '2024-11-01T14:29:28.000Z', { assetName: 'NVDA', assetType: 'stock' }),
      tx('sx', 'sell', 'BORRADO', 0.3, '2025-09-02T23:50:52.000Z', { assetName: 'NVDA', assetType: 'stock' }),
    ];
    const { transactions, orphans, corrections } = alignLedgerWithLots(txs, byId(lots), 'ibkr');
    expect(orphans.map((t) => t.id)).toEqual(['sx']);
    expect(corrections).toEqual([]);
    expect(unitsOf(reconstructAssetStateForDate(transactions, '2026-01-01'), 'NVDA')).toBeCloseTo(1, 8);
    expect(calculateDailyCashFlow(transactions, '2025-09-02')).toBe(0);
  });

  test('nombre y tipo salen del lote, como agrupa el EOD', () => {
    const lots = [lot('V', 'VUAA.L', 2)];
    const txs = [tx('bv', 'buy', 'V', 2, '2024-11-01T14:29:28.000Z', { assetName: 'VUAA' })];
    const { transactions, corrections } = alignLedgerWithLots(txs, byId(lots), 'ibkr');
    expect(corrections).toEqual([]);
    expect(transactions[0].assetName).toBe('VUAA.L');
  });

  test('un libro que cuadra no cambia nada; el efectivo y lo antiguo sin assetId pasan tal cual', () => {
    const lots = [lot('L2', 'SPYG', 4)];
    const txs = [
      tx('b2', 'buy', 'L2', 4, '2025-01-28T14:39:45.000Z'),
      { id: 'c1', type: 'cash_income', amount: 100, date: '2025-02-01', portfolioAccountId: 'ibkr' },
      tx('old', 'buy', undefined, 1, '2024-01-01T00:00:00.000Z', { assetName: 'AAPL' }),
    ];
    const out = alignLedgerWithLots(txs, byId(lots), 'ibkr');
    expect(out.corrections).toEqual([]);
    expect(out.orphans).toEqual([]);
    expect(out.transactions.map((t) => t.id).sort()).toEqual(['b2', 'c1', 'old']);
  });

  test('un lote inactivo vale 0 aunque conserve unidades, como en el EOD (NKE 0,5 en XTB)', () => {
    const lots = [
      lot('K1', 'NKE', 0.5, { isActive: false, assetType: 'stock' }),
      lot('K2', 'NKE', 1, { isActive: false, assetType: 'stock' }),
    ];
    const txs = [tx('bk', 'buy', 'K2', 1, '2025-02-10T10:00:00.000Z', { assetName: 'NKE', assetType: 'stock' })];
    const { transactions, corrections } = alignLedgerWithLots(txs, byId(lots), 'ibkr');
    // K1 no tiene libro y vale 0: nada que corregir. K2 compró 1 y vale 0: se quita.
    expect(corrections.map((c) => [c.assetId, c.gap])).toEqual([['K2', -1]]);
    expect(unitsOf(reconstructAssetStateForDate(transactions, '2026-11-10'), 'NKE')).toBe(0);
  });

  test('sólo corrige los lotes de la cuenta que se procesa', () => {
    const lots = [lot('L1', 'SPYG', 0.33, { portfolioAccount: 'xtb' })];
    expect(alignLedgerWithLots([], byId(lots), 'ibkr').corrections).toEqual([]);
  });
});

describe('precios implícitos del EOD (impliedPricesByDate)', () => {
  const existing = {
    overall: { '2026-02-19': { USD: { assetPerformance: { BKNG_stock: { units: 0.0557, totalValue: 223.21 }, X_stock: { units: 0, totalValue: 0 } } } } },
    accounts: { xtb: { '2026-02-19': { USD: { assetPerformance: { MA_stock: { units: 0.0825, totalValue: 42.92 } } } } } },
  };

  test('valor / unidades por día; ignora posiciones vacías', () => {
    const out = impliedPricesByDate(existing);
    expect(out['2026-02-19'].BKNG_stock).toBeCloseTo(4007.36, 2);
    expect(out['2026-02-19'].MA_stock).toBeCloseTo(520.24, 2);
    expect(out['2026-02-19'].X_stock).toBeUndefined();
  });

  test('un split no deforma el día: el implícito manda sobre el histórico ajustado', () => {
    const txs = [{ id: 'b', type: 'buy', assetName: 'BKNG', assetType: 'stock', amount: 0.0557, price: 3945, currency: 'USD', date: '2026-02-19T15:00:00Z', portfolioAccountId: 'xtb' }];
    // Histórico ajustado por el split 25:1
    const prices = { BKNG: { '2026-02-19': 160.29 } };
    const conHistorico = calculateAccountDayPerformance(txs, '2026-02-19', { USD: 1 }, prices, null);
    const conImplicito = calculateAccountDayPerformance(txs, '2026-02-19', { USD: 1 }, prices, null, null, impliedPricesByDate(existing)['2026-02-19']);
    expect(conHistorico.USD.totalValue).toBeCloseTo(8.93, 2);
    expect(conImplicito.USD.totalValue).toBeCloseTo(223.21, 2);
  });
});

// --- backfillUserPerformance --------------------------------------------------
const UID = 'u1';
const DAYS = ['2026-03-02', '2026-03-03', '2026-03-04', '2026-03-05', '2026-03-06'];

function seed({ withCop = false } = {}) {
  mockState.collections = {
    portfolioAccounts: [
      { id: 'ibkr', data: { userId: UID, isActive: true } },
      ...(withCop ? [{ id: 'trii', data: { userId: UID, isActive: true } }] : []),
    ],
    currencies: [{ id: 'COP', data: { code: 'COP', isActive: true } }],
    assets: [
      { id: 'L1', data: lot('L1', 'SPYG', 0, { isActive: false }) },
      { id: 'L2', data: lot('L2', 'SPYG', 3.6004, { acquisitionDate: '2025-01-28T14:39:45.000Z' }) },
      ...(withCop ? [{ id: 'E1', data: lot('E1', 'ECO', 10, { portfolioAccount: 'trii', currency: 'COP', unitValue: 2000, assetType: 'stock' }) }] : []),
    ],
    transactions: [
      { id: 'b2', data: tx('b2', 'buy', 'L2', 4, '2025-01-28T14:39:45.000Z') },
      { id: 's2', data: tx('s2', 'sell', 'L1', 0.1987, '2026-03-04T15:29:33.000Z', { price: 103 }) },
      { id: 's3', data: tx('s3', 'sell', 'L2', 0.3996, '2026-03-04T15:29:33.000Z', { price: 103 }) },
      ...(withCop ? [{ id: 'be', data: tx('be', 'buy', 'E1', 10, '2024-11-01T14:29:28.000Z', { assetName: 'ECO', assetType: 'stock', currency: 'COP', price: 2000, portfolioAccountId: 'trii' }) }] : []),
    ],
    // Un doc viejo de la cuenta con una llave que ya no existe (MC.PA en XTB)
    [`portfolioPerformance/${UID}/accounts/ibkr/dates`]: [
      { id: '2026-03-03', data: { date: '2026-03-03', USD: { totalValue: 1, assetPerformance: { 'MC.PA_stock': { units: 0.0867 }, SPYG_etf: { units: 4 } } } } },
    ],
  };
  Object.assign(mockPrices, {
    SPYG: { '2026-02-27': 100, '2026-03-02': 101, '2026-03-03': 102, '2026-03-04': 103, '2026-03-05': 104, '2026-03-06': 105 },
    ECO: { '2026-02-27': 2000, '2026-03-06': 2000 },
  });
}

beforeEach(() => {
  mockState.writes = [];
  mockState.snapshotsSaved = [];
  mockHttpStatus.code = 200;
  clearHistoricalPriceCache();
  mockRates.mockResolvedValue(Object.fromEntries(['2026-02-27', ...DAYS].map((d) => [d, { COP: 4000 }])));
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

describe('backfillUserPerformance', () => {
  test('prueba en seco: calcula todo, no escribe nada y las unidades son las de los lotes', async () => {
    seed();
    const result = await backfillUserPerformance(UID, DAYS, { dryRun: true });

    expect(result.success).toBe(true);
    expect(mockState.writes).toEqual([]);
    expect(mockState.snapshotsSaved).toEqual([]);
    expect(result.plan.map((d) => d.date)).toEqual(DAYS);
    expect(result.corrections).toEqual([expect.objectContaining({ assetId: 'L1' })]);

    const units = (date) => result.plan.find((d) => d.date === date).accounts.ibkr.USD.assetPerformance.SPYG_etf.units;
    expect(units('2026-03-03')).toBeCloseTo(4.1987, 8);
    // El último día cuadra con los lotes: el EOD siguiente no verá salto
    expect(units('2026-03-06')).toBeCloseTo(3.6004, 8);

    // El día de las ventas rinde lo que el precio (102 → 103), no un salto
    const sellDay = result.plan.find((d) => d.date === '2026-03-04').accounts.ibkr.USD;
    expect(sellDay.adjustedDailyChangePercentage).toBeCloseTo((103 / 102 - 1) * 100, 6);
  });

  test('con el API caído (401) falla sin escribir ni un día', async () => {
    seed();
    mockHttpStatus.code = 401;
    const result = await backfillUserPerformance(UID, DAYS);

    expect(result.success).toBe(false);
    expect(result.errors[0]).toMatch(/HTTP 401/);
    expect(result.daysProcessed).toBe(0);
    expect(mockState.writes).toEqual([]);
  });

  test('un activo sin tasa ese día hace fallar la corrida entera, sin escrituras', async () => {
    seed({ withCop: true });
    mockRates.mockResolvedValue({});
    const result = await backfillUserPerformance(UID, DAYS);

    expect(result.success).toBe(false);
    expect(result.errors.join(' ')).toMatch(/sin tasa de COP/);
    expect(mockState.writes).toEqual([]);
  });

  test('si todo cuadra escribe todos los días y borra las llaves viejas de las cuentas', async () => {
    seed();
    const result = await backfillUserPerformance(UID, DAYS);

    expect(result.success).toBe(true);
    expect(result.daysProcessed).toBe(DAYS.length);
    const accountDoc = mockState.writes.find((w) => w.path === `portfolioPerformance/${UID}/accounts/ibkr/dates/2026-03-03`);
    expect(accountDoc.opts).toEqual({ merge: true });
    expect(accountDoc.data.USD.assetPerformance['MC.PA_stock']).toBe('__delete__');
    expect(mockState.writes.filter((w) => w.path.startsWith(`portfolioPerformance/${UID}/dates/`))).toHaveLength(DAYS.length);
  });

  test('los docs del rango que el recálculo no produce se borran (un sábado de un proceso viejo)', async () => {
    seed();
    mockPrices.SPYG['2026-03-09'] = 106;
    const saturday = { date: '2026-03-07', USD: { totalValue: 500, adjustedDailyChangePercentage: 7.7 } };
    mockState.collections[`portfolioPerformance/${UID}/dates`] = [{ id: '2026-03-07', data: saturday }];
    mockState.collections[`portfolioPerformance/${UID}/accounts/ibkr/dates`].push({ id: '2026-03-07', data: saturday });
    const days = ['2026-03-05', '2026-03-06', '2026-03-09'];

    const dry = await backfillUserPerformance(UID, days, { dryRun: true });
    expect(dry.staleDocs).toEqual(expect.arrayContaining([
      { scope: 'overall', date: '2026-03-07' }, { scope: 'ibkr', date: '2026-03-07' },
    ]));
    expect(mockState.writes).toEqual([]);

    const result = await backfillUserPerformance(UID, days);
    expect(result.success).toBe(true);
    const deleted = mockState.writes.filter((w) => w.deleted).map((w) => w.path).sort();
    expect(deleted).toEqual([
      `portfolioPerformance/${UID}/accounts/ibkr/dates/2026-03-07`,
      `portfolioPerformance/${UID}/dates/2026-03-07`,
    ]);
  });
});
