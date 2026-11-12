/**
 * FLOW-WINDOW en el cierre nocturno: cuando falta el doc de un día, el flujo
 * del día siguiente es todo lo movido desde el último doc que existe.
 *
 * Caso real (DDeR8P, IBKR): no hay doc del 2026-07-22 y ese día se vendieron
 * ~$218. El doc del 07-23 se calculó contra el del 07-21 contando sólo los
 * flujos del 07-23, así que el dinero de las ventas salió como pérdida:
 * −4,61 % con el mercado en −2,23 %.
 *
 * Usa el cálculo real (`calculateAccountPerformance`); sólo se sustituyen la
 * persistencia y los snapshots.
 */

jest.mock('firebase-functions/v2/scheduler', () => ({ onSchedule: jest.fn((opts, handler) => handler) }));
jest.mock('firebase-functions/params', () => ({ defineSecret: jest.fn(() => ({ value: () => 'mock-secret' })) }));
jest.mock('firebase-admin', () => ({ firestore: jest.fn(() => ({})), initializeApp: jest.fn() }));
jest.mock('../firebaseAdmin', () => ({ firestore: jest.fn(() => ({})) }));
jest.mock('firebase-admin/functions', () => ({ getFunctions: jest.fn() }));
jest.mock('firebase-admin/firestore', () => ({
  FieldPath: { documentId: () => '__name__' },
  FieldValue: { increment: (n) => ({ __increment: n }), delete: () => '__delete__' },
  getFirestore: jest.fn(() => ({})),
}));
jest.mock('../../utils/performanceStaleMarker', () => ({ markPerformanceAsStale: jest.fn().mockResolvedValue(undefined) }));
jest.mock('../../utils/logger', () => ({
  StructuredLogger: { forScheduled: () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), startOperation: () => ({ success: jest.fn(), failure: jest.fn() }) }) },
  crearLoggerDeConsola: () => ({ debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }),
}));
jest.mock('../snapshotGenerator', () => ({
  fetchAllDailyDocs: jest.fn().mockResolvedValue([]),
  generatePerformanceSnapshot: jest.fn().mockResolvedValue(undefined),
  generateAssetSnapshot: jest.fn().mockResolvedValue(undefined),
  buildSnapshotDocId: jest.fn((...parts) => parts.filter(Boolean).join('_')),
}));
jest.mock('../snapshotIncrementalService', () => ({
  updateSnapshotIncremental: jest.fn().mockResolvedValue({ updated: true, method: 'incremental' }),
  updateAssetSnapshotIncremental: jest.fn().mockResolvedValue({ updated: true, method: 'incremental' }),
}));
jest.mock('../calculatePortfolioRisk', () => ({ calculatePortfolioRisk: jest.fn() }));
jest.mock('../cacheInvalidationService', () => ({ invalidatePerformanceCacheBatch: jest.fn(), invalidatePerformanceCache: jest.fn() }));
jest.mock('../benchmarkSnapshotService', () => ({ updateBenchmarkSnapshots: jest.fn() }));
jest.mock('../marketDataScheduled', () => ({ saveIndicesHistoryDataInternal: jest.fn() }));
jest.mock('../indexHistoryService', () => ({ refreshIndexCacheInternal: jest.fn() }));
jest.mock('../marketDataHelper', () => ({ getPricesFromApi: jest.fn(), getCurrencyRatesFromApi: jest.fn() }));
jest.mock('../config', () => ({ FINANCE_QUERY_API_URL: 'https://api.test' }));
jest.mock('../eodProgressService', () => ({}));
jest.mock('../eodMarketSnapshot', () => ({ leerSnapshotDelDia: jest.fn(), guardarSnapshotDelDia: jest.fn() }));

const { processUserPerformance, PerformanceDataCache } = require('../unifiedMarketDataUpdate');
const { markPerformanceAsStale } = require('../../utils/performanceStaleMarker');

/** Firestore mínimo: guarda lo que el batch escribe, por ruta. */
function crearDb() {
  const escritos = {};
  const ref = (path) => ({
    path,
    collection: (name) => ({ doc: (id) => ref(`${path}/${name}/${id}`) }),
    get: async () => ({ exists: false, data: () => undefined }),
    set: jest.fn().mockResolvedValue(undefined),
  });
  return {
    collection: (name) => ({ doc: (id) => ref(`${name}/${id}`) }),
    batch: () => ({
      set: (r, data) => { escritos[r.path] = data; },
      commit: async () => undefined,
    }),
    escritos,
  };
}

const USD = [{ code: 'USD', exchangeRate: 1 }];
const lote = (id, name, units, overrides = {}) => ({
  id, name, assetType: 'stock', portfolioAccount: 'ibkr', isActive: units > 0,
  units, unitValue: 90, currency: 'USD', acquisitionDate: '2026-01-10T15:00:00.000Z', ...overrides,
});
const venta = (id, assetId, amount, price, date) => ({
  id, type: 'sell', assetId, assetName: assetId, assetType: 'stock', portfolioAccountId: 'ibkr',
  amount, price, currency: 'USD', date, dollarPriceToDate: 1, defaultCurrencyForAdquisitionDollar: 'USD',
});

/** Doc del 07-21: 10 A a 100 y 5 B a 100. */
const DOC_0721 = {
  date: '2026-07-21',
  USD: {
    totalValue: 1500,
    assetPerformance: {
      A_stock: { totalValue: 1000, units: 10 },
      B_stock: { totalValue: 500, units: 5 },
    },
  },
};

function cacheCon(lastDoc) {
  const cache = new PerformanceDataCache();
  if (lastDoc) {
    cache.userLastPerformance.set('u1', lastDoc);
    cache.accountLastPerformance.set('ibkr', lastDoc);
  }
  return cache;
}

/**
 * 07-22 (sin doc): se venden las 5 B a 102. 07-23: A cierra en 99.
 * Mercado: A −1 % con peso 2/3, B +2 % con peso 1/3 → 0 %.
 */
function escenario({ lastDoc = DOC_0721, transacciones } = {}) {
  const a = lote('A', 'A', 10);
  const b = lote('B', 'B', 0); // vendido del todo: inactivo
  const todas = transacciones || [venta('s1', 'B', 5, 102, '2026-07-22T14:47:36.000Z')];
  const db = crearDb();
  return {
    db,
    run: () => processUserPerformance({
      db,
      userId: 'u1',
      accounts: [{ id: 'ibkr', userId: 'u1' }],
      currentPrices: [{ symbol: 'A', price: 99, currency: 'USD' }, { symbol: 'B', price: 101, currency: 'USD' }],
      currencies: USD,
      cache: cacheCon(lastDoc),
      allAssets: [a, b],
      assetsToInclude: [a, b],
      sellTransactions: todas.filter((t) => t.type === 'sell'),
      sellTransactionsByAccount: {},
      inactiveAssets: [b],
      activeAssets: [a],
      todaysTransactions: todas,
      formattedDate: '2026-07-23',
    }),
  };
}

beforeEach(() => jest.clearAllMocks());

describe('FLOW-WINDOW en processUserPerformance', () => {
  test('las ventas del día sin doc entran como flujo: el rendimiento es el del mercado', async () => {
    const { db, run } = escenario();
    const result = await run();
    expect(result.success).toBe(true);

    const overall = db.escritos['portfolioPerformance/u1/dates/2026-07-23'].USD;
    const cuenta = db.escritos['portfolioPerformance/u1/accounts/ibkr/dates/2026-07-23'].USD;
    // (990 − 1500 + 510) / 1500 = 0
    expect(overall.adjustedDailyChangePercentage).toBeCloseTo(0, 6);
    expect(cuenta.adjustedDailyChangePercentage).toBeCloseTo(0, 6);
    expect(overall.totalCashFlow).toBeCloseTo(510, 6);
    // El vendido aparece con 0 unidades y su flujo, como cualquier venta total
    expect(cuenta.assetPerformance.B_stock).toMatchObject({ units: 0, totalCashFlow: 510 });
  });

  test('sin la ventana la venta habría contado como pérdida (lo que pasaba)', async () => {
    // Mismo día, pero con un doc del 07-22 que ya incluyera la venta: entonces
    // la venta NO es flujo del 07-23 y B no debe aparecer.
    const doc0722 = { date: '2026-07-22', USD: { totalValue: 1000, assetPerformance: { A_stock: { totalValue: 1000, units: 10 } } } };
    const { db, run } = escenario({ lastDoc: doc0722 });
    await run();
    const overall = db.escritos['portfolioPerformance/u1/dates/2026-07-23'].USD;
    expect(overall.totalCashFlow).toBe(0);
    expect(overall.adjustedDailyChangePercentage).toBeCloseTo(-1, 6);
    expect(overall.assetPerformance.B_stock).toBeUndefined();
  });

  test('lo fechado el día del último doc no se vuelve a contar', async () => {
    const { db, run } = escenario({
      transacciones: [venta('s0', 'B', 5, 102, '2026-07-21T19:00:00.000Z')],
    });
    await run();
    const overall = db.escritos['portfolioPerformance/u1/dates/2026-07-23'].USD;
    expect(overall.totalCashFlow).toBe(0);
  });

  test('un hueco mayor que el tope marca al usuario para reconciliar', async () => {
    const viejo = { ...DOC_0721, date: '2026-06-01' };
    const { run } = escenario({ lastDoc: viejo });
    await run();
    expect(markPerformanceAsStale).toHaveBeenCalledWith('u1', '2026-06-01', expect.objectContaining({ reason: 'flow_window_capped' }));
  });

  test('con el doc de ayer presente no marca nada', async () => {
    const doc0722 = { date: '2026-07-22', USD: { totalValue: 1000, assetPerformance: { A_stock: { totalValue: 1000, units: 10 } } } };
    const { run } = escenario({ lastDoc: doc0722, transacciones: [] });
    await run();
    expect(markPerformanceAsStale).not.toHaveBeenCalled();
  });
});
