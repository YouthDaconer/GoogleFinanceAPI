/**
 * HU 6.3 — El cierre actual y el cierre repartido dan las mismas cifras.
 *
 * **Por qué este archivo existe.** Hasta ahora `calculateDailyPortfolioPerformance`
 * no tenía ni un test que la invocara: los tests de «procesamiento paralelo»
 * de `unifiedMarketDataUpdate.test.js` **reconstruyen a mano** el `p-limit` +
 * `Promise.allSettled` y prueban su propia copia, no el código real. Apoyar el
 * reparto del cierre nocturno en esa cobertura sería apoyarlo en nada (D8).
 *
 * Defiende:
 * - **Escenario 7 / RN-6-A**: las cifras del cierre repartido son idénticas al
 *   céntimo a las del cierre actual. Se comprueba ejecutando los dos caminos
 *   sobre los mismos datos y comparando **lo que cada uno escribe**.
 * - **Escenario 1**: queda constancia de cuántos se procesaron.
 * - **Escenario 3**: un inversor que falla no arrastra a los demás.
 * - **T2**: el día que se cierra llega como parámetro y se puede relanzar.
 *
 * @see platform-docs/stories/6.3-cierre-nocturno-termina-siempre/refinamiento.md (T2, T9, D8)
 */

// === Mocks ===

jest.mock('firebase-functions/v2/scheduler', () => ({ onSchedule: jest.fn((opts, handler) => handler) }));
jest.mock('firebase-functions/v2/tasks', () => ({ onTaskDispatched: jest.fn((opts, handler) => handler) }));
jest.mock('firebase-functions/params', () => ({ defineSecret: jest.fn(() => ({ value: () => 'mock-secret' })) }));

jest.mock('firebase-admin', () => ({ firestore: jest.fn(() => ({})), initializeApp: jest.fn() }));
jest.mock('../firebaseAdmin', () => ({ firestore: jest.fn(() => ({})) }));
jest.mock('firebase-admin/functions', () => ({ getFunctions: jest.fn() }));
jest.mock('firebase-admin/firestore', () => ({
  FieldPath: { documentId: () => '__name__' },
  FieldValue: { increment: (n) => ({ __increment: n }), delete: () => '__delete__' },
  getFirestore: jest.fn(() => ({})),
}));

/**
 * El cálculo real es puro respecto de sus entradas; lo que este archivo
 * defiende es que **las entradas y las escrituras** sean las mismas por los dos
 * caminos. Se sustituye por una función igual de determinista pero legible, de
 * modo que una diferencia de un céntimo entre caminos salga como diferencia de
 * valor y no se esconda detrás de la aritmética real.
 */
const mockCalculoDeterminista = (assets, currentPrices, currencies) => {
  const resultado = {};
  for (const currency of currencies) {
    const tasa = currency.exchangeRate || 1;
    let totalValue = 0;
    const assetPerformance = {};
    for (const asset of assets) {
      const precio = (currentPrices.find((p) => p.symbol === asset.name) || {}).price || 0;
      const valor = asset.units * precio * tasa;
      totalValue += valor;
      assetPerformance[`${asset.name}_${asset.assetType}`] = {
        totalValue: valor,
        totalInvestment: asset.units * asset.unitValue * tasa,
        units: asset.units,
      };
    }
    resultado[currency.code] = {
      totalValue,
      totalInvestment: assets.reduce((s, a) => s + a.units * a.unitValue * tasa, 0),
      adjustedDailyChangePercentage: 1.5,
      dailyChangePercentage: 1.5,
      totalCashFlow: 0,
      assetPerformance,
    };
  }
  return resultado;
};

jest.mock('../../utils/portfolioCalculations', () => ({
  calculateAccountPerformance: jest.fn(),
  convertCurrency: jest.fn(),
  detectUnexplainedUnitChanges: jest.fn(),
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
  buildSnapshotDocId: jest.fn((userId, accountId, currency, ticker, assetType) =>
    [userId, accountId, currency, ticker, assetType].filter(Boolean).join('_')),
}));
jest.mock('../snapshotIncrementalService', () => ({
  updateSnapshotIncremental: jest.fn().mockResolvedValue({ updated: true, method: 'incremental' }),
  updateAssetSnapshotIncremental: jest.fn().mockResolvedValue({ updated: true, method: 'incremental' }),
}));
jest.mock('../calculatePortfolioRisk', () => ({ calculatePortfolioRisk: jest.fn().mockResolvedValue(undefined) }));
jest.mock('../cacheInvalidationService', () => ({
  invalidatePerformanceCacheBatch: jest.fn().mockResolvedValue({ usersProcessed: 0, cachesDeleted: 0 }),
  invalidatePerformanceCache: jest.fn().mockResolvedValue(undefined),
}));
jest.mock('../benchmarkSnapshotService', () => ({ updateBenchmarkSnapshots: jest.fn().mockResolvedValue({}) }));
jest.mock('../marketDataScheduled', () => ({ saveIndicesHistoryDataInternal: jest.fn().mockResolvedValue({ count: 0 }) }));
jest.mock('../indexHistoryService', () => ({ refreshIndexCacheInternal: jest.fn().mockResolvedValue({ refreshed: 0 }) }));
jest.mock('../marketDataHelper', () => ({
  getPricesFromApi: jest.fn().mockResolvedValue([]),
  getCurrencyRatesFromApi: jest.fn().mockResolvedValue([]),
}));
jest.mock('../config', () => ({ FINANCE_QUERY_API_URL: 'https://api.test' }));
jest.mock('../eodProgressService', () => ({
  yaProcesado: jest.fn().mockResolvedValue(false),
  marcarCompletado: jest.fn().mockResolvedValue({ contado: true }),
  marcarFallido: jest.fn().mockResolvedValue({ contado: true }),
  iniciarProgreso: jest.fn().mockResolvedValue(undefined),
}));
jest.mock('../eodMarketSnapshot', () => ({
  leerSnapshotDelDia: jest.fn(),
  guardarSnapshotDelDia: jest.fn().mockResolvedValue({ creado: true }),
}));
jest.mock('../backfillCoreModule', () => ({
  chunkArray: (array, size) => {
    const chunks = [];
    for (let i = 0; i < array.length; i += size) chunks.push(array.slice(i, i + size));
    return chunks;
  },
}));

const { calculateAccountPerformance, convertCurrency, detectUnexplainedUnitChanges } = require('../../utils/portfolioCalculations');
const { calculateDailyPortfolioPerformance } = require('../unifiedMarketDataUpdate');
const { leerSnapshotDelDia } = require('../eodMarketSnapshot');
const { procesarInversor } = require('../eodUserPerformanceTask');

// ============================================================================
// Fábricas locales
// ============================================================================

const cuenta = (id, userId, overrides = {}) => ({ id, userId, name: `Cuenta ${id}`, isActive: true, ...overrides });

const activo = (id, portfolioAccount, overrides = {}) => ({
  id, name: 'AAPL', assetType: 'stock', portfolioAccount, isActive: true,
  units: 10, unitValue: 100, currency: 'USD', ...overrides,
});

const cotizacion = (symbol, price) => ({ symbol, price, regularMarketPrice: price, currency: 'USD' });

const divisa = (code, exchangeRate) => ({ id: code, code, exchangeRate, isActive: true, name: code, symbol: code });

/**
 * Firestore en memoria que **recuerda cada escritura con su ruta**. Es lo que
 * permite comparar los dos caminos: no se comparan llamadas, se comparan los
 * documentos que cada uno dejó.
 */
const crearDb = ({ cuentas = [], activos = [], inactivos = [], movimientos = [] } = {}) => {
  const escrito = new Map();

  const resultado = (items) => ({
    docs: items.map((d) => ({
      id: d.id,
      data: () => d,
      ref: { id: d.id, delete: () => {} },
    })),
    size: items.length,
    empty: items.length === 0,
  });

  const consulta = (coleccion, filtros) => ({
    where: (campo, _op, valor) => consulta(coleccion, [...filtros, [campo, valor]]),
    orderBy: () => consulta(coleccion, filtros),
    limit: () => consulta(coleccion, filtros),
    get: async () => {
      if (coleccion === 'portfolioAccounts') {
        const porUsuario = filtros.find(([c]) => c === 'userId');
        return resultado(porUsuario ? cuentas.filter((c) => c.userId === porUsuario[1]) : cuentas);
      }
      if (coleccion === 'transactions') {
        const porCuentas = filtros.find(([c]) => c === 'portfolioAccountId');
        if (porCuentas) return resultado(movimientos.filter((m) => porCuentas[1].includes(m.portfolioAccountId)));
        const porTipo = filtros.find(([c]) => c === 'type');
        if (porTipo) return resultado(movimientos.filter((m) => m.type === porTipo[1]));
        return resultado(movimientos);
      }
      if (coleccion === 'assets') {
        const porActivo = filtros.find(([c]) => c === 'isActive');
        const base = porActivo && porActivo[1] === false ? inactivos : activos;
        const porCuentas = filtros.find(([c]) => c === 'portfolioAccount');
        return resultado(porCuentas ? base.filter((a) => porCuentas[1].includes(a.portfolioAccount)) : base);
      }
      return resultado([]);
    },
  });

  const crearDoc = (ruta) => ({
    ruta,
    id: ruta.split('/').pop(),
    set: async (data, opciones = {}) => {
      escrito.set(ruta, opciones.merge ? { ...(escrito.get(ruta) || {}), ...data } : data);
    },
    get: async () => ({ exists: escrito.has(ruta), data: () => escrito.get(ruta) }),
    collection: (nombre) => ({
      ...consulta(nombre, []),
      doc: (id) => crearDoc(`${ruta}/${nombre}/${id}`),
    }),
  });

  return {
    collection: (nombre) => ({ ...consulta(nombre, []), doc: (id) => crearDoc(`${nombre}/${id}`) }),
    batch: () => {
      const pendientes = [];
      return {
        set: (ref, data, opciones = {}) => pendientes.push([ref.ruta, data, opciones]),
        delete: () => {},
        commit: async () => {
          for (const [ruta, data, opciones] of pendientes) {
            escrito.set(ruta, opciones.merge ? { ...(escrito.get(ruta) || {}), ...data } : data);
          }
        },
      };
    },
    _escrito: escrito,
  };
};

const PRECIOS = [cotizacion('AAPL', 231.47), cotizacion('MSFT', 412.09)];
const DIVISAS = [divisa('USD', 1), divisa('COP', 4210.55)];
const FECHA = '2026-10-20';

/** Los documentos de rendimiento que quedaron escritos, sin ruido. */
const docsDeRendimiento = (db) =>
  [...db._escrito.entries()]
    .filter(([ruta]) => ruta.startsWith('portfolioPerformance/') && ruta.includes('/dates/'))
    .sort(([a], [b]) => a.localeCompare(b));

beforeEach(() => {
  jest.clearAllMocks();
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
  // `clearMocks` limpia las llamadas pero NO las implementaciones: sin reponerlas,
  // el test del inversor que revienta dejaría su excepción puesta para los de después.
  calculateAccountPerformance.mockImplementation(mockCalculoDeterminista);
  convertCurrency.mockImplementation((amount) => amount);
  detectUnexplainedUnitChanges.mockImplementation(() => []);
  leerSnapshotDelDia.mockResolvedValue({ prices: PRECIOS, currencies: DIVISAS, createdAt: 'x', sourceRunId: 'run-1' });
});

afterEach(() => jest.restoreAllMocks());

// ============================================================================

describe('calculateDailyPortfolioPerformance — el cierre actual, invocado de verdad (HU 6.3, D8)', () => {
  it('procesa a todos los inversores y deja constancia de cuántos (Escenario 1)', async () => {
    const db = crearDb({
      cuentas: [cuenta('acc-1', 'u1'), cuenta('acc-2', 'u2')],
      activos: [activo('asset-1', 'acc-1'), activo('asset-2', 'acc-2', { name: 'MSFT' })],
    });

    const resultado = await calculateDailyPortfolioPerformance(db, PRECIOS, DIVISAS, { formattedDate: FECHA });

    expect(resultado.count).toBe(2);
    expect(resultado.failedCount).toBe(0);
    expect(resultado.userIds.sort()).toEqual(['u1', 'u2']);
  });

  it('el día que se cierra llega como parámetro, no se deduce de la hora (T2)', async () => {
    const db = crearDb({ cuentas: [cuenta('acc-1', 'u1')], activos: [activo('asset-1', 'acc-1')] });

    await calculateDailyPortfolioPerformance(db, PRECIOS, DIVISAS, { formattedDate: '2026-03-05' });

    expect(db._escrito.has('portfolioPerformance/u1/dates/2026-03-05')).toBe(true);
    expect(db._escrito.get('portfolioPerformance/u1/dates/2026-03-05').date).toBe('2026-03-05');
  });

  it('escribe el día del inversor y el de cada una de sus cuentas', async () => {
    const db = crearDb({
      cuentas: [cuenta('acc-1', 'u1'), cuenta('acc-2', 'u1')],
      activos: [activo('asset-1', 'acc-1'), activo('asset-2', 'acc-2', { name: 'MSFT' })],
    });

    await calculateDailyPortfolioPerformance(db, PRECIOS, DIVISAS, { formattedDate: FECHA });

    expect(db._escrito.has(`portfolioPerformance/u1/dates/${FECHA}`)).toBe(true);
    expect(db._escrito.has(`portfolioPerformance/u1/accounts/acc-1/dates/${FECHA}`)).toBe(true);
    expect(db._escrito.has(`portfolioPerformance/u1/accounts/acc-2/dates/${FECHA}`)).toBe(true);
  });

  it('un inversor que falla queda marcado _stale y no arrastra al resto (Escenario 3)', async () => {
    calculateAccountPerformance.mockImplementation((assets, ...resto) => {
      if (assets.some((a) => a.portfolioAccount === 'acc-1')) throw new Error('el cálculo de u1 reventó');
      return mockCalculoDeterminista(assets, ...resto);
    });

    const db = crearDb({
      cuentas: [cuenta('acc-1', 'u1'), cuenta('acc-2', 'u2')],
      activos: [activo('asset-1', 'acc-1'), activo('asset-2', 'acc-2', { name: 'MSFT' })],
    });

    const resultado = await calculateDailyPortfolioPerformance(db, PRECIOS, DIVISAS, { formattedDate: FECHA });

    expect(resultado.failedUserIds).toEqual(['u1']);
    expect(resultado.userIds).toEqual(['u2']);
    expect(db._escrito.get('portfolioPerformance/u1')._stale.since).toBe(FECHA);
    expect(db._escrito.has(`portfolioPerformance/u2/dates/${FECHA}`)).toBe(true);
  });
});

describe('Las cifras del cierre repartido son idénticas al céntimo (HU 6.3, RN-6-A, Escenario 7)', () => {
  const escenario = {
    cuentas: [cuenta('acc-1', 'u1'), cuenta('acc-2', 'u1')],
    activos: [
      activo('asset-1', 'acc-1', { units: 13, unitValue: 97.33 }),
      activo('asset-2', 'acc-2', { name: 'MSFT', units: 7, unitValue: 301.11 }),
    ],
    movimientos: [],
  };

  it('el mismo día calculado por los dos caminos escribe exactamente los mismos documentos', async () => {
    const dbActual = crearDb(escenario);
    await calculateDailyPortfolioPerformance(dbActual, PRECIOS, DIVISAS, { formattedDate: FECHA });

    const dbRepartido = crearDb(escenario);
    await procesarInversor({ db: dbRepartido, userId: 'u1', date: FECHA });

    expect(docsDeRendimiento(dbRepartido)).toEqual(docsDeRendimiento(dbActual));
  });

  it('las cifras de cada divisa coinciden hasta el último decimal', async () => {
    const dbActual = crearDb(escenario);
    await calculateDailyPortfolioPerformance(dbActual, PRECIOS, DIVISAS, { formattedDate: FECHA });

    const dbRepartido = crearDb(escenario);
    await procesarInversor({ db: dbRepartido, userId: 'u1', date: FECHA });

    const actual = dbActual._escrito.get(`portfolioPerformance/u1/dates/${FECHA}`);
    const repartido = dbRepartido._escrito.get(`portfolioPerformance/u1/dates/${FECHA}`);

    for (const codigo of ['USD', 'COP']) {
      expect(repartido[codigo].totalValue).toBe(actual[codigo].totalValue);
      expect(repartido[codigo].totalInvestment).toBe(actual[codigo].totalInvestment);
      expect(repartido[codigo].unrealizedProfitAndLoss).toBe(actual[codigo].unrealizedProfitAndLoss);
      expect(repartido[codigo].assetPerformance).toEqual(actual[codigo].assetPerformance);
    }
  });

  it('también coinciden los documentos por cuenta, no sólo el total del inversor', async () => {
    const dbActual = crearDb(escenario);
    await calculateDailyPortfolioPerformance(dbActual, PRECIOS, DIVISAS, { formattedDate: FECHA });

    const dbRepartido = crearDb(escenario);
    await procesarInversor({ db: dbRepartido, userId: 'u1', date: FECHA });

    for (const accountId of ['acc-1', 'acc-2']) {
      const ruta = `portfolioPerformance/u1/accounts/${accountId}/dates/${FECHA}`;
      expect(dbRepartido._escrito.get(ruta)).toEqual(dbActual._escrito.get(ruta));
    }
  });
});
