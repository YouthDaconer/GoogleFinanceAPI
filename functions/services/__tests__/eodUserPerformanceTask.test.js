/**
 * HU 6.3 — El cierre nocturno de un solo inversor.
 *
 * Defiende el Escenario 3 (un inversor que falla no arrastra a los demás),
 * el Escenario 5 (entero o nada), el Escenario 6 / RN-6.3-L (un reintento no
 * duplica ni altera) y el Escenario 8 / RN-6.3-M (todos usan los mismos
 * precios del día, aunque se reintente a otra hora).
 *
 * @see platform-docs/stories/6.3-cierre-nocturno-termina-siempre/refinamiento.md (T3, T8, T9)
 */

// === Mocks ===

jest.mock('../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({})),
}));

jest.mock('firebase-admin/firestore', () => ({
  FieldPath: { documentId: () => '__name__' },
  FieldValue: { increment: (n) => ({ __increment: n }) },
}));

jest.mock('firebase-functions/v2/tasks', () => ({
  onTaskDispatched: jest.fn((opts, handler) => handler),
}));

jest.mock('firebase-functions/params', () => ({
  defineSecret: jest.fn(() => ({ value: () => 'mock-secret' })),
}));

jest.mock('../eodMarketSnapshot', () => ({
  leerSnapshotDelDia: jest.fn(),
}));

jest.mock('../eodProgressService', () => ({
  yaProcesado: jest.fn().mockResolvedValue(false),
  marcarCompletado: jest.fn().mockResolvedValue({ contado: true }),
  marcarFallido: jest.fn().mockResolvedValue({ contado: true }),
}));

jest.mock('../unifiedMarketDataUpdate', () => ({
  processUserPerformance: jest.fn().mockResolvedValue({ userId: 'u1', success: true, durationMs: 42 }),
  PerformanceDataCache: jest.fn().mockImplementation(() => ({
    preloadHistoricalData: jest.fn().mockResolvedValue(undefined),
    validateDataConsistency: jest.fn(() => ({ isConsistent: true, gap: 0 })),
  })),
  markInconsistentUsersAsStale: jest.fn().mockResolvedValue(0),
  setModuleLogger: jest.fn(),
}));

jest.mock('../backfillCoreModule', () => ({
  chunkArray: (array, size) => {
    const chunks = [];
    for (let i = 0; i < array.length; i += size) chunks.push(array.slice(i, i + size));
    return chunks;
  },
}));

const { leerSnapshotDelDia } = require('../eodMarketSnapshot');
const progreso = require('../eodProgressService');
const { processUserPerformance } = require('../unifiedMarketDataUpdate');
const { procesarInversor, cargarDatosDelInversor } = require('../eodUserPerformanceTask');

// ============================================================================
// Fábricas locales
// ============================================================================

const cuenta = (id, userId = 'u1', overrides = {}) => ({ id, userId, isActive: true, ...overrides });

const activo = (id, portfolioAccount = 'acc-1', overrides = {}) => ({
  id, name: 'AAPL', assetType: 'stock', portfolioAccount, isActive: true,
  units: 10, unitValue: 100, currency: 'USD', ...overrides,
});

const movimiento = (id, overrides = {}) => ({
  id, type: 'buy', portfolioAccountId: 'acc-1', assetId: 'asset-1',
  amount: 1, price: 100, currency: 'USD', date: '2026-10-20T15:00:00.000Z', ...overrides,
});

const cotizacion = (symbol, price) => ({ symbol, price, regularMarketPrice: price, currency: 'USD' });

/** Firestore de mentira: devuelve por colección lo que el test le pone. */
const crearDb = ({ cuentas = [], activos = [], inactivos = [], movimientos = [], snapshots = [] } = {}) => {
  const borrados = [];

  const resultado = (docs) => ({
    docs: docs.map((d) => ({ id: d.id, data: () => d, ref: { id: d.id } })),
    size: docs.length,
    empty: docs.length === 0,
  });

  const query = (coleccion, filtros) => ({
    where: (campo, _op, valor) => query(coleccion, [...filtros, [campo, valor]]),
    get: async () => {
      if (coleccion === 'portfolioAccounts') return resultado(cuentas);
      if (coleccion === 'transactions') return resultado(movimientos);
      if (coleccion === 'performanceSnapshots') return resultado(snapshots);
      if (coleccion === 'assets') {
        const activoFiltro = filtros.find(([c]) => c === 'isActive');
        return resultado(activoFiltro && activoFiltro[1] === false ? inactivos : activos);
      }
      return resultado([]);
    },
  });

  return {
    collection: (nombre) => ({
      ...query(nombre, []),
      doc: (id) => ({ id, set: jest.fn().mockResolvedValue(undefined) }),
    }),
    batch: () => ({
      delete: (ref) => borrados.push(ref.id),
      commit: async () => undefined,
    }),
    _borrados: borrados,
  };
};

const PRECIOS_DEL_DIA = {
  prices: [cotizacion('AAPL', 231.47)],
  currencies: [{ id: 'USD', code: 'USD', exchangeRate: 1, isActive: true }],
  createdAt: '2026-10-21T00:05:00.000Z',
  sourceRunId: 'run-1',
};

beforeEach(() => {
  jest.clearAllMocks();
  leerSnapshotDelDia.mockResolvedValue(PRECIOS_DEL_DIA);
  progreso.yaProcesado.mockResolvedValue(false);
  progreso.marcarCompletado.mockResolvedValue({ contado: true });
  processUserPerformance.mockResolvedValue({ userId: 'u1', success: true, durationMs: 42 });
});

// ============================================================================

describe('eodUserPerformanceTask — los precios son los del día (HU 6.3, RN-6.3-M, Escenario 8)', () => {
  it('el cálculo recibe los precios del documento del día, no del mercado en vivo', async () => {
    const db = crearDb({ cuentas: [cuenta('acc-1')], activos: [activo('asset-1')] });

    await procesarInversor({ db, userId: 'u1', date: '2026-10-20' });

    expect(leerSnapshotDelDia).toHaveBeenCalledWith(db, '2026-10-20');
    const argumentos = processUserPerformance.mock.calls[0][0];
    expect(argumentos.currentPrices).toEqual(PRECIOS_DEL_DIA.prices);
    expect(argumentos.currencies).toEqual(PRECIOS_DEL_DIA.currencies);
    expect(argumentos.formattedDate).toBe('2026-10-20');
  });

  it('un reintento a otra hora usa exactamente los mismos precios', async () => {
    const db = crearDb({ cuentas: [cuenta('acc-1')], activos: [activo('asset-1')] });

    await procesarInversor({ db, userId: 'u1', date: '2026-10-20', intento: 0 });
    const primera = processUserPerformance.mock.calls[0][0].currentPrices;

    processUserPerformance.mockClear();
    progreso.yaProcesado.mockResolvedValue(false);
    await procesarInversor({ db, userId: 'u1', date: '2026-10-20', intento: 1 });
    const segunda = processUserPerformance.mock.calls[0][0].currentPrices;

    expect(segunda).toEqual(primera);
    expect(segunda[0].price).toBe(231.47);
  });

  it('sin precios del día la tarea falla en vez de calcular con otros', async () => {
    leerSnapshotDelDia.mockResolvedValue(null);
    const db = crearDb({ cuentas: [cuenta('acc-1')] });

    await expect(procesarInversor({ db, userId: 'u1', date: '2026-10-20' }))
      .rejects.toThrow('No hay snapshot de mercado materializado para 2026-10-20');
    expect(processUserPerformance).not.toHaveBeenCalled();
  });
});

describe('eodUserPerformanceTask — un reintento no duplica (HU 6.3, RN-6.3-L, Escenario 6)', () => {
  it('si el inversor ya estaba procesado, no se vuelve a calcular nada', async () => {
    progreso.yaProcesado.mockResolvedValue(true);
    const db = crearDb({ cuentas: [cuenta('acc-1')], activos: [activo('asset-1')] });

    const resultado = await procesarInversor({ db, userId: 'u1', date: '2026-10-20', intento: 1 });

    expect(resultado.estado).toBe('omitido-ya-procesado');
    expect(processUserPerformance).not.toHaveBeenCalled();
    expect(progreso.marcarCompletado).not.toHaveBeenCalled();
  });

  it('el primer intento no borra snapshots: no hay nada a medias que rehacer', async () => {
    const db = crearDb({
      cuentas: [cuenta('acc-1')],
      activos: [activo('asset-1')],
      snapshots: [{ id: 'u1_overall_USD' }],
    });

    await procesarInversor({ db, userId: 'u1', date: '2026-10-20', intento: 0 });

    expect(db._borrados).toEqual([]);
  });

  it('un reintento borra los snapshots del inversor para que se rehagan y no se salten', async () => {
    const db = crearDb({
      cuentas: [cuenta('acc-1')],
      activos: [activo('asset-1')],
      snapshots: [{ id: 'u1_overall_USD' }, { id: 'u1_acc-1_USD' }],
    });

    await procesarInversor({ db, userId: 'u1', date: '2026-10-20', intento: 1 });

    expect(db._borrados).toEqual(['u1_overall_USD', 'u1_acc-1_USD']);
  });
});

describe('eodUserPerformanceTask — entero o nada (HU 6.3, RN-6.3-K, Escenario 5)', () => {
  it('el inversor se da por terminado sólo DESPUÉS de que su cálculo completo haya ido bien', async () => {
    const orden = [];
    processUserPerformance.mockImplementation(async () => {
      orden.push('calculo');
      return { userId: 'u1', success: true, durationMs: 42 };
    });
    progreso.marcarCompletado.mockImplementation(async () => {
      orden.push('marca');
      return { contado: true };
    });
    const db = crearDb({ cuentas: [cuenta('acc-1')], activos: [activo('asset-1')] });

    await procesarInversor({ db, userId: 'u1', date: '2026-10-20' });

    expect(orden).toEqual(['calculo', 'marca']);
  });

  it('si el cálculo falla, el inversor NO queda marcado como terminado', async () => {
    processUserPerformance.mockResolvedValue({ userId: 'u1', success: false, error: 'Firestore DEADLINE_EXCEEDED' });
    const db = crearDb({ cuentas: [cuenta('acc-1')], activos: [activo('asset-1')] });

    await expect(procesarInversor({ db, userId: 'u1', date: '2026-10-20' }))
      .rejects.toThrow('Firestore DEADLINE_EXCEEDED');
    expect(progreso.marcarCompletado).not.toHaveBeenCalled();
  });
});

describe('eodUserPerformanceTask — un inversor que falla no arrastra (HU 6.3, Escenario 3)', () => {
  it('el fallo se propaga para que la cola lo reintente, sin tocar a nadie más', async () => {
    processUserPerformance.mockRejectedValue(new Error('se cayó la red'));
    const db = crearDb({ cuentas: [cuenta('acc-1')], activos: [activo('asset-1')] });

    await expect(procesarInversor({ db, userId: 'u1', date: '2026-10-20' }))
      .rejects.toThrow('se cayó la red');
  });

  it('un inversor sin cuentas activas se da por hecho en vez de quedar colgado', async () => {
    const db = crearDb({ cuentas: [] });

    const resultado = await procesarInversor({ db, userId: 'u1', date: '2026-10-20' });

    expect(resultado.estado).toBe('omitido-sin-cuentas');
    expect(progreso.marcarCompletado).toHaveBeenCalledWith(db, '2026-10-20', 'u1', { intento: 0 });
  });
});

describe('eodUserPerformanceTask — la carga es sólo de su inversor (HU 6.3, T3)', () => {
  it('reúne cuentas, activos y movimientos del inversor con la forma que espera el cálculo', async () => {
    const db = crearDb({
      cuentas: [cuenta('acc-1'), cuenta('acc-2')],
      activos: [activo('asset-1', 'acc-1'), activo('asset-2', 'acc-2')],
      movimientos: [movimiento('tx-1'), movimiento('tx-2', { type: 'sell', assetId: 'asset-1' })],
    });

    const datos = await cargarDatosDelInversor(db, 'u1', '2026-10-20');

    expect(datos.accounts).toHaveLength(2);
    expect(datos.accountIds).toEqual(['acc-1', 'acc-2']);
    expect(datos.todaysTransactions).toHaveLength(2); // las 2 cuentas caben en un solo lote de 30
    expect(datos.sellTransactions.every((t) => t.type === 'sell')).toBe(true);
    expect(datos.sellTransactionsByAccount['acc-1']).toBeDefined();
  });

  it('un inversor sin cuentas activas no provoca ninguna lectura más', async () => {
    const db = crearDb({ cuentas: [] });
    expect(await cargarDatosDelInversor(db, 'u1', '2026-10-20')).toBeNull();
  });
});
