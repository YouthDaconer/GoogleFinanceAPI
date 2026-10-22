/**
 * HU 6.3 — El cierre póstumo: nadie queda fuera en silencio.
 *
 * Defiende el Escenario 1 (queda constancia de cuántos se procesaron) y el
 * Escenario 4 / RN-6.3-N (cuando alguien queda fuera hay un aviso visible, con
 * cuántos, cuáles y por qué).
 *
 * @see platform-docs/stories/6.3-cierre-nocturno-termina-siempre/refinamiento.md (T6, T9)
 */

// === Mocks ===

jest.mock('../firebaseAdmin', () => ({ firestore: jest.fn(() => ({})) }));

jest.mock('firebase-admin/firestore', () => ({
  FieldValue: { serverTimestamp: () => 'SERVER_TIMESTAMP' },
}));

jest.mock('firebase-functions/v2/scheduler', () => ({
  onSchedule: jest.fn((opts, handler) => handler),
}));

jest.mock('firebase-functions/params', () => ({
  defineSecret: jest.fn(() => ({ value: () => 'mock-secret' })),
}));

jest.mock('../eodProgressService', () => ({
  leerProgreso: jest.fn(),
  listarFallidos: jest.fn().mockResolvedValue([]),
  listarSinRastro: jest.fn().mockResolvedValue([]),
  cerrarProgreso: jest.fn().mockResolvedValue(undefined),
  ESTADO_DIA: { EN_CURSO: 'en-curso', COMPLETO: 'completo', INCOMPLETO: 'incompleto' },
}));

jest.mock('../eodMarketSnapshot', () => ({
  leerSnapshotDelDia: jest.fn().mockResolvedValue({
    prices: [{ symbol: 'AAPL', price: 231.47 }],
    currencies: [{ code: 'USD', exchangeRate: 1, isActive: true }],
  }),
}));

jest.mock('../calculatePortfolioRisk', () => ({ calculatePortfolioRisk: jest.fn().mockResolvedValue(undefined) }));
jest.mock('../cacheInvalidationService', () => ({
  invalidatePerformanceCacheBatch: jest.fn().mockResolvedValue({ usersProcessed: 0, cachesDeleted: 3 }),
}));
jest.mock('../benchmarkSnapshotService', () => ({ updateBenchmarkSnapshots: jest.fn().mockResolvedValue({}) }));
jest.mock('../marketDataScheduled', () => ({ saveIndicesHistoryDataInternal: jest.fn().mockResolvedValue({ count: 5 }) }));
jest.mock('../indexHistoryService', () => ({ refreshIndexCacheInternal: jest.fn().mockResolvedValue({ refreshed: 2 }) }));

const progreso = require('../eodProgressService');
const { calculatePortfolioRisk } = require('../calculatePortfolioRisk');
const { finalizarCierre, COLECCION_FALLOS, PREFIJO_ALERTA } = require('../eodFinalizationScheduled');

// ============================================================================
// Fábricas locales
// ============================================================================

const progresoDelDia = (overrides = {}) => ({
  date: '2026-10-20',
  totalUsers: 3,
  userIds: ['u1', 'u2', 'u3'],
  completedUsers: 3,
  failedUsers: 0,
  estado: 'en-curso',
  startedAt: '2026-10-21T00:05:00.000Z',
  finishedAt: null,
  runId: 'run-1',
  ...overrides,
});

/** Firestore de mentira que recuerda lo que se le escribe. */
const crearDb = () => {
  const escrito = new Map();
  return {
    collection: (nombre) => ({
      where: function () { return this; },
      get: async () => ({ docs: [], size: 0, empty: true }),
      doc: (id) => ({
        set: async (data) => { escrito.set(`${nombre}/${id}`, data); },
        get: async () => ({ exists: false, data: () => undefined }),
      }),
    }),
    _escrito: escrito,
  };
};

let errores;
beforeEach(() => {
  jest.clearAllMocks();
  errores = [];
  jest.spyOn(console, 'error').mockImplementation((...args) => errores.push(args.join(' ')));
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
  progreso.listarFallidos.mockResolvedValue([]);
  progreso.listarSinRastro.mockResolvedValue([]);
});

afterEach(() => jest.restoreAllMocks());

// ============================================================================

describe('eodFinalization — todos quedan procesados (HU 6.3, Escenario 1)', () => {
  it('con todos dentro, el día se cierra como completo y no se avisa de nada', async () => {
    progreso.leerProgreso.mockResolvedValue(progresoDelDia());
    const db = crearDb();

    const resultado = await finalizarCierre({ db, fecha: '2026-10-20' });

    expect(resultado.estado).toBe('completo');
    expect(resultado.totalFuera).toBe(0);
    expect(progreso.cerrarProgreso).toHaveBeenCalledWith(db, '2026-10-20', 'completo');
    expect(db._escrito.has(`${COLECCION_FALLOS}/2026-10-20`)).toBe(false);
    expect(errores).toEqual([]);
  });

  it('los pasos globales corren con los inversores que terminaron', async () => {
    progreso.leerProgreso.mockResolvedValue(progresoDelDia());

    await finalizarCierre({ db: crearDb(), fecha: '2026-10-20' });

    expect(calculatePortfolioRisk).toHaveBeenCalledTimes(1);
    expect(calculatePortfolioRisk.mock.calls[0][0].userIds).toEqual(['u1', 'u2', 'u3']);
    expect(calculatePortfolioRisk.mock.calls[0][0].calculationDate).toBe('2026-10-20');
  });

  it('queda constancia del recuento en systemStatus', async () => {
    progreso.leerProgreso.mockResolvedValue(progresoDelDia());
    const db = crearDb();

    await finalizarCierre({ db, fecha: '2026-10-20' });

    const estado = db._escrito.get('systemStatus/marketData');
    expect(estado.performanceCalculated).toBe(3);
    expect(estado.failedUsers).toBe(0);
    expect(estado.source).toBe('eod-fanout');
  });
});

describe('eodFinalization — nadie queda fuera en silencio (HU 6.3, RN-6.3-N, Escenario 4)', () => {
  it('un inversor que agotó sus reintentos queda nombrado, con su motivo', async () => {
    progreso.leerProgreso.mockResolvedValue(progresoDelDia({ completedUsers: 2, failedUsers: 1 }));
    progreso.listarFallidos.mockResolvedValue([
      { userId: 'u2', error: 'DEADLINE_EXCEEDED', intentos: 3 },
    ]);
    const db = crearDb();

    const resultado = await finalizarCierre({ db, fecha: '2026-10-20' });

    expect(resultado.estado).toBe('incompleto');
    expect(resultado.totalFuera).toBe(1);

    const aviso = db._escrito.get(`${COLECCION_FALLOS}/2026-10-20`);
    expect(aviso.totalUsers).toBe(3);
    expect(aviso.totalFuera).toBe(1);
    expect(aviso.fallidos).toEqual([{ userId: 'u2', error: 'DEADLINE_EXCEEDED', intentos: 3 }]);
  });

  it('el aviso sale al log con el prefijo que engancha la alerta de GCP', async () => {
    progreso.leerProgreso.mockResolvedValue(progresoDelDia({ completedUsers: 2, failedUsers: 1 }));
    progreso.listarFallidos.mockResolvedValue([{ userId: 'u2', error: 'timeout', intentos: 3 }]);

    await finalizarCierre({ db: crearDb(), fecha: '2026-10-20' });

    const aviso = errores.find((e) => e.includes(PREFIJO_ALERTA));
    expect(aviso).toBeDefined();
    expect(aviso).toContain('1 de 3 inversores quedaron sin rendimiento');
    expect(aviso).toContain('u2');
  });

  it('el que nunca se ejecutó también entra en el aviso, distinguido del que falló', async () => {
    progreso.leerProgreso.mockResolvedValue(progresoDelDia({ completedUsers: 1, failedUsers: 1 }));
    progreso.listarFallidos.mockResolvedValue([{ userId: 'u2', error: 'timeout', intentos: 3 }]);
    progreso.listarSinRastro.mockResolvedValue(['u3']);
    const db = crearDb();

    const resultado = await finalizarCierre({ db, fecha: '2026-10-20' });

    expect(resultado.totalFuera).toBe(2);
    const aviso = db._escrito.get(`${COLECCION_FALLOS}/2026-10-20`);
    expect(aviso.sinRastro).toEqual(['u3']);
    expect(aviso.fallidos).toHaveLength(1);
    expect(errores.find((e) => e.includes(PREFIJO_ALERTA)))
      .toContain('1 fallaron tras sus reintentos, 1 nunca se ejecutaron');
  });

  it('que falte alguien no deja a los demás sin sus pasos globales', async () => {
    progreso.leerProgreso.mockResolvedValue(progresoDelDia({ completedUsers: 2, failedUsers: 1 }));
    progreso.listarFallidos.mockResolvedValue([{ userId: 'u2', error: 'timeout', intentos: 3 }]);

    await finalizarCierre({ db: crearDb(), fecha: '2026-10-20' });

    expect(calculatePortfolioRisk.mock.calls[0][0].userIds).toEqual(['u1', 'u3']);
  });
});

describe('eodFinalization — cuándo no hay nada que finalizar (HU 6.3, T6)', () => {
  it('sin cierre repartido ese día no se avisa de nada: el reparto puede estar apagado', async () => {
    progreso.leerProgreso.mockResolvedValue(null);
    const db = crearDb();

    const resultado = await finalizarCierre({ db, fecha: '2026-10-20' });

    expect(resultado.estado).toBe('sin-cierre-repartido');
    expect(errores).toEqual([]);
    expect(progreso.cerrarProgreso).not.toHaveBeenCalled();
  });

  it('un día ya finalizado no se vuelve a finalizar', async () => {
    progreso.leerProgreso.mockResolvedValue(progresoDelDia({ estado: 'completo' }));

    const resultado = await finalizarCierre({ db: crearDb(), fecha: '2026-10-20' });

    expect(resultado.estado).toBe('completo');
    expect(calculatePortfolioRisk).not.toHaveBeenCalled();
    expect(progreso.cerrarProgreso).not.toHaveBeenCalled();
  });
});
