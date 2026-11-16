/**
 * HU 6.12 — La corrección nocturna rehace los resúmenes una sola vez, al
 * terminar, desde la fecha más antigua de la corrección.
 *
 * Corre la corrida real sobre la Firestore en memoria y un reloj simulado:
 * `runReconciliation`, `corregirTramo`, `ejecutarTramo` con su propagación, la
 * marca, la revisión y el cierre de los resúmenes. Se simulan dos cosas:
 * - el motor (`backfillUserPerformance`), que solo dice cuántos días corrigió;
 * - la reconstrucción (`rehacerResumenesDelInversor`), que reescribe los
 *   resúmenes o falla a mitad. Su cálculo lo prueba `snapshotRebuildService.test.js`.
 *
 * Fechas de `qa.md`: el día D es el 2026-11-16 y N1, la noche del 16 al 17
 * (02:00 ET del 2026-11-17).
 *
 * @see platform-docs/stories/6.12-correccion-desde-fecha-afectada/refinamiento.md (T6b)
 */

const { DateTime } = require('luxon');

const {
  clock: mockClock,
  store: mockStore,
  db: mockDb,
  clone: mockClone,
} = require('./support/firestoreEnMemoria');

jest.mock('firebase-admin/firestore', () => require('./support/firestoreEnMemoria').firestoreModule);
jest.mock('../firebaseAdmin', () => ({ firestore: () => mockDb }));
jest.mock('firebase-functions/v2/scheduler', () => ({ onSchedule: (_opts, fn) => fn }));
jest.mock('../periodConsolidationScheduled', () => ({
  consolidateMonth: jest.fn(async () => {}),
  consolidateYear: jest.fn(async () => {}),
}));
jest.mock('../cacheInvalidationService', () => ({ invalidatePerformanceCache: jest.fn(async () => {}) }));
jest.mock('../config', () => ({ getServiceTokenSecret: () => 'token-de-prueba' }));

/** Lo que cuesta cada tramo en el reloj simulado. */
const mockMotor = { costoMs: 5000, alCorregir: null };
jest.mock('../backfillCoreModule', () => {
  const real = jest.requireActual('../backfillCoreModule');
  return {
    ...real,
    backfillUserPerformance: jest.fn(async (userId, tramo) => {
      require('./support/firestoreEnMemoria').clock.ms += mockMotor.costoMs;
      if (mockMotor.alCorregir) await mockMotor.alCorregir(userId, tramo);
      return { success: true, daysProcessed: tramo.length, staleDocs: [], errors: [] };
    }),
  };
});

jest.mock('../snapshots/snapshotRebuildService', () => ({
  ...jest.requireActual('../snapshots/snapshotRebuildService'),
  rehacerResumenesDelInversor: jest.fn(),
}));

const { runReconciliation, liberarRevision, LIMITS } = require('../reconcileStalePerformance');
const { markPerformanceAsStale } = require('../../utils/performanceStaleMarker');
const { rehacerResumenesDelInversor } = require('../snapshots/snapshotRebuildService');
const { invalidatePerformanceCache } = require('../cacheInvalidationService');

const G = 'uid-inversor-G';
const P = 'uid-inversor-P';
const N1 = DateTime.fromISO('2026-11-17T02:00:00', { zone: 'America/New_York' });
const noche = (n) => N1.plus({ days: n - 1 });
const iso = (ms) => new Date(ms).toISOString();

// ============================================================================
// ESTADO
// ============================================================================

const VIEJO = '2026-11-16T21:30:00.000Z';
const RESUMENES_G = [`${G}_USD`, `${G}_COP`, `${G}_CTA-1_USD`, `${G}_AAPL_stock_USD`, `${G}_VOO_etf_USD`];

function resetStore() {
  mockStore.docs = new Map([['marketHolidays/US', { holidays: { '2026-09-07': 'Labor Day', '2025-07-04': 'Independence Day' } }]]);
  mockStore.failCommit = null;
  mockStore.afterCommit = null;
  mockStore.writes = [];
  mockClock.commitMs = 0;
  mockMotor.costoMs = 5000;
  mockMotor.alCorregir = null;
  for (const uid of [G, P]) {
    mockStore.docs.set(`portfolioPerformance/${uid}`, { userId: uid, lastSnapshotUpdate: VIEJO });
    mockStore.docs.set(`userData/${uid}`, { lastSnapshotUpdate: VIEJO });
  }
  for (const id of [...RESUMENES_G, `${P}_USD`]) {
    mockStore.docs.set(`performanceSnapshots/${id}`, { cifras: 'anteriores', lastUpdated: VIEJO });
  }
}

/** La reconstrucción simulada: reescribe `rehace` (o todos) y luego falla si se pide. */
function reconstruccion({ rehace = RESUMENES_G, falla = null, comoHoy = [] } = {}) {
  rehacerResumenesDelInversor.mockImplementation(async (db, { userId, desde }) => {
    for (const id of rehace) {
      mockClock.ms += 1000;
      await db.collection('performanceSnapshots').doc(id).set({ cifras: `nuevas desde ${desde}`, lastUpdated: iso(mockClock.ms) });
    }
    if (falla) throw new Error(falla);
    for (const { id } of comoHoy) await db.collection('performanceSnapshots').doc(id).delete();
    return { rehechos: rehace, sinCambios: [], comoHoy, diasLeidos: 15, ms: 1000 * rehace.length };
  });
}

const resumen = (id) => mockClone(mockStore.docs.get(`performanceSnapshots/${id}`));
const senalDe = (uid) => mockStore.docs.get(`portfolioPerformance/${uid}`).lastSnapshotUpdate;
const marcaDe = (uid) => mockClone(mockStore.docs.get(`portfolioPerformance/${uid}`)._stale);

async function correr(n = 1, opciones = {}) {
  mockClock.ms = noche(n).toMillis();
  return runReconciliation({ now: () => mockClock.ms, ...opciones });
}

/** Registra la operación tardía el día D (el reloj real de la marca no importa aquí). */
async function registrar(uid, fecha) {
  await markPerformanceAsStale(uid, fecha, { transactionType: 'buy', portfolioAccount: 'CTA-1' });
}

let logs;
beforeEach(() => {
  resetStore();
  rehacerResumenesDelInversor.mockReset();
  reconstruccion();
  invalidatePerformanceCache.mockClear();
  delete process.env.REBUILD_SNAPSHOTS_FROM_DATE;
  logs = [];
  jest.spyOn(console, 'log').mockImplementation((...a) => logs.push(a.join(' ')));
  jest.spyOn(console, 'error').mockImplementation((...a) => logs.push(a.join(' ')));
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

// ============================================================================
// ESCENARIOS
// ============================================================================

describe('HU 6.12 — una corrección de una noche (Escenario 1, RN-6.12-A; QA-01)', () => {
  it('COR-3S: rehace una vez desde la fecha de la compra, antes de retirar la marca, y deja constancia', async () => {
    await registrar(G, '2026-10-26');

    const totals = await correr(1);

    expect(totals.reconciled).toBe(1);
    expect(rehacerResumenesDelInversor).toHaveBeenCalledTimes(1);
    expect(rehacerResumenesDelInversor).toHaveBeenCalledWith(mockDb, { userId: G, desde: '2026-10-26' });
    expect(resumen(`${G}_USD`).cifras).toBe('nuevas desde 2026-10-26');
    expect(marcaDe(G)).toBeUndefined();
    expect(logs.find((l) => l.includes(`RESUMENES ${G} desde 2026-10-26`))).toMatch(/5 rehechos, 0 sin cambios, 0 como hoy; 15 días leídos/);
  });

  it('QA-13: un inversor sin corrección no se toca', async () => {
    await registrar(G, '2026-10-26');
    await correr(1);
    expect(resumen(`${P}_USD`)).toEqual({ cifras: 'anteriores', lastUpdated: VIEJO });
    expect(senalDe(P)).toBe(VIEJO);
    expect(rehacerResumenesDelInversor).not.toHaveBeenCalledWith(mockDb, expect.objectContaining({ userId: P }));
  });
});

describe('HU 6.12 — la señal va al final y sella los rehechos (Escenario 5, RN-6.12-E; QA-07)', () => {
  it('la señal es posterior a toda escritura de resúmenes y los rehechos quedan con esa misma hora', async () => {
    await registrar(G, '2026-10-26');
    const ordenDeEscrituras = [];
    mockStore.afterCommit = async (ops) => ops.forEach((o) => ordenDeEscrituras.push(`${o.type}:${o.path}`));

    await correr(1);

    const senal = senalDe(G);
    expect(senal > VIEJO).toBe(true);
    expect(mockStore.docs.get(`userData/${G}`).lastSnapshotUpdate).toBe(senal);
    for (const id of RESUMENES_G) expect(resumen(id).lastUpdated).toBe(senal);
    // Orden: resúmenes rehechos → caché → señal → sellos → marca retirada
    const ultimaReescritura = Math.max(...RESUMENES_G.map((id) => ordenDeEscrituras.indexOf(`set:performanceSnapshots/${id}`)));
    const primeraSenal = ordenDeEscrituras.indexOf(`set:portfolioPerformance/${G}`);
    const primerSello = ordenDeEscrituras.findIndex((o) => o.startsWith('update:performanceSnapshots/'));
    const marcaRetirada = ordenDeEscrituras.lastIndexOf(`update:portfolioPerformance/${G}`);
    expect(ultimaReescritura).toBeLessThan(primeraSenal);
    expect(primeraSenal).toBeLessThan(primerSello);
    expect(primerSello).toBeLessThan(marcaRetirada);
    expect(invalidatePerformanceCache).toHaveBeenCalledWith(G);
  });
});

describe('HU 6.12 — varias correcciones antes de la noche (Escenario 4, RN-6.12-C; QA-05)', () => {
  it.each([
    ['COR-3S y después COR-2M', ['2026-10-26', '2026-09-16']],
    ['COR-2M y después COR-3S', ['2026-09-16', '2026-10-26']],
  ])('%s: rehace desde la más antigua', async (_caso, fechas) => {
    for (const f of fechas) await registrar(G, f);

    await correr(1);

    expect(rehacerResumenesDelInversor).toHaveBeenCalledTimes(1);
    expect(rehacerResumenesDelInversor).toHaveBeenCalledWith(mockDb, { userId: G, desde: '2026-09-16' });
  });
});

describe('HU 6.12 — una corrección de varias noches (Escenario 6, RN-6.12-D; QA-08)', () => {
  it('COR-LARGA: entre noches no toca resúmenes, caché ni señal; al terminar rehace una vez desde el inicio', async () => {
    await registrar(G, '2025-06-02');
    // Un tramo por noche: cada uno consume el presupuesto de la noche
    mockMotor.costoMs = LIMITS.TIME_BUDGET_MS;

    let n = 1;
    await correr(n);
    // N1: la corrección sigue; el inversor ve las cifras anteriores con el aviso
    expect(marcaDe(G).since > '2025-06-02').toBe(true);
    expect(marcaDe(G).desdeInicial).toBe('2025-06-02');
    expect(resumen(`${G}_USD`)).toEqual({ cifras: 'anteriores', lastUpdated: VIEJO });
    expect(senalDe(G)).toBe(VIEJO);
    expect(invalidatePerformanceCache).not.toHaveBeenCalled();
    expect(rehacerResumenesDelInversor).not.toHaveBeenCalled();

    // Siguientes noches hasta terminar; el último tramo es corto y deja tiempo para rehacer
    while (marcaDe(G) && n < 12) {
      n++;
      const pendientes = await require('../backfillCoreModule').generateTradingDays(marcaDe(G).since, noche(n).minus({ days: 1 }).toISODate());
      if (pendientes.length <= LIMITS.DIAS_POR_TRAMO) mockMotor.costoMs = 5000;
      await correr(n);
    }

    expect(marcaDe(G)).toBeUndefined();
    expect(rehacerResumenesDelInversor).toHaveBeenCalledTimes(1);
    expect(rehacerResumenesDelInversor).toHaveBeenCalledWith(mockDb, { userId: G, desde: '2025-06-02' });
    expect(invalidatePerformanceCache).toHaveBeenCalledTimes(1);
  });
});

describe('HU 6.12 — si la reconstrucción falla a mitad (Escenario 3, RN-6.12-F; QA-14)', () => {
  it('los que no alcanzó a rehacer se borran, la corrección termina y no hay reintento', async () => {
    await registrar(G, '2026-10-26');
    reconstruccion({ rehace: [`${G}_USD`, `${G}_CTA-1_USD`], falla: 'superó el tope de 512 MB' });

    const totals = await correr(1);

    expect(totals.reconciled).toBe(1);
    expect(marcaDe(G)).toBeUndefined();
    expect(resumen(`${G}_USD`).cifras).toBe('nuevas desde 2026-10-26');
    expect(resumen(`${G}_USD`).lastUpdated).toBe(senalDe(G));
    for (const id of [`${G}_COP`, `${G}_AAPL_stock_USD`, `${G}_VOO_etf_USD`]) expect(resumen(id)).toBeUndefined();
    expect(logs.find((l) => l.includes(`RESUMENES ${G}`))).toMatch(/falló \(error: superó el tope de 512 MB\); 2 rehechos, 3 como hoy, sin reintento/);

    // La noche siguiente no hay nada que hacer
    rehacerResumenesDelInversor.mockClear();
    await correr(2);
    expect(rehacerResumenesDelInversor).not.toHaveBeenCalled();
  });

  it('sin tiempo en la noche para rehacer, se tratan como hoy', async () => {
    await registrar(G, '2026-10-26');
    mockMotor.costoMs = LIMITS.TIEMPO_LIMITE_NOCHE_MS - 10 * 1000;

    await correr(1);

    expect(rehacerResumenesDelInversor).not.toHaveBeenCalled();
    for (const id of RESUMENES_G) expect(resumen(id)).toBeUndefined();
    expect(marcaDe(G)).toBeUndefined();
    expect(logs.find((l) => l.includes(`RESUMENES ${G}`))).toMatch(/falló \(tiempo: quedaban 10 s de la noche\)/);
  });
});

describe('HU 6.12 — la marca se mueve durante el último tramo', () => {
  it('ese tramo no rehace; la vuelta siguiente corrige desde la nueva fecha y rehace una vez desde ella', async () => {
    await registrar(G, '2026-10-26');
    mockMotor.alCorregir = async (uid) => {
      mockMotor.alCorregir = null;
      await registrar(uid, '2026-10-05');
    };

    const totals = await correr(1);

    expect(totals.moved).toBe(1);
    expect(totals.reconciled).toBe(1);
    expect(rehacerResumenesDelInversor).toHaveBeenCalledTimes(1);
    expect(rehacerResumenesDelInversor).toHaveBeenCalledWith(mockDb, { userId: G, desde: '2026-10-05' });
    expect(marcaDe(G)).toBeUndefined();
  });
});

describe('HU 6.12 — liberar de revisión (QA-12)', () => {
  it('liberar dos veces vuelve a la cola una vez y rehace una vez desde la fecha original', async () => {
    await registrar(G, '2026-10-26');
    // La corrección falló tres noches y pasó a revisión
    const stale = marcaDe(G);
    mockStore.docs.set(`reconcileFailures/${G}`, {
      userId: G, staleData: { ...stale, since: '2026-11-02' }, affectedRange: { since: '2026-11-02' }, pendienteDesde: stale.pendienteDesde,
    });
    const doc = mockStore.docs.get(`portfolioPerformance/${G}`);
    delete doc._stale;

    expect((await liberarRevision(G)).estado).toBe('liberado');
    expect((await liberarRevision(G)).estado).toBe('ya-liberado');
    expect(marcaDe(G)).toMatchObject({ since: '2026-11-02', desdeInicial: '2026-10-26' });

    await correr(1);
    expect(rehacerResumenesDelInversor).toHaveBeenCalledTimes(1);
    expect(rehacerResumenesDelInversor).toHaveBeenCalledWith(mockDb, { userId: G, desde: '2026-10-26' });
  });
});

describe('HU 6.12 — reversible y marcas anteriores (Escenario 9, RN-6-H; QA-11)', () => {
  it('con REBUILD_SNAPSHOTS_FROM_DATE=false la corrección borra todos los resúmenes como hoy', async () => {
    process.env.REBUILD_SNAPSHOTS_FROM_DATE = 'false';
    await registrar(G, '2026-10-26');

    await correr(1);

    expect(rehacerResumenesDelInversor).not.toHaveBeenCalled();
    for (const id of RESUMENES_G) expect(resumen(id)).toBeUndefined();
    expect(resumen(`${P}_USD`)).toEqual({ cifras: 'anteriores', lastUpdated: VIEJO });
    expect(marcaDe(G)).toBeUndefined();
  });

  it('una marca anterior a la HU (sin desdeInicial) sigue el camino de hoy en cada tramo', async () => {
    mockStore.docs.get(`portfolioPerformance/${G}`)._stale = {
      since: '2026-10-26', reason: 'retroactive_transaction', registeredAt: '2026-11-10T12:00:00.000Z',
      pendienteDesde: '2026-11-10T12:00:00.000Z', retryCount: 0, intentos: [],
    };

    await correr(1);

    expect(rehacerResumenesDelInversor).not.toHaveBeenCalled();
    for (const id of RESUMENES_G) expect(resumen(id)).toBeUndefined();
  });
});
