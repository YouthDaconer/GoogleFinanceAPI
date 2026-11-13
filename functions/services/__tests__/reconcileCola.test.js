/**
 * HU 6.8.2 — Ningún inversor bloquea a los demás y operación ve el atraso.
 *
 * La corrida real (`runReconciliation`, `corregirTramo`, la marca y la
 * revisión) sobre una Firestore en memoria y un reloj simulado. El tramo lo
 * ejecuta un ejecutor simulado que sigue un guion por inversor: avanza, se
 * queda sin memoria o sin tiempo, falla o termina sin corregir ningún día. Lo
 * que se prueba aquí es la cola, los intentos y el estado de la noche; el
 * cálculo de cada tramo ya lo prueba el simulador de la 6.8.1
 * (`reconcileTramos.test.js`), y el aislamiento real, `ejecutorAislado.test.js`.
 *
 * Fechas de `qa.md`: N1 = 2026-11-13 02:00 ET; su último día cerrado es el
 * 2026-11-12.
 *
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/qa.md
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

const { runReconciliation, liberarRevision, LIMITS } = require('../reconcileStalePerformance');
const { markPerformanceAsStale } = require('../../utils/performanceStaleMarker');
const { PREFIJO_ALERTA, COLECCION_NOCHES } = require('../reconcileNightReportService');

// --- Noches ------------------------------------------------------------------
const nocheN = (n) => DateTime.fromISO('2026-11-13T02:00:00', { zone: 'America/New_York' }).plus({ days: n - 1 });
const fechaN = (n) => nocheN(n).toISODate();

// --- Ejecutor simulado ---------------------------------------------------------
/** uid → acciones, una por tramo, en orden. Sin guion, el tramo avanza. */
let guion = {};
/** uid → lo que cuesta su tramo, en ms del reloj simulado. */
let costos = {};
/** Cada tramo que se le pidió al ejecutor. */
let llamadas = [];
const COSTO_POR_DEFECTO = 5000;

const RESULTADOS = {
  'avanza': ({ tramo }) => ({ ok: true, resultado: { success: true, daysProcessed: tramo.length, docsBorrados: 0, errors: [] } }),
  'solo-borra': () => ({ ok: true, resultado: { success: true, daysProcessed: 0, docsBorrados: 4, errors: [] } }),
  'sin_avance': () => ({ ok: true, resultado: { success: true, daysProcessed: 0, docsBorrados: 0, errors: [] } }),
  'motor-falla': () => ({ ok: true, resultado: { success: false, daysProcessed: 0, docsBorrados: 0, errors: ['Sin precios por fallo del API'] } }),
  'memoria': () => ({ ok: false, motivo: 'memoria', detalle: 'superó el tope de 512 MB de memoria' }),
  'tiempo': () => ({ ok: false, motivo: 'tiempo', detalle: 'superó el tope de 120 s' }),
  'error': () => ({ ok: false, motivo: 'error', detalle: 'fallo del tramo' }),
};

const ejecutor = {
  ejecutar: async (payload) => {
    llamadas.push({ uid: payload.userId, desde: payload.tramo[0], dias: payload.tramo.length, at: mockClock.ms });
    const accion = (guion[payload.userId] || []).shift() || 'avanza';
    if (typeof accion === 'function') return accion(payload);
    mockClock.ms += accion === 'tiempo' ? LIMITS.TIEMPO_MAX_TRAMO_MS : (costos[payload.userId] ?? COSTO_POR_DEFECTO);
    return RESULTADOS[accion](payload);
  },
  cerrar: async () => {},
};

// --- Estado ------------------------------------------------------------------
let logs = [];

function resetStore() {
  mockStore.docs = new Map([['marketHolidays/US', { holidays: { '2026-07-03': 'Independence Day (observed)', '2026-09-07': 'Labor Day' } }]]);
  mockStore.failCommit = null;
  mockStore.afterCommit = null;
  mockStore.writes = [];
  mockClock.commitMs = 0;
  guion = {};
  costos = {};
  llamadas = [];
}

/**
 * Un inversor pendiente. `pendienteDesde` acepta una fecha (se le pone hora) o
 * un ISO completo.
 */
function pendiente(uid, { since, pendienteDesde, retryCount = 0, intentos, extra = {} }) {
  const desde = pendienteDesde.includes('T') ? pendienteDesde : `${pendienteDesde}T12:00:00.000Z`;
  mockStore.docs.set(`portfolioPerformance/${uid}`, {
    userId: uid,
    _stale: {
      since,
      reason: 'retroactive_transaction',
      registeredAt: desde,
      pendienteDesde: desde,
      retryCount,
      ...(intentos ? { intentos } : {}),
      ...extra,
    },
  });
}

const staleOf = (uid) => mockClone(mockStore.docs.get(`portfolioPerformance/${uid}`)?._stale);
const revisionOf = (uid) => mockClone(mockStore.docs.get(`reconcileFailures/${uid}`));
const atendidos = (desde = 0) => llamadas.slice(desde).map((l) => l.uid);

/** Corre la corrección la noche `n` (02:00 ET) o a una hora dada. */
async function run({ n = 1, atTime = null, timeBudgetMs } = {}) {
  mockClock.ms = (atTime || nocheN(n)).toMillis();
  const desde = llamadas.length;
  const inicio = mockClock.ms;
  const totals = await runReconciliation({
    now: () => mockClock.ms,
    ejecutor,
    ...(timeBudgetMs ? { timeBudgetMs } : {}),
  });
  return { totals, estado: totals.estado, atendidos: atendidos(desde), duracionMs: mockClock.ms - inicio };
}

/** CQ-01 de `qa.md`: la cola normal. */
function colaNormal() {
  pendiente('inv-a', { since: '2024-08-05', pendienteDesde: '2026-11-05' }); // larga: 557 días
  pendiente('inv-b', { since: '2026-09-15', pendienteDesde: '2026-11-03' }); // hasta 6 meses
  pendiente('inv-c', { since: '2026-10-20', pendienteDesde: '2026-11-10' });
  pendiente('inv-d', { since: '2026-05-12', pendienteDesde: '2026-11-12T14:15:00.000Z' }); // justo 6 meses
  pendiente('inv-f', { since: '2026-05-11', pendienteDesde: '2026-11-12T21:40:00.000Z' }); // 6 meses y 1 día
}

/** CQ-02: 12 inversores con correcciones cortas, uno por día de espera. */
const P = Array.from({ length: 12 }, (_, i) => `inv-p${String(i + 1).padStart(2, '0')}`);
function colaSaturada() {
  P.forEach((uid, i) => pendiente(uid, { since: '2026-10-01', pendienteDesde: `2026-11-${String(i + 1).padStart(2, '0')}` }));
  // 50 s por tramo con 400 s de presupuesto: caben 8
  P.forEach((uid) => { costos[uid] = 50000; });
}

beforeEach(() => {
  resetStore();
  logs = [];
  const capturar = (...args) => logs.push(args.map(String).join(' '));
  jest.spyOn(console, 'log').mockImplementation(capturar);
  jest.spyOn(console, 'error').mockImplementation(capturar);
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

// --- Escenario 1 ----------------------------------------------------------------
describe('HU 6.8.2 — un inversor que falla no bloquea a los demás (Escenario 1)', () => {
  test.each([
    ['memoria, antes de completar su primer tramo', 'memoria'],
    ['tiempo, antes de completar su primer tramo', 'tiempo'],
    ['error, al empezar', 'error'],
    ['el motor falla a mitad del tramo y no escribe nada', 'motor-falla'],
  ])('QA-01 %s: 1 intento con su motivo y los que van detrás se corrigen esa noche', async (_fila, accion) => {
    colaNormal();
    guion['inv-a'] = [accion];

    const { atendidos: orden, estado } = await run({ n: 1 });

    const motivo = accion === 'motor-falla' ? 'error' : accion;
    expect(staleOf('inv-a')).toMatchObject({ since: '2024-08-05', retryCount: 1 });
    expect(staleOf('inv-a').intentos).toEqual([expect.objectContaining({ noche: '2026-11-13', motivo })]);
    expect(staleOf('inv-a').enCurso).toBeUndefined();
    // Los que van detrás de A se procesan esa misma noche
    expect(orden.slice(0, 3)).toEqual(['inv-b', 'inv-a', 'inv-c']);
    ['inv-b', 'inv-c', 'inv-d'].forEach((uid) => expect(staleOf(uid)).toBeUndefined());
    expect(orden).toContain('inv-f');
    // No pasa en silencio: A figura entre los atrasados (larga sin avance)
    expect(estado.atrasados.lista.map((p) => p.userId)).toContain('inv-a');
  });

  test('el intento se cuenta ANTES de empezar el tramo: si el proceso muere con él, ya contó', async () => {
    pendiente('inv-a', { since: '2026-10-01', pendienteDesde: '2026-11-05' });
    let durante = null;
    guion['inv-a'] = [(payload) => {
      durante = staleOf('inv-a');
      return RESULTADOS.avanza(payload);
    }];

    await run({ n: 1 });

    expect(durante).toMatchObject({ retryCount: 1 });
    expect(durante.intentos).toEqual([expect.objectContaining({ noche: '2026-11-13', motivo: 'interrumpida' })]);
    expect(durante.enCurso).toEqual(expect.any(String));
  });

  test('un inversor que llega con 3 intentos (el proceso murió en el tercero) pasa a revisión sin atenderlo, y la noche sigue', async () => {
    pendiente('inv-a', {
      since: '2026-10-01',
      pendienteDesde: '2026-11-05',
      retryCount: 3,
      intentos: [
        { noche: '2026-11-10', motivo: 'memoria', detalle: 'x' },
        { noche: '2026-11-11', motivo: 'tiempo', detalle: 'x' },
        { noche: '2026-11-12', motivo: 'interrumpida', detalle: 'el proceso de la noche se cortó con este inversor en curso' },
      ],
      extra: { enCurso: '2026-11-12T07:00:30.000Z' },
    });
    pendiente('inv-b', { since: '2026-10-01', pendienteDesde: '2026-11-06' });

    const { atendidos: orden, estado } = await run({ n: 1 });

    expect(orden).toEqual(['inv-b']);
    expect(staleOf('inv-a')).toBeUndefined();
    expect(revisionOf('inv-a').intentos.map((i) => i.motivo)).toEqual(['memoria', 'tiempo', 'interrumpida']);
    expect(estado.revision.nuevos.map((r) => r.userId)).toEqual(['inv-a']);
  });

  test('un relanzamiento el mismo día no suma otro intento: reemplaza el motivo de esa noche', async () => {
    pendiente('inv-a', { since: '2026-10-01', pendienteDesde: '2026-11-05' });
    guion['inv-a'] = ['memoria', 'error'];

    await run({ n: 1 });
    await run({ atTime: nocheN(1).set({ hour: 9 }) });

    expect(staleOf('inv-a').retryCount).toBe(1);
    expect(staleOf('inv-a').intentos).toEqual([expect.objectContaining({ noche: '2026-11-13', motivo: 'error' })]);
  });

  test('si entra una operación durante el tramo, el intento de esa noche se deshace: llegó información nueva, no falló nada', async () => {
    pendiente('inv-a', { since: '2026-10-01', pendienteDesde: '2026-11-05' });
    guion['inv-a'] = [async (payload) => {
      await markPerformanceAsStale('inv-a', '2026-09-01', { reason: 'asset_edit' });
      mockClock.ms += COSTO_POR_DEFECTO;
      return RESULTADOS.avanza(payload);
    }];

    await run({ n: 1, timeBudgetMs: 1 });

    expect(staleOf('inv-a')).toMatchObject({ since: '2026-09-01', retryCount: 0, intentos: [] });
    expect(staleOf('inv-a').enCurso).toBeUndefined();
    // Conserva su espera original
    expect(staleOf('inv-a').pendienteDesde).toBe('2026-11-05T12:00:00.000Z');
  });
});

describe('HU 6.8.2 — avanzar no es un intento (Escenario 1, RN-6.8.2-J)', () => {
  test.each([
    ['1 tramo completo; luego se queda sin memoria', ['avanza', 'memoria']],
    ['2 tramos completos; luego se queda sin tiempo', ['avanza', 'avanza', 'tiempo']],
  ])('QA-02 %s: 0 intentos, sigue en la cola y la noche siguiente continúa desde donde quedó', async (_fila, acciones) => {
    colaNormal();
    guion['inv-a'] = acciones;

    await run({ n: 1 });
    const queda = staleOf('inv-a');

    expect(queda).toMatchObject({ retryCount: 0 });
    expect(queda.intentos).toBeUndefined();
    expect(queda.since > '2024-08-05').toBe(true);

    const n2 = await run({ n: 2 });
    expect(llamadas.filter((l) => l.uid === 'inv-a').pop().desde >= queda.since).toBe(true);
    expect(n2.atendidos).toContain('inv-a');
  });

  test('un tramo que solo borra docs sobrantes también corrige: no es un intento sin avance', async () => {
    pendiente('inv-a', { since: '2026-10-01', pendienteDesde: '2026-11-05' });
    guion['inv-a'] = ['solo-borra'];

    await run({ n: 1 });

    expect(staleOf('inv-a')).toBeUndefined();
    expect(revisionOf('inv-a')).toBeUndefined();
  });
});

describe('HU 6.8.2 — una corrección que termina sin corregir ningún día también es un fallo (QA-19)', () => {
  test.each([
    ['INV-B, corrección de hasta 6 meses', 'inv-b', 'corta'],
    ['INV-A, corrección larga', 'inv-a', 'larga'],
  ])('%s: 1 intento "sin_avance", sigue pendiente y figura entre los atrasados', async (_fila, uid, tipo) => {
    colaNormal();
    guion[uid] = ['sin_avance'];

    const { estado } = await run({ n: 1 });

    expect(staleOf(uid)).toMatchObject({ retryCount: 1 });
    expect(staleOf(uid).intentos).toEqual([expect.objectContaining({
      noche: '2026-11-13', motivo: 'sin_avance', detalle: 'terminó sin error pero sin corregir ningún día',
    })]);
    expect(estado.pendientes.lista.map((p) => p.userId)).toContain(uid);
    expect(estado.atrasados.lista).toContainEqual(expect.objectContaining({ userId: uid, tipo }));
    expect(logs.find((l) => l.startsWith(PREFIJO_ALERTA))).toBeDefined();
  });
});

// --- Escenario 2 ----------------------------------------------------------------
describe('HU 6.8.2 — tres intentos sin avance seguidos pasan a revisión (Escenario 2)', () => {
  test('QA-03: con 2 intentos sigue en la cola y se le vuelve a atender', async () => {
    colaNormal();
    guion['inv-a'] = ['memoria', 'tiempo'];

    await run({ n: 1 });
    await run({ n: 2 });

    expect(staleOf('inv-a')).toMatchObject({ retryCount: 2 });
    expect(revisionOf('inv-a')).toBeUndefined();
    const n3 = await run({ n: 3 });
    expect(n3.atendidos).toContain('inv-a');
  });

  test('QA-04: al tercero pasa a revisión con el motivo y la fecha de cada intento, y deja de ocupar sitio', async () => {
    colaNormal();
    guion['inv-a'] = ['memoria', 'tiempo', 'error'];

    await run({ n: 1 });
    await run({ n: 2 });
    const n3 = await run({ n: 3 });

    expect(staleOf('inv-a')).toBeUndefined();
    const revision = revisionOf('inv-a');
    expect(revision.intentos.map((i) => [i.motivo, i.noche])).toEqual([
      ['memoria', '2026-11-13'], ['tiempo', '2026-11-14'], ['error', '2026-11-15'],
    ]);
    expect(revision.resolvedAt).toBeUndefined();
    expect(revision.pendienteDesde).toBe('2026-11-05T12:00:00.000Z');
    expect(n3.estado.revision.nuevos).toEqual([expect.objectContaining({ userId: 'inv-a' })]);

    // N4: B, C, D y F, pendientes de nuevo; A no se atiende
    ['inv-b', 'inv-c', 'inv-d', 'inv-f'].forEach((uid, i) => pendiente(uid, { since: '2026-11-02', pendienteDesde: `2026-11-1${i}` }));
    const n4 = await run({ n: 4 });
    expect(n4.atendidos).toEqual(['inv-b', 'inv-c', 'inv-d', 'inv-f']);
  });

  test('QA-05: operación lo libera dos veces y vuelve a la cola una sola vez, sin intentos y con su espera original', async () => {
    colaNormal();
    guion['inv-a'] = ['memoria', 'tiempo', 'error'];
    for (let n = 1; n <= 3; n++) await run({ n });

    mockClock.ms = DateTime.fromISO('2026-11-16T10:00:00', { zone: 'America/New_York' }).toMillis();
    const primera = await liberarRevision('inv-a', { now: () => mockClock.ms });
    mockClock.ms += 5 * 60 * 1000;
    const segunda = await liberarRevision('inv-a', { now: () => mockClock.ms });

    expect(primera).toMatchObject({ estado: 'liberado', pendienteDesde: '2026-11-05T12:00:00.000Z' });
    expect(segunda.estado).toBe('ya-liberado');
    expect(staleOf('inv-a')).toMatchObject({ retryCount: 0, intentos: [], pendienteDesde: '2026-11-05T12:00:00.000Z' });
    expect(revisionOf('inv-a').resolvedAt).toEqual(expect.any(String));
    expect([...mockStore.docs.keys()].filter((k) => k === 'portfolioPerformance/inv-a')).toHaveLength(1);

    const n5 = await run({ n: 5 });
    expect(n5.atendidos[0]).toBe('inv-a');
    expect(n5.estado.revision.totalEnRevision).toBe(0);
  });

  test('QA-05 (borde): liberar a quien no está en revisión no hace nada; sin --aplicar solo simula', async () => {
    pendiente('inv-a', { since: '2026-10-01', pendienteDesde: '2026-11-05' });
    expect(await liberarRevision('inv-a')).toEqual({ estado: 'sin-revision' });

    mockStore.docs.set('reconcileFailures/inv-b', { userId: 'inv-b', staleData: { since: '2026-10-01', pendienteDesde: '2026-11-01T00:00:00.000Z' }, affectedRange: { since: '2026-10-01' } });
    mockStore.docs.set('portfolioPerformance/inv-b', { userId: 'inv-b' });
    const simulado = await liberarRevision('inv-b', { aplicar: false });
    expect(simulado).toMatchObject({ estado: 'simulado', since: '2026-10-01' });
    expect(staleOf('inv-b')).toBeUndefined();
    expect(revisionOf('inv-b').resolvedAt).toBeUndefined();
  });

  test('QA-18: una noche con avance reinicia la cuenta; solo 3 noches sin avance SEGUIDAS llevan a revisión', async () => {
    colaNormal();
    guion['inv-a'] = ['memoria', 'avanza', 'tiempo', 'error', 'memoria'];
    // Que en N2 avance un tramo y no tome otro: un tramo de A cuesta toda la noche
    costos['inv-a'] = LIMITS.TIME_BUDGET_MS;

    await run({ n: 1 });
    expect(staleOf('inv-a').retryCount).toBe(1);
    await run({ n: 2 });
    expect(staleOf('inv-a')).toMatchObject({ retryCount: 0 });
    await run({ n: 3 });
    expect(staleOf('inv-a').retryCount).toBe(1);
    await run({ n: 4 });
    // 3 noches fallidas desde N1, pero no seguidas
    expect(staleOf('inv-a').retryCount).toBe(2);
    expect(revisionOf('inv-a')).toBeUndefined();

    const n5 = await run({ n: 5 });
    expect(n5.atendidos).toContain('inv-a');
    expect(staleOf('inv-a')).toBeUndefined();
    expect(revisionOf('inv-a').intentos.map((i) => [i.motivo, i.noche])).toEqual([
      ['tiempo', '2026-11-15'], ['error', '2026-11-16'], ['memoria', '2026-11-17'],
    ]);
  });
});

// --- Escenario 3 ----------------------------------------------------------------
describe('HU 6.8.2 — con más trabajo que tiempo, el que más espera va primero (Escenario 3, RN-6.8-P)', () => {
  test('QA-06: se atiende en orden de espera hasta agotar el tiempo; los que no se alcanzan son los de menor espera', async () => {
    colaSaturada();

    const { atendidos: orden } = await run({ n: 1 });

    expect(orden).toEqual(P.slice(0, 8));
    P.slice(8).forEach((uid) => expect(staleOf(uid)).toMatchObject({ retryCount: 0 }));
    P.slice(8).forEach((uid) => expect(staleOf(uid).intentos).toBeUndefined());
  });

  test('el empate de espera lo desempata la hora exacta: INV-D (09:15) antes que INV-F (16:40)', async () => {
    colaNormal();
    const { atendidos: orden } = await run({ n: 1 });
    expect(orden.indexOf('inv-d')).toBeLessThan(orden.indexOf('inv-f'));
  });

  test('QA-07: con llegadas cada día, ningún recién llegado pasa delante de uno que espera más, y nadie se queda al final', async () => {
    colaSaturada();
    const llegadas = Array.from({ length: 24 }, (_, i) => `inv-q${String(i + 1).padStart(2, '0')}`);

    const porNoche = [];
    for (let n = 1; n <= 4; n++) {
      porNoche.push((await run({ n })).atendidos);
      // Llegan 6 nuevos ese día, después de la corrida
      llegadas.slice((n - 1) * 6, n * 6).forEach((uid, i) => {
        pendiente(uid, { since: '2026-11-02', pendienteDesde: `${fechaN(n)}T1${i}:00:00.000Z` });
        costos[uid] = 50000;
      });
    }

    const orden = porNoche.flat();
    const posicion = (uid) => orden.indexOf(uid);
    P.forEach((p) => {
      expect(posicion(p)).toBeGreaterThanOrEqual(0);
      llegadas.filter((q) => posicion(q) >= 0).forEach((q) => expect(posicion(p)).toBeLessThan(posicion(q)));
    });
    P.forEach((p) => expect(staleOf(p)).toBeUndefined());
    // Y los recién llegados también en su orden de llegada
    const qAtendidos = orden.filter((u) => llegadas.includes(u));
    expect(qAtendidos).toEqual(llegadas.slice(0, qAtendidos.length));
  });

  test('QA-16: relanzar a mano atiende a los que quedan por espera y no vuelve a corregir a nadie; el segundo relanzamiento no corrige nada', async () => {
    colaSaturada();
    await run({ n: 1 });

    const manana = nocheN(1).set({ hour: 9 });
    const primera = await run({ atTime: manana });
    const segunda = await run({ atTime: manana.plus({ minutes: 30 }) });

    expect(primera.atendidos).toEqual(P.slice(8));
    expect(segunda.atendidos).toEqual([]);
    P.forEach((uid) => expect(llamadas.filter((l) => l.uid === uid)).toHaveLength(1));
  });
});

// --- Escenarios 5 y 6 -------------------------------------------------------------
describe('HU 6.8.2 — el estado de cada mañana (Escenario 5)', () => {
  test('QA-12: corregidos, pendientes, el más antiguo y quién pasó a revisión con sus 3 intentos', async () => {
    colaNormal();
    pendiente('inv-g', {
      since: '2026-10-01',
      pendienteDesde: '2026-11-10T23:00:00.000Z',
      retryCount: 2,
      intentos: [
        { noche: '2026-11-11', motivo: 'memoria', detalle: 'superó el tope de 512 MB de memoria' },
        { noche: '2026-11-12', motivo: 'tiempo', detalle: 'superó el tope de 120 s' },
      ],
    });
    guion['inv-g'] = ['error'];
    // A y F avanzan un tramo; el segundo no les cabe y no completan
    guion['inv-a'] = ['avanza', 'tiempo'];
    guion['inv-f'] = ['avanza', 'tiempo'];

    const { estado } = await run({ n: 1 });

    expect(estado.corregidos).toBe(3);
    expect(estado.pendientes.total).toBe(2);
    expect(estado.pendientes.lista.map((p) => p.userId)).toEqual(['inv-a', 'inv-f']);
    expect(estado.pendientes.masAntiguoDesde).toBe('2026-11-05T12:00:00.000Z');
    expect(estado.revision.nuevos).toEqual([expect.objectContaining({ userId: 'inv-g' })]);
    expect(estado.revision.nuevos[0].intentos.map((i) => [i.motivo, i.noche])).toEqual([
      ['memoria', '2026-11-11'], ['tiempo', '2026-11-12'], ['error', '2026-11-13'],
    ]);
    // Ninguno de los pendientes está atrasado: los dos son largos y avanzaron
    expect(estado.atrasados.total).toBe(0);

    const doc = mockStore.docs.get(`${COLECCION_NOCHES}/2026-11-13_020000`);
    expect(doc).toMatchObject({ noche: '2026-11-13', corregidos: 3 });
    expect(logs.find((l) => l.startsWith('[reconcile] ESTADO 2026-11-13'))).toMatch(/3 corregidos, 2 pendientes \(el más antiguo desde 2026-11-05\)/);
    // Pasar a revisión también se avisa (RN-6-E)
    expect(logs.find((l) => l.startsWith(PREFIJO_ALERTA))).toMatch(/0 inversor\(es\) atrasado\(s\); 1 pasaron a revisión \(inv-g\)/);
  });

  test('QA-13: una noche sin pendientes también deja su estado, sin aviso y sin tramos', async () => {
    const { estado, atendidos: orden, duracionMs } = await run({ n: 1 });

    expect(orden).toEqual([]);
    expect(estado).toMatchObject({
      corregidos: 0,
      pendientes: { total: 0, masAntiguoDesde: null, lista: [] },
      atrasados: { total: 0 },
      revision: { nuevos: [], totalNuevos: 0, totalEnRevision: 0 },
    });
    expect(mockStore.docs.has(`${COLECCION_NOCHES}/2026-11-13_020000`)).toBe(true);
    expect(logs.find((l) => l.startsWith(PREFIJO_ALERTA))).toBeUndefined();
    expect(duracionMs).toBe(0);
  });
});

describe('HU 6.8.2 — el gasto de una noche está acotado (Escenario 6, RN-6.8-N, RN-6.8.2-L)', () => {
  test.each([[1], [5], [12], [200]])('QA-14 y QA-10 con %i pendientes: ningún tramo empieza pasados 400 s y la noche no pasa de 520 s', async (cuantos) => {
    for (let i = 1; i <= cuantos; i++) {
      const uid = `inv-v${String(i).padStart(3, '0')}`;
      pendiente(uid, { since: '2026-10-01', pendienteDesde: `2026-11-12T${String(i % 24).padStart(2, '0')}:00:00.000Z` });
      guion[uid] = ['tiempo']; // el peor caso: cada tramo agota su tope
    }

    const { duracionMs } = await run({ n: 1 });

    const inicio = nocheN(1).toMillis();
    expect(llamadas.every((l) => l.at - inicio < LIMITS.TIME_BUDGET_MS)).toBe(true);
    expect(duracionMs).toBeLessThanOrEqual(LIMITS.TIME_BUDGET_MS + LIMITS.TIEMPO_MAX_TRAMO_MS);
    expect(LIMITS.TIME_BUDGET_MS + LIMITS.TIEMPO_MAX_TRAMO_MS).toBeLessThan(540 * 1000);
    // El atraso solo se avisa: la corrida no lanza otra ni se alarga
    expect(llamadas.length).toBe(Math.min(cuantos, Math.ceil(LIMITS.TIME_BUDGET_MS / LIMITS.TIEMPO_MAX_TRAMO_MS)));
  });
});
