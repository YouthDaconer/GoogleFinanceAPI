/**
 * HU 6.13 (T6, T7a) — La herramienta `scripts/ruta-antigua-consolidados.js`
 * (raíz del proyecto).
 *
 * - QA-08: el inventario clasifica cada archivo que nombra la ruta antigua o los
 *   consolidados y señala lo no esperado; sobre el código de hoy da 0 no
 *   esperados, y relanzado da lo mismo.
 * - QA-03: la comparación distingue idéntica, aviso nuevo y diferente.
 * - Solo lectura: el proxy de Firestore deja pasar las lecturas y se queda con las
 *   escrituras (set, lotes, transacciones, `doc.ref.set`).
 * - QA-11: los registros se cuentan igual antes y después de la HU.
 * - QA-12: `rehacer` calcula los meses y años cerrados desde un mes.
 *
 * @see platform-docs/stories/6.13-retirar-ruta-antigua-consolidados/refinamiento.md (T6, T7a)
 */

const path = require('path');
const { DateTime } = require('luxon');

const herramienta = require(path.resolve(__dirname, '..', '..', '..', '..', '..', 'scripts', 'ruta-antigua-consolidados.js'));
const {
  parseArgs, clasificar, inventariar, crearFirestoreSoloLectura, normalizar, clasificarCombinacion,
  contarRegistros, periodosARehacer, PATRON_INVENTARIO,
} = herramienta;
const { closedMonthsSince } = require('../../services/reconcileStalePerformance');

const F = 'src/GoogleFinanceAPI/functions/';

describe('HU 6.13 — inventario (QA-08, RN-6.13-D)', () => {
  test('lo que encuentra el patrón, y lo que no', () => {
    expect(PATRON_INVENTARIO.test('db.collection(`${base}/consolidatedPeriods/monthly/periods`)')).toBe(true);
    expect(PATRON_INVENTARIO.test("await consolidateMonth(uid, null, '2026-10')")).toBe(true);
    expect(PATRON_INVENTARIO.test('getHistoricalReturnsOptimized')).toBe(true);
    // El interruptor no es un consolidado; las funciones puras compartidas tampoco
    expect(PATRON_INVENTARIO.test("require('./consolidatedPeriodsRetirement')")).toBe(false);
    expect(PATRON_INVENTARIO.test('consolidatePeriod(docs, periodKey)')).toBe(false);
  });

  test.each([
    [`${F}services/consolidatedReturnsService.js`, '', true, /ruta antigua/],
    [`${F}services/periodConsolidationScheduled.js`, '', true, /escritor detrás del interruptor/],
    [`${F}services/__tests__/reconcileTramos.test.js`, '', true, /^test$/],
    [`${F}scripts/migration/diagnoseMigration.js`, '/**\n * OBSOLETO (HU 6.13): …', true, /obsoleto/],
    [`${F}scripts/diagnostics/gold/backfillPortfolioPerformance.js`, 'if (consolidadosRetirados()) {', true, /salta la reconsolidación/],
    [`${F}services/copilot/copilotPayloadBuilder.js`, 'getHistoricalReturnsV2(uid)', false, /NO ESPERADO/],
    ['src/portafolio-inversiones/hooks/useReturns.ts', "callQueryOperation('getHistoricalReturnsOptimized')", false, /NO ESPERADO/],
    ['scripts/otro.js', 'consolidatedPeriods', false, /NO ESPERADO/],
  ])('clasifica %s', (rel, contenido, esperado, clase) => {
    const r = clasificar(rel, contenido);
    expect(r.esperado).toBe(esperado);
    expect(r.clase).toMatch(clase);
  });

  test('sobre el código de hoy: 0 no esperados, y relanzado da lo mismo', () => {
    const primera = inventariar();
    const segunda = inventariar();

    expect(primera.filter((h) => !h.esperado)).toEqual([]);
    expect(segunda).toEqual(primera);
    // Nada del navegador ni de finance-query usa la ruta antigua ni los consolidados
    expect(primera.filter((h) => h.archivo.startsWith('src/portafolio-inversiones/') || h.archivo.startsWith('src/finance-query/'))).toEqual([]);
  });
});

// ============================================================================
// Firestore de solo lectura
// ============================================================================

/** Un Firestore falso con los nombres de clase reales que el proxy reconoce. */
function crearFirestoreFalso(datos) {
  const escriturasReales = [];
  class DocumentSnapshot {
    constructor(ref) { this.ref = ref; this.id = ref.id; this.exists = datos[ref.path] !== undefined; }
    data() { return datos[this.ref.path]; }
  }
  class QuerySnapshot { constructor(docs) { this.docs = docs; this.size = docs.length; this.empty = docs.length === 0; } }
  class DocumentReference {
    constructor(ruta) { this.path = ruta; this.id = ruta.split('/').pop(); }
    async get() { return new DocumentSnapshot(this); }
    async set() { escriturasReales.push(`set ${this.path}`); }
    async update() { escriturasReales.push(`update ${this.path}`); }
    async delete() { escriturasReales.push(`delete ${this.path}`); }
  }
  class CollectionReference {
    constructor(ruta) { this.path = ruta; }
    doc(id) { return new DocumentReference(`${this.path}/${id || 'nuevo'}`); }
    where() { return this; }
    async get() {
      const docs = Object.keys(datos).filter((r) => r.startsWith(`${this.path}/`)).map((r) => new DocumentSnapshot(new DocumentReference(r)));
      return new QuerySnapshot(docs);
    }
    async add() { escriturasReales.push(`add ${this.path}`); }
  }
  class Firestore {
    collection(ruta) { return new CollectionReference(ruta); }
    doc(ruta) { return new DocumentReference(ruta); }
    batch() { return { set: () => escriturasReales.push('batch.set'), commit: async () => escriturasReales.push('batch.commit') }; }
    async runTransaction(fn) { escriturasReales.push('runTransaction'); return fn({}); }
    async getAll(...refs) { return refs.map((r) => new DocumentSnapshot(r)); }
  }
  return { db: new Firestore(), escriturasReales };
}

describe('HU 6.13 — Firestore de solo lectura (comparar no escribe)', () => {
  const DATOS = {
    'performanceSnapshots/uid-g_COP': { returns: { ytdReturn: 9.75 } },
    'userData/uid-g': { defaultCurrency: 'COP' },
  };

  test('las lecturas pasan con sus datos', async () => {
    const { db } = crearFirestoreFalso(DATOS);
    const { db: ro } = crearFirestoreSoloLectura(db);

    const doc = await ro.doc('performanceSnapshots/uid-g_COP').get();
    const coleccion = await ro.collection('userData').get();

    expect(doc.exists).toBe(true);
    expect(doc.data().returns.ytdReturn).toBe(9.75);
    expect(coleccion.docs.map((d) => d.id)).toEqual(['uid-g']);
  });

  test('set, update, delete, add, lotes, transacciones y doc.ref.set no llegan a la base', async () => {
    const { db, escriturasReales } = crearFirestoreFalso(DATOS);
    const { db: ro, escrituras } = crearFirestoreSoloLectura(db);

    await ro.doc('performanceSnapshots/uid-g_COP').set({ x: 1 });
    await ro.collection('userData').doc('uid-g').update({ y: 2 });
    await ro.doc('userData/uid-g/performanceCache/COP_overall').delete();
    await ro.collection('logs').add({ z: 3 });
    const lote = ro.batch();
    lote.set(ro.doc('a/b'), {});
    await lote.commit();
    const leidoEnTx = await ro.runTransaction(async (tx) => {
      const d = await tx.get(ro.doc('userData/uid-g'));
      tx.set(ro.doc('userData/uid-g'), { w: 1 });
      return d.data().defaultCurrency;
    });
    const snap = await ro.doc('userData/uid-g').get();
    await snap.ref.set({ v: 1 });

    expect(escriturasReales).toEqual([]);
    expect(leidoEnTx).toBe('COP');
    expect(escrituras.map((e) => `${e.op} ${e.ruta}`)).toEqual([
      'set performanceSnapshots/uid-g_COP',
      'update userData/uid-g',
      'delete userData/uid-g/performanceCache/COP_overall',
      'add logs',
      'batch.set a/b',
      'tx.set userData/uid-g',
      'set userData/uid-g',
    ]);
  });
});

// ============================================================================
// Comparar
// ============================================================================

describe('HU 6.13 — comparar antes y después (QA-03, RN-6-A)', () => {
  const respuesta = (overrides = {}) => ({
    returns: { ytdReturn: 9.75, oneYearReturn: 12.69 },
    totalValueData: { dates: ['2026-11-17'], values: [18240.57] },
    cacheHit: false,
    lastCalculated: '2026-11-18T10:00:00Z',
    validUntil: '2026-11-18T23:59:59Z',
    _metadata: { version: 'snapshot', snapshotLastUpdated: '2026-11-18T06:00:00Z' },
    ...overrides,
  });
  const aviso = { error: { code: 'unavailable', details: { code: 'RETURNS_UNAVAILABLE', pantalla: 'portafolio', motivo: 'sin-resumen' } } };

  test('idéntica aunque cambien la hora del cálculo y la del resumen preparado', () => {
    const antes = { valor: respuesta() };
    const despues = { valor: respuesta({ lastCalculated: '2026-11-18T10:00:05Z', _metadata: { version: 'snapshot', snapshotLastUpdated: '2026-11-18T10:00:05Z' } }) };
    expect(clasificarCombinacion(antes, despues)).toBe('idéntica');
  });

  test('una cifra distinta en el último decimal es diferente', () => {
    const despues = { valor: respuesta({ returns: { ytdReturn: 9.750000001, oneYearReturn: 12.69 } }) };
    expect(clasificarCombinacion({ valor: respuesta() }, despues)).toBe('diferente');
  });

  test('antes la ruta antigua (V2 o su caché), después el aviso: aviso nuevo', () => {
    expect(clasificarCombinacion({ valor: respuesta({ _metadata: { version: 'v2' } }) }, aviso)).toBe('aviso nuevo');
    expect(clasificarCombinacion({ valor: respuesta({ cacheHit: true, _metadata: { version: 'v1' } }) }, aviso)).toBe('aviso nuevo');
  });

  test('antes un resumen y después el aviso no es lo decidido: diferente', () => {
    expect(clasificarCombinacion({ valor: respuesta() }, aviso)).toBe('diferente');
  });

  test('el mismo aviso de moneda antes y después es idéntico', () => {
    const moneda = { error: { code: 'unavailable', details: { code: 'CURRENCY_UNAVAILABLE' } } };
    expect(normalizar(moneda)).toEqual({ error: 'unavailable', aviso: 'CURRENCY_UNAVAILABLE' });
    expect(clasificarCombinacion(moneda, moneda)).toBe('idéntica');
  });
});

// ============================================================================
// Medir y rehacer
// ============================================================================

describe('HU 6.13 — los registros (QA-11, RN-6-I)', () => {
  test('cuenta caídas, avisos, la corrección y los trabajos, antes y después', () => {
    const r = contarRegistros([
      { textPayload: '[queryHandlers][getHistoricalReturnsLegacy] Cache hit' },
      { textPayload: '[queryHandlers][getHistoricalReturnsLegacy] Éxito - version: v2' },
      { textPayload: '[returnsUnavailable] uid-f portafolio overall COP: sin-resumen' },
      { jsonPayload: { message: '[reconcile] CONSOLIDADOS uid-g 2026-11-04..2026-11-25: meses=1 años=0 alcances=3 escrituras=3' } },
      { textPayload: '[reconcile] CONSOLIDADOS uid-g 2026-11-04..2026-11-25: retirado (HU 6.13), escrituras=0' },
      { textPayload: '[reconcile] User uid-g: propagado — 18 meses, 1 años, 6 snapshots borrados' },
      { textPayload: '[consolidateMonthlyPerformance] Retirado (HU 6.13): no se consolida' },
      { textPayload: '[consolidateMonthlyPerformance] Completado {}' },
      { textPayload: 'otra cosa' },
    ]);
    expect(r).toEqual({
      caidasRutaAntigua: 2, avisos: 1, correccionesConsolidando: 1, escriturasDeCorreccion: 3,
      correccionesRetiradas: 1, propagadas: 1, mesesPropagados: 18, añosPropagados: 1,
      trabajosRetirados: 1, trabajosCompletados: 1,
    });
  });
});

describe('HU 6.13 — rehacer para volver atrás (QA-12, RN-6.13-E)', () => {
  test('desde 2025-11, el 2026-12-03: los meses cerrados hasta 2026-11 y el año 2025', () => {
    const ahora = DateTime.fromISO('2026-12-03T10:00:00', { zone: 'America/New_York' });
    const { meses, años } = periodosARehacer('2025-11', ahora, closedMonthsSince);

    expect(meses[0]).toBe('2025-11');
    expect(meses[meses.length - 1]).toBe('2026-11');
    expect(meses).toHaveLength(13);
    expect(años).toEqual(['2025']);
  });

  test('el mes en curso no se rehace', () => {
    const ahora = DateTime.fromISO('2026-12-03T10:00:00', { zone: 'America/New_York' });
    expect(periodosARehacer('2026-12', ahora, closedMonthsSince)).toEqual({ meses: [], años: [] });
  });

  test('parseArgs: --aplicar explícito y --desde con formato AAAA-MM', () => {
    expect(parseArgs(['rehacer', '--uid', 'uid-g', '--desde', '2026-11'])).toMatchObject({ comando: 'rehacer', uids: ['uid-g'], desde: '2026-11', aplicar: false });
    expect(parseArgs(['rehacer', '--aplicar']).aplicar).toBe(true);
    expect(() => parseArgs(['rehacer', '--desde', '2026-11-01'])).toThrow(/AAAA-MM/);
  });
});
