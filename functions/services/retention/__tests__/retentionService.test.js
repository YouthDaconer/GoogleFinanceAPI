/**
 * HU 6.10.1 — La limpieza de datos vencidos, con los datos de prueba de qa.md.
 *
 * Los conjuntos DS-01…DS-05 se reproducen con sus fechas y cantidades. Hay dos
 * ajustes:
 * - Los tipos cuyo id es la fecha (fallos del cierre, métricas) llevan sufijo
 *   (`2026-05-20_0`), para poder tener varios del mismo día.
 * - DS-04 y DS-05 van con 10 inversores en vez de 1.000: las fronteras de
 *   fecha son las mismas y el conteo escala igual.
 *
 * @see services/retention/retentionService.js
 * @see platform-docs/stories/6.10.1-plazos-conservacion-datos/qa.md
 */

jest.mock('firebase-admin/firestore', () => ({ FieldValue: {} }));
jest.mock('../../firebaseAdmin', () => ({
  firestore: { FieldPath: { documentId: () => '__name__' } },
}));

const {
  TAMANO_TANDA,
  MOTIVO_PARADA,
  contarVencidos,
  retirarVencidos,
  activarTipo,
  ejecutarLimpieza,
  leerInforme,
} = require('../retentionService');
const { TIPOS, tipoPorId, corteDe } = require('../retentionPolicies');

// ============================================================================
// FIRESTORE FALSO
// ============================================================================

const clonar = (v) => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
const getPath = (obj, ruta) => ruta.split('.').reduce((o, k) => (o == null ? undefined : o[k]), obj);
function setPath(obj, ruta, valor) {
  const partes = ruta.split('.');
  let o = obj;
  for (const k of partes.slice(0, -1)) {
    if (typeof o[k] !== 'object' || o[k] === null) o[k] = {};
    o = o[k];
  }
  o[partes[partes.length - 1]] = clonar(valor);
}
function mezclar(base, extra) {
  for (const [k, v] of Object.entries(extra)) {
    if (v && typeof v === 'object' && !Array.isArray(v) && base[k] && typeof base[k] === 'object') mezclar(base[k], v);
    else base[k] = clonar(v);
  }
  return base;
}

/**
 * Firestore en memoria: rutas de documento → datos. Soporta lo que usa la
 * limpieza: `where('<')` por campo o por id, `orderBy`, `select`, `startAfter`,
 * `limit`, `count`, `listDocuments` (con padres sin documento), subcolecciones,
 * `batch` y `set` con `merge`/`mergeFields`.
 *
 * @param {Object<string, Object>} inicial - `{ 'coleccion/id': datos }`
 * @param {Object} [opciones]
 * @param {Object} [opciones.reloj] - Para fechar cada escritura de borrado
 */
function crearFirestore(inicial = {}, { reloj = null } = {}) {
  const docs = new Map(Object.entries(inicial).map(([p, d]) => [p, clonar(d)]));
  const borrados = []; // { n, at }
  const fallos = new Set();

  const idDe = (ruta) => ruta.split('/').pop();
  const directos = (col) => [...docs.keys()].filter((p) => p.startsWith(`${col}/`) && !p.slice(col.length + 1).includes('/'));

  function snapDoc(ruta, campos) {
    const datos = docs.get(ruta);
    return {
      id: idDe(ruta),
      ref: ref(ruta),
      exists: datos !== undefined,
      data: () => {
        if (datos === undefined) return undefined;
        if (!campos) return clonar(datos);
        const out = {};
        for (const c of campos) if (getPath(datos, c) !== undefined) setPath(out, c, getPath(datos, c));
        return out;
      },
    };
  }

  function ref(ruta) {
    return {
      id: idDe(ruta),
      path: ruta,
      get: async () => snapDoc(ruta),
      set: async (datos, opciones = {}) => {
        const actual = clonar(docs.get(ruta)) || {};
        if (opciones.mergeFields) {
          for (const f of opciones.mergeFields) setPath(actual, f, getPath(datos, f));
          docs.set(ruta, actual);
        } else if (opciones.merge) docs.set(ruta, mezclar(actual, datos));
        else docs.set(ruta, clonar(datos));
      },
      delete: async () => {
        docs.delete(ruta);
        borrados.push({ n: 1, at: reloj ? reloj.actual() : null });
      },
      collection: (sub) => coleccion(`${ruta}/${sub}`),
    };
  }

  function coleccion(col) {
    const e = { filtros: [], orden: null, campos: null, despuesDe: null, limite: Infinity };
    const valorDe = (p, campo) => (campo === '__name__' ? idDe(p) : getPath(docs.get(p), campo));
    const q = {
      where(campo, op, valor) {
        if (op !== '<') throw new Error(`operador no soportado: ${op}`);
        e.filtros.push({ campo, valor });
        return q;
      },
      orderBy(campo, dir = 'asc') {
        e.orden = { campo, dir };
        return q;
      },
      select(...campos) {
        e.campos = campos;
        return q;
      },
      startAfter(cursor) {
        e.despuesDe = cursor;
        return q;
      },
      limit(n) {
        e.limite = n;
        return q;
      },
      async get() {
        if (fallos.has(col)) throw new Error(`lectura fallida de ${col}`);
        let rutas = directos(col).filter((p) => e.filtros.every(({ campo, valor }) => {
          const v = valorDe(p, campo);
          return v !== undefined && typeof v === typeof valor && v < valor;
        }));
        const campoOrden = e.orden ? e.orden.campo : '__name__';
        rutas.sort((a, b) => {
          const [x, y] = [valorDe(a, campoOrden), valorDe(b, campoOrden)];
          if (x !== y) return x < y ? -1 : 1;
          return idDe(a) < idDe(b) ? -1 : 1;
        });
        if (e.orden && e.orden.dir === 'desc') rutas.reverse();
        if (e.despuesDe) rutas = rutas.slice(rutas.findIndex((p) => idDe(p) === e.despuesDe.id) + 1);
        const snaps = rutas.slice(0, e.limite).map((p) => snapDoc(p, e.campos));
        return { docs: snaps, size: snaps.length, empty: snaps.length === 0 };
      },
      count() {
        return {
          get: async () => {
            const { size } = await q.get();
            return { data: () => ({ count: size }) };
          },
        };
      },
      async listDocuments() {
        const ids = new Set();
        for (const p of docs.keys()) if (p.startsWith(`${col}/`)) ids.add(p.slice(col.length + 1).split('/')[0]);
        return [...ids].map((id) => ref(`${col}/${id}`));
      },
      doc: (id) => ref(`${col}/${id}`),
    };
    return q;
  }

  const db = {
    collection: coleccion,
    batch() {
      const rutas = [];
      return {
        delete: (r) => rutas.push(r.path),
        commit: async () => {
          rutas.forEach((p) => docs.delete(p));
          borrados.push({ n: rutas.length, at: reloj ? reloj.actual() : null });
        },
      };
    },
  };

  /** Rutas bajo un prefijo, para comprobar qué quedó. */
  const bajo = (prefijo) => [...docs.keys()].filter((p) => p.startsWith(prefijo)).sort();

  return { db, docs, borrados, fallos, bajo };
}

/** Reloj que avanza `pasoMs` cada vez que alguien pregunta la hora. */
function crearReloj(inicio, pasoMs = 0) {
  let ms = new Date(inicio).getTime();
  return {
    ahora: () => {
      const t = new Date(ms);
      ms += pasoMs;
      return t;
    },
    actual: () => new Date(ms),
  };
}

// ============================================================================
// DATOS DE PRUEBA (qa.md)
// ============================================================================

/** La limpieza bajo prueba: 2026-11-12, 06:00 ET. */
const LIMPIEZA = '2026-11-12T11:00:00Z';

function porFechas(spec, crear) {
  for (const [fecha, n] of Object.entries(spec)) for (let i = 0; i < n; i++) crear(fecha, i);
}

/** DS-01: datos con plazo nuevo (fallos de inversores que no están en revisión). */
function ds01() {
  const d = {};
  porFechas({ '2026-05-20': 12, '2026-08-13': 3, '2026-08-14': 2, '2026-08-15': 1 }, (f, i) => {
    d[`eodFailures/${f}_${i}`] = { date: f, totalFuera: 1 };
  });
  porFechas({ '2026-05-20': 5, '2026-08-13': 1, '2026-08-14': 1, '2026-08-15': 1 }, (f, i) => {
    d[`reconcileFailures/uid-liberado-${f}-${i}`] = {
      userId: `uid-liberado-${f}-${i}`,
      abandonedAt: '2026-05-01T06:00:00.000Z',
      resolvedAt: `${f}T15:00:00.000Z`,
      liberadoAt: `${f}T15:00:00.000Z`,
    };
  });
  porFechas({ '2026-05-20': 2, '2026-08-13': 2, '2026-08-14': 2, '2026-08-15': 2 }, (f, i) => {
    d[`reconcileNights/${f}_02000${i}`] = { noche: f, corregidos: 3 };
  });
  porFechas({ '2024-06-03': 10, '2025-11-11': 4, '2025-11-12': 4, '2025-11-13': 4 }, (f, i) => {
    // Como lo escribe briefing_compiler.py: isoformat de Python con +00:00
    d[`briefingHistory/b-${f}-${i}`] = { userId: 'uid-inversor-B', type: 'morning', sentAt: `${f}T13:30:00.123456+00:00` };
  });
  porFechas({ '2023-09-01': 2, '2024-11-11': 1, '2024-11-12': 1 }, (f, i) => {
    d[`archivedSnapshots/uid-arch-${f}-${i}_overall_USD`] = { userId: `uid-arch-${f}-${i}`, archivedAt: `${f}T08:00:00.000Z` };
  });
  // INV-G: archivado el 2023-09-01 (se retira); INV-K: archivado el 2024-11-13 (se conserva)
  d['archivedSnapshots/uid-inversor-inactivo-G_overall_USD'] = { userId: 'uid-inversor-inactivo-G', archivedAt: '2023-09-01T08:00:00.000Z' };
  d['archivedSnapshots/uid-inversor-inactivo-K_overall_USD'] = { userId: 'uid-inversor-inactivo-K', archivedAt: '2024-11-13T09:00:00.000Z' };
  return d;
}

/** DS-02: lo que se conserva siempre. */
function ds02() {
  return {
    'subscriptionEvents/e-alta': { userId: 'uid-inversor-J', type: 'created', createdAt: '2022-07-04T10:00:00.000Z' },
    'subscriptionEvents/e-renovacion': { userId: 'uid-inversor-J', type: 'renewed', createdAt: '2023-07-04T10:00:00.000Z' },
    'subscriptionEvents/e-cancelacion': { userId: 'uid-inversor-J', type: 'canceled', createdAt: '2024-02-10T10:00:00.000Z' },
    'userData/uid-inversor-J': { subscription: { plan: 'pro', lastPayment: { amount: 9.99, currency: 'USD', at: '2023-01-15' } } },
    'indexHistories/^GSPC': { from: '2015-01-02' },
    'currentPrices/AAPL': { since: '2015-01-02' },
    'sectorsHistory/technology': { from: '2018-01-02' },
    'portfolioPerformance/uid-inversor-inactivo-G': { userId: 'uid-inversor-inactivo-G' },
    'portfolioPerformance/uid-inversor-inactivo-G/dates/2021-03-01': { date: '2021-03-01' },
    'portfolioPerformance/uid-inversor-inactivo-G/dates/2026-11-11': { date: '2026-11-11' },
  };
}

/** DS-03: los plazos que ya existían, que esta limpieza no toca. */
function ds03() {
  return {
    'userNotifications/n-60d': { createdAt: '2026-09-13T12:00:00.000Z' },
    'userNotifications/n-91d': { createdAt: '2026-08-13T12:00:00.000Z' },
    'alertHistory/a-200d': { triggeredAt: '2026-04-26T12:00:00.000Z' },
    '_backfillSnapshots/b-20d': { ttlExpiry: '2026-11-22T12:00:00.000Z' },
    'performanceSnapshots/uid-inversor-H_NFLX_USD': { type: 'asset', ticker: 'NFLX' },
  };
}

/** Progreso del cierre repartido: `inversores` registros por día entre dos fechas. */
function progreso(desde, hasta, inversores) {
  const d = {};
  for (let t = new Date(`${desde}T12:00:00Z`); t <= new Date(`${hasta}T12:00:00Z`); t.setUTCDate(t.getUTCDate() + 1)) {
    const f = t.toISOString().slice(0, 10);
    d[`eodProgress/${f}`] = { date: f, totalUsers: inversores, estado: 'completo' };
    for (let i = 1; i <= inversores; i++) {
      d[`eodProgress/${f}/usuarios/uid-prueba-${String(i).padStart(4, '0')}`] = { estado: 'completado' };
    }
  }
  return d;
}

const contarDias = (desde, hasta) => Math.round((new Date(hasta) - new Date(desde)) / 86400000) + 1;

/** Ensaya y activa todos los tipos, como haría operación la primera vez. */
async function ensayarYActivarTodo(db, ahora) {
  await ejecutarLimpieza(db, { ahora: () => new Date(ahora), soloEnsayo: true });
  for (const tipo of TIPOS) {
    const r = await activarTipo(db, tipo.id, { aplicar: true, quien: 'ops-guardia@ejemplo.com', ahora: () => new Date(ahora) });
    expect(r.estado).toBe('activado');
  }
}

/** Lo que nunca debe cambiar: DS-02 y DS-03, tal como estaban. */
function inventarioIntocable(fs) {
  const prefijos = ['subscriptionEvents/', 'userData/', 'indexHistories/', 'currentPrices/', 'sectorsHistory/',
    'portfolioPerformance/', 'userNotifications/', 'alertHistory/', '_backfillSnapshots/', 'performanceSnapshots/'];
  return Object.fromEntries(prefijos.flatMap((p) => fs.bajo(p)).map((r) => [r, fs.docs.get(r)]));
}

beforeEach(() => {
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => jest.restoreAllMocks());

// ============================================================================
// ESCENARIO 4 — LA PRIMERA LIMPIEZA SE ENSAYA
// ============================================================================

describe('Escenario 4 — la primera limpieza de cada tipo es un ensayo en seco (RN-6.10-O)', () => {
  it('QA-08: cuenta por tipo y mes lo que retiraría, no retira nada y el relanzamiento da lo mismo', async () => {
    const fs = crearFirestore({ ...ds01(), ...ds02() });
    const antes = new Map(fs.docs);

    const primera = await ejecutarLimpieza(fs.db, { ahora: () => new Date(LIMPIEZA) });

    const registros = (c) => Object.fromEntries(Object.entries(c.tipos).map(([id, t]) => [id, t.registros]));
    expect(registros(primera)).toEqual({
      fallosCierre: 15,
      fallosCorreccion: 6,
      metricasCorreccion: 4,
      constancias: 0,
      briefings: 14,
      resumenesArchivados: 4,
      preciosCierre: 0,
      progresoCierre: 0,
    });
    expect(Object.values(primera.tipos).every((t) => t.modo === 'ensayo')).toBe(true);
    expect(primera.tipos.fallosCierre.porTramo).toEqual({ '2026-05': 12, '2026-08': 3 });
    expect(primera.tipos.briefings.porTramo).toEqual({ '2024-06': 10, '2025-11': 4 });
    expect(primera.tipos.resumenesArchivados.porTramo).toEqual({ '2023-09': 3, '2024-11': 1 });

    // Nada retirado: solo apareció la constancia (y la configuración con el ensayo)
    const nuevos = [...fs.docs.keys()].filter((p) => !antes.has(p));
    expect(nuevos.sort()).toEqual(['dataRetentionPolicies/config', 'dataRetentionRuns/2026-11-12_060000']);
    for (const [ruta, datos] of antes) expect(fs.docs.get(ruta)).toEqual(datos);
    expect(fs.borrados).toEqual([]);

    const segunda = await ejecutarLimpieza(fs.db, { ahora: () => new Date('2026-11-12T11:30:00Z') });
    expect(registros(segunda)).toEqual(registros(primera));
    expect(fs.borrados).toEqual([]);
  });

  it('el ensayo queda en la configuración, y el siguiente reemplaza sus meses en vez de mezclarlos', async () => {
    const fs = crearFirestore({ 'eodFailures/2026-05-20': { date: '2026-05-20' } });
    await ejecutarLimpieza(fs.db, { ahora: () => new Date(LIMPIEZA) });
    expect(fs.docs.get('dataRetentionPolicies/config').ultimosEnsayos.fallosCierre).toEqual({
      runId: '2026-11-12_060000',
      at: '2026-11-12T11:00:00.000Z',
      corte: '2026-08-14',
      registros: 1,
      porTramo: { '2026-05': 1 },
      completo: true,
    });

    fs.docs.delete('eodFailures/2026-05-20');
    fs.docs.set('eodFailures/2026-06-02', { date: '2026-06-02' });
    await ejecutarLimpieza(fs.db, { ahora: () => new Date('2026-11-13T11:00:00Z') });
    expect(fs.docs.get('dataRetentionPolicies/config').ultimosEnsayos.fallosCierre.porTramo).toEqual({ '2026-06': 1 });
  });

  it('QA-09: sin revisar su ensayo no se retira nada de ese tipo; sin ensayo previo solo se ensaya', async () => {
    const fs = crearFirestore(ds01());
    // Briefings: ensayo hecho y activado. Resúmenes archivados: ensayo hecho, sin activar.
    await ejecutarLimpieza(fs.db, { ahora: () => new Date('2026-11-11T11:00:00Z'), soloEnsayo: true });
    await activarTipo(fs.db, 'briefings', { aplicar: true });
    // Métricas internas: sin ninguna ejecución previa
    const config = fs.docs.get('dataRetentionPolicies/config');
    delete config.ultimosEnsayos.metricasCorreccion;
    fs.docs.set('dataRetentionPolicies/config', config);

    const c = await ejecutarLimpieza(fs.db, { ahora: () => new Date(LIMPIEZA) });

    expect(c.tipos.briefings).toMatchObject({ modo: 'retirada', registros: 14, completo: true });
    expect(c.tipos.resumenesArchivados).toMatchObject({ modo: 'ensayo', registros: 4 });
    expect(c.tipos.metricasCorreccion).toMatchObject({ modo: 'ensayo', registros: 4 });
    expect(fs.bajo('briefingHistory/')).toHaveLength(8);
    expect(fs.bajo('archivedSnapshots/')).toHaveLength(6);
    expect(fs.bajo('reconcileNights/')).toHaveLength(8);
  });
});

describe('activarTipo — operación activa un tipo después de revisar su ensayo', () => {
  it('no activa un tipo sin ensayo ni uno desconocido', async () => {
    const fs = crearFirestore();
    expect(await activarTipo(fs.db, 'briefings', { aplicar: true })).toEqual({ estado: 'sin-ensayo' });
    expect(await activarTipo(fs.db, 'pagos', { aplicar: true })).toEqual({ estado: 'tipo-desconocido' });
    expect(fs.docs.has('dataRetentionPolicies/config')).toBe(false);
  });

  it('no activa con un ensayo que no terminó', async () => {
    const fs = crearFirestore({
      'dataRetentionPolicies/config': { ultimosEnsayos: { briefings: { runId: 'r1', completo: false } } },
    });
    expect((await activarTipo(fs.db, 'briefings', { aplicar: true })).estado).toBe('sin-ensayo');
  });

  it('sin --aplicar solo simula; con --aplicar deja quién, cuándo y qué ensayo revisó; la segunda vez no hace nada', async () => {
    const fs = crearFirestore({ ...ds01() });
    await ejecutarLimpieza(fs.db, { ahora: () => new Date(LIMPIEZA), soloEnsayo: true });

    const simulado = await activarTipo(fs.db, 'briefings');
    expect(simulado.estado).toBe('simulado');
    expect(simulado.ensayo.registros).toBe(14);
    expect(fs.docs.get('dataRetentionPolicies/config').tipos).toBeUndefined();

    const activado = await activarTipo(fs.db, 'briefings', {
      aplicar: true, quien: 'ops-guardia@ejemplo.com', ahora: () => new Date('2026-11-12T15:00:00Z'),
    });
    expect(activado.estado).toBe('activado');
    expect(fs.docs.get('dataRetentionPolicies/config').tipos.briefings).toEqual({
      activo: true,
      activadoAt: '2026-11-12T15:00:00.000Z',
      activadoPor: 'ops-guardia@ejemplo.com',
      ensayoRevisado: '2026-11-12_060000',
    });
    expect(fs.docs.get('dataRetentionPolicies/config').ultimosEnsayos.briefings.registros).toBe(14);

    expect((await activarTipo(fs.db, 'briefings', { aplicar: true })).estado).toBe('ya-activo');
  });
});

// ============================================================================
// ESCENARIOS 1, 2 Y 6 — CADA DATO RESPETA SU PLAZO
// ============================================================================

describe('Escenarios 1 y 2 — los vencidos se retiran y cada dato respeta su plazo', () => {
  let fs;
  let constancia;
  let intocableAntes;

  beforeAll(async () => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
    fs = crearFirestore({ ...ds01(), ...ds02(), ...ds03() });
    intocableAntes = inventarioIntocable(fs);
    await ensayarYActivarTodo(fs.db, '2026-11-11T11:00:00Z');
    constancia = await ejecutarLimpieza(fs.db, {
      ahora: () => new Date(LIMPIEZA),
      medirEspacio: async () => 380 * 1024 * 1024,
    });
    console.log.mockRestore();
  });

  it('QA-01: retira los registros operativos de más de 90 días y conserva los de 90 o menos', () => {
    expect(fs.bajo('eodFailures/').map((p) => p.split('/')[1].slice(0, 10)).sort())
      .toEqual(['2026-08-14', '2026-08-14', '2026-08-15']);
    expect(fs.bajo('reconcileFailures/').map((p) => fs.docs.get(p).resolvedAt.slice(0, 10)).sort())
      .toEqual(['2026-08-14', '2026-08-15']);
    expect(fs.bajo('reconcileNights/').map((p) => p.split('/')[1].slice(0, 10)))
      .toEqual(['2026-08-14', '2026-08-14', '2026-08-15', '2026-08-15']);
  });

  it('QA-03: retira 14 briefings de más de 1 año y conserva los 8 de 1 año o menos', () => {
    expect(constancia.tipos.briefings.registros).toBe(14);
    expect(fs.bajo('briefingHistory/').map((p) => fs.docs.get(p).sentAt.slice(0, 10)).sort())
      .toEqual(['2025-11-12', '2025-11-12', '2025-11-12', '2025-11-12', '2025-11-13', '2025-11-13', '2025-11-13', '2025-11-13']);
  });

  it('QA-04: retira los archivados hace más de 2 años, incluido INV-G, y conserva INV-K aunque no entra hace 3 años', () => {
    expect(constancia.tipos.resumenesArchivados.registros).toBe(4);
    expect(fs.bajo('archivedSnapshots/')).toEqual([
      'archivedSnapshots/uid-arch-2024-11-12-0_overall_USD',
      'archivedSnapshots/uid-inversor-inactivo-K_overall_USD',
    ]);
  });

  it('QA-02: la constancia dice cuántos se retiraron de cada tipo y guarda el espacio medido', () => {
    const guardada = fs.docs.get('dataRetentionRuns/2026-11-12_060000');
    expect(guardada).toEqual(constancia);
    expect(guardada.estado).toBe('completa');
    expect(guardada.almacenamientoBytes).toBe(380 * 1024 * 1024);
    expect(Object.fromEntries(Object.entries(guardada.tipos).map(([id, t]) => [id, [t.modo, t.registros]]))).toEqual({
      fallosCierre: ['retirada', 15],
      fallosCorreccion: ['retirada', 6],
      metricasCorreccion: ['retirada', 4],
      constancias: ['retirada', 0],
      briefings: ['retirada', 14],
      resumenesArchivados: ['retirada', 4],
      preciosCierre: ['retirada', 0],
      progresoCierre: ['retirada', 0],
    });
  });

  it('QA-05: pagos, suscripciones, historiales de mercado y datos de origen siguen intactos', () => {
    expect(inventarioIntocable(fs)).toEqual(intocableAntes);
    expect(Object.keys(constancia.tipos)).not.toEqual(expect.arrayContaining(['subscriptionEvents']));
  });

  it('QA-06: notificaciones, alertas, respaldos y resúmenes de activos vendidos no los toca esta limpieza', () => {
    expect(fs.docs.has('userNotifications/n-60d')).toBe(true);
    // La de 91 días la retira alertRetention.js con su plazo de siempre, no esta limpieza
    expect(fs.docs.has('userNotifications/n-91d')).toBe(true);
    expect(fs.docs.has('alertHistory/a-200d')).toBe(true);
    expect(fs.docs.has('_backfillSnapshots/b-20d')).toBe(true);
    expect(fs.docs.has('performanceSnapshots/uid-inversor-H_NFLX_USD')).toBe(true);
  });

  it('QA-13: relanzarla justo después no retira nada más y su constancia dice 0 en cada tipo', async () => {
    const antes = [...fs.docs.keys()].filter((p) => !p.startsWith('dataRetention')).sort();
    const otra = await ejecutarLimpieza(fs.db, { ahora: () => new Date('2026-11-12T12:00:00Z') });
    expect(Object.values(otra.tipos).map((t) => t.registros)).toEqual(TIPOS.map(() => 0));
    expect([...fs.docs.keys()].filter((p) => !p.startsWith('dataRetention')).sort()).toEqual(antes);
  });
});

describe('Escenario 6 — los motivos de un inversor en revisión no se pierden (RN-6.10.1-K)', () => {
  /** INV-H en revisión desde el 2026-07-20; INV-I con las mismas fechas, ya liberado (control). */
  function revision() {
    const intentos = ['2026-07-18', '2026-07-19', '2026-07-20'].map((noche) => ({ motivo: 'memoria', noche }));
    return {
      'reconcileFailures/uid-inversor-H': {
        userId: 'uid-inversor-H', intentos, pendienteDesde: '2026-07-10T12:00:00.000Z', abandonedAt: '2026-07-20T06:09:00.000Z',
      },
      'reconcileFailures/uid-inversor-I': {
        userId: 'uid-inversor-I', intentos, abandonedAt: '2026-07-20T06:09:00.000Z',
        resolvedAt: '2026-07-20T15:00:00.000Z', liberadoAt: '2026-07-20T15:00:00.000Z',
      },
    };
  }

  async function correrEl(fs, iso) {
    return ejecutarLimpieza(fs.db, { ahora: () => new Date(iso) });
  }

  it('QA-14: los fallos de INV-H se conservan con sus motivos; los de INV-I se retiran', async () => {
    const fs = crearFirestore(revision());
    await ensayarYActivarTodo(fs.db, '2026-11-11T11:00:00Z');
    const c = await correrEl(fs, LIMPIEZA);

    expect(fs.bajo('reconcileFailures/')).toEqual(['reconcileFailures/uid-inversor-H']);
    expect(fs.docs.get('reconcileFailures/uid-inversor-H').intentos).toHaveLength(3);
    expect(c.tipos.fallosCorreccion.registros).toBe(1);
  });

  it('QA-15: liberado el 2026-11-12, se conserva el 2027-02-09 y el 2027-02-10 y se retira el 2027-02-11', async () => {
    const fs = crearFirestore(revision());
    await ensayarYActivarTodo(fs.db, '2026-11-11T11:00:00Z');
    await correrEl(fs, LIMPIEZA);
    // Operación lo libera el 2026-11-12, después de la limpieza
    const h = fs.docs.get('reconcileFailures/uid-inversor-H');
    fs.docs.set('reconcileFailures/uid-inversor-H', { ...h, resolvedAt: '2026-11-12T15:00:00.000Z', liberadoAt: '2026-11-12T15:00:00.000Z' });

    await correrEl(fs, '2027-02-09T11:00:00Z');
    expect(fs.docs.has('reconcileFailures/uid-inversor-H')).toBe(true);
    await correrEl(fs, '2027-02-10T11:00:00Z');
    expect(fs.docs.has('reconcileFailures/uid-inversor-H')).toBe(true);
    const c = await correrEl(fs, '2027-02-11T11:00:00Z');
    expect(fs.docs.has('reconcileFailures/uid-inversor-H')).toBe(false);
    expect(c.tipos.fallosCorreccion).toMatchObject({ modo: 'retirada', registros: 1, porTramo: { '2026-11': 1 } });
  });
});

// ============================================================================
// ESCENARIOS 5 Y 7 — VOLUMEN GRANDE Y CIERRE REPARTIDO
// ============================================================================

describe('Escenario 7 — el progreso del cierre repartido caduca a los 90 días', () => {
  it('QA-16: de DS-04 se retiran los días del 2026-07-16 al 2026-08-13 y se conservan los 91 siguientes', async () => {
    const fs = crearFirestore(progreso('2026-07-16', '2026-11-12', 10));
    await ensayarYActivarTodo(fs.db, '2026-11-11T11:00:00Z');
    const c = await ejecutarLimpieza(fs.db, { ahora: () => new Date(LIMPIEZA) });

    // 29 días × 10 inversores (en qa.md, × 1.000 = 29.000)
    expect(c.tipos.progresoCierre).toMatchObject({ modo: 'retirada', registros: 290, documentos: 290 + 29, completo: true });
    expect(c.tipos.progresoCierre.porTramo).toEqual({ '2026-07': 160, '2026-08': 130 });
    const dias = fs.bajo('eodProgress/').filter((p) => p.split('/').length === 2);
    expect(dias).toHaveLength(91);
    expect(dias[0]).toBe('eodProgress/2026-08-14');
    expect(fs.bajo('eodProgress/').filter((p) => p.includes('/usuarios/'))).toHaveLength(910);
  });

  it('el ensayo cuenta los registros por inversor, no los días', async () => {
    const fs = crearFirestore(progreso('2026-07-16', '2026-11-12', 10));
    const tipo = tipoPorId('progresoCierre');
    const r = await contarVencidos(fs.db, tipo, corteDe(tipo, new Date(LIMPIEZA)));
    expect(r).toMatchObject({ registros: 290, documentos: 319, completo: true });
  });
});

describe('Escenario 5 — mucho por retirar no afecta al servicio (RN-6.10.1-J, RN-6-H)', () => {
  const VENCIDOS = contarDias('2025-11-13', '2026-08-13') * 10; // 274 días × 10
  const VIGENTES = contarDias('2026-08-14', '2026-11-12') * 10; // 91 días × 10

  it('QA-10: con DS-05 avanza en varias tandas y al terminar retira todos los vencidos y ninguno vigente', async () => {
    expect([VENCIDOS, VIGENTES]).toEqual([2740, 910]);
    const fs = crearFirestore(progreso('2025-11-13', '2026-11-12', 10));
    await ensayarYActivarTodo(fs.db, '2026-11-11T11:00:00Z');

    const c = await ejecutarLimpieza(fs.db, { ahora: () => new Date(LIMPIEZA) });

    expect(c.tipos.progresoCierre).toMatchObject({ registros: VENCIDOS, completo: true });
    expect(c.tipos.progresoCierre.tandas).toBeGreaterThan(1);
    expect(fs.borrados.every((b) => b.n <= TAMANO_TANDA)).toBe(true);
    expect(fs.bajo('eodProgress/').filter((p) => p.includes('/usuarios/'))).toHaveLength(VIGENTES);
  });

  it('QA-10: ninguna tanda empieza dentro del horario nocturno; la ejecución se detiene al llegar a él', async () => {
    const datos = progreso('2025-11-13', '2026-11-12', 10);
    const fs0 = crearFirestore(datos);
    await ensayarYActivarTodo(fs0.db, '2026-11-11T11:00:00Z');
    // Arranca a las 23:58:30 ET y el reloj avanza 1 s por consulta: llega a medianoche a mitad
    const reloj = crearReloj('2026-11-12T23:58:30-05:00', 1000);
    const fs = crearFirestore(Object.fromEntries(fs0.docs), { reloj });

    const c = await ejecutarLimpieza(fs.db, { ahora: reloj.ahora, presupuestoMs: 3600 * 1000 });

    expect(c.estado).toBe('parcial');
    expect(c.detenidoPor).toBe(MOTIVO_PARADA.HORARIO_NOCTURNO);
    // Toda escritura de borrado ocurrió antes de las 00:00 ET (con un segundo de gracia por la propia consulta)
    for (const b of fs.borrados) expect(b.at.getTime()).toBeLessThanOrEqual(new Date('2026-11-13T00:00:01-05:00').getTime());
    expect(fs.borrados.length).toBeGreaterThan(0);
  });

  it('lanzada a mano dentro del horario nocturno, no arranca ni retira nada', async () => {
    const fs = crearFirestore(ds01());
    await ensayarYActivarTodo(fs.db, '2026-11-11T11:00:00Z');
    const antes = fs.bajo('eodFailures/');

    const c = await ejecutarLimpieza(fs.db, { ahora: () => new Date('2026-11-12T01:30:00-05:00') });

    expect(c).toMatchObject({ estado: 'no-iniciada', detenidoPor: 'horario-nocturno', tipos: {} });
    expect(fs.bajo('eodFailures/')).toEqual(antes);
    expect(fs.docs.get(`dataRetentionRuns/${c.id}`).estado).toBe('no-iniciada');
  });

  it('se detiene al agotar el presupuesto y deja constancia de lo hecho hasta entonces', async () => {
    const fs0 = crearFirestore(progreso('2025-11-13', '2026-11-12', 10));
    await ensayarYActivarTodo(fs0.db, '2026-11-11T11:00:00Z');
    const reloj = crearReloj(LIMPIEZA, 1000);
    const fs = crearFirestore(Object.fromEntries(fs0.docs), { reloj });

    const c = await ejecutarLimpieza(fs.db, { ahora: reloj.ahora, presupuestoMs: 120 * 1000 });

    expect(c).toMatchObject({ estado: 'parcial', detenidoPor: 'presupuesto' });
    const retirados = c.tipos.progresoCierre.registros;
    expect(retirados).toBeGreaterThan(0);
    expect(retirados).toBeLessThan(VENCIDOS);
    expect(fs.bajo('eodProgress/').filter((p) => p.includes('/usuarios/'))).toHaveLength(VENCIDOS + VIGENTES - retirados);
    expect(fs.docs.get(`dataRetentionRuns/${c.id}`).tipos.progresoCierre.registros).toBe(retirados);
  });

  it('QA-11: detenida tras la tercera tanda no deja nada a medias y la siguiente continúa hasta sumar todo', async () => {
    const fs = crearFirestore(progreso('2025-11-13', '2026-11-12', 10));
    const tipo = tipoPorId('progresoCierre');
    const corte = corteDe(tipo, new Date(LIMPIEZA));
    const constancias = [];

    let tandas = 0;
    const primera = await retirarVencidos(fs.db, tipo, corte, {
      debeParar: () => (tandas >= 3 ? MOTIVO_PARADA.PRESUPUESTO : null),
      alTerminarTanda: async (avance) => {
        tandas = avance.tandas;
        constancias[0] = avance;
      },
    });

    expect(primera).toMatchObject({ completo: false, detenidoPor: 'presupuesto', tandas: 3, registros: 30 });
    expect(constancias[0].registros).toBe(30);
    // Cada día queda retirado entero o intacto; ningún vigente se tocó
    const usuariosPorDia = {};
    for (const p of fs.bajo('eodProgress/').filter((r) => r.includes('/usuarios/'))) {
      const dia = p.split('/')[1];
      usuariosPorDia[dia] = (usuariosPorDia[dia] || 0) + 1;
    }
    expect(Object.values(usuariosPorDia).every((n) => n === 10)).toBe(true);
    expect(Object.keys(usuariosPorDia).filter((d) => d >= '2026-08-14')).toHaveLength(91);

    const segunda = await retirarVencidos(fs.db, tipo, corte);
    expect(segunda.completo).toBe(true);
    expect(primera.registros + segunda.registros).toBe(VENCIDOS);
    expect(fs.bajo('eodProgress/').filter((p) => p.includes('/usuarios/'))).toHaveLength(VIGENTES);
    expect(fs.bajo('eodProgress/').filter((p) => p.split('/').length === 2)).toHaveLength(91);
  });
});

// ============================================================================
// PRECIOS DEL CIERRE, FALLOS Y MEDICIÓN
// ============================================================================

describe('preciosCierre — los precios congelados de cada noche (decisión del PO, 90 días)', () => {
  it('retira el día vencido con sus trozos, también si quedó sin raíz, y conserva el vigente', async () => {
    const fs = crearFirestore({
      'marketData/eod/dates/2026-05-20': { date: '2026-05-20', estado: 'listo', trozos: 2 },
      'marketData/eod/dates/2026-05-20/chunks/0': { indice: 0, prices: [] },
      'marketData/eod/dates/2026-05-20/chunks/1': { indice: 1, prices: [] },
      // Un día a medias: trozos escritos, raíz nunca escrita
      'marketData/eod/dates/2026-06-03/chunks/0': { indice: 0, prices: [] },
      'marketData/eod/dates/2026-11-11': { date: '2026-11-11', estado: 'listo', trozos: 1 },
      'marketData/eod/dates/2026-11-11/chunks/0': { indice: 0, prices: [] },
      'marketData/eod': { tipo: 'raiz' },
    });
    await ensayarYActivarTodo(fs.db, '2026-11-11T11:00:00Z');

    const c = await ejecutarLimpieza(fs.db, { ahora: () => new Date(LIMPIEZA) });

    expect(c.tipos.preciosCierre).toMatchObject({ registros: 2, documentos: 5, porTramo: { '2026-05': 1, '2026-06': 1 } });
    expect(fs.bajo('marketData/')).toEqual([
      'marketData/eod',
      'marketData/eod/dates/2026-11-11',
      'marketData/eod/dates/2026-11-11/chunks/0',
    ]);
  });
});

describe('fallos y medición — la limpieza nunca se cae entera', () => {
  it('si un tipo falla, lo anota y sigue con los demás', async () => {
    const fs = crearFirestore(ds01());
    await ensayarYActivarTodo(fs.db, '2026-11-11T11:00:00Z');
    fs.fallos.add('reconcileNights');

    const c = await ejecutarLimpieza(fs.db, { ahora: () => new Date(LIMPIEZA) });

    expect(c.estado).toBe('con-errores');
    expect(c.tipos.metricasCorreccion.error).toBe('lectura fallida de reconcileNights');
    expect(c.tipos.briefings.registros).toBe(14);
  });

  it('si no se puede medir el espacio, la constancia lo deja en null y la limpieza sigue', async () => {
    const fs = crearFirestore(ds01());
    const c = await ejecutarLimpieza(fs.db, {
      ahora: () => new Date(LIMPIEZA),
      medirEspacio: async () => { throw new Error('Monitoring 403'); },
    });
    expect(c.almacenamientoBytes).toBeNull();
    expect(c.estado).toBe('completa');
  });
});

describe('leerInforme — RN-6-I, el espacio liberado sale de la medición de la plataforma', () => {
  it('devuelve las constancias en orden con la diferencia frente a la medición anterior', async () => {
    const MiB = 1024 * 1024;
    const fs = crearFirestore({
      'dataRetentionRuns/2026-11-12_060000': { id: '2026-11-12_060000', almacenamientoBytes: 380 * MiB },
      'dataRetentionRuns/2026-11-13_060000': { id: '2026-11-13_060000', almacenamientoBytes: null },
      'dataRetentionRuns/2026-11-14_060000': { id: '2026-11-14_060000', almacenamientoBytes: 350 * MiB },
      'dataRetentionPolicies/config': { tipos: { briefings: { activo: true } } },
    });

    const { config, constancias } = await leerInforme(fs.db);

    expect(config.tipos.briefings.activo).toBe(true);
    expect(constancias.map((c) => [c.id, c.liberadoBytes])).toEqual([
      ['2026-11-12_060000', null],
      ['2026-11-13_060000', null],
      ['2026-11-14_060000', 30 * MiB],
    ]);
  });
});
