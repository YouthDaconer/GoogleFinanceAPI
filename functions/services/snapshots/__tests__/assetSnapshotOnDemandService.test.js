/**
 * HU 6.4 — Resúmenes por activo solo cuando se piden.
 *
 * Firestore en memoria que aplica de verdad los filtros, el orden y la
 * selección de campos (`select`), para comprobar con el cálculo real que:
 * - poner al día al abrir da las mismas cifras que las noches (RN-6.4-N);
 * - leer solo los campos del activo da el mismo resumen (T3b);
 * - la retirada solo toca activos que el inversor ya no tiene (RN-6.4-M).
 *
 * @see platform-docs/stories/6.4-resumenes-activo-bajo-demanda/refinamiento.md (T8b, T8c)
 */

jest.mock('firebase-admin/firestore', () => {
  class FieldPath {
    constructor(...segments) { this.segments = segments; }
    static documentId() { return '__name__'; }
  }
  return { FieldPath };
});

const { DateTime } = require('luxon');
const {
  generateAssetSnapshot,
  fetchAssetDailyDocs,
} = require('../../snapshotGenerator');
const { aplicarDiaAResumenDeActivo } = require('../../snapshotIncrementalService');
const {
  resumenesPorActivoBajoDemanda,
  monedasDelCierre,
  ponerAlDiaResumenDeActivo,
  retirarResumenesSinTenencia,
} = require('../assetSnapshotOnDemandService');

// ============================================================================
// Firestore en memoria
// ============================================================================

const copia = (v) => (v === undefined ? v : JSON.parse(JSON.stringify(v)));

function leerRuta(data, segmentos) {
  return segmentos.reduce((acc, s) => (acc == null ? undefined : acc[s]), data);
}

function escribirRuta(destino, segmentos, valor) {
  let actual = destino;
  segmentos.slice(0, -1).forEach((s) => {
    if (!actual[s]) actual[s] = {};
    actual = actual[s];
  });
  actual[segmentos[segmentos.length - 1]] = copia(valor);
}

function proyectar(data, campos) {
  if (!campos) return copia(data);
  const resultado = {};
  campos.forEach((c) => {
    const segmentos = typeof c === 'string' ? [c] : c.segments;
    const valor = leerRuta(data, segmentos);
    if (valor !== undefined) escribirRuta(resultado, segmentos, valor);
  });
  return resultado;
}

function crearFirestore(inicial = {}) {
  const datos = new Map(Object.entries(inicial).map(([p, docs]) => [p, new Map(Object.entries(copia(docs)))]));
  const contador = { docsLeidos: 0, escrituras: [], borrados: [] };
  const col = (p) => {
    if (!datos.has(p)) datos.set(p, new Map());
    return datos.get(p);
  };

  const docRef = (p, id) => ({
    id,
    get: async () => {
      contador.docsLeidos++;
      const d = col(p).get(id);
      return { exists: d !== undefined, id, data: () => copia(d) };
    },
    set: async (data) => {
      contador.escrituras.push(`${p}/${id}`);
      col(p).set(id, copia(data));
    },
    borrar: () => {
      contador.borrados.push(`${p}/${id}`);
      col(p).delete(id);
    },
  });

  const valorDe = (doc, campo) => (campo === '__name__' ? doc.id : doc.data[campo]);
  const comparable = (v) => (v && typeof v === 'object' && 'id' in v ? v.id : v);
  const cumple = (doc, { campo, op, valor }) => {
    const v = valorDe(doc, campo);
    const x = comparable(valor);
    switch (op) {
      case '==': return v === x;
      case '>': return v > x;
      case '>=': return v >= x;
      case '<': return v < x;
      case 'in': return x.includes(v);
      default: throw new Error(`op no soportada: ${op}`);
    }
  };

  const consulta = (p, filtros = [], campos = null, orden = null) => ({
    where: (campo, op, valor) => consulta(p, [...filtros, { campo, op, valor }], campos, orden),
    select: (...cs) => consulta(p, filtros, cs, orden),
    orderBy: (campo, dir = 'asc') => consulta(p, filtros, campos, { campo, dir }),
    get: async () => {
      let docs = [...col(p).entries()].map(([id, data]) => ({ id, data }));
      docs = docs.filter((d) => filtros.every((f) => cumple(d, f)));
      const clave = orden ? (d) => valorDe(d, orden.campo) : (d) => d.id;
      docs.sort((a, b) => (clave(a) < clave(b) ? -1 : clave(a) > clave(b) ? 1 : 0));
      if (orden?.dir === 'desc') docs.reverse();
      contador.docsLeidos += docs.length;
      const resultado = docs.map((d) => ({ id: d.id, ref: docRef(p, d.id), data: () => proyectar(d.data, campos) }));
      return { docs: resultado, size: resultado.length, empty: resultado.length === 0 };
    },
  });

  return {
    collection: (p) => ({ doc: (id) => docRef(p, id), ...consulta(p) }),
    batch: () => {
      const refs = [];
      return { delete: (ref) => refs.push(ref), commit: async () => refs.forEach((r) => r.borrar()) };
    },
    contador,
    leer: (p, id) => copia(col(p).get(id)),
  };
}

// ============================================================================
// Datos: un inversor con días hábiles de historia
// ============================================================================

const UID = 'user-1';
const RUTA_DIAS = `portfolioPerformance/${UID}/dates`;
const ZONA = 'America/New_York';

function diasHabiles(desde, n) {
  const dias = [];
  let d = DateTime.fromISO(desde, { zone: ZONA });
  while (dias.length < n) {
    if (d.weekday <= 5) dias.push(d.toISODate());
    d = d.plus({ days: 1 });
  }
  return dias;
}

/** Activo con cifras que cambian cada día; `ausente(i)` = ese día no lo tiene. */
function datosDelActivo(i, base, ausente = () => false) {
  if (ausente(i)) return undefined;
  const totalValue = base + i * 7.31;
  return {
    totalValue,
    totalInvestment: base * 0.9,
    units: 10,
    dailyChangePercentage: ((i % 5) - 2) * 0.37,
    adjustedDailyChangePercentage: ((i % 5) - 2) * 0.37,
    doneProfitAndLoss: 0,
    unrealizedProfitAndLoss: totalValue - base * 0.9,
    totalROI: 11.1,
  };
}

// MSFT se vende el día 20 y se recompra el día 30
const msftAusente = (i) => i >= 20 && i < 30;

function crearHistoria(n, { desde = '2026-01-02' } = {}) {
  const docs = {};
  diasHabiles(desde, n).forEach((date, i) => {
    const activos = (factor) => {
      const ap = {
        AAPL_stock: datosDelActivo(i, 1000 * factor),
        'ECOPETROL.CL_stock': datosDelActivo(i, 300 * factor),
        MSFT_stock: datosDelActivo(i, 2000 * factor, msftAusente),
      };
      Object.keys(ap).forEach((k) => ap[k] === undefined && delete ap[k]);
      return ap;
    };
    docs[date] = {
      date,
      USD: { totalValue: 3300 + i, totalInvestment: 3000, adjustedDailyChangePercentage: 0.1, assetPerformance: activos(1) },
      COP: { totalValue: 13e6 + i, totalInvestment: 12e6, adjustedDailyChangePercentage: 0.2, assetPerformance: activos(4000) },
    };
  });
  return docs;
}

function crearInversor({ dias = 40, defaultCurrency = 'USD', desde } = {}) {
  return crearFirestore({
    [RUTA_DIAS]: crearHistoria(dias, { desde }),
    userData: { [UID]: { defaultCurrency } },
  });
}

const sinHora = (s) => {
  const { lastUpdated, ...resto } = s;
  return resto;
};

const SENAL = '2026-03-03T05:05:00.000Z';
const AHORA = DateTime.fromISO(SENAL).setZone(ZONA);

/** El resumen tal como lo dejaba la noche tras los primeros `hasta` días. */
async function resumenHasta(db, hasta, clave = ['AAPL', 'stock'], moneda = 'USD') {
  const todos = (await db.collection(RUTA_DIAS).orderBy('date').get()).docs;
  return generateAssetSnapshot(db, UID, 'overall', moneda, ...clave, { dailyDocs: todos.slice(0, hasta) });
}

/** Lo que habría hecho la noche, un día por vez, con sus datos del día. */
async function nochesSeguidas(db, inicial, desdeIndice, clave = 'AAPL_stock', moneda = 'USD') {
  const todos = (await db.collection(RUTA_DIAS).orderBy('date').get()).docs.slice(desdeIndice);
  let resumen = inicial;
  for (const doc of todos) {
    const data = doc.data();
    const paso = aplicarDiaAResumenDeActivo(resumen, data[moneda].assetPerformance[clave], data.date, { now: AHORA });
    if (paso.method === 'incremental') resumen = paso.snapshot;
  }
  return resumen;
}

// ============================================================================
// Tests
// ============================================================================

describe('HU 6.4: interruptor y monedas del cierre', () => {
  afterEach(() => { delete process.env.ASSET_SNAPSHOTS_ON_DEMAND; });

  it('encendido salvo ASSET_SNAPSHOTS_ON_DEMAND=false (RN-6-H)', () => {
    expect(resumenesPorActivoBajoDemanda()).toBe(true);
    process.env.ASSET_SNAPSHOTS_ON_DEMAND = 'false';
    expect(resumenesPorActivoBajoDemanda()).toBe(false);
    process.env.ASSET_SNAPSHOTS_ON_DEMAND = 'true';
    expect(resumenesPorActivoBajoDemanda()).toBe(true);
  });

  it('USD y la moneda principal, sin repetir USD (PERF-SNAP-026)', () => {
    expect(monedasDelCierre('COP')).toEqual(['USD', 'COP']);
    expect(monedasDelCierre('USD')).toEqual(['USD']);
    expect(monedasDelCierre(null)).toEqual(['USD']);
  });
});

describe('HU 6.4: poner al día un resumen de activo al abrirlo (Escenario 4, RN-6.4-K, RN-6.4-N)', () => {
  it('lee solo los 3 días que faltan y da las cifras idénticas a las de 3 noches', async () => {
    const db = crearInversor({ dias: 40 });
    const inicial = await resumenHasta(db, 37);
    const noche = await nochesSeguidas(db, inicial, 37);
    db.contador.docsLeidos = 0;

    const r = await ponerAlDiaResumenDeActivo(db, {
      userId: UID, accountId: 'overall', currency: 'USD', ticker: 'AAPL', assetType: 'stock', snapshot: inicial, senal: SENAL,
    });

    expect(r.metodo).toBe('incremental');
    expect(r.diasLeidos).toBe(3);
    expect(db.contador.docsLeidos).toBe(4); // userData + 3 días
    expect(sinHora(r.snapshot)).toEqual(sinHora(noche));
    expect(r.snapshot.lastDateInTimeline).toBe(noche.lastDateInTimeline);
    expect(db.leer('performanceSnapshots', `${UID}_AAPL_stock_USD`)).toEqual(r.snapshot);
  });

  it('60 días sin abrirlo: añade los 42 días hábiles sin rehacer, igual que 42 noches seguidas', async () => {
    const db = crearInversor({ dias: 82 });
    const inicial = await resumenHasta(db, 40);
    const noche = await nochesSeguidas(db, inicial, 40);

    const r = await ponerAlDiaResumenDeActivo(db, {
      userId: UID, accountId: 'overall', currency: 'USD', ticker: 'AAPL', assetType: 'stock', snapshot: inicial, senal: SENAL,
    });

    expect(r.metodo).toBe('incremental');
    expect(r.diasLeidos).toBe(42);
    expect(sinHora(r.snapshot)).toEqual(sinHora(noche));
  });

  it('con clave con punto (ECOPETROL.CL) y en la moneda principal también da lo mismo que la noche', async () => {
    const db = crearInversor({ dias: 40, defaultCurrency: 'COP' });
    const inicial = await resumenHasta(db, 35, ['ECOPETROL.CL', 'stock'], 'COP');
    const noche = await nochesSeguidas(db, inicial, 35, 'ECOPETROL.CL_stock', 'COP');

    const r = await ponerAlDiaResumenDeActivo(db, {
      userId: UID, accountId: 'overall', currency: 'COP', ticker: 'ECOPETROL.CL', assetType: 'stock', snapshot: inicial, senal: SENAL,
    });

    expect(r.metodo).toBe('incremental');
    expect(sinHora(r.snapshot)).toEqual(sinHora(noche));
  });

  it('si el activo se vendió en esos días, lo rehace entero (la noche nunca añadía días a lo vendido)', async () => {
    const db = crearInversor({ dias: 40 });
    const inicial = await resumenHasta(db, 15, ['MSFT', 'stock']);
    const completo = await resumenHasta(db, 40, ['MSFT', 'stock']);

    const r = await ponerAlDiaResumenDeActivo(db, {
      userId: UID, accountId: 'overall', currency: 'USD', ticker: 'MSFT', assetType: 'stock', snapshot: inicial, senal: SENAL,
    });

    expect(r.metodo).toBe('rehecho');
    expect(r.motivo).toMatch(/^sin-posicion-/);
    expect(sinHora(r.snapshot)).toEqual(sinHora(completo));
  });

  it('con esquema viejo o sin fecha, lo rehace entero', async () => {
    const db = crearInversor({ dias: 10 });
    const inicial = await resumenHasta(db, 8);

    const viejo = await ponerAlDiaResumenDeActivo(db, {
      userId: UID, accountId: 'overall', currency: 'USD', ticker: 'AAPL', assetType: 'stock',
      snapshot: { ...inicial, schemaVersion: 2 }, senal: SENAL,
    });
    const sinFecha = await ponerAlDiaResumenDeActivo(db, {
      userId: UID, accountId: 'overall', currency: 'USD', ticker: 'AAPL', assetType: 'stock',
      snapshot: { ...inicial, lastDateInTimeline: null }, senal: SENAL,
    });

    expect(viejo.metodo).toBe('rehecho');
    expect(sinFecha.metodo).toBe('rehecho');
    expect(viejo.snapshot.lastDateInTimeline).toBe(diasHabiles('2026-01-02', 10)[9]);
  });

  it('sin días nuevos solo marca la hora: las cifras no cambian y no se vuelve a dar por viejo', async () => {
    const db = crearInversor({ dias: 10 });
    const inicial = { ...await resumenHasta(db, 10), lastUpdated: '2026-01-01T00:00:00.000Z' };

    const r = await ponerAlDiaResumenDeActivo(db, {
      userId: UID, accountId: 'overall', currency: 'USD', ticker: 'AAPL', assetType: 'stock', snapshot: inicial, senal: SENAL,
    });

    expect(r.metodo).toBe('sin-dias-nuevos');
    expect(r.diasLeidos).toBe(0);
    expect(sinHora(r.snapshot)).toEqual(sinHora(inicial));
    expect(new Date(r.snapshot.lastUpdated).getTime()).toBeGreaterThan(new Date(SENAL).getTime());
  });

  it('lo que no mantenía la noche se rehace entero, como hoy: una cuenta, u otra moneda (RN-6-A)', async () => {
    const db = crearInversor({ dias: 10 });
    const inicial = await resumenHasta(db, 8);

    const cuenta = await ponerAlDiaResumenDeActivo(db, {
      userId: UID, accountId: 'acc-1', currency: 'USD', ticker: 'AAPL', assetType: 'stock', snapshot: inicial, senal: SENAL,
    });
    const otraMoneda = await ponerAlDiaResumenDeActivo(db, {
      userId: UID, accountId: 'overall', currency: 'COP', ticker: 'AAPL', assetType: 'stock', snapshot: inicial, senal: SENAL,
    });

    expect(cuenta).toMatchObject({ metodo: 'rehecho', motivo: 'cuenta' });
    expect(otraMoneda).toMatchObject({ metodo: 'rehecho', motivo: 'moneda-no-mantenida' });
  });

  it('si una corrección borró el resumen y cambió la historia, se rehace con las cifras corregidas (Escenario 5, RN-6.4-L)', async () => {
    const db = crearInversor({ dias: 10 });
    const antes = await resumenHasta(db, 10);
    const dia = diasHabiles('2026-01-02', 10)[9];
    const corregido = db.leer(RUTA_DIAS, dia);
    corregido.USD.assetPerformance.AAPL_stock.totalValue = 9999;
    await db.collection(RUTA_DIAS).doc(dia).set(corregido);

    const r = await ponerAlDiaResumenDeActivo(db, {
      userId: UID, accountId: 'overall', currency: 'USD', ticker: 'AAPL', assetType: 'stock', snapshot: null, senal: SENAL,
    });

    expect(antes.timeline[9].v).not.toBe(9999);
    expect(r.metodo).toBe('rehecho');
    expect(r.snapshot.timeline[9]).toMatchObject({ d: dia, v: 9999 });
  });
});

describe('HU 6.4: leer solo los campos del activo (T3b)', () => {
  it.each([
    ['AAPL', 'stock'],
    ['ECOPETROL.CL', 'stock'],
    ['MSFT', 'stock'],
  ])('%s: el resumen es idéntico al de los documentos completos', async (ticker, assetType) => {
    const db = crearInversor({ dias: 40 });
    const completos = (await db.collection(RUTA_DIAS).orderBy('date').get()).docs;

    const conCompletos = await generateAssetSnapshot(db, UID, 'overall', 'USD', ticker, assetType, { dailyDocs: completos });
    const conCampos = await generateAssetSnapshot(db, UID, 'overall', 'USD', ticker, assetType);

    expect(sinHora(conCampos)).toEqual(sinHora(conCompletos));
  });

  it('la venta y la recompra siguen marcadas en la serie (u: 0 el día de la venta)', async () => {
    const db = crearInversor({ dias: 40 });
    const r = await generateAssetSnapshot(db, UID, 'overall', 'USD', 'MSFT', 'stock');
    const venta = diasHabiles('2026-01-02', 40)[20];

    expect(r.timeline.filter((p) => p.u === 0)).toEqual([{ d: venta, v: 0, c: 0, u: 0 }]);
    expect(r.timeline).toHaveLength(31); // 20 días + la venta + 10 tras la recompra
  });

  it('cada día trae solo la fecha, los totales de la moneda y ese activo', async () => {
    const db = crearInversor({ dias: 3 });
    const docs = await fetchAssetDailyDocs(db, UID, 'overall', 'USD', 'ECOPETROL.CL', 'stock');

    expect(docs).toHaveLength(3);
    expect(docs[0].data()).toEqual({
      date: '2026-01-02',
      USD: {
        totalValue: 3300,
        totalInvestment: 3000,
        assetPerformance: { 'ECOPETROL.CL_stock': datosDelActivo(0, 300) },
      },
    });
  });

  it('con despuesDe devuelve solo los días posteriores, en orden', async () => {
    const db = crearInversor({ dias: 5 });
    const dias = diasHabiles('2026-01-02', 5);

    const docs = await fetchAssetDailyDocs(db, UID, 'overall', 'USD', 'AAPL', 'stock', { despuesDe: dias[2] });

    expect(docs.map((d) => d.data().date)).toEqual([dias[3], dias[4]]);
  });
});

describe('HU 6.4: retirar resúmenes de activos que el inversor ya no tiene (Escenario 6, RN-6.4-M)', () => {
  const resumen = (userId, ticker, extra = {}) => ({ userId, ticker, assetType: 'stock', type: 'asset', ...extra });

  function crearPlataforma() {
    return crearFirestore({
      portfolioAccounts: {
        'acc-1': { userId: UID, isActive: true },
        'acc-2': { userId: UID, isActive: false },
        'acc-9': { userId: 'user-2', isActive: true },
      },
      assets: {
        l1: { portfolioAccount: 'acc-1', name: 'AAPL', assetType: 'stock', isActive: true },
        l2: { portfolioAccount: 'acc-1', name: 'MSFT', assetType: 'stock', isActive: false },
        l3: { portfolioAccount: 'acc-2', name: 'TSLA', assetType: 'stock', isActive: true },
        l4: { portfolioAccount: 'acc-1', name: 'ECOPETROL.CL', assetType: 'stock', isActive: true },
        l9: { portfolioAccount: 'acc-9', name: 'MSFT', assetType: 'stock', isActive: true },
      },
      performanceSnapshots: {
        [`${UID}_USD`]: { userId: UID, accountId: 'overall' },
        [`${UID}_AAPL_stock_USD`]: resumen(UID, 'AAPL'),
        [`${UID}_AAPL_stock_COP`]: resumen(UID, 'AAPL'),
        [`${UID}_ECOPETROL.CL_stock_USD`]: resumen(UID, 'ECOPETROL.CL'),
        [`${UID}_MSFT_stock_USD`]: resumen(UID, 'MSFT'),
        [`${UID}_MSFT_stock_acc-1_USD`]: resumen(UID, 'MSFT', { accountId: 'acc-1' }),
        [`${UID}_TSLA_stock_USD`]: resumen(UID, 'TSLA'),
        'user-2_MSFT_stock_USD': resumen('user-2', 'MSFT'),
      },
    });
  }

  it('sin aplicar solo cuenta: MSFT vendido y TSLA de una cuenta cerrada, de cualquier cuenta o moneda', async () => {
    const db = crearPlataforma();

    const r = await retirarResumenesSinTenencia(db, { userId: UID });

    expect(r.revisados).toBe(6);
    expect(r.sobrantes.sort()).toEqual([`${UID}_MSFT_stock_USD`, `${UID}_MSFT_stock_acc-1_USD`, `${UID}_TSLA_stock_USD`]);
    expect(r.retirados).toBe(0);
    expect(r.conservados).toBe(3);
    expect(db.contador.borrados).toEqual([]);
  });

  it('al aplicar retira solo esos; conserva los que tiene aunque nunca los abrió, los de portafolio y los de otro inversor', async () => {
    const db = crearPlataforma();

    const r = await retirarResumenesSinTenencia(db, { userId: UID, aplicar: true });

    expect(r.retirados).toBe(3);
    expect(db.contador.borrados.sort()).toEqual([
      `performanceSnapshots/${UID}_MSFT_stock_USD`,
      `performanceSnapshots/${UID}_MSFT_stock_acc-1_USD`,
      `performanceSnapshots/${UID}_TSLA_stock_USD`,
    ]);
    expect(db.leer('performanceSnapshots', `${UID}_AAPL_stock_COP`)).toBeDefined();
    expect(db.leer('performanceSnapshots', `${UID}_ECOPETROL.CL_stock_USD`)).toBeDefined();
    expect(db.leer('performanceSnapshots', `${UID}_USD`)).toBeDefined();
    expect(db.leer('performanceSnapshots', 'user-2_MSFT_stock_USD')).toBeDefined();
  });

  it('relanzarla no retira nada más', async () => {
    const db = crearPlataforma();
    await retirarResumenesSinTenencia(db, { userId: UID, aplicar: true });

    const segunda = await retirarResumenesSinTenencia(db, { userId: UID, aplicar: true });

    expect(segunda).toEqual({ revisados: 3, sobrantes: [], retirados: 0, conservados: 3 });
  });
});
