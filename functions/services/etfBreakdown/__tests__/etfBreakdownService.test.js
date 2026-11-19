/**
 * HU 6.16 — El desglose de cada ETF se guarda una vez y lo usan todas las consultas.
 *
 * Defiende el formato del guardado (RN-6.16-D, ajuste 2 del PO), la vigencia de
 * 7 días (RN-6.16-B), la validación del refresco (RN-6.16-F), la conservación
 * del guardado cuando la fuente falla (RN-6.16-C) y el «un intento al día»
 * guardado en el documento (ajuste 1 del PO).
 *
 * @see platform-docs/stories/6.16-desglose-etf-compartido/refinamiento.md (T1, T7)
 */

const mockEnqueue = jest.fn(async () => {});
jest.mock('firebase-admin/functions', () => ({
  getFunctions: () => ({ taskQueue: () => ({ enqueue: (...a) => mockEnqueue(...a) }) }),
}));

const desglose = require('../etfBreakdownService');

// ============================================================================
// Fábricas locales
// ============================================================================

/** Copia profunda que conserva los binarios como Buffer. */
const clonar = (valor) => {
  if (Buffer.isBuffer(valor) || valor instanceof Uint8Array) return Buffer.from(valor);
  if (Array.isArray(valor)) return valor.map(clonar);
  if (valor && typeof valor === 'object') {
    return Object.fromEntries(Object.entries(valor).map(([k, v]) => [k, clonar(v)]));
  }
  return valor;
};

/**
 * Firestore en memoria, solo con lo que usa el módulo: doc/get/set (con
 * merge)/update, subcolecciones, `getAll`, `batch` y `runTransaction`.
 */
const crearDb = () => {
  const docs = new Map();
  const conteo = { lecturas: 0, escrituras: 0, lotes: 0 };

  const leer = (ruta) => {
    conteo.lecturas++;
    return { exists: docs.has(ruta), data: () => clonar(docs.get(ruta)) };
  };
  const escribir = (ruta, data, opciones) => {
    conteo.escrituras++;
    const previo = opciones?.merge ? (docs.get(ruta) || {}) : {};
    docs.set(ruta, clonar({ ...previo, ...data }));
  };

  const crearDoc = (ruta) => ({
    ruta,
    get: async () => leer(ruta),
    set: async (data, opciones) => escribir(ruta, data, opciones),
    update: async (data) => escribir(ruta, data, { merge: true }),
    collection: (nombre) => crearColeccion(`${ruta}/${nombre}`),
  });
  const crearColeccion = (prefijo) => ({ doc: (id) => crearDoc(`${prefijo}/${id}`) });

  return {
    collection: (nombre) => crearColeccion(nombre),
    getAll: async (...refs) => refs.map((r) => leer(r.ruta)),
    batch: () => {
      const operaciones = [];
      return {
        set: (ref, data) => operaciones.push(() => escribir(ref.ruta, data)),
        delete: (ref) => operaciones.push(() => { conteo.escrituras++; docs.delete(ref.ruta); }),
        commit: async () => { conteo.lotes++; operaciones.forEach((op) => op()); },
      };
    },
    runTransaction: async (fn) => fn({
      get: async (ref) => leer(ref.ruta),
      update: (ref, data) => escribir(ref.ruta, data, { merge: true }),
    }),
    _docs: docs,
    _conteo: conteo,
  };
};

const holding = (i, overrides = {}) => ({
  symbol: `S${i}`, isin: `US${String(i).padStart(10, '0')}`, name: `Empresa número ${i} de un fondo mundial`,
  weight: 1 / (i + 7), asset_type: 'stock', ...overrides,
});

/** Respuesta de la fuente: `n` posiciones, con campos que el cálculo no usa. */
const respuestaFuente = (n, overrides = {}) => ({
  symbol: 'VUAA.L',
  source: 'etf.com',
  holdings: Array.from({ length: n }, (_, i) => ({ ...holding(i), sharesHeld: 1000 + i, marketValue: 12.5 * i })),
  sectors: [{ name: 'Technology', weight: 0.3141592653589793, extra: 'x' }, { name: 'Healthcare', weight: 0.1 }],
  countries: [{ name: 'United States', weight: 0.987654321 }, { name: 'Caymen Islands', weight: 0.012345679 }],
  ...overrides,
});

const D = new Date('2026-11-19T15:00:00.000Z');
const diasAntes = (n) => new Date(D.getTime() - n * 24 * 60 * 60 * 1000);

beforeEach(() => {
  mockEnqueue.mockClear();
});

// ============================================================================

describe('HU 6.16 — formato del desglose guardado (RN-6.16-D, D2)', () => {
  test('lo que se guarda es exactamente lo que usa el cálculo: mismos pesos en todos los decimales (QA-03)', async () => {
    const db = crearDb();
    const fuente = respuestaFuente(504);

    await desglose.guardarDesglose(db, 'vuaa.l', fuente, { ahora: D, anterior: null });
    const { guardados } = await desglose.leerDesgloses(db, ['VUAA.L']);
    const leido = guardados.get('VUAA.L').data;

    expect(leido).toEqual(desglose.proyectarDesglose(fuente));
    expect(leido.holdings).toHaveLength(504);
    expect(leido.holdings[3]).toEqual(holding(3));
    expect(Object.is(leido.sectors[0].weight, 0.3141592653589793)).toBe(true);
    expect(leido.countries[1]).toEqual({ name: 'Caymen Islands', weight: 0.012345679 });
  });

  test('la proyección quita lo que el cálculo no usa y no inventa campos ausentes', () => {
    const p = desglose.proyectarDesglose({
      holdings: [{ symbol: 'AAPL', weight: 0.07, marketValue: 1 }, { isin: 'IE00B', name: 'Bono', weight: 0.01 }],
      sectors: [{ name: 'Technology', weight: 0.5, extra: true }],
    });
    expect(p).toEqual({
      holdings: [{ symbol: 'AAPL', weight: 0.07 }, { isin: 'IE00B', name: 'Bono', weight: 0.01 }],
      sectors: [{ name: 'Technology', weight: 0.5 }],
      countries: [],
    });
  });

  test('la raíz lleva los metadatos y el binario; un ETF normal cabe en un solo documento', async () => {
    const db = crearDb();
    const r = await desglose.guardarDesglose(db, 'EIMI.L', respuestaFuente(1858), { ahora: D, anterior: null });
    const raiz = db._docs.get('etfBreakdowns/EIMI.L');

    expect(r.partes).toBe(1);
    expect(raiz).toMatchObject({
      symbol: 'EIMI.L', formato: 'gzip-json-v1', posiciones: 1858, sectores: 2, paises: 2, partes: 1,
      obtenidoEn: '2026-11-19T15:00:00.000Z', ultimoIntentoEn: '2026-11-19', ultimoFallo: null,
    });
    expect(Buffer.isBuffer(raiz.datos)).toBe(true);
    expect(raiz.bytes).toBeLessThan(desglose.BYTES_POR_PARTE);
  });

  test('un ETF de 9.800 posiciones se guarda completo en varias partes y se lee entero (QA-10, Escenario 6)', async () => {
    const db = crearDb();
    const fuente = respuestaFuente(9800);

    const r = await desglose.guardarDesglose(db, 'VT', fuente, { ahora: D, anterior: null, bytesPorParte: 20000 });
    expect(r.partes).toBeGreaterThan(3);
    expect(db._conteo.lotes).toBe(1);

    const { guardados, lecturas } = await desglose.leerDesgloses(db, ['VT']);
    const leido = guardados.get('VT').data;
    expect(leido.holdings).toHaveLength(9800);
    expect(leido.holdings[9799]).toEqual(holding(9799));
    expect(lecturas).toBe(r.partes);
  });

  test('una versión más corta borra en el mismo lote las partes sobrantes de la anterior (ajuste 2)', async () => {
    const db = crearDb();
    const larga = await desglose.guardarDesglose(db, 'VT', respuestaFuente(9800), { ahora: diasAntes(8), anterior: null, bytesPorParte: 20000 });
    await desglose.guardarDesglose(db, 'VT', respuestaFuente(9800), { ahora: D, bytesPorParte: 400000 });

    const partes = [...db._docs.keys()].filter((k) => k.startsWith('etfBreakdowns/VT/partes/'));
    const raiz = db._docs.get('etfBreakdowns/VT');
    expect(larga.partes).toBeGreaterThan(raiz.partes);
    expect(partes).toHaveLength(raiz.partes - 1);
    partes.forEach((k) => expect(db._docs.get(k).version).toBe(raiz.version));
    expect(db._conteo.lotes).toBe(2);
  });

  test('si las partes no llevan la versión de la raíz, el conjunto se descarta y cuenta como no guardado (ajuste 2)', async () => {
    const db = crearDb();
    await desglose.guardarDesglose(db, 'VT', respuestaFuente(9800), { ahora: D, anterior: null, bytesPorParte: 20000 });
    const ruta = 'etfBreakdowns/VT/partes/2';
    db._docs.set(ruta, { ...db._docs.get(ruta), version: 'otra-version' });

    const { guardados, descartados } = await desglose.leerDesgloses(db, ['VT']);
    expect(guardados.has('VT')).toBe(false);
    expect(descartados).toEqual(['VT']);
  });

  test('un binario corrupto también cuenta como no guardado, sin lanzar', async () => {
    const db = crearDb();
    await desglose.guardarDesglose(db, 'TLT', respuestaFuente(42), { ahora: D, anterior: null });
    db._docs.get('etfBreakdowns/TLT').datos = Buffer.from('no es gzip');

    const { guardados, descartados } = await desglose.leerDesgloses(db, ['TLT']);
    expect(guardados.size).toBe(0);
    expect(descartados).toEqual(['TLT']);
  });

  test('varios ETF se leen en una sola ida, una lectura por ETF aunque no exista (RN-6.16-E)', async () => {
    const db = crearDb();
    await desglose.guardarDesglose(db, 'VUAA.L', respuestaFuente(504), { ahora: D, anterior: null });
    await desglose.guardarDesglose(db, 'EIMI.L', respuestaFuente(1858), { ahora: D, anterior: null });

    const { guardados, lecturas } = await desglose.leerDesgloses(db, ['VUAA.L', 'EIMI.L', 'SPYG']);
    expect([...guardados.keys()]).toEqual(['VUAA.L', 'EIMI.L']);
    expect(lecturas).toBe(3);
  });
});

describe('HU 6.16 — vigencia de 7 días (RN-6.16-B, QA-04)', () => {
  test.each([
    [diasAntes(2), false],
    [diasAntes(7), false],
    [new Date(diasAntes(7).getTime() - 1), true],
    [diasAntes(9), true],
  ])('obtenido el %s → vencido: %s', (obtenido, vencido) => {
    expect(desglose.estaVencido({ obtenidoEn: obtenido.toISOString() }, D)).toBe(vencido);
  });

  test('sin fecha legible se trata como vencido', () => {
    expect(desglose.estaVencido({}, D)).toBe(true);
  });
});

describe('HU 6.16 — validación del refresco (RN-6.16-F, QA-07)', () => {
  test.each([
    [10, false],
    [251, false],
    [252, true],
    [510, true],
  ])('con 504 guardadas, %i posiciones de la fuente → válida: %s', (n, valido) => {
    const r = desglose.validarRefresco(respuestaFuente(n), { posiciones: 504 });
    expect(r.valido).toBe(valido);
    if (!valido) expect(r.motivo).toBe(`respuesta corta: ${n} posiciones frente a 504 guardadas`);
  });

  test('sin respuesta o sin datos no vale, con su motivo (QA-06)', () => {
    expect(desglose.validarRefresco(null, { posiciones: 504 })).toEqual({ valido: false, motivo: 'la fuente no respondió' });
    expect(desglose.validarRefresco({ holdings: [], sectors: [], countries: [] }, { posiciones: 504 }))
      .toEqual({ valido: false, motivo: 'la fuente no trajo datos' });
  });

  test('si nunca se guardó, cualquier respuesta con datos vale', () => {
    expect(desglose.validarRefresco(respuestaFuente(10), null).valido).toBe(true);
  });
});

describe('HU 6.16 — refrescarDesglose (RN-6.16-C, QA-04, QA-06, QA-08)', () => {
  const guardarVencido = async (db, posiciones = 1858) =>
    desglose.guardarDesglose(db, 'EIMI.L', respuestaFuente(posiciones), { ahora: diasAntes(9), anterior: null });

  test('con la fuente bien, reemplaza el vencido: 1.860 posiciones y fecha de hoy (QA-04)', async () => {
    const db = crearDb();
    await guardarVencido(db);

    const r = await desglose.refrescarDesglose(db, 'EIMI.L', { ahora: D, obtener: async () => respuestaFuente(1860) });

    expect(r).toMatchObject({ resultado: 'actualizado', posiciones: 1860 });
    const raiz = db._docs.get('etfBreakdowns/EIMI.L');
    expect(raiz).toMatchObject({ posiciones: 1860, obtenidoEn: D.toISOString(), ultimoIntentoEn: '2026-11-19', ultimoFallo: null });
  });

  test.each([
    ['error', async () => null, 'la fuente no respondió'],
    ['sin datos', async () => ({ holdings: [] }), 'la fuente no trajo datos'],
    ['respuesta corta', async () => respuestaFuente(10), 'respuesta corta: 10 posiciones frente a 1858 guardadas'],
  ])('con la fuente en «%s» conserva el guardado y anota el fallo con su motivo (QA-06, QA-07)', async (_, obtener, motivo) => {
    const db = crearDb();
    await guardarVencido(db);
    const antes = clonar(db._docs.get('etfBreakdowns/EIMI.L'));

    const r = await desglose.refrescarDesglose(db, 'EIMI.L', { ahora: D, obtener });

    expect(r).toMatchObject({ resultado: 'conservado', motivo, posiciones: 1858 });
    const raiz = db._docs.get('etfBreakdowns/EIMI.L');
    expect(raiz.version).toBe(antes.version);
    expect(raiz.obtenidoEn).toBe(antes.obtenidoEn);
    expect(raiz.posiciones).toBe(1858);
    expect(raiz.ultimoIntentoEn).toBe('2026-11-19');
    expect(raiz.ultimoFallo).toEqual({ en: D.toISOString(), motivo });

    const { guardados } = await desglose.leerDesgloses(db, ['EIMI.L']);
    expect(guardados.get('EIMI.L').data.holdings).toHaveLength(1858);
  });

  test('al día siguiente, con la fuente bien, el reintento lo actualiza (QA-06 paso 2)', async () => {
    const db = crearDb();
    await guardarVencido(db);
    await desglose.refrescarDesglose(db, 'EIMI.L', { ahora: D, obtener: async () => null });

    const manana = new Date(D.getTime() + 24 * 60 * 60 * 1000);
    const r = await desglose.refrescarDesglose(db, 'EIMI.L', { ahora: manana, obtener: async () => respuestaFuente(1860) });

    expect(r.resultado).toBe('actualizado');
    expect(db._docs.get('etfBreakdowns/EIMI.L')).toMatchObject({ posiciones: 1860, ultimoIntentoEn: '2026-11-20', ultimoFallo: null });
  });

  test('nunca guardado y la fuente caída: no escribe nada (QA-08)', async () => {
    const db = crearDb();
    const r = await desglose.refrescarDesglose(db, 'SPYG', { ahora: D, obtener: async () => null });

    expect(r).toMatchObject({ resultado: 'sin-guardar', motivo: 'la fuente no respondió' });
    expect(db._docs.size).toBe(0);
  });
});

describe('HU 6.16 — un intento de refresco al día, guardado en el documento (ajuste 1 del PO)', () => {
  test('el primero del día reclama el intento y encola; el segundo, en otra instancia, no', async () => {
    const db = crearDb();
    await desglose.guardarDesglose(db, 'EIMI.L', respuestaFuente(1858), { ahora: diasAntes(9), anterior: null });
    const metaInstancia1 = { ultimoIntentoEn: '2026-11-10' };
    const metaInstancia2 = { ultimoIntentoEn: '2026-11-10' };

    const r1 = await desglose.programarRefresco(db, 'EIMI.L', metaInstancia1, { ahora: D });
    const r2 = await desglose.programarRefresco(db, 'EIMI.L', metaInstancia2, { ahora: D });

    expect(r1).toEqual({ estado: 'encolado', lecturas: 1, escrituras: 1 });
    expect(r2).toEqual({ estado: 'ya-intentado-hoy', lecturas: 1, escrituras: 0 });
    expect(mockEnqueue).toHaveBeenCalledTimes(1);
    expect(mockEnqueue).toHaveBeenCalledWith({ symbol: 'EIMI.L' }, { dispatchDeadlineSeconds: 120 });
    expect(db._docs.get('etfBreakdowns/EIMI.L').ultimoIntentoEn).toBe('2026-11-19');
  });

  test('en la misma instancia, el segundo cálculo del día ni siquiera lee', async () => {
    const db = crearDb();
    await desglose.guardarDesglose(db, 'EIMI.L', respuestaFuente(1858), { ahora: diasAntes(9), anterior: null });
    const meta = { ultimoIntentoEn: '2026-11-10' };

    await desglose.programarRefresco(db, 'EIMI.L', meta, { ahora: D });
    const lecturasAntes = db._conteo.lecturas;
    const r = await desglose.programarRefresco(db, 'EIMI.L', meta, { ahora: D });

    expect(r).toEqual({ estado: 'ya-intentado-hoy', lecturas: 0, escrituras: 0 });
    expect(db._conteo.lecturas).toBe(lecturasAntes);
  });

  test('un intento fallido también gasta el día: no se vuelve a encolar hasta mañana', async () => {
    const db = crearDb();
    await desglose.guardarDesglose(db, 'EIMI.L', respuestaFuente(1858), { ahora: diasAntes(9), anterior: null });
    await desglose.refrescarDesglose(db, 'EIMI.L', { ahora: D, obtener: async () => null });

    const hoy = await desglose.programarRefresco(db, 'EIMI.L', { ultimoIntentoEn: '2026-11-10' }, { ahora: D });
    const manana = await desglose.programarRefresco(db, 'EIMI.L', { ultimoIntentoEn: '2026-11-19' },
      { ahora: new Date(D.getTime() + 24 * 60 * 60 * 1000) });

    expect(hoy.estado).toBe('ya-intentado-hoy');
    expect(manana.estado).toBe('encolado');
    expect(mockEnqueue).toHaveBeenCalledTimes(1);
  });

  test('si el encolado falla, el día queda gastado y no lanza (se reintenta mañana)', async () => {
    const db = crearDb();
    await desglose.guardarDesglose(db, 'EIMI.L', respuestaFuente(1858), { ahora: diasAntes(9), anterior: null });

    const r = await desglose.programarRefresco(db, 'EIMI.L', { ultimoIntentoEn: null },
      { ahora: D, encolar: async () => { throw new Error('NOT_FOUND: queue'); } });

    expect(r).toEqual({ estado: 'error', lecturas: 1, escrituras: 1 });
    expect(db._docs.get('etfBreakdowns/EIMI.L').ultimoIntentoEn).toBe('2026-11-19');
  });

  test('el día se cuenta en Nueva York: las 02:00 UTC del 20 aún son el 19', () => {
    expect(desglose.hoyEnNuevaYork(new Date('2026-11-20T02:00:00.000Z'))).toBe('2026-11-19');
  });
});

describe('HU 6.16 — interruptor (D7, RN-6-H)', () => {
  const original = process.env.ETF_BREAKDOWN_STORE;
  afterEach(() => {
    if (original === undefined) delete process.env.ETF_BREAKDOWN_STORE;
    else process.env.ETF_BREAKDOWN_STORE = original;
  });

  test('encendido por defecto; solo «false» lo apaga', () => {
    delete process.env.ETF_BREAKDOWN_STORE;
    expect(desglose.desgloseCompartidoActivo()).toBe(true);
    process.env.ETF_BREAKDOWN_STORE = 'true';
    expect(desglose.desgloseCompartidoActivo()).toBe(true);
    process.env.ETF_BREAKDOWN_STORE = 'false';
    expect(desglose.desgloseCompartidoActivo()).toBe(false);
  });
});
