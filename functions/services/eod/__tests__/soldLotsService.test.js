/**
 * HU 6.9.3 — El cierre busca sólo los lotes de las ventas del día.
 *
 * Firestore está falseado en memoria y **cuenta cada documento que entrega**,
 * por consulta o por identificador: es la medición de lecturas (MED) de
 * `qa.md`. Los datos son los de `qa.md`: noches D, D-1 y D-2, inversores INV-A
 * e INV-B y los escenarios de plataforma E1 (4.200 lotes vendidos en la
 * historia) y E10 (42.000).
 *
 * Defiende:
 * - Escenario 1 / RN-6.9.3-J (QA-01): se leen sólo los lotes de las ventas de
 *   esa noche, incluido uno de 2019, y el resultado es el de siempre (RN-6-A).
 * - Escenario 5 (QA-02): una noche sin ventas no lee ningún lote.
 * - Escenario 3 (QA-03): las lecturas no dependen del tamaño de la plataforma.
 * - QA-04: relanzar un día concreto lee sólo los lotes de ese día.
 * - QA-08: relanzar la medición da las mismas lecturas.
 * - RN-6-B (QA-09) y RN-6-H (QA-10).
 *
 * @see platform-docs/stories/6.9.3-solo-periodo-y-ventas-del-dia/qa.md
 * @see platform-docs/stories/6.9.3-solo-periodo-y-ventas-del-dia/refinamiento.md (T2, T7b)
 */

const { cargarLotesVendidos, TAMANO_GETALL, MODO_SIN_VENTAS } = require('../soldLotsService');
const { _reiniciarBloqueos, MODOS } = require('../../../utils/scopedReads');

// ============================================================================
// Firestore en memoria que cuenta lo que lee
// ============================================================================

const porId = (a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);

/**
 * @param {Array<{id: string, data: Object}>} lotes - Documentos de `assets`
 * @param {Object} [opciones]
 * @param {(id: string, data: Object) => Object} [opciones.alterarGetAll] -
 *   Cambia lo que devuelve la lectura por identificador: es la diferencia
 *   «preparada a propósito en el método nuevo» de QA-09.
 * @param {boolean} [opciones.getAllFalla]
 */
function crearDb(lotes, { alterarGetAll = null, getAllFalla = false } = {}) {
  const mapa = new Map(lotes.map((l) => [l.id, l]));
  const medicion = { documentos: 0, consultas: [], pedidosPorId: [] };

  const cumple = (data, [campo, op, valor]) => (op === 'in' ? valor.includes(data[campo]) : data[campo] === valor);

  const consulta = (filtros) => ({
    where: (campo, op, valor) => consulta([...filtros, [campo, op, valor]]),
    get: async () => {
      medicion.consultas.push(filtros);
      // Firestore: una consulta sólo con igualdades sale en orden de identificador
      const docs = lotes.filter((l) => filtros.every((f) => cumple(l.data, f))).sort(porId)
        .map((l) => ({ id: l.id, data: () => l.data }));
      medicion.documentos += docs.length;
      return { docs, size: docs.length, empty: docs.length === 0 };
    },
  });

  return {
    medicion,
    collection: () => ({ ...consulta([]), doc: (id) => ({ id }) }),
    getAll: async (...refs) => {
      if (getAllFalla) throw new Error('UNAVAILABLE');
      medicion.pedidosPorId.push(refs.map((r) => r.id));
      medicion.documentos += refs.length; // Firestore cobra cada documento pedido
      return refs.map((r) => {
        const lote = mapa.get(r.id);
        const data = lote && alterarGetAll ? alterarGetAll(r.id, lote.data) : lote?.data;
        return { id: r.id, exists: !!lote, data: () => data };
      });
    },
  };
}

// ============================================================================
// Fábricas locales: los datos de qa.md
// ============================================================================

const lote = (id, portfolioAccount, overrides = {}) => ({
  id,
  data: { name: 'AAPL', assetType: 'stock', portfolioAccount, isActive: false, units: 0, unitValue: 100, ...overrides },
});

/** Lotes de las noches de prueba. VTA-3 es una venta parcial: su lote sigue activo. */
const LOTES_DE_PRUEBA = [
  lote('a-aapl-2025-03-10', 'cta-a', { name: 'AAPL', acquisitionDate: '2025-03-10' }), // VTA-1
  lote('a-msft-2024-05-14', 'cta-a', { name: 'MSFT', acquisitionDate: '2024-05-14' }), // VTA-2
  lote('a-msft-2025-09-02', 'cta-a', { name: 'MSFT', acquisitionDate: '2025-09-02' }), // VTA-2
  lote('b-btc-2019-04-15', 'cta-b', { name: 'BTC-USD', assetType: 'crypto', isActive: true, units: 0.75, acquisitionDate: '2019-04-15' }), // VTA-3
  lote('b-ko-2023-02-07', 'cta-b', { name: 'KO', acquisitionDate: '2023-02-07' }), // VTA-4
  lote('b-voo-2022-10-19', 'cta-b', { name: 'VOO', assetType: 'etf', acquisitionDate: '2022-10-19' }), // VTA-5
];

/** Lotes vendidos de otros inversores, hasta completar el total de la plataforma. */
function plataforma(lotesVendidosEnLaHistoria) {
  const propiosVendidos = LOTES_DE_PRUEBA.filter((l) => l.data.isActive === false).length;
  const ajenos = Array.from({ length: lotesVendidosEnLaHistoria - propiosVendidos }, (_, i) =>
    lote(`z-hist-${String(i).padStart(6, '0')}`, `cta-otro-${i % 900}`, { name: 'SPY' }));
  return [...LOTES_DE_PRUEBA, ...ajenos];
}

const venta = (assetId, portfolioAccountId) => ({ type: 'sell', assetId, portfolioAccountId });
const compra = (assetId, portfolioAccountId) => ({ type: 'buy', assetId, portfolioAccountId });
const dividendo = (portfolioAccountId) => ({ type: 'dividendPay', symbol: 'KO', portfolioAccountId });

const NOCHES = {
  D: [
    venta('a-aapl-2025-03-10', 'cta-a'), // VTA-1
    venta('a-msft-2024-05-14', 'cta-a'), // VTA-2, primer lote
    venta('a-msft-2025-09-02', 'cta-a'), // VTA-2, segundo lote
    venta('b-btc-2019-04-15', 'cta-b'), // VTA-3, parcial de un lote de 2019
  ],
  'D-1': [compra('a-ko-2026-11-11', 'cta-a'), dividendo('cta-a')],
  'D-2': [venta('b-ko-2023-02-07', 'cta-b'), venta('b-voo-2022-10-19', 'cta-b')],
};

/** Igual que los dos cierres: `assetId` únicos de las ventas de la ventana. */
const assetIdsDeVentas = (movimientos) =>
  [...new Set(movimientos.filter((t) => t.type === 'sell').map((t) => t.assetId).filter(Boolean))];

/** La referencia de qa.md: lo que entrega la lectura de siempre. */
async function conLaLecturaDeSiempre(fn) {
  process.env.SCOPED_READS_ENABLED = 'false';
  try {
    return await fn();
  } finally {
    delete process.env.SCOPED_READS_ENABLED;
  }
}

const ENTORNO_ORIGINAL = { ...process.env };

beforeEach(() => {
  delete process.env.SCOPED_READS_ENABLED;
  delete process.env.SCOPED_READS_VERIFY;
  _reiniciarBloqueos();
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  process.env = { ...ENTORNO_ORIGINAL };
  jest.restoreAllMocks();
});

// ============================================================================

describe('cargarLotesVendidos — noche con ventas (HU 6.9.3, Escenario 1, RN-6.9.3-J, QA-01)', () => {
  it('noche D: lee sólo los 4 lotes de VTA-1, VTA-2 y VTA-3, incluido el de 2019', async () => {
    const db = crearDb(plataforma(4200));

    const r = await cargarLotesVendidos({ db, assetIds: assetIdsDeVentas(NOCHES.D) });

    expect(r.lecturas).toBe(4);
    expect(db.medicion.documentos).toBe(4);
    expect(db.medicion.pedidosPorId).toEqual([[
      'a-aapl-2025-03-10', 'a-msft-2024-05-14', 'a-msft-2025-09-02', 'b-btc-2019-04-15',
    ]]);
    expect(db.medicion.consultas).toEqual([]); // ninguna consulta de "todos los vendidos"
    // El lote de 2019 sigue activo (venta parcial): el cálculo lo toma de los activos, como siempre
    expect(r.activos.map((a) => a.id)).toEqual(['a-aapl-2025-03-10', 'a-msft-2024-05-14', 'a-msft-2025-09-02']);
  });

  it('el resultado es idéntico al de la lectura de siempre: mismos documentos, campos y orden (RN-6-A)', async () => {
    const ids = assetIdsDeVentas(NOCHES.D);
    const referencia = await conLaLecturaDeSiempre(() => cargarLotesVendidos({ db: crearDb(plataforma(4200)), assetIds: ids }));
    const nueva = await cargarLotesVendidos({ db: crearDb(plataforma(4200)), assetIds: ids });

    expect(nueva.activos).toEqual(referencia.activos);
    expect(nueva.activos[0]).toEqual({
      id: 'a-aapl-2025-03-10', name: 'AAPL', assetType: 'stock', portfolioAccount: 'cta-a',
      isActive: false, units: 0, unitValue: 100, acquisitionDate: '2025-03-10',
    });
  });

  it('límite inferior: con una sola venta (VTA-1) lee 1 lote', async () => {
    const db = crearDb(plataforma(4200));
    const r = await cargarLotesVendidos({ db, assetIds: assetIdsDeVentas([NOCHES.D[0]]) });
    expect(r.lecturas).toBe(1);
    expect(db.medicion.documentos).toBe(1);
    expect(r.activos.map((a) => a.id)).toEqual(['a-aapl-2025-03-10']);
  });

  it('pide por identificador en grupos de 100: 250 ventas son 3 pedidos y 250 lecturas', async () => {
    const vendidos = Array.from({ length: 250 }, (_, i) => lote(`v-${String(i).padStart(3, '0')}`, 'cta-a'));
    const db = crearDb([...vendidos, ...plataforma(4200)]);

    const r = await cargarLotesVendidos({ db, assetIds: vendidos.map((l) => l.id) });

    expect(TAMANO_GETALL).toBe(100);
    expect(db.medicion.pedidosPorId.map((p) => p.length)).toEqual([100, 100, 50]);
    expect(r.lecturas).toBe(250);
    expect(r.activos).toHaveLength(250);
  });

  it('un assetId que no es texto o trae "/" no se pide: nunca casaba con un lote', async () => {
    const db = crearDb(plataforma(4200));
    const r = await cargarLotesVendidos({ db, assetIds: ['a-aapl-2025-03-10', 12345, 'assets/otro', ''] });
    expect(db.medicion.pedidosPorId).toEqual([['a-aapl-2025-03-10']]);
    expect(r.activos.map((a) => a.id)).toEqual(['a-aapl-2025-03-10']);
  });
});

describe('cargarLotesVendidos — noche sin ventas (Escenario 5, QA-02)', () => {
  it.each([
    ['sin ninguna operación', []],
    ['sólo con la compra de 5 KO', [NOCHES['D-1'][0]]],
    ['sólo con el dividendo de KO', [NOCHES['D-1'][1]]],
  ])('noche D-1 %s: 0 lotes leídos, ni consulta ni pedido', async (_caso, movimientos) => {
    const db = crearDb(plataforma(4200));

    const r = await cargarLotesVendidos({ db, assetIds: assetIdsDeVentas(movimientos) });

    expect(r).toEqual({ activos: [], lecturas: 0, modo: MODO_SIN_VENTAS });
    expect(db.medicion.documentos).toBe(0);
    expect(db.medicion.consultas).toEqual([]);
    expect(db.medicion.pedidosPorId).toEqual([]);
  });
});

describe('cargarLotesVendidos — el costo no depende del tamaño de la plataforma (Escenario 3, QA-03)', () => {
  it('después: E1 (4.200 lotes vendidos) y E10 (42.000) leen exactamente lo mismo', async () => {
    const ids = assetIdsDeVentas(NOCHES.D);
    const e1 = crearDb(plataforma(4200));
    const e10 = crearDb(plataforma(42000));

    const r1 = await cargarLotesVendidos({ db: e1, assetIds: ids });
    const r10 = await cargarLotesVendidos({ db: e10, assetIds: ids });

    expect(e1.medicion.documentos).toBe(4);
    expect(e10.medicion.documentos).toBe(e1.medicion.documentos);
    expect(r10.activos).toEqual(r1.activos);
  });

  it('antes: la lectura de siempre leía todos los lotes vendidos de la plataforma (4.200 y 42.000)', async () => {
    const ids = assetIdsDeVentas(NOCHES.D);
    const e1 = crearDb(plataforma(4200));
    const e10 = crearDb(plataforma(42000));

    await conLaLecturaDeSiempre(() => cargarLotesVendidos({ db: e1, assetIds: ids }));
    await conLaLecturaDeSiempre(() => cargarLotesVendidos({ db: e10, assetIds: ids }));

    expect(e1.medicion.documentos).toBe(4200);
    expect(e10.medicion.documentos).toBe(42000);
  });

  it('la verificación muestra el antes y el después de cada escenario con la misma ejecución', async () => {
    process.env.SCOPED_READS_VERIFY = 'true';
    const ids = assetIdsDeVentas(NOCHES.D);

    const r1 = await cargarLotesVendidos({ db: crearDb(plataforma(4200)), assetIds: ids });
    const r10 = await cargarLotesVendidos({ db: crearDb(plataforma(42000)), assetIds: ids });

    expect([r1.lecturasCompleta, r1.lecturasAcotada]).toEqual([4200, 4]);
    expect([r10.lecturasCompleta, r10.lecturasAcotada]).toEqual([42000, 4]);
    expect(r1.modo).toBe(MODOS.VERIFICADA);
    expect(r10.modo).toBe(MODOS.VERIFICADA);
  });
});

describe('cargarLotesVendidos — relanzar un día concreto (QA-04, QA-08)', () => {
  it('la noche D relanzada dos veces lee 4 lotes cada vez y entrega lo mismo', async () => {
    const db = crearDb(plataforma(4200));
    const ids = assetIdsDeVentas(NOCHES.D);

    const primera = await cargarLotesVendidos({ db, assetIds: ids });
    const segunda = await cargarLotesVendidos({ db, assetIds: ids });

    expect(primera.lecturas).toBe(4);
    expect(segunda.lecturas).toBe(4);
    expect(segunda.activos).toEqual(primera.activos);
  });

  it('relanzar D-2 lee sus 2 lotes y D-1 ninguno', async () => {
    const db = crearDb(plataforma(4200));

    const d2 = await cargarLotesVendidos({ db, assetIds: assetIdsDeVentas(NOCHES['D-2']) });
    const d1 = await cargarLotesVendidos({ db, assetIds: assetIdsDeVentas(NOCHES['D-1']) });

    expect(d2.lecturas).toBe(2);
    expect(d2.activos.map((a) => a.id)).toEqual(['b-ko-2023-02-07', 'b-voo-2022-10-19']);
    expect(d1.lecturas).toBe(0);
  });

  it('relanzar la medición sin cambios de por medio da las mismas lecturas (QA-08)', async () => {
    process.env.SCOPED_READS_VERIFY = 'true';
    const db = crearDb(plataforma(4200));
    const ids = assetIdsDeVentas(NOCHES.D);

    const primera = await cargarLotesVendidos({ db, assetIds: ids });
    const segunda = await cargarLotesVendidos({ db, assetIds: ids });

    expect([segunda.lecturasCompleta, segunda.lecturasAcotada, segunda.modo])
      .toEqual([primera.lecturasCompleta, primera.lecturasAcotada, primera.modo]);
    expect(segunda.activos).toEqual(primera.activos);
  });
});

describe('cargarLotesVendidos — en el reparto, acotado a las cuentas del inversor (HU 6.3)', () => {
  // 35 cuentas: dos grupos de la consulta `in` (30 + 5), como en eodUserPerformanceTask
  const cuentas = Array.from({ length: 35 }, (_, i) => `cta-${String(i).padStart(2, '0')}`);
  const lotesDeCuentas = [cuentas.slice(0, 30), cuentas.slice(30)];
  const lotesDelInversor = [
    lote('l-zz', 'cta-31'),
    lote('l-aa', 'cta-32'),
    lote('l-mm', 'cta-02'),
    lote('l-bb', 'cta-05'),
    lote('l-historia', 'cta-05'), // vendido otro día: no se pide
  ];
  const ajeno = lote('l-ajeno', 'cta-de-otro'); // un assetId que no es de sus cuentas

  it('entrega lo mismo que la lectura de siempre: grupo de cuentas por grupo y por identificador', async () => {
    const ids = ['l-zz', 'l-aa', 'l-mm', 'l-bb', 'l-ajeno'];
    const datos = [...lotesDelInversor, ajeno];

    const referencia = await conLaLecturaDeSiempre(() =>
      cargarLotesVendidos({ db: crearDb(datos), assetIds: ids, lotesDeCuentas }));
    const db = crearDb(datos);
    const nueva = await cargarLotesVendidos({ db, assetIds: ids, lotesDeCuentas });

    expect(referencia.activos.map((a) => a.id)).toEqual(['l-bb', 'l-mm', 'l-aa', 'l-zz']);
    expect(nueva.activos).toEqual(referencia.activos);
    // Antes leía sus 5 lotes vendidos; ahora sólo los 5 pedidos (uno ajeno, que se descarta)
    expect(referencia.lecturas).toBe(5);
    expect(nueva.lecturas).toBe(5);
    expect(db.medicion.consultas).toEqual([]);
  });
});

describe('cargarLotesVendidos — comparar antes de aplicar y volver atrás (RN-6-B QA-09, RN-6-H QA-10)', () => {
  it('QA-09: una diferencia preparada en el método nuevo bloquea el cambio y se usa la lectura de siempre', async () => {
    process.env.SCOPED_READS_VERIFY = 'true';
    // A propósito: la lectura por identificador ve un lote como si siguiera activo
    const conDiferencia = () => crearDb(plataforma(4200), {
      alterarGetAll: (id, data) => (id === 'a-msft-2024-05-14' ? { ...data, isActive: true } : data),
    });
    const ids = assetIdsDeVentas(NOCHES.D);
    const referencia = await conLaLecturaDeSiempre(() => cargarLotesVendidos({ db: crearDb(plataforma(4200)), assetIds: ids }));

    const r = await cargarLotesVendidos({ db: conDiferencia(), assetIds: ids });

    expect(r.modo).toBe(MODOS.POR_DIFERENCIA);
    expect(r.diferencias).toEqual({ faltan: ['a-msft-2024-05-14'], sobran: [] });
    expect(r.activos).toEqual(referencia.activos);
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('DIFERENCIA en lotes-vendidos'));

    // Bloqueado: la noche siguiente sigue leyendo como antes de la historia
    delete process.env.SCOPED_READS_VERIFY;
    const db = crearDb(plataforma(4200));
    const siguiente = await cargarLotesVendidos({ db, assetIds: ids });
    expect(siguiente.modo).toBe(MODOS.BLOQUEADA);
    expect(db.medicion.documentos).toBe(4200);
  });

  it('QA-10: revertido lee todos los lotes vendidos; reaplicado vuelve a leer 4, con el mismo resultado', async () => {
    const ids = assetIdsDeVentas(NOCHES.D);

    const dbRevertido = crearDb(plataforma(4200));
    const revertido = await conLaLecturaDeSiempre(() => cargarLotesVendidos({ db: dbRevertido, assetIds: ids }));
    const dbReaplicado = crearDb(plataforma(4200));
    const reaplicado = await cargarLotesVendidos({ db: dbReaplicado, assetIds: ids });

    expect(revertido.modo).toBe(MODOS.COMPLETA);
    expect(dbRevertido.medicion.documentos).toBe(4200);
    expect(dbReaplicado.medicion.documentos).toBe(4);
    expect(reaplicado.activos).toEqual(revertido.activos);
  });

  it('si la lectura por identificador falla, usa la de siempre: el cierre no se queda sin lotes', async () => {
    const ids = assetIdsDeVentas(NOCHES.D);
    const referencia = await conLaLecturaDeSiempre(() => cargarLotesVendidos({ db: crearDb(plataforma(4200)), assetIds: ids }));

    const r = await cargarLotesVendidos({ db: crearDb(plataforma(4200), { getAllFalla: true }), assetIds: ids });

    expect(r.modo).toBe(MODOS.POR_ERROR);
    expect(r.activos).toEqual(referencia.activos);
  });
});
