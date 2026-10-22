/**
 * HU 6.3 — Los precios del día, escritos una vez y leídos por todos.
 *
 * Defiende RN-6.3-M y el Escenario 8: todos los inversores de un mismo día
 * usan los mismos precios y tipos de cambio, aunque se procesen o se reintenten
 * a distinta hora.
 *
 * @see platform-docs/stories/6.3-cierre-nocturno-termina-siempre/refinamiento.md (T1, T9)
 */

const { guardarSnapshotDelDia, leerSnapshotDelDia, COTIZACIONES_POR_TROZO } = require('../eodMarketSnapshot');

// ============================================================================
// Fábricas locales
// ============================================================================

/** Firestore en memoria, sólo con lo que este módulo usa: doc, set, get y una subcolección. */
const crearDb = () => {
  const docs = new Map();

  const crearColeccion = (prefijo) => ({
    doc: (id) => crearDoc(`${prefijo}/${id}`),
    get: async () => {
      const hijos = [...docs.entries()].filter(([ruta]) => {
        if (!ruta.startsWith(`${prefijo}/`)) return false;
        return !ruta.slice(prefijo.length + 1).includes('/');
      });
      return { docs: hijos.map(([, data]) => ({ data: () => data })), size: hijos.length };
    },
  });

  const crearDoc = (ruta) => ({
    ruta,
    set: async (data) => { docs.set(ruta, JSON.parse(JSON.stringify(data))); },
    get: async () => ({
      exists: docs.has(ruta),
      data: () => docs.get(ruta),
    }),
    collection: (nombre) => crearColeccion(`${ruta}/${nombre}`),
  });

  return {
    collection: (nombre) => crearColeccion(nombre),
    _docs: docs,
  };
};

const cotizacion = (symbol, overrides = {}) => ({
  symbol,
  price: 100,
  regularMarketPrice: 100,
  name: symbol,
  currency: 'USD',
  change: 1,
  percentChange: 1,
  ...overrides,
});

const divisa = (code, exchangeRate, overrides = {}) => ({
  id: code,
  code,
  exchangeRate,
  isActive: true,
  name: code,
  symbol: code,
  ...overrides,
});

// ============================================================================

describe('eodMarketSnapshot — los precios del día (HU 6.3, RN-6.3-M, Escenario 8)', () => {
  it('lo que se guarda es exactamente lo que se lee: mismos precios y mismas tasas', async () => {
    const db = crearDb();
    const prices = [cotizacion('AAPL', { price: 231.47 }), cotizacion('MSFT', { price: 412.09 })];
    const currencies = [divisa('USD', 1), divisa('COP', 4210.55)];

    await guardarSnapshotDelDia(db, '2026-10-20', { prices, currencies, sourceRunId: 'run-1' });
    const leido = await leerSnapshotDelDia(db, '2026-10-20');

    expect(leido.prices).toEqual(prices);
    expect(leido.currencies).toEqual(currencies);
    expect(leido.sourceRunId).toBe('run-1');
  });

  it('dos lecturas a distinta hora devuelven el mismo precio al céntimo', async () => {
    const db = crearDb();
    await guardarSnapshotDelDia(db, '2026-10-20', {
      prices: [cotizacion('AAPL', { price: 231.47 })],
      currencies: [divisa('USD', 1)],
    });

    const primera = await leerSnapshotDelDia(db, '2026-10-20');
    const segunda = await leerSnapshotDelDia(db, '2026-10-20');

    expect(primera.prices[0].price).toBe(231.47);
    expect(segunda.prices[0].price).toBe(231.47);
  });

  it('es inmutable: un segundo guardado del mismo día no pisa los precios del primero', async () => {
    const db = crearDb();
    await guardarSnapshotDelDia(db, '2026-10-20', {
      prices: [cotizacion('AAPL', { price: 231.47 })],
      currencies: [divisa('USD', 1)],
    });

    const resultado = await guardarSnapshotDelDia(db, '2026-10-20', {
      prices: [cotizacion('AAPL', { price: 999.99 })],
      currencies: [divisa('USD', 1)],
    });

    expect(resultado.creado).toBe(false);
    const leido = await leerSnapshotDelDia(db, '2026-10-20');
    expect(leido.prices[0].price).toBe(231.47);
  });

  it('cada día tiene su propio documento: el de ayer no contamina al de hoy', async () => {
    const db = crearDb();
    await guardarSnapshotDelDia(db, '2026-10-19', { prices: [cotizacion('AAPL', { price: 100 })], currencies: [] });
    await guardarSnapshotDelDia(db, '2026-10-20', { prices: [cotizacion('AAPL', { price: 200 })], currencies: [] });

    expect((await leerSnapshotDelDia(db, '2026-10-19')).prices[0].price).toBe(100);
    expect((await leerSnapshotDelDia(db, '2026-10-20')).prices[0].price).toBe(200);
  });

  it('un día que no se materializó devuelve null, no precios a medias', async () => {
    const db = crearDb();
    expect(await leerSnapshotDelDia(db, '2026-10-20')).toBeNull();
  });

  it('con más símbolos que un trozo, se guardan en varios y se leen en orden', async () => {
    const db = crearDb();
    const prices = Array.from(
      { length: COTIZACIONES_POR_TROZO + 10 },
      (_, i) => cotizacion(`SYM${i}`, { price: i })
    );

    const resultado = await guardarSnapshotDelDia(db, '2026-10-20', { prices, currencies: [] });
    expect(resultado.trozos).toBe(2);

    const leido = await leerSnapshotDelDia(db, '2026-10-20');
    expect(leido.prices).toHaveLength(COTIZACIONES_POR_TROZO + 10);
    expect(leido.prices[0].symbol).toBe('SYM0');
    expect(leido.prices[COTIZACIONES_POR_TROZO].symbol).toBe(`SYM${COTIZACIONES_POR_TROZO}`);
  });

  it('una tasa ausente se conserva como null y no se inventa (HU #3, RN-3-D)', async () => {
    const db = crearDb();
    await guardarSnapshotDelDia(db, '2026-10-20', {
      prices: [],
      currencies: [divisa('USD', 1), divisa('COP', null, { rateSource: 'unavailable' })],
    });

    const leido = await leerSnapshotDelDia(db, '2026-10-20');
    expect(leido.currencies.find((c) => c.code === 'COP').exchangeRate).toBeNull();
  });
});
