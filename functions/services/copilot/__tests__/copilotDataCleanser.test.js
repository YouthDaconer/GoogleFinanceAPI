/**
 * HU 5.3 — Depurar antes de concluir (RN-5-G, Escenario 13).
 *
 * Es la guarda que la prueba real de la HU 5.1 demostró que no se puede dejar a
 * criterio del modelo: con la estrategia dentro de las instrucciones, dos de
 * los tres motores ignoraron en silencio la posición sin precio.
 *
 * @see services/copilot/copilotDataCleanser.js
 */

const { cleanse, motivoDeExclusion, EXCLUSION_REASON } = require('../copilotDataCleanser');

const crearAsset = (overrides = {}) => ({
  name: 'MSFT',
  units: 10,
  unitValue: 300,
  currency: 'USD',
  portfolioAccount: 'acc-1',
  ...overrides,
});

const precios = (entradas) => new Map(entradas);

const PRECIO_OK = { price: 400, currency: 'USD' };

describe('motivoDeExclusion', () => {
  it('acepta una posición sana', () => {
    expect(motivoDeExclusion(crearAsset(), PRECIO_OK)).toBeNull();
  });

  it('excluye la que no tiene precio de mercado', () => {
    expect(motivoDeExclusion(crearAsset(), undefined)).toBe(EXCLUSION_REASON.SIN_PRECIO);
  });

  it('excluye la de precio cero', () => {
    expect(motivoDeExclusion(crearAsset(), { price: 0 })).toBe(EXCLUSION_REASON.PRECIO_INVALIDO);
  });

  it('excluye la de precio negativo', () => {
    expect(motivoDeExclusion(crearAsset(), { price: -5 })).toBe(EXCLUSION_REASON.PRECIO_INVALIDO);
  });

  it('excluye un precio NaN, que envenena cualquier peso que se calcule después', () => {
    expect(motivoDeExclusion(crearAsset(), { price: NaN })).toBe(EXCLUSION_REASON.PRECIO_INVALIDO);
  });

  it('excluye las unidades negativas: una posición abierta no debe acciones', () => {
    expect(motivoDeExclusion(crearAsset({ units: -3 }), PRECIO_OK))
      .toBe(EXCLUSION_REASON.UNIDADES_INVALIDAS);
  });

  it('excluye la posición cerrada', () => {
    expect(motivoDeExclusion(crearAsset({ units: 0 }), PRECIO_OK))
      .toBe(EXCLUSION_REASON.UNIDADES_INVALIDAS);
  });

  it('excluye el costo negativo', () => {
    expect(motivoDeExclusion(crearAsset({ unitValue: -100 }), PRECIO_OK))
      .toBe(EXCLUSION_REASON.COSTO_INVALIDO);
  });

  it('acepta la posición sin costo: importada sin precio de compra, pero valorable', () => {
    expect(motivoDeExclusion(crearAsset({ unitValue: undefined }), PRECIO_OK)).toBeNull();
  });
});

describe('cleanse', () => {
  it('separa las válidas de las que no, con su motivo', () => {
    const { validos, excluidos } = cleanse(
      [crearAsset(), crearAsset({ name: 'BTC-USD' }), crearAsset({ name: 'SPYG' })],
      precios([['MSFT', PRECIO_OK], ['SPYG', { price: 0 }]]),
    );

    expect(validos.map((a) => a.name)).toEqual(['MSFT']);
    expect(excluidos).toEqual([
      { ticker: 'BTC-USD', motivo: EXCLUSION_REASON.SIN_PRECIO },
      { ticker: 'SPYG', motivo: EXCLUSION_REASON.PRECIO_INVALIDO },
    ]);
  });

  it('declara un ticker una sola vez aunque esté en dos cuentas', () => {
    const { excluidos } = cleanse(
      [
        crearAsset({ name: 'BTC-USD', portfolioAccount: 'acc-1' }),
        crearAsset({ name: 'BTC-USD', portfolioAccount: 'acc-2' }),
      ],
      precios([]),
    );

    expect(excluidos).toHaveLength(1);
  });

  it('una posición sana rescata a su ticker aunque otra cuenta lo tenga corrupto', () => {
    // El usuario tiene MSFT en dos cuentas y una está rota: el análisis usa la
    // buena en vez de quedarse sin MSFT entero.
    const { validos, excluidos } = cleanse(
      [
        crearAsset({ portfolioAccount: 'acc-1', units: -1 }),
        crearAsset({ portfolioAccount: 'acc-2' }),
      ],
      precios([['MSFT', PRECIO_OK]]),
    );

    expect(validos).toHaveLength(1);
    expect(excluidos).toEqual([]);
  });

  it('la lista de excluidos es estable entre análisis', () => {
    const { excluidos } = cleanse(
      [crearAsset({ name: 'ZZZ' }), crearAsset({ name: 'AAA' })],
      precios([]),
    );

    expect(excluidos.map((e) => e.ticker)).toEqual(['AAA', 'ZZZ']);
  });

  it('un portafolio entero corrupto no deja nada válido', () => {
    const { validos } = cleanse([crearAsset({ name: 'A' }), crearAsset({ name: 'B' })], precios([]));

    expect(validos).toEqual([]);
  });

  it('ignora las posiciones sin ticker', () => {
    const { validos, excluidos } = cleanse([crearAsset({ name: '' })], precios([]));

    expect(validos).toEqual([]);
    expect(excluidos).toEqual([]);
  });
});
