/**
 * HU 5.3 — "Resto del portafolio", en conjunto y sin investigar (RN-5-L).
 *
 * Agrupar no necesita internet, y cada llamada que no se hace es cuota gratuita
 * que queda para lo que sí la necesita.
 *
 * @see services/copilot/copilotRestOfPortfolio.js
 */

const { resumirResto, agrupar, MAX_GRUPOS } = require('../copilotRestOfPortfolio');

const crearHolding = (overrides = {}) => ({
  symbol: 'AAA',
  weight: 0.02,
  sector: 'Technology',
  asset_type: 'stock',
  currency: 'USD',
  ...overrides,
});

describe('agrupar', () => {
  it('suma los pesos y ordena por el mayor', () => {
    const grupos = agrupar(
      [
        crearHolding({ symbol: 'A', sector: 'Energy', weight: 0.01 }),
        crearHolding({ symbol: 'B', sector: 'Technology', weight: 0.03 }),
        crearHolding({ symbol: 'C', sector: 'Technology', weight: 0.02 }),
      ],
      (h) => h.sector,
    );

    expect(grupos[0]).toEqual({
      nombre: 'Technology',
      peso: 5,
      activos: 2,
      tickers: ['B', 'C'],
    });
    expect(grupos[1].nombre).toBe('Energy');
  });

  it('lo que no declara el criterio cae en "Otros"', () => {
    expect(agrupar([crearHolding({ sector: undefined })], (h) => h.sector)[0].nombre).toBe('Otros');
  });

  it('no entrega más grupos de los que el modelo necesita leer', () => {
    const muchos = Array.from({ length: 20 }, (_, i) =>
      crearHolding({ symbol: `T${i}`, sector: `S${i}` }));

    expect(agrupar(muchos, (h) => h.sector)).toHaveLength(MAX_GRUPOS);
  });
});

describe('resumirResto', () => {
  it('agrupa por sector, por tipo y por divisa', () => {
    const resumen = resumirResto([
      crearHolding({ symbol: 'A', sector: 'Energy', asset_type: 'stock', currency: 'USD' }),
      crearHolding({ symbol: 'B', sector: 'Energy', asset_type: 'etf', currency: 'COP' }),
    ]);

    expect(resumen.activos).toBe(2);
    expect(resumen.pesoTotal).toBe(4);
    expect(resumen.porSector[0].nombre).toBe('Energy');
    expect(resumen.porTipo.map((g) => g.nombre).sort()).toEqual(['etf', 'stock']);
    expect(resumen.porDivisa.map((g) => g.nombre).sort()).toEqual(['COP', 'USD']);
  });

  it('sin resto no hay apartado (Escenario 3)', () => {
    expect(resumirResto([])).toBeNull();
    expect(resumirResto(null)).toBeNull();
  });
});
