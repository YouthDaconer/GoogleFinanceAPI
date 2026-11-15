/**
 * Integración 6.11.3 + 6.11.4 — El día anterior del intradía no descarga todas
 * las monedas.
 *
 * `getPreviousDayPerformance` leía el último documento diario entero. En una
 * moneda poco usada la atribución calcula el intradía en USD
 * (`intradiaDesdeUsd`), pero el documento llegaba con el bloque guardado en esa
 * moneda (RN-6.11-J) y el `medir` de la 6.11.4 (QA-15) lo marcaba en "Día en
 * curso". Ahora solo viajan `date`, la moneda pedida y `USD`.
 *
 * @see platform-docs/stories/6.11.4-atribucion-desde-usd/dev-record.md (Publicación)
 */

const mockConsultas = [];

/** Un Firestore falso que recuerda colección, orden, límite y campos pedidos. */
function mockCrearDb(mockDocsPorRuta) {
  return {
    collection: (ruta) => {
      const consulta = { ruta, campos: null };
      mockConsultas.push(consulta);
      const q = {
        orderBy: () => q,
        limit: () => q,
        select: (...campos) => { consulta.campos = campos; return q; },
        get: async () => {
          const docs = (mockDocsPorRuta[ruta] || []).map((d) => {
            const datos = consulta.campos
              ? Object.fromEntries(Object.entries(d).filter(([k]) => consulta.campos.includes(k)))
              : d;
            return { id: d.date, data: () => datos };
          });
          return { empty: docs.length === 0, docs };
        },
      };
      return q;
    },
  };
}

const DIA = {
  date: '2026-11-13',
  USD: { totalValue: 1000 },
  COP: { totalValue: 4000000 },
  MXN: { totalValue: 18000 },
};

let mockDocsPorRuta = {};
jest.mock('../../firebaseAdmin', () => ({ firestore: () => mockCrearDb(mockDocsPorRuta) }));
jest.mock('../../financeQuery', () => ({ getQuotes: jest.fn(), getExchangeRates: jest.fn() }));

describe('getPreviousDayPerformance — solo los campos que se usan', () => {
  let calc;

  beforeAll(() => {
    mockDocsPorRuta = {
      'portfolioPerformance/u1/dates': [DIA],
      'portfolioPerformance/u1/accounts/a1/dates': [{ ...DIA, USD: { totalValue: 400 } }],
      'portfolioPerformance/u1/accounts/a2/dates': [{ ...DIA, USD: { totalValue: 600 } }],
    };
    jest.spyOn(console, 'log').mockImplementation(() => {});
    calc = require('../intradayCalculator');
  });

  beforeEach(() => { mockConsultas.length = 0; });
  afterAll(() => { console.log.mockRestore(); });

  test('en USD pide solo la fecha y USD: la moneda poco usada no viaja', async () => {
    const datos = await calc.getPreviousDayPerformanceMultiAccount('u1', ['overall'], 'USD');
    expect(mockConsultas.map((c) => c.campos)).toEqual([['date', 'USD']]);
    expect(datos).toEqual({ id: '2026-11-13', date: '2026-11-13', USD: { totalValue: 1000 } });
    expect(datos).not.toHaveProperty('MXN');
  });

  test('en la moneda principal pide esa moneda y USD (su respaldo), con las mismas cifras', async () => {
    const datos = await calc.getPreviousDayPerformanceMultiAccount('u1', ['overall'], 'COP');
    expect(mockConsultas[0].campos).toEqual(['date', 'COP', 'USD']);
    expect(datos.COP).toEqual({ totalValue: 4000000 });
  });

  test('una cuenta y varias cuentas también proyectan', async () => {
    await calc.getPreviousDayPerformanceMultiAccount('u1', ['a1'], 'USD');
    const varias = await calc.getPreviousDayPerformanceMultiAccount('u1', ['a1', 'a2'], 'USD');
    expect(mockConsultas.map((c) => c.campos)).toEqual([['date', 'USD'], ['date', 'USD'], ['date', 'USD']]);
    expect(varias).toEqual({ date: '2026-11-13', USD: { totalValue: 1000 } });
  });

  test('sin moneda se comporta como antes: el documento completo', async () => {
    const datos = await calc.getPreviousDayPerformance('u1', 'overall');
    expect(mockConsultas[0].campos).toBeNull();
    expect(datos.MXN).toEqual({ totalValue: 18000 });
  });
});
