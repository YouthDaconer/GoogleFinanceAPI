/**
 * HU 5.3 — Qué activos se investigan a fondo (RN-5-L, RN-5.3-N).
 *
 * El umbral de "movimiento fuerte" no es una preferencia de implementación: lo
 * aprobó el PO en la HU 5.1 (§7.2), con un valor por clase de activo porque la
 * volatilidad de una cripto es de 3 a 4 veces la de un índice.
 *
 * @see services/copilot/copilotRelevance.js
 */

const mockGetHistoricalPrices = jest.fn();

jest.mock('../../financeQuery', () => ({
  getHistoricalPrices: (...args) => mockGetHistoricalPrices(...args),
}));

const {
  TOP_POR_PESO,
  UMBRAL_VARIACION,
  claseDeActivo,
  umbralDe,
  variacionSemanal,
  seleccionarRelevantes,
  _cacheHistoria,
} = require('../copilotRelevance');

const AHORA = new Date('2026-10-09T12:00:00.000Z');

const crearHolding = (overrides = {}) => ({
  symbol: 'MSFT',
  weight: 0.1,
  sector: 'Technology',
  asset_type: 'stock',
  currency: 'USD',
  ...overrides,
});

/** Una serie diaria con el cierre de hace 8 días y el de hoy. */
const serie = (previo, actual) => ({
  '2026-10-01': { close: previo },
  '2026-10-09': { close: actual },
});

beforeEach(() => {
  jest.clearAllMocks();
  _cacheHistoria.clear();
  mockGetHistoricalPrices.mockResolvedValue(serie(100, 100));
});

describe('claseDeActivo y su umbral (HU 5.1 §7.2)', () => {
  it('reconoce las tres clases', () => {
    expect(claseDeActivo(crearHolding({ asset_type: 'crypto' }))).toBe('crypto');
    expect(claseDeActivo(crearHolding({ asset_type: 'etf' }))).toBe('etf');
    expect(claseDeActivo(crearHolding({ asset_type: 'stock' }))).toBe('stock');
  });

  it('lo que no declara tipo se trata como una acción', () => {
    expect(claseDeActivo(crearHolding({ asset_type: undefined }))).toBe('stock');
  });

  it('cada clase tiene su umbral: un único valor marcaría las cripto a diario', () => {
    expect(umbralDe(crearHolding({ asset_type: 'etf' }))).toBe(UMBRAL_VARIACION.etf);
    expect(umbralDe(crearHolding({ asset_type: 'stock' }))).toBe(UMBRAL_VARIACION.stock);
    expect(umbralDe(crearHolding({ asset_type: 'crypto' }))).toBe(UMBRAL_VARIACION.crypto);
    expect(UMBRAL_VARIACION.crypto).toBeGreaterThan(UMBRAL_VARIACION.stock);
    expect(UMBRAL_VARIACION.stock).toBeGreaterThan(UMBRAL_VARIACION.etf);
  });
});

describe('variacionSemanal', () => {
  it('compara el último cierre con el de hace una semana', () => {
    expect(variacionSemanal(serie(100, 89), AHORA)).toBeCloseTo(-11, 5);
  });

  it('acepta el último cierre anterior al corte: una semana puede traer festivos', () => {
    const conHueco = {
      '2026-09-28': { close: 100 },
      '2026-10-09': { close: 110 },
    };
    expect(variacionSemanal(conHueco, AHORA)).toBeCloseTo(10, 5);
  });

  it('sin dos cierres con los que comparar, no decide nada', () => {
    expect(variacionSemanal({ '2026-10-09': { close: 100 } }, AHORA)).toBeNull();
    expect(variacionSemanal(null, AHORA)).toBeNull();
  });

  it('un cierre cero o no finito no sirve de base', () => {
    expect(variacionSemanal(serie(0, 100), AHORA)).toBeNull();
    expect(variacionSemanal(serie(100, NaN), AHORA)).toBeNull();
  });
});

describe('seleccionarRelevantes', () => {
  const portafolio = (n) =>
    Array.from({ length: n }, (_, i) =>
      crearHolding({ symbol: `T${i}`, weight: (n - i) / 100 }));

  it('hasta el tope por peso se investigan todos y no hay resto (Escenario 3, QA-05)', async () => {
    const { relevantes, resto } = await seleccionarRelevantes(portafolio(TOP_POR_PESO), AHORA);

    expect(relevantes).toHaveLength(TOP_POR_PESO);
    expect(resto).toEqual([]);
    expect(relevantes.every((r) => r.motivo === 'peso')).toBe(true);
    // Hasta el tope no hace falta mirar ninguna serie: no hay nada que decidir.
    expect(mockGetHistoricalPrices).not.toHaveBeenCalled();
  });

  it('con uno más, los de mayor peso entran y el último va al resto (QA-06)', async () => {
    const { relevantes, resto } = await seleccionarRelevantes(portafolio(TOP_POR_PESO + 1), AHORA);

    expect(relevantes).toHaveLength(TOP_POR_PESO);
    expect(resto.map((r) => r.symbol)).toEqual([`T${TOP_POR_PESO}`]);
  });

  it('un activo fuera del top entra si supera su umbral, con su motivo (QA-01)', async () => {
    mockGetHistoricalPrices.mockImplementation(async (symbol) =>
      (symbol === `T${TOP_POR_PESO}` ? serie(100, 89) : serie(100, 100)));

    const { relevantes, resto } = await seleccionarRelevantes(
      portafolio(TOP_POR_PESO + 2), AHORA,
    );

    const entrante = relevantes.find((r) => r.symbol === `T${TOP_POR_PESO}`);
    expect(entrante.motivo).toBe('variacion');
    expect(entrante.variacionSemanal).toBe(-11);
    expect(resto.map((r) => r.symbol)).toEqual([`T${TOP_POR_PESO + 1}`]);
  });

  it('el mismo movimiento entra para un ETF y no para una cripto', async () => {
    // 6 % semanal: pasa el umbral del ETF (5 %) y no el de la cripto (15 %).
    mockGetHistoricalPrices.mockResolvedValue(serie(100, 106));

    const holdings = [
      ...portafolio(TOP_POR_PESO),
      crearHolding({ symbol: 'VUAA.L', weight: 0.001, asset_type: 'etf' }),
      crearHolding({ symbol: 'BTC-USD', weight: 0.001, asset_type: 'crypto' }),
    ];

    const { relevantes } = await seleccionarRelevantes(holdings, AHORA);
    const simbolos = relevantes.map((r) => r.symbol);

    expect(simbolos).toContain('VUAA.L');
    expect(simbolos).not.toContain('BTC-USD');
  });

  it('el criterio es el mismo para todos: no depende del usuario (QA-03)', async () => {
    mockGetHistoricalPrices.mockResolvedValue(serie(100, 91));

    const unaVez = await seleccionarRelevantes(portafolio(12), AHORA);
    _cacheHistoria.clear();
    const otraVez = await seleccionarRelevantes(portafolio(12), AHORA);

    expect(unaVez.relevantes.map((r) => r.symbol)).toEqual(otraVez.relevantes.map((r) => r.symbol));
  });

  it('una serie que no llega deja al activo en el resto, no lo excluye del análisis', async () => {
    mockGetHistoricalPrices.mockRejectedValue(new Error('404'));

    const { relevantes, resto } = await seleccionarRelevantes(portafolio(TOP_POR_PESO + 1), AHORA);

    expect(relevantes).toHaveLength(TOP_POR_PESO);
    expect(resto.map((r) => r.symbol)).toEqual([`T${TOP_POR_PESO}`]);
  });

  it('la serie se cachea: dos análisis seguidos no la piden dos veces', async () => {
    await seleccionarRelevantes(portafolio(TOP_POR_PESO + 1), AHORA);
    await seleccionarRelevantes(portafolio(TOP_POR_PESO + 1), AHORA);

    expect(mockGetHistoricalPrices).toHaveBeenCalledTimes(1);
  });
});
