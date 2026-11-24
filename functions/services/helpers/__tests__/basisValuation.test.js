/**
 * HU 2.8 — Tests de la tasa que pide la base cuando nadie la declaró.
 *
 * @module services/helpers/__tests__/basisValuation.test
 * @see platform-docs/stories/2.8-base-costo-cambio-referencia/refinamiento.md (T2, T6)
 */

jest.mock('../../firebaseAdmin', () => {
  const mockAdmin = {
    firestore: jest.fn(() => ({ collection: jest.fn(), doc: jest.fn() })),
  };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };
  return mockAdmin;
});

const mockResolveRealizationRate = jest.fn();
jest.mock('../realizedFxDecomposition', () => ({
  resolveRealizationRate: (...args) => mockResolveRealizationRate(...args),
}));

const {
  resolveBasisValuation,
  basisValuationFields,
  BASIS_VALUATION_SOURCES,
} = require('../basisValuation');

/** Saldo en dólares con base en pesos: el usuario cambió su referencia a USD */
const accountWithCopBasis = (balance = 100, cost = 330000) => ({
  balances: { USD: balance },
  balanceCostBasis: { USD: { cost, referenceCurrency: 'COP', status: 'known' } },
});

describe('resolveBasisValuation (HU 2.8)', () => {
  beforeEach(() => {
    mockResolveRealizationRate.mockReset();
  });

  it('valora una entrada al cierre del día en la moneda de la base (QA-02, RN-2.8-D)', async () => {
    mockResolveRealizationRate.mockResolvedValue({
      realizationRate: 3500, realizationRateSource: 'market-date', realizationRateDate: '2026-11-17',
    });

    const valuation = await resolveBasisValuation({
      account: accountWithCopBasis(),
      currency: 'USD',
      amountDelta: 100,
      costDelta: 100,
      referenceCurrency: 'USD',
      date: '2026-11-17T15:00:00.000Z',
    });

    expect(mockResolveRealizationRate).toHaveBeenCalledWith('USD', 'COP', '2026-11-17T15:00:00.000Z');
    expect(valuation).toEqual({
      referenceCurrency: 'COP',
      rate: 3500,
      rateDate: '2026-11-17',
      source: BASIS_VALUATION_SOURCES.MARKET_DATE,
    });
  });

  it('no consulta nada cuando no hace falta tasa: lo declarado entra tal cual (RN-2.8-C, RN-2.8-G)', async () => {
    const valuation = await resolveBasisValuation({
      account: { balances: { USD: 1000 }, balanceCostBasis: { USD: { cost: 3400000, referenceCurrency: 'COP', status: 'known' } } },
      currency: 'USD',
      amountDelta: 100,
      costDelta: 345000,
      referenceCurrency: 'COP',
      date: '2026-11-17',
    });

    expect(valuation).toBeNull();
    expect(mockResolveRealizationRate).not.toHaveBeenCalled();
  });

  it('no consulta nada para una salida (RN-2.8-B)', async () => {
    const valuation = await resolveBasisValuation({
      account: accountWithCopBasis(),
      currency: 'USD',
      amountDelta: -50,
      referenceCurrency: 'USD',
      date: '2026-11-17',
    });

    expect(valuation).toBeNull();
    expect(mockResolveRealizationRate).not.toHaveBeenCalled();
  });

  it.each([
    ['sin tasa en el canal', async () => ({ realizationRate: null, realizationRateSource: 'unavailable', realizationRateDate: null })],
    ['con el canal caído', async () => { throw new Error('ECONNRESET'); }],
    ['con una tasa cero', async () => ({ realizationRate: 0, realizationRateSource: 'market-date', realizationRateDate: '2026-11-19' })],
  ])('%s declara la ausencia en lugar de inventar un número (QA-05, RN-2.8-F)', async (_caso, respuesta) => {
    mockResolveRealizationRate.mockImplementation(respuesta);

    const valuation = await resolveBasisValuation({
      account: accountWithCopBasis(),
      currency: 'USD',
      amountDelta: 50,
      referenceCurrency: 'USD',
      date: '2026-11-19',
    });

    expect(valuation).toEqual({
      referenceCurrency: 'COP',
      rate: null,
      rateDate: null,
      source: BASIS_VALUATION_SOURCES.UNAVAILABLE,
    });
  });

  it('pide la tasa de lo que ya había al reubicar la base (QA-06, RN-2.8-E)', async () => {
    mockResolveRealizationRate.mockResolvedValue({
      realizationRate: 3250, realizationRateSource: 'market-date', realizationRateDate: '2026-11-18',
    });

    const valuation = await resolveBasisValuation({
      account: { balances: { USD: 50 }, balanceCostBasis: {} },
      currency: 'USD',
      amountDelta: 100,
      costDelta: 330000,
      referenceCurrency: 'COP',
      date: '2026-11-18',
    });

    expect(valuation.referenceCurrency).toBe('COP');
    expect(valuation.rate).toBe(3250);
  });
});

describe('basisValuationFields (HU 2.8)', () => {
  it('no añade nada al movimiento cuando no hubo valoración (RN-2.8-G)', () => {
    expect(basisValuationFields(null)).toEqual({});
  });

  it('guarda la valoración en el movimiento (RN-3-B)', () => {
    const valuation = { referenceCurrency: 'COP', rate: 3500, rateDate: '2026-11-17', source: 'market-date' };
    expect(basisValuationFields(valuation)).toEqual({ basisValuation: valuation });
  });
});
