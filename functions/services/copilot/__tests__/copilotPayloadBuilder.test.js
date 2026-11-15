/**
 * HU 6.11.2 — El Copiloto prepara su análisis desde los resúmenes, y en una
 * moneda poco usada sin con qué calcular lo entrega en USD y lo dice.
 *
 * @see platform-docs/stories/6.11.2-una-cuenta-y-copiloto-sin-recorrer/refinamiento.md (T3, T5c)
 */

const mockDisponible = jest.fn();
const mockDistribucion = jest.fn();
const mockRendimientos = jest.fn();
jest.mock('../../handlers/queryHandlers', () => ({
  rendimientoDisponibleEn: (...args) => mockDisponible(...args),
  getPortfolioDistribution: (...args) => mockDistribucion(...args),
  getMultiAccountHistoricalReturns: (...args) => mockRendimientos(...args),
}));

jest.mock('../copilotPositions', () => ({
  leerPosiciones: jest.fn(async () => ({
    assets: [
      { name: 'VOO', units: 10, unitValue: 400, currency: 'USD', assetType: 'etf' },
      { name: 'PFBCOLOM', units: 20, unitValue: 39800, currency: 'COP', assetType: 'stock' },
    ],
    accounts: [{ id: 'cta-us' }, { id: 'cta-co' }],
  })),
}));

jest.mock('../../config', () => ({
  FINANCE_QUERY_API_URL: 'https://fq.test',
  getServiceHeaders: () => ({ 'x-service-token': 't' }),
}));

jest.mock('../../firebaseAdmin', () => {
  const coleccion = (docs) => ({ get: async () => ({ docs: docs.map((d) => ({ id: d.id, data: () => d })) }) });
  const db = {
    collection: (nombre) => (nombre === 'currentPrices'
      ? coleccion([
        { id: 'VOO', price: 500, currency: 'USD', sector: 'Index' },
        { id: 'PFBCOLOM', price: 40100, currency: 'COP', sector: 'Financial Services' },
      ])
      : coleccion([{ id: 'COP', code: 'COP', exchangeRate: 4012.5 }])),
  };
  return { firestore: () => db };
});

const { buildBasePayload, monedaDelAnalisis } = require('../copilotPayloadBuilder');

const UID = 'inv-m';

describe('HU 6.11.2: la moneda del análisis del Copiloto', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockDistribucion.mockResolvedValue({ sectors: [], totals: { portfolioValue: 48250 } });
    mockRendimientos.mockResolvedValue({ returns: { ytdReturn: 7.3413, oneMonthReturn: 1.25 } });
    global.fetch = jest.fn(async () => ({ ok: true, json: async () => ({ beta: 1.1, profitableWeeks: 55 }) }));
  });

  it('en EUR sin con qué calcular, todo el análisis va en USD y recuerda que el EUR no estaba disponible (QA-14, QA-15)', async () => {
    mockDisponible.mockResolvedValue(false);

    const { payload } = await buildBasePayload(UID, { language: 'es', currency: 'EUR' });

    expect(mockDisponible).toHaveBeenCalledWith(UID, 'EUR');
    expect(mockDistribucion).toHaveBeenCalledWith({ auth: { uid: UID } }, { currency: 'USD', includeHoldings: true });
    expect(mockRendimientos).toHaveBeenCalledWith({ auth: { uid: UID } }, { currency: 'USD', accountIds: ['overall'] });
    expect(global.fetch.mock.calls[0][0]).toContain('currency=USD');
    expect(payload.userCurrency).toBe('USD');
    expect(payload.unavailableCurrency).toBe('EUR');
    expect(payload.ytdReturn).toBe(7.3);
  });

  it('en EUR derivable, el análisis va en EUR, sale del rendimiento de todo el portafolio y no lleva aviso (QA-03)', async () => {
    mockDisponible.mockResolvedValue(true);

    const { payload } = await buildBasePayload(UID, { language: 'es', currency: 'EUR' });

    expect(mockRendimientos).toHaveBeenCalledWith({ auth: { uid: UID } }, { currency: 'EUR', accountIds: ['overall'] });
    expect(payload.userCurrency).toBe('EUR');
    expect(payload).not.toHaveProperty('unavailableCurrency');
  });

  it('si la comprobación misma falla, sigue con la moneda pedida: ninguna fuente auxiliar tumba el análisis', async () => {
    mockDisponible.mockRejectedValue(new Error('UNAVAILABLE'));

    expect(await monedaDelAnalisis(UID, 'COP')).toEqual({ currency: 'COP', unavailableCurrency: null });
  });

  it('USD y la moneda principal salen disponibles y conservan su moneda (QA-05)', async () => {
    mockDisponible.mockResolvedValue(true);

    expect(await monedaDelAnalisis(UID, 'USD')).toEqual({ currency: 'USD', unavailableCurrency: null });
    expect(await monedaDelAnalisis('inv-p', 'COP')).toEqual({ currency: 'COP', unavailableCurrency: null });
  });
});
