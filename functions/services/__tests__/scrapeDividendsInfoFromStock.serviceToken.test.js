/**
 * FIX-DIV-401 (2026-11-19): las llamadas del scraper de dividendos a la Lambda
 * deben llevar `x-service-token` (RN-7-I). Sin él, la Lambda respondía 401 todos
 * los días desde el 2026-11-03.
 */

jest.mock('axios', () => ({ get: jest.fn() }));
jest.mock('firebase-admin', () => ({ firestore: jest.fn(() => ({})) }));

const axios = require('axios');
const {
  getDividendInfoFromStockEvents,
  getDividendInfoFromLambda,
  scrapeDividendsInfoFromStockEvents,
} = require('../scrapeDividendsInfoFromStock');

const TOKEN = 'abc123def456';

function snapshotDeActivos(nombres) {
  return {
    empty: nombres.length === 0,
    docs: nombres.map((name) => ({ data: () => ({ name, assetType: 'stock' }) })),
  };
}

describe('scrapeDividendsInfoFromStock — token de servicio', () => {
  const tokenAnterior = process.env.CF_SERVICE_TOKEN;

  beforeEach(() => {
    process.env.CF_SERVICE_TOKEN = TOKEN;
    axios.get.mockReset();
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  afterAll(() => {
    if (tokenAnterior === undefined) delete process.env.CF_SERVICE_TOKEN;
    else process.env.CF_SERVICE_TOKEN = tokenAnterior;
  });

  it('el lote y el respaldo por trozos envían x-service-token', async () => {
    axios.get
      .mockRejectedValueOnce(new Error('lote caído'))
      .mockResolvedValueOnce({ data: [{ symbol: 'KO', dividend: 0.51, dividendDate: 'Dec 15, 2026' }] });

    const mapa = await scrapeDividendsInfoFromStockEvents({ injectedAssetsSnapshot: snapshotDeActivos(['KO']) });

    expect(axios.get).toHaveBeenCalledTimes(2);
    for (const [, opciones] of axios.get.mock.calls) {
      expect(opciones.headers['x-service-token']).toBe(TOKEN);
    }
    expect(mapa.get('KO')).toMatchObject({ dividend: 0.51, source: 'lambda' });
  });

  it('las consultas de un símbolo también envían x-service-token', async () => {
    axios.get.mockResolvedValue({ data: { dividendDate: 'Dec 15, 2026' } });

    await getDividendInfoFromStockEvents('KO');
    await getDividendInfoFromLambda('KO');

    expect(axios.get).toHaveBeenCalledTimes(2);
    for (const [, opciones] of axios.get.mock.calls) {
      expect(opciones.headers['x-service-token']).toBe(TOKEN);
    }
  });
});
