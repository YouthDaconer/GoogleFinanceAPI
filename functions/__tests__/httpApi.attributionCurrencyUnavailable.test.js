/**
 * HU 6.11.4 (T9d) — La ruta de la atribución lleva el aviso de la 6.11.1 al
 * navegador, sobre la app de Express REAL (`httpApi.js`) con sus servicios
 * simulados (mismo montaje que `httpApi.investorAccess.test.js`).
 *
 * - Sin con qué calcular en una moneda poco usada: 503 con
 *   `code: 'CURRENCY_UNAVAILABLE'` y la moneda, sin cifras (QA-09).
 * - Cualquier otro fallo sigue como antes (404), y una atribución con cifras
 *   sigue llegando intacta (QA-12).
 * - El guardia de la HU 7 no cambia: otro inversor no recibe el aviso ni las
 *   cifras de INV-P (QA-17).
 *
 * @see platform-docs/stories/6.11.4-atribucion-desde-usd/refinamiento.md (T9d)
 */

jest.mock('../services/firebaseAdmin', () => ({
  auth: () => ({
    verifyIdToken: jest.fn(async (token) => {
      const tokens = { 'token-inv-m': { uid: 'uid-inversor-M' }, 'token-inv-p': { uid: 'uid-inversor-P' } };
      if (!tokens[token]) throw new Error('auth/argument-error');
      return tokens[token];
    }),
  }),
}));
jest.mock('../services/config', () => ({ getServiceTokenSecret: () => 'a1b2c3d4e5f6' }));
jest.mock('../services/scrapeIndices', () => jest.fn());
jest.mock('../services/scrapeIndicesByCountry', () => jest.fn());
jest.mock('../services/scrapeQuote', () => ({ scrapeFullQuote: jest.fn(), scrapeSimpleQuote: jest.fn() }));
jest.mock('../services/yahoo/scrapeQuote', () => ({ fetchPriceFromYahooFinance: jest.fn() }));
jest.mock('../services/scrapeActiveStock', () => ({ scrapeActiveStock: jest.fn() }));
jest.mock('../services/scrapeCurrencies', () => ({ scrapeSimpleCurrencie: jest.fn() }));
jest.mock('../services/scrapeGainers', () => ({ scrapeGainers: jest.fn() }));
jest.mock('../services/scrapeLosers', () => ({ scrapeLosers: jest.fn() }));
jest.mock('../services/scrapeNews', () => ({ scrapeNews: jest.fn() }));
jest.mock('../services/financeQuery', () => ({
  getQuotes: jest.fn(), getSimpleQuotes: jest.fn(), getNewsFromSymbol: jest.fn(), search: jest.fn(),
}));
jest.mock('../services/attribution', () => ({
  getPortfolioAttribution: jest.fn(),
  getTopContributors: jest.fn(),
  checkAttributionAvailability: jest.fn(),
}));
jest.mock('../services/riskMetrics', () => ({ calculateRiskMetrics: jest.fn() }));
jest.mock('../services/riskMetrics/riskMetricsCache', () => ({
  buildRiskCacheKey: jest.fn(), getCachedRiskMetrics: jest.fn(() => null), setCachedRiskMetrics: jest.fn(),
}));
jest.mock('../services/closedPositions', () => ({ getClosedPositions: jest.fn() }));

const API_KEY_PRUEBAS = 'clave-de-pruebas-hu-6-11-4';
process.env.API_KEY = API_KEY_PRUEBAS;

const attribution = require('../services/attribution');
const app = require('../httpApi');

/** Lo que devuelve el servicio cuando no hay con qué calcular desde USD. */
const NO_DISPONIBLE_EUR = {
  success: false,
  error: 'La cifra no está disponible en EUR por ahora',
  currencyUnavailable: { currency: 'EUR', motivo: 'sin-tasas' },
  assetAttributions: [],
  waterfallData: [],
  summary: null,
};

let servidor;
let base;

beforeAll(async () => {
  await new Promise((resolve) => { servidor = app.listen(0, resolve); });
  base = `http://127.0.0.1:${servidor.address().port}`;
});
afterAll(async () => { await new Promise((resolve) => servidor.close(resolve)); });

beforeEach(() => {
  jest.spyOn(console, 'warn').mockImplementation(() => {});
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

async function pedir(ruta, { token, query = {} } = {}) {
  const respuesta = await fetch(`${base}${ruta}?${new URLSearchParams(query)}`, {
    headers: { 'X-API-Key': API_KEY_PRUEBAS, Authorization: `Bearer ${token}` },
  });
  let cuerpo = null;
  try { cuerpo = await respuesta.json(); } catch (e) { cuerpo = null; }
  return { status: respuesta.status, cuerpo };
}

describe('HU 6.11.4 — la ruta de la atribución responde el aviso de moneda no disponible', () => {
  it.each(['/attribution', '/attribution/top'])('QA-09: %s en EUR sin con qué calcular → 503 CURRENCY_UNAVAILABLE, sin cifras', async (ruta) => {
    attribution.getPortfolioAttribution.mockImplementation(async () => NO_DISPONIBLE_EUR);
    attribution.getTopContributors.mockImplementation(async () => NO_DISPONIBLE_EUR);

    const r = await pedir(ruta, { token: 'token-inv-p', query: { currency: 'EUR', period: 'YTD' } });

    expect(r.status).toBe(503);
    expect(r.cuerpo).toEqual({
      success: false,
      error: 'La cifra no está disponible en EUR por ahora',
      code: 'CURRENCY_UNAVAILABLE',
      currency: 'EUR',
      motivo: 'sin-tasas',
    });
  });

  it('cualquier otro fallo del servicio sigue respondiendo 404 como antes', async () => {
    attribution.getPortfolioAttribution.mockImplementation(async () => ({ success: false, error: 'No performance data found' }));

    const r = await pedir('/attribution', { token: 'token-inv-p', query: { currency: 'EUR' } });

    expect(r.status).toBe(404);
    expect(r.cuerpo).toEqual({ error: 'No performance data found' });
  });

  it('QA-12: una atribución con cifras llega intacta (200)', async () => {
    const conCifras = { success: true, summary: { portfolioReturn: 8.42 }, assetAttributions: [{ ticker: 'NUTRESA', contribution: 3.15 }], waterfallData: [] };
    attribution.getPortfolioAttribution.mockImplementation(async () => conCifras);

    const r = await pedir('/attribution', { token: 'token-inv-p', query: { currency: 'COP' } });

    expect(r.status).toBe(200);
    expect(r.cuerpo).toEqual(conCifras);
  });

  it('QA-17: con la sesión de INV-M no se obtiene la atribución de INV-P en EUR (ni el aviso)', async () => {
    attribution.getPortfolioAttribution.mockImplementation(async () => NO_DISPONIBLE_EUR);

    const r = await pedir('/attribution', { token: 'token-inv-m', query: { userId: 'uid-inversor-P', currency: 'EUR' } });

    expect(r.status).not.toBe(200);
    expect(r.status).not.toBe(503);
    expect(attribution.getPortfolioAttribution).not.toHaveBeenCalled();
  });
});
