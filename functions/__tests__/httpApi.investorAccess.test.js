/**
 * HU 7 — Las cinco consultas con cifras de un inversor, sobre la app de Express
 * REAL de la function `app` (httpApi.js), con sus servicios simulados.
 *
 * Se levanta la app en un puerto efímero y se le habla con el `fetch` de Node:
 * así se prueba el enganche tal como corre en producción (orden de middlewares,
 * CORS, rutas), sin dependencias nuevas.
 *
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/refinamiento.md (T4, T8b)
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/qa.md
 */

// === Dobles: nada sale a la red ni a Firestore ===

const TOKEN_SERVICIO = 'a1b2c3d4e5f6';
// jest.mock se eleva sobre las constantes: los dobles repiten los valores literalmente.
jest.mock('../services/firebaseAdmin', () => ({
  auth: () => ({
    verifyIdToken: jest.fn(async (token) => {
      const tokens = {
        'token-inv-a': { uid: 'uid-inversor-A' },
        'token-inv-b': { uid: 'uid-inversor-B' },
        'token-admin': { uid: 'uid-admin-01', admin: true, role: 'admin' },
      };
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
  getQuotes: jest.fn(async () => [{ symbol: 'AAPL', regularMarketPrice: 228.52 }]),
  getSimpleQuotes: jest.fn(),
  getNewsFromSymbol: jest.fn(),
  search: jest.fn(),
}));
jest.mock('../services/attribution', () => ({
  getPortfolioAttribution: jest.fn(),
  getTopContributors: jest.fn(),
  checkAttributionAvailability: jest.fn(),
}));
jest.mock('../services/riskMetrics', () => ({ calculateRiskMetrics: jest.fn() }));
jest.mock('../services/riskMetrics/riskMetricsCache', () => ({
  buildRiskCacheKey: jest.fn((userId, period) => `${userId}|${period}`),
  getCachedRiskMetrics: jest.fn(() => null),
  setCachedRiskMetrics: jest.fn(),
}));
jest.mock('../services/closedPositions', () => ({ getClosedPositions: jest.fn() }));

const API_KEY_PRUEBAS = 'clave-de-pruebas-hu7';
process.env.API_KEY = API_KEY_PRUEBAS;

const attribution = require('../services/attribution');
const { calculateRiskMetrics } = require('../services/riskMetrics');
const { getClosedPositions } = require('../services/closedPositions');
const app = require('../httpApi');

// === Cifras de referencia (qa.md) ===

const ATRIBUCION_A = {
  success: true,
  summary: { portfolioReturn: 8.42 },
  assetAttributions: [
    { ticker: 'NVDA', contribution: 3.15, weight: 18.4 },
    { ticker: 'MSFT', contribution: 1.08, weight: 12.1 },
    { ticker: 'KO', contribution: -0.37, weight: 4.25 },
  ],
};
const TOP_A = { success: true, topContributors: [{ ticker: 'NVDA' }, { ticker: 'MSFT' }, { ticker: 'MELI' }], bottomContributors: [{ ticker: 'KO' }, { ticker: 'PFBCOLOM' }] };
const DISPONIBILIDAD_A = { available: true, since: '2024-03-04', currencies: ['USD', 'COP'], assets: 14 };
const RIESGO_A = { success: true, volatility: 17.85, maxDrawdown: -12.3 };
const VENTAS_A = { success: true, positions: [{ ticker: 'AAPL', units: 10, realizedPnL: 412.5 }, { ticker: 'KO', units: 5, realizedPnL: -38.2 }, { ticker: 'BTC-USD', units: 0.05, realizedPnL: 1105 }] };
const VENTAS_B = { success: true, positions: [{ ticker: 'MSFT', units: 20, realizedPnL: 1932.4 }] };

const CONSULTAS = [
  { clave: 'C1', ruta: '/attribution', servicio: () => attribution.getPortfolioAttribution, uidDe: (llamada) => llamada[0].userId },
  { clave: 'C2', ruta: '/attribution/top', servicio: () => attribution.getTopContributors, uidDe: (llamada) => llamada[0].userId },
  { clave: 'C3', ruta: '/attribution/check', servicio: () => attribution.checkAttributionAvailability, uidDe: (llamada) => llamada[0] },
  { clave: 'C4', ruta: '/risk-metrics', servicio: () => calculateRiskMetrics, uidDe: (llamada) => llamada[0] },
  { clave: 'C5', ruta: '/closed-positions', servicio: () => getClosedPositions, uidDe: (llamada) => llamada[0] },
];

const RECHAZO = { success: false, error: 'UNAUTHORIZED', message: 'No autorizado' };

// === Servidor efímero ===

let servidor;
let base;

beforeAll(async () => {
  // Una consulta nueva con datos de un inversor, agregada sin acordarse de protegerla (QA-17).
  app.get('/dividendos-cobrados', (req, res) => res.status(200).json({ success: true, userId: req.investorUid, dividendos: 3 }));

  await new Promise((resolve) => { servidor = app.listen(0, resolve); });
  base = `http://127.0.0.1:${servidor.address().port}`;
});

afterAll(async () => {
  await new Promise((resolve) => servidor.close(resolve));
});

let avisos;
beforeEach(() => {
  attribution.getPortfolioAttribution.mockImplementation(async () => ATRIBUCION_A);
  attribution.getTopContributors.mockImplementation(async () => TOP_A);
  attribution.checkAttributionAvailability.mockImplementation(async () => DISPONIBILIDAD_A);
  calculateRiskMetrics.mockImplementation(async () => RIESGO_A);
  getClosedPositions.mockImplementation(async (uid) => (uid === 'uid-inversor-B' ? VENTAS_B : VENTAS_A));
  avisos = jest.spyOn(console, 'warn').mockImplementation(() => {});
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  jest.restoreAllMocks();
});

/** Las líneas de rechazo que dejó el guardia en el registro. */
const rechazosRegistrados = () => avisos.mock.calls
  .map(([texto]) => { try { return JSON.parse(texto); } catch (e) { return null; } })
  .filter((l) => l && l.event === 'investor_data_access_denied');

/**
 * GET a la app. Se manda la X-API-Key para no chocar con el limitador de 10/min
 * por IP (todas las peticiones del test salen de 127.0.0.1); salvo `sinApiKey`.
 */
async function pedir(ruta, { token, servicio, userId, sinApiKey = false, metodo = 'GET', headers = {} } = {}) {
  const query = userId !== undefined ? `?${new URLSearchParams({ userId })}` : '';
  const h = { ...headers };
  if (!sinApiKey) h['X-API-Key'] = API_KEY_PRUEBAS;
  if (token) h.Authorization = `Bearer ${token}`;
  if (servicio) h['x-service-token'] = servicio;
  const respuesta = await fetch(`${base}${ruta}${query}`, { method: metodo, headers: h });
  let cuerpo = null;
  try { cuerpo = await respuesta.json(); } catch (e) { cuerpo = null; }
  return { status: respuesta.status, cuerpo, headers: respuesta.headers };
}

// === Escenario 1 ===

describe('HU 7 — el inversor ve sus cifras como hoy (Escenario 1)', () => {
  it.each(CONSULTAS)('$clave $ruta con su sesión y su userId devuelve lo mismo que el servicio (QA-01)', async (c) => {
    const r = await pedir(c.ruta, { token: 'token-inv-a', userId: 'uid-inversor-A' });

    expect(r.status).toBe(200);
    expect(c.servicio()).toHaveBeenCalledTimes(1);
    expect(c.uidDe(c.servicio().mock.calls[0])).toBe('uid-inversor-A');
    expect(rechazosRegistrados()).toEqual([]);
  });

  it('la atribución llega intacta: +8,42 %, NVDA +3,15 pp con 18,40 % (QA-01)', async () => {
    const r = await pedir('/attribution', { token: 'token-inv-a', userId: 'uid-inversor-A' });

    expect(r.cuerpo).toEqual(ATRIBUCION_A);
  });

  it.each(CONSULTAS)('$clave $ruta con sesión y sin userId responde las cifras del dueño (QA-19)', async (c) => {
    const r = await pedir(c.ruta, { token: 'token-inv-a' });

    expect(r.status).toBe(200);
    expect(c.uidDe(c.servicio().mock.calls[0])).toBe('uid-inversor-A');
  });

  it('INV-B sin userId recibe su venta de 20 MSFT, nada de INV-A (QA-19)', async () => {
    const r = await pedir('/closed-positions', { token: 'token-inv-b' });

    expect(r.cuerpo).toEqual(VENTAS_B);
  });

  it('actualizar dos veces la atribución trae las mismas cifras y ningún rechazo (QA-03)', async () => {
    const primera = await pedir('/attribution', { token: 'token-inv-a', userId: 'uid-inversor-A' });
    const segunda = await pedir('/attribution', { token: 'token-inv-a', userId: 'uid-inversor-A' });

    expect(primera.cuerpo).toEqual(ATRIBUCION_A);
    expect(segunda.cuerpo).toEqual(ATRIBUCION_A);
    expect(rechazosRegistrados()).toEqual([]);
  });

  it('el caché de riesgo se indexa con el inversor de la sesión', async () => {
    const { buildRiskCacheKey } = require('../services/riskMetrics/riskMetricsCache');
    await pedir('/risk-metrics', { token: 'token-inv-a' });

    expect(buildRiskCacheKey.mock.calls[0][0]).toBe('uid-inversor-A');
  });
});

// === Escenarios 2-4 ===

describe('HU 7 — sin sesión no hay datos ni lecturas (Escenario 2, RN-7-C)', () => {
  it.each(CONSULTAS)('$clave $ruta sin credenciales: 401 «No autorizado» y el servicio no se llama (QA-04, QA-05)', async (c) => {
    const r = await pedir(c.ruta, { userId: 'uid-inversor-A' });

    expect(r.status).toBe(401);
    expect(r.cuerpo).toEqual(RECHAZO);
    expect(c.servicio()).not.toHaveBeenCalled();
    expect(rechazosRegistrados()).toEqual([
      expect.objectContaining({ actorUid: null, targetUserId: 'uid-inversor-A', route: c.ruta, reason: 'sin_sesion' }),
    ]);
  });

  it('40 peticiones del periodo completo contra INV-G, sin sesión o con sesión ajena: ninguna lee nada (QA-05)', async () => {
    for (let i = 0; i < 20; i++) await pedir('/attribution', { userId: 'uid-inversor-grande' });
    for (let i = 0; i < 20; i++) await pedir('/attribution', { token: 'token-inv-a', userId: 'uid-inversor-grande' });

    expect(attribution.getPortfolioAttribution).not.toHaveBeenCalled();
    expect(rechazosRegistrados()).toHaveLength(40);
  });

  it('la X-API-Key de despliegue no da acceso a cifras (RN-7-B)', async () => {
    const r = await pedir('/closed-positions', { userId: 'uid-inversor-B' });

    expect(r.status).toBe(401);
    expect(getClosedPositions).not.toHaveBeenCalled();
  });
});

describe('HU 7 — nadie consulta cifras ajenas (Escenarios 3 y 4)', () => {
  it.each(CONSULTAS)('$clave $ruta con la sesión de INV-A y el userId de INV-B: rechazo sin cifras de nadie (QA-06)', async (c) => {
    const r = await pedir(c.ruta, { token: 'token-inv-a', userId: 'uid-inversor-B' });

    expect(r.status).toBe(401);
    expect(r.cuerpo).toEqual(RECHAZO);
    expect(c.servicio()).not.toHaveBeenCalled();
  });

  it('el administrador con su sesión tampoco (QA-08)', async () => {
    const r = await pedir('/attribution', { token: 'token-admin', userId: 'uid-inversor-B' });

    expect(r.status).toBe(401);
    expect(attribution.getPortfolioAttribution).not.toHaveBeenCalled();
    expect(rechazosRegistrados()[0]).toMatchObject({ actorUid: 'uid-admin-01', targetUserId: 'uid-inversor-B', reason: 'cifras_ajenas' });
  });

  it('todos los rechazos responden exactamente igual (QA-20)', async () => {
    const respuestas = await Promise.all([
      pedir('/attribution/check', { userId: 'uid-inversor-B' }),
      pedir('/attribution/check', { userId: 'uid-inexistente-99' }),
      pedir('/attribution/check', { token: 'token-vencido', userId: 'uid-inversor-A' }),
      pedir('/attribution/check', { token: 'token-inv-a', userId: 'uid-inversor-B' }),
      pedir('/attribution/check', { token: 'token-inv-a', userId: 'uid-inexistente-99' }),
      pedir('/attribution/check', { token: 'token-admin', userId: 'uid-inversor-B' }),
    ]);

    for (const r of respuestas) {
      expect(r.status).toBe(401);
      expect(r.cuerpo).toEqual(RECHAZO);
    }
  });
});

// === Sistema ===

describe('HU 7 — el sistema consulta a cualquier inversor (RN-7-I)', () => {
  it.each(CONSULTAS)('$clave $ruta con el token de servicio lee las cifras de INV-B', async (c) => {
    const r = await pedir(c.ruta, { servicio: TOKEN_SERVICIO, userId: 'uid-inversor-B' });

    expect(r.status).toBe(200);
    expect(c.uidDe(c.servicio().mock.calls[0])).toBe('uid-inversor-B');
    expect(rechazosRegistrados()).toEqual([]);
  });

  it('el sistema sin userId recibe 400 y no se lee nada', async () => {
    const r = await pedir('/risk-metrics', { servicio: TOKEN_SERVICIO });

    expect(r.status).toBe(400);
    expect(calculateRiskMetrics).not.toHaveBeenCalled();
  });

  it('un token de servicio falso es un rechazo más', async () => {
    const r = await pedir('/closed-positions', { servicio: 'token-falso', userId: 'uid-inversor-B' });

    expect(r.status).toBe(401);
    expect(r.cuerpo).toEqual(RECHAZO);
    expect(rechazosRegistrados()[0].reason).toBe('servicio_invalido');
  });
});

// === Escenarios 7 y 9 ===

describe('HU 7 — las consultas de mercado siguen abiertas (Escenario 7, QA-13)', () => {
  it('GET /quotes sin sesión ni API key responde como hoy y no deja rechazo', async () => {
    const r = await pedir('/quotes?symbols=AAPL', { sinApiKey: true });

    expect(r.status).toBe(200);
    expect(r.cuerpo).toEqual([{ symbol: 'AAPL', regularMarketPrice: 228.52 }]);
    expect(rechazosRegistrados()).toEqual([]);
  });

  it('GET /health sin sesión responde', async () => {
    const r = await pedir('/health', { sinApiKey: true });

    expect(r.status).toBe(200);
  });
});

describe('HU 7 — una consulta nueva nace protegida (Escenario 9, QA-17)', () => {
  it('sin sesión y con sesión ajena se rechaza; con la propia responde', async () => {
    const sinSesion = await pedir('/dividendos-cobrados', { userId: 'uid-inversor-A' });
    const ajena = await pedir('/dividendos-cobrados', { token: 'token-inv-a', userId: 'uid-inversor-B' });
    const propia = await pedir('/dividendos-cobrados', { token: 'token-inv-a', userId: 'uid-inversor-A' });

    expect(sinSesion.cuerpo).toEqual(RECHAZO);
    expect(ajena.cuerpo).toEqual(RECHAZO);
    expect(propia.cuerpo).toEqual({ success: true, userId: 'uid-inversor-A', dividendos: 3 });
    expect(rechazosRegistrados().map((l) => l.reason)).toEqual(['sin_sesion', 'cifras_ajenas']);
  });

  it('una ruta que no existe se rechaza sin sesión, pero no cuenta como intento', async () => {
    const r = await pedir('/wp-login.php');

    expect(r.status).toBe(401);
    expect(rechazosRegistrados()).toEqual([]);
  });
});

// === La comprobación repetible sobre la app real (QA-16) ===

describe('HU 7 — la auditoría sobre httpApi.js da cero abiertas sin decisión (QA-16)', () => {
  it('las cinco consultas (y la nueva) protegidas; mercado, ETF y salud abiertos por decisión', () => {
    const { listarRutas, auditarRutas } = require('../scripts/investorRouteAudit');
    const resultado = auditarRutas(listarRutas(app._router.stack));

    expect(resultado.abiertasSinDecision).toEqual([]);
    expect(resultado.entradasIncompletas).toEqual([]);
    expect(resultado.entradasHuerfanas).toEqual([]);
    expect(resultado.protegidas.map((r) => r.path)).toEqual([
      '/attribution', '/attribution/top', '/attribution/check', '/risk-metrics', '/closed-positions', '/dividendos-cobrados',
    ]);
    expect(resultado.abiertasPorDecision).toHaveLength(19);
    expect(resultado.enCero).toBe(true);
  });
});

// === CORS (RN-7-E) ===

describe('HU 7 — el preflight del Authorization pasa y se cachea (RN-7-E)', () => {
  it('OPTIONS /attribution desde la app responde sin rechazo y con Max-Age de 2 h', async () => {
    const r = await fetch(`${base}/attribution`, {
      method: 'OPTIONS',
      headers: {
        Origin: 'https://portafolio-inversiones.web.app',
        'Access-Control-Request-Method': 'GET',
        'Access-Control-Request-Headers': 'authorization',
      },
    });

    expect(r.status).toBe(200);
    expect(r.headers.get('access-control-max-age')).toBe('7200');
    expect((r.headers.get('access-control-allow-headers') || '').toLowerCase()).toContain('authorization');
  });
});
