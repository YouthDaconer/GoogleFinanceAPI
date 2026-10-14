/**
 * HU 5.6 — La investigación, en una sola llamada.
 *
 * Lo que defiende este archivo es el Escenario 7: **un análisis de seis activos
 * hace una llamada con *grounding*, no ocho**. Y que abaratarlo no se llevó por
 * delante nada de lo que la HU 5.3 garantizaba — el fallo aislado de un activo,
 * el caché que evita pagar dos veces, y el macro con la exposición de este
 * portafolio y no de uno genérico.
 *
 * @see services/copilot/copilotResearch.js
 */

jest.mock('../copilotEngineClient');
jest.mock('../copilotResearchCache');
jest.mock('../../financeQuery', () => ({ getHistoricalPrices: jest.fn() }));

const engineClient = require('../copilotEngineClient');
const cache = require('../copilotResearchCache');
const { CLAVE_MACRO, construirExposicion, investigarTanda } = require('../copilotResearch');

const ENTORNO = { userId: 'u-pro', secret: 'secreto', language: 'es' };

const crearHolding = (overrides = {}) => ({
  symbol: 'MSFT',
  name: 'Microsoft',
  asset_type: 'stock',
  weight: 0.5,
  currency: 'USD',
  sector: 'Technology',
  ...overrides,
});

const HOLDINGS = [crearHolding(), crearHolding({ symbol: 'AAPL', name: 'Apple', weight: 0.5 })];

/** Un hallazgo completo, con todo lo que RN-5-E y RN-5.3-Q exigen. */
const hallazgo = (label = 'Qué está pasando') => ({
  label,
  value: 'Azure acelera',
  period: null,
  publishedAt: '2026-10-10',
  sourceUrl: 'https://reuters.com/a',
  sourceTitle: 'Reuters',
});

/** La respuesta de la tanda: un objeto por ticker, más el macro. */
const respuestaDeLaTanda = (claves = ['MSFT', 'AAPL', CLAVE_MACRO]) => ({
  text: JSON.stringify(Object.fromEntries(
    claves.map((c) => [c, { findings: [hallazgo()], notes: [], conflicts: [] }]),
  )),
  tokensUsed: 9000,
  sources: [{ url: 'https://vertexaisearch.cloud.google.com/x', title: 'Reuters' }],
});

const datosDePlataforma = (entradas) => new Map(entradas);

beforeEach(() => {
  jest.clearAllMocks();
  cache.leer.mockResolvedValue(null);
  cache.guardar.mockResolvedValue(undefined);
  engineClient.investigar.mockResolvedValue(respuestaDeLaTanda());
});

describe('una sola llamada para todo el análisis (Escenario 7)', () => {
  it('dos activos y el macro se piden juntos, no por separado', async () => {
    await investigarTanda(HOLDINGS, ENTORNO);

    expect(engineClient.investigar).toHaveBeenCalledTimes(1);
    const [, , peticion] = engineClient.investigar.mock.calls[0];
    expect(peticion.task).toBe('assets');
    expect(peticion.assets.map((a) => a.ticker)).toEqual(['MSFT', 'AAPL']);
    expect(peticion.exposure).toBeDefined();
  });

  it('seis activos siguen siendo una sola llamada', async () => {
    const seis = ['A', 'B', 'C', 'D', 'E', 'F'].map((s) => crearHolding({ symbol: s }));
    engineClient.investigar.mockResolvedValue(
      respuestaDeLaTanda(['A', 'B', 'C', 'D', 'E', 'F', CLAVE_MACRO]),
    );

    await investigarTanda(seis, ENTORNO);

    expect(engineClient.investigar).toHaveBeenCalledTimes(1);
  });

  it('el consumo es el de la llamada, no la suma por activo', async () => {
    const { tokensUsed } = await investigarTanda(HOLDINGS, ENTORNO);

    expect(tokensUsed).toBe(9000);
  });

  it('las investigaciones vuelven en el mismo orden que los activos', async () => {
    const { investigaciones } = await investigarTanda(HOLDINGS, ENTORNO);

    expect(investigaciones.map((i) => i.ticker)).toEqual(['MSFT', 'AAPL']);
  });

  it('el macro vuelve aparte, con lo que la tanda entregó bajo su clave', async () => {
    const { macro } = await investigarTanda(HOLDINGS, ENTORNO);

    expect(macro.outcome).toBe('ok');
    expect(macro.findings).toHaveLength(1);
  });
});

describe('el fallo de un activo no se lleva a los demás (RN-5.3-R)', () => {
  it('la clave que el modelo omite queda fallida, no "sin datos"', async () => {
    engineClient.investigar.mockResolvedValue(respuestaDeLaTanda(['MSFT', CLAVE_MACRO]));

    const { investigaciones } = await investigarTanda(HOLDINGS, ENTORNO);

    expect(investigaciones[0].outcome).toBe('ok');
    expect(investigaciones[1].outcome).toBe('fallo');
  });

  it('si la tanda entera falla, todos vuelven fallidos y no se lanza', async () => {
    engineClient.investigar.mockRejectedValue(
      Object.assign(new Error('motor caído'), { code: 'ENGINE_QUOTA_EXHAUSTED' }),
    );

    const { investigaciones, macro, tokensUsed } = await investigarTanda(HOLDINGS, ENTORNO);

    expect(investigaciones.every((i) => i.outcome === 'fallo')).toBe(true);
    expect(macro.outcome).toBe('fallo');
    expect(tokensUsed).toBe(0);
  });

  it('la cuota agotada no se reintenta: el segundo intento la encontraría igual', async () => {
    engineClient.investigar.mockRejectedValue(
      Object.assign(new Error('sin cuota'), { code: 'ENGINE_QUOTA_EXHAUSTED' }),
    );

    await investigarTanda(HOLDINGS, ENTORNO);

    expect(engineClient.investigar).toHaveBeenCalledTimes(1);
  });

  it('una respuesta ilegible merece un segundo turno', async () => {
    engineClient.investigar
      .mockResolvedValueOnce({ text: 'esto no es JSON', tokensUsed: 10, sources: [] })
      .mockResolvedValueOnce(respuestaDeLaTanda());

    const { investigaciones } = await investigarTanda(HOLDINGS, ENTORNO);

    expect(engineClient.investigar).toHaveBeenCalledTimes(2);
    expect(investigaciones[0].outcome).toBe('ok');
  });
});

describe('el caché evita pagar dos veces por el mismo activo', () => {
  it('lo ya investigado sale de la tanda y no vuelve al motor', async () => {
    cache.leer.mockImplementation(async (ticker) => (ticker === 'MSFT' ? {
      ticker: 'MSFT', outcome: 'ok', desdeCache: true, tokensUsed: 0,
      findings: [hallazgo('Del caché')], notes: [], conflicts: [], groundingSources: [],
    } : null));
    engineClient.investigar.mockResolvedValue(respuestaDeLaTanda(['AAPL', CLAVE_MACRO]));

    const { investigaciones } = await investigarTanda(HOLDINGS, ENTORNO);

    const [, , peticion] = engineClient.investigar.mock.calls[0];
    expect(peticion.assets.map((a) => a.ticker)).toEqual(['AAPL']);
    expect(investigaciones[0].findings[0].label).toBe('Del caché');
    expect(investigaciones[1].outcome).toBe('ok');
  });

  it('con todos en caché sólo queda el macro, que no se cachea', async () => {
    cache.leer.mockResolvedValue({
      ticker: 'X', outcome: 'ok', desdeCache: true, tokensUsed: 0,
      findings: [hallazgo()], notes: [], conflicts: [], groundingSources: [],
    });
    engineClient.investigar.mockResolvedValue({
      text: JSON.stringify({ findings: [hallazgo()], notes: [], conflicts: [] }),
      tokensUsed: 1200,
      sources: [],
    });

    const { macro } = await investigarTanda(HOLDINGS, ENTORNO);

    const [, , peticion] = engineClient.investigar.mock.calls[0];
    expect(peticion.task).toBe('macro');
    expect(macro.outcome).toBe('ok');
  });

  it('lo que sí se investigó se guarda, activo por activo', async () => {
    await investigarTanda(HOLDINGS, ENTORNO);

    expect(cache.guardar).toHaveBeenCalledWith('MSFT', 'es', expect.any(Object));
    expect(cache.guardar).toHaveBeenCalledWith('AAPL', 'es', expect.any(Object));
    expect(cache.guardar).not.toHaveBeenCalledWith(CLAVE_MACRO, 'es', expect.anything());
  });

  it('se consulta y se guarda por idioma', async () => {
    // Los hallazgos vienen redactados: servir los de un análisis en español a
    // uno en inglés metería textos en el idioma equivocado en el informe.
    await investigarTanda(HOLDINGS, { ...ENTORNO, language: 'en' });

    expect(cache.leer).toHaveBeenCalledWith('MSFT', 'en');
    expect(cache.guardar).toHaveBeenCalledWith('MSFT', 'en', expect.any(Object));
  });

  it('un caché que falla no tumba la investigación', async () => {
    cache.leer.mockRejectedValue(new Error('sin conexión'));

    const { investigaciones } = await investigarTanda(HOLDINGS, ENTORNO);

    expect(investigaciones[0].outcome).toBe('ok');
  });
});

describe('los datos de la plataforma se añaden encima de lo investigado', () => {
  const DE_PLATAFORMA = {
    label: 'Ingresos',
    value: '70.00 B USD',
    period: 'Q2 2026',
    publishedAt: '2026-06-30',
    sourceUrl: 'https://api.portastock.net',
    sourceTitle: 'Portastock',
    verified: true,
  };

  it('van delante de lo que encontró el modelo', async () => {
    const { investigaciones } = await investigarTanda(HOLDINGS, ENTORNO, datosDePlataforma([
      ['MSFT', { findings: [DE_PLATAFORMA], cubierto: true }],
    ]));

    expect(investigaciones[0].findings[0].label).toBe('Ingresos');
    expect(investigaciones[0].findings[1].label).toBe('Qué está pasando');
  });

  it('al activo cubierto no se le piden las cifras al modelo (RN-5.6-A)', async () => {
    await investigarTanda(HOLDINGS, ENTORNO, datosDePlataforma([
      ['MSFT', { findings: [DE_PLATAFORMA], cubierto: true }],
      ['AAPL', { findings: [], cubierto: false }],
    ]));

    const [, , peticion] = engineClient.investigar.mock.calls[0];
    expect(peticion.assets.find((a) => a.ticker === 'MSFT').skipDatos).toBe(true);
    expect(peticion.assets.find((a) => a.ticker === 'AAPL').skipDatos).toBe(false);
  });

  it('un activo con datos de plataforma deja de estar fallido aunque la web caiga', async () => {
    // Es lo que la HU 5.6 añade a la resiliencia: antes, sin búsqueda web no
    // había ni un dato en vivo y el análisis se declaraba fallido.
    engineClient.investigar.mockRejectedValue(
      Object.assign(new Error('motor caído'), { code: 'ENGINE_FAILED' }),
    );

    const { investigaciones } = await investigarTanda(HOLDINGS, ENTORNO, datosDePlataforma([
      ['MSFT', { findings: [DE_PLATAFORMA], cubierto: true }],
    ]));

    expect(investigaciones[0].outcome).toBe('ok');
    expect(investigaciones[1].outcome).toBe('fallo');
  });
});

describe('el macro mira este portafolio, no el mercado en general', () => {
  it('la exposición lleva las divisas y los sectores con su peso', () => {
    const exposicion = construirExposicion([
      crearHolding({ weight: 0.6, currency: 'USD', sector: 'Technology' }),
      crearHolding({ symbol: 'IBE.MC', weight: 0.4, currency: 'EUR', sector: 'Utilities' }),
    ]);

    expect(exposicion.divisas).toEqual([
      { nombre: 'USD', codigo: 'USD', peso: 60 },
      { nombre: 'EUR', codigo: 'EUR', peso: 40 },
    ]);
    expect(exposicion.sectores[0].nombre).toBe('Technology');
  });

  it('la exposición que viaja es la del portafolio entero, no sólo la de los no cacheados', async () => {
    cache.leer.mockImplementation(async (ticker) => (ticker === 'MSFT' ? {
      ticker: 'MSFT', outcome: 'ok', desdeCache: true, tokensUsed: 0,
      findings: [hallazgo()], notes: [], conflicts: [], groundingSources: [],
    } : null));
    engineClient.investigar.mockResolvedValue(respuestaDeLaTanda(['AAPL', CLAVE_MACRO]));

    await investigarTanda(HOLDINGS, ENTORNO);

    const [, , peticion] = engineClient.investigar.mock.calls[0];
    expect(peticion.exposure.divisas[0].peso).toBe(100);
  });
});
