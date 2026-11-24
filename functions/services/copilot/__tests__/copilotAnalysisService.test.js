/**
 * HU 5.3 — La pipeline del análisis.
 *
 * Depurar → seleccionar → investigar activo por activo → resto → macro →
 * redactar. Lo que defiende este archivo es **cuándo el análisis se entrega y
 * cuándo falla**, que es donde las reglas se contradicen más fácilmente:
 *
 * - tras depurar no queda nada → falla sin cobrar (Escenario 13, QA-21);
 * - falla un activo → el análisis **se entrega** (Escenario 19, QA-30);
 * - fallan todos los activos pero el macro responde → **se entrega** (QA-31);
 * - no se obtiene ni un dato en vivo → falla sin cobrar (Escenario 15, QA-24).
 *
 * @see services/copilot/copilotAnalysisService.js
 */

const documentosEscritos = [];
const documentosBorrados = [];

jest.mock('../../firebaseAdmin', () => ({
  firestore: () => ({
    collection: () => ({
      doc: () => ({
        collection: () => ({
          doc: (id) => ({
            set: async (datos) => documentosEscritos.push({ id, datos }),
            delete: async () => documentosBorrados.push(id),
          }),
        }),
      }),
    }),
  }),
}));

jest.mock('../copilotRunRepository');
jest.mock('../copilotPayloadBuilder');
jest.mock('../copilotFundamentals');
jest.mock('../copilotRelevance');
jest.mock('../copilotResearch');
jest.mock('../copilotComposer');
jest.mock('../copilotNotifications');
jest.mock('../copilotQuota');
jest.mock('../copilotFollowUp');

const runRepository = require('../copilotRunRepository');
const payloadBuilder = require('../copilotPayloadBuilder');
const fundamentals = require('../copilotFundamentals');
const relevance = require('../copilotRelevance');
const research = require('../copilotResearch');
const composer = require('../copilotComposer');
const notifications = require('../copilotNotifications');
const quota = require('../copilotQuota');
const followUp = require('../copilotFollowUp');

const servicio = require('../copilotAnalysisService');
const { FAILURE_CODE, STEP_KIND } = require('../copilotConstants');
const { RESEARCH_OUTCOME } = require('../copilotFindings');

// ============================================================================
// FÁBRICAS
// ============================================================================

const crearRun = (overrides = {}) => ({
  runId: 'run-1',
  userId: 'u-pro',
  status: 'queued',
  steps: [{ kind: STEP_KIND.CLEANSING, status: 'pending' }],
  stepIndex: 0,
  startedAt: '2026-10-09T10:00:00.000Z',
  stageAt: '2026-10-09T10:00:00.000Z',
  language: 'es',
  currency: 'USD',
  ...overrides,
});

const crearPayload = (overrides = {}) => ({
  portfolioBeta: 1.1,
  riskCategory: 'moderate',
  ytdReturn: 12.5,
  totalPortfolioValue: 48000,
  userCurrency: 'USD',
  userLanguage: 'es',
  concentration: { top3Weight: 55.2, totalAssets: 7, herfindahlIndex: 0.18 },
  isLive: false,
  ...overrides,
});

const crearAsset = (overrides = {}) => ({
  name: 'MSFT',
  units: 10,
  unitValue: 300,
  currency: 'USD',
  ...overrides,
});

const crearHolding = (overrides = {}) => ({
  symbol: 'MSFT',
  weight: 0.5,
  sector: 'Technology',
  asset_type: 'stock',
  currency: 'USD',
  motivo: 'peso',
  ...overrides,
});

const crearInvestigacion = (overrides = {}) => ({
  ticker: 'MSFT',
  outcome: RESEARCH_OUTCOME.OK,
  findings: [{ label: 'Ingresos', value: '65.000 M', period: 'Q3 2026', publishedAt: '2026-10-01', sourceUrl: 'https://x' }],
  notes: [],
  conflicts: [],
  tokensUsed: 1000,
  groundingSources: [],
  ...overrides,
});

const crearInforme = (overrides = {}) => ({
  markdown: '## Resumen ejecutivo\n\n- Todo en orden.\n\n## Hallazgos fundamentales\n',
  executiveSummary: '- Todo en orden.',
  tokensUsed: 2000,
  modelId: 'gemini-2.5-flash',
  sources: [{ n: 1, title: 'Reuters', url: 'https://x', date: null }],
  ...overrides,
});

const ENTORNO = { marketDataTokenSecret: 'secreto' };

/**
 * Deja el camino feliz montado: un portafolio con un activo válido que se
 * investiga bien y un informe que se redacta bien.
 */
function montarCaminoFeliz({ assets = [crearAsset()], priceMap } = {}) {
  const precios = priceMap || new Map([['MSFT', { price: 400, currency: 'USD' }]]);

  payloadBuilder.buildBasePayload.mockResolvedValue({
    sinPosiciones: false,
    payload: crearPayload(),
    estado: { assets, priceMap: precios, rates: new Map([['USD', 1]]), accounts: [] },
  });
  payloadBuilder.buildUserHoldings.mockReturnValue([crearHolding()]);
  relevance.seleccionarRelevantes.mockResolvedValue({
    relevantes: [crearHolding()],
    resto: [],
  });
  fundamentals.obtenerDatosDePlataforma.mockResolvedValue(new Map());
  fundamentals.seriesDeActivos.mockResolvedValue(new Map());
  montarTanda([crearInvestigacion()]);
  composer.redactarInforme.mockResolvedValue(crearInforme());
}

/**
 * HU 5.6: la investigación es UNA llamada que devuelve todos los activos y el
 * macro a la vez. Los tests dicen qué devolvió esa llamada, no qué devolvió
 * cada activo, porque es lo que de verdad ocurre.
 *
 * @param {object[]} investigaciones - Una por activo relevante, en su orden.
 * @param {object} [macro]
 * @param {number} [tokensUsed]
 */
function montarTanda(investigaciones, macro = crearInvestigacion({ ticker: 'macro' }), tokensUsed = 1000) {
  research.investigarTanda.mockResolvedValue({ investigaciones, macro, tokensUsed });
}

beforeEach(() => {
  jest.clearAllMocks();
  documentosEscritos.length = 0;
  documentosBorrados.length = 0;

  runRepository.startStep.mockResolvedValue(true);
  runRepository.finishStep.mockResolvedValue(true);
  runRepository.setSteps.mockResolvedValue(true);
  runRepository.completeRun.mockResolvedValue(true);
  runRepository.failRun.mockResolvedValue(true);
  quota.consumeQuota.mockResolvedValue(undefined);
  followUp.prepararComparacion.mockResolvedValue({
    estado: 'first', referencia: null, recomendaciones: [], tesis: [],
    referenceLine: null, activosARevisar: [],
  });
  followUp.contarSeguimiento.mockReturnValue({
    vigentes: 0, ajustadas: 0, noVigentes: 0, tesisInvalidadas: 0,
  });
  notifications.notifyAnalysisReady.mockResolvedValue(undefined);
  notifications.notifyAnalysisFailed.mockResolvedValue(undefined);

  montarCaminoFeliz();
});

// ============================================================================
// TESTS
// ============================================================================

describe('construirPlan — una etapa por activo (Escenario 6)', () => {
  it('pone un paso por activo relevante, el macro y la redacción', () => {
    const plan = servicio.construirPlan(
      [crearHolding(), crearHolding({ symbol: 'NVDA' })],
      false,
    );

    expect(plan.map((p) => p.kind)).toEqual([
      STEP_KIND.ASSET,
      STEP_KIND.ASSET,
      STEP_KIND.MACRO,
      STEP_KIND.COMPOSING,
    ]);
    expect(plan[0].ticker).toBe('MSFT');
  });

  it('añade "resto del portafolio" sólo cuando lo hay (Escenario 3)', () => {
    expect(servicio.construirPlan([crearHolding()], true).map((p) => p.kind))
      .toContain(STEP_KIND.REST);
    expect(servicio.construirPlan([crearHolding()], false).map((p) => p.kind))
      .not.toContain(STEP_KIND.REST);
  });

  it('declara el motivo sólo cuando el activo no entró por peso (RN-5.3-N)', () => {
    const plan = servicio.construirPlan(
      [crearHolding(), crearHolding({ symbol: 'NVDA', motivo: 'variacion', variacionSemanal: -11.2 })],
      false,
    );

    expect(plan[0].variation).toBeUndefined();
    expect(plan[1].reason).toBe('variacion');
    expect(plan[1].variation).toBe(-11.2);
  });
});

describe('resumenDePaso — lo que se lee al lado de la etapa', () => {
  it('nombra lo que obtuvo', () => {
    const inv = crearInvestigacion({
      findings: [
        { label: 'resultados Q3' }, { label: 'consenso' }, { label: 'noticias' }, { label: 'extra' },
      ],
    });
    expect(servicio.resumenDePaso(inv)).toBe('resultados Q3, consenso, noticias');
  });

  it('con una ausencia, la declara', () => {
    const inv = crearInvestigacion({ outcome: RESEARCH_OUTCOME.SIN_DATOS, findings: [], notes: ['Sin cobertura de analistas'] });
    expect(servicio.resumenDePaso(inv)).toBe('Sin cobertura de analistas');
  });

  it('un paso fallido no lleva resumen: no es una ausencia (RN-5.3-R)', () => {
    expect(servicio.resumenDePaso(crearInvestigacion({ outcome: RESEARCH_OUTCOME.FALLO }))).toBeNull();
  });
});

describe('runAnalysis — el camino feliz', () => {
  it('guarda el análisis con su resumen ejecutivo aparte (Escenario 14)', async () => {
    const resultado = await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(resultado.desenlace).toBe('completed');
    expect(documentosEscritos).toHaveLength(1);
    expect(documentosEscritos[0].datos.executiveSummary).toBe('- Todo en orden.');
    // HU 5.7.3: decidir → corregir → redactar.
    expect(documentosEscritos[0].datos._meta.promptVersion).toBe('v5');
  });

  it('la fecha del análisis es la del lanzamiento (RN-5.2-R)', async () => {
    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(documentosEscritos[0].id).toBe('20261009T100000Z');
    expect(documentosEscritos[0].datos.createdAt).toBe('2026-10-09T10:00:00.000Z');
  });

  it('consume el cupo y avisa', async () => {
    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(quota.consumeQuota).toHaveBeenCalledWith('u-pro');
    expect(notifications.notifyAnalysisReady).toHaveBeenCalled();
  });

  it('suma los tokens de todas las llamadas, no sólo los de la última', async () => {
    await servicio.runAnalysis(crearRun(), ENTORNO);

    // HU 5.6: 1000 de la tanda —una sola llamada para los activos y el
    // macro— + 2000 del informe.
    expect(documentosEscritos[0].datos.tokensUsed).toBe(3000);
  });
});

describe('runAnalysis — la depuración (Escenario 13)', () => {
  it('excluye la posición sin precio y la declara', async () => {
    montarCaminoFeliz({
      assets: [crearAsset(), crearAsset({ name: 'BTC-USD' })],
      priceMap: new Map([['MSFT', { price: 400, currency: 'USD' }]]),
    });

    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(documentosEscritos[0].datos._meta.excludedPositions).toEqual(['BTC-USD']);
    // El modelo nunca llega a ver la posición corrupta.
    const entrada = composer.redactarInforme.mock.calls[0][0];
    expect(entrada.excluidos.map((e) => e.ticker)).toEqual(['BTC-USD']);
  });

  it('si tras depurar no queda nada, el análisis falla sin cobrar (QA-21)', async () => {
    montarCaminoFeliz({
      assets: [crearAsset({ units: -5 })],
      priceMap: new Map(),
    });

    const resultado = await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(resultado.errorCode).toBe(FAILURE_CODE.NO_VALID_POSITIONS);
    expect(quota.consumeQuota).not.toHaveBeenCalled();
    expect(documentosEscritos).toHaveLength(0);
    expect(notifications.notifyAnalysisFailed).toHaveBeenCalled();
  });

  it('los pesos se recalculan sobre lo depurado (QA-20)', async () => {
    montarCaminoFeliz({
      assets: [crearAsset(), crearAsset({ name: 'SPYG' })],
      priceMap: new Map([['MSFT', { price: 400, currency: 'USD' }]]),
    });

    await servicio.runAnalysis(crearRun(), ENTORNO);

    // buildUserHoldings recibe SÓLO las posiciones válidas, no todas.
    const asignados = payloadBuilder.buildUserHoldings.mock.calls[0][0];
    expect(asignados.map((a) => a.name)).toEqual(['MSFT']);
  });
});

describe('runAnalysis — qué tumba el análisis y qué no', () => {
  it('un activo que no se pudo investigar NO tumba el análisis (Escenario 19, QA-30)', async () => {
    montarTanda([crearInvestigacion({ outcome: RESEARCH_OUTCOME.FALLO, findings: [] })]);

    const resultado = await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(resultado.desenlace).toBe('completed');
    expect(documentosEscritos[0].datos._meta.failedAssets).toEqual(['MSFT']);
  });

  it('con todos los activos caídos pero el macro vivo, se entrega (QA-31)', async () => {
    montarTanda(
      [crearInvestigacion({ outcome: RESEARCH_OUTCOME.FALLO, findings: [] })],
      crearInvestigacion({ ticker: 'macro' }),
    );

    const resultado = await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(resultado.desenlace).toBe('completed');
  });

  it('sin ni un solo dato en vivo, falla sin cobrar (Escenario 15, QA-24)', async () => {
    const fallida = crearInvestigacion({ outcome: RESEARCH_OUTCOME.FALLO, findings: [] });
    montarTanda([fallida], { ...fallida, ticker: 'macro' });

    const resultado = await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(resultado.errorCode).toBe(FAILURE_CODE.RESEARCH_UNAVAILABLE);
    expect(quota.consumeQuota).not.toHaveBeenCalled();
    expect(documentosEscritos).toHaveLength(0);
    expect(composer.redactarInforme).not.toHaveBeenCalled();
  });

  it('un macro caído no se le pasa a la redacción como si tuviera datos', async () => {
    montarTanda(
      [crearInvestigacion()],
      crearInvestigacion({ ticker: 'macro', outcome: RESEARCH_OUTCOME.FALLO, findings: [] }),
    );

    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(composer.redactarInforme.mock.calls[0][0].macro).toBeNull();
  });

  it('si la redacción falla, el análisis falla sin dejar parcial', async () => {
    composer.redactarInforme.mockRejectedValue(
      Object.assign(new Error('motor caído'), { code: FAILURE_CODE.ENGINE_FAILED }),
    );

    const resultado = await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(resultado.errorCode).toBe(FAILURE_CODE.ENGINE_FAILED);
    expect(documentosEscritos).toHaveLength(0);
    expect(quota.consumeQuota).not.toHaveBeenCalled();
  });
});

describe('runAnalysis — el análisis dejó de ser el vigente', () => {
  it('abandona en silencio si el plan no se pudo escribir', async () => {
    runRepository.setSteps.mockResolvedValue(false);

    const resultado = await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(resultado.desenlace).toBe('abandoned');
    expect(research.investigarTanda).not.toHaveBeenCalled();
    expect(notifications.notifyAnalysisFailed).not.toHaveBeenCalled();
  });

  it('retira el análisis que llega tarde (QA-21 de la HU 5.2)', async () => {
    runRepository.completeRun.mockResolvedValue(false);

    const resultado = await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(resultado.desenlace).toBe('abandoned');
    expect(documentosBorrados).toEqual(['20261009T100000Z']);
    expect(quota.consumeQuota).not.toHaveBeenCalled();
    expect(notifications.notifyAnalysisReady).not.toHaveBeenCalled();
  });
});

describe('HU 5.6 — la investigación es UNA llamada (Escenario 7)', () => {
  /** Un portafolio con `n` activos relevantes. */
  const conRelevantes = (n) => {
    const holdings = Array.from({ length: n }, (_, i) => crearHolding({ symbol: `T${i}` }));
    relevance.seleccionarRelevantes.mockResolvedValue({ relevantes: holdings, resto: [] });
    payloadBuilder.buildUserHoldings.mockReturnValue(holdings);
    montarTanda(holdings.map((h) => crearInvestigacion({ ticker: h.symbol })));
    return holdings;
  };

  it('seis activos y el macro se investigan con una sola llamada, no con ocho', async () => {
    conRelevantes(6);

    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(research.investigarTanda).toHaveBeenCalledTimes(1);
  });

  it('la tanda recibe todos los activos del plan, en su orden', async () => {
    conRelevantes(6);

    await servicio.runAnalysis(crearRun(), ENTORNO);

    const [relevantes] = research.investigarTanda.mock.calls[0];
    expect(relevantes.map((h) => h.symbol)).toEqual(['T0', 'T1', 'T2', 'T3', 'T4', 'T5']);
  });

  it('cada activo cierra SU paso, aunque la llamada fuera una sola', async () => {
    conRelevantes(6);

    await servicio.runAnalysis(crearRun(), ENTORNO);

    // El paso 0 es la depuración: los activos van del 1 al 6.
    const cerrados = runRepository.finishStep.mock.calls
      .map((c) => c[2])
      .filter((i) => i >= 1 && i <= 6)
      .sort((a, b) => a - b);
    expect(cerrados).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it('los hallazgos conservan el orden del plan', async () => {
    conRelevantes(4);

    await servicio.runAnalysis(crearRun(), ENTORNO);

    const entrada = composer.redactarInforme.mock.calls[0][0];
    expect(entrada.investigaciones.map((i) => i.ticker)).toEqual(['T0', 'T1', 'T2', 'T3']);
  });

  it('el paso del macro se cierra con lo que la tanda entregó bajo su clave', async () => {
    conRelevantes(2);
    montarTanda(
      [crearInvestigacion({ ticker: 'T0' }), crearInvestigacion({ ticker: 'T1' })],
      crearInvestigacion({ ticker: 'macro', outcome: RESEARCH_OUTCOME.FALLO, findings: [] }),
    );

    await servicio.runAnalysis(crearRun(), ENTORNO);

    // Dos activos (1 y 2) y el macro (3): no hay "resto" en este portafolio.
    const pasoMacro = runRepository.finishStep.mock.calls.find((c) => c[2] === 3);
    expect(pasoMacro[3].ok).toBe(false);
  });

  it('los datos de la plataforma se piden ANTES de investigar (RN-5.6-A)', async () => {
    const holdings = conRelevantes(2);

    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(fundamentals.obtenerDatosDePlataforma).toHaveBeenCalledWith(
      holdings, { language: 'es' },
    );
    const [, , datos] = research.investigarTanda.mock.calls[0];
    expect(datos).toBeInstanceOf(Map);
  });

  it('el consumo registrado es el de la llamada, no la suma por activo', async () => {
    conRelevantes(6);
    montarTanda(
      Array.from({ length: 6 }, (_, i) => crearInvestigacion({ ticker: `T${i}` })),
      crearInvestigacion({ ticker: 'macro' }),
      9000,
    );

    await servicio.runAnalysis(crearRun(), ENTORNO);

    // 9.000 de la tanda + 2.000 de la redacción. Sumar los 1.000 de cada
    // investigación contaría seis veces el mismo prompt.
    expect(documentosEscritos[0].datos.tokensUsed).toBe(11000);
  });
});

describe('HU 5.4 — la comparación contra el análisis anterior', () => {
  const comparacionCompleta = {
    estado: 'compared',
    referencia: { id: '20260912T100000Z', createdAt: '2026-09-12T10:00:00.000Z' },
    recomendaciones: [
      { statement: 'Reducir NVDA al 8 %', asset: 'NVDA', conviction: 'alta',
        targetPct: 8, priceZone: null, horizon: '3-6 meses',
        closedState: null, closedReason: null },
      { statement: 'Aumentar INTC al 5 %', asset: 'INTC', conviction: 'media',
        targetPct: 5, priceZone: null, horizon: null,
        closedState: 'no_vigente', closedReason: 'activo_ausente' },
    ],
    tesis: [{ statement: 'La nube sostiene MSFT', asset: 'MSFT' }],
    referenceLine: { fecha: '12 sept 2026', transcurrido: '28 días', stale: false, dias: 28 },
    activosARevisar: ['NVDA'],
  };

  beforeEach(() => {
    followUp.prepararComparacion.mockResolvedValue(comparacionCompleta);
  });

  it('consulta el análisis anterior ANTES de decidir qué investigar (RN-5.4-X)', async () => {
    await servicio.runAnalysis(crearRun(), ENTORNO);

    // Lo que se recomendó amplía la lista de activos relevantes.
    expect(relevance.seleccionarRelevantes).toHaveBeenCalledWith(
      expect.anything(), expect.any(Date), ['NVDA'],
    );
  });

  it('le pasa al redactor las recomendaciones ya cerradas como tales (D4)', async () => {
    await servicio.runAnalysis(crearRun(), ENTORNO);

    const comparacion = composer.redactarInforme.mock.calls[0][0].comparacion;
    expect(comparacion.state).toBe('compared');
    expect(comparacion.recommendations[1].closedState).toBe('no_vigente');
    expect(comparacion.recommendations[1].closedReason).toBe('activo_ausente');
  });

  it('guarda la comparación hecha, con sus recuentos (RN-5.4-V)', async () => {
    followUp.contarSeguimiento.mockReturnValue({
      vigentes: 1, ajustadas: 0, noVigentes: 1, tesisInvalidadas: 0,
    });

    await servicio.runAnalysis(crearRun(), ENTORNO);

    const guardado = documentosEscritos[0].datos.comparison;
    expect(guardado.state).toBe('compared');
    expect(guardado.referenceId).toBe('20260912T100000Z');
    expect(guardado.followUp.noVigentes).toBe(1);
  });

  /**
   * Con las funciones de verdad, no con el mock. El resto del archivo mockea
   * `copilotFollowUp` entero, y eso es justo lo que dejaba pasar que el
   * recuento se calculara sobre unas recomendaciones que todavía no tienen
   * estado final: todo a cero, y la tarjeta sin su línea.
   */
  const conElRecuentoDeVerdad = () => {
    const real = jest.requireActual('../copilotFollowUp');
    followUp.contarSeguimiento.mockImplementation(real.contarSeguimiento);
  };

  it('el recuento sale de los estados ya calculados que devuelve la redacción (HU 5.7.3)', async () => {
    conElRecuentoDeVerdad();
    composer.redactarInforme.mockResolvedValue(crearInforme({
      followUp: {
        recommendations: [
          { id: 'R1', asset: 'NVDA', state: 'ajustada' },
          // La que el portafolio cerró de entrada llega ya con su estado.
          { id: 'R2', asset: 'INTC', state: 'no_vigente' },
        ],
        theses: [{ id: 'T1', asset: 'MSFT', state: 'invalidada' }],
      },
    }));

    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(documentosEscritos[0].datos.comparison.followUp).toEqual({
      vigentes: 0, ajustadas: 1, noVigentes: 1, tesisInvalidadas: 1,
    });
  });

  it('una redacción sin seguimiento no inventa vigencias', async () => {
    conElRecuentoDeVerdad();
    composer.redactarInforme.mockResolvedValue(crearInforme());

    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(documentosEscritos[0].datos.comparison.followUp).toEqual({
      vigentes: 0, ajustadas: 0, noVigentes: 0, tesisInvalidadas: 0,
    });
  });

  it('guarda las recomendaciones y tesis extraídas, para el análisis siguiente', async () => {
    composer.redactarInforme.mockResolvedValue(crearInforme({
      recommendations: [{ statement: 'Reducir MSFT al 12 %', asset: 'MSFT' }],
      theses: [{ statement: 'La nube aguanta', asset: 'MSFT' }],
    }));

    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(documentosEscritos[0].datos.recommendations).toHaveLength(1);
    expect(documentosEscritos[0].datos.theses).toHaveLength(1);
  });

  it('una extracción que falló se guarda vacía, no se inventa', async () => {
    composer.redactarInforme.mockResolvedValue(crearInforme());

    await servicio.runAnalysis(crearRun(), ENTORNO);

    // El siguiente análisis lo tratará como formato antiguo, que es lo que es.
    expect(documentosEscritos[0].datos.recommendations).toEqual([]);
    expect(documentosEscritos[0].datos.theses).toEqual([]);
  });

  it('no poder consultar el anterior NO tumba el análisis (Escenario 13)', async () => {
    followUp.prepararComparacion.mockResolvedValue({
      estado: 'unavailable', referencia: null, recomendaciones: [], tesis: [],
      referenceLine: null, activosARevisar: [],
    });

    const resultado = await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(resultado.desenlace).toBe('completed');
    expect(documentosEscritos[0].datos.comparison.state).toBe('unavailable');
  });

  it('el primer análisis no añade activos por recomendaciones que no existen', async () => {
    followUp.prepararComparacion.mockResolvedValue({
      estado: 'first', referencia: null, recomendaciones: [], tesis: [],
      referenceLine: null, activosARevisar: [],
    });

    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(relevance.seleccionarRelevantes).toHaveBeenCalledWith(
      expect.anything(), expect.any(Date), [],
    );
    expect(documentosEscritos[0].datos.comparison.state).toBe('first');
  });
});

describe('HU 6.11.2 — la moneda pedida no estaba disponible (Escenario 5)', () => {
  const conMonedaNoDisponible = () => {
    payloadBuilder.buildBasePayload.mockResolvedValue({
      sinPosiciones: false,
      payload: crearPayload({ userCurrency: 'USD', unavailableCurrency: 'EUR' }),
      estado: {
        assets: [crearAsset()],
        priceMap: new Map([['MSFT', { price: 400, currency: 'USD' }]]),
        rates: new Map([['USD', 1]]),
        accounts: [],
      },
    });
  };

  it('el informe empieza con la nota fija y el redactor sabe qué moneda faltó (QA-14, QA-15)', async () => {
    conMonedaNoDisponible();

    await servicio.runAnalysis(crearRun({ currency: 'EUR' }), ENTORNO);

    const markdown = documentosEscritos[0].datos.analysisMarkdown;
    expect(markdown.startsWith('> Este análisis está en USD porque EUR no está disponible por ahora.\n\n## Resumen ejecutivo')).toBe(true);
    expect(composer.redactarInforme.mock.calls[0][0].portafolio).toEqual({
      currency: 'USD', totalAssets: 1, unavailableCurrency: 'EUR',
    });
    expect(documentosEscritos[0].datos.payloadSummary.currency).toBe('USD');
  });

  it('en inglés, la nota va en inglés', async () => {
    conMonedaNoDisponible();

    await servicio.runAnalysis(crearRun({ currency: 'EUR', language: 'en' }), ENTORNO);

    expect(documentosEscritos[0].datos.analysisMarkdown)
      .toMatch(/^> This analysis is in USD because EUR isn't available right now\.\n\n/);
  });

  it('con la moneda disponible el informe no lleva nota ni el contexto la menciona (QA-17)', async () => {
    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(documentosEscritos[0].datos.analysisMarkdown).toBe(crearInforme().markdown);
    expect(composer.redactarInforme.mock.calls[0][0].portafolio).not.toHaveProperty('unavailableCurrency');
  });

  it('conNotaDeMoneda: sin moneda no disponible devuelve el informe tal cual; con idioma desconocido, en español', () => {
    expect(servicio.conNotaDeMoneda('# Informe', null, 'es')).toBe('# Informe');
    expect(servicio.conNotaDeMoneda('# Informe', 'COP', 'fr'))
      .toBe('> Este análisis está en USD porque COP no está disponible por ahora.\n\n# Informe');
  });
});

// ============================================================================
// HU 5.7.1 — El análisis guarda su criterio y la ficha llega al redactor
// ============================================================================

describe('HU 5.7.1 — el criterio de decisión con que se generó (QA-23)', () => {
  beforeEach(() => montarCaminoFeliz());

  it('el análisis guardado lleva el criterio vigente (5.7.3)', async () => {
    await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(documentosEscritos[0].datos.decisionCriteria).toBe('5.7.3');
  });

  it('la ficha de cada activo viaja al redactor junto a su investigación', async () => {
    const ficha = { kind: 'stock', asOf: '2026-10-08', items: [], missing: [], signals: {} };
    fundamentals.obtenerDatosDePlataforma.mockResolvedValue(new Map([
      ['MSFT', { findings: [], cubierto: true, card: ficha }],
    ]));

    await servicio.runAnalysis(crearRun(), ENTORNO);

    const entrada = composer.redactarInforme.mock.calls[0][0];
    expect(entrada.investigaciones[0].card).toBe(ficha);
  });

  it('sin datos de la plataforma, el activo va sin ficha y el análisis sigue', async () => {
    fundamentals.obtenerDatosDePlataforma.mockRejectedValue(new Error('finance-query caído'));

    const resultado = await servicio.runAnalysis(crearRun(), ENTORNO);

    expect(resultado.desenlace).toBe('completed');
    expect(composer.redactarInforme.mock.calls[0][0].investigaciones[0].card).toBeNull();
  });
});

// ============================================================================
// HU 5.7.3 — El seguimiento con precio y criterio
// ============================================================================

describe('HU 5.7.3 — preciosDelSeguimiento: el precio de hoy y las series (RN-5.7-L)', () => {
  const serieCon = (cierres) => Object.fromEntries(Object.entries(cierres).map(([f, c]) => [f, { close: c }]));

  it('toma el precio del portafolio; si falta, el de la ficha; y pide la serie de lo que no tiene ficha', async () => {
    fundamentals.seriesDeActivos.mockResolvedValue(new Map([['INTC', serieCon({ '2026-11-18': 21, '2026-11-19': 22.5 })]]));

    const { preciosHoy, series } = await servicio.preciosDelSeguimiento({
      holdings: [crearHolding({ symbol: 'MSFT' }), crearHolding({ symbol: 'NVDA' })],
      priceMap: new Map([['MSFT', { price: 400 }]]),
      datosDePlataforma: new Map([
        ['MSFT', { card: { price: 395 }, serie: { a: 1 } }],
        ['NVDA', { card: { price: 145.98 }, serie: { b: 2 } }],
      ]),
      comparacion: { recomendaciones: [{ asset: 'NVDA' }, { asset: 'INTC' }] },
    });

    expect(preciosHoy.get('MSFT')).toBe(400);
    expect(preciosHoy.get('NVDA')).toBe(145.98);
    expect(preciosHoy.get('INTC')).toBe(22.5);
    expect(fundamentals.seriesDeActivos).toHaveBeenCalledWith(['INTC']);
    expect(series.get('NVDA')).toEqual({ b: 2 });
  });

  it('un historial caído no tumba nada: el seguimiento dirá "no disponible"', async () => {
    fundamentals.seriesDeActivos.mockRejectedValue(new Error('finance-query caído'));

    const { preciosHoy } = await servicio.preciosDelSeguimiento({
      holdings: [],
      priceMap: new Map(),
      datosDePlataforma: new Map(),
      comparacion: { recomendaciones: [{ asset: 'INTC' }] },
    });

    expect(preciosHoy.size).toBe(0);
  });
});

describe('HU 5.7.3 — crearSeguimiento: el código corrige y calcula antes de redactar', () => {
  const real = jest.requireActual('../copilotFollowUp');

  beforeEach(() => {
    followUp.cerrarPorCriterio.mockImplementation(real.cerrarPorCriterio);
    followUp.estadosCalculados.mockImplementation(real.estadosCalculados);
    followUp.estadosDeTesis.mockImplementation(real.estadosDeTesis);
  });

  const fichaNflx = {
    kind: 'stock',
    signals: {
      weightPct: 0.6,
      overweightThresholdPct: 10,
      overweight: false,
      roomToThresholdPct: 9.4,
      maxTranchePct: 3.1,
      extremeValuation: null,
      deterioration: [],
      opportunityException: null,
      allowedActions: ['mantener', 'aumentar', 'comprar'],
      level1Eligible: false,
    },
  };

  const contexto = () => ({
    assets: [{ ticker: 'NFLX', card: fichaNflx }],
    comparison: {
      state: 'compared',
      recommendations: [{
        id: 'R1', statement: 'Reducir NFLX del 0,6 % al 0,3 %', asset: 'NFLX', action: 'reducir', closedState: null,
      }],
      theses: [{ id: 'T1', statement: 'El streaming madura', asset: 'NFLX' }],
    },
  });

  const seguimiento = () => servicio.crearSeguimiento({
    preciosHoy: new Map([['NFLX', 98.4]]),
    pesos: new Map([['NFLX', 0.6]]),
    language: 'es',
  });

  it('QA-10/QA-03: una reducción que la ficha ya no permite llega cerrada por criterio a la decisión', () => {
    const preparado = seguimiento().antesDeDecidir(contexto());

    expect(preparado.comparison.recommendations[0]).toMatchObject({
      closedState: 'no_vigente', closedReason: 'criterio',
    });
  });

  it('QA-11 + QA-10: corrige los tramos, guarda el precio y calcula el estado de la anterior', () => {
    const s = seguimiento();
    const contextoPreparado = s.antesDeDecidir(contexto());

    const resuelto = s.alDecidir({
      recommendations: [{
        statement: 'Aumentar NFLX', asset: 'NFLX', action: 'aumentar', targetPct: 3, conviction: 'media',
        tranches: [{ zone: 'Z1', pct: 3.1 }], pillar: 'valoracion', level: null,
      }],
      theses: [{ statement: 'El streaming sigue creciendo', asset: 'NFLX' }],
      // El modelo dice "vigente": el código no le hace caso.
      followUp: { recommendations: [{ id: 'R1', state: 'vigente' }], theses: [{ id: 'T1', state: 'sin_cambio' }] },
    }, contextoPreparado);

    expect(resuelto.recommendations[0]).toMatchObject({
      statement: 'Aumentar NFLX por tramos del 0,6 % al 3 % del portafolio',
      priceAtIssue: 98.4,
    });
    expect(resuelto.recommendations[0].tranches.map((t) => t.pct)).toEqual([0.8, 0.8, 0.8]);
    expect(resuelto.followUp.recommendations[0]).toMatchObject({ state: 'no_vigente', stateReason: 'criterio' });
    expect(resuelto.followUp.theses[0].state).toBe('sin_cambio');
    expect(resuelto.comparison.recommendations[0].state).toBe('no_vigente');
    expect(resuelto.corrections.map((c) => c.rule)).toContain('RN-5.7-Q');
  });

  it('sin comparación (primer análisis) no hay estados que calcular, pero sí correcciones y precios', () => {
    const resuelto = seguimiento().alDecidir(
      { recommendations: [{ statement: 'Mantener NFLX', asset: 'NFLX', action: 'mantener' }], theses: [], followUp: null },
      { assets: [{ ticker: 'NFLX', card: fichaNflx }], comparison: { state: 'first', recommendations: [], theses: [] } },
    );

    expect(resuelto.followUp).toEqual({ recommendations: [], theses: [] });
    expect(resuelto.recommendations[0].priceAtIssue).toBe(98.4);
  });
});

describe('HU 5.7.3 — runAnalysis guarda lo decidido y lo corregido', () => {
  beforeEach(() => montarCaminoFeliz());

  it('QA-02 (paso 1): cada recomendación guardada lleva su precio de emisión', async () => {
    composer.redactarInforme.mockResolvedValue(crearInforme({
      recommendations: [{ statement: 'Mantener MSFT', asset: 'MSFT', priceAtIssue: 400 }],
      decisionPath: 'decidida',
      corrections: [{ asset: 'MSFT', rule: 'RN-5.7-T', field: 'conviction', from: 'alta', to: 'baja' }],
    }));

    await servicio.runAnalysis(crearRun(), ENTORNO);

    const guardado = documentosEscritos[0].datos;
    expect(guardado.recommendations[0].priceAtIssue).toBe(400);
    expect(guardado._meta.decisionPath).toBe('decidida');
    expect(guardado._meta.guardCorrections).toEqual([
      { asset: 'MSFT', rule: 'RN-5.7-T', field: 'conviction', from: 'alta', to: 'baja' },
    ]);
  });

  it('le pasa al composer los ganchos del seguimiento', async () => {
    await servicio.runAnalysis(crearRun(), ENTORNO);

    const ganchos = composer.redactarInforme.mock.calls[0][2];
    expect(typeof ganchos.antesDeDecidir).toBe('function');
    expect(typeof ganchos.alDecidir).toBe('function');
  });

  it('la comparación que ve el redactor lleva ids, acción, precio y si el criterio es nuevo', async () => {
    const real = jest.requireActual('../copilotFollowUp');
    followUp.precioDeRecomendacion.mockImplementation(real.precioDeRecomendacion);
    followUp.prepararComparacion.mockResolvedValue({
      estado: 'compared',
      referencia: { id: 'A-1110', createdAt: '2026-11-10T15:00:00.000Z' },
      recomendaciones: [{
        id: 'R1', statement: 'Reducir la posición de NVDA del 7 % al 3 %', asset: 'MSFT', closedState: null,
      }],
      tesis: [{ id: 'T1', statement: 'La nube sostiene MSFT', asset: 'MSFT' }],
      referenceLine: { fecha: '10 nov 2026', transcurrido: '9 días', stale: false, dias: 9 },
      activosARevisar: ['MSFT'],
      criterionChanged: true,
    });
    fundamentals.obtenerDatosDePlataforma.mockResolvedValue(new Map([
      ['MSFT', { card: null, serie: { '2026-11-10': { close: 380 } } }],
    ]));

    await servicio.runAnalysis(crearRun(), ENTORNO);

    const { comparacion } = composer.redactarInforme.mock.calls[0][0];
    expect(comparacion.criterionChanged).toBe(true);
    expect(comparacion.recommendations[0]).toMatchObject({ id: 'R1', action: 'reducir' });
    expect(comparacion.recommendations[0].price.line).toBe('380,00 → 400,00 (+5,3 %) · cierre del 10 nov 2026');
    expect(comparacion.theses[0].id).toBe('T1');
  });
});
