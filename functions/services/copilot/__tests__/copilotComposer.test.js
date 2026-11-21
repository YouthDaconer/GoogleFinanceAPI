/**
 * HU 5.3 — Lo que ve la redacción final, y lo que devuelve.
 *
 * Dos cosas importan aquí:
 *
 * 1. **La redacción sólo ve hechos verificados y fuentes numeradas.** Es lo que
 *    permite exigirle que cite `[3]` y que la sección Fuentes coincida.
 * 2. **El resumen ejecutivo se devuelve aparte**, porque la tarjeta del
 *    historial tiene que mostrarlo (Escenario 14) y hoy recorta los primeros
 *    150 caracteres del markdown.
 *
 * @see services/copilot/copilotComposer.js
 */

jest.mock('../copilotEngineClient');
jest.mock('../copilotSources');

const engineClient = require('../copilotEngineClient');
const sources = require('../copilotSources');
const composer = require('../copilotComposer');
const { RESEARCH_OUTCOME } = require('../copilotFindings');

const crearInvestigacion = (overrides = {}) => ({
  ticker: 'MSFT',
  name: 'Microsoft',
  outcome: RESEARCH_OUTCOME.OK,
  motivo: 'peso',
  findings: [{
    label: 'Ingresos',
    value: '65.000 M',
    period: 'Q3 2026',
    publishedAt: '2026-10-01',
    sourceUrl: 'https://reuters.com/x',
    sourceTitle: 'Reuters',
  }],
  notes: [],
  conflicts: [],
  ...overrides,
});

const ENTORNO = { userId: 'u-pro', secret: 'secreto', language: 'es' };

beforeEach(() => {
  jest.clearAllMocks();
  sources.construirListaDeFuentes.mockResolvedValue([
    { n: 1, title: 'Reuters', url: 'https://reuters.com/x', date: '2026-10-01', verificada: true },
  ]);
  sources.construirIndice.mockResolvedValue(new Map([['https://reuters.com/x', 1]]));
  sources.dominiosRespaldados.mockResolvedValue(new Set(['reuters.com']));
  engineClient.redactar.mockResolvedValue({
    text: '## Resumen ejecutivo\n\n- Concentración alta en tecnología.\n\n## Hallazgos fundamentales\n\nTexto.',
    tokensUsed: 2000,
    modelId: 'gemini-2.5-flash',
  });
});

describe('extraerResumenEjecutivo', () => {
  it('recorta la sección en español', () => {
    const markdown = '## Resumen ejecutivo\n\n- Uno\n- Dos\n\n## Hallazgos fundamentales\n\nOtra cosa';
    expect(composer.extraerResumenEjecutivo(markdown)).toBe('- Uno\n- Dos');
  });

  it('recorta la sección en inglés', () => {
    const markdown = '## Executive summary\n\n- One\n\n## Key findings\n\nMore';
    expect(composer.extraerResumenEjecutivo(markdown)).toBe('- One');
  });

  it('con el resumen al final, llega hasta el final', () => {
    expect(composer.extraerResumenEjecutivo('## Resumen ejecutivo\n\n- Único')).toBe('- Único');
  });

  it('sin la sección devuelve vacío, y la tarjeta cae a su recorte de siempre', () => {
    expect(composer.extraerResumenEjecutivo('## Otra cosa\n\nTexto')).toBe('');
    expect(composer.extraerResumenEjecutivo(null)).toBe('');
  });
});

describe('construirContexto — lo que ve la redacción', () => {
  const entrada = () => ({
    investigaciones: [crearInvestigacion()],
    macro: null,
    resto: null,
    excluidos: [{ ticker: 'BTC-USD', motivo: 'sin_precio' }],
    portafolio: { currency: 'USD', totalAssets: 7 },
  });

  it('cada hallazgo lleva el número de su fuente, no su URL', async () => {
    const { context } = await composer.construirContexto(entrada());

    expect(context.assets[0].findings[0].source).toBe(1);
    expect(context.assets[0].findings[0].sourceUrl).toBeUndefined();
  });

  it('conserva el periodo y la fecha de cada cifra (RN-5.3-Q)', async () => {
    const { context } = await composer.construirContexto(entrada());

    expect(context.assets[0].findings[0].period).toBe('Q3 2026');
    expect(context.assets[0].findings[0].publishedAt).toBe('2026-10-01');
  });

  it('declara el motivo sólo cuando el activo no entró por peso (RN-5.3-N)', async () => {
    const porPeso = await composer.construirContexto(entrada());
    expect(porPeso.context.assets[0].includedBecause).toBeNull();

    const porVariacion = await composer.construirContexto({
      ...entrada(),
      investigaciones: [crearInvestigacion({ motivo: 'variacion', variacionSemanal: -11.2 })],
    });
    expect(porVariacion.context.assets[0].includedBecause).toEqual({
      reason: 'variacion',
      weeklyChangePct: -11.2,
    });
  });

  it('marca el activo que no se pudo investigar (Escenario 19)', async () => {
    const { context } = await composer.construirContexto({
      ...entrada(),
      investigaciones: [crearInvestigacion({ outcome: RESEARCH_OUTCOME.FALLO, findings: [] })],
    });

    expect(context.assets[0].researched).toBe(false);
  });

  it('pasa las posiciones excluidas para la línea de depuración (Escenario 13)', async () => {
    const { context } = await composer.construirContexto(entrada());

    expect(context.excluded).toEqual([{ ticker: 'BTC-USD', motivo: 'sin_precio' }]);
  });

  it('el macro entra como un apartado más, no como otra cosa', async () => {
    const { context } = await composer.construirContexto({
      ...entrada(),
      macro: crearInvestigacion({ ticker: 'macro', name: 'macro' }),
    });

    expect(context.macro.ticker).toBe('macro');
    expect(context.macro.findings[0].source).toBe(1);
  });
});

describe('redactarInforme', () => {
  it('devuelve el markdown y el resumen ejecutivo por separado', async () => {
    const informe = await composer.redactarInforme({
      investigaciones: [crearInvestigacion()],
      macro: null,
      resto: null,
      excluidos: [],
      portafolio: { currency: 'USD', totalAssets: 7 },
    }, ENTORNO);

    expect(informe.markdown).toContain('## Resumen ejecutivo');
    expect(informe.executiveSummary).toBe('- Concentración alta en tecnología.');
    expect(informe.sources).toHaveLength(1);
  });

  it('redacta sin buscar: usa el modo que no tiene búsqueda web', async () => {
    await composer.redactarInforme({
      investigaciones: [crearInvestigacion()],
      macro: null,
      resto: null,
      excluidos: [],
      portafolio: {},
    }, ENTORNO);

    // Separar investigar de redactar es lo que impide que la síntesis invente
    // una cifra que ninguna etapa obtuvo.
    expect(engineClient.redactar).toHaveBeenCalled();
    expect(engineClient.investigar).not.toHaveBeenCalled();
  });
});

describe('normalizarSeguimiento — HU 5.4, los estados de la sección 2', () => {
  it('conserva el enunciado, el activo y el estado', () => {
    const salida = composer.normalizarSeguimiento({
      recommendations: [{ statement: 'Reducir NVDA al 8 %', asset: 'NVDA', state: 'ajustada' }],
      theses: [{ statement: 'La nube sostiene MSFT', asset: 'MSFT', state: 'invalidada' }],
    });

    expect(salida.recommendations).toEqual([
      { statement: 'Reducir NVDA al 8 %', asset: 'NVDA', state: 'ajustada' },
    ]);
    expect(salida.theses[0].state).toBe('invalidada');
  });

  it('descarta un estado que no es de los suyos', () => {
    // Contarlo como vigente sería afirmar lo que el informe no dijo.
    const salida = composer.normalizarSeguimiento({
      recommendations: [
        { statement: 'Reducir NVDA', asset: 'NVDA', state: 'quizá' },
        { statement: 'Mantener MSFT', asset: 'MSFT', state: 'vigente' },
      ],
      theses: [{ statement: 'Algo', state: 'confirmada' }],
    });

    expect(salida.recommendations.map((r) => r.statement)).toEqual(['Mantener MSFT']);
  });

  it('un vocabulario de recomendación no vale para una tesis, ni al revés', () => {
    const salida = composer.normalizarSeguimiento({
      recommendations: [{ statement: 'Reducir NVDA', state: 'confirmada' }],
      theses: [{ statement: 'La nube sostiene MSFT', state: 'vigente' }],
    });

    expect(salida).toEqual({ recommendations: [], theses: [] });
  });

  it('sin sección que leer, dos listas vacías', () => {
    expect(composer.normalizarSeguimiento(undefined))
      .toEqual({ recommendations: [], theses: [] });
    expect(composer.normalizarSeguimiento({ recommendations: 'no es una lista' }))
      .toEqual({ recommendations: [], theses: [] });
  });

  it('una extracción que falló deja el seguimiento vacío, no ausente', async () => {
    // Quien lo consume cuenta con las dos listas: que falten no puede hacerle
    // nada distinto a que estén vacías.
    engineClient.redactar.mockRejectedValueOnce(new Error('timeout'));

    const salida = await composer.extraerEstructura('## Informe', ENTORNO);

    expect(salida.followUp).toEqual({ recommendations: [], theses: [] });
  });
});

describe('RN-5-E — la procedencia llega al informe', () => {
  const conFuentes = (investigaciones) => composer.construirContexto({
    investigaciones,
    macro: null,
    resto: null,
    excluidos: [],
    portafolio: { currency: 'USD' },
  })

  it('contrasta lo que el modelo citó contra lo que el grounding visitó', async () => {
    // `groundingSources` se guardaba en cada investigación y no lo leía nadie:
    // las fuentes del informe salían de los `sourceUrl` del propio modelo, que
    // es justo lo que la regla quiere impedir.
    await conFuentes([crearInvestigacion({
      groundingSources: [{ url: 'https://vertexaisearch.cloud.google.com/redirect/abc' }],
    })])

    expect(sources.dominiosRespaldados).toHaveBeenCalledWith(
      [{ url: 'https://vertexaisearch.cloud.google.com/redirect/abc' }],
    )
    expect(sources.construirListaDeFuentes).toHaveBeenCalledWith(
      expect.any(Array), new Set(['reuters.com']),
    )
  })

  it('reúne el respaldo de todas las etapas, incluida la macro', async () => {
    await composer.construirContexto({
      investigaciones: [crearInvestigacion({ groundingSources: [{ url: 'https://a' }] })],
      macro: crearInvestigacion({ ticker: 'macro', groundingSources: [{ url: 'https://b' }] }),
      resto: null,
      excluidos: [],
      portafolio: {},
    })

    expect(sources.dominiosRespaldados)
      .toHaveBeenCalledWith([{ url: 'https://a' }, { url: 'https://b' }])
  })

  it('el contexto dice de cada fuente si está verificada', async () => {
    sources.construirListaDeFuentes.mockResolvedValue([
      { n: 1, title: 'Morningstar', url: 'https://morningstar.com/x', date: null, verificada: true },
      { n: 2, title: 'Reuters', url: 'https://reuters.com/y', date: null, verificada: false },
    ])

    const { context } = await conFuentes([crearInvestigacion()])

    expect(context.sources).toEqual([
      { n: 1, title: 'Morningstar', url: 'https://morningstar.com/x', date: null, verified: true },
      { n: 2, title: 'Reuters', url: 'https://reuters.com/y', date: null, verified: false },
    ])
  })

  it('una investigación sin grounding no aporta respaldo', async () => {
    await conFuentes([crearInvestigacion()])

    expect(sources.dominiosRespaldados).toHaveBeenCalledWith([])
  })
})

describe('HU 5.6 — la marca de procedencia viaja del hallazgo a la fuente', () => {
  it('un dato de la plataforma llega a la lista de fuentes ya verificado', async () => {
    sources.dominiosRespaldados.mockResolvedValue(new Set());
    sources.construirListaDeFuentes.mockImplementation(async (crudas) =>
      crudas.map((f, i) => ({ n: i + 1, title: f.title, url: f.url, date: f.date, verificada: f.verified === true })));
    sources.construirIndice.mockResolvedValue(new Map([['https://api.portastock.net', 1]]));

    await composer.construirContexto({
      investigaciones: [crearInvestigacion({
        findings: [{
          label: 'Ingresos',
          value: '70.00 B USD',
          period: 'Q2 2026',
          publishedAt: '2026-06-30',
          sourceUrl: 'https://api.portastock.net',
          sourceTitle: 'Portastock — datos de la plataforma (finance-query)',
          verified: true,
        }],
      })],
      macro: null,
      resto: null,
      excluidos: [],
      portafolio: { currency: 'USD', totalAssets: 1 },
    });

    const [crudas] = sources.construirListaDeFuentes.mock.calls[0];
    expect(crudas[0].verified).toBe(true);
  });

  it('un hallazgo del modelo no se cuela como verificado', async () => {
    sources.dominiosRespaldados.mockResolvedValue(new Set());
    sources.construirListaDeFuentes.mockResolvedValue([]);
    sources.construirIndice.mockResolvedValue(new Map());

    await composer.construirContexto({
      investigaciones: [crearInvestigacion()],
      macro: null,
      resto: null,
      excluidos: [],
      portafolio: { currency: 'USD', totalAssets: 1 },
    });

    const [crudas] = sources.construirListaDeFuentes.mock.calls[0];
    expect(crudas[0].verified).toBe(false);
  });
});

// ============================================================================
// HU 5.7.1 — La ficha llega al redactor y las recomendaciones vuelven con su criterio
// ============================================================================

describe('HU 5.7.1 — la ficha llega con la fuente de la plataforma (QA-01, QA-03)', () => {
  const PLATAFORMA = 'https://api.portastock.net';
  const FICHA = {
    kind: 'crypto',
    asOf: '2026-11-19',
    items: [{ label: 'Precio (último cierre)', value: '61914.00', period: '2026-11-19' }],
    missing: [],
    signals: { weightPct: 11.5, overweight: true, roomToThresholdPct: 0 },
  };
  const entrada = (overrides = {}) => ({
    investigaciones: [crearInvestigacion({ ticker: 'BTC-USD', findings: [], card: FICHA, ...overrides })],
    macro: null,
    resto: null,
    excluidos: [],
    portafolio: { currency: 'USD', totalAssets: 1 },
    now: new Date('2026-11-20T03:00:00Z'),
  });

  beforeEach(() => {
    sources.dominiosRespaldados.mockResolvedValue(new Set());
    sources.construirListaDeFuentes.mockImplementation(async (crudas) =>
      crudas.map((f, i) => ({ n: i + 1, title: f.title, url: f.url, date: f.date, verificada: f.verified === true })));
    sources.construirIndice.mockResolvedValue(new Map([[PLATAFORMA, 1]]));
  });

  it('una cripto sin hallazgos de la plataforma estrena su fuente, verificada', async () => {
    await composer.construirContexto({ ...entrada(), language: 'en' });

    const [crudas] = sources.construirListaDeFuentes.mock.calls[0];
    expect(crudas).toEqual([{
      title: 'Portastock — platform data (finance-query)',
      url: PLATAFORMA,
      date: '2026-11-19',
      verified: true,
    }]);
  });

  it('la ficha viaja entera, con el número de su fuente', async () => {
    const { context } = await composer.construirContexto(entrada());

    expect(context.assets[0].card).toEqual({ ...FICHA, source: 1 });
  });

  it('sin ficha, el activo lleva card: null y no añade fuente', async () => {
    const { context } = await composer.construirContexto(entrada({ card: undefined }));

    expect(context.assets[0].card).toBeNull();
    expect(sources.construirListaDeFuentes.mock.calls[0][0]).toEqual([]);
  });

  it('el redactor sabe qué día es hoy en Nueva York, para la ventana de 30 días', async () => {
    // 03:00 UTC del 20 son las 22:00 del 19 en Nueva York.
    const { context } = await composer.construirContexto(entrada());

    expect(context.today).toBe('2026-11-19');
  });

  it('el efecto en la tesis de cada hallazgo llega al redactor (QA-02)', async () => {
    const { context } = await composer.construirContexto(entrada({
      findings: [{ ...crearInvestigacion().findings[0], impact: 'ruido' }],
    }));

    expect(context.assets[0].findings[0].impact).toBe('ruido');
  });
});

describe('HU 5.7.1 — normalizarRecomendaciones: acción, pilar, nivel y tramos (QA-22)', () => {
  const base = { statement: 'Reducir BTC-USD hacia el 10 %', asset: 'BTC-USD' };
  const normalizar = (extra) => composer.normalizarRecomendaciones([{ ...base, ...extra }])[0];

  it('conserva lo que el informe declara', () => {
    expect(normalizar({ action: 'reducir', pillar: 'valoracion', level: 3 })).toMatchObject({
      action: 'reducir', pillar: 'valoracion', level: 3, tranches: [],
    });
  });

  it.each([0, 4, '3', 2.5, null])('un nivel %p queda null', (level) => {
    expect(normalizar({ action: 'vender', level }).level).toBeNull();
  });

  it.each(['mantener', 'aumentar', 'comprar'])('una recomendación de %s no lleva nivel aunque lo escriba', (action) => {
    expect(normalizar({ action, level: 2 }).level).toBeNull();
  });

  it('sin acción legible el nivel válido se conserva: no se puede decir que sobre', () => {
    expect(normalizar({ action: 'liquidar', level: 1 })).toMatchObject({ action: null, level: 1 });
  });

  it('un pilar fuera de la lista queda null', () => {
    expect(normalizar({ pillar: 'intuición' }).pillar).toBeNull();
  });

  it('dos o tres tramos válidos se conservan', () => {
    const tranches = [{ zone: '145-150 (mínimo de 52 semanas)', pct: 0.5 }, { zone: '200-205 (SMA200)', pct: 1 }];

    expect(normalizar({ action: 'aumentar', tranches }).tranches).toEqual(tranches);
  });

  it.each([
    ['un solo tramo', [{ zone: '145', pct: 1 }]],
    ['cuatro tramos', [1, 2, 3, 4].map((n) => ({ zone: `z${n}`, pct: 0.2 }))],
    ['un tramo sin zona', [{ zone: '', pct: 1 }, { zone: '150', pct: 1 }]],
    ['un tramo sin porcentaje', [{ zone: '140', pct: '1 %' }, { zone: '150', pct: 1 }]],
    ['algo que no es una lista', 'dos tramos'],
  ])('%s: no hay tramos', (_caso, tranches) => {
    expect(normalizar({ action: 'aumentar', tranches }).tranches).toEqual([]);
  });

  it('una recomendación de antes de la HU (sin estos campos) sale con todo a null', () => {
    expect(composer.normalizarRecomendaciones([base])[0]).toMatchObject({
      action: null, pillar: null, level: null, tranches: [],
    });
  });
});

describe('limpiarMarcasDelCuerpo — la marca de procedencia va solo en Fuentes (regla 13)', () => {
  it('la quita de las citas del cuerpo y la conserva en Fuentes', () => {
    const informe = [
      '## Hallazgos fundamentales',
      'Un dato [4 — procedencia no verificada]. Otro [33 — procedencia no verificada, 34].',
      '## Fuentes',
      '4. https://a.test — procedencia no verificada',
    ].join('\n');

    expect(composer.limpiarMarcasDelCuerpo(informe)).toBe([
      '## Hallazgos fundamentales',
      'Un dato [4]. Otro [33, 34].',
      '## Fuentes',
      '4. https://a.test — procedencia no verificada',
    ].join('\n'));
  });

  it('en inglés, igual', () => {
    const informe = 'A fact [2 — unverified provenance].\n## Sources\n2. https://b.test — unverified provenance';

    expect(composer.limpiarMarcasDelCuerpo(informe))
      .toBe('A fact [2].\n## Sources\n2. https://b.test — unverified provenance');
  });

  it('sin sección de fuentes, limpia todo el texto; sin texto, no rompe', () => {
    expect(composer.limpiarMarcasDelCuerpo('Algo [1 — procedencia no verificada]')).toBe('Algo [1]');
    expect(composer.limpiarMarcasDelCuerpo(undefined)).toBeUndefined();
  });
});
