/**
 * HU 5.4 — Qué cambió desde el último análisis.
 *
 * Este archivo defiende lo que se decide **sin preguntarle nada al modelo**:
 * contra qué análisis se compara, qué recomendaciones se cierran de entrada y
 * en qué estado queda la comparación. Si eso dependiera del modelo, el
 * Escenario 10 ("ya no tienes INTC") sería una opinión y el 11 ("su horizonte
 * terminó") también.
 *
 * @see services/copilot/copilotFollowUp.js
 */

const firestoreFalso = {};

jest.mock('../../firebaseAdmin', () => ({
  firestore: () => firestoreFalso,
}));

const followUp = require('../copilotFollowUp');

const AHORA = new Date('2026-10-10T12:00:00.000Z');

// ============================================================================
// FÁBRICAS
// ============================================================================

const crearRecomendacion = (overrides = {}) => ({
  statement: 'Reducir NVDA al 8 % del portafolio',
  asset: 'NVDA',
  type: 'accion_concreta',
  conviction: 'alta',
  targetPct: 8,
  priceZone: 'por encima de 520 USD',
  horizon: '3-6 meses',
  isNewAsset: false,
  ...overrides,
});

const crearAnalisis = (overrides = {}) => ({
  id: '20260912T100000Z',
  createdAt: '2026-09-12T10:00:00.000Z',
  status: 'completed',
  recommendations: [crearRecomendacion()],
  theses: [{ statement: 'La nube sostiene el crecimiento de MSFT', asset: 'MSFT' }],
  ...overrides,
});

/**
 * Firestore con una lista de análisis, del más reciente al más antiguo.
 *
 * @param {object[]|Error} analisis - Los documentos, o un error al leer.
 */
function montarFirestore(analisis) {
  firestoreFalso.collection = () => ({
    doc: () => ({
      collection: () => {
        const consulta = {
          orderBy: () => consulta,
          limit: () => consulta,
          get: async () => {
            if (analisis instanceof Error) throw analisis;
            return { docs: analisis.map((a) => ({ id: a.id, data: () => a })) };
          },
        };
        return consulta;
      },
    }),
  });
}

const EN_CARTERA = new Set(['NVDA', 'MSFT', 'VUAA.L']);

// ============================================================================
// TESTS
// ============================================================================

describe('buscarReferencia — contra qué se compara (RN-5.4-N)', () => {
  it('toma el análisis completado más reciente', async () => {
    montarFirestore([
      crearAnalisis({ id: 'nuevo', createdAt: '2026-10-01T10:00:00.000Z' }),
      crearAnalisis({ id: 'viejo', createdAt: '2026-09-01T10:00:00.000Z' }),
    ]);

    expect((await followUp.buscarReferencia('u-pro')).id).toBe('nuevo');
  });

  it('salta los fallidos: un análisis que falló no cuenta (Escenario 7)', async () => {
    montarFirestore([
      crearAnalisis({ id: 'fallido', status: 'error' }),
      crearAnalisis({ id: 'bueno' }),
    ]);

    expect((await followUp.buscarReferencia('u-pro')).id).toBe('bueno');
  });

  it('sin ningún completado devuelve null', async () => {
    montarFirestore([crearAnalisis({ status: 'error' })]);

    expect(await followUp.buscarReferencia('u-pro')).toBeNull();
  });
});

describe('horizonteCumplido — RN-5.4-U', () => {
  const emitido = '2026-01-10T10:00:00.000Z';

  it('un horizonte de 3-6 meses emitido en enero ya terminó en octubre', () => {
    expect(followUp.horizonteCumplido('3-6 meses', emitido, AHORA).cumplido).toBe(true);
  });

  it('el mismo horizonte emitido hace un mes sigue abierto', () => {
    expect(followUp.horizonteCumplido('3-6 meses', '2026-09-10T10:00:00.000Z', AHORA).cumplido)
      .toBe(false);
  });

  it('entiende semanas, años y días, y en inglés', () => {
    expect(followUp.horizonteCumplido('2 semanas', emitido, AHORA).cumplido).toBe(true);
    expect(followUp.horizonteCumplido('3-6 months', emitido, AHORA).cumplido).toBe(true);
    expect(followUp.horizonteCumplido('5 años', emitido, AHORA).cumplido).toBe(false);
  });

  it('lo que no se puede leer NO se cierra', () => {
    // Cerrar una recomendación por no entender su horizonte sería peor que
    // dejarla abierta para que el analista la juzgue.
    expect(followUp.horizonteCumplido('largo plazo', emitido, AHORA).cumplido).toBe(false);
    expect(followUp.horizonteCumplido(null, emitido, AHORA).cumplido).toBe(false);
    expect(followUp.horizonteCumplido('6', emitido, AHORA).cumplido).toBe(false);
  });

  it('dice cuándo terminó, para la línea del usuario', () => {
    expect(followUp.horizonteCumplido('3 meses', emitido, AHORA).finalizaEn).toBe('2026-04-10');
  });
});

describe('cerrarDeEntrada — lo que no necesita juicio (RN-5.4-T, RN-5.4-U)', () => {
  const contexto = {
    tickersEnCartera: EN_CARTERA,
    emitidoEn: '2026-09-12T10:00:00.000Z',
    now: AHORA,
  };

  it('deja abierta la que sí requiere juicio', () => {
    const [rec] = followUp.cerrarDeEntrada([crearRecomendacion()], contexto);

    expect(rec.closedState).toBeNull();
    expect(rec.closedReason).toBeNull();
  });

  it('cierra la de un activo que el usuario ya no tiene (Escenario 10)', () => {
    const [rec] = followUp.cerrarDeEntrada(
      [crearRecomendacion({ asset: 'INTC', statement: 'Aumentar INTC al 5 %' })],
      contexto,
    );

    expect(rec.closedState).toBe(followUp.RECOMMENDATION_STATE.NO_LONGER);
    expect(rec.closedReason).toBe(followUp.CLOSE_REASON.ASSET_GONE);
  });

  it('NO cierra una recomendación de comprar un activo que nunca tuvo', () => {
    // Se evalúa como cualquier otra: el usuario nunca lo tuvo, ese es el punto.
    const [rec] = followUp.cerrarDeEntrada(
      [crearRecomendacion({ asset: 'BND', isNewAsset: true, statement: 'Comprar BND al 10 %' })],
      contexto,
    );

    expect(rec.closedState).toBeNull();
  });

  it('cierra la de horizonte cumplido, con su fecha (Escenario 11)', () => {
    const [rec] = followUp.cerrarDeEntrada(
      [crearRecomendacion({ horizon: '1 semana' })],
      { ...contexto, emitidoEn: '2026-01-10T10:00:00.000Z' },
    );

    expect(rec.closedState).toBe(followUp.RECOMMENDATION_STATE.NO_LONGER);
    expect(rec.closedReason).toBe(followUp.CLOSE_REASON.HORIZON_OVER);
    expect(rec.closedAt).toBe('2026-01-17');
  });

  it('una recomendación de rebalanceo sin activo no se cierra por el activo', () => {
    const [rec] = followUp.cerrarDeEntrada(
      [crearRecomendacion({ asset: null, type: 'rebalanceo_riesgo', horizon: null })],
      contexto,
    );

    expect(rec.closedState).toBeNull();
  });
});

describe('activosARevisar — RN-5.4-X (Escenario 18)', () => {
  it('trae los activos de las recomendaciones que siguen abiertas', () => {
    const cerradas = followUp.cerrarDeEntrada(
      [crearRecomendacion({ asset: 'NVDA' }), crearRecomendacion({ asset: 'MSFT' })],
      { tickersEnCartera: EN_CARTERA, emitidoEn: '2026-09-12T10:00:00.000Z', now: AHORA },
    );

    expect(followUp.activosARevisar(cerradas).sort()).toEqual(['MSFT', 'NVDA']);
  });

  it('NO trae el de una recomendación ya cerrada: no hay nada que sostener', () => {
    const cerradas = followUp.cerrarDeEntrada(
      [crearRecomendacion({ asset: 'INTC' })],
      { tickersEnCartera: EN_CARTERA, emitidoEn: '2026-09-12T10:00:00.000Z', now: AHORA },
    );

    expect(followUp.activosARevisar(cerradas)).toEqual([]);
  });

  it('no repite un activo con dos recomendaciones', () => {
    const cerradas = followUp.cerrarDeEntrada(
      [crearRecomendacion(), crearRecomendacion({ statement: 'otra' })],
      { tickersEnCartera: EN_CARTERA, emitidoEn: '2026-09-12T10:00:00.000Z', now: AHORA },
    );

    expect(followUp.activosARevisar(cerradas)).toEqual(['NVDA']);
  });
});

describe('prepararComparacion — los cuatro estados', () => {
  const opciones = { tickersEnCartera: EN_CARTERA, language: 'es', now: AHORA };

  it('con referencia comparable, compara (Escenario 1)', async () => {
    montarFirestore([crearAnalisis()]);

    const r = await followUp.prepararComparacion('u-pro', opciones);

    expect(r.estado).toBe(followUp.COMPARISON_STATE.COMPARED);
    expect(r.recomendaciones).toHaveLength(1);
    expect(r.tesis).toHaveLength(1);
    expect(r.referenceLine.fecha).toContain('2026');
  });

  it('sin ningún análisis, es el primero (Escenario 6)', async () => {
    montarFirestore([]);

    const r = await followUp.prepararComparacion('u-pro', opciones);

    expect(r.estado).toBe(followUp.COMPARISON_STATE.FIRST);
    expect(r.referenceLine).toBeNull();
  });

  it('con un análisis del formato antiguo, lo declara (Escenario 9)', async () => {
    montarFirestore([crearAnalisis({ recommendations: undefined, theses: undefined })]);

    const r = await followUp.prepararComparacion('u-pro', opciones);

    expect(r.estado).toBe(followUp.COMPARISON_STATE.LEGACY);
    // Pero sí sabe contra qué habría comparado: la fecha se muestra igual.
    expect(r.referenceLine).not.toBeNull();
    expect(r.recomendaciones).toEqual([]);
  });

  it('un análisis v3 cuya extracción falló también es formato antiguo', async () => {
    // Se mira el campo, no el metadato: tratarlo como comparable sería
    // inventarle recomendaciones que no tiene.
    montarFirestore([crearAnalisis({ recommendations: [] })]);

    expect((await followUp.prepararComparacion('u-pro', opciones)).estado)
      .toBe(followUp.COMPARISON_STATE.LEGACY);
  });

  it('si no se puede consultar, NO es un primer análisis (Escenario 13)', async () => {
    montarFirestore(new Error('Firestore caído'));

    const r = await followUp.prepararComparacion('u-pro', opciones);

    expect(r.estado).toBe(followUp.COMPARISON_STATE.UNAVAILABLE);
    expect(r.activosARevisar).toEqual([]);
  });

  it('un análisis de más de 90 días lleva su aviso de antigüedad (Escenario 11)', async () => {
    montarFirestore([crearAnalisis({ createdAt: '2026-05-03T10:00:00.000Z' })]);

    const r = await followUp.prepararComparacion('u-pro', opciones);

    expect(r.referenceLine.stale).toBe(true);
    expect(r.referenceLine.dias).toBeGreaterThan(followUp.DIAS_PARA_AVISO_DE_ANTIGUEDAD);
  });

  it('uno reciente no lo lleva', async () => {
    montarFirestore([crearAnalisis({ createdAt: '2026-09-12T10:00:00.000Z' })]);

    expect((await followUp.prepararComparacion('u-pro', opciones)).referenceLine.stale).toBe(false);
  });
});

describe('estadosCalculados — sin recomendación de hoy, el estado declarado (respaldo de la HU 5.4)', () => {
  /** Las recomendaciones tal como salen de `prepararComparacion`. */
  const anteriores = () => [
    { statement: 'Reducir NVDA al 8 %', asset: 'NVDA', closedState: null, closedReason: null },
    { statement: 'Mantener MSFT', asset: 'MSFT', closedState: null, closedReason: null },
    {
      statement: 'Aumentar INTC al 5 %',
      asset: 'INTC',
      closedState: followUp.RECOMMENDATION_STATE.NO_LONGER,
      closedReason: followUp.CLOSE_REASON.ASSET_GONE,
    },
  ];

  it('lo toma del informe, emparejando por el enunciado', () => {
    const estados = followUp.estadosCalculados(anteriores(), [], [
      { statement: 'Reducir NVDA al 8 %', asset: 'NVDA', state: 'ajustada' },
      { statement: 'Mantener MSFT', asset: 'MSFT', state: 'vigente' },
    ]);

    expect(estados.map((e) => e.state)).toEqual(['ajustada', 'vigente', 'no_vigente']);
  });

  it('un enunciado con cursivas o puntuación de más sigue siendo el mismo', () => {
    const estados = followUp.estadosCalculados(anteriores(), [], [
      { statement: '  **Reducir NVDA al 8 %.**  ', asset: 'NVDA', state: 'ajustada' },
    ]);

    expect(estados[0].state).toBe('ajustada');
  });

  it('si el modelo lo reescribió entero, lo empareja por su activo', () => {
    const estados = followUp.estadosCalculados(anteriores(), [], [
      { statement: 'Bajar la exposición al fabricante de GPUs', asset: 'NVDA', state: 'vigente' },
    ]);

    expect(estados[0].state).toBe('vigente');
  });

  it('con dos estados sueltos del mismo activo no adivina', () => {
    // Adjudicar uno de los dos sería inventarse cuál.
    const estados = followUp.estadosCalculados(
      [{ statement: 'Reducir NVDA', asset: 'NVDA', closedState: null }],
      [],
      [
        { statement: 'otra cosa', asset: 'NVDA', state: 'vigente' },
        { statement: 'y otra', asset: 'NVDA', state: 'no_vigente' },
      ],
    );

    expect(estados[0].state).toBeNull();
  });

  it('un estado no responde por dos recomendaciones', () => {
    const estados = followUp.estadosCalculados(
      [
        { statement: 'Reducir NVDA', asset: 'NVDA', closedState: null },
        { statement: 'Reducir NVDA', asset: 'NVDA', closedState: null },
      ],
      [],
      [{ statement: 'Reducir NVDA', asset: 'NVDA', state: 'vigente' }],
    );

    expect(estados.map((e) => e.state)).toEqual(['vigente', null]);
  });

  it('lo que el backend cerró de entrada manda sobre lo que diga el informe', () => {
    // RN-5.4-T no es un juicio: el activo está o no está en el portafolio.
    const estados = followUp.estadosCalculados(anteriores(), [], [
      { statement: 'Aumentar INTC al 5 %', asset: 'INTC', state: 'vigente' },
    ]);

    expect(estados[2].state).toBe(followUp.RECOMMENDATION_STATE.NO_LONGER);
  });

  it('lo que nadie declaró se queda sin estado, no en vigente', () => {
    const estados = followUp.estadosCalculados(anteriores(), [], []);

    expect(estados[0].state).toBeNull();
    expect(estados[1].state).toBeNull();
  });

  it('sin estados declarados sólo cuentan los cierres de entrada', () => {
    const estados = followUp.estadosCalculados(anteriores(), [], []);

    expect(followUp.contarSeguimiento(estados, [])).toEqual({
      vigentes: 0, ajustadas: 0, noVigentes: 1, tesisInvalidadas: 0,
    });
  });
});

describe('estadosDeTesis — las tesis anteriores no traen estado propio', () => {
  it('se lo pone el informe', () => {
    const estados = followUp.estadosDeTesis(
      [
        { statement: 'La nube sostiene MSFT', asset: 'MSFT' },
        { statement: 'El ciclo de memoria tocó fondo', asset: 'MU' },
      ],
      [
        { statement: 'la nube sostiene msft', asset: 'MSFT', state: 'invalidada' },
        { statement: 'El ciclo de memoria tocó fondo', asset: 'MU', state: 'confirmada' },
      ],
    );

    expect(estados.map((e) => e.state)).toEqual(['invalidada', 'confirmada']);
  });
});

describe('contarSeguimiento — con la forma que produce el pipeline', () => {
  /**
   * La prueba que faltaba. Antes se construían los estados a mano —
   * `{ state: 'vigente' }` — y eso ocultaba que en producción llegaban
   * recomendaciones con `closedState` y tesis con sólo su enunciado, de modo
   * que el recuento salía siempre en ceros y la tarjeta se quedaba sin línea.
   */
  it('cuenta lo que de verdad le llega por el pipeline (Escenario 15)', () => {
    const anteriores = [
      { statement: 'Reducir NVDA al 8 %', asset: 'NVDA', closedState: null, closedReason: null },
      { statement: 'Mantener MSFT', asset: 'MSFT', closedState: null, closedReason: null },
      {
        statement: 'Aumentar INTC al 5 %',
        asset: 'INTC',
        closedState: followUp.RECOMMENDATION_STATE.NO_LONGER,
        closedReason: followUp.CLOSE_REASON.ASSET_GONE,
      },
    ];
    const tesisAnteriores = [{ statement: 'La nube sostiene MSFT', asset: 'MSFT' }];

    const declarados = {
      recommendations: [
        { statement: 'Reducir NVDA al 8 %', asset: 'NVDA', state: 'ajustada' },
        { statement: 'Mantener MSFT', asset: 'MSFT', state: 'vigente' },
      ],
      theses: [{ statement: 'La nube sostiene MSFT', asset: 'MSFT', state: 'invalidada' }],
    };

    const recuento = followUp.contarSeguimiento(
      followUp.estadosCalculados(anteriores, [], declarados.recommendations),
      followUp.estadosDeTesis(tesisAnteriores, declarados.theses),
    );

    expect(recuento).toEqual({
      vigentes: 1, ajustadas: 1, noVigentes: 1, tesisInvalidadas: 1,
    });
  });
});

describe('contarSeguimiento — lo que muestra la tarjeta (Escenario 15)', () => {
  it('cuenta cada estado por separado', () => {
    const estados = [
      { state: followUp.RECOMMENDATION_STATE.STILL_VALID },
      { state: followUp.RECOMMENDATION_STATE.STILL_VALID },
      { state: followUp.RECOMMENDATION_STATE.ADJUSTED },
      { state: followUp.RECOMMENDATION_STATE.NO_LONGER },
    ];
    const tesis = [{ state: 'invalidada' }, { state: 'confirmada' }];

    expect(followUp.contarSeguimiento(estados, tesis)).toEqual({
      vigentes: 2,
      ajustadas: 1,
      noVigentes: 1,
      tesisInvalidadas: 1,
    });
  });

  it('sin nada que contar, todo a cero', () => {
    expect(followUp.contarSeguimiento([], [])).toEqual({
      vigentes: 0, ajustadas: 0, noVigentes: 0, tesisInvalidadas: 0,
    });
  });
});

// ============================================================================
// HU 5.7.3 — El seguimiento sabe cuánto se movió el precio y con qué criterio
// ============================================================================

/** Serie diaria con la forma de finance-query: `{ 'YYYY-MM-DD': { close } }`. */
const crearSerie = (cierres) => Object.fromEntries(
  Object.entries(cierres).map(([fecha, close]) => [fecha, { close, high: close, low: close }]),
);

/** A-1110: el análisis del 2026-11-10, antes de la 5.7.1 y sin precios guardados. */
const EMITIDO_A1110 = '2026-11-10T15:00:00.000Z';

describe('HU 5.7.3 — precio de emisión, de hoy y variación (RN-5.7-L)', () => {
  it('QA-01: sin precio guardado toma el cierre de ese día y lo dice', () => {
    const precio = followUp.precioDeRecomendacion(
      crearRecomendacion({ statement: 'Reducir la posición de NVDA del 7 % actual al 3 %' }),
      {
        emitidoEn: EMITIDO_A1110,
        serie: crearSerie({ '2026-11-07': 170.1, '2026-11-10': 168.89, '2026-11-11': 166.5 }),
        precioHoy: 145.98,
        language: 'es',
      },
    );

    expect(precio.issue).toBe(168.89);
    expect(precio.issueSource).toBe(followUp.ORIGEN_PRECIO.CIERRE);
    expect(precio.issueDate).toBe('2026-11-10');
    expect(precio.line).toBe('168,89 → 145,98 (−13,6 %) · cierre del 10 nov 2026');
  });

  it('QA-01: R2 (VUAA.L) sale igual, como cierre de ese día', () => {
    const precio = followUp.precioDeRecomendacion(
      crearRecomendacion({ asset: 'VUAA.L' }),
      {
        emitidoEn: EMITIDO_A1110, serie: crearSerie({ '2026-11-10': 104.2 }), precioHoy: 101.35, language: 'es',
      },
    );

    expect(precio.line).toBe('104,20 → 101,35 (−2,7 %) · cierre del 10 nov 2026');
  });

  it('un día sin mercado toma la sesión anterior', () => {
    const precio = followUp.precioDeEmision(crearRecomendacion(), {
      emitidoEn: '2026-11-08T15:00:00.000Z', // sábado
      serie: crearSerie({ '2026-11-06': 171.2, '2026-11-10': 168.89 }),
    });

    expect(precio).toEqual({ value: 171.2, source: 'cierre', date: '2026-11-06' });
  });

  it('QA-02: con precio guardado usa ese, sin la marca de cierre', () => {
    const nvda = followUp.precioDeRecomendacion(
      crearRecomendacion({ priceAtIssue: 145.98 }),
      {
        emitidoEn: '2026-11-19T15:00:00.000Z', serie: crearSerie({ '2026-11-19': 150 }), precioHoy: 160, language: 'es',
      },
    );
    const btc = followUp.precioDeRecomendacion(
      crearRecomendacion({ asset: 'BTC-USD', priceAtIssue: 61914 }),
      { emitidoEn: '2026-11-19T15:00:00.000Z', precioHoy: 66000, language: 'es' },
    );

    expect(nvda.issueSource).toBe(followUp.ORIGEN_PRECIO.GUARDADO);
    expect(nvda.line).toBe('145,98 → 160,00 (+9,6 %)');
    expect(btc.line).toBe('61.914 → 66.000 (+6,6 %)');
  });

  it('QA-06: sin cierre en el historial dice que no está disponible y no inventa nada', () => {
    const precio = followUp.precioDeRecomendacion(
      crearRecomendacion({ asset: 'ECOPETROL.CL' }),
      {
        emitidoEn: EMITIDO_A1110, serie: crearSerie({ '2026-11-12': 2450 }), precioHoy: 2390, language: 'es',
      },
    );

    expect(precio.issue).toBeNull();
    expect(precio.changePct).toBeNull();
    expect(precio.line).toBe('precio de emisión no disponible · hoy 2390');
  });

  it('QA-08: los textos de la línea en inglés', () => {
    const conCierre = followUp.precioDeRecomendacion(crearRecomendacion(), {
      emitidoEn: EMITIDO_A1110, serie: crearSerie({ '2026-11-10': 168.89 }), precioHoy: 145.98, language: 'en',
    });
    const sinEmision = followUp.precioDeRecomendacion(crearRecomendacion(), {
      emitidoEn: EMITIDO_A1110, serie: null, precioHoy: 145.98, language: 'en',
    });

    expect(conCierre.line).toBe('168.89 → 145.98 (−13.6%) · close of 10 Nov 2026');
    expect(sinEmision.line).toBe('issue price not available · today 145.98');
  });

  it('sin precio de hoy, muestra el de emisión y dice que falta el de hoy', () => {
    const { line } = followUp.lineaDePrecio({ emision: { value: 168.89, source: 'guardado' }, hoy: null }, 'es');
    expect(line).toBe('168,89 → precio de hoy no disponible');
  });
});

describe('HU 5.7.3 — ids y criterio nuevo en la comparación (Esc. 6, RN-5.7-S)', () => {
  const opciones = { tickersEnCartera: EN_CARTERA, language: 'es', now: AHORA };

  it('cada recomendación y tesis anterior lleva su id', async () => {
    montarFirestore([crearAnalisis({
      recommendations: [crearRecomendacion(), crearRecomendacion({ statement: 'Mantener MSFT', asset: 'MSFT' })],
    })]);

    const comparacion = await followUp.prepararComparacion('u-pro', opciones);

    expect(comparacion.recomendaciones.map((r) => r.id)).toEqual(['R1', 'R2']);
    expect(comparacion.tesis.map((t) => t.id)).toEqual(['T1']);
  });

  it('QA-07: una referencia sin criterio registrado lo marca como nuevo', async () => {
    montarFirestore([crearAnalisis()]);
    expect((await followUp.prepararComparacion('u-pro', opciones)).criterionChanged).toBe(true);
  });

  it('QA-07: una referencia generada con el criterio no lo marca', async () => {
    montarFirestore([crearAnalisis({ decisionCriteria: '5.7.1' })]);
    expect((await followUp.prepararComparacion('u-pro', opciones)).criterionChanged).toBe(false);
  });
});

describe('HU 5.7.3 — cerrarPorCriterio: la ficha de hoy no la sostiene (RN-5.7-P)', () => {
  const permitidas = new Map([
    ['NVDA', ['mantener', 'aumentar', 'comprar']],
    ['BTC-USD', ['mantener', 'reducir']],
  ]);

  it('QA-03: «Reducir NVDA» anterior sin señales hoy llega ya no vigente, por criterio', () => {
    const [nvda] = followUp.cerrarPorCriterio([
      crearRecomendacion({ id: 'R1', closedState: null, closedReason: null }),
    ], permitidas);

    expect(nvda.closedState).toBe(followUp.RECOMMENDATION_STATE.NO_LONGER);
    expect(nvda.closedReason).toBe(followUp.CLOSE_REASON.CRITERIA);
  });

  it('una reducción que la ficha permite sigue abierta para compararla con la de hoy', () => {
    const [btc] = followUp.cerrarPorCriterio([
      crearRecomendacion({ statement: 'Reducir BTC-USD hacia el 10 %', asset: 'BTC-USD', closedState: null }),
    ], permitidas);

    expect(btc.closedState).toBeNull();
  });

  it('no toca lo que no reduce, lo que ya se cerró ni lo que no tiene ficha', () => {
    const recs = [
      crearRecomendacion({ statement: 'Mantener NVDA', closedState: null, closedReason: null }),
      crearRecomendacion({ closedState: 'no_vigente', closedReason: 'activo_ausente' }),
      crearRecomendacion({
        asset: 'TLT', statement: 'Reducir TLT', closedState: null, closedReason: null,
      }),
    ];

    const cerradas = followUp.cerrarPorCriterio(recs, permitidas);

    expect(cerradas.map((r) => r.closedReason)).toEqual([null, 'activo_ausente', null]);
  });
});

describe('HU 5.7.3 — estadosCalculados: el estado lo calcula el código (RN-5.7-P)', () => {
  const anterior = (overrides = {}) => ({
    id: 'R1', closedState: null, closedReason: null, ...crearRecomendacion(overrides),
  });

  it('QA-10: reducción anterior + «aumentar» hoy = ya no vigente, aunque el modelo diga vigente', () => {
    const [nflx] = followUp.estadosCalculados(
      [anterior({ statement: 'Reducir NFLX del 0,6 % al 0,3 %', asset: 'NFLX', action: 'reducir' })],
      [{ asset: 'NFLX', action: 'aumentar', targetPct: 3, conviction: 'media' }],
      [{ id: 'R1', state: 'vigente' }],
    );

    expect(nflx.state).toBe('no_vigente');
    expect(nflx.stateReason).toBe('otra_accion');
  });

  it('QA-09: el cierre por activo ausente manda y conserva el motivo', () => {
    const [eco] = followUp.estadosCalculados(
      [anterior({ asset: 'ECOPETROL.CL', closedState: 'no_vigente', closedReason: 'activo_ausente' })],
      [{ asset: 'ECOPETROL.CL', action: 'mantener' }],
      [],
    );

    expect(eco.state).toBe('no_vigente');
    expect(eco.stateReason).toBe('activo_ausente');
  });

  it('sin recomendación de hoy, un «vigente» declarado que la ficha ya no permite es ya no vigente', () => {
    const [nvda] = followUp.estadosCalculados(
      [anterior({ action: 'reducir' })],
      [],
      [{ id: 'R1', state: 'vigente' }],
      { permitidas: new Map([['NVDA', ['mantener']]]) },
    );

    expect(nvda.state).toBe('no_vigente');
    expect(nvda.stateReason).toBe('criterio');
  });

  it('QA-13: el estado vuelve por id aunque el enunciado venga en otro idioma', () => {
    const [msft] = followUp.estadosCalculados(
      [anterior({ statement: 'Mantener MSFT', asset: null })],
      [],
      [{ id: 'R1', statement: 'Hold MSFT', state: 'vigente' }],
    );

    expect(msft.state).toBe('vigente');
  });

  it('QA-13: una tesis sin activo y sin id que no empareja queda sin estado', () => {
    const tesis = followUp.estadosDeTesis(
      [{ id: 'T1', statement: 'El ciclo de tipos favorece a los bonos largos', asset: null }],
      [{ statement: 'Long bonds benefit from the rate cycle', asset: null, state: 'confirmada' }],
    );

    expect(tesis[0].state).toBeNull();
  });

  it('QA-13: las tesis por id, en cualquier idioma', () => {
    const tesis = followUp.estadosDeTesis(
      [{ id: 'T1', statement: 'La nube sostiene MSFT', asset: 'MSFT' }],
      [{ id: 'T1', state: 'invalidada' }],
    );

    expect(tesis[0].state).toBe('invalidada');
  });
});
