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

describe('estadosFinales — de dónde sale el estado de cada recomendación', () => {
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
    const estados = followUp.estadosFinales(anteriores(), [
      { statement: 'Reducir NVDA al 8 %', asset: 'NVDA', state: 'ajustada' },
      { statement: 'Mantener MSFT', asset: 'MSFT', state: 'vigente' },
    ]);

    expect(estados.map((e) => e.state)).toEqual(['ajustada', 'vigente', 'no_vigente']);
  });

  it('un enunciado con cursivas o puntuación de más sigue siendo el mismo', () => {
    const estados = followUp.estadosFinales(anteriores(), [
      { statement: '  **Reducir NVDA al 8 %.**  ', asset: 'NVDA', state: 'ajustada' },
    ]);

    expect(estados[0].state).toBe('ajustada');
  });

  it('si el modelo lo reescribió entero, lo empareja por su activo', () => {
    const estados = followUp.estadosFinales(anteriores(), [
      { statement: 'Bajar la exposición al fabricante de GPUs', asset: 'NVDA', state: 'vigente' },
    ]);

    expect(estados[0].state).toBe('vigente');
  });

  it('con dos estados sueltos del mismo activo no adivina', () => {
    // Adjudicar uno de los dos sería inventarse cuál.
    const estados = followUp.estadosFinales(
      [{ statement: 'Reducir NVDA', asset: 'NVDA', closedState: null }],
      [
        { statement: 'otra cosa', asset: 'NVDA', state: 'vigente' },
        { statement: 'y otra', asset: 'NVDA', state: 'no_vigente' },
      ],
    );

    expect(estados[0].state).toBeNull();
  });

  it('un estado no responde por dos recomendaciones', () => {
    const estados = followUp.estadosFinales(
      [
        { statement: 'Reducir NVDA', asset: 'NVDA', closedState: null },
        { statement: 'Reducir NVDA', asset: 'NVDA', closedState: null },
      ],
      [{ statement: 'Reducir NVDA', asset: 'NVDA', state: 'vigente' }],
    );

    expect(estados.map((e) => e.state)).toEqual(['vigente', null]);
  });

  it('lo que el backend cerró de entrada manda sobre lo que diga el informe', () => {
    // RN-5.4-T no es un juicio: el activo está o no está en el portafolio.
    const estados = followUp.estadosFinales(anteriores(), [
      { statement: 'Aumentar INTC al 5 %', asset: 'INTC', state: 'vigente' },
    ]);

    expect(estados[2].state).toBe(followUp.RECOMMENDATION_STATE.NO_LONGER);
  });

  it('lo que nadie declaró se queda sin estado, no en vigente', () => {
    const estados = followUp.estadosFinales(anteriores(), []);

    expect(estados[0].state).toBeNull();
    expect(estados[1].state).toBeNull();
  });

  it('sin estados declarados sólo cuentan los cierres de entrada', () => {
    const estados = followUp.estadosFinales(anteriores(), []);

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
      followUp.estadosFinales(anteriores, declarados.recommendations),
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
