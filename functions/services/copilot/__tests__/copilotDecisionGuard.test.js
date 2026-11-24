/**
 * HU 5.7.3 — Lo que el código decide sobre una recomendación antes de que se
 * escriba el informe.
 *
 * Cada caso sale de un análisis real de la cuenta del PO (2026-11-21 y
 * 2026-11-23): NFLX con un tramo de 3,1 % hacia el 3 %, ETH aumentada porque
 * cayó, ECOPETROL vendida con una sola señal y una reducción anterior dada por
 * vigente mientras se recomendaba aumentar el mismo activo.
 *
 * @see services/copilot/copilotDecisionGuard.js
 * @see platform-docs/stories/5.7.3-seguimiento-con-precio-y-criterio/qa.md
 */

const guard = require('../copilotDecisionGuard');

// ============================================================================
// FÁBRICAS
// ============================================================================

const crearSenales = (overrides = {}) => ({
  weightPct: 5,
  overweightThresholdPct: 10,
  overweight: false,
  roomToThresholdPct: 5,
  maxTranchePct: 1.6,
  extremeValuation: null,
  deterioration: [],
  opportunityException: null,
  ...overrides,
});

const crearFicha = (signals = {}, kind = 'stock') => ({ kind, signals: crearSenales(signals) });

const crearRec = (overrides = {}) => ({
  statement: 'Mantener NVDA',
  asset: 'NVDA',
  type: 'accion_concreta',
  conviction: 'media',
  targetPct: null,
  priceZone: null,
  horizon: null,
  isNewAsset: false,
  action: 'mantener',
  pillar: 'fundamentales',
  level: null,
  tranches: [],
  ...overrides,
});

const SENAL = (signal) => ({ signal, evidence: 'dato' });

// ============================================================================
// TESTS
// ============================================================================

describe('accionesPermitidas — las señales limitan las acciones (RN-5.7-T, RN-5.7-R)', () => {
  it('sin señales de los Niveles 1, 2 ni 3: no se reduce ni se vende', () => {
    expect(guard.accionesPermitidas(crearSenales()).allowed)
      .toEqual(['mantener', 'aumentar', 'comprar']);
  });

  it('con una sola señal de deterioro: se puede reducir, nunca vender, y mantener va con convicción baja', () => {
    const permitidas = guard.accionesPermitidas(crearSenales({ deterioration: [SENAL('fy_revenue_down')] }));

    expect(permitidas.allowed).toContain('reducir');
    expect(permitidas.allowed).not.toContain('vender');
    expect(permitidas.holdMaxConviction).toBe('baja');
  });

  it('con dos señales de deterioro se puede vender', () => {
    const permitidas = guard.accionesPermitidas(crearSenales({
      deterioration: [SENAL('q_margin_down_2'), SENAL('fy_margin_down')],
    }));

    expect(permitidas.allowed).toEqual(expect.arrayContaining(['reducir', 'vender']));
    expect(permitidas.holdMaxConviction).toBeNull();
  });

  it('el sobrepeso (Nivel 3) permite reducir, no vender', () => {
    const permitidas = guard.accionesPermitidas(crearSenales({
      weightPct: 10.9, overweight: true, roomToThresholdPct: 0, maxTranchePct: 0,
    }));

    expect(permitidas.allowed).toEqual(['mantener', 'reducir']);
  });

  it('con la excepción de oportunidad, el sobrepeso solo reduce por encima del techo', () => {
    const excepcion = { ceilingPct: 15, roomToCeilingPct: 4.6, maxTranchePct: 1.5 };

    const bajoElTecho = guard.accionesPermitidas(crearSenales({
      weightPct: 10.4, overweight: true, roomToThresholdPct: 0, opportunityException: excepcion,
    }));
    const sobreElTecho = guard.accionesPermitidas(crearSenales({
      weightPct: 16, overweight: true, roomToThresholdPct: 0,
      opportunityException: { ...excepcion, roomToCeilingPct: 0 },
    }));

    expect(bajoElTecho.allowed).toEqual(['mantener', 'aumentar', 'comprar']);
    expect(sobreElTecho.allowed).toEqual(['mantener', 'reducir']);
  });

  it('la valoración extrema (Nivel 3) permite reducir', () => {
    expect(guard.accionesPermitidas(crearSenales({ extremeValuation: 'above_high_target' })).allowed)
      .toContain('reducir');
  });

  it('un hecho de Nivel 1 verificado permite reducir y vender', () => {
    expect(guard.accionesPermitidas(crearSenales(), { level1Eligible: true }).allowed)
      .toEqual(expect.arrayContaining(['reducir', 'vender']));
  });

  it('QA-12: una cripto nunca se aumenta ni se compra', () => {
    expect(guard.accionesPermitidas(crearSenales({ weightPct: 2.6, roomToThresholdPct: 7.4 }), { clase: 'crypto' }).allowed)
      .toEqual(['mantener']);
  });

  it('un ETF, sin umbral, se puede aumentar', () => {
    const permitidas = guard.accionesPermitidas(
      crearSenales({ overweightThresholdPct: null, roomToThresholdPct: null, maxTranchePct: null }),
      { clase: 'etf' },
    );

    expect(permitidas.allowed).toContain('aumentar');
  });

  it('sin margen hasta el umbral no se aumenta', () => {
    expect(guard.accionesPermitidas(crearSenales({ weightPct: 10, roomToThresholdPct: 0 })).allowed)
      .toEqual(['mantener']);
  });

  it('sin señales no hay nada que limitar', () => {
    expect(guard.accionesPermitidas(null)).toBeNull();
  });
});

describe('corregirRecomendacion — QA-11, los tramos se validan en código (RN-5.7-Q)', () => {
  const NFLX = crearFicha({ weightPct: 0.6, roomToThresholdPct: 9.4, maxTranchePct: 3.1 });

  it('un único tramo de 3,1 % para ir del 0,6 % al 3 % se reparte en 3 × 0,8', () => {
    const { rec, corrections } = guard.corregirRecomendacion(
      crearRec({
        asset: 'NFLX', action: 'aumentar', targetPct: 3, priceZone: '95-100 USD', tranches: [],
      }),
      { card: NFLX, language: 'es' },
    );

    expect(rec.tranches.map((t) => t.pct)).toEqual([0.8, 0.8, 0.8]);
    expect(rec.tranches[0].zone).toBe('95-100 USD');
    expect(rec.tranches[1].zone).toBe(guard.TRAMO_EN_EL_TIEMPO.es);
    expect(rec.targetPct).toBe(3);
    expect(rec.statement).toBe('Aumentar NFLX por tramos del 0,6 % al 3 % del portafolio');
    expect(corrections.map((c) => c.rule)).toContain('RN-5.7-Q');
  });

  it('tres tramos de 3,1 % para un objetivo del 3 % también se reparten, conservando las zonas', () => {
    const { rec } = guard.corregirRecomendacion(
      crearRec({
        asset: 'NFLX',
        action: 'aumentar',
        targetPct: 3,
        tranches: [{ zone: 'Z1', pct: 3.1 }, { zone: 'Z2', pct: 3.1 }, { zone: 'Z3', pct: 3.1 }],
      }),
      { card: NFLX, language: 'es' },
    );

    expect(rec.tranches).toEqual([{ zone: 'Z1', pct: 0.8 }, { zone: 'Z2', pct: 0.8 }, { zone: 'Z3', pct: 0.8 }]);
  });

  it('un reparto válido (NVDA del 10,4 % al 20 % en 3 × 3,2 con tope 3,2) se guarda sin cambios', () => {
    const tramos = [{ zone: 'Z1', pct: 3.2 }, { zone: 'Z2', pct: 3.2 }, { zone: 'Z3', pct: 3.2 }];
    const ficha = crearFicha({
      weightPct: 10.4,
      overweight: true,
      roomToThresholdPct: 0,
      maxTranchePct: 0,
      opportunityException: { ceilingPct: 20, roomToCeilingPct: 9.6, maxTranchePct: 3.2 },
    });

    const { rec, corrections } = guard.corregirRecomendacion(
      crearRec({ action: 'aumentar', targetPct: 20, tranches: tramos }),
      { card: ficha, language: 'es' },
    );

    expect(rec.tranches).toEqual(tramos);
    expect(rec.targetPct).toBe(20);
    expect(corrections).toEqual([]);
  });

  it('un objetivo por encima del margen baja hasta donde el margen deja', () => {
    const { rec } = guard.corregirRecomendacion(
      crearRec({ asset: 'NFLX', action: 'aumentar', targetPct: 20 }),
      { card: NFLX, language: 'es' },
    );

    expect(rec.targetPct).toBe(10);
    expect(rec.tranches.map((t) => t.pct)).toEqual([3.1, 3.1, 3.1]);
  });

  it('un objetivo por debajo del peso de hoy no es aumentar: queda en mantener', () => {
    const { rec, corrections } = guard.corregirRecomendacion(
      crearRec({ asset: 'NFLX', action: 'aumentar', targetPct: 0.5 }),
      { card: NFLX, language: 'es' },
    );

    expect(rec.action).toBe('mantener');
    expect(corrections[0]).toMatchObject({ rule: 'RN-5.7-Q', from: 'aumentar', to: 'mantener' });
  });
});

describe('corregirRecomendacion — QA-12, una cripto no se aumenta porque bajó (RN-5.7-R)', () => {
  it('«aumentar ETH-USD hasta el 10 %» queda en mantener', () => {
    const { rec, corrections } = guard.corregirRecomendacion(
      crearRec({
        statement: 'Aumentar ETH-USD hasta el 10 %', asset: 'ETH-USD', action: 'aumentar', targetPct: 10, pillar: 'tecnico',
      }),
      { card: crearFicha({ weightPct: 2.6, roomToThresholdPct: 7.4, maxTranchePct: 2.4 }, 'crypto'), language: 'es' },
    );

    expect(rec.action).toBe('mantener');
    expect(rec.targetPct).toBeNull();
    expect(rec.tranches).toEqual([]);
    expect(rec.statement).toBe('Mantener ETH-USD');
    expect(corrections[0]).toMatchObject({ rule: 'RN-5.7-R', from: 'aumentar', to: 'mantener' });
  });

  it('BTC-USD al 10,9 % sí puede reducirse hasta el 10 % (Nivel 3)', () => {
    const ficha = crearFicha({
      weightPct: 10.9, overweight: true, roomToThresholdPct: 0, maxTranchePct: 0,
    }, 'crypto');

    const { rec, corrections } = guard.corregirRecomendacion(
      crearRec({
        asset: 'BTC-USD', action: 'reducir', targetPct: 10, level: 3, pillar: 'valoracion',
      }),
      { card: ficha, language: 'es' },
    );

    expect(rec.action).toBe('reducir');
    expect(rec.statement).toBe('Reducir BTC-USD del 10,9 % al 10 % del portafolio');
    expect(corrections).toEqual([]);
  });
});

describe('corregirRecomendacion — QA-15, con una sola señal no se vende (RN-5.7-T)', () => {
  const ECOPETROL = crearFicha({ weightPct: 2.1, deterioration: [SENAL('fy_revenue_down')] });

  it('«Vender ECOPETROL» pasa a reducir a la mitad del peso, Nivel 2 y pilar fundamentales', () => {
    const { rec, corrections } = guard.corregirRecomendacion(
      crearRec({
        statement: 'Vender ECOPETROL.CL', asset: 'ECOPETROL.CL', action: 'vender', conviction: 'alta', pillar: 'noticias', level: 1,
      }),
      { card: ECOPETROL, language: 'es' },
    );

    expect(rec).toMatchObject({
      action: 'reducir', targetPct: 1.1, level: 2, pillar: 'fundamentales',
    });
    expect(rec.statement).toBe('Reducir ECOPETROL.CL del 2,1 % al 1,1 % del portafolio');
    expect(corrections.find((c) => c.field === 'action')).toMatchObject({ rule: 'RN-5.7-T', from: 'vender', to: 'reducir' });
  });

  it('«Mantener ECOPETROL» con convicción alta baja a convicción baja', () => {
    const { rec } = guard.corregirRecomendacion(
      crearRec({ asset: 'ECOPETROL.CL', action: 'mantener', conviction: 'alta' }),
      { card: ECOPETROL, language: 'es' },
    );

    expect(rec.conviction).toBe('baja');
  });

  it('UNH con dos señales puede venderse, pero su pilar es fundamentales y su nivel el 2', () => {
    const { rec } = guard.corregirRecomendacion(
      crearRec({
        asset: 'UNH', action: 'vender', conviction: 'alta', pillar: 'noticias', level: 1,
      }),
      {
        card: crearFicha({ weightPct: 3, deterioration: [SENAL('q_margin_down_2'), SENAL('fy_margin_down')] }),
        language: 'es',
      },
    );

    expect(rec).toMatchObject({ action: 'vender', pillar: 'fundamentales', level: 2 });
    expect(rec.statement).toBe('Vender UNH');
  });

  it('«Reducir CRM» sin señales queda en mantener', () => {
    const { rec } = guard.corregirRecomendacion(
      crearRec({
        asset: 'CRM', action: 'reducir', targetPct: 2, level: 2,
      }),
      { card: crearFicha({ weightPct: 4 }), language: 'es' },
    );

    expect(rec).toMatchObject({
      action: 'mantener', targetPct: null, level: null, statement: 'Mantener CRM',
    });
  });

  it('con un hecho de Nivel 1 verificado, vender se respeta como Nivel 1', () => {
    const { rec, corrections } = guard.corregirRecomendacion(
      crearRec({
        asset: 'CRM', action: 'vender', conviction: 'alta', pillar: 'noticias', level: 1,
      }),
      { card: crearFicha({ weightPct: 4 }), level1Eligible: true, language: 'es' },
    );

    expect(rec).toMatchObject({ action: 'vender', level: 1, pillar: 'noticias' });
    expect(corrections).toEqual([]);
  });

  it('sin ficha no corrige nada: no hay señales con qué hacerlo', () => {
    const original = crearRec({ asset: 'TLT', action: 'vender', statement: 'Vender TLT' });

    const { rec, corrections } = guard.corregirRecomendacion(original, { card: null, language: 'es' });

    expect(rec.action).toBe('vender');
    expect(corrections).toEqual([]);
  });
});

describe('aplicarGuarda — todas las de un análisis', () => {
  it('corrige cada una con su ficha y junta lo corregido', () => {
    const fichas = new Map([
      ['ETH-USD', crearFicha({ weightPct: 2.6, roomToThresholdPct: 7.4 }, 'crypto')],
      ['CRM', { ...crearFicha({ weightPct: 4 }), signals: { ...crearSenales({ weightPct: 4 }), level1Eligible: true } }],
    ]);

    const { recomendaciones, correcciones } = guard.aplicarGuarda([
      crearRec({ asset: 'ETH-USD', action: 'aumentar', targetPct: 5 }),
      crearRec({ asset: 'CRM', action: 'vender', level: 1, pillar: 'noticias' }),
      crearRec({ asset: 'TLT', action: 'comprar', targetPct: 3, isNewAsset: true }),
    ], { fichas, pesos: new Map(), language: 'en' });

    expect(recomendaciones.map((r) => r.action)).toEqual(['mantener', 'vender', 'comprar']);
    expect(recomendaciones.map((r) => r.statement)).toEqual(['Hold ETH-USD', 'Sell CRM', 'Buy TLT up to 3% of the portfolio']);
    expect(correcciones).toHaveLength(1);
  });
});

describe('compararConHoy — QA-16, el estado sale de comparar con la de hoy (RN-5.7-P)', () => {
  const anteriores = [
    crearRec({ asset: 'BTC-USD', action: 'reducir', targetPct: 10, conviction: 'alta' }),
    crearRec({ asset: 'MELI', action: 'mantener', conviction: 'media' }),
    crearRec({ asset: 'META', action: 'mantener', conviction: 'media' }),
    crearRec({ asset: 'GOOGL', action: 'mantener', conviction: 'media' }),
    crearRec({ asset: 'UBER', action: 'mantener', conviction: 'media' }),
    crearRec({ asset: 'NU', action: 'aumentar', targetPct: 10, conviction: 'media' }),
    crearRec({ asset: 'ECOPETROL.CL', action: 'mantener', conviction: 'media' }),
    crearRec({ asset: 'UNH', action: 'mantener', conviction: 'media' }),
  ];
  const deHoy = [
    crearRec({ asset: 'BTC-USD', action: 'reducir', targetPct: 10, conviction: 'alta' }),
    crearRec({ asset: 'MELI', action: 'mantener', conviction: 'media' }),
    crearRec({ asset: 'META', action: 'mantener', conviction: 'media' }),
    crearRec({ asset: 'GOOGL', action: 'mantener', conviction: 'baja' }),
    crearRec({ asset: 'UBER', action: 'mantener', conviction: 'baja' }),
    crearRec({ asset: 'NU', action: 'aumentar', targetPct: 15, conviction: 'media' }),
    crearRec({ asset: 'ECOPETROL.CL', action: 'vender', conviction: 'alta' }),
    crearRec({ asset: 'UNH', action: 'vender', conviction: 'alta' }),
  ];

  it('vigentes, ajustadas y ya no vigentes como las esperaría el PO', () => {
    const estados = anteriores.map((a) => guard.compararConHoy(a, deHoy).state);

    expect(estados).toEqual([
      'vigente', 'vigente', 'vigente', 'ajustada', 'ajustada', 'ajustada', 'no_vigente', 'no_vigente',
    ]);
  });

  it('una ajustada dice de qué a qué cambió', () => {
    expect(guard.compararConHoy(anteriores[3], deHoy).changes)
      .toEqual([{ field: 'conviction', from: 'media', to: 'baja' }]);
    expect(guard.compararConHoy(anteriores[5], deHoy).changes)
      .toEqual([{ field: 'targetPct', from: 10, to: 15 }]);
  });

  it('nunca sale «Ajustada» sin ningún cambio ni «Ya no vigente» para un mantener que se repite', () => {
    const resultado = guard.compararConHoy(anteriores[1], deHoy);

    expect(resultado).toEqual({ state: 'vigente', reason: 'misma_recomendacion', changes: [] });
  });

  it('QA-10: una reducción anterior frente a un «aumentar» de hoy es ya no vigente', () => {
    const resultado = guard.compararConHoy(
      crearRec({ statement: 'Reducir NFLX del 0,6 % al 0,3 %', asset: 'NFLX', action: null }),
      [crearRec({ asset: 'NFLX', action: 'aumentar', targetPct: 3 })],
    );

    expect(resultado.state).toBe('no_vigente');
  });

  it('comprar y aumentar son la misma acción', () => {
    const resultado = guard.compararConHoy(
      crearRec({ asset: 'TLT', action: 'comprar', targetPct: 3 }),
      [crearRec({ asset: 'TLT', action: 'aumentar', targetPct: 3 })],
    );

    expect(resultado.state).toBe('vigente');
  });

  it('sin recomendación de hoy para ese activo no hay comparación', () => {
    expect(guard.compararConHoy(anteriores[0], [])).toBeNull();
  });
});

describe('zonaCruzada — QA-05, cruzar la zona ajusta; moverse no (RN-5.7-C)', () => {
  const TLT = crearRec({
    statement: 'Comprar TLT para alcanzar el 3 % del portafolio, tramo 1 en 88–90',
    asset: 'TLT',
    action: null,
    targetPct: 3,
    priceZone: '88–90',
  });

  it('con TLT a 87,95 el tramo 1 se alcanzó: ajustada', () => {
    const resultado = guard.compararConHoy(TLT, [{ ...TLT, action: 'comprar' }], { precioHoy: 87.95 });

    expect(resultado.state).toBe('ajustada');
    expect(resultado.reason).toBe('zona_alcanzada');
    expect(resultado.changes[0]).toMatchObject({ field: 'zone', from: '88–90', price: 87.95 });
  });

  it('con TLT a 90,50 no entró en la zona: sin ajuste por precio', () => {
    expect(guard.compararConHoy(TLT, [{ ...TLT, action: 'comprar' }], { precioHoy: 90.5 }).state).toBe('vigente');
  });

  it('una reducción alcanza su zona cuando el precio sube hasta ella', () => {
    const reducir = crearRec({ action: 'reducir', priceZone: 'por encima de 520 USD' });
    expect(guard.zonaCruzada(reducir, 525)).toEqual({ zone: 'por encima de 520 USD' });
    expect(guard.zonaCruzada(reducir, 500)).toBeNull();
  });

  it('los números de una zona: miles con punto, porcentajes y plazos', () => {
    expect(guard.limitesDeZona('61.914–62.500')).toEqual({ min: 61914, max: 62500 });
    expect(guard.limitesDeZona('130,5-135 USD')).toEqual({ min: 130.5, max: 135 });
    expect(guard.limitesDeZona('dentro de 2 a 4 semanas')).toBeNull();
    expect(guard.limitesDeZona('tramo de 0,8 %')).toBeNull();
  });
});

describe('accionDeEnunciado y enunciadoDe — recomendaciones de antes de la 5.7.1', () => {
  it('lee la acción del verbo, en los dos idiomas', () => {
    expect(guard.accionDeEnunciado('Reducir la posición de NVDA del 7 % actual al 3 %')).toBe('reducir');
    expect(guard.accionDeEnunciado('**Mantener VUAA.L**')).toBe('mantener');
    expect(guard.accionDeEnunciado('Comprar TLT para alcanzar el 3 %')).toBe('comprar');
    expect(guard.accionDeEnunciado('Increase NVDA in tranches up to 13%')).toBe('aumentar');
    expect(guard.accionDeEnunciado('Sell UNH')).toBe('vender');
    expect(guard.accionDeEnunciado('Reducir NVDA para comprar TLT')).toBe('reducir');
    expect(guard.accionDeEnunciado('Diversificar')).toBeNull();
  });

  it('arma el enunciado con la acción, el activo y la magnitud', () => {
    const rec = crearRec({ action: 'reducir', asset: 'MELI', targetPct: 4 });

    expect(guard.enunciadoDe(rec, 5.7, 'es')).toBe('Reducir MELI del 5,7 % al 4 % del portafolio');
    expect(guard.enunciadoDe(rec, 5.7, 'en')).toBe('Reduce MELI from 5.7% to 4% of the portfolio');
    expect(guard.enunciadoDe({ ...rec, asset: null }, 5.7, 'es')).toBeNull();
  });
});
