/**
 * HU 5.7.1 — La ficha de cuatro pilares y las señales del criterio de decisión.
 *
 * Lo que defiende este archivo es que **las fronteras las decide el código**,
 * no la aritmética del modelo: el 10,0 % frente al 10,1 % del sobrepeso
 * (QA-12), el "P/E 28 ≤ 30" de la valoración extrema (QA-13), cada señal de
 * deterioro del Nivel 2 (QA-09, QA-10) y lo que la ficha declara cuando falta
 * un dato (QA-18). Y que la ficha de NVDA del 2026-11-19 sale con las cifras
 * de la historia (QA-03).
 *
 * @see services/copilot/copilotAssetCardService.js
 * @see platform-docs/stories/5.7.1-recomendaciones-sin-panico/qa.md
 */

jest.mock('../../financeQuery', () => ({ getHistoricalPrices: jest.fn() }));

const { DateTime } = require('luxon');
const card = require('../copilotAssetCardService');

const { SENAL_DETERIORO, VALORACION_EXTREMA } = card;

// ============================================================================
// FÁBRICAS
// ============================================================================

/** "Hoy" de la historia: el análisis del 2026-11-19, por la tarde en Nueva York. */
const HOY = new Date('2026-11-20T15:00:00Z');

/**
 * Una serie diaria de días hábiles, con el mismo cierre todos los días y los
 * puntos que se le fijen encima.
 *
 * @param {object} [opciones]
 * @param {string} [opciones.desde]
 * @param {string} [opciones.hasta]
 * @param {number} [opciones.cierre]
 * @param {object} [opciones.puntos] - `{ 'YYYY-MM-DD': {close, high, low} }`.
 * @returns {object}
 */
const crearSerie = ({
  desde = '2025-11-19', hasta = '2026-11-19', cierre = 200, puntos = {},
} = {}) => {
  const serie = {};
  let dia = DateTime.fromISO(desde);
  const fin = DateTime.fromISO(hasta);
  while (dia <= fin) {
    if (dia.weekday <= 5) {
      serie[dia.toISODate()] = { close: cierre, high: cierre, low: cierre };
    }
    dia = dia.plus({ days: 1 });
  }
  for (const [fecha, punto] of Object.entries(puntos)) {
    serie[fecha] = { close: cierre, high: cierre, low: cierre, ...punto };
  }
  return serie;
};

/** Una serie con exactamente `n` sesiones que terminan el 2026-11-19. */
const crearSerieDeSesiones = (n, cierre = 100) => {
  const serie = {};
  let dia = DateTime.fromISO('2026-11-19');
  while (Object.keys(serie).length < n) {
    if (dia.weekday <= 5) serie[dia.toISODate()] = { close: cierre, high: cierre, low: cierre };
    dia = dia.minus({ days: 1 });
  }
  return serie;
};

/**
 * D-NVDA (qa.md): 145,98 el 2026-11-19, máximo 239,24 el 6-oct, mínimo 145,71,
 * y los cierres de los cortes fijados para dar −34,2 %, −32,0 %, −22,7 % y −19,2 %.
 */
const serieNvda = () => crearSerie({
  puntos: {
    '2025-11-19': { close: 180.67 },
    '2025-12-31': { close: 188.84 },
    '2026-08-19': { close: 214.68 },
    '2026-10-06': { close: 238.5, high: 239.24 },
    '2026-10-19': { close: 221.85 },
    '2026-11-18': { close: 147.1, low: 145.71 },
    '2026-11-19': { close: 145.98 },
  },
});

const crearTrimestre = (endDate, totalRevenue, operatingIncome) => ({
  endDate, totalRevenue, operatingIncome,
});

/** Ingresos y margen crecientes en los 4 últimos trimestres (D-NVDA). */
const crearIncome = (statements) => ({
  symbol: 'NVDA',
  currency: 'USD',
  statements: statements || [
    crearTrimestre('2026-10-26', 57.0e9, 36.0e9),
    crearTrimestre('2026-07-27', 46.7e9, 28.4e9),
    crearTrimestre('2026-04-27', 44.1e9, 26.0e9),
    crearTrimestre('2026-01-26', 39.3e9, 22.0e9),
  ],
});

/** FY 2026 frente a FY 2025: +65,5 %. */
const crearAnual = (statements) => ({
  symbol: 'NVDA',
  currency: 'USD',
  statements: statements || [
    crearTrimestre('2026-01-26', 165.5e9, 100e9),
    crearTrimestre('2025-01-26', 100e9, 55e9),
  ],
});

const crearQuote = (overrides = {}) => ({ symbol: 'NVDA', pe: '15.68', eps: '9.31', ...overrides });

const crearAnalisis = (overrides = {}) => ({
  symbol: 'NVDA',
  recommendationKey: 'buy',
  targetPrice: { low: 180, mean: 246.67, high: 320, numberOfAnalysts: 57 },
  earningsEstimates: [{ period: '0q', endDate: '2027-01-31', growth: '+40.00%' }],
  ...overrides,
});

const crearHolding = (overrides = {}) => ({
  symbol: 'NVDA', name: 'NVIDIA', asset_type: 'stock', weight: 0.085, ...overrides,
});

const fichaNvda = (overrides = {}) => card.construirFicha({
  holding: crearHolding(),
  quote: crearQuote(),
  analysis: crearAnalisis(),
  income: crearIncome(),
  anual: crearAnual(),
  serie: serieNvda(),
  language: 'es',
  now: HOY,
  ...overrides,
});

const valorDe = (ficha, label) => (ficha.items.find((i) => i.label === label) || {}).value;
const filaDe = (ficha, label) => ficha.items.find((i) => i.label === label);
const faltaDe = (ficha, label) => ficha.missing.find((m) => m.label === label);
const etiquetas = (ficha) => ficha.items.map((i) => i.label);

const ES = card.ETIQUETAS_FICHA.es;
const EN = card.ETIQUETAS_FICHA.en;

// ============================================================================

describe('QA-03 — la ficha de NVDA del 2026-11-19, con fecha o periodo en cada dato', () => {
  it('el técnico: precio, variaciones y rango de 52 semanas', () => {
    const ficha = fichaNvda();

    expect(filaDe(ficha, ES.precio)).toEqual({ label: ES.precio, value: '145.98', period: '2026-11-19' });
    expect(valorDe(ficha, ES.variacion1m)).toBe('-34.2%');
    expect(valorDe(ficha, ES.variacion3m)).toBe('-32.0%');
    expect(valorDe(ficha, ES.variacionAno)).toBe('-22.7%');
    expect(valorDe(ficha, ES.variacion1a)).toBe('-19.2%');
    expect(filaDe(ficha, ES.maximo52)).toEqual({
      label: ES.maximo52, value: '239.24 (-39.0%)', period: '2026-10-06',
    });
    expect(filaDe(ficha, ES.minimo52)).toEqual({
      label: ES.minimo52, value: '145.71 (+0.2%)', period: '2026-11-18',
    });
  });

  it('cada variación dice entre qué dos cierres se midió', () => {
    expect(filaDe(fichaNvda(), ES.variacion1m).period).toBe('2026-10-19 → 2026-11-19');
  });

  it('la valoración: P/E, consenso, objetivos, potencial +69,0 % y crecimiento del BPA', () => {
    const ficha = fichaNvda();

    expect(valorDe(ficha, ES.per)).toBe('15.68');
    expect(valorDe(ficha, ES.bpa)).toBe('9.31');
    expect(valorDe(ficha, ES.consenso)).toBe('buy');
    expect(valorDe(ficha, ES.objetivos)).toBe('180.00 / 246.67 / 320.00 (57 analistas)');
    expect(valorDe(ficha, ES.potencial)).toBe('+69.0%');
    expect(filaDe(ficha, ES.crecimientoBpa)).toEqual({
      label: ES.crecimientoBpa, value: '+40.0%', period: 'Q1 2027',
    });
  });

  it('los fundamentales: tendencia trimestral con su periodo y crecimiento anual +65,5 %', () => {
    const ficha = fichaNvda();

    expect(filaDe(ficha, ES.ingresosTrimestrales)).toEqual({
      label: ES.ingresosTrimestrales,
      value: '39.30 B USD → 44.10 B USD → 46.70 B USD → 57.00 B USD (creciente)',
      period: 'Q1 2026 – Q4 2026',
    });
    expect(valorDe(ficha, ES.margenTrimestral)).toMatch(/\(creciente\)$/);
    expect(filaDe(ficha, ES.crecimientoAnual)).toEqual({
      label: ES.crecimientoAnual, value: '+65.5%', period: 'FY 2026 vs FY 2025',
    });
  });

  it('todo dato lleva su fecha o su periodo (Escenario 2)', () => {
    for (const item of fichaNvda().items) expect(item.period).toBeTruthy();
  });

  it('con NVDA al 8,5 % no hay sobrepeso, ni valoración extrema, ni deterioro; sí cumple la excepción de oportunidad', () => {
    expect(fichaNvda().signals).toEqual({
      weightPct: 8.5,
      overweightThresholdPct: 10,
      overweight: false,
      roomToThresholdPct: 1.5,
      maxTranchePct: 0.5,
      extremeValuation: null,
      deterioration: [],
      opportunityException: {
        ceilingPct: 15,
        roomToCeilingPct: 6.5,
        maxTranchePct: 2.1,
        dca: null,
        evidence: [
          'no deterioration signals',
          'revenue growth +65.5%',
          'consensus buy',
          'upside to mean target +69.0%',
          'P/E vs revenue growth 0.24',
          '-39.0% from 52-week high',
        ],
      },
      // HU 5.7.3: sin señales, NVDA no se reduce ni se vende.
      allowedActions: ['mantener', 'aumentar', 'comprar'],
      holdMaxConviction: null,
    });
  });

  it('no reformula ninguna métrica del portafolio (RN-5.7-O)', () => {
    const texto = JSON.stringify(fichaNvda()).toLowerCase();

    for (const metrica of ['beta', 'valor total', 'perfil de riesgo', 'sector']) {
      expect(texto).not.toContain(metrica);
    }
  });
});

describe('QA-04 — la ficha de una cripto y la de un ETF solo traen lo que aplica', () => {
  it('BTC-USD: solo técnico, sin P/E, resultados, consenso ni objetivos', () => {
    const ficha = card.construirFicha({
      holding: crearHolding({ symbol: 'BTC-USD', asset_type: 'crypto', weight: 0.115 }),
      quote: crearQuote(),
      analysis: crearAnalisis(),
      serie: crearSerie({ cierre: 61914 }),
      language: 'es',
      now: HOY,
    });

    expect(ficha.kind).toBe('crypto');
    expect(etiquetas(ficha)).toEqual([
      ES.precio, ES.variacion1m, ES.variacion3m, ES.variacionAno, ES.variacion1a,
      ES.maximo52, ES.minimo52, ES.sma50, ES.sma200, ES.rsi,
    ]);
    expect(ficha.missing).toEqual([]);
    expect(ficha.signals.extremeValuation).toBeNull();
    expect(ficha.signals.deterioration).toEqual([]);
  });

  it('VUAA.L: el técnico y el P/E del fondo si la plataforma lo da', () => {
    const ficha = card.construirFicha({
      holding: crearHolding({ symbol: 'VUAA.L', asset_type: 'etf', weight: 0.282 }),
      quote: crearQuote({ pe: '27.10' }),
      analysis: crearAnalisis(),
      serie: crearSerie(),
      language: 'es',
      now: HOY,
    });

    expect(ficha.kind).toBe('etf');
    expect(valorDe(ficha, ES.per)).toBe('27.10');
    for (const label of [ES.consenso, ES.objetivos, ES.potencial, ES.ingresosTrimestrales]) {
      expect(etiquetas(ficha)).not.toContain(label);
    }
  });

  it('un ETF sin P/E no lo declara como faltante: no le aplica', () => {
    const ficha = card.construirFicha({
      holding: crearHolding({ symbol: 'VUAA.L', asset_type: 'etf' }),
      quote: crearQuote({ pe: null }),
      serie: crearSerie(),
      language: 'es',
      now: HOY,
    });

    expect(etiquetas(ficha)).not.toContain(ES.per);
    expect(faltaDe(ficha, ES.per)).toBeUndefined();
  });
});

describe('QA-07 — cuánto se puede aumentar sin pasar del 10 % (RN-5.7-G)', () => {
  it.each([
    [0.099, 0.1],
    [0.1, 0],
    [0.06, 4],
    [0.115, 0],
  ])('con un peso de %p el margen es %p puntos', (weight, margen) => {
    const ficha = fichaNvda({ holding: crearHolding({ weight }) });

    expect(ficha.signals.roomToThresholdPct).toBe(margen);
  });
});

describe('QA-12 — la frontera del sobrepeso y la exención de los ETF (RN-5.7-E)', () => {
  it.each([
    ['MELI al 10,0 %', 'stock', 0.1, false],
    ['MELI al 10,1 %', 'stock', 0.101, true],
    ['BTC-USD al 11,5 %', 'crypto', 0.115, true],
    ['VUAA.L al 28,2 %', 'etf', 0.282, false],
  ])('%s', (_caso, clase, pesoFraccion, sobrepeso) => {
    const senales = card.senalesDeDecision({
      clase, pesoPct: pesoFraccion * 100, tecnico: null, valoracion: null, tendencia: null,
    });

    expect(senales.overweight).toBe(sobrepeso);
  });

  it('el peso se juzga con el decimal que ve el usuario: 10,04 % se lee 10,0 % y no es sobrepeso', () => {
    const senales = card.senalesDeDecision({ clase: 'stock', pesoPct: 10.04 });

    expect(senales.weightPct).toBe(10);
    expect(senales.overweight).toBe(false);
  });

  it('un ETF no tiene umbral ni margen', () => {
    const senales = card.senalesDeDecision({ clase: 'etf', pesoPct: 28.2 });

    expect(senales.overweightThresholdPct).toBeNull();
    expect(senales.roomToThresholdPct).toBeNull();
  });
});

describe('QA-13 — la valoración extrema (RN-5.7-F)', () => {
  const evaluar = ({ precio, medio, alto, pe, crecimiento }) => {
    const valoracion = card.valoracionDe(
      { pe },
      { targetPrice: { mean: medio, high: alto } },
      precio,
      crecimiento,
    );
    return card.senalesDeDecision({
      clase: 'stock', pesoPct: 2.2, tecnico: { precio }, valoracion, tendencia: null,
    }).extremeValuation;
  };

  it.each([
    ['supera el objetivo alto', { precio: 420, medio: 400, alto: 410, pe: '28', crecimiento: 15 },
      VALORACION_EXTREMA.OBJETIVO_ALTO],
    ['supera el medio con P/E 35 > 2 × 15', { precio: 405, medio: 400, alto: 450, pe: '35', crecimiento: 15 },
      VALORACION_EXTREMA.OBJETIVO_MEDIO_Y_PE],
    ['28 ≤ 30: no hay', { precio: 405, medio: 400, alto: 450, pe: '28', crecimiento: 15 }, null],
    ['precio bajo el objetivo medio: no hay', { precio: 395, medio: 400, alto: 450, pe: '40', crecimiento: 15 }, null],
    ['con crecimiento negativo solo cuenta el objetivo alto', {
      precio: 405, medio: 400, alto: 450, pe: '35', crecimiento: -5,
    }, null],
    ['con pérdidas (sin P/E) y sobre el objetivo alto', {
      precio: 455, medio: 400, alto: 450, pe: '—', crecimiento: 10,
    }, VALORACION_EXTREMA.OBJETIVO_ALTO],
  ])('%s', (_caso, datos, esperado) => {
    expect(evaluar(datos)).toBe(esperado);
  });

  it('ni una cripto ni un ETF se juzgan por objetivos de analistas', () => {
    const valoracion = card.valoracionDe({ pe: '50' }, { targetPrice: { mean: 10, high: 20 } }, 100, 1);

    for (const clase of ['crypto', 'etf']) {
      expect(card.senalesDeDecision({ clase, pesoPct: 5, tecnico: { precio: 100 }, valoracion })
        .extremeValuation).toBeNull();
    }
  });
});

describe('QA-09 y QA-10 — las señales de deterioro del Nivel 2 (RN-5.7-D)', () => {
  const crecientes = () => crearIncome([
    crearTrimestre('2026-09-30', 10.0e9, 2.5e9),
    crearTrimestre('2026-06-30', 9.5e9, 2.375e9),
    crearTrimestre('2026-03-31', 9.0e9, 2.25e9),
    crearTrimestre('2025-12-31', 8.5e9, 2.125e9),
  ]);
  const anualSano = () => crearAnual([
    crearTrimestre('2026-01-31', 38e9, 9.5e9),
    crearTrimestre('2025-01-31', 35e9, 8.75e9),
  ]);
  const senales = ({ income = crecientes(), anual = anualSano(), crecimientoBpa = '+5.00%' } = {}) => fichaNvda({
    holding: crearHolding({ symbol: 'CRM', weight: 0.035 }),
    income,
    anual,
    analysis: crearAnalisis({
      earningsEstimates: [{ period: '0q', endDate: '2026-10-31', growth: crecimientoBpa }],
    }),
  }).signals.deterioration.map((d) => d.signal);

  it('con todo sano, ninguna', () => {
    expect(senales()).toEqual([]);
  });

  it('solo los ingresos del último año fiscal por debajo de los del anterior', () => {
    const anual = crearAnual([
      crearTrimestre('2026-01-31', 34e9, 8.5e9),
      crearTrimestre('2025-01-31', 35e9, 8.75e9),
    ]);

    expect(senales({ anual })).toEqual([SENAL_DETERIORO.INGRESOS_FY]);
  });

  describe('el margen operativo anual: caída material y sin desmentir (decisión del PO, 2026-11-20)', () => {
    /** FY 2025 al 25,0 %; FY 2026 con el margen que se pida, ingresos +8,6 %. */
    const anualConMargen = (margenPct) => crearAnual([
      crearTrimestre('2026-01-31', 38e9, 38e9 * (margenPct / 100)),
      crearTrimestre('2025-01-31', 35e9, 8.75e9),
    ]);
    /** Margen trimestral 22 % → 23 % → 24 %: sube en los dos últimos. */
    const margenSubiendo = () => crearIncome([
      crearTrimestre('2026-09-30', 10.0e9, 2.4e9),
      crearTrimestre('2026-06-30', 9.5e9, 2.185e9),
      crearTrimestre('2026-03-31', 9.0e9, 1.98e9),
    ]);

    it('una caída de 3,0 pp con los trimestres planos es señal, con su dato', () => {
      const ficha = fichaNvda({
        holding: crearHolding({ symbol: 'UNH', weight: 0.035 }),
        income: crecientes(),
        anual: anualConMargen(22),
        analysis: crearAnalisis({
          earningsEstimates: [{ period: '0q', endDate: '2026-10-31', growth: '+5.00%' }],
        }),
      });

      expect(ficha.signals.deterioration).toEqual([
        { signal: SENAL_DETERIORO.MARGEN_FY, evidence: 'FY 2026 vs FY 2025: 22.0% < 25.0%' },
      ]);
    });

    it('una caída de 2,9 pp no es material: no es señal', () => {
      expect(senales({ anual: anualConMargen(22.1) })).toEqual([]);
    });

    it('NVDA del 2026-11-20: 62,4 % → 60,4 % (−2,0 pp) con el margen trimestral subiendo, no es señal', () => {
      const anual = crearAnual([
        crearTrimestre('2026-01-26', 165.5e9, 165.5e9 * 0.604),
        crearTrimestre('2025-01-26', 100e9, 62.4e9),
      ]);
      const income = crearIncome([
        crearTrimestre('2026-10-26', 96.22e9, 96.22e9 * 0.662),
        crearTrimestre('2026-07-27', 81.61e9, 81.61e9 * 0.656),
        crearTrimestre('2026-04-27', 68.13e9, 68.13e9 * 0.65),
      ]);

      expect(senales({ anual, income })).toEqual([]);
    });

    it('una caída de 5 pp desmentida por dos trimestres de margen subiendo no es señal', () => {
      expect(senales({ anual: anualConMargen(20), income: margenSubiendo() })).toEqual([]);
    });
  });

  it('solo el margen en caída en los dos últimos trimestres', () => {
    const income = crearIncome([
      crearTrimestre('2026-09-30', 10.0e9, 2.0e9),
      crearTrimestre('2026-06-30', 9.5e9, 2.375e9),
      crearTrimestre('2026-03-31', 9.0e9, 2.7e9),
      crearTrimestre('2025-12-31', 8.5e9, 2.125e9),
    ]);

    expect(senales({ income })).toEqual([SENAL_DETERIORO.MARGEN_2T]);
  });

  it('solo el crecimiento del BPA esperado del −8 %', () => {
    expect(senales({ crecimientoBpa: '-8.00%' })).toEqual([SENAL_DETERIORO.BPA_ESPERADO]);
  });

  it('una caída en un solo trimestre no es señal: hacen falta dos seguidas', () => {
    const income = crearIncome([
      crearTrimestre('2026-09-30', 9.8e9, 2.45e9),
      crearTrimestre('2026-06-30', 10.0e9, 2.5e9),
      crearTrimestre('2026-03-31', 9.0e9, 2.25e9),
    ]);

    expect(senales({ income })).toEqual([]);
  });

  it('QA-10: las tres a la vez, cada una con el dato que la prueba', () => {
    const ficha = fichaNvda({
      holding: crearHolding({ symbol: 'CRM', weight: 0.035 }),
      anual: crearAnual([
        crearTrimestre('2026-01-31', 34e9, 8.5e9),
        crearTrimestre('2025-01-31', 35e9, 8.75e9),
      ]),
      income: crearIncome([
        crearTrimestre('2026-09-30', 10.0e9, 2.0e9),
        crearTrimestre('2026-06-30', 9.5e9, 2.375e9),
        crearTrimestre('2026-03-31', 9.0e9, 2.7e9),
      ]),
      analysis: crearAnalisis({
        earningsEstimates: [{ period: '0q', endDate: '2026-10-31', growth: '-15.00%' }],
      }),
    });

    expect(ficha.signals.deterioration).toEqual([
      { signal: SENAL_DETERIORO.INGRESOS_FY, evidence: 'FY 2026 vs FY 2025: 34.00 B USD < 35.00 B USD' },
      { signal: SENAL_DETERIORO.MARGEN_2T, evidence: 'Q1 2026 → Q2 2026 → Q3 2026: 30.0% → 25.0% → 20.0%' },
      { signal: SENAL_DETERIORO.BPA_ESPERADO, evidence: 'Q4 2026 -15.0%' },
    ]);
  });
});

describe('la excepción de oportunidad y el DCA (RN-5.7-E, decisión del PO del 2026-11-20)', () => {
  /** Las compras de NVDA del usuario de prueba en noviembre de 2026. */
  const COMPRAS_NOVIEMBRE = ['2026-11-05', '2026-11-09', '2026-11-10', '2026-11-12', '2026-11-20'];
  const NOW = new Date('2026-11-20T21:00:00Z');
  const conPeso = (weight, overrides = {}) => fichaNvda({
    holding: crearHolding({ weight, ...overrides.holding }),
    now: NOW,
    ...overrides.ficha,
  }).signals;

  describe('compraEscalonada', () => {
    it('cinco compras en tres semanas distintas son DCA', () => {
      expect(card.compraEscalonada(COMPRAS_NOVIEMBRE, NOW)).toEqual({
        purchases: 5, weeks: 3, from: '2026-11-05', to: '2026-11-20',
      });
    });

    it('cuatro compras en solo dos semanas no lo son', () => {
      expect(card.compraEscalonada(['2026-11-09', '2026-11-10', '2026-11-12', '2026-11-20'], NOW)).toBeNull();
    });

    it('las compras de hace más de 90 días no cuentan', () => {
      expect(card.compraEscalonada(['2026-08-21', '2026-11-10', '2026-11-20'], NOW)).toBeNull();
      expect(card.compraEscalonada(['2026-08-22', '2026-11-10', '2026-11-20'], NOW)).toEqual({
        purchases: 3, weeks: 3, from: '2026-08-22', to: '2026-11-20',
      });
    });

    it('sin compras o con fechas inválidas, no hay patrón', () => {
      expect(card.compraEscalonada([], NOW)).toBeNull();
      expect(card.compraEscalonada(undefined, NOW)).toBeNull();
      expect(card.compraEscalonada(['no-es-fecha', '2026-11-05', '2026-11-12'], NOW)).toBeNull();
    });
  });

  it('NVDA al 10,4 % sin DCA: techo del 15 %, con 4,6 pp de margen y tramos de 1,5 como mucho', () => {
    expect(conPeso(0.104).opportunityException).toMatchObject({
      ceilingPct: 15, roomToCeilingPct: 4.6, maxTranchePct: 1.5, dca: null,
    });
  });

  describe('tramoMaximo — nunca todo de golpe (decisión del PO, 2026-11-20)', () => {
    it.each([
      [9.6, 3.2],
      [7.4, 2.4],
      [0.5, 0.1],
      [0, 0],
    ])('con %d pp de margen, cada tramo hasta %d', (margen, tramo) => {
      expect(card.tramoMaximo(margen)).toBe(tramo);
    });

    it('sin margen calculable (un ETF), no hay tope', () => {
      expect(card.tramoMaximo(null)).toBeNull();
    });

    it('NVDA con DCA: 9,6 pp de margen, tramos de 3,2 como mucho; el margen normal es 0', () => {
      const s = conPeso(0.104, { holding: { purchaseDates: COMPRAS_NOVIEMBRE } });

      expect(s.maxTranchePct).toBe(0);
      expect(s.opportunityException.maxTranchePct).toBe(3.2);
    });
  });

  it('NVDA al 10,4 % con las compras escalonadas de noviembre: techo del 20 %, y la evidencia lo dice', () => {
    const { opportunityException: e, overweight } = conPeso(0.104, {
      holding: { purchaseDates: COMPRAS_NOVIEMBRE },
    });

    expect(overweight).toBe(true);
    expect(e.ceilingPct).toBe(20);
    expect(e.roomToCeilingPct).toBe(9.6);
    expect(e.evidence[e.evidence.length - 1])
      .toBe('staggered buying: 5 purchases in 3 weeks (2026-11-05 → 2026-11-20)');
  });

  it('por encima del techo, el margen es 0', () => {
    expect(conPeso(0.17).opportunityException.roomToCeilingPct).toBe(0);
  });

  it.each([
    ['con una señal de deterioro', { analysis: crearAnalisis({ earningsEstimates: [{ period: '0q', endDate: '2027-01-31', growth: '-1.00%' }] }) }],
    ['con consenso hold', { analysis: crearAnalisis({ recommendationKey: 'hold' }) }],
    ['con potencial por debajo del 30 %', { analysis: crearAnalisis({ targetPrice: { low: 100, mean: 180, high: 320, numberOfAnalysts: 57 } }) }],
    ['con P/E por encima del crecimiento', { quote: crearQuote({ pe: '70' }) }],
    ['con crecimiento de ingresos por debajo del 15 %', {
      anual: crearAnual([crearTrimestre('2026-01-26', 110e9, 60.5e9), crearTrimestre('2025-01-26', 100e9, 55e9)]),
    }],
  ])('no aplica %s', (_caso, ficha) => {
    expect(conPeso(0.104, { ficha }).opportunityException).toBeNull();
  });

  it('no aplica a un ETF ni a una cripto', () => {
    expect(conPeso(0.104, { holding: { symbol: 'VOO', asset_type: 'etf' } }).opportunityException).toBeNull();
    expect(conPeso(0.104, { holding: { symbol: 'BTC-USD', asset_type: 'crypto' } }).opportunityException).toBeNull();
  });
});

describe('QA-18 — la ficha declara lo que falta, sin rellenarlo (RN-5-H)', () => {
  it('ECOPETROL.CL sin objetivos: "no disponible" con su motivo y sin potencial', () => {
    const ficha = fichaNvda({
      holding: crearHolding({ symbol: 'ECOPETROL.CL', weight: 0.014 }),
      analysis: { symbol: 'ECOPETROL.CL', earningsEstimates: [] },
    });

    expect(faltaDe(ficha, ES.objetivos)).toEqual({
      label: ES.objetivos, reason: ES.motivos.sinObjetivos,
    });
    expect(faltaDe(ficha, ES.potencial)).toEqual({
      label: ES.potencial, reason: ES.motivos.sinObjetivos,
    });
    expect(etiquetas(ficha)).not.toContain(ES.potencial);
    expect(ficha.signals.extremeValuation).toBeNull();
  });

  it('un activo con 120 sesiones: SMA200 no disponible por historial insuficiente', () => {
    const ficha = fichaNvda({ serie: crearSerieDeSesiones(120) });

    expect(faltaDe(ficha, ES.sma200)).toEqual({
      label: ES.sma200, reason: 'historial insuficiente: 120 de 200 sesiones',
    });
    expect(etiquetas(ficha)).toContain(ES.sma50);
    expect(etiquetas(ficha)).not.toContain(ES.sma200);
  });

  it('QA-19: una serie sin cierres válidos deja el técnico, las variaciones y el potencial sin calcular', () => {
    const ficha = fichaNvda({ serie: { '2026-11-19': { close: null }, '2026-11-18': {} } });

    expect(faltaDe(ficha, ES.tecnico)).toEqual({ label: ES.tecnico, reason: ES.motivos.sinHistorial });
    expect(faltaDe(ficha, ES.potencial)).toEqual({ label: ES.potencial, reason: ES.motivos.sinPrecio });
    for (const label of [ES.precio, ES.variacion1m, ES.potencial, ES.rsi]) {
      expect(etiquetas(ficha)).not.toContain(label);
    }
  });

  it('sin ningún origen la ficha se arma igual, solo con lo que falta', () => {
    const ficha = card.construirFicha({ holding: crearHolding(), language: 'es', now: HOY });

    expect(ficha.items).toEqual([]);
    expect(ficha.missing).toEqual([
      { label: ES.tecnico, reason: ES.motivos.sinHistorial },
      { label: ES.per, reason: ES.motivos.sinPer },
      { label: ES.consenso, reason: ES.motivos.sinConsenso },
      { label: ES.objetivos, reason: ES.motivos.sinObjetivos },
      { label: ES.potencial, reason: ES.motivos.sinPrecio },
      { label: ES.crecimientoBpa, reason: ES.motivos.sinCrecimientoBpa },
      { label: ES.resultados, reason: ES.motivos.sinResultados },
      { label: ES.crecimientoAnual, reason: ES.motivos.sinAnual },
    ]);
    expect(ficha.asOf).toBe('2026-11-20');
  });
});

describe('QA-21 — las etiquetas en inglés', () => {
  it('las filas y los motivos de lo que falta', () => {
    const ficha = fichaNvda({ language: 'en', serie: crearSerieDeSesiones(120) });

    expect(valorDe(ficha, EN.precio)).toBe('100.00');
    expect(faltaDe(ficha, EN.sma200).reason).toBe('not enough history: 120 of 200 sessions');
    expect(etiquetas(ficha)).toContain('Analyst targets (low / mean / high)');
  });
});

describe('las medias y el RSI, contra valores conocidos', () => {
  it('SMA50: la media de los últimos 50 cierres, y null con menos', () => {
    const valores = Array.from({ length: 60 }, (_, i) => i + 1);

    expect(card.mediaMovil(valores, 50)).toBe(35.5);
    expect(card.mediaMovil(valores.slice(0, 49), 50)).toBeNull();
  });

  it('RSI(14) de Wilder: el ejemplo clásico (medias 0,2386 y 0,1000) da 70,46 y luego 66,25', () => {
    // Las tablas publicadas dicen 70,53 y 66,32 porque redondean las medias a
    // dos decimales antes de dividir; sin redondear (estándar §4.13) es esto.
    const cierres = [44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08,
      45.89, 46.03, 45.61, 46.28, 46.28];

    expect(card.rsiWilder(cierres)).toBeCloseTo(70.464, 3);
    expect(card.rsiWilder([...cierres, 46.00])).toBeCloseTo(66.250, 3);
  });

  it('RSI: todo subidas es 100 y con menos de 15 cierres no hay', () => {
    expect(card.rsiWilder(Array.from({ length: 20 }, (_, i) => 10 + i))).toBe(100);
    expect(card.rsiWilder(Array.from({ length: 14 }, (_, i) => 10 + i))).toBeNull();
  });

  it('la posición frente a la media dice el lado y el porcentaje', () => {
    const ficha = fichaNvda();

    expect(valorDe(ficha, ES.sma50)).toMatch(/^\d+\.\d{2} \(precio por debajo, -\d+\.\d%\)$/);
  });
});

describe('el corte del año se toma en Nueva York', () => {
  it('el 31 de diciembre a las 22:00 de Nueva York sigue siendo el año que termina', () => {
    // En UTC ya es 2027-01-01: la variación "en el año" saldría contra el
    // cierre del 31 de diciembre de 2026, es decir, ~0 %.
    const serie = crearSerie({
      desde: '2025-12-01',
      hasta: '2026-12-31',
      cierre: 100,
      puntos: { '2025-12-31': { close: 80 } },
    });
    const tecnico = card.tecnicoDeLaSerie(serie, new Date('2027-01-01T03:00:00Z'));

    expect(tecnico.variaciones.ytd.desde).toBe('2025-12-31');
    expect(tecnico.variaciones.ytd.pct).toBe(25);
  });

  it('la variación a 1 año acepta el primer cierre de la serie si cae en la holgura', () => {
    // range=1y empieza el día siguiente al corte cuando el corte no fue hábil.
    const serie = crearSerie({ desde: '2025-11-20', puntos: { '2025-11-20': { close: 160 } } });
    const tecnico = card.tecnicoDeLaSerie(serie, HOY);

    expect(tecnico.variaciones.y1.desde).toBe('2025-11-20');
  });
});

describe('crecimientoAnualPct — un solo cálculo para la ficha y el hallazgo', () => {
  it('compara el último año fiscal con el anterior, sin redondear', () => {
    expect(card.crecimientoAnualPct(crearAnual()).pct).toBeCloseTo(65.5, 10);
  });

  it('con un solo año no hay crecimiento', () => {
    expect(card.crecimientoAnualPct(crearAnual([crearTrimestre('2026-01-26', 1, 1)]))).toBeNull();
  });
});

describe('HU 5.7.3 — la ficha dice qué acciones permite (RN-5.7-T, RN-5.7-R)', () => {
  it('QA-12: una cripto solo se mantiene, o se reduce si está en sobrepeso', () => {
    const eth = card.construirFicha({
      holding: crearHolding({ symbol: 'ETH-USD', asset_type: 'crypto', weight: 0.026 }),
      serie: crearSerie({ cierre: 3000 }),
      language: 'es',
      now: HOY,
    });
    const btc = card.construirFicha({
      holding: crearHolding({ symbol: 'BTC-USD', asset_type: 'crypto', weight: 0.109 }),
      serie: crearSerie({ cierre: 61914 }),
      language: 'es',
      now: HOY,
    });

    expect(eth.signals.allowedActions).toEqual(['mantener']);
    expect(btc.signals.allowedActions).toEqual(['mantener', 'reducir']);
  });

  it('un ETF se puede aumentar: no tiene umbral', () => {
    const ficha = card.construirFicha({
      holding: crearHolding({ symbol: 'VUAA.L', asset_type: 'etf', weight: 0.282 }),
      serie: crearSerie(),
      language: 'es',
      now: HOY,
    });

    expect(ficha.signals.allowedActions).toEqual(['mantener', 'aumentar', 'comprar']);
  });

  it('una acción con una sola señal de deterioro permite reducir, no vender, y limita mantener', () => {
    const ficha = fichaNvda({
      // Ingresos anuales a la baja: una sola señal del Nivel 2.
      anual: crearAnual([
        crearTrimestre('2026-01-25', 100e9, 60e9),
        crearTrimestre('2025-01-26', 110e9, 66e9),
      ]),
    });

    expect(ficha.signals.deterioration).toHaveLength(1);
    expect(ficha.signals.allowedActions).toContain('reducir');
    expect(ficha.signals.allowedActions).not.toContain('vender');
    expect(ficha.signals.holdMaxConviction).toBe('baja');
  });

  it('expone el último cierre como precio, sin redondear (RN-5.7-L)', () => {
    expect(fichaNvda().price).toBe(145.98);
    expect(card.construirFicha({ holding: crearHolding(), language: 'es', now: HOY }).price).toBeNull();
  });
});
