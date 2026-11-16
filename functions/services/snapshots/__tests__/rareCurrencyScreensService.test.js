/**
 * HU 6.11.3 — Detalle de activo, comparación y riesgo en monedas poco usadas desde USD.
 *
 * - La conversión de los días en USD: importes a la tasa del día y cambio diario
 *   con la fórmula de la 6.9.2 (Escenario 1, QA-01).
 * - Un festivo toma el cierre hábil anterior hasta 5 días naturales; 6 días, una
 *   tasa inválida o el servicio caído dejan la cifra sin calcular (Escenario 3;
 *   QA-06, QA-07, QA-09).
 * - Un día sin tasa anterior a la compra no deja sin calcular el detalle del
 *   activo, pero sí lo que usa toda la historia (QA-08).
 * - El detalle de activo desde USD lee solo los campos en USD, no escribe nada y
 *   da lo mismo que el cálculo desde cero con los días completos (Escenario 2,
 *   RN-6-A); vendido y vuelto a comprar se ve igual que en USD (Escenario 5, QA-05).
 * - USD, la moneda principal y el interruptor apagado no se calculan desde USD
 *   (Escenarios 6 y 7, QA-11, QA-18).
 *
 * @see platform-docs/stories/6.11.3-activo-comparacion-riesgo-usd/refinamiento.md (T8a)
 */

const { DateTime, Settings } = require('luxon');

const mockGetExchangeRates = jest.fn();
jest.mock('../../financeQuery', () => ({
  getExchangeRates: (...args) => mockGetExchangeRates(...args),
}));

const { armarResumenDeActivo, buildAssetDailyTimeline } = require('../../snapshotGenerator');
const { limpiarCachesDerivados } = require('../derivedCurrencySnapshotService');
const {
  pantallasDesdeUsd,
  calculaDesdeUsd,
  convertirDiasDesdeUsd,
  diasEnMonedaDesdeUsd,
  resumenDeActivoDesdeUsd,
} = require('../rareCurrencyScreensService');

const AHORA = DateTime.fromISO('2026-01-14T18:00:00', { zone: 'America/New_York' });
const NOW_ORIGINAL = Settings.now;
const UID = 'uid-inversor-A';
const CLAVE = 'MSFT_stock';

// ============================================================================
// FÁBRICAS
// ============================================================================

const activo = (overrides = {}) => ({
  totalValue: 1000,
  totalInvestment: 1000,
  totalCashFlow: 0,
  adjustedDailyChangePercentage: 0,
  dailyChangePercentage: 0,
  units: 10,
  totalROI: 0,
  ...overrides,
});

/** Un día en USD; sin `msft`, el activo no está ese día (antes de comprarlo o vendido). */
const dia = (date, msft, totalValue = 5000) => ({
  date,
  USD: {
    totalValue,
    totalInvestment: 5000,
    ...(msft ? { assetPerformance: { [CLAVE]: msft } } : {}),
  },
});

/**
 * ACT-MSFT de `qa.md`, en pequeño: no lo tiene el 2026-01-02, lo compra el 05,
 * lo vende entero el 07, lo recompra el 09; el 12 es festivo en Colombia.
 */
const DIAS = [
  dia('2026-01-02', null),
  dia('2026-01-05', activo({ totalCashFlow: -1000 })),
  dia('2026-01-06', activo({ totalValue: 1010, adjustedDailyChangePercentage: 1, dailyChangePercentage: 1, totalROI: 1 })),
  dia('2026-01-07', null),
  dia('2026-01-08', null),
  dia('2026-01-09', activo({ totalValue: 505, totalInvestment: 505, totalCashFlow: -505, units: 5 })),
  dia('2026-01-12', activo({ totalValue: 515.1, totalInvestment: 505, adjustedDailyChangePercentage: 2, dailyChangePercentage: 2, units: 5 })),
];

/** Cierres USD→COP: no hay del 2026-01-02 (antes de la compra) ni del 12 (festivo). */
const CIERRES = { '2026-01-05': 4000, '2026-01-06': 4010, '2026-01-07': 4005, '2026-01-08': 4015, '2026-01-09': 4020 };

const convertido = (c, f, fPrevia) => ((1 + c / 100) * f / fPrevia - 1) * 100;

/** Firestore en memoria: los días del activo (con la proyección anotada) y `userData`. */
function crearDb({ dias = DIAS, defaultCurrency = 'USD' } = {}) {
  const proyecciones = [];
  const escrituras = [];
  const consulta = {
    where: () => consulta,
    select: (...campos) => { proyecciones.push(campos.map(String)); return consulta; },
    orderBy: () => consulta,
    get: async () => ({ docs: dias.map((d) => ({ id: d.date, data: () => d })), size: dias.length }),
  };
  return {
    proyecciones,
    escrituras,
    collection: (nombre) => {
      if (nombre === 'userData') {
        return { doc: () => ({ get: async () => ({ exists: true, data: () => ({ defaultCurrency }) }) }) };
      }
      if (nombre === 'performanceSnapshots') {
        return { doc: (id) => ({ set: async () => escrituras.push(id) }) };
      }
      return consulta;
    },
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  limpiarCachesDerivados();
  delete process.env.RARE_CURRENCY_SCREENS_FROM_USD;
  delete process.env.CURRENCY_UNAVAILABLE_NOTICE;
  Settings.now = () => AHORA.toMillis();
  mockGetExchangeRates.mockResolvedValue({ rates: { COP: CIERRES } });
});

afterAll(() => {
  Settings.now = NOW_ORIGINAL;
});

// ============================================================================
// CONVERSIÓN
// ============================================================================

describe('HU 6.11.3: convertirDiasDesdeUsd — USD × tasa de cierre del día', () => {
  it('lleva los importes a la tasa del día y convierte el cambio diario (Escenario 1, QA-01)', () => {
    const { dias, motivo } = convertirDiasDesdeUsd(DIAS, 'COP', CIERRES, { claveActivo: CLAVE });

    expect(motivo).toBeUndefined();
    const d06 = dias[2].COP.assetPerformance[CLAVE];
    expect(d06.totalValue).toBe(1010 * 4010);
    expect(d06.totalInvestment).toBe(1000 * 4010);
    expect(d06.adjustedDailyChangePercentage).toBe(convertido(1, 4010, 4000));
    expect(d06.units).toBe(10);
    expect(d06.totalROI).toBe(1);
    // La inversión del día de compra es la de USD por la tasa del día, no lo pagado.
    expect(dias[1].COP.assetPerformance[CLAVE].totalCashFlow).toBe(-1000 * 4000);
  });

  it('un festivo toma el cierre hábil anterior (FES-COP, QA-06)', () => {
    const { dias } = convertirDiasDesdeUsd(DIAS, 'COP', CIERRES, { claveActivo: CLAVE });

    expect(dias[6].COP.assetPerformance[CLAVE].totalValue).toBe(515.1 * 4020);
    expect(dias[6].COP.assetPerformance[CLAVE].adjustedDailyChangePercentage).toBe(convertido(2, 4020, 4020));
  });

  it('sin el activo el día queda con la marca de presencia y sin importes (Escenario 5)', () => {
    const { dias } = convertirDiasDesdeUsd(DIAS, 'COP', CIERRES, { claveActivo: CLAVE });

    expect(dias[3]).toEqual({ date: '2026-01-07', COP: {} });
    // La recompra no se convierte contra un día sin el activo: conserva su cambio en USD.
    expect(dias[5].COP.assetPerformance[CLAVE].adjustedDailyChangePercentage).toBe(0);
  });

  it('un día sin tasa anterior a la compra no deja sin calcular el activo, pero sí toda la historia (QA-08)', () => {
    expect(convertirDiasDesdeUsd(DIAS, 'COP', CIERRES, { claveActivo: CLAVE }).motivo).toBeUndefined();
    expect(convertirDiasDesdeUsd(DIAS, 'COP', CIERRES).motivo).toBe('sin-tasa-2026-01-02');
  });

  it('5 días naturales sin cierre se cubren; 6 dejan la cifra sin calcular (LIM-5, LIM-6, QA-06, QA-07)', () => {
    const dias = [dia('2026-08-10', activo())];

    expect(convertirDiasDesdeUsd(dias, 'COP', { '2026-08-05': 3950 }, { claveActivo: CLAVE }).dias[0].COP.assetPerformance[CLAVE].totalValue)
      .toBe(1000 * 3950);
    expect(convertirDiasDesdeUsd(dias, 'COP', { '2026-08-04': 3950 }, { claveActivo: CLAVE }).motivo).toBe('sin-tasa-2026-08-10');
  });

  it.each([[0], [-3890.2], [null]])('HU 6.14.2: una tasa inválida (%p) es un día sin cierre: toma el del día hábil anterior (RN-6.14-D)', (tasa) => {
    const cierres = { ...CIERRES, '2026-01-06': tasa };

    const { dias, motivo } = convertirDiasDesdeUsd(DIAS, 'COP', cierres, { claveActivo: CLAVE });

    expect(motivo).toBeUndefined();
    expect(dias.find((d) => d.date === '2026-01-06').COP.assetPerformance[CLAVE].totalValue).toBe(1010 * 4000);
  });

  it('sin respuesta del servicio de tasas no hay con qué calcular (QA-09)', async () => {
    mockGetExchangeRates.mockResolvedValue(null);

    await expect(diasEnMonedaDesdeUsd(DIAS, 'COP', { claveActivo: CLAVE })).resolves.toEqual({ motivo: 'sin-tasas' });
  });
});

// ============================================================================
// DETALLE DE ACTIVO
// ============================================================================

describe('HU 6.11.3: resumenDeActivoDesdeUsd', () => {
  const params = { userId: UID, accountId: 'overall', currency: 'COP', ticker: 'MSFT', assetType: 'stock' };

  it('lee solo los campos en USD, no escribe y da lo mismo que desde cero (Escenario 2, RN-6-A, QA-01, QA-15)', async () => {
    const db = crearDb();

    const { snapshot, metodo } = await resumenDeActivoDesdeUsd(db, params);
    const desdeCero = armarResumenDeActivo(
      convertirDiasDesdeUsd(DIAS, 'COP', CIERRES, { claveActivo: CLAVE }).dias,
      { userId: UID, accountId: 'overall', currency: 'COP', ticker: 'MSFT', assetType: 'stock' },
    );

    expect(metodo).toBe('derivado');
    const { lastUpdated: _a, derivedFrom, ...servido } = snapshot;
    const { lastUpdated: _b, ...esperado } = desdeCero;
    expect(servido).toEqual(esperado);
    expect(derivedFrom).toBe('USD');
    expect(db.proyecciones.flat().every((campo) => !campo.startsWith('COP'))).toBe(true);
    expect(db.proyecciones.flat().some((campo) => campo.startsWith('USD'))).toBe(true);
    expect(db.escrituras).toEqual([]);
  });

  it('vendido y vuelto a comprar: los mismos días sin posición que en USD (Escenario 5, QA-05)', async () => {
    const { snapshot } = await resumenDeActivoDesdeUsd(crearDb(), params);
    const enUsd = buildAssetDailyTimeline(DIAS, 'USD', 'MSFT', 'stock');

    expect(snapshot.timeline.map((p) => [p.d, p.u])).toEqual(enUsd.map((p) => [p.d, p.u]));
    expect(snapshot.timeline.find((p) => p.d === '2026-01-07')).toEqual({ d: '2026-01-07', v: 0, c: 0, u: 0 });
  });

  it('sin con qué calcular responde no-derivable con el motivo (Escenario 3)', async () => {
    // HU 6.14.2: un cierre inválido ya no basta (es un día sin cierre); sin ningún cierre en 5 días, sí
    mockGetExchangeRates.mockResolvedValue({ rates: { COP: { '2026-01-02': 0 } } });

    await expect(resumenDeActivoDesdeUsd(crearDb(), params))
      .resolves.toEqual({ snapshot: null, metodo: 'no-derivable', motivo: 'sin-tasa-2026-01-05' });
  });

  it('un activo sin días en el alcance responde sin-datos', async () => {
    await expect(resumenDeActivoDesdeUsd(crearDb({ dias: [dia('2026-01-02', null)] }), params))
      .resolves.toEqual({ snapshot: null, metodo: 'sin-datos' });
  });
});

// ============================================================================
// ALCANCE E INTERRUPTOR
// ============================================================================

describe('HU 6.11.3: calculaDesdeUsd — solo en monedas poco usadas (RN-6.11-N, RN-6-H)', () => {
  it('USD y la moneda principal siguen su camino de siempre (Escenario 6, QA-11)', async () => {
    await expect(calculaDesdeUsd(crearDb(), UID, 'USD')).resolves.toBe(false);
    await expect(calculaDesdeUsd(crearDb({ defaultCurrency: 'COP' }), 'uid-inversor-P', 'COP')).resolves.toBe(false);
  });

  it('una moneda poco usada se calcula desde USD (RN-6.9.2-J)', async () => {
    await expect(calculaDesdeUsd(crearDb(), UID, 'COP')).resolves.toBe(true);
    await expect(calculaDesdeUsd(crearDb({ defaultCurrency: 'COP' }), 'uid-inversor-P', 'EUR')).resolves.toBe(true);
  });

  it('con RARE_CURRENCY_SCREENS_FROM_USD=false vuelve el comportamiento anterior (Escenario 7, QA-18)', async () => {
    process.env.RARE_CURRENCY_SCREENS_FROM_USD = 'false';

    expect(pantallasDesdeUsd()).toBe(false);
    await expect(calculaDesdeUsd(crearDb(), UID, 'COP')).resolves.toBe(false);
  });

  it('con la derivación de la 6.9.2 apagada (DERIVED_CURRENCY_SNAPSHOTS=false) tampoco', async () => {
    process.env.DERIVED_CURRENCY_SNAPSHOTS = 'false';
    try {
      await expect(calculaDesdeUsd(crearDb(), UID, 'COP')).resolves.toBe(false);
    } finally {
      delete process.env.DERIVED_CURRENCY_SNAPSHOTS;
    }
  });
});

// ============================================================================
// HU 6.11.6 — DESDE LOS DÍAS GUARDADOS EN EL RESUMEN EN USD
// ============================================================================

describe('HU 6.11.6: resumenDeActivoDesdeUsd con los días del resumen en USD (T8b)', () => {
  const { extraerDiasDelActivo, docsDesdeDiasDelActivo } = require('../../../utils/assetDays');
  const params = { userId: UID, accountId: 'overall', currency: 'COP', ticker: 'MSFT', assetType: 'stock' };
  const delResumen = (dias = DIAS) => docsDesdeDiasDelActivo(extraerDiasDelActivo(dias, 'USD', 'MSFT', 'stock'), CLAVE);

  it('no lee días ni escribe, y da lo mismo que desde los días (RN-6.11.6-H, QA-01)', async () => {
    const db = crearDb();

    const nuevo = await resumenDeActivoDesdeUsd(db, params, { dias: delResumen() });
    const hoy = await resumenDeActivoDesdeUsd(crearDb(), params);

    expect(db.proyecciones).toEqual([]);
    expect(db.escrituras).toEqual([]);
    const { lastUpdated: _a, ...a } = nuevo.snapshot;
    const { lastUpdated: _b, ...b } = hoy.snapshot;
    expect(a).toEqual(b);
  });

  it('5 días naturales sin cierre se cubren y 6 dan el aviso, igual que hoy (LIM-5, LIM-6, QA-04)', async () => {
    const dias = [dia('2026-08-10', activo())];

    mockGetExchangeRates.mockResolvedValue({ rates: { COP: { '2026-08-05': 3950 } } });
    const cinco = await resumenDeActivoDesdeUsd(crearDb(), { ...params, accountId: 'acc-5' }, { dias: delResumen(dias) });
    limpiarCachesDerivados();
    mockGetExchangeRates.mockResolvedValue({ rates: { COP: { '2026-08-04': 3950 } } });
    const seis = await resumenDeActivoDesdeUsd(crearDb(), { ...params, accountId: 'acc-6' }, { dias: delResumen(dias) });

    expect(cinco.metodo).not.toBe('no-derivable');
    expect(seis).toEqual({ snapshot: null, metodo: 'no-derivable', motivo: 'sin-tasa-2026-08-10' });
  });

  it('HU 6.14.2: una tasa inválida da lo mismo que un día sin cierre, también desde el resumen (RN-6.14-D)', async () => {
    const { [`2026-01-09`]: _sinEl9, ...festivo } = CIERRES;
    mockGetExchangeRates.mockResolvedValue({ rates: { COP: festivo } });
    const comoFestivo = await resumenDeActivoDesdeUsd(crearDb(), params, { dias: delResumen() });

    limpiarCachesDerivados();
    mockGetExchangeRates.mockResolvedValue({ rates: { COP: { ...CIERRES, '2026-01-09': 0 } } });
    const conInvalida = await resumenDeActivoDesdeUsd(crearDb(), params, { dias: delResumen() });

    expect(conInvalida.metodo).not.toBe('no-derivable');
    expect(conInvalida.snapshot.timeline).toEqual(comoFestivo.snapshot.timeline);
  });

  it('el interruptor RARE_ASSET_FROM_USD_SUMMARY está encendido salvo con "false" (RN-6-H, Escenario 9)', () => {
    const { activoDesdeResumenUsd } = require('../rareCurrencyScreensService');
    try {
      delete process.env.RARE_ASSET_FROM_USD_SUMMARY;
      expect(activoDesdeResumenUsd()).toBe(true);
      process.env.RARE_ASSET_FROM_USD_SUMMARY = 'false';
      expect(activoDesdeResumenUsd()).toBe(false);
    } finally {
      delete process.env.RARE_ASSET_FROM_USD_SUMMARY;
    }
  });
});
