/**
 * HU 6.11.4 (T9a) — El módulo de la atribución desde USD.
 *
 * - Tasas de cierre: cierre hábil anterior hasta 5 días naturales; el 6.º da el
 *   aviso (QA-07, QA-08); una tasa inválida y el servicio caído también (QA-09).
 * - Respaldo y varias cuentas con fechas: los días en USD con el cambio
 *   convertido, sin leer el bloque guardado en la moneda (QA-02, QA-04).
 * - Día en curso: el valor de hoy con la tasa vigente y el del día anterior con
 *   la de su cierre (QA-11).
 * - Alcance: USD y la moneda principal no se calculan desde USD; el interruptor
 *   devuelve el comportamiento de antes (QA-12, QA-13).
 *
 * @see platform-docs/stories/6.11.4-atribucion-desde-usd/refinamiento.md (T9a)
 */

const mockGetExchangeRates = jest.fn();
jest.mock('../../financeQuery', () => ({
  getExchangeRates: (...args) => mockGetExchangeRates(...args),
}));

const {
  atribucionDesdeUsd,
  monedaPocoUsadaDeAtribucion,
  esAvisoMonedaNoDisponible,
  conversorDeCierres,
  tasaVigente,
  convertirDiaDeTenencias,
  convertirVentas,
  fechasDeVentas,
  twrDeRangoDesdeUsd,
  intradiaDesdeUsd,
} = require('../attributionFromUsdService');
const { limpiarCachesDerivados } = require('../../snapshots/derivedCurrencySnapshotService');

// ============================================================================
// FÁBRICAS
// ============================================================================

/** Cierres COP del ejemplo de qa.md: el 2026-11-02 es festivo en Colombia. */
const CIERRES_COP = {
  '2026-10-29': 3920.0,
  '2026-10-30': 3912.4,
  '2026-11-03': 3905.1,
  '2026-11-04': 3899.8,
};

const tasasFijas = (cierres) => ({ tasasDelPeriodo: async () => cierres });

/**
 * Firestore en memoria para los días: entiende where / orderBy / limit / select
 * y registra cada vez que alguien usa el bloque de una moneda poco usada.
 */
function crearDb({ dias = {}, usuarios = {} } = {}) {
  const usosMoneda = [];
  const vigilar = (data) => new Proxy(data, {
    get(obj, clave) {
      if (clave === 'COP' || clave === 'EUR') usosMoneda.push(clave);
      return obj[clave];
    },
  });
  const consulta = (ruta, filtros = [], orden = null, tope = null, campos = null) => ({
    where: (f, op, v) => consulta(ruta, [...filtros, [f, op, v]], orden, tope, campos),
    orderBy: (f, dir = 'asc') => consulta(ruta, filtros, [f, dir], tope, campos),
    limit: (n) => consulta(ruta, filtros, orden, n, campos),
    select: (...c) => consulta(ruta, filtros, orden, tope, c),
    doc: (id) => ({
      get: async () => ({ exists: !!usuarios[id], data: () => usuarios[id] }),
    }),
    get: async () => {
      const cumple = (d) => filtros.every(([f, op, v]) => (
        op === '>=' ? d[f] >= v : op === '<=' ? d[f] <= v : op === '<' ? d[f] < v : d[f] === v));
      let docs = (dias[ruta] || []).filter(cumple);
      if (orden) docs = docs.slice().sort((a, b) => (a.date < b.date ? -1 : 1) * (orden[1] === 'desc' ? -1 : 1));
      if (tope !== null) docs = docs.slice(0, tope);
      if (campos) expect(campos.some((c) => c.startsWith('COP') || c.startsWith('EUR'))).toBe(false);
      return { empty: docs.length === 0, size: docs.length, docs: docs.map((d) => ({ id: d.date, data: () => vigilar(d) })) };
    },
  });
  return { usosMoneda, collection: (ruta) => consulta(ruta) };
}

/** Un día con su bloque en USD y un bloque COP guardado que NO debe usarse. */
const dia = (date, totalValue, cambio) => ({
  date,
  USD: { totalValue, adjustedDailyChangePercentage: cambio },
  COP: { totalValue: 1, adjustedDailyChangePercentage: 999 },
});

beforeEach(() => {
  delete process.env.ATTRIBUTION_FROM_USD;
  limpiarCachesDerivados();
  mockGetExchangeRates.mockReset();
  jest.spyOn(console, 'log').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

// ============================================================================
// ALCANCE E INTERRUPTOR
// ============================================================================

describe('HU 6.11.4 — qué atribuciones se calculan desde USD (RN-6.9.2-J, RN-6.11-N, RN-6-H)', () => {
  const db = crearDb({ usuarios: { 'uid-inversor-M': { defaultCurrency: 'USD' }, 'uid-inversor-P': { defaultCurrency: 'COP' } } });

  it('COP y EUR son poco usadas para INV-M; para INV-P solo EUR (su principal es COP)', async () => {
    expect(await monedaPocoUsadaDeAtribucion(db, 'uid-inversor-M', 'COP')).toBe(true);
    expect(await monedaPocoUsadaDeAtribucion(db, 'uid-inversor-M', 'EUR')).toBe(true);
    expect(await monedaPocoUsadaDeAtribucion(db, 'uid-inversor-P', 'EUR')).toBe(true);
    expect(await monedaPocoUsadaDeAtribucion(db, 'uid-inversor-P', 'COP')).toBe(false);
    expect(await monedaPocoUsadaDeAtribucion(db, 'uid-inversor-M', 'USD')).toBe(false);
  });

  it('con ATTRIBUTION_FROM_USD=false ninguna se calcula desde USD (QA-13)', async () => {
    process.env.ATTRIBUTION_FROM_USD = 'false';
    expect(atribucionDesdeUsd()).toBe(false);
    expect(await monedaPocoUsadaDeAtribucion(db, 'uid-inversor-M', 'COP')).toBe(false);
  });
});

// ============================================================================
// TASAS DE CIERRE
// ============================================================================

describe('HU 6.11.4 — conversorDeCierres: la tasa de cierre de cada día (RN-6.11-L, RN-6.11-K)', () => {
  it('QA-07: el festivo 2026-11-02 toma el cierre del 2026-10-30 (3.912,40)', async () => {
    const { tasaDe, serie } = await conversorDeCierres('COP', ['2026-11-03', '2026-11-02', '2026-10-30'], tasasFijas(CIERRES_COP));
    expect(tasaDe('2026-11-02')).toBe(3912.4);
    expect(tasaDe('2026-11-03')).toBe(3905.1);
    expect(serie).toEqual({ dates: ['2026-10-30', '2026-11-02', '2026-11-03'], closes: [3912.4, 3912.4, 3905.1] });
  });

  it('QA-08: a 5 días naturales del último cierre hay tasa; a 6 días, el aviso', async () => {
    const cierres = { '2026-09-11': 4010.8, '2026-09-21': 4002.15 };
    const cinco = await conversorDeCierres('COP', ['2026-09-16'], tasasFijas(cierres));
    expect(cinco.tasaDe('2026-09-16')).toBe(4010.8);

    const error = await conversorDeCierres('COP', ['2026-09-14', '2026-09-17'], tasasFijas(cierres)).catch((e) => e);
    expect(esAvisoMonedaNoDisponible(error)).toBe(true);
    expect(error.details).toEqual({ code: 'CURRENCY_UNAVAILABLE', currency: 'COP', motivo: 'sin-tasa-2026-09-17' });
  });

  it('HU 6.14.2: una tasa cero en un día del periodo es un día sin cierre: toma el cierre anterior (RN-6.14-D)', async () => {
    const { tasaDe } = await conversorDeCierres('COP', ['2026-11-03'], tasasFijas({ ...CIERRES_COP, '2026-11-03': 0 }));
    // No hay cierre el 2 (festivo en Colombia): el hábil anterior es el del 30 de octubre
    expect(tasaDe('2026-11-03')).toBe(CIERRES_COP['2026-10-30']);
  });

  it('QA-09: el servicio de tasas sin responder da el aviso', async () => {
    const error = await conversorDeCierres('EUR', ['2026-11-03'], tasasFijas(null)).catch((e) => e);
    expect(error.details).toEqual({ code: 'CURRENCY_UNAVAILABLE', currency: 'EUR', motivo: 'sin-tasas' });
  });
});

// ============================================================================
// CONVERSIONES PURAS
// ============================================================================

describe('HU 6.11.4 — conversiones de un día y de las ventas (decisión del PO del 2026-11-14)', () => {
  it('un día de tenencias: valores × tasa, unidades iguales; sin bloque por activo sigue sin él', () => {
    const convertido = convertirDiaDeTenencias({
      date: '2026-11-03',
      currencyData: { totalValue: 52610, assetPerformance: { AAPL_stock: { totalValue: 22850, units: 100 } } },
    }, 3905.1);
    expect(convertido).toEqual({
      date: '2026-11-03',
      currencyData: { totalValue: 52610 * 3905.1, assetPerformance: { AAPL_stock: { totalValue: 22850 * 3905.1, units: 100 } } },
    });
    expect(convertirDiaDeTenencias({ date: 'd', currencyData: { totalValue: 10 } }, 2).currencyData.assetPerformance).toBeUndefined();
  });

  it('las ventas: P&G en USD × la tasa del día de cada venta', () => {
    const ventas = {
      ECOPETROL_stock: {
        ticker: 'ECOPETROL',
        totalRealizedPnL: 15,
        transactions: [{ date: '2026-10-30T15:20:41.053Z', pnl: 10 }, { date: '2026-11-03', pnl: 5 }],
      },
    };
    const tasas = { '2026-10-30': 3912.4, '2026-11-03': 3905.1 };
    expect(fechasDeVentas(ventas)).toEqual(['2026-10-30', '2026-11-03']);
    const cop = convertirVentas(ventas, (f) => tasas[f]);
    expect(cop.ECOPETROL_stock.totalRealizedPnL).toBe(10 * 3912.4 + 5 * 3905.1);
    expect(cop.ECOPETROL_stock.transactions.map((t) => t.pnl)).toEqual([10 * 3912.4, 5 * 3905.1]);
  });
});

// ============================================================================
// RESPALDO Y VARIAS CUENTAS CON FECHAS
// ============================================================================

describe('HU 6.11.4 — twrDeRangoDesdeUsd: respaldo por periodo y varias cuentas con fechas (QA-02, QA-04)', () => {
  const RUTA = 'portfolioPerformance/uid-inversor-M/accounts/CTA-US/dates';

  it('compone el cambio convertido (1 + cUSD)·f(d)/f(d−1) − 1 desde el día anterior al rango, sin leer el bloque COP', async () => {
    const db = crearDb({ dias: { [RUTA]: [dia('2026-10-30', 52000, 0.5), dia('2026-11-02', 52430, 0.827), dia('2026-11-03', 52610, 0.343)] } });

    const r = await twrDeRangoDesdeUsd(db, {
      userId: 'uid-inversor-M', accountId: 'CTA-US', currency: 'COP', startStr: '2026-11-02', endStr: '2026-11-03',
    }, tasasFijas(CIERRES_COP));

    // 2026-11-02 festivo: tasa del 30 → factor de tasa 1; 2026-11-03: 3905,1 / 3912,4
    const c1 = ((1 + 0.827 / 100) * 3912.4 / 3912.4 - 1) * 100;
    const c2 = ((1 + 0.343 / 100) * 3905.1 / 3912.4 - 1) * 100;
    expect(r.twr).toBe(((1 + c1 / 100) * (1 + c2 / 100) - 1) * 100);
    expect(r.value).toBe(52610 * 3905.1);
    expect(r.docsCount).toBe(2);
    expect(r.hasData).toBe(true);
    expect(db.usosMoneda).toEqual([]);
  });

  it('F-UN-DIA: un solo día, convertido contra el cierre del día anterior', async () => {
    const db = crearDb({ dias: { [RUTA]: [dia('2026-11-02', 52430, 0.827), dia('2026-11-03', 52610, 0.343)] } });
    const r = await twrDeRangoDesdeUsd(db, {
      userId: 'uid-inversor-M', accountId: 'CTA-US', currency: 'COP', startStr: '2026-11-03', endStr: '2026-11-03',
    }, tasasFijas(CIERRES_COP));
    expect(r.twr).toBe(((1 + 0.343 / 100) * 3905.1 / 3912.4 - 1) * 100);
  });

  it('sin días en el rango no hay datos y no se piden tasas', async () => {
    const pedir = jest.fn();
    const r = await twrDeRangoDesdeUsd(crearDb(), {
      userId: 'u', accountId: 'overall', currency: 'COP', startStr: '2026-11-02', endStr: null,
    }, { tasasDelPeriodo: pedir });
    expect(r).toEqual({ twr: 0, hasData: false, docsCount: 0, value: 0 });
    expect(pedir).not.toHaveBeenCalled();
  });

  it('un día sin cierre en 5 días naturales da el aviso (QA-08)', async () => {
    const db = crearDb({ dias: { 'portfolioPerformance/u/dates': [dia('2026-09-17', 100, 1)] } });
    const error = await twrDeRangoDesdeUsd(db, {
      userId: 'u', accountId: 'overall', currency: 'COP', startStr: '2026-09-14', endStr: '2026-09-17',
    }, tasasFijas({ '2026-09-11': 4010.8 })).catch((e) => e);
    expect(error.details.motivo).toBe('sin-tasa-2026-09-17');
  });
});

// ============================================================================
// DÍA EN CURSO
// ============================================================================

describe('HU 6.11.4 — intradiaDesdeUsd: el día en curso desde USD y la tasa vigente (Escenario 4, QA-11)', () => {
  const INTRADIA_USD = {
    success: true,
    date: '2026-11-13',
    totalValue: 119500,
    totalInvestment: 100000,
    previousDayTotalValue: 118940,
    previousDayDate: '2026-11-12',
    todayFactor: 119500 / 118940,
    dailyChangePercent: 0,
    currency: 'USD',
  };

  it('valor de hoy × vigente (3.874,60) y del 2026-11-12 × su cierre (3.880,25)', async () => {
    const calcular = jest.fn(async () => INTRADIA_USD);
    const r = await intradiaDesdeUsd({ userId: 'uid-inversor-M', currency: 'COP', accountIds: ['overall'] }, {
      calculateIntradayPerformance: calcular,
      getExchangeRates: async () => ({ rates: { COP: 3874.6 } }),
      tasasDelPeriodo: async () => ({ '2026-11-12': 3880.25 }),
    });

    expect(calcular).toHaveBeenCalledWith({ userId: 'uid-inversor-M', currency: 'USD', accountIds: ['overall'] });
    const hoy = 119500 * 3874.6;
    const ayer = 118940 * 3880.25;
    expect(r.totalValue).toBe(hoy);
    expect(r.previousDayTotalValue).toBe(ayer);
    expect(r.totalInvestment).toBe(100000 * 3874.6);
    expect(r.todayFactor).toBe(1 + ((hoy - ayer) / ayer * 100) / 100);
    expect(r.currency).toBe('COP');
    expect(r.derivedFrom).toBe('USD');
  });

  it('QA-09: sin tasa vigente (servicio caído), el aviso', async () => {
    const error = await intradiaDesdeUsd({ userId: 'u', currency: 'COP', accountIds: ['overall'] }, {
      calculateIntradayPerformance: async () => INTRADIA_USD,
      getExchangeRates: async () => null,
    }).catch((e) => e);
    expect(error.details.motivo).toBe('sin-tasas');
  });

  it('una tasa vigente inválida o ausente también da el aviso', async () => {
    await expect(tasaVigente('EUR', { getExchangeRates: async () => ({ rates: { EUR: '0' } }) }))
      .rejects.toMatchObject({ details: { motivo: 'tasa-vigente-invalida' } });
    await expect(tasaVigente('EUR', { getExchangeRates: async () => ({ rates: {} }) }))
      .rejects.toMatchObject({ details: { motivo: 'sin-tasa-vigente' } });
    expect(await tasaVigente('EUR', { getExchangeRates: async () => ({ rates: { EUR: '0.8598' } }) })).toBe(0.8598);
  });

  it('si el intradía en USD no se puede calcular (sin precios), se devuelve tal cual, sin pedir tasas', async () => {
    const pedirTasas = jest.fn();
    const sinPrecios = { success: false, error: 'No prices obtained for any asset', todayFactor: 1 };
    const r = await intradiaDesdeUsd({ userId: 'u', currency: 'COP', accountIds: ['overall'] }, {
      calculateIntradayPerformance: async () => sinPrecios,
      getExchangeRates: pedirTasas,
    });
    expect(r).toBe(sinPrecios);
    expect(pedirTasas).not.toHaveBeenCalled();
  });
});
