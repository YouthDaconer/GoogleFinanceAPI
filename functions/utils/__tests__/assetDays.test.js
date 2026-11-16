/**
 * HU 6.11.6 — Los días de un activo guardados en su resumen en USD.
 *
 * - Identidad: el detalle en una moneda poco usada calculado desde los días
 *   guardados es **el mismo** que el de hoy (6.11.3) calculado desde los
 *   documentos diarios, en todos los decimales, con venta y recompra, festivos,
 *   un día sin USD, flujos y P&G realizado (Escenario 2, RN-6.11.6-H; QA-02, QA-11).
 * - La puesta al día añade los días que el cierre deja y da lo mismo que
 *   prepararlo desde cero (Escenario 4, RN-6.4-K; QA-08).
 *
 * Se usan las funciones reales de la 6.11.3 (`convertirDiasDesdeUsd`,
 * `armarResumenDeActivo`) y de la 6.4 (`aplicarDiaAResumenDeActivo`).
 *
 * @see platform-docs/stories/6.11.6-detalle-activo-desde-resumen-usd/refinamiento.md (T8a)
 */

const { DateTime, Settings } = require('luxon');

jest.mock('../../services/financeQuery', () => ({ getExchangeRates: jest.fn() }));

const {
  registroDelActivo,
  registroDelDia,
  agregarDiaAlActivo,
  extraerDiasDelActivo,
  docsDesdeDiasDelActivo,
} = require('../assetDays');
const { armarResumenDeActivo } = require('../../services/snapshotGenerator');
const { aplicarDiaAResumenDeActivo } = require('../../services/snapshotIncrementalService');
const { convertirDiasDesdeUsd } = require('../../services/snapshots/rareCurrencyScreensService');

const AHORA = DateTime.fromISO('2026-11-13T18:00:00', { zone: 'America/New_York' });
const NOW_ORIGINAL = Settings.now;
const UID = 'uid-inversor-A';
const CLAVE = 'MSFT_stock';

// ============================================================================
// FÁBRICAS
// ============================================================================

const activo = (overrides = {}) => ({
  totalValue: 8964,
  totalInvestment: 8000,
  totalCashFlow: 0,
  doneProfitAndLoss: 0,
  unrealizedProfitAndLoss: 964,
  unrealizedPnL: 964,
  adjustedDailyChangePercentage: 0.37,
  dailyChangePercentage: 0.41,
  rawDailyChangePercentage: 0.41,
  units: 20,
  totalROI: 12.05,
  ...overrides,
});

/** Un documento diario en USD, como lo devuelve `fetchAssetDailyDocs`. */
const dia = (date, msft) => ({
  date,
  USD: {
    totalValue: 50000,
    totalInvestment: 45000,
    ...(msft ? { assetPerformance: { [CLAVE]: msft } } : {}),
  },
});

/**
 * ACT-MSFT de `qa.md` en pequeño: lo compra, sube, lo vende entero (P&G
 * realizado), dos días sin él, un documento sin USD, lo recompra, un festivo en
 * Colombia y sigue hasta noviembre (con meses distintos para el P&G mensual).
 */
const DIAS = [
  dia('2025-12-30', null),
  dia('2026-01-05', activo({ totalCashFlow: -8000, adjustedDailyChangePercentage: 0, dailyChangePercentage: 0 })),
  dia('2026-01-06', activo({ totalValue: 9050.33, adjustedDailyChangePercentage: 0.9596, dailyChangePercentage: 0.9596 })),
  dia('2026-02-02', activo({ totalValue: 9100.1, adjustedDailyChangePercentage: -0.2, dailyChangePercentage: null })),
  dia('2026-02-03', null),
  dia('2026-02-04', null),
  { date: '2026-02-05' },
  dia('2026-06-12', activo({ totalValue: 7576.5, totalInvestment: 7000, units: 15, totalCashFlow: -7000, doneProfitAndLoss: 0 })),
  dia('2026-06-15', activo({ totalValue: 7651.2, totalInvestment: 7000, units: 15, adjustedDailyChangePercentage: 0.986 })),
  dia('2026-06-16', activo({ totalValue: 7400.07, totalInvestment: 6500, units: 14, doneProfitAndLoss: 93.15, totalCashFlow: 490.4 })),
  dia('2026-11-12', activo({ totalValue: 8120.4, totalInvestment: 6500, units: 14, adjustedDailyChangePercentage: 1.1 })),
  dia('2026-11-13', activo({ totalValue: 8170.11, totalInvestment: 6500, units: 14, adjustedDailyChangePercentage: 0.612, unrealizedProfitAndLoss: 1670.11 })),
];

/** Cierres USD→COP: sin el del 2026-06-15 (festivo en Colombia), toma el anterior. */
const CIERRES = {
  '2025-12-30': 4380, '2026-01-05': 4100, '2026-01-06': 4105.5, '2026-02-02': 4010, '2026-02-03': 4012,
  '2026-02-04': 4015, '2026-06-12': 3950, '2026-06-16': 3961.25, '2026-11-12': 3890.2, '2026-11-13': 3901,
};

const ALCANCE = { userId: UID, accountId: 'overall', currency: 'COP', ticker: 'MSFT', assetType: 'stock' };

/** El detalle en COP desde unos días en USD, con las funciones de la 6.11.3. */
function detalleEnCop(diasUsd) {
  const { dias, motivo } = convertirDiasDesdeUsd(diasUsd, 'COP', CIERRES, { claveActivo: CLAVE });
  if (motivo) return { motivo };
  const { lastUpdated: _, ...resumen } = armarResumenDeActivo(dias, ALCANCE);
  return resumen;
}

beforeEach(() => {
  Settings.now = () => AHORA.toMillis();
});

afterAll(() => {
  Settings.now = NOW_ORIGINAL;
});

// ============================================================================
// REGISTROS
// ============================================================================

describe('HU 6.11.6: el registro de un día del activo', () => {
  it('guarda los campos que lee el cálculo tal cual, sin redondear, y omite el flujo y el P&G realizado en 0', () => {
    expect(registroDelActivo('2026-06-16', activo({ totalValue: 7400.07, doneProfitAndLoss: 93.15, totalCashFlow: 490.4 })))
      .toEqual({ d: '2026-06-16', v: 7400.07, a: 0.37, x: 0.41, u: 20, i: 8000, n: 964, f: 490.4, p: 93.15 });
    expect(registroDelActivo('2026-01-06', activo())).toEqual({ d: '2026-01-06', v: 8964, a: 0.37, x: 0.41, u: 20, i: 8000, n: 964 });
  });

  it('un campo ausente o nulo no se guarda; un NaN se guarda tal cual (Firestore lo admite)', () => {
    const registro = registroDelActivo('2026-02-02', { totalValue: Number.NaN, dailyChangePercentage: null, units: 3 });

    expect(Object.keys(registro)).toEqual(['d', 'v', 'u']);
    expect(Number.isNaN(registro.v)).toBe(true);
  });

  it('marca los días sin el activo y sin el bloque de USD', () => {
    expect(registroDelActivo('2026-02-03', null)).toEqual({ d: '2026-02-03', e: 'sin-activo' });
    expect(registroDelDia('2026-02-03', { totalValue: 1 }, CLAVE)).toEqual({ d: '2026-02-03', e: 'sin-activo' });
    expect(registroDelDia('2026-02-05', undefined, CLAVE)).toEqual({ d: '2026-02-05', e: 'sin-moneda' });
  });

  it('agregarDiaAlActivo añade en orden e ignora un día ya registrado o anterior', () => {
    const dias = [{ d: '2026-11-12', v: 1 }];

    expect(agregarDiaAlActivo(dias, { d: '2026-11-13', v: 2 })).toEqual([{ d: '2026-11-12', v: 1 }, { d: '2026-11-13', v: 2 }]);
    expect(agregarDiaAlActivo(dias, { d: '2026-11-12', v: 9 })).toBe(dias);
    expect(agregarDiaAlActivo(dias, { d: '2026-11-11', v: 9 })).toBe(dias);
    expect(dias).toEqual([{ d: '2026-11-12', v: 1 }]);
  });
});

// ============================================================================
// IDENTIDAD
// ============================================================================

describe('HU 6.11.6: desde los días guardados = la cifra de hoy (RN-6.11.6-H)', () => {
  it('el detalle en COP es idéntico en todos los decimales, con venta, recompra, festivo y un día sin USD (QA-02, QA-11)', () => {
    const reconstruidos = docsDesdeDiasDelActivo(extraerDiasDelActivo(DIAS, 'USD', 'MSFT', 'stock'), CLAVE);

    const hoy = detalleEnCop(DIAS);
    const nuevo = detalleEnCop(reconstruidos);

    expect(hoy.motivo).toBeUndefined();
    expect(JSON.stringify(nuevo)).toBe(JSON.stringify(hoy));
    // La venta y la recompra se ven igual: una marca de venta y la serie sigue.
    expect(nuevo.timeline.find((p) => p.d === '2026-02-03')).toEqual({ d: '2026-02-03', v: 0, c: 0, u: 0 });
    expect(nuevo.monthlyCompound['2026']['6'].doneProfitAndLoss).toBe(93.15 * 3961.25);
  });

  it('sin con qué calcular, el mismo motivo (Escenario 3)', () => {
    const reconstruidos = docsDesdeDiasDelActivo(extraerDiasDelActivo(DIAS, 'USD', 'MSFT', 'stock'), CLAVE);
    const sinTasa = { ...CIERRES, '2026-01-06': 0 };

    expect(convertirDiasDesdeUsd(reconstruidos, 'COP', sinTasa, { claveActivo: CLAVE }).motivo)
      .toBe(convertirDiasDesdeUsd(DIAS, 'COP', sinTasa, { claveActivo: CLAVE }).motivo);
  });

  it('acepta snapshots de Firestore (data()) igual que objetos planos', () => {
    const snapshots = DIAS.map((d) => ({ id: d.date, data: () => d }));

    expect(extraerDiasDelActivo(snapshots, 'USD', 'MSFT', 'stock')).toEqual(extraerDiasDelActivo(DIAS, 'USD', 'MSFT', 'stock'));
  });
});

// ============================================================================
// PUESTA AL DÍA
// ============================================================================

describe('HU 6.11.6: la puesta al día mantiene los días (RN-6.4-K, QA-08)', () => {
  const resumenUsd = (dias) => ({
    ...armarResumenDeActivo(dias, { ...ALCANCE, currency: 'USD' }),
    assetDays: extraerDiasDelActivo(dias, 'USD', 'MSFT', 'stock'),
  });

  it('añadir los días del cierre da los mismos días que prepararlo desde cero', () => {
    let resumen = resumenUsd(DIAS.slice(0, -2));
    for (const d of DIAS.slice(-2)) {
      const paso = aplicarDiaAResumenDeActivo(resumen, d.USD.assetPerformance[CLAVE], d.date, { now: AHORA, sinTopeDeHueco: true });
      expect(paso.method).toBe('incremental');
      resumen = paso.snapshot;
    }

    expect(resumen.assetDays).toEqual(extraerDiasDelActivo(DIAS, 'USD', 'MSFT', 'stock'));
    expect(detalleEnCop(docsDesdeDiasDelActivo(resumen.assetDays, CLAVE))).toEqual(detalleEnCop(DIAS));
  });

  it('un día sin el activo (vendido, la noche con la 6.4 apagada) queda marcado sin el activo', () => {
    const resumen = resumenUsd(DIAS.slice(0, -1));

    const { snapshot } = aplicarDiaAResumenDeActivo(resumen, null, '2026-11-13', { now: AHORA });

    expect(snapshot.assetDays[snapshot.assetDays.length - 1]).toEqual({ d: '2026-11-13', e: 'sin-activo' });
  });

  it('un resumen sin días (anterior a la HU) no los gana en la puesta al día: se completan aparte', () => {
    const { assetDays: _, ...sinDias } = resumenUsd(DIAS.slice(0, -1));
    const d = DIAS[DIAS.length - 1];

    const { snapshot } = aplicarDiaAResumenDeActivo(sinDias, d.USD.assetPerformance[CLAVE], d.date, { now: AHORA });

    expect(snapshot.assetDays).toBeUndefined();
  });
});
