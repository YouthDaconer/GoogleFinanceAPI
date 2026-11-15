/**
 * HU 6.9.2 — Los resúmenes en monedas poco usadas se derivan del USD.
 *
 * - La fórmula: valor a la tasa del día y cambio diario convertido (QA-01).
 * - RN-6-A / RN-6-B: derivar el resumen en USD que mantiene la noche da
 *   exactamente lo mismo que derivar uno hecho desde cero, con flujos, ventas,
 *   cambio de mes y de año (QA-01, QA-07, QA-12).
 * - Sin la tasa de un día no se deriva (RN-3-D).
 * - Escenario 5: las lecturas no dependen de los años de historia (QA-10).
 * - RN-6.9.2-J: USD y la moneda principal no se derivan (QA-09).
 * - RN-6-H: con DERIVED_CURRENCY_SNAPSHOTS=false, el comportamiento de siempre (QA-13).
 *
 * @see platform-docs/stories/6.9.2-resumenes-poco-usados-incrementales/refinamiento.md (T6b)
 */

const { DateTime, Settings } = require('luxon');

const mockGetExchangeRates = jest.fn();
jest.mock('../../financeQuery', () => ({
  getExchangeRates: (...args) => mockGetExchangeRates(...args),
}));

const { generatePerformanceSnapshot } = require('../../snapshotGenerator');
const { updateSnapshotIncremental } = require('../../snapshotIncrementalService');
const {
  resumenesDerivadosDelUsd,
  esMonedaPocoUsada,
  derivarResumenDeMoneda,
  obtenerResumenDerivado,
  limpiarCachesDerivados,
} = require('../derivedCurrencySnapshotService');

// "Ahora" fijo: el desde cero usa DateTime.now() por dentro.
const AHORA = DateTime.fromISO('2026-01-07T18:00:00', { zone: 'America/New_York' });
const NOW_ORIGINAL = Settings.now;

// ============================================================================
// FÁBRICAS
// ============================================================================

/** Firestore en memoria con las rutas que usan el servicio y el generador. */
function crearDb({ documentos = {}, diarios = {} } = {}) {
  const datos = { ...documentos };
  const lecturas = [];
  const escrituras = {};
  const ref = (ruta) => ({
    get: async () => {
      lecturas.push(ruta);
      return { exists: datos[ruta] !== undefined, data: () => datos[ruta] };
    },
    set: async (data) => { escrituras[ruta] = data; datos[ruta] = data; },
  });
  return {
    lecturas,
    escrituras,
    doc: ref,
    collection: (nombre) => ({
      doc: (id) => ref(`${nombre}/${id}`),
      orderBy: () => ({
        get: async () => {
          const docs = (diarios[nombre] || []).map((d) => ({ data: () => d }));
          lecturas.push(...docs.map((d) => `${nombre}/${d.data().date}`));
          return { empty: docs.length === 0, docs };
        },
      }),
    }),
  };
}

/** Un día del portafolio en USD, como lo escribe el cierre. */
const diaUsd = (date, totalValue, cambio, extra = {}) => ({
  date,
  USD: {
    totalValue,
    totalInvestment: 900,
    adjustedDailyChangePercentage: cambio,
    dailyChangePercentage: cambio,
    totalCashFlow: 0,
    doneProfitAndLoss: 0,
    unrealizedProfitAndLoss: totalValue - 900,
    assetPerformance: {
      AAPL_stock: { totalValue: totalValue / 2, totalInvestment: 450, units: 3, unrealizedProfitAndLoss: totalValue / 2 - 450, totalROI: 11, dailyChangePercentage: cambio },
    },
    ...extra,
  },
});

/** Del 2025-12-26 al 2026-01-06: cambio de mes y de año, un aporte y una venta. */
const HISTORIA = [
  diaUsd('2025-12-26', 1000, 0),
  diaUsd('2025-12-29', 1010, 1),
  diaUsd('2025-12-30', 1525.05, 0.5, { totalCashFlow: -500 }),
  diaUsd('2025-12-31', 1510, -0.98),
  diaUsd('2026-01-02', 1530, 1.32),
  diaUsd('2026-01-05', 1320, 0.7, { totalCashFlow: 220, doneProfitAndLoss: 35.5 }),
  diaUsd('2026-01-06', 1335, 1.13),
];

const TASAS = {
  '2025-12-26': 3770.1, '2025-12-29': 3765.4, '2025-12-30': 3790.25, '2025-12-31': 3802,
  '2026-01-02': 3810.75, '2026-01-05': 3795.5, '2026-01-06': 3760.2,
};

/** Lo que el cierre le pasa a la noche para un día (unifiedMarketDataUpdate.js). */
const datosDeLaNoche = (dia) => ({ date: dia.date, ...dia.USD });

async function desdeCero(dias, accountId = 'overall') {
  const db = crearDb();
  return generatePerformanceSnapshot(db, 'u1', accountId, 'USD', { dailyDocs: dias.map((d) => ({ data: () => d })) });
}

/** El resumen en USD rehecho con los primeros `n` días y luego extendido noche a noche. */
async function mantenidoPorLaNoche(n) {
  const db = crearDb({ documentos: { 'performanceSnapshots/u1_USD': await desdeCero(HISTORIA.slice(0, n)) } });
  for (const dia of HISTORIA.slice(n)) {
    const r = await updateSnapshotIncremental(db, 'u1_USD', datosDeLaNoche(dia), { now: AHORA });
    expect(r.method).toBe('incremental');
  }
  return db.escrituras['performanceSnapshots/u1_USD'];
}

const sinHora = ({ lastUpdated, ...resto }) => resto;

beforeEach(() => {
  Settings.now = () => AHORA.toMillis();
  limpiarCachesDerivados();
  mockGetExchangeRates.mockReset();
  delete process.env.DERIVED_CURRENCY_SNAPSHOTS;
});

afterAll(() => {
  Settings.now = NOW_ORIGINAL;
});

// ============================================================================
// DERIVACIÓN PURA
// ============================================================================

describe('HU 6.9.2 — derivarResumenDeMoneda: USD × tasa del día', () => {
  it('el valor va a la tasa de su día y el cambio diario se convierte (QA-01)', () => {
    const usd = {
      userId: 'u1', accountId: 'overall', cashFlows: [], realizedPnL: [], monthlyCompound: {},
      timeline: [{ d: '2026-01-05', v: 1000, c: 0 }, { d: '2026-01-06', v: 1010, c: 1 }],
    };

    const { snapshot } = derivarResumenDeMoneda(usd, { '2026-01-05': 4000, '2026-01-06': 4040 }, 'COP', { now: AHORA });

    expect(snapshot.timeline[0]).toEqual({ d: '2026-01-05', v: 4000000, c: 0 });
    expect(snapshot.timeline[1].v).toBeCloseTo(4080400, 6);
    // (1 + 1 %) · 4040 / 4000 − 1 = 2,01 %
    expect(snapshot.timeline[1].c).toBeCloseTo(2.01, 10);
    expect(snapshot.currency).toBe('COP');
    expect(snapshot.derivedFrom).toBe('USD');
  });

  it('el primer día, o un día sin valor previo, conserva el cambio de USD: no hay variación que convertir', () => {
    const usd = {
      userId: 'u1', accountId: 'overall', cashFlows: [], realizedPnL: [], monthlyCompound: {},
      timeline: [{ d: '2026-01-02', v: 0, c: 0 }, { d: '2026-01-05', v: 1000, c: 0 }, { d: '2026-01-06', v: 1010, c: 1 }],
    };

    const { snapshot } = derivarResumenDeMoneda(usd,
      { '2026-01-02': 3900, '2026-01-05': 4000, '2026-01-06': 4040 }, 'COP', { now: AHORA });

    expect(snapshot.timeline.map((p) => p.c)).toEqual([0, 0, expect.closeTo(2.01, 10)]);
  });

  it('RN-6-A / RN-6-B: el resumen que mantiene la noche y uno hecho desde cero dan el mismo derivado, en todos sus decimales (QA-01, QA-07, QA-12)', async () => {
    const porLaNoche = await mantenidoPorLaNoche(3);
    const completo = await desdeCero(HISTORIA);

    const a = derivarResumenDeMoneda(porLaNoche, TASAS, 'COP', { now: AHORA });
    const b = derivarResumenDeMoneda(completo, TASAS, 'COP', { now: AHORA });

    expect(a.snapshot).not.toBeNull();
    expect(sinHora(a.snapshot)).toEqual(sinHora(b.snapshot));
  });

  it('el P&G del mes suma cada venta a la tasa de su día, y el aporte entra en el rendimiento personal', async () => {
    const { snapshot } = derivarResumenDeMoneda(await desdeCero(HISTORIA), TASAS, 'COP', { now: AHORA });

    const enero = snapshot.monthlyCompound['2026']['1'];
    expect(enero.doneProfitAndLoss).toBe(35.5 * TASAS['2026-01-05']);
    expect(enero.startTotalValue).toBe(1530 * TASAS['2026-01-02']);
    expect(enero.endTotalValue).toBe(1335 * TASAS['2026-01-06']);
    expect(enero.lastDayOfMonth).toBe(false);
    expect(snapshot.monthlyCompound['2025']['12'].lastDayOfMonth).toBe(true);
    // Sin los flujos el rendimiento personal saldría distinto
    const sinFlujos = derivarResumenDeMoneda({ ...(await desdeCero(HISTORIA)), cashFlows: [] }, TASAS, 'COP', { now: AHORA });
    expect(sinFlujos.snapshot.returns.ytdPersonalReturn).not.toBe(snapshot.returns.ytdPersonalReturn);
  });

  it('el detalle por activo va a la tasa del último día y conserva unidades y ROI', async () => {
    const { snapshot } = derivarResumenDeMoneda(await desdeCero(HISTORIA), TASAS, 'COP', { now: AHORA });

    expect(snapshot.latestAssetPerformance.AAPL_stock).toMatchObject({
      totalValue: 667.5 * TASAS['2026-01-06'],
      units: 3,
      totalROI: 11,
    });
  });

  it('un día sin cierre de divisa (festivo de Londres) toma el cierre hábil anterior, como historicalRateService', async () => {
    const tasas = { ...TASAS };
    delete tasas['2025-12-31'];

    const { snapshot } = derivarResumenDeMoneda(await desdeCero(HISTORIA), tasas, 'COP', { now: AHORA });

    expect(snapshot.timeline.find((p) => p.d === '2025-12-31').v).toBe(1510 * TASAS['2025-12-30']);
  });

  it('RN-3-D: sin cierre en los 5 días anteriores no hay tasa: no se deriva ni se inventa', async () => {
    const tasas = { ...TASAS };
    delete tasas['2025-12-26']; // primer día, sin cierres previos en la serie

    const resultado = derivarResumenDeMoneda(await desdeCero(HISTORIA), tasas, 'COP', { now: AHORA });

    expect(resultado).toEqual({ snapshot: null, motivo: 'sin-tasa-2025-12-26' });
  });

  it.each([
    ['cero', 0],
    ['negativa', -3950.5],
    ['vacía', null],
  ])('HU 6.11.2: una tasa %s no se tapa con el cierre anterior: no hay con qué calcular (QA-15)', async (_caso, valor) => {
    const tasas = { ...TASAS, '2025-12-31': valor };

    const resultado = derivarResumenDeMoneda(await desdeCero(HISTORIA), tasas, 'COP', { now: AHORA });

    expect(resultado).toEqual({ snapshot: null, motivo: 'sin-tasa-2025-12-31' });
  });

  it('HU 6.11.2: un día sin cierre cuyo cierre anterior es inválido tampoco se calcula (QA-15)', async () => {
    const tasas = { ...TASAS, '2025-12-30': 0 };
    delete tasas['2025-12-31'];

    const resultado = derivarResumenDeMoneda(await desdeCero(HISTORIA), tasas, 'COP', { now: AHORA });

    // El primer día que topa con la tasa inválida es el 30 (la suya)
    expect(resultado).toEqual({ snapshot: null, motivo: 'sin-tasa-2025-12-30' });
  });

  it('un resumen en USD sin las listas por día no se deriva', async () => {
    const { cashFlows, realizedPnL, ...sinListas } = await desdeCero(HISTORIA);

    expect(derivarResumenDeMoneda(sinListas, TASAS, 'COP', { now: AHORA }))
      .toEqual({ snapshot: null, motivo: 'usd-sin-listas' });
  });
});

// ============================================================================
// OBTENER EL DERIVADO
// ============================================================================

describe('HU 6.9.2 — obtenerResumenDerivado', () => {
  /** Historia de `n` días seguidos con valor creciente. */
  function historiaDe(n) {
    const dias = [];
    const tasas = {};
    let fecha = AHORA.minus({ days: n });
    for (let i = 0; i < n; i++) {
      fecha = fecha.plus({ days: 1 });
      const d = fecha.toISODate();
      dias.push(diaUsd(d, 1000 + i, 0.1));
      tasas[d] = 3800 + (i % 7);
    }
    return { dias, tasas };
  }

  async function dbConResumenUsd(dias, defaultCurrency = 'USD') {
    return crearDb({
      documentos: {
        'userData/u1': { defaultCurrency },
        'performanceSnapshots/u1_USD': await desdeCero(dias),
      },
    });
  }

  it('Escenario 5: lee lo mismo con 30 que con 1.260 días de historia: el resumen en USD y nada más (QA-10)', async () => {
    const lecturasCon = async (n) => {
      limpiarCachesDerivados();
      const { dias, tasas } = historiaDe(n);
      mockGetExchangeRates.mockResolvedValue({ rates: { COP: tasas } });
      const db = await dbConResumenUsd(dias);
      const r = await obtenerResumenDerivado(db, { userId: 'u1', accountId: 'overall', currency: 'COP' });
      expect(r.metodo).toBe('derivado');
      expect(r.snapshot.timeline).toHaveLength(n);
      return db.lecturas;
    };

    const corta = await lecturasCon(30);
    const larga = await lecturasCon(1260);

    expect(corta).toEqual(['userData/u1', 'performanceSnapshots/u1_USD']);
    expect(larga).toEqual(corta);
    expect(mockGetExchangeRates).toHaveBeenCalledTimes(2); // una llamada por consulta, no una por día
  });

  it('no escribe nada: no hay resumen guardado que mantener', async () => {
    const { dias, tasas } = historiaDe(30);
    mockGetExchangeRates.mockResolvedValue({ rates: { COP: tasas } });
    const db = await dbConResumenUsd(dias);

    await obtenerResumenDerivado(db, { userId: 'u1', accountId: 'overall', currency: 'COP' });

    expect(db.escrituras).toEqual({});
  });

  it('RN-6.9.2-J: USD y la moneda principal no se derivan (QA-09)', async () => {
    const { dias } = historiaDe(30);
    const db = await dbConResumenUsd(dias, 'COP');

    expect((await obtenerResumenDerivado(db, { userId: 'u1', accountId: 'overall', currency: 'USD' })).metodo).toBe('no-aplica');
    expect((await obtenerResumenDerivado(db, { userId: 'u1', accountId: 'overall', currency: 'COP' })).metodo).toBe('no-aplica');
    expect((await obtenerResumenDerivado(db, { userId: 'u1', accountId: 'overall', currency: 'EUR' })).metodo).not.toBe('no-aplica');
    expect(esMonedaPocoUsada('EUR', 'COP')).toBe(true);
    expect(esMonedaPocoUsada('COP', 'COP')).toBe(false);
  });

  it('pide las tasas desde el 1 de enero del año del primer día (con margen para su cierre anterior), para compartir la consulta entre inversores', async () => {
    const { dias, tasas } = historiaDe(30);
    mockGetExchangeRates.mockResolvedValue({ rates: { COP: tasas } });

    await obtenerResumenDerivado(await dbConResumenUsd(dias), { userId: 'u1', accountId: 'overall', currency: 'COP' });

    expect(mockGetExchangeRates).toHaveBeenCalledWith(['COP'], '2025-01-01', dias[dias.length - 1].date);
  });

  it('si el resumen en USD aún no tiene las listas, no deriva ni rehace nada: lo dice y quien llama sigue su camino', async () => {
    const { cashFlows, realizedPnL, ...sinListas } = await desdeCero(historiaDe(30).dias);
    const db = crearDb({ documentos: { 'userData/u1': { defaultCurrency: 'USD' }, 'performanceSnapshots/u1_USD': sinListas } });

    const r = await obtenerResumenDerivado(db, { userId: 'u1', accountId: 'overall', currency: 'COP' });

    expect(r).toEqual({ snapshot: null, metodo: 'no-derivable', motivo: 'usd-sin-listas' });
    expect(mockGetExchangeRates).not.toHaveBeenCalled();
    expect(db.escrituras).toEqual({});
    expect(db.lecturas).toEqual(['userData/u1', 'performanceSnapshots/u1_USD']);
  });

  it('si finance-query no responde, no deriva y lo dice', async () => {
    mockGetExchangeRates.mockResolvedValue(null);
    const db = await dbConResumenUsd(historiaDe(30).dias);

    const r = await obtenerResumenDerivado(db, { userId: 'u1', accountId: 'overall', currency: 'COP' });

    expect(r).toEqual({ snapshot: null, metodo: 'no-derivable', motivo: 'sin-tasas' });
  });

  it('Escenario 3: si la señal es posterior al resumen en USD, se rehace el de USD antes de derivar (QA-06)', async () => {
    const { dias, tasas } = historiaDe(30);
    mockGetExchangeRates.mockResolvedValue({ rates: { COP: tasas } });
    const viejo = { ...(await desdeCero(dias)), lastUpdated: '2026-01-01T00:00:00.000Z' };
    const db = crearDb({
      documentos: { 'userData/u1': { defaultCurrency: 'USD' }, 'performanceSnapshots/u1_USD': viejo },
      diarios: { 'portfolioPerformance/u1/dates': dias },
    });

    const r = await obtenerResumenDerivado(db, {
      userId: 'u1', accountId: 'overall', currency: 'COP', senal: '2026-01-07T05:00:00.000Z', prepararUsd: true,
    });

    expect(r.metodo).toBe('derivado');
    expect(db.escrituras['performanceSnapshots/u1_USD']).toBeDefined();
  });

  it('sin prepararUsd (atribución, riesgo), un resumen en USD viejo no se rehace: no hay derivado y nada cuesta más que antes', async () => {
    const viejo = { ...(await desdeCero(historiaDe(30).dias)), lastUpdated: '2026-01-01T00:00:00.000Z' };
    const db = crearDb({ documentos: { 'userData/u1': { defaultCurrency: 'USD' }, 'performanceSnapshots/u1_USD': viejo } });

    const r = await obtenerResumenDerivado(db, {
      userId: 'u1', accountId: 'overall', currency: 'COP', senal: '2026-01-07T05:00:00.000Z',
    });

    expect(r).toEqual({ snapshot: null, metodo: 'no-derivable', motivo: 'sin-resumen-usd-al-dia' });
    expect(db.escrituras).toEqual({});
  });
});

describe('HU 6.9.2 — interruptor (RN-6-H, QA-13)', () => {
  it('encendido salvo DERIVED_CURRENCY_SNAPSHOTS=false', () => {
    expect(resumenesDerivadosDelUsd()).toBe(true);
    process.env.DERIVED_CURRENCY_SNAPSHOTS = 'false';
    expect(resumenesDerivadosDelUsd()).toBe(false);
  });
});
