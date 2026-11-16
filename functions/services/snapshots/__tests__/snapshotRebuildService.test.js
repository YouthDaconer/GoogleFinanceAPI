/**
 * HU 6.12 — Los resúmenes se rehacen desde la fecha afectada.
 *
 * - Identidad (RN-6-A, QA-02). El resumen rehecho desde la fecha es **el mismo**
 *   que el recálculo completo con los días ya corregidos, en todos los decimales:
 *   - en portafolio y cuenta, en USD y en la moneda principal;
 *   - en el activo en USD, con venta y recompra;
 *   - con la fecha a mitad de mes, el primer día de un mes y en otro año;
 *   - con resúmenes que el cierre mantuvo añadiendo días (el caso real).
 * - No lee los días anteriores a la fecha (RN-6.12-B, QA-01).
 * - Un resumen que no se puede rehacer se borra, solo ese, con su motivo
 *   (RN-6.12-F, QA-04); uno que termina antes de la fecha no se toca, y no se
 *   crea ninguno (RN-6.12-G, QA-09).
 *
 * Se usan las funciones reales del generador y del incremental del cierre sobre
 * la Firestore en memoria de los simuladores de la corrección nocturna.
 *
 * @see platform-docs/stories/6.12-correccion-desde-fecha-afectada/refinamiento.md (T6a)
 */

const { DateTime, Settings } = require('luxon');

const { store: mockStore, db: mockDb, clone: mockClone } = require('../../__tests__/support/firestoreEnMemoria');

jest.mock('firebase-admin/firestore', () => require('../../__tests__/support/firestoreEnMemoria').firestoreModule);

const {
  rehacerResumenDesde,
  rehacerResumenesDelInversor,
  docsDesdeResumen,
  resumenesRehechosDesdeLaFecha,
} = require('../snapshotRebuildService');
const {
  generatePerformanceSnapshot,
  generateAssetSnapshot,
  armarResumenDePortafolio,
  armarResumenDeActivo,
  fetchAllDailyDocs,
  buildSnapshotDocId,
} = require('../../snapshotGenerator');
const { updateSnapshotIncremental, MAX_DAILY_POINTS } = require('../../snapshotIncrementalService');
const { extraerDiasDelActivo } = require('../../../utils/assetDays');

const UID = 'uid-inversor-G';
const CTA = 'CTA-1';
const AHORA = DateTime.fromISO('2026-11-17T02:05:00', { zone: 'America/New_York' });
const NOW_ORIGINAL = Settings.now;

// ============================================================================
// FÁBRICAS
// ============================================================================

/** Días hábiles (lunes a viernes) entre dos fechas. */
function diasHabiles(desde, hasta) {
  const out = [];
  for (let d = DateTime.fromISO(desde); d.toISODate() <= hasta; d = d.plus({ days: 1 })) {
    if (d.weekday <= 5) out.push(d.toISODate());
  }
  return out;
}

const DIAS = diasHabiles('2025-05-07', '2026-11-16');

/**
 * El bloque de una moneda de un día. `version` cambia las cifras desde la fecha
 * corregida, como haría la corrección nocturna al reescribir los días.
 */
function bloque(i, tasa, { version = 0, conAapl = true } = {}) {
  const ajuste = 1 + version * 0.013;
  const valor = (100000 + 900 * Math.sin(i / 7) + 35 * i) * ajuste;
  const aapl = {
    totalValue: (20000 + 300 * Math.sin(i / 5)) * ajuste * tasa,
    totalInvestment: 18000 * tasa,
    units: 100 + version * 10,
    unrealizedProfitAndLoss: (2000 + 300 * Math.sin(i / 5)) * tasa,
    adjustedDailyChangePercentage: 0.11 * Math.sin(i / 3) + version * 0.01,
    dailyChangePercentage: 0.12 * Math.sin(i / 3),
    totalCashFlow: i % 41 === 0 ? -1500 * tasa : 0,
    doneProfitAndLoss: 0,
    totalROI: 11.1,
  };
  return {
    totalValue: valor * tasa,
    totalInvestment: (90000 + 20 * i) * tasa,
    adjustedDailyChangePercentage: 0.4 * Math.sin(i / 4) + version * 0.05,
    dailyChangePercentage: 0.45 * Math.sin(i / 4),
    totalCashFlow: i % 23 === 0 ? -2500 * tasa : (i % 37 === 0 ? 1200 * tasa : 0),
    doneProfitAndLoss: i % 29 === 0 ? 310.25 * tasa : 0,
    unrealizedProfitAndLoss: (valor - 90000 - 20 * i) * tasa,
    assetPerformance: {
      ...(conAapl ? { AAPL_stock: aapl } : {}),
      VOO_etf: { ...aapl, totalValue: 30000 * ajuste * tasa, units: 60 },
    },
  };
}

/**
 * Un documento diario con USD y COP. AAPL se vende entero en octubre de 2025 y
 * se recompra en diciembre (venta y recompra). Un día de agosto no tiene COP.
 */
function documento(fecha, i, version = 0) {
  const tasa = 4000 + 120 * Math.sin(i / 13);
  const conAapl = !(fecha >= '2025-10-14' && fecha < '2025-12-02');
  const doc = { date: fecha, USD: bloque(i, 1, { version, conAapl }) };
  if (fecha !== '2025-08-19') doc.COP = bloque(i, tasa, { version, conAapl });
  return doc;
}

const ruta = (cuenta, fecha) => (cuenta === 'overall'
  ? `portfolioPerformance/${UID}/dates/${fecha}`
  : `portfolioPerformance/${UID}/accounts/${cuenta}/dates/${fecha}`);

/** Escribe los días de un alcance; `corregidoDesde` reescribe desde esa fecha. */
function escribirDias(cuenta, hasta, corregidoDesde = null) {
  DIAS.forEach((fecha, i) => {
    if (fecha > hasta) return;
    const version = corregidoDesde && fecha >= corregidoDesde ? 1 : 0;
    mockStore.docs.set(ruta(cuenta, fecha), documento(fecha, i, version));
  });
}

/** El cierre de un día como lo pasa `unifiedMarketDataUpdate` al incremental. */
function datosDelCierre(fecha, moneda) {
  const i = DIAS.indexOf(fecha);
  const b = documento(fecha, i)[moneda];
  return { date: fecha, ...b };
}

/**
 * La historia de antes de la corrección: los resúmenes se prepararon el
 * 2026-11-06 y el cierre les fue añadiendo los días siguientes (incremental).
 */
async function prepararResumenes() {
  mockStore.docs = new Map();
  mockStore.writes = [];
  escribirDias('overall', '2026-11-16');
  escribirDias(CTA, '2026-11-16');

  const preparadoHasta = '2026-11-06';
  const docsHasta = async (cuenta) => (await fetchAllDailyDocs(mockDb, UID, cuenta))
    .filter((d) => d.data().date <= preparadoHasta);
  for (const cuenta of ['overall', CTA]) {
    const docs = await docsHasta(cuenta);
    for (const moneda of ['USD', 'COP']) {
      await generatePerformanceSnapshot(mockDb, UID, cuenta, moneda, { dailyDocs: docs });
    }
  }
  const docsOverall = await docsHasta('overall');
  await generateAssetSnapshot(mockDb, UID, 'overall', 'USD', 'AAPL', 'stock', { dailyDocs: docsOverall });
  await generateAssetSnapshot(mockDb, UID, 'overall', 'USD', 'VOO', 'etf', { dailyDocs: docsOverall });
  await generateAssetSnapshot(mockDb, UID, 'overall', 'COP', 'AAPL', 'stock', { dailyDocs: docsOverall });

  // El cierre añade los días del 7 al 16 de noviembre (Escenario real: incremental)
  for (const fecha of DIAS.filter((d) => d > preparadoHasta)) {
    for (const cuenta of ['overall', CTA]) {
      for (const moneda of ['USD', 'COP']) {
        const r = await updateSnapshotIncremental(mockDb, buildSnapshotDocId(UID, cuenta, moneda),
          datosDelCierre(fecha, moneda), { now: AHORA });
        expect(r.method).toBe('incremental');
      }
    }
  }
}

/** El recálculo completo de un resumen con los días ya corregidos (REF-COMPLETO). */
async function recalculoCompleto(resumen) {
  const docs = await fetchAllDailyDocs(mockDb, UID, resumen.accountId);
  const alcance = { userId: UID, accountId: resumen.accountId, currency: resumen.currency };
  if (resumen.type !== 'asset') return armarResumenDePortafolio(docs, alcance);
  const completo = armarResumenDeActivo(docs, { ...alcance, ticker: resumen.ticker, assetType: resumen.assetType });
  completo.assetDays = extraerDiasDelActivo(docs, resumen.currency, resumen.ticker, resumen.assetType);
  return completo;
}

const sinFecha = ({ lastUpdated, ...resto }) => resto;
const resumenGuardado = (id) => mockClone(mockStore.docs.get(`performanceSnapshots/${id}`));
const idsGuardados = () => [...mockStore.docs.keys()]
  .filter((p) => p.startsWith('performanceSnapshots/'))
  .map((p) => p.slice('performanceSnapshots/'.length))
  .sort();

beforeAll(() => {
  Settings.now = () => AHORA.toMillis();
});
afterAll(() => {
  Settings.now = NOW_ORIGINAL;
});
beforeEach(() => {
  jest.spyOn(console, 'log').mockImplementation(() => {});
  delete process.env.REBUILD_SNAPSHOTS_FROM_DATE;
});
afterEach(() => {
  jest.restoreAllMocks();
});

// ============================================================================
// IDENTIDAD
// ============================================================================

describe('HU 6.12 — rehacer desde la fecha = recálculo completo (RN-6-A, QA-02)', () => {
  it.each([
    ['a mitad de mes (COR-3S)', '2026-10-26'],
    ['el primer día hábil de un mes', '2026-09-01'],
    ['en el año anterior (COR-LARGA)', '2025-06-02'],
  ])('fecha %s: todos los resúmenes rehechos son idénticos al recálculo completo', async (_caso, desde) => {
    await prepararResumenes();
    const antes = idsGuardados();
    escribirDias('overall', '2026-11-16', desde);
    escribirDias(CTA, '2026-11-16', desde);

    const r = await rehacerResumenesDelInversor(mockDb, { userId: UID, desde });

    // Portafolio y cuenta en USD y COP + AAPL y VOO en USD; AAPL en COP no tiene sus días
    expect(r.rehechos.sort()).toEqual([
      `${UID}_${CTA}_COP`, `${UID}_${CTA}_USD`, `${UID}_AAPL_stock_USD`, `${UID}_COP`, `${UID}_USD`, `${UID}_VOO_etf_USD`,
    ].sort());
    expect(r.comoHoy).toEqual([{ id: `${UID}_AAPL_stock_COP`, motivo: 'sin-dias' }]);
    for (const id of r.rehechos) {
      const rehecho = resumenGuardado(id);
      expect(sinFecha(rehecho)).toEqual(sinFecha(await recalculoCompleto(rehecho)));
    }
    // RN-6.12-G: ninguno nuevo; solo falta el que se trató como hoy
    expect(idsGuardados()).toEqual(antes.filter((id) => id !== `${UID}_AAPL_stock_COP`));
  });

  it('las cifras cambian de verdad: el rehecho incluye la corrección y no es el resumen viejo', async () => {
    await prepararResumenes();
    const viejo = resumenGuardado(`${UID}_USD`);
    escribirDias('overall', '2026-11-16', '2026-10-26');

    await rehacerResumenesDelInversor(mockDb, { userId: UID, desde: '2026-10-26' });

    const nuevo = resumenGuardado(`${UID}_USD`);
    expect(nuevo.returns.oneMonthReturn).not.toBe(viejo.returns.oneMonthReturn);
    // Lo anterior a la fecha no cambió
    const antesDeLaFecha = (s) => s.timeline.filter((p) => p.d < '2026-10-26');
    expect(antesDeLaFecha(nuevo)).toEqual(antesDeLaFecha(viejo));
  });
});

// ============================================================================
// LECTURAS
// ============================================================================

describe('HU 6.12 — no relee lo anterior a la fecha (RN-6.12-B, QA-01)', () => {
  it('lee los días de cada alcance desde la fecha, una vez, para todos sus resúmenes', async () => {
    await prepararResumenes();
    escribirDias('overall', '2026-11-16', '2026-10-26');
    escribirDias(CTA, '2026-11-16', '2026-10-26');
    const consultas = [];
    const original = mockDb.collection;
    mockDb.collection = (nombre) => {
      const q = original(nombre);
      if (!nombre.endsWith('/dates')) return q;
      return {
        ...q,
        where: (f, op, v) => {
          consultas.push({ nombre, f, op, v });
          return q.where(f, op, v);
        },
        orderBy: () => {
          consultas.push({ nombre, sinFiltro: true });
          return q.orderBy('date', 'asc');
        },
      };
    };
    try {
      const r = await rehacerResumenesDelInversor(mockDb, { userId: UID, desde: '2026-10-26' });
      const diasDesde = DIAS.filter((d) => d >= '2026-10-26' && d <= '2026-11-16').length;
      expect(r.diasLeidos).toBe(diasDesde * 2);
    } finally {
      mockDb.collection = original;
    }
    // Una consulta por alcance, siempre desde la fecha; ninguna sin filtro
    expect(consultas).toHaveLength(2);
    expect(consultas).toEqual(expect.arrayContaining([
      { nombre: `portfolioPerformance/${UID}/dates`, f: 'date', op: '>=', v: '2026-10-26' },
      { nombre: `portfolioPerformance/${UID}/accounts/${CTA}/dates`, f: 'date', op: '>=', v: '2026-10-26' },
    ]));
  });
});

// ============================================================================
// COMO HOY, SIN CAMBIOS
// ============================================================================

describe('HU 6.12 — el que no se puede rehacer se trata como hoy, solo ese (RN-6.12-F, QA-04)', () => {
  it('la fecha es anterior al inicio del resumen: se borra con su motivo y los demás se rehacen', async () => {
    await prepararResumenes();
    // COR-MSFT: un resumen que empezó después de la fecha afectada
    const id = `${UID}_MSFT_stock_USD`;
    mockStore.docs.set(`performanceSnapshots/${id}`, {
      ...resumenGuardado(`${UID}_VOO_etf_USD`), ticker: 'MSFT', startDate: '2026-11-03',
    });
    escribirDias('overall', '2026-11-16', '2026-10-20');
    escribirDias(CTA, '2026-11-16', '2026-10-20');

    const r = await rehacerResumenesDelInversor(mockDb, { userId: UID, desde: '2026-10-20' });

    expect(r.comoHoy).toEqual(expect.arrayContaining([{ id, motivo: 'fecha-anterior-al-inicio' }]));
    expect(mockStore.docs.has(`performanceSnapshots/${id}`)).toBe(false);
    expect(r.rehechos).toEqual(expect.arrayContaining([`${UID}_USD`, `${UID}_AAPL_stock_USD`]));
  });

  it('un error al rehacer uno lo borra y sigue con los demás', async () => {
    await prepararResumenes();
    const roto = resumenGuardado(`${UID}_${CTA}_USD`);
    roto.timeline[3] = null;
    mockStore.docs.set(`performanceSnapshots/${UID}_${CTA}_USD`, roto);
    escribirDias('overall', '2026-11-16', '2026-10-26');
    escribirDias(CTA, '2026-11-16', '2026-10-26');

    const r = await rehacerResumenesDelInversor(mockDb, { userId: UID, desde: '2026-10-26' });

    const fallido = r.comoHoy.find((x) => x.id === `${UID}_${CTA}_USD`);
    expect(fallido.motivo).toMatch(/^error: /);
    expect(mockStore.docs.has(`performanceSnapshots/${UID}_${CTA}_USD`)).toBe(false);
    expect(r.rehechos).toContain(`${UID}_${CTA}_COP`);
  });

  it('un resumen que termina antes de la fecha no se toca (QA-09: tampoco se crea ninguno)', async () => {
    await prepararResumenes();
    const id = `${UID}_NVDA_stock_USD`;
    const viejo = { ...resumenGuardado(`${UID}_VOO_etf_USD`), ticker: 'NVDA', lastDateInTimeline: '2026-09-30' };
    mockStore.docs.set(`performanceSnapshots/${id}`, viejo);
    const antes = idsGuardados();
    escribirDias('overall', '2026-11-16', '2026-10-26');

    const r = await rehacerResumenesDelInversor(mockDb, { userId: UID, desde: '2026-10-26' });

    expect(r.sinCambios).toContain(id);
    expect(resumenGuardado(id)).toEqual(viejo);
    expect(idsGuardados()).toEqual(antes.filter((x) => x !== `${UID}_AAPL_stock_COP`));
  });

  it('otro inversor no se toca (QA-13)', async () => {
    await prepararResumenes();
    const ajeno = { userId: 'uid-inversor-P', currency: 'USD', timeline: [], schemaVersion: 3 };
    mockStore.docs.set('performanceSnapshots/uid-inversor-P_USD', ajeno);

    await rehacerResumenesDelInversor(mockDb, { userId: UID, desde: '2026-10-26' });

    expect(mockStore.docs.get('performanceSnapshots/uid-inversor-P_USD')).toEqual(ajeno);
  });
});

describe('HU 6.12 — rehacerResumenDesde: los motivos para tratarlo como hoy', () => {
  const base = () => ({
    userId: UID, accountId: 'overall', currency: 'USD', schemaVersion: 3,
    timeline: [{ d: '2026-10-01', v: 1, c: 0 }, { d: '2026-10-30', v: 2, c: 1 }],
    cashFlows: [], realizedPnL: [], startDate: '2026-10-01', lastDateInTimeline: '2026-10-30',
    monthlyCompound: { 2026: { 10: { startTotalInvestment: 1, endTotalInvestment: 1, unrealizedProfitAndLoss: 0 } } },
  });
  const dia = { date: '2026-10-30', USD: { totalValue: 2, adjustedDailyChangePercentage: 1 } };

  it.each([
    ['esquema viejo', (s) => ({ ...s, schemaVersion: 2 }), 'esquema-viejo'],
    ['sin las listas de la 6.9.2', (s) => { const { cashFlows, ...r } = s; return r; }, 'sin-listas'],
    ['serie comprimida', (s) => ({ ...s, timeline: Array.from({ length: MAX_DAILY_POINTS + 1 }, (_, i) => ({ d: `2026-10-${String((i % 28) + 1).padStart(2, '0')}`, v: 1, c: 0 })) }), 'serie-comprimida'],
    ['activo en la moneda principal, sin sus días', (s) => ({ ...s, type: 'asset', currency: 'COP' }), 'sin-dias'],
    ['un mes sin su inicio y cierre', (s) => ({ ...s, monthlyCompound: {} }), 'sin-mes'],
    ['sin alcance (no se sabe de qué días sale)', (s) => ({ ...s, accountId: undefined }), 'sin-alcance'],
  ])('%s → como hoy', (_caso, cambiar, motivo) => {
    expect(rehacerResumenDesde(cambiar(base()), [dia], '2026-10-15')).toEqual({ metodo: 'como-hoy', motivo });
  });

  it('sin días desde la fecha → como hoy', () => {
    expect(rehacerResumenDesde(base(), [], '2026-10-15')).toEqual({ metodo: 'como-hoy', motivo: 'sin-dias-desde-la-fecha' });
  });

  it('termina antes de la fecha → sin cambios', () => {
    expect(rehacerResumenDesde(base(), [dia], '2026-11-02')).toEqual({ metodo: 'sin-cambios' });
  });
});

describe('HU 6.12 — docsDesdeResumen', () => {
  it('reconstruye flujo, P&G realizado y la inversión del inicio y del cierre de cada mes', () => {
    const resumen = {
      currency: 'USD',
      timeline: [
        { d: '2026-09-29', v: 10, c: 0.1 }, { d: '2026-09-30', v: 11, c: 0.2 },
        { d: '2026-10-01', v: 12, c: 0.3 }, { d: '2026-10-02', v: 13, c: 0.4 },
      ],
      cashFlows: [{ d: '2026-09-30', f: -500 }],
      realizedPnL: [{ d: '2026-10-01', p: 42.5 }],
      monthlyCompound: {
        2026: {
          9: { startTotalInvestment: 100, endTotalInvestment: 150, unrealizedProfitAndLoss: -3 },
          10: { startTotalInvestment: 150, endTotalInvestment: 160, unrealizedProfitAndLoss: 7 },
        },
      },
    };
    expect(docsDesdeResumen(resumen, '2026-10-02', '2026-10-02')).toEqual([
      { date: '2026-09-29', USD: { totalValue: 10, adjustedDailyChangePercentage: 0.1, dailyChangePercentage: 0.1, totalInvestment: 100 } },
      { date: '2026-09-30', USD: { totalValue: 11, adjustedDailyChangePercentage: 0.2, dailyChangePercentage: 0.2, totalCashFlow: -500, totalInvestment: 150, unrealizedProfitAndLoss: -3 } },
      // Primero de octubre, que sigue con días leídos: solo su inicio
      { date: '2026-10-01', USD: { totalValue: 12, adjustedDailyChangePercentage: 0.3, dailyChangePercentage: 0.3, doneProfitAndLoss: 42.5, totalInvestment: 150 } },
    ]);
  });
});

describe('HU 6.12 — interruptor (RN-6-H)', () => {
  it('encendido salvo REBUILD_SNAPSHOTS_FROM_DATE=false', () => {
    expect(resumenesRehechosDesdeLaFecha()).toBe(true);
    process.env.REBUILD_SNAPSHOTS_FROM_DATE = 'false';
    expect(resumenesRehechosDesdeLaFecha()).toBe(false);
  });
});
