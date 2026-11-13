/**
 * HU 6.8.1: la corrección nocturna avanza por tramos, desde donde quedó.
 *
 * Simulador de noches: Firestore en memoria, el motor real
 * (`backfillUserPerformance`), precios y tasas simulados y un reloj simulado
 * en el que cada escritura cuesta un segundo. RC (el recálculo completo) es
 * una sola llamada al motor sobre todo el rango, desde el mismo estado.
 *
 * El historial "viejo" de cada inversor lo escribe el propio motor antes de
 * la operación tardía, como lo habría dejado el EOD, con docs de fin de semana
 * de procesos viejos. Después el histórico de SPY se mueve un 0,7 %: así la
 * valoración que manda es la que registró el EOD (precio implícito) y no la del
 * histórico, que es lo que podría romper la igualdad entre tramos.
 *
 * @see platform-docs/stories/6.8.1-historial-corregido-por-tramos/qa.md
 */

const { DateTime } = require('luxon');

jest.setTimeout(180000);

// --- Reloj y Firestore en memoria (compartidos con los tests de la HU 6.8.2) --
const {
  clock: mockClock,
  store: mockStore,
  db: mockDb,
  clone: mockClone,
  isMap: mockIsMap,
} = require('./support/firestoreEnMemoria');

jest.mock('firebase-admin/firestore', () => require('./support/firestoreEnMemoria').firestoreModule);
jest.mock('../firebaseAdmin', () => ({ firestore: () => mockDb }));
jest.mock('firebase-functions/v2/scheduler', () => ({ onSchedule: (_opts, fn) => fn }));
jest.mock('../periodConsolidationScheduled', () => ({
  consolidateMonth: jest.fn(async () => {}),
  consolidateYear: jest.fn(async () => {}),
}));
jest.mock('../cacheInvalidationService', () => ({ invalidatePerformanceCache: jest.fn(async () => {}) }));
jest.mock('../config', () => ({ getServiceTokenSecret: () => 'token-de-prueba' }));

const mockRates = { byDate: {} };
jest.mock('../historicalRateService', () => ({
  getRatesForRange: async (_currencies, start, end) => Object.fromEntries(
    Object.entries(mockRates.byDate).filter(([d]) => d >= start && d <= end),
  ),
}));

const mockPrices = {};
jest.mock('node-fetch', () => {
  const fn = jest.fn(async (url) => {
    const symbol = decodeURIComponent(/symbol=([^&]+)/.exec(url)[1]);
    const series = mockPrices[symbol];
    if (!series) return { ok: false, status: 404, json: async () => ({}) };
    return { ok: true, status: 200, json: async () => Object.fromEntries(Object.entries(series).map(([d, c]) => [d, { close: c }])) };
  });
  fn.default = fn;
  return fn;
});

const { runReconciliation, LIMITS } = require('../reconcileStalePerformance');
const {
  backfillUserPerformance,
  generateTradingDays,
  clearHistoricalPriceCache,
  CONFIG,
} = require('../backfillCoreModule');
const { markPerformanceAsStale } = require('../../utils/performanceStaleMarker');
const { consolidateMonth } = require('../periodConsolidationScheduled');

CONFIG.API_DELAY_MS = 0;

// --- Mercado simulado -------------------------------------------------------
const HOLIDAYS = {
  '2024-07-04': 'Independence Day', '2024-09-02': 'Labor Day', '2024-11-28': 'Thanksgiving',
  '2024-12-25': 'Christmas', '2025-01-01': 'New Year', '2025-07-04': 'Independence Day',
  '2025-09-01': 'Labor Day', '2025-11-27': 'Thanksgiving', '2025-12-25': 'Christmas',
  '2026-01-01': 'New Year', '2026-07-03': 'Independence Day (observed)', '2026-09-07': 'Labor Day',
};

function calendar(from, to) {
  const out = [];
  for (let d = DateTime.fromISO(from); d.toISODate() <= to; d = d.plus({ days: 1 })) out.push(d.toISODate());
  return out;
}
const isTradingDay = (date) => DateTime.fromISO(date).weekday <= 5 && !HOLIDAYS[date];
const TRADING_DAYS = calendar('2024-05-01', '2026-12-31').filter(isTradingDay);

/** Día hábil `n` posiciones después del primer día hábil desde `date`. */
function tradingOffset(date, n) {
  const i = TRADING_DAYS.findIndex((d) => d >= date);
  return TRADING_DAYS[i + n];
}

function buildMarket() {
  const series = (base, drift, amp, period) => Object.fromEntries(
    TRADING_DAYS.map((d, i) => [d, Number((base + drift * i + amp * Math.sin(i / period)).toFixed(4))]),
  );
  return {
    prices: {
      SPY: series(450, 0.21, 6, 7), QQQ: series(380, 0.17, 5, 5), ECO: series(2100, 0.4, 40, 11),
      MSFT: series(410, 0.12, 7, 9), GOOGL: series(160, 0.05, 3, 6), KO: series(62, 0.01, 1.5, 8),
    },
    rates: Object.fromEntries(TRADING_DAYS.map((d, i) => [d, { COP: Number((4000 + 120 * Math.sin(i / 13)).toFixed(2)) }])),
  };
}
const MARKET = buildMarket();

// --- Estado ------------------------------------------------------------------
function resetStore() {
  mockStore.docs = new Map([
    ['marketHolidays/US', { holidays: HOLIDAYS }],
    ['currencies/COP', { code: 'COP', isActive: true }],
  ]);
  mockStore.failCommit = null;
  mockStore.afterCommit = null;
  mockStore.writes = [];
  Object.keys(mockPrices).forEach((k) => delete mockPrices[k]);
  Object.assign(mockPrices, mockClone(MARKET.prices));
  mockRates.byDate = MARKET.rates;
}

const snapshotStore = () => new Map([...mockStore.docs].map(([p, d]) => [p, mockClone(d)]));

/** Junta las bases de varios inversores en una Firestore limpia. */
function restore(...bases) {
  resetStore();
  for (const base of bases) for (const [p, d] of base) mockStore.docs.set(p, mockClone(d));
}

/** El histórico se mueve: la valoración del EOD ya no es la del histórico. */
function moveHistoricalPrices() {
  for (const d of Object.keys(mockPrices.SPY)) mockPrices.SPY[d] = Number((mockPrices.SPY[d] * 1.007).toFixed(4));
}

const at = (date) => `${date}T15:00:00.000Z`;
const px = (symbol, date) => MARKET.prices[symbol][date];

function addTx(uid, id, account, { type, asset, name, assetType = 'etf', amount, currency = 'USD', date }) {
  mockStore.docs.set(`transactions/${uid}-${id}`, {
    userId: uid, portfolioAccountId: `${uid}-${account}`, type, assetId: `${uid}-${asset}`,
    assetName: name, assetType, amount, price: px(name, date), currency, date: at(date),
  });
}

/** Los lotes suman lo que dicen sus transacciones, como los deja el registro. */
function syncLots(uid) {
  const lots = new Map();
  for (const [path, tx] of mockStore.docs) {
    if (!path.startsWith('transactions/') || tx.userId !== uid) continue;
    const lot = lots.get(tx.assetId) || {
      id: tx.assetId, name: tx.assetName, assetType: tx.assetType, portfolioAccount: tx.portfolioAccountId,
      currency: tx.currency, units: 0, unitValue: tx.price, acquisitionDate: tx.date,
    };
    lot.units += tx.type === 'buy' ? tx.amount : -tx.amount;
    if (tx.date < lot.acquisitionDate) lot.acquisitionDate = tx.date;
    lots.set(tx.assetId, lot);
  }
  for (const [path, d] of [...mockStore.docs]) {
    if (path.startsWith('assets/') && d.userId === uid) mockStore.docs.delete(path);
  }
  for (const lot of lots.values()) {
    const units = Number(lot.units.toFixed(8));
    mockStore.docs.set(`assets/${lot.id}`, { ...lot, units, userId: uid, isActive: units > 0 });
  }
}

/** Cartera de un inversor: dos cuentas (USD y COP), compras, una venta. */
function seedInvestor(uid, from, extras = []) {
  const start = tradingOffset(from, 0);
  mockStore.docs.set(`portfolioAccounts/${uid}-usd`, { userId: uid, isActive: true, name: 'Broker USD' });
  mockStore.docs.set(`portfolioAccounts/${uid}-cop`, { userId: uid, isActive: true, name: 'Broker COP' });
  mockStore.docs.set(`portfolioPerformance/${uid}`, { userId: uid });
  addTx(uid, 'b-spy', 'usd', { type: 'buy', asset: 'spy', name: 'SPY', amount: 10, date: start });
  addTx(uid, 'b-eco', 'cop', { type: 'buy', asset: 'eco', name: 'ECO', assetType: 'stock', amount: 100, currency: 'COP', date: start });
  addTx(uid, 'b-qqq', 'usd', { type: 'buy', asset: 'qqq', name: 'QQQ', amount: 6, date: tradingOffset(start, 25) });
  const sell = tradingOffset(start, 90);
  if (sell <= '2026-11-12') addTx(uid, 's-spy', 'usd', { type: 'sell', asset: 'spy', name: 'SPY', amount: 3, date: sell });
  extras.forEach(([id, spec]) => addTx(uid, id, 'usd', spec));
  syncLots(uid);
}

/** El historial que dejó el EOD hasta el 2026-11-12, con docs de fin de semana de procesos viejos. */
async function writeOldHistory(uid, from) {
  clearHistoricalPriceCache();
  const days = await generateTradingDays(tradingOffset(from, 0), '2026-11-12');
  const saved = mockClock.commitMs;
  mockClock.commitMs = 0;
  const result = await backfillUserPerformance(uid, days);
  mockClock.commitMs = saved;
  if (!result.success) throw new Error(`historial base de ${uid}: ${result.errors.join('; ')}`);
  for (const date of calendar(days[0], '2026-11-12').filter((d) => !isTradingDay(d))) {
    const old = { date, USD: { totalValue: 1, adjustedDailyChangePercentage: 9.9 } };
    mockStore.docs.set(`portfolioPerformance/${uid}/dates/${date}`, old);
    mockStore.docs.set(`portfolioPerformance/${uid}/accounts/${uid}-usd/dates/${date}`, mockClone(old));
  }
}

/** Registra una operación con fecha pasada y marca la corrección, como el registro real. */
async function registerLate(uid, id, spec, account = 'usd') {
  addTx(uid, id, account, spec);
  syncLots(uid);
  await markPerformanceAsStale(uid, spec.date, { reason: 'retroactive_transaction', transactionType: spec.type });
  const stale = mockStore.docs.get(`portfolioPerformance/${uid}`)._stale;
  if (!stale) throw new Error(`no se marcó ${uid}`);
  return stale;
}

const staleOf = (uid) => mockStore.docs.get(`portfolioPerformance/${uid}`)?._stale;

function historyOf(uid) {
  const prefix = `portfolioPerformance/${uid}/`;
  return Object.fromEntries([...mockStore.docs].filter(([p]) => p.startsWith(prefix)).map(([p, d]) => [p, mockClone(d)]));
}

/** Diferencias entre dos historiales: números a 1e-6 (por debajo del céntimo), sin `lastUpdated`. */
function differences(actual, expected) {
  const out = [];
  const compare = (a, b, where) => {
    if (typeof a === 'number' && typeof b === 'number') {
      if (Math.abs(a - b) > 1e-6) out.push(`${where}: ${a} ≠ ${b}`);
    } else if (mockIsMap(a) && mockIsMap(b)) {
      for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
        if (k !== 'lastUpdated') compare(a[k], b[k], `${where}.${k}`);
      }
    } else if (JSON.stringify(a) !== JSON.stringify(b)) {
      out.push(`${where}: ${JSON.stringify(a)} ≠ ${JSON.stringify(b)}`);
    }
  };
  for (const path of new Set([...Object.keys(actual), ...Object.keys(expected)])) {
    if (!(path in actual)) out.push(`${path}: falta`);
    else if (!(path in expected)) out.push(`${path}: sobra`);
    else compare(actual[path], expected[path], path);
  }
  return out;
}

/** RC: una sola llamada al motor sobre todo el rango, desde `state`. */
async function referenceCorrection(state, uid, since, lastClosed) {
  const saved = snapshotStore();
  mockStore.docs = new Map([...state].map(([p, d]) => [p, mockClone(d)]));
  clearHistoricalPriceCache();
  const savedCost = mockClock.commitMs;
  mockClock.commitMs = 0;
  const result = await backfillUserPerformance(uid, await generateTradingDays(since, lastClosed), { rangeStart: since });
  mockClock.commitMs = savedCost;
  if (!result.success) throw new Error(`RC de ${uid}: ${result.errors.join('; ')}`);
  const rc = historyOf(uid);
  mockStore.docs = saved;
  return rc;
}

// --- Noches -----------------------------------------------------------------
const NIGHT_1 = DateTime.fromISO('2026-11-13T02:00:00', { zone: 'America/New_York' });
const lastClosedOf = (n) => NIGHT_1.plus({ days: n - 2 }).toISODate();
let logs = [];

const TRAMO_RE = /\[reconcile\] TRAMO (\S+) (\d{4}-\d{2}-\d{2})→(\d{4}-\d{2}-\d{2}) \((\d+) días, \d+ ms\): (\w+)/;
function tramosIn(lines) {
  return lines.map((l) => TRAMO_RE.exec(l)).filter(Boolean)
    .map(([, uid, desde, hasta, days, status]) => ({ uid, desde, hasta, days: Number(days), status }));
}

/** Corre la corrección a una hora dada (por defecto, la noche `n` a las 02:00 ET). */
async function run({ n = 1, atTime = null, timeBudgetMs } = {}) {
  mockClock.ms = (atTime || NIGHT_1.plus({ days: n - 1 })).toMillis();
  const from = logs.length;
  const totals = await runReconciliation({ now: () => mockClock.ms, ...(timeBudgetMs ? { timeBudgetMs } : {}) });
  return { totals, tramos: tramosIn(logs.slice(from)) };
}

/** Noches seguidas hasta que el inversor queda sin marca. */
async function nightsUntilDone(uid, { firstNight = 1, timeBudgetMs, max = 20 } = {}) {
  const nights = [];
  for (let n = firstNight; n < firstNight + max; n++) {
    nights.push({ n, ...(await run({ n, timeBudgetMs })), staleAfter: mockClone(staleOf(uid)) });
    if (!staleOf(uid)) return nights;
  }
  throw new Error(`${uid} no terminó en ${max} noches`);
}

/** Una corrida de un tramo exacto: el presupuesto se agota en cuanto empieza. */
const ONE_TRAMO = 1;

// --- Bases: el historial viejo de cada inversor, escrito una vez ------------
const BASES = {};
const FROM = { 'inv-a': '2024-06-03', 'inv-b': '2025-11-03', 'inv-c': '2026-01-05', 'inv-e': '2026-02-02' };

beforeAll(async () => {
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
  for (const [uid, from] of Object.entries(FROM)) {
    resetStore();
    const extras = uid === 'inv-b' ? [
      ['b-msft', { type: 'buy', asset: 'msft', name: 'MSFT', assetType: 'stock', amount: 25, date: '2026-07-01' }],
      ['b-googl', { type: 'buy', asset: 'googl', name: 'GOOGL', assetType: 'stock', amount: 8, date: '2026-06-15' }],
    ] : [];
    seedInvestor(uid, from, extras);
    await writeOldHistory(uid, from);
    BASES[uid] = snapshotStore();
  }
  for (let i = 1; i <= 12; i++) {
    const uid = `sat-${String(i).padStart(2, '0')}`;
    resetStore();
    seedInvestor(uid, '2026-09-01');
    await writeOldHistory(uid, '2026-09-01');
    BASES[uid] = snapshotStore();
  }
  jest.restoreAllMocks();
});

beforeEach(() => {
  logs = [];
  mockClock.commitMs = 1000;
  consolidateMonth.mockClear();
  const capture = (...args) => logs.push(args.map(String).join(' '));
  jest.spyOn(console, 'log').mockImplementation(capture);
  jest.spyOn(console, 'error').mockImplementation(capture);
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

// --- Casos -------------------------------------------------------------------
describe('HU 6.8.1 — una corrección de hasta 6 meses termina esa noche (Escenario 1)', () => {
  test.each([
    ['OP-1, 1 día atrás', '2026-11-11'],
    ['OP-2, 3 meses atrás', '2026-08-12'],
    ['OP-3, 6 meses atrás (límite que entra)', '2026-05-12'],
  ])('QA-01 %s: a la mañana siguiente está corregida, idéntica al céntimo a RC, y sin marca', async (_label, date) => {
    restore(BASES['inv-b']);
    moveHistoricalPrices();
    await registerLate('inv-b', 'late', { type: 'buy', asset: 'ko', name: 'KO', assetType: 'stock', amount: 12, date });
    const state = snapshotStore();

    const { tramos } = await run({ n: 1 });

    expect(staleOf('inv-b')).toBeUndefined();
    expect(tramos[0]).toMatchObject({ uid: 'inv-b', desde: date });
    expect(tramos[tramos.length - 1]).toMatchObject({ hasta: '2026-11-12', status: 'reconciled' });
    expect(differences(historyOf('inv-b'), await referenceCorrection(state, 'inv-b', date, lastClosedOf(1)))).toEqual([]);
  });

  test.each([
    ['QA-02 edición: MSFT de 25 a 20 desde el 2026-07-01', '2026-07-01', () => {
      mockStore.docs.get('transactions/inv-b-b-msft').amount = 20;
    }],
    ['QA-02 borrado: GOOGL, en cartera desde el 2026-06-15', '2026-06-15', () => {
      mockStore.docs.delete('transactions/inv-b-b-googl');
    }],
  ])('%s: queda corregido esa noche e idéntico a RC', async (_label, since, change) => {
    restore(BASES['inv-b']);
    moveHistoricalPrices();
    change();
    syncLots('inv-b');
    await markPerformanceAsStale('inv-b', since, { reason: 'asset_edit' });
    const state = snapshotStore();

    await run({ n: 1 });

    expect(staleOf('inv-b')).toBeUndefined();
    expect(differences(historyOf('inv-b'), await referenceCorrection(state, 'inv-b', since, lastClosedOf(1)))).toEqual([]);
  });
});

describe('HU 6.8.1 — un rango largo avanza por tramos, desde donde quedó (Escenario 3)', () => {
  test('QA-03: OP-4 (6 meses y 1 día) avanza por tramos, sigue marcada mientras falte un día y al completarse es igual a RC', async () => {
    restore(BASES['inv-b']);
    moveHistoricalPrices();
    await registerLate('inv-b', 'op4', { type: 'buy', asset: 'ko', name: 'KO', assetType: 'stock', amount: 6, date: '2026-05-11' });
    const state = snapshotStore();

    const nights = await nightsUntilDone('inv-b', { timeBudgetMs: ONE_TRAMO });
    const tramos = nights.flatMap((night) => night.tramos);

    expect(tramos.length).toBeGreaterThan(1);
    expect(tramos.map((t) => t.status)).toEqual([...tramos.slice(1).map(() => 'advanced'), 'reconciled']);
    nights.slice(0, -1).forEach((night) => expect(night.staleAfter).toBeDefined());
    const lastClosed = lastClosedOf(nights[nights.length - 1].n);
    expect(differences(historyOf('inv-b'), await referenceCorrection(state, 'inv-b', '2026-05-11', lastClosed))).toEqual([]);
  });

  test('QA-06 y RN-6-A: la historia completa se corrige en varias noches, sin repetir días ni dejar huecos, y da lo mismo que RC', async () => {
    restore(BASES['inv-a']);
    moveHistoricalPrices();
    // Edición con efecto desde el primer día de su historia
    const since = tradingOffset(FROM['inv-a'], 0);
    mockStore.docs.get('transactions/inv-a-b-spy').amount = 12;
    syncLots('inv-a');
    await markPerformanceAsStale('inv-a', since, { reason: 'asset_edit' });
    const state = snapshotStore();

    const nights = await nightsUntilDone('inv-a');
    const tramos = nights.flatMap((night) => night.tramos.map((t) => ({ ...t, n: night.n })));

    // Necesita más de una noche y la marca sigue hasta la última
    expect(nights.length).toBeGreaterThan(1);
    nights.slice(0, -1).forEach((night) => expect(night.staleAfter).toBeDefined());
    expect(tramos.filter((t) => t.status === 'reconciled')).toHaveLength(1);
    expect(tramos[tramos.length - 1].status).toBe('reconciled');

    // Cada tramo empieza en el día hábil siguiente al anterior: ni repetidos ni huecos
    expect(tramos[0].desde).toBe(since);
    tramos.forEach((t) => expect(t.days).toBeLessThanOrEqual(LIMITS.DIAS_POR_TRAMO));
    for (let i = 1; i < tramos.length; i++) {
      expect(tramos[i].desde).toBe(tradingOffset(tramos[i - 1].hasta, 1));
    }
    const lastClosed = lastClosedOf(nights[nights.length - 1].n);
    const total = (await generateTradingDays(since, lastClosed)).length;
    expect(tramos.reduce((s, t) => s + t.days, 0)).toBe(total);

    // Hay fines de semana entre tramos: ahí estaban los docs que podían quedarse
    expect(tramos.some((t, i) => i > 0 && calendar(tramos[i - 1].hasta, t.desde).length > 2)).toBe(true);
    const leftovers = Object.keys(historyOf('inv-a')).filter((p) => {
      const date = p.slice(-10);
      return !isTradingDay(date) && date >= since && date <= lastClosed;
    });
    expect(leftovers).toEqual([]);

    expect(differences(historyOf('inv-a'), await referenceCorrection(state, 'inv-a', since, lastClosed))).toEqual([]);
  });

  test.each([
    ['520 días (límite del recorte anterior)', 520],
    ['521 días (antes se perdía 1)', 521],
    ['557 días (antes quedaban mal los 37 más antiguos)', 557],
  ])('QA-07 %s: se corrige entero, empezando por el más antiguo', async (_label, size) => {
    restore(BASES['inv-a']);
    moveHistoricalPrices();
    const end = TRADING_DAYS.indexOf(lastClosedOf(1));
    const since = TRADING_DAYS[end - size + 1];
    await registerLate('inv-a', 'late', { type: 'buy', asset: 'ko', name: 'KO', assetType: 'stock', amount: 7, date: since });
    const state = snapshotStore();

    const nights = await nightsUntilDone('inv-a');
    const tramos = nights.flatMap((night) => night.tramos);

    expect(tramos[0].desde).toBe(since);
    expect(tramos.findIndex((t) => t.status === 'reconciled')).toBe(tramos.length - 1);
    const lastClosed = lastClosedOf(nights[nights.length - 1].n);
    const rc = await referenceCorrection(state, 'inv-a', since, lastClosed);
    // El día más antiguo, el que el recorte perdía, también
    const oldest = `portfolioPerformance/inv-a/dates/${since}`;
    expect(differences({ [oldest]: historyOf('inv-a')[oldest] }, { [oldest]: rc[oldest] })).toEqual([]);
    expect(differences(historyOf('inv-a'), rc)).toEqual([]);
  });

  test('QA-08 y RN-6.8.1-M: el tramo del rango largo no impide corregir esa noche a los demás', async () => {
    restore(BASES['inv-a'], BASES['inv-b'], BASES['inv-e']);
    moveHistoricalPrices();
    const sinceA = tradingOffset(FROM['inv-a'], 0);
    await markPerformanceAsStale('inv-a', sinceA, { reason: 'asset_edit' });
    const n1 = await run({ n: 1, timeBudgetMs: ONE_TRAMO });
    const leftA = staleOf('inv-a').since;

    await registerLate('inv-b', 'late', { type: 'buy', asset: 'ko', name: 'KO', assetType: 'stock', amount: 5, date: '2026-08-12' });
    await registerLate('inv-e', 'late', { type: 'buy', asset: 'ko', name: 'KO', assetType: 'stock', amount: 3, date: '2026-10-01' });
    const state = snapshotStore();

    const n2 = await run({ n: 2 });

    expect(n1.tramos).toHaveLength(1);
    // Vuelta 1: un tramo a cada uno antes de que nadie tome un segundo
    expect(new Set(n2.tramos.slice(0, 3).map((t) => t.uid))).toEqual(new Set(['inv-a', 'inv-b', 'inv-e']));
    expect(n2.tramos.find((t) => t.uid === 'inv-a').desde).toBe(tradingOffset(leftA, 0));
    expect(staleOf('inv-b')).toBeUndefined();
    expect(staleOf('inv-e')).toBeUndefined();
    expect(staleOf('inv-a')).toBeDefined();
    expect(differences(historyOf('inv-b'), await referenceCorrection(state, 'inv-b', '2026-08-12', lastClosedOf(2)))).toEqual([]);
    expect(differences(historyOf('inv-e'), await referenceCorrection(state, 'inv-e', '2026-10-01', lastClosedOf(2)))).toEqual([]);
  });

  test('QA-12: relanzarla a mano continúa desde donde quedó y no toca a quien ya terminó', async () => {
    restore(BASES['inv-a'], BASES['inv-b'], BASES['inv-c']);
    moveHistoricalPrices();
    await markPerformanceAsStale('inv-a', tradingOffset(FROM['inv-a'], 0), { reason: 'asset_edit' });
    await registerLate('inv-b', 'late', { type: 'buy', asset: 'ko', name: 'KO', assetType: 'stock', amount: 5, date: '2026-11-11' });
    // HU 6.8.2: va primero el que más espera; B espera desde antes que A
    mockStore.docs.get('portfolioPerformance/inv-b')._stale.pendienteDesde = '2026-11-01T12:00:00.000Z';
    // B (un par de días) y el primer tramo de A
    const n1 = await run({ n: 1, timeBudgetMs: 10000 });
    expect(n1.tramos.map((t) => [t.uid, t.status])).toEqual([['inv-b', 'reconciled'], ['inv-a', 'advanced']]);
    const before = { b: historyOf('inv-b'), c: historyOf('inv-c') };

    const morning = DateTime.fromISO('2026-11-13T09:00:00', { zone: 'America/New_York' });
    mockStore.writes = [];
    const first = await run({ atTime: morning, timeBudgetMs: ONE_TRAMO });
    const second = await run({ atTime: morning.plus({ minutes: 30 }), timeBudgetMs: ONE_TRAMO });

    expect(first.tramos).toEqual([expect.objectContaining({ uid: 'inv-a', desde: tradingOffset(n1.tramos[1].hasta, 1) })]);
    expect(second.tramos).toEqual([expect.objectContaining({ uid: 'inv-a', desde: tradingOffset(first.tramos[0].hasta, 1) })]);
    expect(mockStore.writes.filter((p) => /inv-[bc]/.test(p))).toEqual([]);
    expect(historyOf('inv-b')).toEqual(before.b);
    expect(historyOf('inv-c')).toEqual(before.c);
  });

  test('QA-04 con la 6.8.2: con más pendientes de los que caben, va primero el que más espera y los que no alcanzan siguen marcados sin sumar intento', async () => {
    const sats = Array.from({ length: 12 }, (_, i) => `sat-${String(i + 1).padStart(2, '0')}`);
    restore(...sats.map((u) => BASES[u]), BASES['inv-b']);
    moveHistoricalPrices();
    for (const [i, uid] of sats.entries()) {
      await registerLate(uid, 'late', { type: 'buy', asset: 'ko', name: 'KO', assetType: 'stock', amount: 1, date: tradingOffset('2026-10-01', i) });
    }
    await registerLate('inv-b', 'late', { type: 'buy', asset: 'ko', name: 'KO', assetType: 'stock', amount: 5, date: '2026-08-12' });
    // Esperas explícitas: sat-01 el más antiguo, inv-b el más reciente (aunque su rango sea el más largo)
    [...sats, 'inv-b'].forEach((uid, i) => {
      mockStore.docs.get(`portfolioPerformance/${uid}`)._stale.pendienteDesde = `2026-11-${String(i + 1).padStart(2, '0')}T12:00:00.000Z`;
    });
    const queue = [...sats, 'inv-b'].map((uid) => ({ uid, since: staleOf(uid).since }));
    const expectedOrder = [...sats, 'inv-b'];

    const { tramos } = await run({ n: 1, timeBudgetMs: 150000 });

    const attended = tramos.map((t) => t.uid);
    expect(attended.length).toBeLessThan(queue.length);
    expect(attended).toEqual(expectedOrder.slice(0, attended.length));
    const notReached = expectedOrder.filter((uid) => !attended.includes(uid));
    expect(notReached).toContain('inv-b');
    notReached.forEach((uid) => {
      expect(staleOf(uid)).toMatchObject({ since: queue.find((q) => q.uid === uid).since, retryCount: 0 });
      expect(staleOf(uid).lastAttempt).toBeUndefined();
    });
  });
});

describe('HU 6.8.1 — avanzar no es un intento fallido (Escenario 4, RN-6.8.1-L)', () => {
  test.each([[3], [4]])('QA-09 con %i noches de avance: 0 intentos fallidos y nunca en revisión', async (nights) => {
    restore(BASES['inv-a']);
    moveHistoricalPrices();
    await markPerformanceAsStale('inv-a', tradingOffset(FROM['inv-a'], 0), { reason: 'asset_edit' });

    for (let n = 1; n <= nights; n++) {
      const { tramos } = await run({ n, timeBudgetMs: ONE_TRAMO });
      expect(tramos).toEqual([expect.objectContaining({ uid: 'inv-a', status: 'advanced' })]);
      expect(staleOf('inv-a').retryCount).toBe(0);
      expect(mockStore.docs.has('reconcileFailures/inv-a')).toBe(false);
    }
  });

  test('una noche con avance pone los intentos en 0, aunque vinieran de noches anteriores', async () => {
    restore(BASES['inv-a']);
    moveHistoricalPrices();
    await markPerformanceAsStale('inv-a', tradingOffset(FROM['inv-a'], 0), { reason: 'asset_edit' });
    await mockDb.collection('portfolioPerformance').doc('inv-a').update({ '_stale.retryCount': 2, '_stale.lastError': 'viejo' });

    await run({ n: 1, timeBudgetMs: ONE_TRAMO });

    expect(staleOf('inv-a')).toMatchObject({ retryCount: 0 });
    expect(staleOf('inv-a').lastError).toBeUndefined();
  });

  test('un fallo en una noche en la que ya avanzó no suma intento; sin avance, sí', async () => {
    restore(BASES['inv-a']);
    moveHistoricalPrices();
    await markPerformanceAsStale('inv-a', tradingOffset(FROM['inv-a'], 0), { reason: 'asset_edit' });

    // Noche 1: el primer tramo avanza y el segundo falla al escribir
    let tramoWrites = 0;
    let fired = false;
    mockStore.failCommit = (ops) => {
      if (!ops.some((op) => op.path.startsWith('portfolioPerformance/inv-a/dates/') && op.type === 'set')) return false;
      tramoWrites++;
      if (!fired && tramoWrites === LIMITS.DIAS_POR_TRAMO + 5) { fired = true; return true; }
      return false;
    };
    const n1 = await run({ n: 1 });
    expect(n1.tramos.map((t) => t.status)).toEqual(['advanced', 'failed']);
    expect(staleOf('inv-a')).toMatchObject({ retryCount: 0 });
    expect(staleOf('inv-a').lastError).toMatch(/forzado/);

    // Noche 2: falla el primer tramo de la noche, sin avance previo
    fired = false;
    tramoWrites = LIMITS.DIAS_POR_TRAMO;
    const n2 = await run({ n: 2 });
    expect(n2.tramos.map((t) => t.status)).toEqual(['failed']);
    expect(staleOf('inv-a')).toMatchObject({ retryCount: 1 });
  });
});

describe('HU 6.8.1 — varias operaciones, una sola corrección (Escenario 5)', () => {
  test('QA-10: cuatro cambios el mismo día se corrigen una vez, desde el más antiguo, sin recalcular ningún día dos veces', async () => {
    restore(BASES['inv-b']);
    moveHistoricalPrices();
    for (const [id, date] of [['op1', '2026-11-11'], ['op2', '2026-08-12'], ['op7', '2026-09-15'], ['op5', '2026-07-01']]) {
      await registerLate('inv-b', id, { type: 'buy', asset: `ko-${id}`, name: 'KO', assetType: 'stock', amount: 2, date });
    }
    expect(staleOf('inv-b').since).toBe('2026-07-01');
    const state = snapshotStore();
    mockStore.writes = [];

    const { tramos } = await run({ n: 1 });

    expect(tramos[0].desde).toBe('2026-07-01');
    const writesPerDay = {};
    mockStore.writes
      .filter((p) => p.startsWith('portfolioPerformance/inv-b/dates/') && isTradingDay(p.slice(-10)))
      .forEach((p) => { writesPerDay[p] = (writesPerDay[p] || 0) + 1; });
    expect(Object.keys(writesPerDay)).toHaveLength((await generateTradingDays('2026-07-01', lastClosedOf(1))).length);
    expect(Object.values(writesPerDay).every((count) => count === 1)).toBe(true);
    expect(staleOf('inv-b')).toBeUndefined();
    expect(differences(historyOf('inv-b'), await referenceCorrection(state, 'inv-b', '2026-07-01', lastClosedOf(1)))).toEqual([]);
  });
});

describe('HU 6.8.1 — un tramo que falla deja todo como estaba (Escenario 7, RN-6-H)', () => {
  async function afterFirstTramo() {
    restore(BASES['inv-a']);
    moveHistoricalPrices();
    const since = tradingOffset(FROM['inv-a'], 0);
    mockStore.docs.get('transactions/inv-a-b-spy').amount = 12;
    syncLots('inv-a');
    await markPerformanceAsStale('inv-a', since, { reason: 'asset_edit' });
    const state = snapshotStore();
    await run({ n: 1, timeBudgetMs: ONE_TRAMO });
    return { since, state };
  }

  test.each([
    ['a mitad de la escritura de los días', (failing) => (ops) => ops.some((op) => op.type === 'set' && op.path === failing)],
    ['al borrar los docs que el recálculo no produce', () => (ops) => ops.length > 0 && ops.every((op) => op.type === 'delete'
      && op.path.startsWith('portfolioPerformance/inv-a/'))],
  ])('QA-11 falla %s: las cifras quedan idénticas a antes del tramo y el siguiente intento lo rehace entero', async (_label, failWhen) => {
    const { since, state } = await afterFirstTramo();
    const f0 = historyOf('inv-a');
    const marker = mockClone(staleOf('inv-a'));
    const tramo = (await generateTradingDays(marker.since, lastClosedOf(2))).slice(0, LIMITS.DIAS_POR_TRAMO);
    const condition = failWhen(`portfolioPerformance/inv-a/dates/${tramo[30]}`);
    let fired = false;
    mockStore.failCommit = (ops) => {
      if (fired || !condition(ops)) return false;
      fired = true;
      return true;
    };

    const n2 = await run({ n: 2, timeBudgetMs: ONE_TRAMO });

    expect(fired).toBe(true);
    expect(n2.tramos).toEqual([expect.objectContaining({ status: 'failed', desde: tramo[0], hasta: tramo[tramo.length - 1] })]);
    // Sin días sueltos ni cuentas a medias: exactamente lo de antes
    expect(historyOf('inv-a')).toEqual(f0);
    expect(staleOf('inv-a')).toMatchObject({ since: marker.since, retryCount: 1 });

    mockStore.failCommit = null;
    const n3 = await run({ n: 3, timeBudgetMs: ONE_TRAMO });
    expect(n3.tramos).toEqual([expect.objectContaining({ status: 'advanced', desde: tramo[0], hasta: tramo[tramo.length - 1] })]);

    const rest = await nightsUntilDone('inv-a', { firstNight: 4 });
    const lastClosed = lastClosedOf(rest[rest.length - 1].n);
    expect(differences(historyOf('inv-a'), await referenceCorrection(state, 'inv-a', since, lastClosed))).toEqual([]);
  });
});

describe('HU 6.8.1 — operaciones nuevas a mitad de un rango (Escenario 8, RN-6.8.1-O)', () => {
  const RLA = '2025-09-02';

  async function rangeInProgress() {
    restore(BASES['inv-a']);
    moveHistoricalPrices();
    await registerLate('inv-a', 'rla', { type: 'buy', asset: 'spy', name: 'SPY', amount: 2, date: RLA });
    const n1 = await run({ n: 1, timeBudgetMs: ONE_TRAMO });
    expect(n1.tramos).toEqual([expect.objectContaining({ desde: RLA, status: 'advanced' })]);
    return n1.tramos[0].hasta;
  }

  test('QA-21: una operación anterior al inicio del rango hace seguir desde su fecha, aunque repita tramos ya hechos', async () => {
    await rangeInProgress();
    await registerLate('inv-a', 'op-a2', { type: 'buy', asset: 'ko', name: 'KO', assetType: 'stock', amount: 12, date: '2025-07-14' });
    const state = snapshotStore();

    const nights = await nightsUntilDone('inv-a', { firstNight: 2 });

    expect(nights[0].tramos[0].desde).toBe('2025-07-14');
    const lastClosed = lastClosedOf(nights[nights.length - 1].n);
    expect(differences(historyOf('inv-a'), await referenceCorrection(state, 'inv-a', '2025-07-14', lastClosed))).toEqual([]);
  });

  test('QA-22: una operación dentro de lo ya corregido rehace desde ella sin tocar los días anteriores', async () => {
    const d1 = await rangeInProgress();
    expect(d1 > '2025-10-01').toBe(true);
    const f1 = Object.fromEntries(Object.entries(historyOf('inv-a'))
      .filter(([p]) => p.slice(-10) >= RLA && p.slice(-10) <= '2025-09-30'));
    await registerLate('inv-a', 'op-a3', { type: 'buy', asset: 'msft', name: 'MSFT', assetType: 'stock', amount: 5, date: '2025-10-01' });
    const state = snapshotStore();

    const nights = await nightsUntilDone('inv-a', { firstNight: 2 });

    expect(nights[0].tramos[0].desde).toBe('2025-10-01');
    const after = historyOf('inv-a');
    Object.keys(f1).forEach((p) => expect(after[p]).toEqual(f1[p]));
    const lastClosed = lastClosedOf(nights[nights.length - 1].n);
    // RC desde RLA, con las dos operaciones
    expect(differences(after, await referenceCorrection(state, 'inv-a', RLA, lastClosed))).toEqual([]);
  });

  test('una operación que llega durante el tramo no lo deja avanzar: se sigue desde la fecha más antigua afectada', async () => {
    restore(BASES['inv-a']);
    moveHistoricalPrices();
    await registerLate('inv-a', 'rla', { type: 'buy', asset: 'spy', name: 'SPY', amount: 2, date: RLA });
    let commits = 0;
    mockStore.afterCommit = async () => {
      if (++commits !== 10) return;
      mockStore.afterCommit = null;
      await registerLate('inv-a', 'durante', { type: 'buy', asset: 'ko', name: 'KO', assetType: 'stock', amount: 4, date: '2025-08-04' });
    };

    const n1 = await run({ n: 1, timeBudgetMs: ONE_TRAMO });
    expect(n1.tramos).toEqual([expect.objectContaining({ desde: RLA, status: 'moved' })]);
    expect(staleOf('inv-a')).toMatchObject({ since: '2025-08-04', retryCount: 0 });

    const n2 = await run({ n: 2, timeBudgetMs: ONE_TRAMO });
    expect(n2.tramos[0]).toMatchObject({ desde: '2025-08-04', status: 'advanced' });
  });
});

describe('HU 6.8.1 — lo que se propaga y lo que se registra por tramo', () => {
  test('solo se rehacen los meses cerrados del tramo y se borran los snapshots del inversor, no los de otros', async () => {
    restore(BASES['inv-a']);
    moveHistoricalPrices();
    mockStore.docs.set('performanceSnapshots/inv-a_overall_USD', { timeline: { a: 1 } });
    mockStore.docs.set('performanceSnapshots/inv-a_inv-a-usd_USD', { timeline: { a: 1 } });
    mockStore.docs.set('performanceSnapshots/otro_overall_USD', { timeline: { a: 1 } });
    const since = tradingOffset(FROM['inv-a'], 0);
    await markPerformanceAsStale('inv-a', since, { reason: 'asset_edit' });

    const { tramos } = await run({ n: 1, timeBudgetMs: ONE_TRAMO });

    const months = [...new Set(consolidateMonth.mock.calls.map((c) => c[2]))];
    expect(months).toEqual(calendar(since, tramos[0].hasta).map((d) => d.slice(0, 7)).filter((m, i, all) => all.indexOf(m) === i));
    expect(mockStore.docs.has('performanceSnapshots/inv-a_overall_USD')).toBe(false);
    expect(mockStore.docs.has('performanceSnapshots/inv-a_inv-a-usd_USD')).toBe(false);
    expect(mockStore.docs.has('performanceSnapshots/otro_overall_USD')).toBe(true);
  });

  test('RN-6-I: una línea por tramo con sus límites y lo que queda, y el resumen de la noche', async () => {
    restore(BASES['inv-a']);
    moveHistoricalPrices();
    await markPerformanceAsStale('inv-a', tradingOffset(FROM['inv-a'], 0), { reason: 'asset_edit' });

    const { tramos } = await run({ n: 1, timeBudgetMs: ONE_TRAMO });

    const line = logs.find((l) => l.includes('[reconcile] TRAMO inv-a'));
    expect(line).toMatch(new RegExp(`advanced — queda desde ${staleOf('inv-a').since}$`));
    expect(tramos[0].days).toBe(LIMITS.DIAS_POR_TRAMO);
    expect(logs.find((l) => l.includes('[reconcile] DONE'))).toMatch(/1 vuelta\(s\), 1 tramo\(s\) — 0 completados, 1 avanzaron/);
  });
});
