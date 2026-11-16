/**
 * HU 6.11.6 — El detalle de un activo en una moneda poco usada sale del resumen
 * en USD.
 *
 * Con los módulos reales (conversión de la 6.11.3, generador, puesta al día de
 * la 6.4, días del activo) sobre un Firestore en memoria que cuenta lo que se
 * lee y se escribe:
 * - Con el resumen en USD preparado: una lectura, ningún documento diario,
 *   ninguna escritura, nada de lo guardado en la moneda, y la misma respuesta que
 *   la 6.11.3 (Escenarios 1 y 2; QA-01, QA-02).
 * - Sin resumen en USD: se prepara una vez, también con dos aperturas a la vez, y
 *   la siguiente lee uno (Escenario 4, RN-6.11.6-J; QA-05).
 * - Un resumen en USD anterior a la HU se completa una vez sin tocar sus cifras,
 *   al día o viejo (RN-6.11.6-A; QA-06).
 * - Si el resumen en USD no se puede usar, la cifra de hoy desde los días en USD,
 *   sin aviso (Escenario 5, RN-6.11.6-D; QA-07).
 * - Varias cuentas: una lectura por cuenta; solo la que falla lee sus días
 *   (Escenario 6, RN-6.11.6-E, I; QA-09).
 * - USD y la moneda principal no cambian (Escenario 8, QA-12) y el interruptor
 *   apagado devuelve la 6.11.3 (Escenario 9, QA-13).
 *
 * @see platform-docs/stories/6.11.6-detalle-activo-desde-resumen-usd/refinamiento.md (T8c)
 */

const { DateTime, Settings } = require('luxon');

jest.mock('firebase-admin', () => ({
  firestore: Object.assign(jest.fn(() => ({})), {
    FieldValue: { serverTimestamp: jest.fn(() => 'mock-server-timestamp') },
  }),
  initializeApp: jest.fn(),
}));

jest.mock('firebase-functions/v2/https', () => ({
  HttpsError: class HttpsError extends Error {
    constructor(code, message, details) {
      super(message);
      this.code = code;
      this.details = details;
      this.httpErrorCode = { canonicalName: code };
    }
  },
  onCall: jest.fn((opciones, fn) => fn || opciones),
}));

jest.mock('../../cacheInvalidationService', () => ({
  calculateDynamicTTL: jest.fn(() => new Date('2026-11-14T23:59:59Z')),
}));
const mockLegacy = jest.fn();
jest.mock('../../historicalReturnsService', () => ({
  ...jest.requireActual('../../historicalReturnsService'),
  getHistoricalReturnsInternal: (...args) => mockLegacy(...args),
}));
jest.mock('../../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: (...args) => mockLegacy(...args),
  checkConsolidatedDataStatus: jest.fn(),
}));
jest.mock('../../portfolioDistributionService', () => ({}));
jest.mock('../../indexHistoryService', () => ({ calculateIndexData: jest.fn() }));
jest.mock('../../marketDataHelper', () => ({ getPricesFromApi: jest.fn() }));
jest.mock('../../../utils/mwrCalculations', () => ({
  calculateSimplePersonalReturn: jest.fn(),
  calculateModifiedDietzReturn: jest.fn(),
}));
jest.mock('../../riskMetrics/riskMetricsCache', () => ({
  isNYSEMarketOpen: jest.fn(() => false),
  calculateTTLUntilNextEOD: jest.fn(() => 86400000),
  MARKET_CACHE_TTL_MS: 300000,
}));
const mockGetExchangeRates = jest.fn();
jest.mock('../../financeQuery', () => ({
  getExchangeRates: (...args) => mockGetExchangeRates(...args),
}));

// ============================================================================
// Firestore en memoria: documentos, días por alcance, lecturas y escrituras
// ============================================================================

const mockDocs = {};
const mockDias = {};
const mockLecturas = { documentos: [], dias: [], campos: [] };
const mockEscrituras = [];
const mockFallarEscritura = { patron: null };

jest.mock('../../firebaseAdmin', () => {
  const copia = (x) => (x === undefined ? undefined : JSON.parse(JSON.stringify(x)));
  const doc = (ruta) => ({
    get: async () => {
      mockLecturas.documentos.push(ruta);
      const data = mockDocs[ruta];
      return { exists: data !== undefined, data: () => copia(data) };
    },
    set: async (data, opciones) => {
      if (mockFallarEscritura.patron && ruta.includes(mockFallarEscritura.patron)) {
        throw new Error('DEADLINE_EXCEEDED');
      }
      mockEscrituras.push({ ruta, campos: Object.keys(data), merge: !!opciones?.merge });
      mockDocs[ruta] = opciones?.merge ? { ...mockDocs[ruta], ...copia(data) } : copia(data);
    },
  });
  const proyectar = (d, campos) => {
    if (!campos) return d;
    const out = { date: d.date };
    for (const campo of campos) {
      const partes = String(campo).split('.');
      if (partes.length === 1) continue;
      let origen = d;
      for (const p of partes) origen = origen?.[p];
      if (origen === undefined) continue;
      let destino = out;
      partes.slice(0, -1).forEach((p) => { destino[p] = destino[p] || {}; destino = destino[p]; });
      destino[partes[partes.length - 1]] = copia(origen);
    }
    return out;
  };
  const consultaDias = (ruta, filtro = null, campos = null) => ({
    where: (campo, op, valor) => consultaDias(ruta, (d) => d.date > valor, campos),
    select: (...fs) => consultaDias(ruta, filtro, fs.map(String)),
    orderBy: () => consultaDias(ruta, filtro, campos),
    get: async () => {
      // Una vuelta al bucle: dos aperturas a la vez llegan a la misma preparación.
      await new Promise((r) => setImmediate(r));
      const dias = (mockDias[ruta] || []).filter((d) => !filtro || filtro(d));
      dias.forEach(() => mockLecturas.dias.push(ruta));
      mockLecturas.campos.push(...(campos || ['*']));
      const docs = dias.map((d) => ({ id: d.date, data: () => proyectar(copia(d), campos) }));
      return { docs, size: docs.length, empty: docs.length === 0 };
    },
  });
  const consultaCuentas = {
    where: () => consultaCuentas,
    get: async () => ({ docs: ['acc-a', 'acc-b', 'acc-c'].map((id) => ({ id, data: () => ({}) })) }),
  };
  const collection = (nombre) => {
    if (nombre === 'portfolioAccounts') return consultaCuentas;
    if (nombre.endsWith('/dates')) return consultaDias(nombre);
    return { doc: (id) => doc(`${nombre}/${id}`) };
  };
  const mockDb = { doc, collection };
  return { firestore: () => mockDb, ...mockDb };
});

const {
  getHistoricalReturns,
  getMultiAccountHistoricalReturns,
  clearSnapshotMemCache,
} = require('../queryHandlers');
const { limpiarCachesDerivados } = require('../../snapshots/derivedCurrencySnapshotService');

// ============================================================================
// Datos
// ============================================================================

const UID = 'uid-inversor-A';
const SENAL = '2026-11-14T05:05:00.000Z';
const ANTES_DE_LA_SENAL = '2026-11-13T05:05:00.000Z';
const AHORA = DateTime.fromISO('2026-11-14T10:00:00', { zone: 'America/New_York' });
const NOW_ORIGINAL = Settings.now;
const CLAVE = 'MSFT_stock';
/** GUARD-X: lo guardado en la moneda poco usada; aparecer en una cifra es un error. */
const MARCA = 1111111111.11;

const activo = (overrides = {}) => ({
  totalValue: 8964,
  totalInvestment: 8000,
  totalCashFlow: 0,
  doneProfitAndLoss: 0,
  unrealizedProfitAndLoss: 964,
  adjustedDailyChangePercentage: 0.37,
  dailyChangePercentage: 0.41,
  units: 20,
  totalROI: 12.05,
  ...overrides,
});

/** Un documento diario completo: USD, la principal (COP) y EUR guardado con la marca. */
const dia = (date, msft, escala = 1) => {
  const bloque = (f) => ({
    totalValue: 50000 * f,
    totalInvestment: 45000 * f,
    adjustedDailyChangePercentage: 0.2,
    assetPerformance: {
      AAPL_stock: activo({ totalValue: 3000 * f }),
      ...(msft ? { [CLAVE]: { ...msft, totalValue: msft.totalValue * f } } : {}),
    },
  });
  return {
    date,
    USD: bloque(escala),
    COP: bloque(4000 * escala),
    EUR: { totalValue: MARCA, assetPerformance: msft ? { [CLAVE]: activo({ totalValue: MARCA }) } : {} },
  };
};

/** ACT-MSFT de `qa.md` en pequeño: compra, venta, recompra, festivo y cierres de noviembre. */
const diasDe = (escala = 1) => [
  dia('2026-01-05', activo({ totalCashFlow: -8000, adjustedDailyChangePercentage: 0 }), escala),
  dia('2026-01-06', activo({ totalValue: 9050.33, adjustedDailyChangePercentage: 0.9596 }), escala),
  dia('2026-02-03', null, escala),
  dia('2026-02-04', null, escala),
  dia('2026-06-12', activo({ totalValue: 7576.5, units: 15, totalCashFlow: -7000 }), escala),
  dia('2026-06-15', activo({ totalValue: 7651.2, units: 15, adjustedDailyChangePercentage: 0.986 }), escala),
  dia('2026-06-16', activo({ totalValue: 7400.07, units: 14, doneProfitAndLoss: 93.15, totalCashFlow: 490.4 }), escala),
  dia('2026-11-12', activo({ totalValue: 8120.4, units: 14, adjustedDailyChangePercentage: 1.1 }), escala),
  dia('2026-11-13', activo({ totalValue: 8170.11, units: 14, adjustedDailyChangePercentage: 0.612 }), escala),
];

const EUR = {
  '2026-01-05': 0.86, '2026-01-06': 0.861, '2026-02-03': 0.85, '2026-02-04': 0.852, '2026-06-12': 0.87,
  '2026-06-16': 0.872, '2026-11-12': 0.92, '2026-11-13': 0.921,
};

const RUTA_DIAS = {
  overall: `portfolioPerformance/${UID}/dates`,
  'acc-a': `portfolioPerformance/${UID}/accounts/acc-a/dates`,
  'acc-b': `portfolioPerformance/${UID}/accounts/acc-b/dates`,
};
const resumenUsd = (accountId = 'overall') => `performanceSnapshots/${UID}_MSFT_stock_${accountId === 'overall' ? '' : `${accountId}_`}USD`;

const contexto = { auth: { uid: UID } };
const detalle = (currency, accountId = 'overall') => getHistoricalReturns(
  contexto, { currency, accountId, ticker: 'MSFT', assetType: 'stock' },
);
const variasCuentas = (currency) => getMultiAccountHistoricalReturns(
  contexto, { currency, accountIds: ['acc-a', 'acc-b'], ticker: 'MSFT', assetType: 'stock' },
);

/** La respuesta sin las horas de cálculo (también la del resumen calculado). */
const sinHoras = ({ lastCalculated: _a, validUntil: _b, _metadata, ...resto }) => {
  if (!_metadata) return resto;
  const { snapshotLastUpdated: _c, ...metadata } = _metadata;
  return { ...resto, _metadata: metadata };
};

/** Otra instancia: sin nada en memoria y con los contadores en cero. */
function nuevaInstancia() {
  clearSnapshotMemCache();
  limpiarCachesDerivados();
  mockLecturas.documentos.length = 0;
  mockLecturas.dias.length = 0;
  mockLecturas.campos.length = 0;
  mockEscrituras.length = 0;
}

/** La respuesta de hoy (6.11.3), con el interruptor de la HU apagado. */
async function respuestaDeHoy(abrir) {
  process.env.RARE_ASSET_FROM_USD_SUMMARY = 'false';
  try {
    nuevaInstancia();
    return sinHoras(await abrir());
  } finally {
    delete process.env.RARE_ASSET_FROM_USD_SUMMARY;
    nuevaInstancia();
  }
}

/** Prepara el resumen en USD del alcance abriéndolo en USD, como el inversor. */
async function prepararEnUsd(accountId = 'overall') {
  await detalle('USD', accountId);
  nuevaInstancia();
}

const lecturasDeEur = () => [
  ...mockLecturas.documentos.filter((r) => r.endsWith('_EUR')),
  ...mockLecturas.campos.filter((c) => c === '*' || c.startsWith('EUR')),
];

beforeEach(() => {
  jest.clearAllMocks();
  Object.keys(mockDocs).forEach((k) => delete mockDocs[k]);
  mockDocs[`portfolioPerformance/${UID}`] = { lastSnapshotUpdate: SENAL };
  mockDocs[`userData/${UID}`] = { defaultCurrency: 'COP' };
  // GUARD-X: resúmenes por activo guardados en EUR, con la marca.
  mockDocs[`performanceSnapshots/${UID}_MSFT_stock_EUR`] = { type: 'asset', currency: 'EUR', timeline: [{ d: '2026-11-13', v: MARCA, c: 0, u: 14 }], lastUpdated: SENAL };
  mockDias[RUTA_DIAS.overall] = diasDe(1);
  mockDias[RUTA_DIAS['acc-a']] = diasDe(0.6);
  mockDias[RUTA_DIAS['acc-b']] = diasDe(0.4);
  mockFallarEscritura.patron = null;
  mockLegacy.mockResolvedValue({ returns: { ytdReturn: -99.98 } });
  mockGetExchangeRates.mockResolvedValue({ rates: { EUR } });
  delete process.env.RARE_ASSET_FROM_USD_SUMMARY;
  Settings.now = () => AHORA.toMillis();
  nuevaInstancia();
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  jest.restoreAllMocks();
});

afterAll(() => {
  Settings.now = NOW_ORIGINAL;
});

// ============================================================================
// UNA LECTURA Y LAS MISMAS CIFRAS
// ============================================================================

describe('HU 6.11.6: con el resumen en USD preparado', () => {
  it('una lectura del resumen, ningún día, ninguna escritura y nada guardado en EUR (Escenario 1, QA-01)', async () => {
    await prepararEnUsd();

    await detalle('EUR');

    expect(mockLecturas.dias).toEqual([]);
    expect(mockEscrituras).toEqual([]);
    expect(mockLecturas.documentos.filter((r) => r.startsWith('performanceSnapshots/'))).toEqual([resumenUsd()]);
    expect(lecturasDeEur()).toEqual([]);
  });

  it('la misma respuesta que hoy en todos los decimales (Escenario 2, RN-6.11.6-H, QA-02, QA-11)', async () => {
    const hoy = await respuestaDeHoy(() => detalle('EUR'));
    await prepararEnUsd();

    const nuevo = sinHoras(await detalle('EUR'));

    expect(JSON.stringify(nuevo)).toBe(JSON.stringify(hoy));
    expect(nuevo.totalValueData.values).not.toContain(MARCA);
    expect(nuevo.totalValueData.values[nuevo.totalValueData.values.length - 1]).toBe(8170.11 * 0.921);
  });

  it('también en una cuenta', async () => {
    const hoy = await respuestaDeHoy(() => detalle('EUR', 'acc-a'));
    await prepararEnUsd('acc-a');

    const nuevo = sinHoras(await detalle('EUR', 'acc-a'));

    expect(mockLecturas.dias).toEqual([]);
    expect(JSON.stringify(nuevo)).toBe(JSON.stringify(hoy));
  });
});

// ============================================================================
// PREPARAR O COMPLETAR UNA VEZ
// ============================================================================

describe('HU 6.11.6: el resumen en USD se prepara o se completa una vez (RN-6.11.6-A, B, J)', () => {
  it('sin resumen en USD: la primera apertura lo prepara y la siguiente lee uno (QA-05)', async () => {
    const hoy = await respuestaDeHoy(() => detalle('EUR'));

    const primera = sinHoras(await detalle('EUR'));

    expect(mockLecturas.dias).toHaveLength(9);
    expect(mockEscrituras).toEqual([{ ruta: resumenUsd(), campos: expect.arrayContaining(['assetDays', 'timeline']), merge: false }]);
    expect(JSON.stringify(primera)).toBe(JSON.stringify(hoy));

    nuevaInstancia();
    await detalle('EUR');
    expect(mockLecturas.dias).toEqual([]);
    expect(mockEscrituras).toEqual([]);
  });

  it('dos aperturas a la vez preparan una sola vez', async () => {
    const [a, b] = await Promise.all([detalle('EUR'), detalle('EUR')]);

    expect(mockLecturas.dias).toHaveLength(9);
    expect(mockEscrituras).toHaveLength(1);
    expect(sinHoras(a)).toEqual(sinHoras(b));
  });

  it('uno al día anterior a la HU se completa una vez y sin tocar sus cifras en USD (QA-06, Escenario 8)', async () => {
    await prepararEnUsd();
    const { assetDays: _, ...anterior } = mockDocs[resumenUsd()];
    mockDocs[resumenUsd()] = anterior;
    const hoy = await respuestaDeHoy(() => detalle('EUR'));

    const primera = sinHoras(await detalle('EUR'));

    expect(mockLecturas.dias).toHaveLength(9);
    expect(mockEscrituras).toEqual([{ ruta: resumenUsd(), campos: ['assetDays'], merge: true }]);
    const { assetDays, ...resto } = mockDocs[resumenUsd()];
    expect(resto).toEqual(anterior);
    expect(assetDays).toHaveLength(9);
    expect(JSON.stringify(primera)).toBe(JSON.stringify(hoy));

    nuevaInstancia();
    await detalle('EUR');
    expect(mockLecturas.dias).toEqual([]);
  });

  it('uno viejo anterior a la HU se pone al día y se completa con una sola lectura de los días (QA-06)', async () => {
    mockDias[RUTA_DIAS.overall] = diasDe(1).slice(0, -1);
    await prepararEnUsd();
    const { assetDays: _, ...anterior } = mockDocs[resumenUsd()];
    mockDocs[resumenUsd()] = { ...anterior, lastUpdated: ANTES_DE_LA_SENAL };
    mockDias[RUTA_DIAS.overall] = diasDe(1);
    const hoy = await respuestaDeHoy(() => detalle('EUR'));

    const primera = sinHoras(await detalle('EUR'));

    expect(mockLecturas.dias).toHaveLength(9);
    expect(mockEscrituras).toHaveLength(1);
    expect(mockDocs[resumenUsd()].lastDateInTimeline).toBe('2026-11-13');
    expect(mockDocs[resumenUsd()].assetDays).toHaveLength(9);
    expect(JSON.stringify(primera)).toBe(JSON.stringify(hoy));
  });

  it('uno viejo con sus días se pone al día leyendo solo los días nuevos (QA-08)', async () => {
    mockDias[RUTA_DIAS.overall] = diasDe(1).slice(0, -1);
    await prepararEnUsd();
    mockDocs[resumenUsd()].lastUpdated = ANTES_DE_LA_SENAL;
    mockDias[RUTA_DIAS.overall] = diasDe(1);
    const hoy = await respuestaDeHoy(() => detalle('EUR'));

    const primera = sinHoras(await detalle('EUR'));

    expect(mockLecturas.dias).toHaveLength(1);
    expect(mockDocs[resumenUsd()].assetDays).toHaveLength(9);
    expect(JSON.stringify(primera)).toBe(JSON.stringify(hoy));
  });
});

// ============================================================================
// SI EL RESUMEN NO SE PUEDE USAR
// ============================================================================

describe('HU 6.11.6: si el resumen en USD no se puede usar (RN-6.11.6-D)', () => {
  it('falla al prepararlo: la cifra de hoy desde los días en USD, sin aviso y sin nada guardado en EUR (QA-07)', async () => {
    const hoy = await respuestaDeHoy(() => detalle('EUR'));
    mockFallarEscritura.patron = 'MSFT_stock_USD';

    const r = sinHoras(await detalle('EUR'));

    expect(JSON.stringify(r)).toBe(JSON.stringify(hoy));
    expect(lecturasDeEur()).toEqual([]);
    expect(mockLegacy).not.toHaveBeenCalled();
  });

  it('sin tasas, el aviso de la 6.11.1 igual que hoy (Escenario 3, QA-04)', async () => {
    await prepararEnUsd();
    mockGetExchangeRates.mockResolvedValue(null);

    await expect(detalle('EUR')).rejects.toMatchObject({ code: 'unavailable', details: { code: 'CURRENCY_UNAVAILABLE' } });
    expect(mockLecturas.dias).toEqual([]);
  });
});

// ============================================================================
// VARIAS CUENTAS
// ============================================================================

describe('HU 6.11.6: varias cuentas (RN-6.11.6-E, I)', () => {
  it('una lectura por cuenta y la misma respuesta que hoy (QA-09)', async () => {
    const hoy = await respuestaDeHoy(() => variasCuentas('EUR'));
    await prepararEnUsd('acc-a');
    await prepararEnUsd('acc-b');

    const nuevo = sinHoras(await variasCuentas('EUR'));

    expect(mockLecturas.dias).toEqual([]);
    expect(mockLecturas.documentos.filter((r) => r.startsWith('performanceSnapshots/')).sort())
      .toEqual([resumenUsd('acc-a'), resumenUsd('acc-b')]);
    expect(JSON.stringify(nuevo)).toBe(JSON.stringify(hoy));
  });

  it('solo la cuenta cuyo resumen no se puede usar lee sus días', async () => {
    const hoy = await respuestaDeHoy(() => variasCuentas('EUR'));
    await prepararEnUsd('acc-a');
    mockFallarEscritura.patron = 'MSFT_stock_acc-b_USD';

    const nuevo = sinHoras(await variasCuentas('EUR'));

    expect(mockLecturas.dias.every((ruta) => ruta === RUTA_DIAS['acc-b'])).toBe(true);
    expect(mockLecturas.dias.length).toBeGreaterThan(0);
    expect(JSON.stringify(nuevo)).toBe(JSON.stringify(hoy));
  });
});

// ============================================================================
// USD, LA PRINCIPAL Y VOLVER ATRÁS
// ============================================================================

describe('HU 6.11.6: USD y la moneda principal no cambian; el interruptor vuelve a la 6.11.3', () => {
  it('en USD el detalle sale de su resumen como siempre: una lectura, sin días ni escrituras (Escenario 8, QA-12)', async () => {
    await prepararEnUsd();

    const r = await detalle('USD');

    expect(mockLecturas.dias).toEqual([]);
    expect(mockEscrituras).toEqual([]);
    expect(r.totalValueData.values[r.totalValueData.values.length - 1]).toBe(8170.11);
  });

  it('en la principal (COP) sigue su resumen en COP, sin tocar el de USD (QA-12)', async () => {
    await detalle('COP');

    expect(mockLecturas.documentos).not.toContain(resumenUsd());
    expect(mockEscrituras.map((e) => e.ruta)).toEqual([`performanceSnapshots/${UID}_MSFT_stock_COP`]);
    expect(mockEscrituras[0].campos).not.toContain('assetDays');
  });

  it('con RARE_ASSET_FROM_USD_SUMMARY=false vuelve el cálculo desde los días y no lee el resumen en USD (Escenario 9, QA-13)', async () => {
    await prepararEnUsd();
    process.env.RARE_ASSET_FROM_USD_SUMMARY = 'false';

    await detalle('EUR');

    expect(mockLecturas.dias).toHaveLength(9);
    expect(mockLecturas.documentos).not.toContain(resumenUsd());
    expect(lecturasDeEur()).toEqual([]);
    expect(mockDocs[resumenUsd()].assetDays).toHaveLength(9);
  });
});
