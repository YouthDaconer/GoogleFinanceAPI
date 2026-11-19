/**
 * HU 6.13 — Se retira la ruta antigua de rendimientos: si el resumen no se puede
 * preparar, el aviso de rendimientos no disponibles por ahora.
 *
 * - Portafolio, cuenta o activo sin resumen preparable: el aviso
 *   `RETURNS_UNAVAILABLE` con la pantalla y el motivo en el registro, sin llamar
 *   a la ruta antigua (V2) ni leer ni escribir su caché (Escenario 3, RN-6.13-A;
 *   QA-04, QA-05, QA-07).
 * - El aviso no se recuerda: la siguiente apertura vuelve a preparar el resumen
 *   (QA-06).
 * - Con el resumen preparado, la respuesta es la misma con el interruptor
 *   encendido que apagado (Escenario 2, RN-6-A). `forceRefresh` ya no se salta
 *   los resúmenes.
 * - Con RETIRE_CONSOLIDATED_PERIODS=false, la ruta antigua como antes (Escenario 7,
 *   RN-6-H; QA-12).
 * - Las acciones `getHistoricalReturnsOptimized` y `getConsolidatedDataStatus` ya
 *   no existen: el router las rechaza como acción no válida (Escenario 4, QA-09).
 *
 * @see platform-docs/stories/6.13-retirar-ruta-antigua-consolidados/refinamiento.md (T2, T7a)
 */

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
  // El router se prueba llamando directamente a su función.
  onCall: (_opciones, fn) => fn,
}));
jest.mock('firebase-functions/params', () => ({ defineSecret: (nombre) => ({ name: nombre }) }));
jest.mock('../../../utils/rateLimiter', () => ({ rateLimiter: { checkLimit: jest.fn(async () => ({})) } }));
jest.mock('../../../config/rateLimits', () => ({ getRateLimitConfig: () => ({}) }));
jest.mock('../performanceOnDemandHandler', () => ({ getPerformanceOnDemand: jest.fn() }));

jest.mock('../../cacheInvalidationService', () => ({
  calculateDynamicTTL: jest.fn(() => new Date('2026-11-18T23:59:59Z')),
}));
jest.mock('../../historicalReturnsService', () => ({
  calculateHistoricalReturns: jest.fn(),
  getHistoricalReturnsInternal: jest.fn(),
}));
const mockLegacyV2 = jest.fn();
jest.mock('../../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: (...args) => mockLegacyV2(...args),
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

const mockGeneratePerformanceSnapshot = jest.fn();
const mockGenerateAssetSnapshot = jest.fn();
jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: (userId, accountId, currency, ticker, assetType) => {
    const base = accountId === 'overall' ? `${userId}_${currency}` : `${userId}_${accountId}_${currency}`;
    return ticker ? `${base}_${ticker}_${assetType}` : base;
  },
  generatePerformanceSnapshot: (...args) => mockGeneratePerformanceSnapshot(...args),
  generateAssetSnapshot: (...args) => mockGenerateAssetSnapshot(...args),
  fetchAssetDailyDocs: jest.fn(),
}));
jest.mock('../../snapshots/assetSnapshotOnDemandService', () => ({
  resumenesPorActivoBajoDemanda: () => true,
  ponerAlDiaResumenDeActivo: jest.fn(),
  completarDiasDelActivo: jest.fn(),
  monedasDelCierre: (moneda) => (moneda && moneda !== 'USD' ? ['USD', moneda] : ['USD']),
}));

// Firestore: cada ruta de documento devuelve lo que diga `mockDocumentos`; cada
// lectura y cada escritura quedan anotadas.
const mockDocumentos = {};
const mockLecturas = [];
const mockEscrituras = [];
jest.mock('../../firebaseAdmin', () => {
  const doc = (ruta) => ({
    get: async () => {
      mockLecturas.push(ruta);
      const data = mockDocumentos[ruta];
      return { exists: data !== undefined, data: () => data };
    },
    set: async () => { mockEscrituras.push(ruta); },
  });
  const coleccion = (nombre) => {
    const consulta = {
      where: () => consulta,
      orderBy: () => consulta,
      get: async () => { mockLecturas.push(nombre); return { empty: true, docs: [] }; },
    };
    return { doc: (id) => doc(`${nombre}/${id}`), ...consulta };
  };
  const mockDb = { doc, collection: coleccion };
  return { firestore: () => mockDb, ...mockDb };
});

const { getHistoricalReturns, clearSnapshotMemCache } = require('../queryHandlers');
const { queryOperations, VALID_ACTIONS } = require('../../unified/queryOperations');

// ============================================================================
// Datos
// ============================================================================

const UID = 'uid-inversor-F';
const contexto = { auth: { uid: UID } };
const SENAL = '2026-11-18T05:05:00.000Z';

/** Fábrica local: un resumen de portafolio, de cuenta o de activo. */
const resumen = (overrides = {}) => ({
  userId: UID,
  currency: 'COP',
  accountId: 'overall',
  schemaVersion: 3,
  lastUpdated: '2026-11-18T06:00:00.000Z',
  timeline: [{ d: '2026-11-16', v: 18000, c: 0 }, { d: '2026-11-17', v: 18240.57, c: 1.34 }],
  returns: { ytdReturn: 9.75, oneYearReturn: 12.69, hasYtdData: true },
  validDocsCountByPeriod: { ytd: 220 },
  performanceByYear: { 2026: { total: 9.75 } },
  monthlyCompound: {},
  availableYears: ['2026'],
  startDate: '2025-05-07',
  ...overrides,
});

/** Una respuesta que guardó la ruta antigua el 2026-11-20, antes de publicar (QA-07). */
const RESPUESTA_ANTIGUA = {
  data: { returns: { ytdReturn: 4.44 }, _metadata: { version: 'v2' } },
  lastCalculated: '2026-11-20T10:00:00.000Z',
  validUntil: '2099-01-01T00:00:00.000Z',
};

const LEGACY = { returns: { ytdReturn: 4.44 }, totalValueData: { dates: [], values: [] }, _metadata: { version: 'v2' } };

let avisos;

beforeEach(() => {
  for (const k of Object.keys(mockDocumentos)) delete mockDocumentos[k];
  mockLecturas.length = 0;
  mockEscrituras.length = 0;
  mockLegacyV2.mockReset().mockResolvedValue(LEGACY);
  mockGeneratePerformanceSnapshot.mockReset().mockResolvedValue(null);
  mockGenerateAssetSnapshot.mockReset().mockResolvedValue(null);
  mockDocumentos[`portfolioPerformance/${UID}`] = { lastSnapshotUpdate: SENAL };
  mockDocumentos[`userData/${UID}`] = { defaultCurrency: 'COP' };
  clearSnapshotMemCache();
  delete process.env.RETIRE_CONSOLIDATED_PERIODS;
  avisos = [];
  jest.spyOn(console, 'warn').mockImplementation((...a) => avisos.push(a.map(String).join(' ')));
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  jest.restoreAllMocks();
  delete process.env.RETIRE_CONSOLIDATED_PERIODS;
});

const lecturasDeLaCacheAntigua = () => mockLecturas.filter((r) => r.includes('/performanceCache/'));

// ============================================================================
// Escenario 3 — el aviso
// ============================================================================

describe('HU 6.13 — un resumen que no se puede preparar responde el aviso (RN-6.13-A)', () => {
  test('QA-04: portafolio en COP sin resumen → RETURNS_UNAVAILABLE, sin ruta antigua ni su caché', async () => {
    await expect(getHistoricalReturns(contexto, { currency: 'COP', accountId: 'overall' }))
      .rejects.toMatchObject({
        code: 'unavailable',
        details: { code: 'RETURNS_UNAVAILABLE', pantalla: 'portafolio', motivo: 'sin-resumen' },
      });

    expect(mockGeneratePerformanceSnapshot).toHaveBeenCalledWith(expect.anything(), UID, 'overall', 'COP');
    expect(mockLegacyV2).not.toHaveBeenCalled();
    expect(lecturasDeLaCacheAntigua()).toEqual([]);
    expect(mockEscrituras).toEqual([]);
    expect(avisos).toContain(`[returnsUnavailable] ${UID} portafolio overall COP: sin-resumen`);
  });

  test('QA-04: si preparar el resumen falla, el motivo es el error de la preparación', async () => {
    mockGeneratePerformanceSnapshot.mockRejectedValue(new Error('registros de 2026-03 dañados'));

    await expect(getHistoricalReturns(contexto, { currency: 'COP', accountId: 'cta-1' }))
      .rejects.toMatchObject({
        details: { code: 'RETURNS_UNAVAILABLE', pantalla: 'cuenta', motivo: 'preparacion-fallida: registros de 2026-03 dañados' },
      });
    expect(mockLegacyV2).not.toHaveBeenCalled();
    expect(avisos).toContain(`[returnsUnavailable] ${UID} cuenta cta-1 COP: preparacion-fallida: registros de 2026-03 dañados`);
  });

  test('QA-05: detalle de un activo sin resumen por activo → aviso con la pantalla del activo', async () => {
    await expect(getHistoricalReturns(contexto, { currency: 'USD', accountId: 'overall', ticker: 'AAPL', assetType: 'stock' }))
      .rejects.toMatchObject({
        details: { code: 'RETURNS_UNAVAILABLE', pantalla: 'activo', motivo: 'resumen-por-activo-no-disponible' },
      });
    expect(mockGenerateAssetSnapshot).toHaveBeenCalledWith(expect.anything(), UID, 'overall', 'USD', 'AAPL', 'stock');
    expect(mockLegacyV2).not.toHaveBeenCalled();
    expect(avisos).toContain(`[returnsUnavailable] ${UID} activo overall USD AAPL_stock: resumen-por-activo-no-disponible`);
  });

  test('QA-05: el aviso del activo no afecta al portafolio, que sigue respondiendo su resumen', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_USD`] = resumen({ currency: 'USD' });

    await expect(getHistoricalReturns(contexto, { currency: 'USD', ticker: 'AAPL', assetType: 'stock' }))
      .rejects.toMatchObject({ details: { code: 'RETURNS_UNAVAILABLE' } });
    const portafolio = await getHistoricalReturns(contexto, { currency: 'USD' });

    expect(portafolio.returns.ytdReturn).toBe(9.75);
    expect(portafolio._metadata.version).toBe('snapshot');
  });

  test('QA-06: el aviso no se recuerda; la siguiente apertura prepara el resumen y responde las cifras', async () => {
    await expect(getHistoricalReturns(contexto, { currency: 'COP' })).rejects.toMatchObject({
      details: { code: 'RETURNS_UNAVAILABLE' },
    });

    mockGeneratePerformanceSnapshot.mockResolvedValue(resumen());
    const segunda = await getHistoricalReturns(contexto, { currency: 'COP' });
    const tercera = await getHistoricalReturns(contexto, { currency: 'COP' });

    expect(segunda.returns.ytdReturn).toBe(9.75);
    expect(tercera.returns).toEqual(segunda.returns);
    expect(mockGeneratePerformanceSnapshot).toHaveBeenCalledTimes(2); // la tercera sale de la memoria
  });

  test('QA-07: una respuesta guardada por la ruta antigua no reemplaza al aviso', async () => {
    mockDocumentos[`userData/${UID}/performanceCache/COP_overall`] = RESPUESTA_ANTIGUA;

    await expect(getHistoricalReturns(contexto, { currency: 'COP' })).rejects.toMatchObject({
      details: { code: 'RETURNS_UNAVAILABLE' },
    });
    expect(lecturasDeLaCacheAntigua()).toEqual([]);
  });
});

// ============================================================================
// Escenario 2 — las mismas cifras
// ============================================================================

describe('HU 6.13 — con el resumen preparado nada cambia (RN-6-A)', () => {
  const sinVolatiles = ({ lastCalculated, validUntil, ...resto }) => resto;

  test('la misma respuesta con el interruptor encendido que apagado', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_COP`] = resumen();

    const despues = await getHistoricalReturns(contexto, { currency: 'COP' });
    clearSnapshotMemCache();
    process.env.RETIRE_CONSOLIDATED_PERIODS = 'false';
    const antes = await getHistoricalReturns(contexto, { currency: 'COP' });

    expect(sinVolatiles(despues)).toEqual(sinVolatiles(antes));
    expect(mockLegacyV2).not.toHaveBeenCalled();
  });

  test('forceRefresh ya no se salta los resúmenes: responde el resumen, no la ruta antigua', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_COP`] = resumen();

    const r = await getHistoricalReturns(contexto, { currency: 'COP', forceRefresh: true });

    expect(r.returns.ytdReturn).toBe(9.75);
    expect(mockLegacyV2).not.toHaveBeenCalled();
    expect(lecturasDeLaCacheAntigua()).toEqual([]);
  });
});

// ============================================================================
// Escenario 7 — volver atrás
// ============================================================================

describe('HU 6.13 — con RETIRE_CONSOLIDATED_PERIODS=false, la ruta antigua como antes (RN-6-H, QA-12)', () => {
  beforeEach(() => { process.env.RETIRE_CONSOLIDATED_PERIODS = 'false'; });

  test('portafolio sin resumen → ruta antigua (V2), sin aviso', async () => {
    const r = await getHistoricalReturns(contexto, { currency: 'COP' });

    expect(r.returns.ytdReturn).toBe(4.44);
    expect(mockLegacyV2).toHaveBeenCalledWith(UID, expect.objectContaining({ currency: 'COP', accountId: 'overall' }));
    expect(avisos.some((a) => a.includes('[returnsUnavailable]'))).toBe(false);
  });

  test('activo sin resumen → ruta antigua', async () => {
    await getHistoricalReturns(contexto, { currency: 'USD', ticker: 'AAPL', assetType: 'stock' });
    expect(mockLegacyV2).toHaveBeenCalledWith(UID, expect.objectContaining({ ticker: 'AAPL', assetType: 'stock' }));
  });

  test('forceRefresh vuelve a saltarse los resúmenes', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_COP`] = resumen();
    await getHistoricalReturns(contexto, { currency: 'COP', forceRefresh: true });
    expect(mockLegacyV2).toHaveBeenCalled();
  });
});

// ============================================================================
// Escenario 4 — las acciones retiradas
// ============================================================================

describe('HU 6.13 — las acciones que leían consolidados ya no existen (QA-09)', () => {
  test.each(['getHistoricalReturnsOptimized', 'getConsolidatedDataStatus'])(
    '%s no está entre las acciones y el router la rechaza sin devolver cifras',
    async (accion) => {
      expect(VALID_ACTIONS).not.toContain(accion);
      await expect(queryOperations({ auth: { uid: UID }, data: { action: accion, payload: {} } }))
        .rejects.toMatchObject({ code: 'invalid-argument', message: expect.stringContaining(`Acción no válida: ${accion}`) });
      expect(mockLegacyV2).not.toHaveBeenCalled();
    },
  );

  test('getHistoricalReturns sigue disponible', () => {
    expect(VALID_ACTIONS).toContain('getHistoricalReturns');
  });
});
