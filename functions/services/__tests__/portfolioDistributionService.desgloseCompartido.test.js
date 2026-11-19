/**
 * HU 6.16 — La distribución usa el desglose de ETF guardado y no espera a las
 * fuentes externas.
 *
 * Inversor de prueba con una cuenta: VUAA.L, EIMI.L y SPYG (ETF) y AAPL
 * (acción). Cada `jest.resetModules()` + `require` es una **instancia nueva**
 * del servidor (memoria vacía); `etfBreakdowns` vive en un Firestore en
 * memoria que comparten todas.
 *
 * @see platform-docs/stories/6.16-desglose-etf-compartido/refinamiento.md (T2, T4, T5, T8)
 */

const UID = 'inv-g';
const DIA = 24 * 60 * 60 * 1000;

// ============================================================================
// Fábricas locales
// ============================================================================

const holding = (symbol, weight, overrides = {}) => ({ symbol, isin: `ISIN-${symbol}`, name: `${symbol} Inc.`, weight, asset_type: 'stock', ...overrides });

/** Lo que devuelve la fuente por ETF (con campos que el cálculo no usa). */
const FUENTE = {
  'VUAA.L': {
    source: 'etf.com',
    holdings: [holding('AAPL', 0.07), holding('MSFT', 0.065), holding('NVDA', 0.06), holding('AMZN', 0.04, { sharesHeld: 9 })],
    sectors: [{ name: 'Technology', weight: 0.31 }, { name: 'Healthcare', weight: 0.12 }],
    countries: [{ name: 'United States', weight: 0.99 }, { name: 'Ireland', weight: 0.01 }],
  },
  'EIMI.L': {
    holdings: [holding('2330.TW', 0.09), holding('0700.HK', 0.045), holding('BABA', 0.03)],
    sectors: [{ name: 'Technology', weight: 0.24 }, { name: 'Financial Services', weight: 0.21 }],
    countries: [
      { name: 'Taiwan', weight: 0.2 }, { name: 'China', weight: 0.25 },
      { name: 'Cyprus', weight: 0.0011 }, { name: 'Czech Republic', weight: 0.0017 }, { name: 'Caymen Islands', weight: 0.0023 },
    ],
  },
  SPYG: {
    holdings: [holding('NVDA', 0.13), holding('AAPL', 0.11)],
    sectors: [{ name: 'Technology', weight: 0.45 }],
    countries: [{ name: 'United States', weight: 1 }],
  },
};

const QUOTES = {
  'VUAA.L': { symbol: 'VUAA.L', price: 101.25, name: 'Vanguard S&P 500 UCITS', type: 'etf', currency: 'USD' },
  'EIMI.L': { symbol: 'EIMI.L', price: 37.9, name: 'iShares Core MSCI EM IMI', type: 'etf', currency: 'USD' },
  SPYG: { symbol: 'SPYG', price: 88.4, name: 'SPDR S&P 500 Growth', type: 'etf', currency: 'USD' },
  AAPL: { symbol: 'AAPL', price: 231.47, name: 'Apple Inc.', type: 'stock', currency: 'USD', sector: 'Technology', country: 'United States' },
};

const activo = (name, units, assetType) => ({ id: `a-${name}`, name, units, assetType, isActive: true, portfolioAccount: 'acc-1' });

const ESTATICAS = {
  portfolioAccounts: [{ id: 'acc-1', userId: UID, isActive: true }],
  assets: [activo('VUAA.L', 120, 'etf'), activo('EIMI.L', 300, 'etf'), activo('SPYG', 50, 'etf'), activo('AAPL', 10, 'stock')],
  sectors: [
    { id: 's1', sector: 'Technology', etfSectorName: 'Technology' },
    { id: 's2', sector: 'Healthcare', etfSectorName: 'Healthcare' },
    { id: 's3', sector: 'Financial Services', etfSectorName: 'Financial Services' },
  ],
  // Como en producción: Chequia se llama «Czechia», Islas Caimán existe y Chipre no está
  countries: [
    { id: 'US', country: 'United States', code2: 'US', code3: 'USA', codeNo: '840' },
    { id: 'IE', country: 'Ireland', code2: 'IE', code3: 'IRL', codeNo: '372' },
    { id: 'TW', country: 'Taiwan', code2: 'TW', code3: 'TWN', codeNo: '158' },
    { id: 'CN', country: 'China', code2: 'CN', code3: 'CHN', codeNo: '156' },
    { id: 'CZ', country: 'Czechia', name2: 'Czechia', code2: 'CZ', code3: 'CZE', codeNo: '203' },
    { id: 'KY', country: 'Cayman Islands', code2: 'KY', code3: 'CYM', codeNo: '136' },
  ],
};

// --- Firestore en memoria: colecciones fijas + etfBreakdowns compartido ----

const clonar = (valor) => {
  if (Buffer.isBuffer(valor) || valor instanceof Uint8Array) return Buffer.from(valor);
  if (Array.isArray(valor)) return valor.map(clonar);
  if (valor && typeof valor === 'object') return Object.fromEntries(Object.entries(valor).map(([k, v]) => [k, clonar(v)]));
  return valor;
};

const mockDocs = new Map();
const mockConteo = { getAll: 0, lotes: 0, transacciones: 0 };
let mockFallarLectura = false;

const mockDoc = (ruta) => ({
  ruta,
  get: async () => ({ exists: mockDocs.has(ruta), data: () => clonar(mockDocs.get(ruta)) }),
  set: async (data, opciones) => {
    mockDocs.set(ruta, clonar({ ...(opciones?.merge ? mockDocs.get(ruta) || {} : {}), ...data }));
  },
  update: async (data) => { mockDocs.set(ruta, clonar({ ...mockDocs.get(ruta), ...data })); },
  collection: (nombre) => mockColeccion(`${ruta}/${nombre}`),
});

function mockColeccion(nombre) {
  const q = {
    where: () => q,
    select: () => q,
    orderBy: () => q,
    limit: () => q,
    get: async () => {
      const docs = (ESTATICAS[nombre] || []).map((d) => ({ id: d.id, data: () => ({ ...d }) }));
      return { docs, empty: docs.length === 0 };
    },
    doc: (id) => mockDoc(`${nombre}/${id}`),
  };
  return q;
}

const mockDb = {
  collection: (nombre) => mockColeccion(nombre),
  getAll: async (...refs) => {
    mockConteo.getAll++;
    if (mockFallarLectura) throw new Error('UNAVAILABLE');
    return refs.map((r) => ({ exists: mockDocs.has(r.ruta), data: () => clonar(mockDocs.get(r.ruta)) }));
  },
  batch: () => {
    const ops = [];
    return {
      set: (ref, data) => ops.push(() => mockDocs.set(ref.ruta, clonar(data))),
      delete: (ref) => ops.push(() => mockDocs.delete(ref.ruta)),
      commit: async () => { mockConteo.lotes++; ops.forEach((op) => op()); },
    };
  },
  runTransaction: async (fn) => {
    mockConteo.transacciones++;
    return fn({
      get: async (ref) => ({ exists: mockDocs.has(ref.ruta), data: () => clonar(mockDocs.get(ref.ruta)) }),
      update: (ref, data) => mockDocs.set(ref.ruta, clonar({ ...mockDocs.get(ref.ruta), ...data })),
    });
  },
};

jest.mock('../firebaseAdmin', () => ({ firestore: () => mockDb }));

const mockGetQuotes = jest.fn(async () => QUOTES);
jest.mock('../financeQuery', () => ({ getQuotes: (...a) => mockGetQuotes(...a) }));

const mockWarn = jest.fn();
jest.mock('../../utils/logger', () => ({
  StructuredLogger: jest.fn().mockImplementation(() => ({ info: jest.fn(), warn: (...a) => mockWarn(...a), error: jest.fn(), debug: jest.fn() })),
}));

const mockEnqueue = jest.fn(async () => {});
jest.mock('firebase-admin/functions', () => ({
  getFunctions: () => ({ taskQueue: () => ({ enqueue: (...a) => mockEnqueue(...a) }) }),
}));

/** La fuente: responde con `FUENTE`, salvo los símbolos que se indique que fallan. */
let caidos = new Set();
const fetchMock = jest.fn(async (url) => {
  const symbol = decodeURIComponent(/\/v1\/etf\/([^/]+)\/unified/.exec(url)[1]);
  if (caidos.has(symbol) || !FUENTE[symbol]) return { ok: false, status: 503, text: async () => '' };
  return { ok: true, status: 200, text: async () => JSON.stringify(FUENTE[symbol]) };
});

/** Una instancia nueva del servidor: módulos recién cargados, memoria vacía. */
const instanciaNueva = () => {
  jest.resetModules();
  return require('../portfolioDistributionService');
};

/** Guarda un desglose como si se hubiera obtenido hace `dias` días. */
const guardar = async (symbol, dias = 0) => {
  const { guardarDesglose } = require('../etfBreakdown/etfBreakdownService');
  await guardarDesglose(mockDb, symbol, FUENTE[symbol], { ahora: new Date(Date.now() - dias * DIA), anterior: null });
};

const llamadasAFuente = () => fetchMock.mock.calls.map(([url]) => /\/v1\/etf\/([^/]+)\/unified/.exec(url)[1]);
const OPCIONES = { currency: 'USD', includeHoldings: true };
const original = process.env.ETF_BREAKDOWN_STORE;
let logSpy;

beforeEach(() => {
  mockDocs.clear();
  Object.assign(mockConteo, { getAll: 0, lotes: 0, transacciones: 0 });
  mockFallarLectura = false;
  caidos = new Set();
  fetchMock.mockClear();
  mockEnqueue.mockClear();
  mockWarn.mockClear();
  global.fetch = fetchMock;
  delete process.env.ETF_BREAKDOWN_STORE;
  logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  jest.restoreAllMocks();
  if (original === undefined) delete process.env.ETF_BREAKDOWN_STORE;
  else process.env.ETF_BREAKDOWN_STORE = original;
});

const lineaDelPaso = () => logSpy.mock.calls.map((c) => c[0]).find((l) => String(l).includes('[etfBreakdown] Paso del desglose'));

// ============================================================================

describe('HU 6.16 — con el desglose guardado, una instancia nueva no espera a la fuente (Escenario 1)', () => {
  test('todo vigente: cero llamadas a la fuente, una lectura por ETF y los tres desglosados (QA-01)', async () => {
    await Promise.all(['VUAA.L', 'EIMI.L', 'SPYG'].map((s) => guardar(s, 2)));
    const servicio = instanciaNueva();

    const r = await servicio.getPortfolioDistribution(UID, OPCIONES);

    expect(fetchMock).not.toHaveBeenCalled();
    expect(mockEnqueue).not.toHaveBeenCalled();
    expect(r.metadata.etfDecomposed).toBe(3);
    expect(r.holdings.find((h) => h.symbol === 'MSFT').sources).toEqual([{ symbol: 'VUAA.L', contribution: expect.any(Number) }]);
    expect(lineaDelPaso()).toMatch(/guardado=3 vencidos=0 fuente=0 sinDesglose=0 lecturas=3 escrituras=0 encolados=0$/);
  });

  test('un desglose vencido se usa tal cual y su refresco se encola aparte, sin llamar a la fuente (QA-02, QA-04)', async () => {
    await guardar('VUAA.L', 2);
    await guardar('EIMI.L', 9);
    await guardar('SPYG', 2);
    const servicio = instanciaNueva();

    const r = await servicio.getPortfolioDistribution(UID, OPCIONES);

    expect(fetchMock).not.toHaveBeenCalled();
    expect(r.holdings.some((h) => h.symbol === '2330.TW')).toBe(true);
    expect(mockEnqueue).toHaveBeenCalledTimes(1);
    expect(mockEnqueue).toHaveBeenCalledWith({ symbol: 'EIMI.L' }, { dispatchDeadlineSeconds: 120 });
    expect(lineaDelPaso()).toMatch(/vencidos=1 fuente=0 .*encolados=1 refrescos=EIMI\.L$/);
  });

  test('el mismo día, otra instancia con el mismo vencido no vuelve a encolar (ajuste 1 del PO)', async () => {
    await Promise.all([guardar('VUAA.L', 2), guardar('EIMI.L', 9), guardar('SPYG', 2)]);
    await instanciaNueva().getPortfolioDistribution(UID, OPCIONES);
    await instanciaNueva().getPortfolioDistribution(UID, OPCIONES);

    expect(mockEnqueue).toHaveBeenCalledTimes(1);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('VUAA.L se guarda una sola vez y lo usa otro inversor sin consultar la fuente ni crear otra copia (QA-11, RN-6.16-E)', async () => {
    await Promise.all([guardar('EIMI.L', 1), guardar('SPYG', 1)]);
    await instanciaNueva().getPortfolioDistribution(UID, OPCIONES);
    expect(llamadasAFuente()).toEqual(['VUAA.L']);

    fetchMock.mockClear();
    await instanciaNueva().getPortfolioDistribution(UID, { ...OPCIONES, currency: 'EUR' });

    expect(fetchMock).not.toHaveBeenCalled();
    expect([...mockDocs.keys()].filter((k) => k.startsWith('etfBreakdowns/VUAA.L'))).toEqual(['etfBreakdowns/VUAA.L']);
  });
});

describe('HU 6.16 — nunca guardado (Escenario 3 y 4)', () => {
  test('SPYG nunca guardado: espera a la fuente una vez, se guarda y la siguiente instancia no la llama (QA-05)', async () => {
    await Promise.all([guardar('VUAA.L', 1), guardar('EIMI.L', 1)]);

    await instanciaNueva().getPortfolioDistribution(UID, OPCIONES);
    expect(llamadasAFuente()).toEqual(['SPYG']);
    expect(mockDocs.get('etfBreakdowns/SPYG')).toMatchObject({ symbol: 'SPYG', posiciones: 2 });
    expect(lineaDelPaso()).toMatch(/fuente=1 sinDesglose=0 lecturas=3 escrituras=1 encolados=0 esperaronFuente=SPYG$/);

    fetchMock.mockClear();
    await instanciaNueva().getPortfolioDistribution(UID, OPCIONES);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('nunca guardado con la fuente caída: SPYG queda como una sola posición y no se guarda nada (QA-08)', async () => {
    await Promise.all([guardar('VUAA.L', 1), guardar('EIMI.L', 1)]);
    caidos = new Set(['SPYG']);

    const r = await instanciaNueva().getPortfolioDistribution(UID, OPCIONES);

    expect(mockDocs.has('etfBreakdowns/SPYG')).toBe(false);
    expect(r.metadata.etfNotDecomposed).toEqual(['SPYG']);
    expect(r.holdings.find((h) => h.symbol === 'SPYG')).toMatchObject({ asset_type: 'etf' });
    expect(r.metadata.etfDecomposed).toBe(2);
  });

  test('si leer el guardado falla, la distribución sigue pidiendo a la fuente, como antes', async () => {
    mockFallarLectura = true;

    const r = await instanciaNueva().getPortfolioDistribution(UID, OPCIONES);

    expect(llamadasAFuente().sort()).toEqual(['EIMI.L', 'SPYG', 'VUAA.L']);
    expect(r.metadata.etfDecomposed).toBe(3);
  });
});

describe('HU 6.16 — mismas cifras con el mismo desglose de entrada (Escenario 2, QA-03)', () => {
  test('desde el guardado sale exactamente lo mismo que desde la fuente, en todos los decimales', async () => {
    process.env.ETF_BREAKDOWN_STORE = 'false';
    const desdeFuente = await instanciaNueva().getPortfolioDistribution(UID, OPCIONES);

    delete process.env.ETF_BREAKDOWN_STORE;
    await Promise.all(['VUAA.L', 'EIMI.L', 'SPYG'].map((s) => guardar(s, 1)));
    fetchMock.mockClear();
    const servicio = instanciaNueva();
    const desdeGuardado = await servicio.getPortfolioDistribution(UID, OPCIONES);

    expect(fetchMock).not.toHaveBeenCalled();
    expect(servicio.diferenciasDeDistribucion(desdeFuente, desdeGuardado)).toEqual([]);
    expect(desdeGuardado.holdings.length).toBeGreaterThan(8);
  });

  test('reunirEntradas + calcularDesdeEntradas da lo mismo que getPortfolioDistribution', async () => {
    await Promise.all(['VUAA.L', 'EIMI.L', 'SPYG'].map((s) => guardar(s, 1)));
    const servicio = instanciaNueva();

    const entradas = await servicio.reunirEntradas(UID, OPCIONES);
    const calculada = servicio.calcularDesdeEntradas(UID, entradas, OPCIONES);
    const completa = await instanciaNueva().getPortfolioDistribution(UID, OPCIONES);

    expect(servicio.diferenciasDeDistribucion(completa, calculada)).toEqual([]);
  });
});

describe('HU 6.16 — Chipre, República Checa e Islas Caimán en el mapa (Escenario 5, QA-09)', () => {
  test('cuentan en su país y el registro ya no los da por «sin mapear»', async () => {
    await Promise.all(['VUAA.L', 'EIMI.L', 'SPYG'].map((s) => guardar(s, 1)));

    const r = await instanciaNueva().getPortfolioDistribution(UID, OPCIONES);
    const pais = (id) => r.countries.find((c) => c.id === id);
    const pesoEimi = (300 * 37.9) / r.totals.portfolioValue;

    expect(pais('196')).toMatchObject({ name: 'Cyprus' });
    expect(pais('203')).toMatchObject({ name: 'Czechia' });
    expect(pais('136')).toMatchObject({ name: 'Cayman Islands' });
    expect(pais('196').percentage).toBeCloseTo(0.0011 * pesoEimi * 100, 10);
    expect(pais('136').assets).toEqual([expect.objectContaining({ symbol: 'EIMI.L', assetType: 'etf' })]);
    const sinMapear = mockWarn.mock.calls.filter(([m]) => m === 'Unmapped ETF country found').map(([, d]) => d.country);
    expect(sinMapear).toEqual([]);
  });

  test('el mapeo «como hoy» (alias de master) no los reconoce: es la referencia de la comparación (ajuste 3)', async () => {
    const servicio = instanciaNueva();
    const comoHoy = await servicio.getCountryMappings({ comoHoy: true });
    const nuevo = await servicio.getCountryMappings();

    ['cyprus', 'czech republic', 'caymen islands'].forEach((nombre) => {
      expect(comoHoy.has(nombre)).toBe(false);
      expect(nuevo.has(nombre)).toBe(true);
    });
    expect(nuevo.get('czech republic').codeNo).toBe('203');
    expect(nuevo.get('caymen islands').codeNo).toBe('136');
    expect(nuevo.get('cyprus').codeNo).toBe('196');
    expect(comoHoy.get('czechia').codeNo).toBe('203');
  });
});

describe('HU 6.16 — volver atrás (D7, RN-6-H, QA-13)', () => {
  test('con ETF_BREAKDOWN_STORE=false: la fuente como hoy, sin tocar etfBreakdowns ni encolar, y 24 h en memoria', async () => {
    await Promise.all([guardar('VUAA.L', 2), guardar('EIMI.L', 9), guardar('SPYG', 2)]);
    const antes = clonar([...mockDocs.entries()]);
    Object.assign(mockConteo, { getAll: 0, lotes: 0, transacciones: 0 });
    process.env.ETF_BREAKDOWN_STORE = 'false';
    const servicio = instanciaNueva();

    const r = await servicio.getPortfolioDistribution(UID, OPCIONES);
    expect(llamadasAFuente().sort()).toEqual(['EIMI.L', 'SPYG', 'VUAA.L']);
    expect(r.metadata.etfDecomposed).toBe(3);

    fetchMock.mockClear();
    await servicio.getPortfolioDistribution(UID, { ...OPCIONES, currency: 'EUR' });
    expect(fetchMock).not.toHaveBeenCalled();

    expect(mockConteo).toEqual({ getAll: 0, lotes: 0, transacciones: 0 });
    expect(mockEnqueue).not.toHaveBeenCalled();
    expect(lineaDelPaso()).toBeUndefined();
    expect([...mockDocs.entries()]).toEqual(antes);
  });

  test('forceRefresh vacía la memoria pero relee el guardado, sin ir a la fuente (D4)', async () => {
    await Promise.all(['VUAA.L', 'EIMI.L', 'SPYG'].map((s) => guardar(s, 1)));
    const servicio = instanciaNueva();
    await servicio.getPortfolioDistribution(UID, OPCIONES);
    const lecturas = mockConteo.getAll;

    await servicio.getPortfolioDistribution(UID, { ...OPCIONES, forceRefresh: true });

    expect(fetchMock).not.toHaveBeenCalled();
    expect(mockConteo.getAll).toBe(lecturas + 1);
  });
});
