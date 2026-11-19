/**
 * HU 6.15 — La distribución llega ligera: las principales primero y el resto
 * al usar la tabla, del mismo cálculo y sin lecturas.
 *
 * Se arma una distribución de 2.344 posiciones directas (el total del inversor
 * de referencia) con pesos distintos, sin ETF para no depender de la API de
 * ETFs: la forma de `holdings` es la misma.
 *
 * @see platform-docs/stories/6.15-distribucion-portafolio-carga-ligera/refinamiento.md (T1, T2, D2, D3, D4)
 */

const TOTAL = 2344;
const N = 100;
const UID = 'inv-g';

const simbolo = (i) => `T${String(i).padStart(4, '0')}`;

// Unidades crecientes → pesos todos distintos; la de más peso es la última
const ASSETS = Array.from({ length: TOTAL }, (_, i) => ({
  id: `asset-${i}`,
  data: () => ({
    name: simbolo(i),
    units: i + 1,
    assetType: 'stock',
    isActive: true,
    portfolioAccount: 'acc-1',
  }),
}));

const QUOTES = Object.fromEntries(
  Array.from({ length: TOTAL }, (_, i) => [simbolo(i), {
    price: 10, name: `Empresa ${i}`, sector: 'Technology', type: 'stock', currency: 'USD', country: 'United States',
  }])
);

const mockGetQuotes = jest.fn(async () => QUOTES);
jest.mock('../financeQuery', () => ({ getQuotes: (...args) => mockGetQuotes(...args) }));

jest.mock('../../utils/logger', () => ({
  StructuredLogger: jest.fn().mockImplementation(() => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() })),
}));

const mockCollection = jest.fn();
jest.mock('../firebaseAdmin', () => ({ firestore: () => ({ collection: (...a) => mockCollection(...a) }) }));

function consulta(docs) {
  const q = {
    where: () => q,
    select: () => q,
    orderBy: () => q,
    limit: () => q,
    get: async () => ({ docs, empty: docs.length === 0 }),
    doc: () => ({ get: async () => ({ exists: false, data: () => ({}) }) }),
  };
  return q;
}

function firestoreDelInversor() {
  mockCollection.mockImplementation((nombre) => {
    switch (nombre) {
      case 'portfolioAccounts':
        return consulta([{ id: 'acc-1', data: () => ({ userId: UID, isActive: true }) }]);
      case 'assets':
        return consulta(ASSETS);
      case 'sectors':
        return consulta([{ id: 's1', data: () => ({ sector: 'Technology', etfSectorName: 'Technology' }) }]);
      default:
        return consulta([]);
    }
  });
}

let servicio;
const T0 = new Date('2026-11-19T15:00:00.000Z').getTime();

beforeEach(() => {
  jest.resetModules();
  jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] });
  jest.setSystemTime(T0);
  mockCollection.mockReset();
  mockGetQuotes.mockClear();
  firestoreDelInversor();
  servicio = require('../portfolioDistributionService');
});

afterEach(() => {
  jest.useRealTimers();
});

const LIGERA = { currency: 'USD', includeHoldings: true, holdingsLimit: N };
const COMPLETA = { currency: 'USD', includeHoldings: true };

describe('HU 6.15 — recortarPosiciones (T1, RN-6.15-A)', () => {
  const resultado = {
    sectors: [{ sector: 'Technology', weight: 1, percentage: 100 }],
    holdings: Array.from({ length: 150 }, (_, i) => ({ symbol: simbolo(i), weight: (150 - i) / 1e4 })),
    totals: { portfolioValue: 1000, currency: 'USD' },
    metadata: { calculatedAt: '2026-11-19T15:00:00.000Z' },
  };

  test('con límite 100 deja las 100 primeras, el total real 150 y no muta la entrada', () => {
    const r = servicio.recortarPosiciones(resultado, { includeHoldings: true, holdingsLimit: 100 });
    expect(r.holdings).toHaveLength(100);
    expect(r.holdings[0]).toBe(resultado.holdings[0]);
    expect(r.holdings[99]).toBe(resultado.holdings[99]);
    expect(r.holdingsTotal).toBe(150);
    expect(r.holdingsComplete).toBe(false);
    expect(resultado.holdings).toHaveLength(150);
  });

  test('un portafolio de 37 posiciones cabe entero en la carga ligera (Escenario 4)', () => {
    const pequeño = { ...resultado, holdings: resultado.holdings.slice(0, 37) };
    const r = servicio.recortarPosiciones(pequeño, { includeHoldings: true, holdingsLimit: 100 });
    expect(r.holdings).toHaveLength(37);
    expect(r.holdingsTotal).toBe(37);
    expect(r.holdingsComplete).toBe(true);
  });

  test('sin límite devuelve la lista completa sin campos nuevos (Inteligencia, Copilot)', () => {
    const r = servicio.recortarPosiciones(resultado, { includeHoldings: true });
    expect(r.holdings).toHaveLength(150);
    expect(r).not.toHaveProperty('holdingsTotal');
    expect(r).not.toHaveProperty('holdingsComplete');
  });

  test('sin includeHoldings no viaja la lista, como antes', () => {
    expect(servicio.recortarPosiciones(resultado, {}).holdings).toBeUndefined();
    expect(servicio.recortarPosiciones({ ...resultado, holdings: [] }, {}).holdings).toEqual([]);
  });
});

describe('HU 6.15 — getPortfolioDistribution con holdingsLimit (T1, D2, QA-02)', () => {
  test('la carga ligera trae las 100 de mayor peso, en el orden de la tabla, y el total real 2.344', async () => {
    const ligera = await servicio.getPortfolioDistribution(UID, LIGERA);

    expect(ligera.holdings).toHaveLength(N);
    expect(ligera.holdingsTotal).toBe(TOTAL);
    expect(ligera.holdingsComplete).toBe(false);
    expect(ligera.holdings[0].symbol).toBe(simbolo(TOTAL - 1));
    expect(ligera.holdings[N - 1].symbol).toBe(simbolo(TOTAL - N));
    for (let i = 1; i < N; i++) {
      expect(ligera.holdings[i - 1].weight).toBeGreaterThan(ligera.holdings[i].weight);
    }
    // Sectores y totales llegan completos
    expect(ligera.sectors).toEqual([expect.objectContaining({ sector: 'Technology' })]);
    expect(ligera.totals.portfolioValue).toBe(10 * (TOTAL * (TOTAL + 1)) / 2);
  });

  test('la caché guarda la lista completa: la carga completa posterior recibe las 2.344 del mismo cálculo', async () => {
    const ligera = await servicio.getPortfolioDistribution(UID, LIGERA);
    const completa = await servicio.getPortfolioDistribution(UID, COMPLETA);

    expect(mockGetQuotes).toHaveBeenCalledTimes(1);
    expect(completa.holdings).toHaveLength(TOTAL);
    expect(completa.metadata.calculatedAt).toBe(ligera.metadata.calculatedAt);
    expect(completa.holdings.slice(0, N)).toEqual(ligera.holdings);
  });

  test('un acierto de caché con límite también llega recortado', async () => {
    await servicio.getPortfolioDistribution(UID, COMPLETA);
    const ligera = await servicio.getPortfolioDistribution(UID, LIGERA);

    expect(ligera.metadata.fromCache).toBe(true);
    expect(ligera.holdings).toHaveLength(N);
    expect(ligera.holdingsTotal).toBe(TOTAL);
  });
});

describe('HU 6.15 — obtenerRestoDePosiciones desde la caché (T2, D3, RN-6.15-C, QA-09, QA-10)', () => {
  test('devuelve las posiciones 101–2.344 de la misma instantánea sin tocar Firestore ni la API', async () => {
    const ligera = await servicio.getPortfolioDistribution(UID, LIGERA);
    const llamadasFirestore = mockCollection.mock.calls.length;
    const llamadasApi = mockGetQuotes.mock.calls.length;

    jest.setSystemTime(T0 + 4 * 60 * 1000); // dentro de los 5 min
    const resto = await servicio.obtenerRestoDePosiciones(UID, { currency: 'USD' }, {
      offset: N, snapshot: ligera.metadata.calculatedAt,
    });

    expect(resto.origen).toBe('cache');
    expect(resto.motivo).toBeUndefined();
    expect(resto.offset).toBe(N);
    expect(resto.holdingsTotal).toBe(TOTAL);
    expect(resto.holdings).toHaveLength(TOTAL - N);
    expect(resto.holdings[0].symbol).toBe(simbolo(TOTAL - N - 1));
    expect(mockCollection.mock.calls.length).toBe(llamadasFirestore);
    expect(mockGetQuotes.mock.calls.length).toBe(llamadasApi);
  });

  test('principales + resto = la distribución completa, posición por posición (QA-09)', async () => {
    const ligera = await servicio.getPortfolioDistribution(UID, LIGERA);
    const resto = await servicio.obtenerRestoDePosiciones(UID, { currency: 'USD' }, {
      offset: N, snapshot: ligera.metadata.calculatedAt,
    });
    const completa = await servicio.getPortfolioDistribution(UID, COMPLETA);

    const ensamblada = { ...ligera, holdings: [...ligera.holdings, ...resto.holdings] };
    expect(servicio.diferenciasDeDistribucion(completa, ensamblada)).toEqual([]);
  });
});

describe('HU 6.15 — el resto recalculado (D4, ajuste del PO): lista completa con offset 0 y su motivo', () => {
  test('otra-instancia: la instancia no tiene entrada → recalcula y devuelve las 2.344', async () => {
    // Nada calculado en esta instancia (módulo recién cargado)
    const resto = await servicio.obtenerRestoDePosiciones(UID, { currency: 'USD' }, {
      offset: N, snapshot: '2026-11-19T14:59:00.000Z',
    });

    expect(resto.origen).toBe('recalculado');
    expect(resto.motivo).toBe('otra-instancia');
    expect(resto.offset).toBe(0);
    expect(resto.holdings).toHaveLength(TOTAL);
    expect(resto.holdingsTotal).toBe(TOTAL);
    expect(resto.snapshot).toBe(new Date(T0).toISOString());
    expect(mockGetQuotes).toHaveBeenCalledTimes(1);
  });

  test('cache-vencida: la entrada es de esa instantánea pero pasaron más de 5 min → recalcula', async () => {
    const ligera = await servicio.getPortfolioDistribution(UID, LIGERA);

    jest.setSystemTime(T0 + 5 * 60 * 1000 + 1);
    const resto = await servicio.obtenerRestoDePosiciones(UID, { currency: 'USD' }, {
      offset: N, snapshot: ligera.metadata.calculatedAt,
    });

    expect(resto.origen).toBe('recalculado');
    expect(resto.motivo).toBe('cache-vencida');
    expect(resto.offset).toBe(0);
    expect(resto.holdings).toHaveLength(TOTAL);
    expect(resto.snapshot).toBe(new Date(T0 + 5 * 60 * 1000 + 1).toISOString());
    expect(resto.snapshot).not.toBe(ligera.metadata.calculatedAt);
    expect(mockGetQuotes).toHaveBeenCalledTimes(2);
  });

  test('instantanea-reemplazada: la entrada es de otro cálculo → devuelve la del cálculo vigente, completa', async () => {
    const ligera = await servicio.getPortfolioDistribution(UID, LIGERA);
    jest.setSystemTime(T0 + 1000);
    const nueva = await servicio.getPortfolioDistribution(UID, { ...COMPLETA, forceRefresh: true });

    const resto = await servicio.obtenerRestoDePosiciones(UID, { currency: 'USD' }, {
      offset: N, snapshot: ligera.metadata.calculatedAt,
    });

    expect(resto.origen).toBe('recalculado');
    expect(resto.motivo).toBe('instantanea-reemplazada');
    expect(resto.offset).toBe(0);
    expect(resto.snapshot).toBe(nueva.metadata.calculatedAt);
    expect(resto.holdings).toEqual(nueva.holdings);
  });
});

describe('HU 6.15 — diferenciasDeDistribucion (RN-6-A, RN-6-B, D8)', () => {
  const base = () => ({
    sectors: [{ sector: 'Technology', weight: 0.6, percentage: 60 }],
    countries: [{ id: '840', name: 'United States', value: 600, percentage: 60, assets: [{ symbol: 'NVDA', percentage: 11.36 }] }],
    nonGeographicData: { totalPercentage: 0, assets: [], assetTypeDistribution: [] },
    totals: { portfolioValue: 1000, currency: 'USD' },
    holdings: [{
      symbol: 'NVDA', weight: 0.1136,
      sources: [{ symbol: 'NVDA', contribution: 0.05 }, { symbol: 'VUAA.L', contribution: 0.04 }, { symbol: 'SPYG', contribution: 0.0236 }],
    }],
    metadata: { calculatedAt: 'a', fromCache: false },
  });

  test('dos distribuciones iguales no tienen diferencias, aunque cambien los metadatos', () => {
    const b = base();
    b.metadata = { calculatedAt: 'a', fromCache: true };
    expect(servicio.diferenciasDeDistribucion(base(), b)).toEqual([]);
  });

  test('un aporte por origen que difiere en 1e-12 es una diferencia, nombrada por símbolo', () => {
    const b = base();
    b.holdings[0].sources[1].contribution = 0.04 + 1e-12;
    const lineas = servicio.diferenciasDeDistribucion(base(), b);
    expect(lineas).toHaveLength(1);
    expect(lineas[0]).toMatch(/^holdings\[0\]\(NVDA\)\.sources\[1\]\(VUAA\.L\)\.contribution: /);
  });

  test('una posición de menos se informa con el conteo', () => {
    const b = base();
    b.holdings = [];
    expect(servicio.diferenciasDeDistribucion(base(), b)).toEqual(['holdings: 1 elementos ≠ 0']);
  });

  test('un país o un total distinto también cuenta', () => {
    const b = base();
    b.countries[0].assets[0].percentage = 11.37;
    b.totals.portfolioValue = 1000.01;
    expect(servicio.diferenciasDeDistribucion(base(), b)).toEqual([
      'countries[0].assets[0](NVDA).percentage: 11.36 ≠ 11.37',
      'totals.portfolioValue: 1000 ≠ 1000.01',
    ]);
  });
});
