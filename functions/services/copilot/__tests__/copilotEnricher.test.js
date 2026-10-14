/**
 * HU 5.2 — "Consultando analistas y noticias", ahora en el backend.
 *
 * Lo que defiende este archivo es QA-19: si la fuente de analistas y noticias no
 * responde, el análisis se completa igual y esas secciones simplemente no
 * aparecen. Ninguna fuente auxiliar puede tumbar un análisis.
 *
 * @see services/copilot/copilotEnricher.js
 */

jest.mock('../../config', () => ({
  FINANCE_QUERY_API_URL: 'https://api.ejemplo/v1',
  getServiceHeaders: (extra) => ({ 'x-service-token': 'token', ...extra }),
}));

const enricher = require('../copilotEnricher');

// ============================================================================
// FÁBRICAS
// ============================================================================

const crearAsset = (overrides = {}) => ({
  name: 'MSFT',
  units: 10,
  unitValue: 300,
  currency: 'USD',
  sector: 'Technology',
  portfolioAccount: 'acc-ibkr',
  company: 'Microsoft Corp',
  ...overrides,
});

const crearPrecio = (overrides = {}) => ({
  price: 400,
  currency: 'USD',
  sector: 'Technology',
  name: 'Microsoft Corporation',
  ...overrides,
});

const crearEstado = (overrides = {}) => ({
  assets: [crearAsset()],
  priceMap: new Map([['MSFT', crearPrecio()]]),
  rates: new Map([['USD', 1]]),
  accounts: [{ id: 'acc-ibkr', name: 'Interactive Brokers' }],
  ...overrides,
});

const crearAnalistas = (overrides = {}) => ({
  symbol: 'MSFT',
  recommendationKey: 'strong_buy',
  targetPrice: { current: 400, low: 380, mean: 460, high: 520, numberOfAnalysts: 42 },
  earningsEstimates: [{ period: '0q', endDate: '2026-12-31', avg: 3.1, low: 2.9, high: 3.3 }],
  ...overrides,
});

const fetchersQueResponden = () => ({
  fetchAnalystConsensus: jest.fn().mockResolvedValue(crearAnalistas()),
  fetchNews: jest.fn().mockResolvedValue([
    { title: 'Microsoft sube', source: 'Reuters', time: '2026-10-09T08:00:00.000Z' },
  ]),
});

// ============================================================================
// TESTS
// ============================================================================

describe('rankAssetsByValue — las posiciones, normalizadas a USD', () => {
  it('ordena por valor de mercado y calcula peso y ROI', () => {
    const estado = crearEstado({
      assets: [crearAsset(), crearAsset({ name: 'NVDA', units: 5, unitValue: 100 })],
      priceMap: new Map([
        ['MSFT', crearPrecio()],
        ['NVDA', crearPrecio({ price: 200, name: 'NVIDIA' })],
      ]),
    });

    const ranked = enricher.rankAssetsByValue(estado.assets, estado.priceMap, estado.rates);

    expect(ranked.map((a) => a.ticker)).toEqual(['MSFT', 'NVDA']);
    expect(ranked[0].weight).toBe(80);
    expect(ranked[0].roi).toBe(33.3);
    expect(ranked[1].roi).toBe(100);
  });

  it('convierte a USD con la tasa de la divisa del activo', () => {
    const estado = crearEstado({
      assets: [crearAsset({ name: 'PFBCOLOM', units: 500, unitValue: 40000, currency: 'COP' })],
      priceMap: new Map([['PFBCOLOM', crearPrecio({ price: 44000, currency: 'COP' })]]),
      rates: new Map([['USD', 1], ['COP', 4000]]),
    });

    const [posicion] = enricher.rankAssetsByValue(estado.assets, estado.priceMap, estado.rates);

    expect(posicion.currentPrice).toBe(11);
    expect(posicion.averageCost).toBe(10);
    expect(posicion.roi).toBe(10);
  });

  it('ignora las posiciones cerradas y las que no tienen precio', () => {
    const estado = crearEstado({
      assets: [crearAsset(), crearAsset({ name: 'VENDIDO', units: 0 })],
      priceMap: new Map([['MSFT', crearPrecio()], ['VENDIDO', crearPrecio({ price: 0 })]]),
    });

    const ranked = enricher.rankAssetsByValue(estado.assets, estado.priceMap, estado.rates);

    expect(ranked.map((a) => a.ticker)).toEqual(['MSFT']);
  });

  it('prefiere el nombre de la API al que escribió el usuario, y nunca el de la cuenta', () => {
    const estado = crearEstado();

    const [posicion] = enricher.rankAssetsByValue(estado.assets, estado.priceMap, estado.rates);

    expect(posicion.name).toBe('Microsoft Corporation');
  });
});

describe('enrichCopilotPayload — degradación aislada (QA-19)', () => {
  it('trae analistas y noticias cuando la fuente responde', async () => {
    const sections = await enricher.enrichCopilotPayload(crearEstado(), fetchersQueResponden());

    expect(sections.topHoldings[0].ticker).toBe('MSFT');
    expect(sections.topHoldings[0].analystConsensus.recommendation).toBe('strongBuy');
    expect(sections.topHoldings[0].analystConsensus.upsidePercent).toBe(15);
    expect(sections.topHoldings[0].nextEarnings.period).toBe('Q4 2026');
    expect(sections.recentNews[0].ticker).toBe('MSFT');
  });

  it('sin datos de analistas ni noticias, esas secciones no aparecen', async () => {
    const fetchers = {
      fetchAnalystConsensus: jest.fn().mockResolvedValue(null),
      fetchNews: jest.fn().mockResolvedValue(null),
    };

    const sections = await enricher.enrichCopilotPayload(crearEstado(), fetchers);

    // topHoldings sigue existiendo —son las posiciones del usuario, que el
    // backend sí conoce— pero sin el bloque de analistas.
    expect(sections.topHoldings[0].analystConsensus).toBeUndefined();
    expect(sections.recentNews).toBeUndefined();
  });

  it('una fuente que falla no arrastra a la otra', async () => {
    const fetchers = {
      fetchAnalystConsensus: jest.fn().mockRejectedValue(new Error('502')),
      fetchNews: jest.fn().mockResolvedValue([
        { title: 'Titular', source: 'Reuters', time: '2026-10-09T08:00:00.000Z' },
      ]),
    };

    const sections = await enricher.enrichCopilotPayload(crearEstado(), fetchers);

    expect(sections.recentNews).toHaveLength(1);
  });

  it('una fuente que nunca responde se rinde al plazo y no cuelga el análisis', async () => {
    jest.useFakeTimers();
    const fetchers = {
      fetchAnalystConsensus: () => new Promise(() => {}),
      fetchNews: () => new Promise(() => {}),
    };

    const promesa = enricher.enrichCopilotPayload(crearEstado(), fetchers);
    jest.advanceTimersByTime(enricher.FETCH_TIMEOUT_MS + 1000);
    const sections = await promesa;

    expect(sections.topHoldings[0].analystConsensus).toBeUndefined();
    expect(sections.recentNews).toBeUndefined();
    jest.useRealTimers();
  });

  it('el comparativo de cuentas sale de datos propios, sin pedir nada fuera', async () => {
    const fetchers = {
      fetchAnalystConsensus: jest.fn().mockResolvedValue(null),
      fetchNews: jest.fn().mockResolvedValue(null),
    };

    const sections = await enricher.enrichCopilotPayload(crearEstado(), fetchers);

    expect(sections.accountsSummary).toHaveLength(1);
    expect(sections.accountsSummary[0].accountName).toBe('Interactive Brokers');
    expect(sections.accountsSummary[0].topAsset).toBe('MSFT');
    expect(sections.accountsSummary[0].accountROI).toBe(33.3);
    expect(sections.accountsSummary[0].originalCurrency).toBe('USD');
  });
});

describe('límites heredados del enriquecedor del navegador', () => {
  it('nombra como mucho 5 posiciones principales', async () => {
    const tickers = ['A', 'B', 'C', 'D', 'E', 'F', 'G'];
    const estado = crearEstado({
      assets: tickers.map((tk, i) => crearAsset({ name: tk, units: 10 - i })),
      priceMap: new Map(tickers.map((tk) => [tk, crearPrecio()])),
    });

    const sections = await enricher.enrichCopilotPayload(estado, fetchersQueResponden());

    expect(sections.topHoldings).toHaveLength(enricher.MAX_TOP_HOLDINGS);
  });

  it('no pide noticias de más de 3 tickers', async () => {
    const tickers = ['A', 'B', 'C', 'D', 'E'];
    const estado = crearEstado({
      assets: tickers.map((tk, i) => crearAsset({ name: tk, units: 10 - i })),
      priceMap: new Map(tickers.map((tk) => [tk, crearPrecio()])),
    });
    const fetchers = fetchersQueResponden();

    await enricher.enrichCopilotPayload(estado, fetchers);

    expect(fetchers.fetchNews).toHaveBeenCalledTimes(enricher.MAX_NEWS_TICKERS);
  });
});

describe('formatTimeAgo', () => {
  const ahora = new Date('2026-10-09T10:00:00.000Z');

  it('respeta lo que ya viene formateado', () => {
    expect(enricher.formatTimeAgo('2h ago', ahora)).toBe('2h ago');
  });

  it('cuenta las horas de una fecha del mismo día', () => {
    expect(enricher.formatTimeAgo('2026-10-09T07:00:00.000Z', ahora)).toBe('3h ago');
  });

  it('cuenta los días cuando pasa de uno', () => {
    expect(enricher.formatTimeAgo('2026-10-07T10:00:00.000Z', ahora)).toBe('2d ago');
  });
});
