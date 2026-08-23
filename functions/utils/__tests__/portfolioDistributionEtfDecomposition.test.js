/**
 * FIX-ETF-EU-001 — Desglose de ETFs con cobertura parcial de holdings
 *
 * Contexto del bug:
 *   `/v1/etf/{ticker}/unified` devolvía 404 para los UCITS europeos
 *   (VUAA.L, EIMI.L), así que esos ETFs quedaban sin desagregar: sin sectores,
 *   sin país (caían en `nonGeographicData`) y como bloque opaco en el treemap.
 *
 *   Al añadir Yahoo/justETF como fuente aparece un segundo problema: esas
 *   fuentes sólo publican el TOP-10 de la cartera (VUAA.L ≈ 36%). Si se
 *   desagregara ese 36% y se descartara el ETF, el ~64% restante desaparecería
 *   del treemap y los pesos dejarían de sumar 100%.
 *
 * Estas pruebas fijan ambos comportamientos.
 */

jest.mock('../../services/firebaseAdmin', () => ({
  firestore: () => ({ collection: jest.fn() }),
}));

jest.mock('../../utils/logger', () => ({
  StructuredLogger: jest.fn().mockImplementation(() => ({
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  })),
}));

const {
  calculateSectorDistribution,
  calculateCountryDistribution,
  hasUsableETFData,
} = require('../../services/portfolioDistributionService').__testing;

const USER_ID = 'user-1';
const ACCOUNTS = [
  { id: 'acc-1', isActive: true, userId: USER_ID },
  { id: 'acc-2', isActive: true, userId: USER_ID },
];
const CURRENCY_RATES = { USD: 1 };
const NO_MAPPINGS = {};

/** Datos tal como los devuelve Yahoo topHoldings: top-10 (~36% del fondo). */
const VUAA_PARTIAL = {
  ticker: 'VUAA.L',
  name: 'Vanguard S&P 500 UCITS ETF USD Accumulation',
  holdings: [
    { symbol: 'NVDA', name: 'NVIDIA Corp', weight: 0.2 },
    { symbol: 'MSFT', name: 'Microsoft Corp', weight: 0.16 },
  ],
  sectors: [
    { name: 'Technology', weight: 0.6 },
    { name: 'Financial Services', weight: 0.4 },
  ],
  countries: [
    { name: 'United States', weight: 0.97 },
    { name: 'Ireland', weight: 0.03 },
  ],
};

/** Datos tal como los devuelve etf.com: cartera prácticamente completa. */
const SPYG_FULL = {
  ticker: 'SPYG',
  name: 'SPDR Portfolio S&P 500 Growth ETF',
  holdings: [
    { symbol: 'NVDA', name: 'NVIDIA Corporation', weight: 0.6 },
    { symbol: 'MSFT', name: 'Microsoft Corporation', weight: 0.4 },
  ],
  sectors: [{ name: 'Technology', weight: 1 }],
  countries: [{ name: 'United States', weight: 1 }],
};

const sumWeights = (holdings) => holdings.reduce((s, h) => s + h.weight, 0);

describe('FIX-ETF-EU-001 · desglose de ETFs', () => {
  describe('cobertura parcial de holdings', () => {
    const assets = [
      { name: 'VUAA.L', units: 10, assetType: 'etf', isActive: true, portfolioAccount: 'acc-1' },
    ];
    const prices = { 'VUAA.L': { price: 10, currency: 'USD', type: 'etf', name: VUAA_PARTIAL.name } };
    const totalValue = 100;

    it('conserva el remanente no desagregado para que los pesos sumen 100%', () => {
      const etfData = new Map([['VUAA.L', VUAA_PARTIAL]]);

      const { holdings, etfStats } = calculateSectorDistribution(
        assets, prices, etfData, NO_MAPPINGS, ACCOUNTS, USER_ID, totalValue, CURRENCY_RATES
      );

      expect(sumWeights(holdings)).toBeCloseTo(1, 6);

      const remainder = holdings.find(h => h.symbol === 'VUAA.L');
      expect(remainder).toBeDefined();
      // 100% del portafolio - 36% desagregado
      expect(remainder.weight).toBeCloseTo(0.64, 6);
      expect(remainder.isPartialRemainder).toBe(true);
      expect(remainder.holdingsCoverage).toBeCloseTo(0.36, 6);

      expect(holdings.find(h => h.symbol === 'NVDA').weight).toBeCloseTo(0.2, 6);
      expect(holdings.find(h => h.symbol === 'MSFT').weight).toBeCloseTo(0.16, 6);

      expect(etfStats.decomposed).toBe(1);
      expect(Array.from(etfStats.partiallyDecomposed)).toEqual(['VUAA.L']);
    });

    it('no deja remanente cuando la fuente publica la cartera completa', () => {
      const spygAssets = [
        { name: 'SPYG', units: 10, assetType: 'etf', isActive: true, portfolioAccount: 'acc-1' },
      ];
      const spygPrices = { SPYG: { price: 10, currency: 'USD', type: 'etf', name: SPYG_FULL.name } };

      const { holdings, etfStats } = calculateSectorDistribution(
        spygAssets, spygPrices, new Map([['SPYG', SPYG_FULL]]),
        NO_MAPPINGS, ACCOUNTS, USER_ID, totalValue, CURRENCY_RATES
      );

      expect(holdings.find(h => h.symbol === 'SPYG')).toBeUndefined();
      expect(sumWeights(holdings)).toBeCloseTo(1, 6);
      expect(Array.from(etfStats.partiallyDecomposed)).toEqual([]);
    });

    it('conserva entero un ETF cuya cartera llega sin identificadores', () => {
      // Caso real de TLT: etf.com devuelve los 42 bonos del Tesoro sin symbol ni
      // isin. Se descartan al desglosar, así que la cobertura efectiva es 0 y el
      // ETF debe permanecer completo en el treemap.
      const tltAssets = [
        { name: 'TLT', units: 10, assetType: 'etf', isActive: true, portfolioAccount: 'acc-1' },
      ];
      const tltPrices = { TLT: { price: 10, currency: 'USD', type: 'etf', name: 'iShares 20+ Year Treasury Bond ETF' } };
      const tltData = {
        ticker: 'TLT',
        holdings: [
          { name: 'United States Treasury Bond 4.75% 15-MAY-2055', weight: 0.5, symbol: null, isin: null },
          { name: 'United States Treasury Bond 4.625% 15-MAY-2054', weight: 0.4998, symbol: null, isin: null },
        ],
        sectors: [{ name: 'Government', weight: 1 }],
        countries: [{ name: 'United States', weight: 1 }],
      };

      const { holdings } = calculateSectorDistribution(
        tltAssets, tltPrices, new Map([['TLT', tltData]]),
        NO_MAPPINGS, ACCOUNTS, USER_ID, 100, CURRENCY_RATES
      );

      const tlt = holdings.find(h => h.symbol === 'TLT');
      expect(tlt).toBeDefined();
      expect(tlt.weight).toBeCloseTo(1, 6);
      expect(sumWeights(holdings)).toBeCloseTo(1, 6);
    });

    it('acumula el remanente del mismo ETF repartido en varias cuentas', () => {
      const multiAccountAssets = [
        { name: 'VUAA.L', units: 5, assetType: 'etf', isActive: true, portfolioAccount: 'acc-1' },
        { name: 'VUAA.L', units: 5, assetType: 'etf', isActive: true, portfolioAccount: 'acc-2' },
      ];

      const { holdings } = calculateSectorDistribution(
        multiAccountAssets, prices, new Map([['VUAA.L', VUAA_PARTIAL]]),
        NO_MAPPINGS, ACCOUNTS, USER_ID, totalValue, CURRENCY_RATES
      );

      // Regresión: antes el `delete` dentro del bucle borraba el remanente
      // acumulado por la cuenta anterior.
      const remainder = holdings.find(h => h.symbol === 'VUAA.L');
      expect(remainder.weight).toBeCloseTo(0.64, 6);
      expect(sumWeights(holdings)).toBeCloseTo(1, 6);
    });
  });

  describe('distribución sectorial', () => {
    it('imputa a "Other" la parte del fondo que la fuente no clasifica', () => {
      const assets = [
        { name: 'VUAA.L', units: 10, assetType: 'etf', isActive: true, portfolioAccount: 'acc-1' },
      ];
      const prices = { 'VUAA.L': { price: 10, currency: 'USD', type: 'etf', name: VUAA_PARTIAL.name } };
      const partialSectors = { ...VUAA_PARTIAL, sectors: [{ name: 'Technology', weight: 0.7 }] };

      const { sectors } = calculateSectorDistribution(
        assets, prices, new Map([['VUAA.L', partialSectors]]),
        NO_MAPPINGS, ACCOUNTS, USER_ID, 100, CURRENCY_RATES
      );

      const total = sectors.reduce((s, x) => s + x.weight, 0);
      expect(total).toBeCloseTo(1, 6);
      expect(sectors.find(s => s.sector === 'Technology').weight).toBeCloseTo(0.7, 6);
      expect(sectors.find(s => s.sector === 'Other').weight).toBeCloseTo(0.3, 6);
    });

    it('cuenta los sectores aunque la fuente no publique la cartera', () => {
      const assets = [
        { name: 'VUAA.L', units: 10, assetType: 'etf', isActive: true, portfolioAccount: 'acc-1' },
      ];
      const prices = { 'VUAA.L': { price: 10, currency: 'USD', type: 'etf', name: VUAA_PARTIAL.name } };
      const sectorsOnly = { ...VUAA_PARTIAL, holdings: [] };

      const { sectors, holdings, etfStats } = calculateSectorDistribution(
        assets, prices, new Map([['VUAA.L', sectorsOnly]]),
        NO_MAPPINGS, ACCOUNTS, USER_ID, 100, CURRENCY_RATES
      );

      // Sin cartera el ETF sigue siendo una posición opaca...
      expect(etfStats.notDecomposed).toEqual(['VUAA.L']);
      expect(holdings.find(h => h.symbol === 'VUAA.L').weight).toBeCloseTo(1, 6);
      // ...pero su exposición sectorial sí se refleja.
      expect(sectors.find(s => s.sector === 'Technology').weight).toBeCloseTo(0.6, 6);
      expect(sectors.reduce((s, x) => s + x.weight, 0)).toBeCloseTo(1, 6);
    });

    it('no inventa un sector "Other" cuando la fuente no clasifica nada', () => {
      const assets = [
        { name: 'VUAA.L', units: 10, assetType: 'etf', isActive: true, portfolioAccount: 'acc-1' },
      ];
      const prices = { 'VUAA.L': { price: 10, currency: 'USD', type: 'etf', name: VUAA_PARTIAL.name } };

      const { sectors } = calculateSectorDistribution(
        assets, prices, new Map(), NO_MAPPINGS, ACCOUNTS, USER_ID, 100, CURRENCY_RATES
      );

      expect(sectors).toEqual([]);
    });

    it('reparte el 100% del ETF cuando la fuente sí cubre todos los sectores', () => {
      const assets = [
        { name: 'VUAA.L', units: 10, assetType: 'etf', isActive: true, portfolioAccount: 'acc-1' },
      ];
      const prices = { 'VUAA.L': { price: 10, currency: 'USD', type: 'etf', name: VUAA_PARTIAL.name } };

      const { sectors } = calculateSectorDistribution(
        assets, prices, new Map([['VUAA.L', VUAA_PARTIAL]]),
        NO_MAPPINGS, ACCOUNTS, USER_ID, 100, CURRENCY_RATES
      );

      expect(sectors.reduce((s, x) => s + x.weight, 0)).toBeCloseTo(1, 6);
      expect(sectors.find(s => s.sector === 'Other')).toBeUndefined();
    });
  });

  describe('distribución geográfica', () => {
    it('reparte el ETF por país usando los pesos de justETF', () => {
      const assets = [
        { name: 'VUAA.L', units: 10, assetType: 'etf', isActive: true, portfolioAccount: 'acc-1' },
      ];
      const prices = { 'VUAA.L': { price: 10, currency: 'USD', type: 'etf', name: VUAA_PARTIAL.name } };
      const countryMappings = new Map([
        ['united states', { codeNo: '840', country: 'United States' }],
        ['ireland', { codeNo: '372', country: 'Ireland' }],
      ]);

      const countries = calculateCountryDistribution(
        assets, prices, new Map([['VUAA.L', VUAA_PARTIAL]]),
        countryMappings, ACCOUNTS, USER_ID, 100, CURRENCY_RATES
      );

      const us = countries.find(c => c.id === '840');
      expect(us.percentage).toBeCloseTo(97, 6);
      expect(countries.find(c => c.id === '372').percentage).toBeCloseTo(3, 6);
    });
  });

  describe('hasUsableETFData', () => {
    it('acepta respuestas con sectores o países aunque no traigan holdings', () => {
      expect(hasUsableETFData({ holdings: [], sectors: [{ name: 'Technology', weight: 1 }] })).toBe(true);
      expect(hasUsableETFData({ holdings: [], countries: [{ name: 'Ireland', weight: 1 }] })).toBe(true);
    });

    it('rechaza respuestas vacías o nulas', () => {
      expect(hasUsableETFData(null)).toBe(false);
      expect(hasUsableETFData({ holdings: [], sectors: [], countries: [] })).toBe(false);
    });
  });
});
