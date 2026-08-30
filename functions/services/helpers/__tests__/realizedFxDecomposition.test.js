/**
 * HU 2.4 — Tests de la descomposición del resultado realizado.
 *
 * @module __tests__/services/helpers/realizedFxDecomposition.test
 * @see platform-docs/stories/2.4-venta-dividendo-tasa-del-dia/refinamiento.md (T1, T14)
 */

const mockGetCrossRate = jest.fn();

jest.mock('../../historicalRateService', () => ({
  getCrossRate: (...args) => mockGetCrossRate(...args),
  getRateForDate: jest.fn(),
}));

jest.mock('../../firebaseAdmin', () => {
  const mockAdmin = {
    firestore: jest.fn(() => ({ collection: jest.fn(), doc: jest.fn() })),
  };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };
  return mockAdmin;
});

const {
  resolveLotAcquisitionRate,
  resolveRealizationRate,
  decomposeRealizedResult,
  buildRealizedFxFields,
  ACQUISITION_RATE_SOURCES,
  REALIZATION_RATE_SOURCES,
  DECOMPOSITION_AVAILABILITY,
  UNAVAILABLE_REASONS,
} = require('../realizedFxDecomposition');

beforeEach(() => {
  jest.clearAllMocks();
  mockGetCrossRate.mockResolvedValue(null);
});

// ============================================================================
// AC-1, AC-2, AC-3 — la descomposición
// ============================================================================

describe('decomposeRealizedResult — el caso de referencia (AC-1)', () => {
  // Compra por 1.000 USD con la TRM en 4.000; venta por 1.200 USD con la TRM en 4.300
  const referenceCase = {
    grossProceeds: 1200,
    invested: 1000,
    acquisitionRate: 4000,
    realizationRate: 4300,
  };

  it('reporta 1.160.000 descompuestos en 800.000 de activo y 360.000 de divisa', () => {
    const result = decomposeRealizedResult(referenceCase);

    expect(result.assetMeritAmount).toBe(800000);
    expect(result.realizedFxAmount).toBe(360000);
    expect(result.realizedTotalAmount).toBe(1160000);
    expect(result.availability).toBe(DECOMPOSITION_AVAILABILITY.AVAILABLE);
  });

  it('el total coincide con valorar cada punta a la tasa de su propio día', () => {
    const { realizedTotalAmount } = decomposeRealizedResult(referenceCase);

    // 1200 × 4300 − 1000 × 4000
    expect(realizedTotalAmount).toBe(1200 * 4300 - 1000 * 4000);
  });

  it('lo que el sistema reportaba antes —el resultado en dólares por la tasa de compra— era 800.000', () => {
    // Deja constancia del defecto que corrige la historia: el mérito solo.
    const { assetMeritAmount, realizedTotalAmount } = decomposeRealizedResult(referenceCase);

    expect(assetMeritAmount).toBe((1200 - 1000) * 4000);
    expect(realizedTotalAmount - assetMeritAmount).toBe(360000);
  });
});

describe('decomposeRealizedResult — la suma es exacta (AC-2)', () => {
  const cases = [
    { name: 'ganancia con la divisa a favor', grossProceeds: 1200, invested: 1000, acquisitionRate: 4000, realizationRate: 4300 },
    { name: 'pérdida en el activo con la divisa a favor', grossProceeds: 800, invested: 1000, acquisitionRate: 4000, realizationRate: 4300 },
    { name: 'ganancia con la divisa en contra', grossProceeds: 1200, invested: 1000, acquisitionRate: 4300, realizationRate: 4000 },
    { name: 'cifras con decimales', grossProceeds: 1337.77, invested: 991.13, acquisitionRate: 4123.45, realizationRate: 4387.91 },
    { name: 'la divisa no se movió', grossProceeds: 1200, invested: 1000, acquisitionRate: 4000, realizationRate: 4000 },
  ];

  // La comparación se hace en céntimos enteros, que es la precisión en la que el
  // producto presenta dinero. En coma flotante binaria `800000.01 + 360000.35`
  // puede dar `1160000.3599999999`: es la representación del número, no un
  // residuo de la descomposición. Lo que el usuario debe poder comprobar es que
  // las dos cifras que ve suman la que ve arriba, y eso es exacto.
  const cents = (value) => Math.round(value * 100);

  it.each(cases)('mérito + divisa = total en $name, sin residuo', (testCase) => {
    const { assetMeritAmount, realizedFxAmount, realizedTotalAmount } =
      decomposeRealizedResult(testCase);

    expect(cents(assetMeritAmount) + cents(realizedFxAmount)).toBe(cents(realizedTotalAmount));
  });
});

describe('decomposeRealizedResult — la divisa se movió en contra (AC-3)', () => {
  // Compra con la TRM en 4.300, venta con la TRM en 4.000, con ganancia en dólares
  const result = () => decomposeRealizedResult({
    grossProceeds: 1200,
    invested: 1000,
    acquisitionRate: 4300,
    realizationRate: 4000,
  });

  it('el efecto divisa se reporta negativo', () => {
    expect(result().realizedFxAmount).toBe(-360000);
  });

  it('el resultado total es menor que el mérito del activo y la suma sigue cuadrando', () => {
    const { assetMeritAmount, realizedFxAmount, realizedTotalAmount } = result();

    expect(assetMeritAmount).toBe(860000);
    expect(realizedTotalAmount).toBe(500000);
    expect(realizedTotalAmount).toBeLessThan(assetMeritAmount);
    expect(assetMeritAmount + realizedFxAmount).toBe(realizedTotalAmount);
  });
});

describe('decomposeRealizedResult — una ausencia se declara (RN-13, AC-9)', () => {
  it('sin tasa de compra no se reporta cero efecto divisa', () => {
    const result = decomposeRealizedResult({
      grossProceeds: 1200,
      invested: 1000,
      acquisitionRate: null,
      realizationRate: 4300,
    });

    expect(result.availability).toBe(DECOMPOSITION_AVAILABILITY.UNAVAILABLE);
    expect(result.unavailableReason).toBe(UNAVAILABLE_REASONS.MISSING_ACQUISITION_RATE);
    expect(result.realizedFxAmount).toBeNull();
    expect(result.realizedTotalAmount).toBeNull();
  });

  it('sin tasa del día de la venta tampoco', () => {
    const result = decomposeRealizedResult({
      grossProceeds: 1200,
      invested: 1000,
      acquisitionRate: 4000,
      realizationRate: null,
    });

    expect(result.availability).toBe(DECOMPOSITION_AVAILABILITY.UNAVAILABLE);
    expect(result.unavailableReason).toBe(UNAVAILABLE_REASONS.MISSING_REALIZATION_RATE);
    expect(result.assetMeritAmount).toBeNull();
  });

  it('una tasa de cero o negativa se trata como ausencia, no como cifra', () => {
    expect(decomposeRealizedResult({
      grossProceeds: 1200, invested: 1000, acquisitionRate: 0, realizationRate: 4300,
    }).availability).toBe(DECOMPOSITION_AVAILABILITY.UNAVAILABLE);

    expect(decomposeRealizedResult({
      grossProceeds: 1200, invested: 1000, acquisitionRate: 4000, realizationRate: -1,
    }).availability).toBe(DECOMPOSITION_AVAILABILITY.UNAVAILABLE);
  });
});

describe('decomposeRealizedResult — sin exposición cambiaria (AC-10, RN-14)', () => {
  it('con ambas tasas en 1 el efecto divisa es cero y el mérito es el resultado', () => {
    const result = decomposeRealizedResult({
      grossProceeds: 1200,
      invested: 1000,
      acquisitionRate: 1,
      realizationRate: 1,
    });

    expect(result.assetMeritAmount).toBe(200);
    expect(result.realizedFxAmount).toBe(0);
    expect(result.realizedTotalAmount).toBe(200);
  });
});

// ============================================================================
// D4 — precedencia de la tasa de compra del lote
// ============================================================================

describe('resolveLotAcquisitionRate — precedencia (D4)', () => {
  it('1. la divisa del activo es la de referencia → identidad', async () => {
    const result = await resolveLotAcquisitionRate({ currency: 'COP' }, 'COP');

    expect(result).toEqual({
      acquisitionRate: 1,
      acquisitionRateSource: ACQUISITION_RATE_SOURCES.IDENTITY,
    });
    expect(mockGetCrossRate).not.toHaveBeenCalled();
  });

  it('2. el activo trae la tasa que derivó 2.3 → asset', async () => {
    const result = await resolveLotAcquisitionRate({
      currency: 'USD',
      acquisitionRate: 4150,
      referenceCurrency: 'COP',
      acquisitionDollarValue: 9999,
    }, 'COP');

    expect(result.acquisitionRate).toBe(4150);
    expect(result.acquisitionRateSource).toBe(ACQUISITION_RATE_SOURCES.ASSET);
  });

  it('2b. la tasa del activo se descarta si su moneda de referencia ya no es la vigente', async () => {
    mockGetCrossRate.mockResolvedValue({ rate: 5.4, rateDate: '2026-03-12' });

    const result = await resolveLotAcquisitionRate({
      currency: 'USD',
      acquisitionRate: 4150,
      referenceCurrency: 'COP',
      acquisitionDate: '2026-03-12',
    }, 'BRL');

    expect(result.acquisitionRate).toBe(5.4);
    expect(result.acquisitionRateSource).toBe(ACQUISITION_RATE_SOURCES.MARKET_DATE);
  });

  it('3. activo en USD anclado a la moneda de referencia → el campo antiguo YA es la tasa', async () => {
    const result = await resolveLotAcquisitionRate({
      currency: 'USD',
      acquisitionDollarValue: 4000,
      defaultCurrencyForAdquisitionDollar: 'COP',
      acquisitionDate: '2026-03-12',
    }, 'COP');

    expect(result.acquisitionRate).toBe(4000);
    expect(result.acquisitionRateSource).toBe(ACQUISITION_RATE_SOURCES.LEGACY);
    expect(mockGetCrossRate).not.toHaveBeenCalled();
  });

  it('3b. el campo antiguo NO se usa si su ancla es otra moneda', async () => {
    mockGetCrossRate.mockResolvedValue({ rate: 4321, rateDate: '2026-03-12' });

    const result = await resolveLotAcquisitionRate({
      currency: 'USD',
      acquisitionDollarValue: 4000,
      defaultCurrencyForAdquisitionDollar: 'USD',
      acquisitionDate: '2026-03-12',
    }, 'COP');

    expect(result.acquisitionRate).toBe(4321);
    expect(result.acquisitionRateSource).toBe(ACQUISITION_RATE_SOURCES.MARKET_DATE);
  });

  it('4. sin nada de lo anterior → tasa de mercado de la fecha de adquisición', async () => {
    mockGetCrossRate.mockResolvedValue({ rate: 4500, rateDate: '2026-01-15' });

    const result = await resolveLotAcquisitionRate({
      currency: 'EUR',
      acquisitionDate: '2026-01-15T18:30:00.000Z',
    }, 'COP');

    expect(mockGetCrossRate).toHaveBeenCalledWith('EUR', 'COP', '2026-01-15');
    expect(result.acquisitionRate).toBe(4500);
  });

  it('5. tampoco hay tasa de mercado → unavailable, nunca cero', async () => {
    mockGetCrossRate.mockResolvedValue(null);

    const result = await resolveLotAcquisitionRate({
      currency: 'EUR',
      acquisitionDate: '2026-01-15',
    }, 'COP');

    expect(result.acquisitionRate).toBeNull();
    expect(result.acquisitionRateSource).toBe(ACQUISITION_RATE_SOURCES.UNAVAILABLE);
  });

  it('un fallo del proveedor de tasas se trata como ausencia, no propaga el error', async () => {
    mockGetCrossRate.mockRejectedValue(new Error('Yahoo caído'));

    const result = await resolveLotAcquisitionRate({
      currency: 'EUR',
      acquisitionDate: '2026-01-15',
    }, 'COP');

    expect(result.acquisitionRateSource).toBe(ACQUISITION_RATE_SOURCES.UNAVAILABLE);
  });
});

// ============================================================================
// RN-03 — la tasa del día en que el dinero nace
// ============================================================================

describe('resolveRealizationRate', () => {
  it('sin exposición cambiaria la tasa es 1 y no se consulta nada (AC-10)', async () => {
    const result = await resolveRealizationRate('COP', 'COP', '2026-08-28');

    expect(result.realizationRate).toBe(1);
    expect(result.realizationRateSource).toBe(REALIZATION_RATE_SOURCES.IDENTITY);
    expect(mockGetCrossRate).not.toHaveBeenCalled();
  });

  it('toma la fecha del día que eligió el usuario, no el instante UTC compuesto', async () => {
    mockGetCrossRate.mockResolvedValue({ rate: 4300, rateDate: '2026-08-28' });

    await resolveRealizationRate('USD', 'COP', '2026-08-28T23:45:00.000Z');

    expect(mockGetCrossRate).toHaveBeenCalledWith('USD', 'COP', '2026-08-28');
  });

  it('informa el cierre anterior cuando la fecha no cotiza', async () => {
    mockGetCrossRate.mockResolvedValue({ rate: 4290, rateDate: '2026-08-27', source: 'previous-close' });

    const result = await resolveRealizationRate('USD', 'COP', '2026-08-28');

    expect(result.realizationRate).toBe(4290);
    expect(result.realizationRateDate).toBe('2026-08-27');
  });

  it('sin tasa devuelve unavailable en lugar de una tasa inventada', async () => {
    mockGetCrossRate.mockResolvedValue(null);

    const result = await resolveRealizationRate('USD', 'COP', '2026-08-28');

    expect(result.realizationRate).toBeNull();
    expect(result.realizationRateSource).toBe(REALIZATION_RATE_SOURCES.UNAVAILABLE);
  });

  it('una fecha ilegible es una ausencia, no una excepción', async () => {
    const result = await resolveRealizationRate('USD', 'COP', 'ayer');

    expect(result.realizationRateSource).toBe(REALIZATION_RATE_SOURCES.UNAVAILABLE);
    expect(mockGetCrossRate).not.toHaveBeenCalled();
  });
});

// ============================================================================
// D1 — los campos que se persisten
// ============================================================================

describe('buildRealizedFxFields', () => {
  it('devuelve los campos de trazabilidad con el vocabulario de la épica', () => {
    const decomposition = decomposeRealizedResult({
      grossProceeds: 1200, invested: 1000, acquisitionRate: 4000, realizationRate: 4300,
    });

    const fields = buildRealizedFxFields({
      referenceCurrency: 'COP',
      acquisitionRate: 4000,
      acquisitionRateSource: ACQUISITION_RATE_SOURCES.ASSET,
      realizationRate: 4300,
      realizationRateSource: REALIZATION_RATE_SOURCES.MARKET_DATE,
      decomposition,
      acquisitionCost: 5160000,
    });

    expect(fields).toEqual({
      referenceCurrency: 'COP',
      acquisitionRate: 4000,
      acquisitionRateSource: 'asset',
      realizationRate: 4300,
      realizationRateSource: 'market-date',
      assetMeritAmount: 800000,
      realizedFxAmount: 360000,
      realizedTotalAmount: 1160000,
      realizedFxAvailability: 'available',
      realizedFxUnavailableReason: null,
      acquisitionCost: 5160000,
    });
  });

  it('no inventa cifras cuando la descomposición no está disponible', () => {
    const decomposition = decomposeRealizedResult({
      grossProceeds: 1200, invested: 1000, acquisitionRate: null, realizationRate: null,
    });

    const fields = buildRealizedFxFields({
      referenceCurrency: 'COP',
      acquisitionRate: null,
      acquisitionRateSource: ACQUISITION_RATE_SOURCES.UNAVAILABLE,
      realizationRate: null,
      realizationRateSource: REALIZATION_RATE_SOURCES.UNAVAILABLE,
      decomposition,
      acquisitionCost: null,
    });

    expect(fields.assetMeritAmount).toBeNull();
    expect(fields.realizedFxAmount).toBeNull();
    expect(fields.acquisitionCost).toBeNull();
    expect(fields.realizedFxAvailability).toBe('unavailable');
  });
});
