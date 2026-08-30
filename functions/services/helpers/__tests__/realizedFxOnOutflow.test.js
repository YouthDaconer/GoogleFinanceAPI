/**
 * HU 2.5 — Tests de la diferencia en cambio que realiza una salida de divisa.
 *
 * @module services/helpers/__tests__/realizedFxOnOutflow.test
 * @see platform-docs/stories/2.5-diferencia-cambio-salidas-efectivo/refinamiento.md (T15)
 */

jest.mock('../../firebaseAdmin', () => {
  const mockAdmin = {
    firestore: jest.fn(() => ({
      collection: jest.fn(() => ({ doc: jest.fn(() => ({ get: jest.fn() })) })),
    })),
  };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };
  return mockAdmin;
});

const {
  computeOutflowRealizedFx,
  buildOutflowFxFields,
  OUTFLOW_FX_AVAILABILITY,
  OUTFLOW_UNAVAILABLE_REASONS,
  OUTFLOW_RATE_SOURCES,
} = require('../realizedFxOnOutflow');
const { buildBalanceUpdate, deriveAverageRate } = require('../balanceCostBasis');

// ============================================================================
// Fixtures
// ============================================================================

/**
 * Cuenta con un saldo en divisa extranjera y su base de costo.
 *
 * @param {number} balance - Saldo de la divisa
 * @param {number|null} cost - Costo en moneda de referencia
 * @param {Object} [overrides] - Sobrescrituras de la entrada de base de costo
 * @returns {Object} Documento de cuenta
 */
const accountWith = (balance, cost, overrides = {}) => ({
  balances: { USD: balance },
  balanceCostBasis: {
    USD: {
      cost,
      referenceCurrency: 'COP',
      status: cost === null ? 'unknown' : 'known',
      ...overrides,
    },
  },
});

/** Escenario 1: 1.000 USD adquiridos a un promedio de 4.000 */
const referenceAccount = () => accountWith(1000, 4000000);

describe('computeOutflowRealizedFx — la salida realiza la diferencia en cambio (HU 2.5)', () => {
  // ==========================================================================
  describe('Escenario 1 — retirar con la divisa a favor', () => {
    it('realiza +150.000 COP al retirar 500 USD comprados a 4.000 con la TRM en 4.300', () => {
      const result = computeOutflowRealizedFx({
        account: referenceAccount(),
        currency: 'USD',
        amount: 500,
        outflowRate: 4300,
        referenceCurrency: 'COP',
      });

      expect(result.realizedFxAmount).toBe(150000);
      expect(result.availability).toBe(OUTFLOW_FX_AVAILABILITY.AVAILABLE);
      expect(result.unavailableReason).toBeNull();
    });

    it('informa lo que costó y lo que vale lo que sale', () => {
      const result = computeOutflowRealizedFx({
        account: referenceAccount(),
        currency: 'USD',
        amount: 500,
        outflowRate: 4300,
        referenceCurrency: 'COP',
      });

      expect(result.releasedCost).toBe(2000000);
      expect(result.outflowValue).toBe(2150000);
      expect(result.averageRate).toBe(4000);
    });

    it('RN-08: la tasa promedio del remanente no cambia', () => {
      // La escritura del saldo NO la hace este helper: la hace
      // `buildBalanceUpdate` desde 2.1. Se mide sobre ella, que es la que manda.
      const account = referenceAccount();

      const update = buildBalanceUpdate({
        account,
        currency: 'USD',
        amountDelta: -500,
        referenceCurrency: 'COP',
      });

      const newBalance = update['balances.USD'];
      const newBasis = update['balanceCostBasis.USD'];

      expect(newBalance).toBe(500);
      expect(newBasis.cost).toBe(2000000);
      expect(deriveAverageRate(newBasis, newBalance, 'COP')).toBe(4000);
    });
  });

  // ==========================================================================
  describe('Escenario 3 — la divisa se movió en contra', () => {
    it('realiza −150.000 COP al retirar 500 USD comprados a 4.300 con la TRM en 4.000', () => {
      const result = computeOutflowRealizedFx({
        account: accountWith(1000, 4300000),
        currency: 'USD',
        amount: 500,
        outflowRate: 4000,
        referenceCurrency: 'COP',
      });

      expect(result.realizedFxAmount).toBe(-150000);
      expect(result.availability).toBe(OUTFLOW_FX_AVAILABILITY.AVAILABLE);
    });

    it('la pérdida no se disfraza: el signo es parte de la cifra', () => {
      const result = computeOutflowRealizedFx({
        account: accountWith(1000, 4300000),
        currency: 'USD',
        amount: 500,
        outflowRate: 4000,
        referenceCurrency: 'COP',
      });

      expect(result.realizedFxAmount).toBeLessThan(0);
    });

    it('la tasa promedio del remanente sigue siendo 4.300', () => {
      const account = accountWith(1000, 4300000);

      const update = buildBalanceUpdate({
        account,
        currency: 'USD',
        amountDelta: -500,
        referenceCurrency: 'COP',
      });

      expect(deriveAverageRate(update['balanceCostBasis.USD'], update['balances.USD'], 'COP'))
        .toBe(4300);
    });
  });

  // ==========================================================================
  describe('Escenario 4 — retirar en la propia moneda de referencia (RN-14)', () => {
    it('no realiza nada y lo declara como "no aplica", no como cero', () => {
      const result = computeOutflowRealizedFx({
        account: { balances: { COP: 5000000 } },
        currency: 'COP',
        amount: 1000000,
        outflowRate: 1,
        referenceCurrency: 'COP',
      });

      expect(result.availability).toBe(OUTFLOW_FX_AVAILABILITY.NOT_APPLICABLE);
      expect(result.realizedFxAmount).toBeNull();
    });

    it('el documento no gana ni un campo nuevo (AC-4)', () => {
      const outcome = computeOutflowRealizedFx({
        account: { balances: { COP: 5000000 } },
        currency: 'COP',
        amount: 1000000,
        outflowRate: 1,
        referenceCurrency: 'COP',
      });

      const fields = buildOutflowFxFields({
        outcome,
        referenceCurrency: 'COP',
        outflowRate: 1,
        outflowRateSource: OUTFLOW_RATE_SOURCES.IDENTITY,
      });

      expect(Object.keys(fields)).toHaveLength(0);
    });
  });

  // ==========================================================================
  describe('Escenario 8 — retiro que vacía el saldo', () => {
    it('realiza la diferencia en cambio de la totalidad', () => {
      const result = computeOutflowRealizedFx({
        account: accountWith(500, 2000000),
        currency: 'USD',
        amount: 500,
        outflowRate: 4300,
        referenceCurrency: 'COP',
      });

      expect(result.realizedFxAmount).toBe(150000);
      expect(result.releasedCost).toBe(2000000);
    });

    it('deja el saldo en cero con su costo también en cero', () => {
      const update = buildBalanceUpdate({
        account: accountWith(500, 2000000),
        currency: 'USD',
        amountDelta: -500,
        referenceCurrency: 'COP',
      });

      expect(update['balances.USD']).toBe(0);
      expect(update['balanceCostBasis.USD'].cost).toBe(0);
    });
  });

  // ==========================================================================
  describe('La identidad: valor de salida − costo liberado', () => {
    const cases = [
      { label: 'divisa a favor', balance: 1000, cost: 4000000, amount: 500, rate: 4300 },
      { label: 'divisa en contra', balance: 1000, cost: 4300000, amount: 500, rate: 4000 },
      { label: 'tasa sin cambio', balance: 1000, cost: 4000000, amount: 250, rate: 4000 },
      { label: 'salida total', balance: 750, cost: 3000000, amount: 750, rate: 4123.45 },
      { label: 'importe con decimales', balance: 1234.56, cost: 4938240, amount: 321.09, rate: 4111.11 },
    ];

    it.each(cases)('$label: la cifra es exactamente la resta de las dos puntas', ({ balance, cost, amount, rate }) => {
      const result = computeOutflowRealizedFx({
        account: accountWith(balance, cost),
        currency: 'USD',
        amount,
        outflowRate: rate,
        referenceCurrency: 'COP',
      });

      // En céntimos enteros, que es la precisión en la que el producto presenta
      // dinero: en coma flotante binaria la resta de dos redondeos puede dejar
      // residuo, y ese residuo es la representación del número, no la cifra.
      const cents = (value) => Math.round(value * 100);
      expect(cents(result.outflowValue) - cents(result.releasedCost))
        .toBe(cents(result.realizedFxAmount));
    });

    it('sin movimiento del tipo de cambio la diferencia es cero', () => {
      const result = computeOutflowRealizedFx({
        account: accountWith(1000, 4000000),
        currency: 'USD',
        amount: 500,
        outflowRate: 4000,
        referenceCurrency: 'COP',
      });

      expect(result.realizedFxAmount).toBe(0);
      expect(result.availability).toBe(OUTFLOW_FX_AVAILABILITY.AVAILABLE);
    });
  });

  // ==========================================================================
  describe('RN-13 — un dato ausente se declara ausente', () => {
    it('sin base de costo utilizable no inventa una tasa promedio', () => {
      const result = computeOutflowRealizedFx({
        account: accountWith(1000, null),
        currency: 'USD',
        amount: 500,
        outflowRate: 4300,
        referenceCurrency: 'COP',
      });

      expect(result.availability).toBe(OUTFLOW_FX_AVAILABILITY.UNAVAILABLE);
      expect(result.unavailableReason).toBe(OUTFLOW_UNAVAILABLE_REASONS.UNKNOWN_COST_BASIS);
      expect(result.realizedFxAmount).toBeNull();
    });

    it('con la moneda de referencia cambiada el costo ya no describe el saldo', () => {
      const result = computeOutflowRealizedFx({
        account: accountWith(1000, 4000000, { referenceCurrency: 'BRL' }),
        currency: 'USD',
        amount: 500,
        outflowRate: 4300,
        referenceCurrency: 'COP',
      });

      expect(result.unavailableReason).toBe(OUTFLOW_UNAVAILABLE_REASONS.UNKNOWN_COST_BASIS);
    });

    it('sin tasa del día declara el motivo, pero conserva lo que sí sabe', () => {
      const result = computeOutflowRealizedFx({
        account: referenceAccount(),
        currency: 'USD',
        amount: 500,
        outflowRate: null,
        referenceCurrency: 'COP',
      });

      expect(result.availability).toBe(OUTFLOW_FX_AVAILABILITY.UNAVAILABLE);
      expect(result.unavailableReason).toBe(OUTFLOW_UNAVAILABLE_REASONS.MISSING_REALIZATION_RATE);
      expect(result.realizedFxAmount).toBeNull();
      // Lo que ese dinero costó sí se sabe: perderlo obligaría a reconstruirlo.
      expect(result.releasedCost).toBe(2000000);
    });

    it('una tasa de cero o negativa no es una tasa', () => {
      for (const rate of [0, -4300]) {
        const result = computeOutflowRealizedFx({
          account: referenceAccount(),
          currency: 'USD',
          amount: 500,
          outflowRate: rate,
          referenceCurrency: 'COP',
        });

        expect(result.unavailableReason).toBe(OUTFLOW_UNAVAILABLE_REASONS.MISSING_REALIZATION_RATE);
      }
    });

    it('ningún camino devuelve cero efecto divisa por no saberlo', () => {
      const casos = [
        { account: accountWith(1000, null), outflowRate: 4300 },
        { account: referenceAccount(), outflowRate: null },
      ];

      for (const caso of casos) {
        const result = computeOutflowRealizedFx({
          currency: 'USD',
          amount: 500,
          referenceCurrency: 'COP',
          ...caso,
        });

        expect(result.realizedFxAmount).not.toBe(0);
        expect(result.realizedFxAmount).toBeNull();
      }
    });
  });

  // ==========================================================================
  describe('buildOutflowFxFields — lo que se persiste', () => {
    it('escribe los siete campos con la tasa que declaró el usuario', () => {
      const outcome = computeOutflowRealizedFx({
        account: referenceAccount(),
        currency: 'USD',
        amount: 500,
        outflowRate: 4300,
        referenceCurrency: 'COP',
      });

      const fields = buildOutflowFxFields({
        outcome,
        referenceCurrency: 'COP',
        outflowRate: 4300,
        outflowRateSource: OUTFLOW_RATE_SOURCES.USER,
      });

      expect(fields).toEqual({
        referenceCurrency: 'COP',
        realizationRate: 4300,
        realizationRateSource: 'user',
        releasedCost: 2000000,
        realizedFxAmount: 150000,
        realizedFxCurrency: 'COP',
        realizedFxAvailability: 'available',
        realizedFxUnavailableReason: null,
      });
    });

    it('sin tasa deja el campo en null y el origen en "unavailable"', () => {
      const outcome = computeOutflowRealizedFx({
        account: referenceAccount(),
        currency: 'USD',
        amount: 500,
        outflowRate: null,
        referenceCurrency: 'COP',
      });

      const fields = buildOutflowFxFields({
        outcome,
        referenceCurrency: 'COP',
        outflowRate: null,
        outflowRateSource: null,
      });

      expect(fields.realizationRate).toBeNull();
      expect(fields.realizationRateSource).toBe('unavailable');
      expect(fields.realizedFxAvailability).toBe('unavailable');
    });

    it('la tasa se persiste con seis decimales, no con la basura del flotante', () => {
      const outcome = computeOutflowRealizedFx({
        account: referenceAccount(),
        currency: 'USD',
        amount: 500,
        outflowRate: 4300.123456789,
        referenceCurrency: 'COP',
      });

      const fields = buildOutflowFxFields({
        outcome,
        referenceCurrency: 'COP',
        outflowRate: 4300.123456789,
        outflowRateSource: OUTFLOW_RATE_SOURCES.MARKET_DATE,
      });

      expect(fields.realizationRate).toBe(4300.123457);
    });
  });
});
