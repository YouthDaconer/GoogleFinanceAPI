/**
 * HU 2.1 — Tests de la base de costo de los saldos de efectivo.
 *
 * @module __tests__/services/balanceCostBasis.test
 * @see platform-docs/stories/2.1-base-costo-saldo-efectivo/refinamiento.md (T4, T16)
 */

const mockGet = jest.fn();
const mockDoc = jest.fn(() => ({ get: mockGet }));
const mockCollection = jest.fn(() => ({ doc: mockDoc }));

jest.mock('../firebaseAdmin', () => {
  const mockAdmin = {
    firestore: jest.fn(() => ({ collection: mockCollection, doc: mockDoc })),
  };

  mockAdmin.firestore.FieldValue = {
    serverTimestamp: () => 'SERVER_TIMESTAMP',
  };

  return mockAdmin;
});

const {
  buildBalanceUpdate,
  deriveAverageRate,
  getUserReferenceCurrency,
  COST_BASIS_STATUS,
} = require('../helpers/balanceCostBasis');

/** Cuenta con saldo y base de costo conocidos */
const accountWith = (balance, cost, status = COST_BASIS_STATUS.KNOWN, referenceCurrency = 'COP') => ({
  balances: { USD: balance },
  balanceCostBasis: { USD: { cost, referenceCurrency, status } },
});

describe('balanceCostBasis', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('buildBalanceUpdate — entradas', () => {
    it('el primer ingreso deja el saldo con su costo de adquisición', () => {
      const update = buildBalanceUpdate({
        account: { balances: {} },
        currency: 'USD',
        amountDelta: 1000,
        costDelta: 4000000,
        referenceCurrency: 'COP',
      });

      expect(update['balances.USD']).toBe(1000);
      expect(update['balanceCostBasis.USD']).toMatchObject({
        cost: 4000000,
        referenceCurrency: 'COP',
        status: COST_BASIS_STATUS.KNOWN,
      });
    });

    it('ingresos sucesivos a tasas distintas producen el promedio ponderado (escenario 5)', () => {
      // 1.000 USD a 4.000 + 1.000 USD a 4.400 → 2.000 USD, 8.400.000 COP, promedio 4.200
      const update = buildBalanceUpdate({
        account: accountWith(1000, 4000000),
        currency: 'USD',
        amountDelta: 1000,
        costDelta: 4400000,
        referenceCurrency: 'COP',
      });

      expect(update['balances.USD']).toBe(2000);
      expect(update['balanceCostBasis.USD'].cost).toBe(8400000);

      const averageRate = deriveAverageRate(
        { ...update['balanceCostBasis.USD'] },
        update['balances.USD'],
        'COP'
      );
      expect(averageRate).toBe(4200);
    });

    it('el promedio no depende del orden de los ingresos (RN-02)', () => {
      const forward = buildBalanceUpdate({
        account: accountWith(1000, 4000000),
        currency: 'USD',
        amountDelta: 1000,
        costDelta: 4400000,
        referenceCurrency: 'COP',
      });

      const reverse = buildBalanceUpdate({
        account: accountWith(1000, 4400000),
        currency: 'USD',
        amountDelta: 1000,
        costDelta: 4000000,
        referenceCurrency: 'COP',
      });

      expect(forward['balanceCostBasis.USD'].cost).toBe(reverse['balanceCostBasis.USD'].cost);
    });

    it('una entrada sin costo conocido declara el saldo indeterminado, no cero (RN-13)', () => {
      const update = buildBalanceUpdate({
        account: accountWith(1000, 4000000),
        currency: 'USD',
        amountDelta: 1200,
        costDelta: null,
        referenceCurrency: 'COP',
      });

      expect(update['balances.USD']).toBe(2200);
      expect(update['balanceCostBasis.USD']).toMatchObject({
        cost: null,
        status: COST_BASIS_STATUS.UNKNOWN,
      });
    });

    it('una entrada con costo conocido no rescata un saldo ya indeterminado', () => {
      const update = buildBalanceUpdate({
        account: accountWith(1000, null, COST_BASIS_STATUS.UNKNOWN),
        currency: 'USD',
        amountDelta: 500,
        costDelta: 2000000,
        referenceCurrency: 'COP',
      });

      expect(update['balanceCostBasis.USD'].status).toBe(COST_BASIS_STATUS.UNKNOWN);
    });

    it('descarta un costo expresado en otra moneda de referencia (D3)', () => {
      const update = buildBalanceUpdate({
        account: accountWith(1000, 4000000, COST_BASIS_STATUS.KNOWN, 'MXN'),
        currency: 'USD',
        amountDelta: 1000,
        costDelta: 4400000,
        referenceCurrency: 'COP',
      });

      // El costo viejo no se reinterpreta: solo cuenta lo que entra ahora
      expect(update['balanceCostBasis.USD'].cost).toBe(4400000);
      expect(update['balanceCostBasis.USD'].referenceCurrency).toBe('COP');
    });
  });

  describe('buildBalanceUpdate — salidas', () => {
    it('retira costo a la tasa promedio y deja el promedio intacto', () => {
      const update = buildBalanceUpdate({
        account: accountWith(2000, 8400000), // promedio 4.200
        currency: 'USD',
        amountDelta: -500,
        referenceCurrency: 'COP',
      });

      expect(update['balances.USD']).toBe(1500);
      expect(update['balanceCostBasis.USD'].cost).toBe(6300000); // 1.500 × 4.200

      const averageRate = deriveAverageRate(update['balanceCostBasis.USD'], 1500, 'COP');
      expect(averageRate).toBe(4200);
    });

    it('una salida sobre un saldo indeterminado lo deja indeterminado', () => {
      const update = buildBalanceUpdate({
        account: accountWith(1000, null, COST_BASIS_STATUS.UNKNOWN),
        currency: 'USD',
        amountDelta: -400,
        referenceCurrency: 'COP',
      });

      expect(update['balanceCostBasis.USD'].status).toBe(COST_BASIS_STATUS.UNKNOWN);
    });

    it('un saldo que llega a cero reinicia su base de costo', () => {
      const update = buildBalanceUpdate({
        account: accountWith(1000, 4000000),
        currency: 'USD',
        amountDelta: -1000,
        referenceCurrency: 'COP',
      });

      expect(update['balances.USD']).toBe(0);
      expect(update['balanceCostBasis.USD']).toMatchObject({
        cost: 0,
        status: COST_BASIS_STATUS.KNOWN,
      });
    });
  });

  describe('buildBalanceUpdate — sin exposición cambiaria (RN-14)', () => {
    it('no lleva base de costo del efectivo en la propia moneda de referencia', () => {
      const update = buildBalanceUpdate({
        account: { balances: { COP: 2500000 } },
        currency: 'COP',
        amountDelta: 500000,
        costDelta: 500000,
        referenceCurrency: 'COP',
      });

      expect(update).toEqual({ 'balances.COP': 3000000 });
      expect(update['balanceCostBasis.COP']).toBeUndefined();
    });

    it('un usuario con referencia USD que mueve dólares no gana ningún campo', () => {
      const update = buildBalanceUpdate({
        account: { balances: { USD: 1000 } },
        currency: 'USD',
        amountDelta: 250,
        costDelta: 250,
        referenceCurrency: 'USD',
      });

      expect(Object.keys(update)).toEqual(['balances.USD']);
    });
  });

  describe('buildBalanceUpdate — el saldo conserva su forma (RN-16)', () => {
    it('escribe balances con la misma precisión que antes del cambio', () => {
      const update = buildBalanceUpdate({
        account: { balances: { USD: 100.1 } },
        currency: 'USD',
        amountDelta: 0.2,
        costDelta: 800,
        referenceCurrency: 'COP',
      });

      expect(update['balances.USD']).toBe(100.3);
    });
  });

  describe('deriveAverageRate', () => {
    it('no inventa una tasa cuando el saldo es cero', () => {
      expect(deriveAverageRate({ cost: 4000000, referenceCurrency: 'COP', status: 'known' }, 0, 'COP'))
        .toBeNull();
    });

    it('no inventa una tasa cuando el costo es indeterminado', () => {
      expect(deriveAverageRate({ cost: null, referenceCurrency: 'COP', status: 'unknown' }, 1000, 'COP'))
        .toBeNull();
    });

    it('no inventa una tasa cuando la moneda de referencia cambió', () => {
      expect(deriveAverageRate({ cost: 4000000, referenceCurrency: 'MXN', status: 'known' }, 1000, 'COP'))
        .toBeNull();
    });
  });

  describe('getUserReferenceCurrency', () => {
    it('devuelve la moneda de referencia configurada', async () => {
      mockGet.mockResolvedValueOnce({ data: () => ({ defaultCurrency: 'COP' }) });

      await expect(getUserReferenceCurrency('user-1')).resolves.toBe('COP');
    });

    it('cae a USD si el usuario no la tiene configurada', async () => {
      mockGet.mockResolvedValueOnce({ data: () => ({}) });

      await expect(getUserReferenceCurrency('user-1')).resolves.toBe('USD');
    });

    it('cae a USD si la lectura falla, sin propagar el error', async () => {
      mockGet.mockRejectedValueOnce(new Error('unavailable'));

      await expect(getUserReferenceCurrency('user-1')).resolves.toBe('USD');
    });
  });
});
