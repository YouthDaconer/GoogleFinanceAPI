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
  basisValuationNeed,
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

    it('no reinterpreta un costo en otra moneda: valora lo que había a la tasa del día (D3, RN-2.8-E)', () => {
      const update = buildBalanceUpdate({
        account: accountWith(1000, 4000000, COST_BASIS_STATUS.KNOWN, 'MXN'),
        currency: 'USD',
        amountDelta: 1000,
        costDelta: 4400000,
        referenceCurrency: 'COP',
        basisValuation: { referenceCurrency: 'COP', rate: 4300 },
      });

      // Los 4.000.000 MXN no se leen como pesos; los 1.000 USD que ya estaban
      // tampoco salen gratis (antes de la HU 2.8 la base quedaba en 4.400.000).
      expect(update['balanceCostBasis.USD'].cost).toBe(8700000);
      expect(update['balanceCostBasis.USD'].referenceCurrency).toBe('COP');
    });

    it('sin la tasa del día para lo que había, la base se declara desconocida (RN-2.8-F)', () => {
      const update = buildBalanceUpdate({
        account: accountWith(1000, 4000000, COST_BASIS_STATUS.KNOWN, 'MXN'),
        currency: 'USD',
        amountDelta: 1000,
        costDelta: 4400000,
        referenceCurrency: 'COP',
      });

      expect(update['balanceCostBasis.USD'].status).toBe(COST_BASIS_STATUS.UNKNOWN);
      expect(update['balanceCostBasis.USD'].cost).toBeNull();
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

  // ==========================================================================
  // HU 2.8 — la base sobrevive al cambio de moneda de referencia
  // ==========================================================================

  /**
   * Aplica un fragmento al estado, como hace Firestore con el `update`: así se
   * encadenan movimientos y cambios de referencia igual que en producción.
   */
  const apply = (account, update, currency = 'USD') => ({
    balances: { ...account.balances, [currency]: update[`balances.${currency}`] },
    balanceCostBasis: update[`balanceCostBasis.${currency}`] !== undefined
      ? { ...account.balanceCostBasis, [currency]: update[`balanceCostBasis.${currency}`] }
      : account.balanceCostBasis,
  });

  /** Tasa promedio que vería la tarjeta con la referencia en pesos */
  const copRate = (account) => deriveAverageRate(
    account.balanceCostBasis.USD, account.balances.USD, 'COP',
  );

  describe('HU 2.8 — salidas con otra moneda de referencia (QA-01, RN-2.8-B)', () => {
    it.each([
      ['una compra'],
      ['un retiro'],
      ['una conversión'],
    ])('%s de la mitad del saldo con referencia USD deja la base en pesos a la mitad y la tasa intacta', () => {
      // 200 USD a 3.300 COP; el usuario cambia a USD y saca 100.
      const account = accountWith(200, 660000);
      const after = apply(account, buildBalanceUpdate({
        account, currency: 'USD', amountDelta: -100, referenceCurrency: 'USD',
      }));

      expect(after.balances.USD).toBe(100);
      expect(after.balanceCostBasis.USD).toMatchObject({
        cost: 330000, referenceCurrency: 'COP', status: COST_BASIS_STATUS.KNOWN,
      });
      // Al volver a COP la tarjeta ve exactamente la tasa que declaró.
      expect(copRate(after)).toBe(3300);
    });

    it('no pide tasa para una salida (RN-2.8-C)', () => {
      expect(basisValuationNeed({
        account: accountWith(200, 660000), currency: 'USD', amountDelta: -100, referenceCurrency: 'USD',
      })).toBeNull();
    });
  });

  describe('HU 2.8 — entradas con otra moneda de referencia (QA-02, QA-03, RN-2.8-D)', () => {
    it('pide la tasa del día en pesos para lo que entra', () => {
      expect(basisValuationNeed({
        account: accountWith(100, 330000), currency: 'USD', amountDelta: 100, costDelta: 100, referenceCurrency: 'USD',
      })).toBe('COP');
    });

    it.each([
      ['una venta'],
      ['un dividendo'],
      ['un ingreso'],
    ])('%s de 100 USD a 3.500 deja 680.000 COP sobre 200 USD (3.400)', () => {
      const account = accountWith(100, 330000);
      const after = apply(account, buildBalanceUpdate({
        account,
        currency: 'USD',
        amountDelta: 100,
        // Con referencia USD el costo que llega está en dólares: no se usa.
        costDelta: 100,
        referenceCurrency: 'USD',
        basisValuation: { referenceCurrency: 'COP', rate: 3500 },
      }));

      expect(after.balances.USD).toBe(200);
      expect(after.balanceCostBasis.USD.cost).toBe(680000);
      expect(copRate(after)).toBe(3400);
    });

    it('lo que entra valorado es traducido, no comprado: no suma a la porción convertida (QA-03)', () => {
      const account = {
        balances: { USD: 100 },
        balanceCostBasis: { USD: {
          cost: 330000, referenceCurrency: 'COP', status: COST_BASIS_STATUS.KNOWN,
          convertedAmount: 0, convertedCost: 0,
        } },
      };
      const update = buildBalanceUpdate({
        account,
        currency: 'USD',
        amountDelta: 100,
        referenceCurrency: 'USD',
        convertedDelta: { amount: 100, cost: 100 },
        basisValuation: { referenceCurrency: 'COP', rate: 3500 },
      });

      expect(update['balanceCostBasis.USD'].convertedAmount).toBe(0);
    });
  });

  describe('HU 2.8 — lo declarado no se recalcula (QA-04, RN-2.8-C)', () => {
    /** Ingreso de 101 USD declarado a 3.326,95 con referencia COP, sobre saldo cero */
    const declaredIncome = () => {
      const empty = { balances: { USD: 0 }, balanceCostBasis: {} };
      return apply(empty, buildBalanceUpdate({
        account: empty, currency: 'USD', amountDelta: 101, costDelta: 101 * 3326.95, referenceCurrency: 'COP',
      }));
    };

    it('alternar la referencia sin mover el saldo no toca el valor ni la tasa', () => {
      let account = declaredIncome();
      const before = { ...account.balanceCostBasis.USD };

      // Cambiar de referencia no escribe la base: lo único que puede llegar es
      // un movimiento. Aun uno nulo, bajo cualquier referencia, la deja igual.
      for (const reference of ['USD', 'COP', 'USD', 'COP']) {
        account = apply(account, buildBalanceUpdate({
          account, currency: 'USD', amountDelta: 0, referenceCurrency: reference,
        }));
      }

      expect(account.balanceCostBasis.USD.cost).toBe(before.cost);
      expect(copRate(account)).toBeCloseTo(3326.95, 6);
    });

    it('la tasa declarada sobrevive a una salida bajo USD', () => {
      let account = declaredIncome();
      account = apply(account, buildBalanceUpdate({
        account, currency: 'USD', amountDelta: -50.5, referenceCurrency: 'USD',
      }));

      // El costo se guarda a centavos (convención monetaria del helper), así
      // que la tasa puede moverse en la cuarta decimal; nunca más.
      expect(copRate(account)).toBeCloseTo(3326.95, 3);
    });
  });

  describe('HU 2.8 — sin tasa del día se declara la ausencia (QA-05, RN-2.8-F)', () => {
    it.each([
      ['sin valoración', null],
      ['con la tasa ausente', { referenceCurrency: 'COP', rate: null }],
      ['con una tasa cero', { referenceCurrency: 'COP', rate: 0 }],
      ['con una tasa negativa', { referenceCurrency: 'COP', rate: -3500 }],
    ])('%s el saldo se mueve igual y la base queda desconocida', (_caso, basisValuation) => {
      const account = accountWith(200, 660000);
      const update = buildBalanceUpdate({
        account, currency: 'USD', amountDelta: 50, referenceCurrency: 'USD', basisValuation,
      });

      expect(update['balances.USD']).toBe(250);
      expect(update['balanceCostBasis.USD']).toMatchObject({
        cost: null, referenceCurrency: 'COP', status: COST_BASIS_STATUS.UNKNOWN,
      });
    });
  });

  describe('HU 2.8 — el dinero que ya estaba no es gratis (QA-06, QA-07, RN-2.8-E)', () => {
    const noBasis = (balance) => ({ balances: { USD: balance }, balanceCostBasis: {} });

    it('pide la tasa del día para lo que ya había', () => {
      expect(basisValuationNeed({
        account: noBasis(50), currency: 'USD', amountDelta: 100, costDelta: 330000, referenceCurrency: 'COP',
      })).toBe('COP');
    });

    it('50 USD previos a 3.250 + 100 USD declarados a 3.300 = 492.500 COP (3.283,33)', () => {
      const account = noBasis(50);
      const after = apply(account, buildBalanceUpdate({
        account,
        currency: 'USD',
        amountDelta: 100,
        costDelta: 330000,
        referenceCurrency: 'COP',
        basisValuation: { referenceCurrency: 'COP', rate: 3250 },
      }));

      expect(after.balanceCostBasis.USD.cost).toBe(492500);
      expect(copRate(after)).toBeCloseTo(3283.33, 2);
    });

    it('con −20 USD previos solo queda base por los 80 que quedan, a la tasa declarada', () => {
      const account = noBasis(-20);
      const after = apply(account, buildBalanceUpdate({
        account, currency: 'USD', amountDelta: 100, costDelta: 330000, referenceCurrency: 'COP',
      }));

      expect(after.balances.USD).toBe(80);
      expect(after.balanceCostBasis.USD.cost).toBe(264000);
      expect(copRate(after)).toBe(3300);
    });

    it('un saldo previo negativo no pide tasa', () => {
      expect(basisValuationNeed({
        account: noBasis(-20), currency: 'USD', amountDelta: 100, costDelta: 330000, referenceCurrency: 'COP',
      })).toBeNull();
    });

    it('sin tasa para lo que ya había la base queda desconocida, no 330.000 sobre 150 (QA-07)', () => {
      const account = noBasis(50);
      const update = buildBalanceUpdate({
        account, currency: 'USD', amountDelta: 100, costDelta: 330000, referenceCurrency: 'COP',
        basisValuation: { referenceCurrency: 'COP', rate: null },
      });

      expect(update['balances.USD']).toBe(150);
      expect(update['balanceCostBasis.USD'].status).toBe(COST_BASIS_STATUS.UNKNOWN);
      expect(update['balanceCostBasis.USD'].cost).toBeNull();
    });
  });

  describe('HU 2.8 — en la dirección inversa (QA-08)', () => {
    it('un saldo en pesos con base en dólares la mantiene mientras la referencia es COP', () => {
      let account = {
        balances: { COP: 400000 },
        balanceCostBasis: { COP: { cost: 125, referenceCurrency: 'USD', status: COST_BASIS_STATUS.KNOWN } },
      };

      account = apply(account, buildBalanceUpdate({
        account, currency: 'COP', amountDelta: -200000, referenceCurrency: 'COP',
      }), 'COP');
      account = apply(account, buildBalanceUpdate({
        account,
        currency: 'COP',
        amountDelta: 350000,
        costDelta: 350000,
        referenceCurrency: 'COP',
        basisValuation: { referenceCurrency: 'USD', rate: 1 / 3500 },
      }), 'COP');

      expect(account.balances.COP).toBe(550000);
      expect(account.balanceCostBasis.COP).toMatchObject({ cost: 162.5, referenceCurrency: 'USD' });
      // De vuelta a USD: 550.000 / 162,5 = 3.384,62 COP por dólar.
      expect(1 / deriveAverageRate(account.balanceCostBasis.COP, account.balances.COP, 'USD'))
        .toBeCloseTo(3384.62, 2);
    });
  });

  describe('HU 2.8 — sin regresión para quien no cambia de referencia (QA-09, RN-2.8-G)', () => {
    it('un saldo en la propia referencia sin base de antes no escribe base', () => {
      const update = buildBalanceUpdate({
        account: { balances: { COP: 1000000 } }, currency: 'COP', amountDelta: 500000, referenceCurrency: 'COP',
      });

      expect(update).toEqual({ 'balances.COP': 1500000 });
    });

    it('no pide tasa cuando la base está en la referencia vigente', () => {
      expect(basisValuationNeed({
        account: accountWith(1000, 3400000), currency: 'USD', amountDelta: 100, costDelta: 345000, referenceCurrency: 'COP',
      })).toBeNull();
    });

    it('una entrada declarada sobre una base vigente suma como siempre', () => {
      const update = buildBalanceUpdate({
        account: accountWith(1000, 3400000), currency: 'USD', amountDelta: 100, costDelta: 345000, referenceCurrency: 'COP',
      });

      expect(update['balanceCostBasis.USD'].cost).toBe(3745000);
    });
  });

  describe('HU 2.8 — un saldo agotado empieza limpio (QA-10)', () => {
    it.each([
      ['llega a cero', -200, 340000, 100],
      ['queda en −30', -230, 238000, 70],
    ])('cuando %s, el siguiente ingreso a 3.400 no arrastra los 660.000 anteriores', (_caso, retiro, costoEsperado, saldoEsperado) => {
      let account = accountWith(200, 660000);
      account = apply(account, buildBalanceUpdate({
        account, currency: 'USD', amountDelta: retiro, referenceCurrency: 'USD',
      }));

      expect(account.balanceCostBasis.USD).toMatchObject({ cost: 0, referenceCurrency: 'COP' });

      account = apply(account, buildBalanceUpdate({
        account, currency: 'USD', amountDelta: 100, costDelta: 340000, referenceCurrency: 'COP',
      }));

      expect(account.balances.USD).toBe(saldoEsperado);
      expect(account.balanceCostBasis.USD.cost).toBe(costoEsperado);
      expect(copRate(account)).toBe(3400);
    });

    it('nunca escribe un costo negativo al sobregirar una base vigente', () => {
      const update = buildBalanceUpdate({
        account: accountWith(100, 330000), currency: 'USD', amountDelta: -150, referenceCurrency: 'COP',
      });

      expect(update['balanceCostBasis.USD'].cost).toBe(0);
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
