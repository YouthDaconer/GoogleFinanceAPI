/**
 * HU 2.6 — Tests de la proyección del saldo a partir de sus movimientos.
 *
 * Lo que se comprueba aquí es la afirmación central de la historia: que el
 * saldo es la suma de su historial, y que cada fila dice a qué tasa entró o
 * salió ese dinero y con qué promedio quedó el saldo después.
 *
 * @module services/helpers/__tests__/balanceLedger.test
 * @see platform-docs/stories/2.6-libro-mayor-saldo-migracion/refinamiento.md (T21)
 */

jest.mock('../../firebaseAdmin', () => {
  const mockAdmin = {
    firestore: jest.fn(() => ({ collection: jest.fn(), doc: jest.fn() })),
  };

  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };

  return mockAdmin;
});

const {
  projectBalanceLedger,
  resolveCashImpact,
  movesCash,
  MOVEMENT_KINDS,
  RECONCILIATION_STATUS,
} = require('../balanceLedger');

// ============================================================================
// Fixtures — un usuario con el peso como referencia y saldos en dólares
// ============================================================================

const REFERENCE = 'COP';

/** Saldo de apertura de 500 USD a 3.950 COP, el 2 de enero */
const opening = {
  id: 'tx-opening',
  type: 'cash_adjustment',
  adjustmentReason: 'opening',
  adjustmentDelta: 500,
  amount: 500,
  price: 1,
  currency: 'USD',
  date: '2026-01-02T10:00:00.000Z',
  acquisitionRate: 3950,
  acquisitionCost: 1975000,
  referenceCurrency: REFERENCE,
  costBasisEstimated: true,
};

/** Conversión de 4.100.000 COP a 1.000 USD, el 10 de marzo */
const conversion = {
  id: 'tx-conversion',
  type: 'cash_conversion',
  amount: 4100000,
  price: 1,
  currency: 'COP',
  toCurrency: 'USD',
  toAmount: 1000,
  conversionRate: 0.000243902,
  date: '2026-03-10T10:00:00.000Z',
  acquisitionRate: 4100,
  acquisitionCost: 4100000,
  releasedCost: 4100000,
  referenceCurrency: REFERENCE,
};

/** Compra de 10 acciones a 100 USD, el 12 de marzo */
const buy = {
  id: 'tx-buy',
  type: 'buy',
  assetName: 'META',
  amount: 10,
  price: 100,
  commission: 0,
  currency: 'USD',
  date: '2026-03-12T10:00:00.000Z',
  acquisitionRate: 4050,
  releasedCost: 4050000,
  referenceCurrency: REFERENCE,
};

/** Venta de 12 acciones a 100 USD, el 28 de agosto */
const sell = {
  id: 'tx-sell',
  type: 'sell',
  assetName: 'META',
  amount: 12,
  price: 100,
  commission: 0,
  currency: 'USD',
  date: '2026-08-28T10:00:00.000Z',
  realizationRate: 4300,
  acquisitionCost: 5160000,
  referenceCurrency: REFERENCE,
};

const ALL = [opening, conversion, buy, sell];

describe('resolveCashImpact — cuánto movió la caja cada tipo (D2)', () => {
  it('un ingreso entra por su monto, con la tasa a la que se declaró', () => {
    const impact = resolveCashImpact({
      type: 'cash_income',
      amount: 300,
      currency: 'USD',
      acquisitionRate: 4200,
      acquisitionCost: 1260000,
    }, 'USD');

    expect(impact).toMatchObject({
      kind: MOVEMENT_KINDS.INCOME,
      amount: 300,
      appliedRate: 4200,
      costDelta: 1260000,
    });
  });

  it('un retiro sale por su monto, con la tasa del día en que salió (2.5)', () => {
    const impact = resolveCashImpact({
      type: 'cash_expense',
      amount: 200,
      currency: 'USD',
      realizationRate: 4400,
      acquisitionRate: 4100,
    }, 'USD');

    expect(impact.kind).toBe(MOVEMENT_KINDS.EXPENSE);
    expect(impact.amount).toBe(-200);
    // La tasa de salida manda sobre `acquisitionRate`, que en un egreso no
    // describe ninguna adquisición.
    expect(impact.appliedRate).toBe(4400);
  });

  it('un retiro anterior a 2.5 conserva su tasa en `acquisitionRate`', () => {
    const impact = resolveCashImpact({
      type: 'cash_expense',
      amount: 200,
      currency: 'USD',
      acquisitionRate: 4100,
    }, 'USD');

    expect(impact.appliedRate).toBe(4100);
  });

  it('una compra saca el valor de la operación MÁS su comisión', () => {
    const impact = resolveCashImpact({
      type: 'buy',
      amount: 10,
      price: 100,
      commission: 15,
      currency: 'USD',
      acquisitionRate: 4050,
    }, 'USD');

    expect(impact.kind).toBe(MOVEMENT_KINDS.BUY);
    expect(impact.amount).toBe(-1015);
  });

  it('una venta entra por el producto neto de comisión', () => {
    const impact = resolveCashImpact({
      type: 'sell',
      amount: 12,
      price: 100,
      commission: 20,
      currency: 'USD',
      realizationRate: 4300,
      acquisitionCost: 5074000,
    }, 'USD');

    expect(impact.kind).toBe(MOVEMENT_KINDS.SELL);
    expect(impact.amount).toBe(1180);
    expect(impact.appliedRate).toBe(4300);
  });

  it('un dividendo entra por su neto tras retención', () => {
    // `price` ya es el neto por unidad: 100 unidades x 0,85 = 85
    const impact = resolveCashImpact({
      type: 'dividendPay',
      amount: 100,
      price: 0.85,
      currency: 'USD',
      realizationRate: 4250,
    }, 'USD');

    expect(impact.kind).toBe(MOVEMENT_KINDS.DIVIDEND);
    expect(impact.amount).toBe(85);
  });

  it('un tipo que no mueve caja no entra en el libro mayor, ni siquiera con delta cero', () => {
    expect(resolveCashImpact({ type: 'importBatch', currency: 'USD', amount: 0 }, 'USD')).toBeNull();
  });

  describe('la conversión es un documento leído desde dos saldos (RN-2.2-A)', () => {
    it('desde el saldo de origen se ve salir dinero', () => {
      const impact = resolveCashImpact(conversion, 'COP');

      expect(impact.kind).toBe(MOVEMENT_KINDS.CONVERSION_OUT);
      expect(impact.amount).toBe(-4100000);
      expect(impact.counterpartCurrency).toBe('USD');
    });

    it('desde el saldo de destino se ve entrar dinero, con su tasa de valoración', () => {
      const impact = resolveCashImpact(conversion, 'USD');

      expect(impact.kind).toBe(MOVEMENT_KINDS.CONVERSION_IN);
      expect(impact.amount).toBe(1000);
      expect(impact.appliedRate).toBe(4100);
      expect(impact.counterpartCurrency).toBe('COP');
    });

    it('desde una divisa que no participó, no pertenece a su historial', () => {
      expect(resolveCashImpact(conversion, 'EUR')).toBeNull();
    });
  });
});

describe('projectBalanceLedger — el saldo es la suma de su historial', () => {
  it('recorre los movimientos y deja el saldo y el promedio de cada paso (AC-1)', () => {
    const { rows } = projectBalanceLedger({
      transactions: ALL,
      currency: 'USD',
      referenceCurrency: REFERENCE,
      balance: 1700,
    });

    // Se devuelven de más reciente a más antiguo (AC-1); el recorrido fue al revés.
    const chronological = [...rows].reverse();

    expect(chronological.map((row) => row.kind)).toEqual([
      MOVEMENT_KINDS.OPENING,
      MOVEMENT_KINDS.CONVERSION_IN,
      MOVEMENT_KINDS.BUY,
      MOVEMENT_KINDS.SELL,
    ]);

    expect(chronological.map((row) => row.balanceAfter)).toEqual([500, 1500, 500, 1700]);

    // 500 a 3.950 → 1.000 más a 4.100 da 4.050 de promedio; la compra retira al
    // promedio y no lo mueve; la venta entra a 4.300 y lo sube.
    expect(chronological[0].averageRateAfter).toBe(3950);
    expect(chronological[1].averageRateAfter).toBe(4050);
    expect(chronological[2].averageRateAfter).toBe(4050);
    expect(chronological[3].averageRateAfter).toBeCloseTo(4226.47, 2);
  });

  it('el orden cronológico manda, aunque las transacciones lleguen desordenadas', () => {
    const shuffled = [sell, buy, opening, conversion];

    const { rows } = projectBalanceLedger({
      transactions: shuffled,
      currency: 'USD',
      referenceCurrency: REFERENCE,
      balance: 1700,
    });

    expect(rows.map((row) => row.id)).toEqual([
      'tx-sell', 'tx-buy', 'tx-conversion', 'tx-opening',
    ]);
  });

  it('el promedio ponderado de dos entradas no depende del orden de llegada (RN-02)', () => {
    const first = { ...opening, id: 'a', adjustmentDelta: 1000, amount: 1000, acquisitionRate: 4000, acquisitionCost: 4000000 };
    const second = { ...opening, id: 'b', date: '2026-02-02T10:00:00.000Z', adjustmentDelta: 1000, amount: 1000, acquisitionRate: 4400, acquisitionCost: 4400000 };

    const forward = projectBalanceLedger({
      transactions: [first, second], currency: 'USD', referenceCurrency: REFERENCE, balance: 2000,
    });
    const backward = projectBalanceLedger({
      transactions: [second, first], currency: 'USD', referenceCurrency: REFERENCE, balance: 2000,
    });

    expect(forward.rows[0].averageRateAfter).toBe(4200);
    expect(backward.rows[0].averageRateAfter).toBe(4200);
  });

  it('el ajuste marcado como estimado por la migración se propaga a su fila (AC-7)', () => {
    const { rows } = projectBalanceLedger({
      transactions: [opening], currency: 'USD', referenceCurrency: REFERENCE, balance: 500,
    });

    expect(rows[0].estimated).toBe(true);
    expect(rows[0].adjustmentReason).toBe('opening');
  });

  it('una entrada sin costo determinable deja el saldo indeterminado (RN-13)', () => {
    const blind = {
      id: 'tx-blind', type: 'cash_income', amount: 300, price: 1, currency: 'USD',
      date: '2026-04-01T10:00:00.000Z', acquisitionRate: null, acquisitionCost: null,
    };

    const { rows, replayedCostBasis } = projectBalanceLedger({
      transactions: [opening, blind], currency: 'USD', referenceCurrency: REFERENCE, balance: 800,
    });

    expect(rows[0].costStatus).toBe('unknown');
    expect(rows[0].averageRateAfter).toBeNull();
    expect(replayedCostBasis.cost).toBeNull();
  });
});

describe('projectBalanceLedger — el veredicto de conciliación (AC-2, AC-3)', () => {
  it('cuando el historial explica el saldo entero, cuadra', () => {
    const { reconciliation } = projectBalanceLedger({
      transactions: ALL, currency: 'USD', referenceCurrency: REFERENCE, balance: 1700,
    });

    expect(reconciliation).toMatchObject({
      ledgerBalance: 1700,
      difference: 0,
      status: RECONCILIATION_STATUS.RECONCILED,
      movementCount: 4,
    });
  });

  it('cuando sobra dinero sin explicar, se marca la deriva y se dice cuánta', () => {
    const { reconciliation } = projectBalanceLedger({
      transactions: ALL, currency: 'USD', referenceCurrency: REFERENCE, balance: 1750,
    });

    expect(reconciliation.status).toBe(RECONCILIATION_STATUS.DRIFT);
    expect(reconciliation.difference).toBe(50);
  });

  it('un residuo de céntimo por redondeo no es una deriva', () => {
    const { reconciliation } = projectBalanceLedger({
      transactions: ALL, currency: 'USD', referenceCurrency: REFERENCE, balance: 1700.001,
    });

    expect(reconciliation.status).toBe(RECONCILIATION_STATUS.RECONCILED);
  });

  it('un saldo sin ningún movimiento es una deriva por su importe entero (AC-8)', () => {
    const { rows, reconciliation } = projectBalanceLedger({
      transactions: [], currency: 'USD', referenceCurrency: REFERENCE, balance: 450,
    });

    expect(rows).toEqual([]);
    expect(reconciliation.ledgerBalance).toBe(0);
    expect(reconciliation.difference).toBe(450);
    expect(reconciliation.status).toBe(RECONCILIATION_STATUS.DRIFT);
  });
});

describe('projectBalanceLedger — sin exposición cambiaria, sin ruido (AC-9, RN-14)', () => {
  const cop = {
    id: 'tx-cop', type: 'cash_income', amount: 1000000, price: 1, currency: 'COP',
    date: '2026-05-01T10:00:00.000Z',
  };

  it('un saldo en la moneda de referencia no trae ninguna tasa que mostrar', () => {
    const { rows, hasFxExposure } = projectBalanceLedger({
      transactions: [cop, conversion], currency: 'COP', referenceCurrency: REFERENCE, balance: -3100000,
    });

    expect(hasFxExposure).toBe(false);
    expect(rows.every((row) => row.appliedRate === null)).toBe(true);
    expect(rows.every((row) => row.averageRateAfter === null)).toBe(true);
  });

  it('pero sus movimientos siguen contando para el saldo', () => {
    const { reconciliation } = projectBalanceLedger({
      transactions: [cop, conversion], currency: 'COP', referenceCurrency: REFERENCE, balance: -3100000,
    });

    // 1.000.000 que entran menos 4.100.000 que salen a la conversión
    expect(reconciliation.ledgerBalance).toBe(-3100000);
    expect(reconciliation.status).toBe(RECONCILIATION_STATUS.RECONCILED);
  });
});

describe('movesCash', () => {
  it('reconoce los siete tipos que mueven caja', () => {
    const types = ['cash_income', 'cash_expense', 'cash_conversion', 'cash_adjustment', 'buy', 'sell', 'dividendPay'];

    expect(types.every((type) => movesCash({ type }))).toBe(true);
  });

  it('no reconoce lo que no la mueve', () => {
    expect(movesCash({ type: 'importBatch' })).toBe(false);
    expect(movesCash(null)).toBe(false);
  });
});
