/**
 * HU 2.7 — Tests del núcleo de la corrección del saldo inicial.
 *
 * Lo que se comprueba aquí es la afirmación central de la historia: que se
 * puede arreglar el primer número sin tocar ni un movimiento posterior, que el
 * saldo de hoy queda exactamente en la cifra que el usuario escribió, y que la
 * corrección se rechaza —diciendo dónde y hasta dónde— cuando dejaría el saldo
 * bajo cero en algún momento del pasado.
 *
 * @module services/helpers/__tests__/openingBalanceCorrection.test
 * @see platform-docs/stories/2.7-ajuste-saldo-registro-o-apertura/refinamiento.md (T10)
 */

jest.mock('../../firebaseAdmin', () => {
  const mockAdmin = {
    firestore: jest.fn(() => ({ collection: jest.fn(), doc: jest.fn() })),
  };

  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };

  return mockAdmin;
});

jest.mock('../../historicalRateService', () => ({
  getCrossRate: jest.fn(),
  getRateForDate: jest.fn(),
}));

const {
  planOpeningCorrection,
  applyOpeningCorrection,
  CORRECTION_COST_BASIS_SOURCE,
  _previousDay: previousDay,
} = require('../openingBalanceCorrection');
const { projectBalanceLedger } = require('../balanceLedger');

// ============================================================================
// Fixtures — un usuario con el peso como referencia y un saldo en dólares
// ============================================================================

const REFERENCE = 'COP';

/** Apertura de 60 USD el 15 de marzo de 2024, a 3.900 COP */
const opening = () => ({
  id: 'tx-opening',
  type: 'cash_adjustment',
  adjustmentReason: 'opening',
  adjustmentDelta: 60,
  amount: 60,
  price: 1,
  currency: 'USD',
  date: '2024-03-15T10:00:00.000Z',
  createdAt: { toMillis: () => 1710500000000 },
  acquisitionRate: 3900,
  acquisitionRateSource: 'market-date',
  acquisitionCost: 234000,
  dollarPriceToDate: 3900,
  referenceCurrency: REFERENCE,
  costBasisEstimated: false,
});

/** Ingreso de 200 USD el 18 de agosto de 2026, a 3.960 COP */
const income = () => ({
  id: 'tx-income',
  type: 'cash_income',
  amount: 200,
  price: 1,
  currency: 'USD',
  date: '2026-08-18T10:00:00.000Z',
  createdAt: { toMillis: () => 1755500000000 },
  acquisitionRate: 3960,
  acquisitionCost: 792000,
  referenceCurrency: REFERENCE,
});

/** Retiro de 80 USD el 12 de julio de 2025 */
const withdrawal = () => ({
  id: 'tx-withdrawal',
  type: 'cash_expense',
  amount: 80,
  price: 1,
  currency: 'USD',
  date: '2025-07-12T10:00:00.000Z',
  createdAt: { toMillis: () => 1752300000000 },
  realizationRate: 4010,
  referenceCurrency: REFERENCE,
});

/** El historial de ejemplo: apertura 60, retiro 80, ingreso 200 → 180 USD */
const history = () => [opening(), withdrawal(), income()];

const plan = (transactions, overrides = {}) => planOpeningCorrection({
  transactions,
  currency: 'USD',
  referenceCurrency: REFERENCE,
  storedBalance: 180,
  ...overrides,
});

const apply = (transactions, currentPlan, overrides = {}) => applyOpeningCorrection({
  plan: currentPlan,
  transactions,
  currency: 'USD',
  referenceCurrency: REFERENCE,
  accountId: 'acc-1',
  userId: 'user-1',
  openingId: currentPlan.openingId || 'tx-new-opening',
  acquisitionRate: currentPlan.openingRate,
  acquisitionRateSource: 'market-date',
  dollarPriceToDate: 3900,
  estimated: false,
  openingTime: '00:00:00.000Z',
  ...overrides,
});

// ============================================================================
// El plan: lo que el diálogo necesita saber antes de corregir
// ============================================================================

describe('planOpeningCorrection', () => {
  it('encuentra la apertura del saldo con su fecha, su monto y su tasa', () => {
    const result = plan(history());

    expect(result.hasOpening).toBe(true);
    expect(result.openingId).toBe('tx-opening');
    expect(result.openingDate).toBe('2024-03-15');
    expect(result.openingAmount).toBe(60);
    expect(result.openingRate).toBe(3900);
    expect(result.openingRateEstimated).toBe(false);
  });

  it('el saldo del historial es la suma de sus movimientos, no el guardado', () => {
    const result = plan(history(), { storedBalance: 175.29 });

    // 60 − 80 + 200 = 180, aunque la cuenta tenga 175,29 guardados.
    expect(result.ledgerBalance).toBe(180);
    expect(result.storedBalance).toBe(175.29);
    expect(result.reconciliationStatus).toBe('drift');
  });

  it('cuenta los movimientos posteriores a la apertura, sin contarla a ella', () => {
    expect(plan(history()).movementCountAfter).toBe(2);
  });

  it('el punto más bajo del recorrido viene con la fecha en que se alcanza', () => {
    const result = plan(history());

    // Recorrido: 60 (apertura) → −20 (retiro) → 180 (ingreso). El mínimo es el
    // del retiro, y ya está bajo cero con los datos actuales.
    expect(result.minBalanceAfter).toBe(-20);
    expect(result.minBalanceDate).toBe('2025-07-12');
  });

  it('no pide tasa cuando la apertura ya la tiene y no es estimada (RN-2.7-E)', () => {
    expect(plan(history()).openingRateMissing).toBe(false);
  });

  it('pide tasa cuando la apertura la tiene marcada como estimada (AC-7)', () => {
    const estimated = { ...opening(), costBasisEstimated: true };
    const result = plan([estimated, withdrawal(), income()]);

    expect(result.openingRateEstimated).toBe(true);
    expect(result.openingRateMissing).toBe(true);
  });

  it('pide tasa cuando la apertura no tiene ninguna (RN-13)', () => {
    const sinTasa = { ...opening(), acquisitionRate: null, acquisitionCost: null };
    const result = plan([sinTasa, withdrawal(), income()]);

    expect(result.openingRate).toBeNull();
    expect(result.openingRateMissing).toBe(true);
  });

  it('no pide tasa nunca sin exposición cambiaria (AC-11, RN-14)', () => {
    const enPesos = history().map((transaction) => ({ ...transaction, currency: 'COP' }));
    const result = planOpeningCorrection({
      transactions: enPesos,
      currency: 'COP',
      referenceCurrency: 'COP',
      storedBalance: 180,
    });

    expect(result.hasFxExposure).toBe(false);
    expect(result.openingRateMissing).toBe(false);
  });

  describe('un saldo que nunca tuvo apertura (AC-6, RN-2.7-F)', () => {
    it('propone crearla el día anterior al movimiento más antiguo', () => {
      const result = plan([withdrawal(), income()]);

      expect(result.hasOpening).toBe(false);
      expect(result.openingAmount).toBe(0);
      expect(result.earliestMovementDate).toBe('2025-07-12');
      expect(result.newOpeningDate).toBe('2025-07-11');
    });

    it('cuenta como posteriores TODOS los movimientos, porque va antes de todos', () => {
      expect(plan([withdrawal(), income()]).movementCountAfter).toBe(2);
    });

    it('sin ningún movimiento, la fecha es la de creación de la cuenta', () => {
      const result = plan([], { accountCreatedAt: '2026-02-02T08:00:00.000Z', storedBalance: 0 });

      expect(result.hasOpening).toBe(false);
      expect(result.newOpeningDate).toBe('2026-02-02');
      expect(result.earliestMovementDate).toBeNull();
      expect(result.minBalanceAfter).toBe(0);
    });
  });

  it('con varias aperturas manda la más antigua, no la primera que aparezca', () => {
    const later = {
      ...opening(),
      id: 'tx-opening-late',
      date: '2025-01-01T10:00:00.000Z',
      createdAt: { toMillis: () => 1735700000000 },
    };
    const result = plan([later, opening(), income()]);

    expect(result.openingId).toBe('tx-opening');
  });

  it('el día anterior cruza bien el inicio de mes', () => {
    expect(previousDay('2025-01-09')).toBe('2025-01-08');
    expect(previousDay('2025-03-01')).toBe('2025-02-28');
  });
});

// ============================================================================
// La corrección: el primer número cambia y nada más
// ============================================================================

describe('applyOpeningCorrection', () => {
  it('despeja la apertura para que el saldo de hoy sea el escrito (AC-4, D3)', () => {
    const transactions = history();
    const current = plan(transactions);

    const result = apply(transactions, current, { targetBalance: 300 });

    // Objetivo 300, historial 180 → la apertura sube 120: de 60 a 180.
    expect(result.shift).toBe(120);
    expect(result.previousOpeningAmount).toBe(60);
    expect(result.newOpeningAmount).toBe(180);
    expect(result.newBalance).toBe(300);
  });

  it('el saldo escrito se alcanza incluso partiendo de una cuenta con deriva (AC-10)', () => {
    const transactions = history();
    const current = plan(transactions, { storedBalance: 175.29 });

    const result = apply(transactions, current, { targetBalance: 300 });

    expect(result.newBalance).toBe(300);
    expect(result.accountUpdate['balanceReconciliation.USD']).toEqual({
      ledgerBalance: 300,
      difference: 0,
      status: 'reconciled',
    });
  });

  it('ningún movimiento posterior cambia de monto ni de fecha (AC-5, RN-2.7-C)', () => {
    const transactions = history();
    const current = plan(transactions);

    apply(transactions, current, { targetBalance: 300 });

    // El núcleo es puro: no muta lo que recibe.
    expect(transactions.find((t) => t.id === 'tx-income')).toMatchObject({
      amount: 200,
      date: '2026-08-18T10:00:00.000Z',
      acquisitionRate: 3960,
    });
    expect(transactions.find((t) => t.id === 'tx-withdrawal')).toMatchObject({
      amount: 80,
      date: '2025-07-12T10:00:00.000Z',
    });
  });

  it('todos los saldos acumulados se desplazan en la misma diferencia (AC-5)', () => {
    const transactions = history();
    const current = plan(transactions);

    const before = projectBalanceLedger({
      transactions,
      currency: 'USD',
      referenceCurrency: REFERENCE,
      balance: 180,
    }).rows.map((row) => ({ id: row.id, balanceAfter: row.balanceAfter }));

    const corrected = transactions.map((transaction) => (transaction.id === 'tx-opening'
      ? { ...transaction, adjustmentDelta: 180, amount: 180, acquisitionCost: 702000 }
      : transaction));

    const after = projectBalanceLedger({
      transactions: corrected,
      currency: 'USD',
      referenceCurrency: REFERENCE,
      balance: 300,
    }).rows.map((row) => ({ id: row.id, balanceAfter: row.balanceAfter }));

    after.forEach((row, index) => {
      expect(row.balanceAfter - before[index].balanceAfter).toBeCloseTo(120, 6);
    });

    expect(apply(transactions, current, { targetBalance: 300 }).shift).toBe(120);
  });

  it('conserva la fecha de la apertura y no la mete en el parche (AC-9)', () => {
    const transactions = history();
    const current = plan(transactions);

    const result = apply(transactions, current, { targetBalance: 300 });

    expect(result.openingDate).toBe('2024-03-15');
    expect(result.transactionPatch).not.toHaveProperty('date');
    expect(result.transactionPatch).not.toHaveProperty('createdAt');
  });

  it('deja constancia de cuánto decía la apertura antes (AC-8, RN-2.7-G)', () => {
    const transactions = history();
    const current = plan(transactions);

    const result = apply(transactions, current, { targetBalance: 300 });

    expect(result.transactionPatch.openingCorrectedFrom).toBe(60);
    expect(result.transactionPatch.adjustmentDelta).toBe(180);
    expect(result.transactionPatch.amount).toBe(180);
  });

  it('recalcula el costo del monto nuevo a la tasa que la apertura ya tenía (RN-2.7-E)', () => {
    const transactions = history();
    const current = plan(transactions);

    const result = apply(transactions, current, { targetBalance: 300 });

    // 180 USD × 3.900 COP = 702.000 COP
    expect(result.transactionPatch.acquisitionRate).toBe(3900);
    expect(result.transactionPatch.acquisitionCost).toBe(702000);
    expect(result.cost).toBe(702000);
  });

  it('la base de costo se escribe desde el replay, con su origen (D2)', () => {
    const transactions = history();
    const current = plan(transactions);

    const result = apply(transactions, current, { targetBalance: 300 });
    const basis = result.accountUpdate['balanceCostBasis.USD'];

    expect(basis.source).toBe(CORRECTION_COST_BASIS_SOURCE);
    expect(basis.status).toBe('known');
    expect(basis.referenceCurrency).toBe(REFERENCE);
    expect(result.accountUpdate['balances.USD']).toBe(300);
  });

  it('declara la base ausente cuando el replay no determina el costo (RN-13)', () => {
    const sinCosto = { ...income(), acquisitionRate: null, acquisitionCost: null };
    const transactions = [opening(), withdrawal(), sinCosto];
    const current = plan(transactions);

    const result = apply(transactions, current, { targetBalance: 300 });
    const basis = result.accountUpdate['balanceCostBasis.USD'];

    expect(basis.status).toBe('unknown');
    expect(basis.cost).toBeNull();
    expect(basis.source).toBe(CORRECTION_COST_BASIS_SOURCE);
  });

  describe('el recorrido no puede bajar de cero (AC-3, RN-2.7-D)', () => {
    it('rechaza con la fecha del conflicto y el mínimo admisible', () => {
      const transactions = history();
      const current = plan(transactions);

      // Objetivo 40: la apertura bajaría a −80 y el 12/07/2025 el saldo
      // quedaría en −160. El mínimo admisible es 40 − (−160) = 200.
      const result = apply(transactions, current, { targetBalance: 40 });

      expect(result.conflict).not.toBeNull();
      expect(result.conflict.date).toBe('2025-07-12');
      expect(result.conflict.balance).toBe(-160);
      expect(result.conflict.minimumBalance).toBe(200);
    });

    it('el mínimo que devuelve es admisible de verdad: con él no hay conflicto', () => {
      const transactions = history();
      const current = plan(transactions);

      const rejected = apply(transactions, current, { targetBalance: 40 });
      const atMinimum = apply(transactions, current, {
        targetBalance: rejected.conflict.minimumBalance,
      });

      expect(atMinimum.conflict).toBeNull();
      expect(atMinimum.newBalance).toBe(200);
    });

    it('no hay conflicto cuando la corrección sube el saldo', () => {
      const transactions = history();
      const current = plan(transactions);

      expect(apply(transactions, current, { targetBalance: 300 }).conflict).toBeNull();
    });

    it('un negativo anterior a la apertura no bloquea la corrección', () => {
      // Un retiro fechado ANTES de la apertura: recorrido preexistente en
      // negativo que esta corrección no desplaza y no puede arreglar.
      const early = {
        ...withdrawal(),
        id: 'tx-early',
        date: '2024-01-05T10:00:00.000Z',
        createdAt: { toMillis: () => 1704400000000 },
      };
      const transactions = [early, opening(), income()];
      const current = planOpeningCorrection({
        transactions,
        currency: 'USD',
        referenceCurrency: REFERENCE,
        storedBalance: 180,
      });

      const result = apply(transactions, current, { targetBalance: 300 });

      expect(current.minBalanceDate).not.toBe('2024-01-05');
      expect(result.conflict).toBeNull();
    });
  });

  describe('un saldo sin apertura la recibe (AC-6, RN-2.7-F)', () => {
    it('crea el asiento en lugar de parchear uno que no existe', () => {
      const transactions = [withdrawal(), income()];
      const current = plan(transactions, { storedBalance: 120 });

      const result = apply(transactions, current, {
        targetBalance: 140,
        openingId: 'tx-new-opening',
        acquisitionRate: 3900,
      });

      expect(result.creates).toBe(true);
      expect(result.transactionPatch).toBeNull();
      expect(result.transactionData.adjustmentReason).toBe('opening');
      expect(result.transactionData.adjustmentDelta).toBe(20);
      expect(result.transactionData.date).toBe('2025-07-11T00:00:00.000Z');
    });

    it('la apertura que se crea no se declara corregida: no se modificó nada', () => {
      const transactions = [withdrawal(), income()];
      const current = plan(transactions, { storedBalance: 120 });

      const result = apply(transactions, current, {
        targetBalance: 140,
        openingId: 'tx-new-opening',
        acquisitionRate: 3900,
      });

      expect(result.transactionData).not.toHaveProperty('openingCorrectedFrom');
      expect(result.transactionData).not.toHaveProperty('openingCorrectedAt');
    });

    it('el saldo resultante cuenta ya con la apertura creada (RN-06)', () => {
      const transactions = [withdrawal(), income()];
      const current = plan(transactions, { storedBalance: 120 });

      const result = apply(transactions, current, {
        targetBalance: 140,
        openingId: 'tx-new-opening',
        acquisitionRate: 3900,
      });

      expect(result.newBalance).toBe(140);
      expect(result.accountUpdate['balanceReconciliation.USD'].status).toBe('reconciled');
    });
  });

  describe('sin exposición cambiaria, sin ruido (AC-11, RN-14)', () => {
    const enPesos = () => [
      { ...opening(), currency: 'COP', adjustmentDelta: 0, amount: 0, acquisitionRate: null, acquisitionCost: null },
      { ...income(), currency: 'COP', amount: 142325, acquisitionRate: null, acquisitionCost: null },
    ];

    it('no escribe base de costo para un saldo en la moneda de referencia', () => {
      const transactions = enPesos();
      const current = planOpeningCorrection({
        transactions,
        currency: 'COP',
        referenceCurrency: 'COP',
        storedBalance: 142325,
      });

      const result = applyOpeningCorrection({
        plan: current,
        transactions,
        currency: 'COP',
        referenceCurrency: 'COP',
        targetBalance: 1799227,
        accountId: 'acc-1',
        userId: 'user-1',
        openingId: current.openingId,
        acquisitionRate: null,
        acquisitionRateSource: 'identity',
        dollarPriceToDate: 1,
      });

      expect(result.accountUpdate).not.toHaveProperty('balanceCostBasis.COP');
      expect(result.newBalance).toBe(1799227);
      expect(result.transactionPatch.acquisitionRate).toBeNull();
      expect(result.transactionPatch.acquisitionCost).toBeNull();
    });
  });

  it('una diferencia de cero no propone nada: el monto no cambia', () => {
    const transactions = history();
    const current = plan(transactions);

    const result = apply(transactions, current, { targetBalance: 180 });

    expect(result.shift).toBe(0);
    expect(result.newOpeningAmount).toBe(60);
  });
});
