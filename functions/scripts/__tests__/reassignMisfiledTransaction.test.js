/**
 * HU 4.4 — Tests del script que devuelve un movimiento a la cuenta de su activo.
 *
 * El caso es el de la venta de 0,3 NVDA del 2025-09-02, simplificado: la venta
 * está en la cuenta origen (que cuadra gracias a ella), y su activo y su compra
 * en la cuenta destino (a la que le falta).
 *
 * @see scripts/reassignMisfiledTransaction.js
 * @see platform-docs/stories/4.4-coherencia-historia-activo/qa.md (QA-01..QA-04)
 */

jest.mock('../../services/firebaseAdmin', () => {
  const firestore = jest.fn(() => ({}));
  firestore.FieldValue = { serverTimestamp: () => 'TS' };
  return { firestore, auth: jest.fn() };
});
jest.mock('../../services/historicalRateService', () => ({ getCrossRate: jest.fn(), getRateForDate: jest.fn() }));

const {
  buildReassignmentPlan,
  diaDelMovimiento,
  applyReassignment,
  PLAN_STATUS,
} = require('../reassignMisfiledTransaction');

// ============================================================================
// DATOS
// ============================================================================

const TX_ID = 'venta-ajena';
const ORIGEN = 'cuenta-xtb';
const DESTINO = 'cuenta-acciones';

/** Producto de la venta: 0,3 × 170,57, sin comisión */
const PRODUCTO = 51.171;

const venta = (overrides = {}) => ({
  type: 'sell', amount: 0.3, price: 170.57, commission: 0, currency: 'USD',
  assetName: 'NVDA', assetId: 'activo-nvda-b', portfolioAccountId: ORIGEN,
  date: '2025-09-02T23:50:52.477Z', valuePnL: 4.341, ...overrides,
});

const movimientosOrigen = (overrides = {}) => [
  { id: 'ingreso-a', type: 'cash_income', amount: 50, price: 1, currency: 'USD', date: '2025-08-01T10:00:00.000Z' },
  { id: TX_ID, ...venta(overrides) },
];

const movimientosDestino = () => [
  { id: 'ingreso-b', type: 'cash_income', amount: 100, price: 1, currency: 'USD', date: '2025-06-30T10:00:00.000Z' },
  {
    id: 'compra-b', type: 'buy', amount: 0.3, price: 156.1, commission: 0, currency: 'USD',
    assetName: 'NVDA', assetId: 'activo-nvda-b', portfolioAccountId: DESTINO, date: '2025-07-01T23:50:53.884Z',
  },
];

const cuenta = (id, overrides = {}) => ({ id, data: { userId: `dueno-${id}`, name: id, ...overrides } });

/** Plan con el mundo esperado; cada test cambia solo lo que defiende */
const plan = (overrides = {}) => buildReassignmentPlan({
  txId: TX_ID,
  tx: venta(),
  asset: { name: 'NVDA', portfolioAccount: DESTINO, units: 0 },
  // El origen guarda justo lo que explica su historial, venta incluida
  source: cuenta(ORIGEN, { balances: { USD: 50 + PRODUCTO } }),
  // Al destino le falta la venta: guarda el producto de más
  target: cuenta(DESTINO, { balances: { USD: 100 - 46.83 + PRODUCTO } }),
  sourceTransactions: movimientosOrigen(),
  targetTransactions: movimientosDestino(),
  sourceReferenceCurrency: 'USD',
  targetReferenceCurrency: 'USD',
  adjustmentRate: { acquisitionRate: null, acquisitionRateSource: 'identity', dollarPriceToDate: 1 },
  ...overrides,
});

// ============================================================================
// PLAN
// ============================================================================

describe('buildReassignmentPlan — HU 4.4, Escenario 1 (RN-4.4-A, RN-4.4-B)', () => {
  it('mueve solo la cuenta y el dueño del movimiento, sin tocar fecha, unidades, precio ni resultado', () => {
    const p = plan();

    expect(p.status).toBe(PLAN_STATUS.READY);
    expect(p.move).toEqual({
      txId: TX_ID,
      before: { portfolioAccountId: ORIGEN, userId: null },
      after: { portfolioAccountId: DESTINO, userId: `dueno-${DESTINO}` },
    });
  });

  it('compensa el origen con un único ajuste del producto de la venta, fechado el día de la venta', () => {
    const p = plan();

    expect(p.delta).toBeCloseTo(PRODUCTO, 6);
    expect(p.adjustment).toMatchObject({
      type: 'cash_adjustment',
      adjustmentReason: 'manual',
      adjustmentDelta: PRODUCTO,
      amount: PRODUCTO,
      currency: 'USD',
      date: '2025-09-02T23:50:52.477Z',
      portfolioAccountId: ORIGEN,
      userId: `dueno-${ORIGEN}`,
      misfiledTransactionId: TX_ID,
    });
    // El usuario lo lee en su historial: dice qué compensa, sin ids internos
    expect(p.adjustment.description).toContain('0.3 NVDA del 2025-09-02');
    expect(p.adjustment.description).not.toContain(TX_ID);
  });

  it('deja el origen cuadrado y el destino cuadrado, sin escribir ningún saldo', () => {
    const p = plan();

    expect(p.reconciliation.source.before).toMatchObject({ status: 'reconciled', difference: 0 });
    expect(p.reconciliation.source.after).toMatchObject({ status: 'reconciled', difference: 0 });
    expect(p.reconciliation.target.before.status).toBe('drift');
    expect(p.reconciliation.target.before.difference).toBeCloseTo(51.17, 2);
    expect(p.reconciliation.target.after).toMatchObject({ status: 'reconciled', difference: 0 });
    expect(JSON.stringify(p)).not.toContain('balances.');
  });
});

describe('buildReassignmentPlan — HU 4.4, Escenario 2 (validaciones e idempotencia)', () => {
  it('relanzada sobre una corrección aplicada, responde ALREADY_APPLIED y no planea nada', () => {
    const p = plan({
      tx: venta({ portfolioAccountId: DESTINO }),
      sourceTransactions: [
        movimientosOrigen()[0],
        { id: 'ajuste-hecho', type: 'cash_adjustment', adjustmentDelta: PRODUCTO, currency: 'USD', misfiledTransactionId: TX_ID },
      ],
    });

    expect(p.status).toBe(PLAN_STATUS.ALREADY_APPLIED);
    expect(p.adjustment).toBeUndefined();
  });

  it.each([
    [
      'el activo no está en la cuenta destino',
      { asset: { name: 'NVDA', portfolioAccount: 'otra-cuenta' } },
      'no está en la cuenta destino',
    ],
    ['el activo no existe', { asset: null }, 'no existe'],
    ['la cuenta destino no tiene dueño', { target: cuenta(DESTINO, { userId: undefined, balances: { USD: 0 } }) }, 'destino no tiene dueño'],
    ['la cuenta origen no tiene dueño', { source: cuenta(ORIGEN, { userId: undefined, balances: { USD: 50 + PRODUCTO } }) }, 'origen no tiene dueño'],
    ['el origen no cuadra antes de tocarlo', { source: cuenta(ORIGEN, { balances: { USD: 999 } }) }, 'no cuadra antes de tocarla'],
    ['el movimiento no existe', { tx: null }, 'no existe'],
    [
      'la corrección quedó a medias (ajuste sin mover el movimiento)',
      {
        sourceTransactions: [
          ...movimientosOrigen(),
          { id: 'ajuste-hecho', type: 'cash_adjustment', adjustmentDelta: PRODUCTO, currency: 'USD', misfiledTransactionId: TX_ID },
        ],
      },
      'a medias',
    ],
  ])('se detiene sin planear escrituras cuando %s', (_caso, overrides, motivo) => {
    const p = plan(overrides);

    expect(p.status).toBe(PLAN_STATUS.BLOCKED);
    expect(p.reasons.join(' ')).toContain(motivo);
    expect(p.adjustment).toBeUndefined();
  });
});

// ============================================================================
// ESCRITURA
// ============================================================================

describe('applyReassignment — HU 4.4, constancia antes de escribir y un solo lote', () => {
  it('guarda la constancia primero y escribe movimiento, ajuste y los dos veredictos, nunca saldos', async () => {
    const orden = [];
    const operaciones = [];
    const batch = {
      update: (ref, datos) => operaciones.push(['update', ref.path, datos]),
      set: (ref, datos) => operaciones.push(['set', ref.path, datos]),
      commit: jest.fn(async () => orden.push('commit')),
    };
    const db = {
      batch: () => batch,
      collection: (nombre) => ({ doc: (id = 'nuevo-ajuste') => ({ id, path: `${nombre}/${id}` }) }),
    };
    const p = plan();

    const { adjustmentId } = await applyReassignment({
      db,
      FieldValue: { serverTimestamp: () => 'TS' },
      plan: p,
      constancia: { movimiento: p.move },
      escribirConstancia: () => orden.push('constancia'),
    });

    expect(orden).toEqual(['constancia', 'commit']);
    expect(adjustmentId).toBe('nuevo-ajuste');
    expect(operaciones.map(([tipo, ruta]) => `${tipo} ${ruta}`)).toEqual([
      `update transactions/${TX_ID}`,
      'set transactions/nuevo-ajuste',
      `update portfolioAccounts/${ORIGEN}`,
      `update portfolioAccounts/${DESTINO}`,
    ]);
    expect(operaciones[0][2]).toEqual({ portfolioAccountId: DESTINO, userId: `dueno-${DESTINO}` });
    operaciones.slice(2).forEach(([, , datos]) => {
      expect(Object.keys(datos)).toEqual(['balanceReconciliation.USD']);
    });
  });
});

describe('diaDelMovimiento — el día se cuenta en Nueva York (estándar CF §4 regla 10)', () => {
  it.each([
    ['2025-09-02T23:50:52.477Z', '2025-09-02'],
    // 21:30 de Nueva York ya es el día siguiente en UTC
    ['2025-09-03T01:30:00.000Z', '2025-09-02'],
    ['2026-04-01', '2026-04-01'],
  ])('%s → %s', (fecha, dia) => {
    expect(diaDelMovimiento(fecha)).toBe(dia);
  });
});
