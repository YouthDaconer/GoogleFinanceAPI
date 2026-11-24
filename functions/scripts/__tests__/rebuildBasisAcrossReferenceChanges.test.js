/**
 * HU 2.8 — Tests del script que rehace la base de los saldos que pasaron por un
 * cambio de moneda de referencia.
 *
 * El caso es el de XTB el 2026-11-20/21, simplificado: 0,363 USD sin base en
 * pesos; dos ingresos declarados a 3.326,95 y 3.326,96 con referencia COP; dos
 * compras con referencia COP; y, ya con referencia USD, un retroactivo (HD)
 * registrado al final que la escritura de entonces no descontó de la base.
 *
 * @see scripts/rebuildBasisAcrossReferenceChanges.js
 * @see platform-docs/stories/2.8-base-costo-cambio-referencia/qa.md (QA-12, QA-13)
 */

jest.mock('../../services/firebaseAdmin', () => {
  const firestore = jest.fn(() => ({}));
  firestore.FieldValue = { serverTimestamp: () => 'TS' };
  return { firestore, auth: jest.fn() };
});
jest.mock('../../services/historicalRateService', () => ({ getCrossRate: jest.fn(), getRateForDate: jest.fn() }));

const {
  ordenDeRegistro,
  completarValoraciones,
  planificarCorreccion,
  aplicarCorreccion,
  pasoPorVariasReferencias,
} = require('../rebuildBasisAcrossReferenceChanges');

// ============================================================================
// DATOS
// ============================================================================

const creado = (iso) => ({ _seconds: Math.floor(Date.parse(iso) / 1000) });

const movimientos = () => [
  {
    id: 'saldo-previo', type: 'cash_adjustment', adjustmentDelta: 0.363, amount: 0.363, currency: 'USD',
    date: '2026-11-19T10:00:00.000Z', createdAt: creado('2026-11-19T10:00:00Z'), referenceCurrency: 'USD',
  },
  {
    id: 'ingreso-1', type: 'cash_income', amount: 101, price: 1, currency: 'USD',
    date: '2026-11-20T15:14:37.000Z', createdAt: creado('2026-11-20T15:14:37Z'),
    acquisitionRate: 3326.95, acquisitionCost: 336021.95, referenceCurrency: 'COP',
  },
  {
    id: 'compra-meta', type: 'buy', amount: 0.1696, price: 583.83, commission: 0, currency: 'USD',
    date: '2026-11-20T15:16:06.000Z', createdAt: creado('2026-11-20T15:16:06Z'), referenceCurrency: 'COP',
  },
  {
    id: 'ingreso-2', type: 'cash_income', amount: 101, price: 1, currency: 'USD',
    date: '2026-11-20T20:13:50.000Z', createdAt: creado('2026-11-20T20:13:50Z'),
    acquisitionRate: 3326.96, acquisitionCost: 336022.96, referenceCurrency: 'COP',
  },
  {
    id: 'compra-nvda', type: 'buy', amount: 0.453, price: 128.04, commission: 0, currency: 'USD',
    date: '2026-11-20T20:14:50.000Z', createdAt: creado('2026-11-20T20:14:51Z'), referenceCurrency: 'COP',
  },
  {
    // Retroactivo: dice abril, se registró el 21-nov con referencia USD.
    id: 'compra-hd', type: 'buy', amount: 0.0731, price: 320.09, commission: 0, currency: 'USD',
    date: '2026-04-29T02:38:24.000Z', createdAt: creado('2026-11-21T02:38:24Z'), referenceCurrency: 'USD',
  },
];

const tasaFija = (rate) => jest.fn(async () => ({ rate, rateDate: '2026-11-20' }));

const saldoFinal = 0.363 + 101 - 0.1696 * 583.83 + 101 - 0.453 * 128.04 - 0.0731 * 320.09;

// ============================================================================
// TESTS
// ============================================================================

describe('ordenDeRegistro (HU 2.8)', () => {
  it('un retroactivo entra cuando se registró, no en la fecha que dice', () => {
    const ordenados = [...movimientos()].sort((a, b) => (ordenDeRegistro(a) < ordenDeRegistro(b) ? -1 : 1));
    expect(ordenados.map((t) => t.id).pop()).toBe('compra-hd');
  });
});

describe('pasoPorVariasReferencias (HU 2.8, --detectar)', () => {
  it('detecta un saldo que tuvo movimientos con dos referencias', () => {
    expect(pasoPorVariasReferencias(movimientos(), 'USD')).toBe(true);
  });

  it('no marca a quien siempre usó la misma', () => {
    const soloCop = movimientos().filter((t) => t.referenceCurrency === 'COP');
    expect(pasoPorVariasReferencias(soloCop, 'USD')).toBe(false);
  });
});

describe('completarValoraciones (HU 2.8, RN-2.8-E)', () => {
  it('solo pide tasa para el dinero que ya estaba al reubicar la base', async () => {
    const obtenerTasa = tasaFija(3428.83);

    const { valoraciones } = await completarValoraciones({
      transactions: movimientos(), currency: 'USD', referenceCurrency: 'COP',
      legacyReferenceCurrency: 'USD', obtenerTasa,
    });

    expect(valoraciones.map((v) => v.id)).toEqual(['ingreso-1']);
    expect(obtenerTasa).toHaveBeenCalledWith('USD', 'COP', '2026-11-20');
  });
});

describe('planificarCorreccion — modo prueba (QA-12)', () => {
  it('propone la base con las tasas declaradas y no escribe nada', async () => {
    const plan = await planificarCorreccion({
      transactions: movimientos(), currency: 'USD', referenceCurrency: 'COP',
      legacyReferenceCurrency: 'USD', obtenerTasa: tasaFija(3428.83),
      storedBalance: saldoFinal,
      // Lo que dejó la escritura vieja: la compra HD no bajó la base.
      storedBasis: { cost: 150843.13, referenceCurrency: 'COP', status: 'known' },
    });

    expect(plan.cambia).toBe(true);
    expect(plan.baseAntes.cost).toBe(150843.13);
    expect(plan.baseDespues).toMatchObject({ referenceCurrency: 'COP', status: 'known' });
    // La tasa resultante es la declarada (≈ 3.326,96), no 6.923 (RN-2.8-C).
    expect(plan.baseDespues.cost / saldoFinal).toBeCloseTo(3326.96, 0);
    expect(plan.saldoReproducido).toBeCloseTo(saldoFinal, 6);
  });
});

describe('planificarCorreccion — idempotencia (QA-13)', () => {
  it('una segunda pasada sobre lo ya corregido no encuentra nada que cambiar', async () => {
    const primera = await planificarCorreccion({
      transactions: movimientos(), currency: 'USD', referenceCurrency: 'COP',
      legacyReferenceCurrency: 'USD', obtenerTasa: tasaFija(3428.83),
      storedBalance: saldoFinal, storedBasis: { cost: 150843.13, referenceCurrency: 'COP', status: 'known' },
    });

    // Lo que habría quedado escrito: valoraciones en los movimientos y la base.
    const corregidos = movimientos().map((t) => {
      const v = primera.valoraciones.find((x) => x.id === t.id);
      return v ? { ...t, basisValuation: v.despues } : t;
    });
    const obtenerTasa = tasaFija(9999);

    const segunda = await planificarCorreccion({
      transactions: corregidos, currency: 'USD', referenceCurrency: 'COP',
      legacyReferenceCurrency: 'USD', obtenerTasa,
      storedBalance: saldoFinal, storedBasis: { ...primera.baseDespues, source: 'reference-change-rebuild' },
    });

    expect(segunda.cambia).toBe(false);
    expect(segunda.valoraciones).toHaveLength(0);
    // La tasa guardada en el movimiento manda: no se vuelve a consultar (RN-3-B).
    expect(obtenerTasa).not.toHaveBeenCalled();
  });
});

describe('aplicarCorreccion (RN-2.8-H)', () => {
  it('guarda la constancia ANTES de escribir y escribe base y valoraciones', async () => {
    const orden = [];
    const batch = { update: jest.fn(), commit: jest.fn(async () => orden.push('commit')) };
    const db = {
      batch: () => batch,
      collection: (nombre) => ({ doc: (id) => ({ ruta: `${nombre}/${id}` }) }),
    };

    const plan = {
      currency: 'USD',
      baseDespues: { cost: 72493.92, referenceCurrency: 'COP', status: 'known', convertedAmount: 0, convertedCost: 0 },
      valoraciones: [{ id: 'ingreso-1', despues: { referenceCurrency: 'COP', rate: 3428.83 } }],
    };

    const { escrituras } = await aplicarCorreccion({
      db,
      FieldValue: { serverTimestamp: () => 'TS' },
      cuentas: [{ accountId: 'xtb', plan }],
      constancia: { historia: '2.8' },
      escribirConstancia: () => orden.push('constancia'),
    });

    expect(orden).toEqual(['constancia', 'commit']);
    expect(escrituras).toBe(2);
    expect(batch.update).toHaveBeenCalledWith(
      { ruta: 'transactions/ingreso-1' },
      { basisValuation: { referenceCurrency: 'COP', rate: 3428.83 } },
    );
    expect(batch.update).toHaveBeenCalledWith(
      { ruta: 'portfolioAccounts/xtb' },
      { 'balanceCostBasis.USD': expect.objectContaining({ cost: 72493.92, source: 'reference-change-rebuild' }) },
    );
  });
});
