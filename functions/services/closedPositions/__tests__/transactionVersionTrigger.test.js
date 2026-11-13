/**
 * HU 6.9.1 — La marca de versión de las transacciones de un inversor.
 *
 * Defiende las dos reglas que hacen que la copia del navegador nunca se use
 * vieja: cualquier compra o venta nueva, editada o borrada, también con fecha
 * pasada, sube la marca (RN-6.9.1-J), y un recálculo de la historia que
 * reescribe ventas también (RN-6.9.1-K). Y que lo que no toca posiciones
 * cerradas, ni lo de otro inversor, no la mueve (RN-6.9-J, QA-02).
 *
 * @see services/closedPositions/transactionVersionTrigger.js
 */

const escrituras = [];
const cuentas = {};

const FieldValue = {
  increment: (n) => ({ __op: 'increment', n }),
  serverTimestamp: () => ({ __op: 'serverTimestamp' }),
};

const firestoreFalso = {
  collection: (nombre) => ({
    doc: (id) => ({
      set: async (datos, opciones) => {
        escrituras.push({ coleccion: nombre, id, datos, opciones });
      },
      get: async () => {
        const datos = nombre === 'portfolioAccounts' ? cuentas[id] : undefined;
        return { exists: datos !== undefined, data: () => datos };
      },
    }),
  }),
};

jest.mock('firebase-functions/v2/firestore', () => ({
  onDocumentWritten: (_opts, handler) => handler,
}));
jest.mock('../../firebaseAdmin', () => {
  const firestore = () => firestoreFalso;
  firestore.FieldValue = FieldValue;
  return { firestore };
});

const {
  transactionVersionDecision,
  handleTransactionWrite,
  transactionVersionOnWrite,
  FUNCTION_CONFIG,
} = require('../transactionVersionTrigger');

// ============================================================================
// FÁBRICAS
// ============================================================================

const crearVenta = (overrides = {}) => ({
  type: 'sell',
  userId: 'uid-inversor-A',
  portfolioAccountId: 'acc-A',
  assetId: 'asset-aapl',
  assetName: 'AAPL',
  amount: 10,
  price: 228.5,
  valuePnL: 461,
  date: '2026-11-05',
  ...overrides,
});

const crearCompra = (overrides = {}) => crearVenta({
  type: 'buy', amount: 20, price: 182.4, valuePnL: undefined, date: '2026-01-15', ...overrides,
});

/** Un evento de Firestore v2 con los dos lados de la escritura */
const crearEvento = (antes, despues, txId = 'tx-1') => ({
  params: { txId },
  data: {
    before: { exists: antes !== null, data: () => antes },
    after: { exists: despues !== null, data: () => despues },
  },
});

const marcasSubidas = () => escrituras
  .filter((e) => e.coleccion === 'transactionVersions')
  .map((e) => e.id);

beforeEach(() => {
  escrituras.length = 0;
  for (const id of Object.keys(cuentas)) delete cuentas[id];
});

// ============================================================================
// DECISIÓN
// ============================================================================

describe('transactionVersionDecision — qué escrituras cambian las posiciones cerradas (RN-6.9.1-J)', () => {
  it('registrar una venta es relevante para su inversor', () => {
    expect(transactionVersionDecision(null, crearVenta())).toEqual({
      relevant: true,
      owners: [{ userId: 'uid-inversor-A', portfolioAccountId: 'acc-A' }],
    });
  });

  it('registrar una compra con fecha pasada también lo es (QA-04: cambia el resultado de una venta antigua)', () => {
    const decision = transactionVersionDecision(null, crearCompra({ date: '2025-11-20', assetName: 'MSFT' }));
    expect(decision.relevant).toBe(true);
  });

  it('editar el precio de una venta es relevante (QA-03)', () => {
    const decision = transactionVersionDecision(crearVenta(), crearVenta({ price: 229.1 }));
    expect(decision.relevant).toBe(true);
    expect(decision.owners).toHaveLength(2);
  });

  it('borrar una venta es relevante: el lado "antes" basta (QA-03, QA-04)', () => {
    expect(transactionVersionDecision(crearVenta(), null)).toEqual({
      relevant: true,
      owners: [{ userId: 'uid-inversor-A', portfolioAccountId: 'acc-A' }],
    });
  });

  it('convertir una venta en otro tipo es relevante: la venta desaparece de la lista', () => {
    expect(transactionVersionDecision(crearVenta(), crearVenta({ type: 'withdraw' })).relevant).toBe(true);
  });

  it('un recálculo que reescribe valuePnL de una venta existente es relevante (RN-6.9.1-K, QA-06)', () => {
    const decision = transactionVersionDecision(crearVenta({ valuePnL: 94 }), crearVenta({ valuePnL: 91.6 }));
    expect(decision.relevant).toBe(true);
  });

  it.each(['deposit', 'withdraw', 'dividendPay', 'conversion', 'adjustment'])(
    'un movimiento "%s" no toca posiciones cerradas y no sube la marca',
    (tipo) => {
      expect(transactionVersionDecision(null, crearVenta({ type: tipo }))).toEqual({
        relevant: false,
        owners: [],
      });
    }
  );

  it('sin documento a ningún lado no hay nada que decidir', () => {
    expect(transactionVersionDecision(null, null)).toEqual({ relevant: false, owners: [] });
  });
});

// ============================================================================
// ESCRITURA DE LA MARCA
// ============================================================================

describe('handleTransactionWrite — sube la marca del inversor dueño (HU 6.9.1)', () => {
  it('una venta nueva sube transactionVersions/{uid} con increment(1) y la hora del servidor, sin pisar el documento', async () => {
    const subidas = await handleTransactionWrite(null, crearVenta(), 'tx-nvda');

    expect(subidas).toEqual(['uid-inversor-A']);
    expect(escrituras).toEqual([{
      coleccion: 'transactionVersions',
      id: 'uid-inversor-A',
      datos: {
        version: { __op: 'increment', n: 1 },
        changedAt: { __op: 'serverTimestamp' },
      },
      opciones: { merge: true },
    }]);
  });

  it('editar una venta del mismo inversor sube su marca una sola vez, no una por lado', async () => {
    await handleTransactionWrite(crearVenta(), crearVenta({ price: 229.1 }), 'tx-vta-a1');
    expect(marcasSubidas()).toEqual(['uid-inversor-A']);
  });

  it('una venta de INV-B solo sube la marca de INV-B, no la de INV-A (QA-02)', async () => {
    await handleTransactionWrite(null, crearVenta({ userId: 'uid-inversor-B', portfolioAccountId: 'acc-B' }), 'tx-b');
    expect(marcasSubidas()).toEqual(['uid-inversor-B']);
  });

  it('un depósito no escribe nada', async () => {
    const subidas = await handleTransactionWrite(null, crearVenta({ type: 'deposit' }), 'tx-dep');
    expect(subidas).toEqual([]);
    expect(escrituras).toEqual([]);
  });

  it('un documento antiguo sin userId resuelve el inversor por su cuenta (1 lectura)', async () => {
    cuentas['acc-A'] = { userId: 'uid-inversor-A', name: 'Cuenta A' };
    const subidas = await handleTransactionWrite(null, crearVenta({ userId: undefined }), 'tx-antigua');
    expect(subidas).toEqual(['uid-inversor-A']);
  });

  it('sin userId ni cuenta existente no sube ninguna marca y no falla (reintentar no lo arreglaría)', async () => {
    const aviso = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const subidas = await handleTransactionWrite(null, crearVenta({ userId: undefined, portfolioAccountId: 'acc-borrada' }), 'tx-huerfana');

    expect(subidas).toEqual([]);
    expect(escrituras).toEqual([]);
    expect(aviso).toHaveBeenCalledWith(expect.stringContaining('[transactionVersionOnWrite] tx-huerfana'));
    aviso.mockRestore();
  });

  it('una venta que cambia de inversor sube la marca de los dos', async () => {
    const subidas = await handleTransactionWrite(
      crearVenta(),
      crearVenta({ userId: 'uid-inversor-B', portfolioAccountId: 'acc-B' }),
      'tx-movida'
    );
    expect(subidas).toEqual(['uid-inversor-A', 'uid-inversor-B']);
  });
});

describe('transactionVersionOnWrite — el disparador', () => {
  it('escucha transactions/{txId} y reintenta si falla (una marca sin subir dejaría una copia vieja en uso)', () => {
    expect(FUNCTION_CONFIG).toMatchObject({
      document: 'transactions/{txId}',
      retry: true,
      minInstances: 0,
    });
  });

  it('lee los dos lados del evento: un borrado sube la marca de quien tenía la venta', async () => {
    await transactionVersionOnWrite(crearEvento(crearVenta(), null, 'tx-borrada'));
    expect(marcasSubidas()).toEqual(['uid-inversor-A']);
  });

  it('una creación sube la marca de quien la registra', async () => {
    await transactionVersionOnWrite(crearEvento(null, crearCompra({ userId: 'uid-inversor-C', portfolioAccountId: 'acc-C' })));
    expect(marcasSubidas()).toEqual(['uid-inversor-C']);
  });
});
