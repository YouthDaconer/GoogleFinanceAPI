/**
 * HU 6.9.2 (T4) — Las métricas de riesgo derivan del USD los resúmenes en
 * monedas poco usadas en lugar de leer los guardados, que nadie mantiene.
 *
 * @see platform-docs/stories/6.9.2-resumenes-poco-usados-incrementales/refinamiento.md (T6c)
 */
const mockGet = jest.fn();
const mockDocGet = jest.fn();
const mockWhere = jest.fn();
const mockOrderBy = jest.fn();
const mockCollectionGet = jest.fn();

const mockDb = {
  doc: jest.fn(() => ({ get: mockDocGet })),
  collection: jest.fn(() => ({
    where: mockWhere,
  })),
};

mockWhere.mockReturnValue({ where: mockWhere, orderBy: mockOrderBy });
mockOrderBy.mockReturnValue({ get: mockCollectionGet });

// §6: se dobla el módulo que importa multiAccountAggregator, no el SDK.
jest.mock('../../firebaseAdmin', () => ({ firestore: () => mockDb }));

jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: jest.fn((userId, accountId, currency) => {
    if (accountId === 'overall') return `${userId}_${currency}`;
    return `${userId}_${accountId}_${currency}`;
  }),
}));

const mockObtenerDerivado = jest.fn();
jest.mock('../../snapshots/derivedCurrencySnapshotService', () => ({
  resumenesDerivadosDelUsd: () => process.env.DERIVED_CURRENCY_SNAPSHOTS !== 'false',
  obtenerResumenDerivado: (...args) => mockObtenerDerivado(...args),
}));

const { fetchOverallPerformanceData, clearSnapshotCache } = require('../multiAccountAggregator');

const serie = [{ d: '2026-11-12', v: 4000000, c: 0 }, { d: '2026-11-13', v: 4080400, c: 2.01 }];

describe('HU 6.9.2: métricas de riesgo en una moneda poco usada', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    clearSnapshotCache();
    delete process.env.DERIVED_CURRENCY_SNAPSHOTS;
  });

  it('usa la serie derivada del USD y la recuerda en la caché local', async () => {
    mockObtenerDerivado.mockResolvedValue({ snapshot: { timeline: serie }, metodo: 'derivado' });

    const r = await fetchOverallPerformanceData('user-1', '2026-11-01', '2026-11-30', 'COP');
    await fetchOverallPerformanceData('user-1', '2026-11-01', '2026-11-30', 'COP');

    expect(r.dailyData).toEqual([
      { date: '2026-11-12', return: 0, value: 4000000 },
      { date: '2026-11-13', return: 2.01 / 100, value: 4080400 },
    ]);
    expect(r.currentValue).toBe(4080400);
    expect(mockObtenerDerivado).toHaveBeenCalledTimes(1);
    expect(mockDocGet).not.toHaveBeenCalled();
  });

  it.each(['no-aplica', 'no-derivable'])('si el derivado es %s, lee el resumen guardado como antes (QA-09)', async (metodo) => {
    mockObtenerDerivado.mockResolvedValue({ snapshot: null, metodo });
    mockDocGet.mockResolvedValue({ exists: true, data: () => ({ timeline: serie }) });

    const r = await fetchOverallPerformanceData('user-1', '2026-11-01', '2026-11-30', 'COP');

    expect(mockDocGet).toHaveBeenCalledTimes(1);
    expect(r.currentValue).toBe(4080400);
  });
});
