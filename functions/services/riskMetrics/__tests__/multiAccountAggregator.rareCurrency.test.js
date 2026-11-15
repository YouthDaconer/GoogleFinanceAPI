/**
 * HU 6.11.3 — Las métricas de riesgo en una moneda poco usada ya no vuelven a lo
 * guardado en esa moneda, en ninguno de sus respaldos (RN-6.11-J).
 *
 * - Sin con qué derivar → el aviso de la 6.11.1, sin leer el resumen guardado
 *   (Escenario 3, RN-6.11-K; QA-09).
 * - Serie sin días en el rango → sin datos suficientes, como en USD, sin leer los
 *   días guardados (Escenario 4; QA-04). Un rango de 1 día sí se calcula.
 * - Un error no cae a la ruta legacy (QA-15).
 * - Varias cuentas: las que no tienen días se omiten, sin ruta legacy.
 * - USD, la moneda principal y el interruptor apagado: como antes (QA-11, QA-18).
 *
 * @see platform-docs/stories/6.11.3-activo-comparacion-riesgo-usd/refinamiento.md (T8c)
 */

const mockDocGet = jest.fn();
const mockCollectionGet = jest.fn();
const mockConsulta = { where: () => mockConsulta, orderBy: () => mockConsulta, get: mockCollectionGet };
const mockDb = {
  doc: jest.fn(() => ({ get: mockDocGet })),
  collection: jest.fn(() => mockConsulta),
};
jest.mock('../../firebaseAdmin', () => ({ firestore: () => mockDb }));

jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: (userId, accountId, currency) => (accountId === 'overall'
    ? `${userId}_${currency}`
    : `${userId}_${accountId}_${currency}`),
}));

const mockObtenerDerivado = jest.fn();
jest.mock('../../snapshots/derivedCurrencySnapshotService', () => {
  const { HttpsError } = require('firebase-functions/v2/https');
  return {
    resumenesDerivadosDelUsd: () => true,
    obtenerResumenDerivado: (...args) => mockObtenerDerivado(...args),
    monedaNoDisponible: (currency, motivo) => new HttpsError(
      'unavailable',
      `La cifra no está disponible en ${currency} por ahora`,
      { code: 'CURRENCY_UNAVAILABLE', currency, motivo },
    ),
  };
});

// Poco usada = ni USD ni COP (la principal), salvo con el interruptor apagado.
jest.mock('../../snapshots/rareCurrencyScreensService', () => ({
  calculaDesdeUsd: async (db, userId, currency) => process.env.RARE_CURRENCY_SCREENS_FROM_USD !== 'false'
    && !['USD', 'COP'].includes(currency),
}));

const {
  fetchOverallPerformanceData,
  fetchAccountPerformanceData,
  aggregateMultiAccountData,
  clearSnapshotCache,
} = require('../multiAccountAggregator');

const serie = [
  { d: '2026-11-12', v: 18000 * 0.8641, c: 0 },
  { d: '2026-11-13', v: 18240.57 * 0.8625, c: 1.1 },
];
const derivado = (timeline = serie) => ({ snapshot: { timeline }, metodo: 'derivado' });

beforeEach(() => {
  jest.clearAllMocks();
  clearSnapshotCache();
  delete process.env.RARE_CURRENCY_SCREENS_FROM_USD;
  // GUARD-X: lo guardado en EUR existe y tiene días en cualquier rango
  mockDocGet.mockResolvedValue({ exists: true, data: () => ({ timeline: [{ d: '2026-11-07', v: 1111111111.11, c: 5 }] }) });
  mockCollectionGet.mockResolvedValue({
    empty: false,
    docs: [{ id: '2026-11-07', data: () => ({ EUR: { totalValue: 1111111111.11, adjustedDailyChangePercentage: 5 } }) }],
  });
});

afterAll(() => {
  delete process.env.RARE_CURRENCY_SCREENS_FROM_USD;
});

describe('HU 6.11.3: riesgo en una moneda poco usada, sin respaldos a lo guardado', () => {
  it('sin con qué derivar, lanza el aviso sin leer el resumen guardado (QA-09)', async () => {
    mockObtenerDerivado.mockResolvedValue({ snapshot: null, metodo: 'no-derivable', motivo: 'sin-tasas' });

    const error = await fetchOverallPerformanceData('uid-A', '2026-01-02', '2026-11-13', 'EUR').catch((e) => e);

    expect(error.details).toEqual({ code: 'CURRENCY_UNAVAILABLE', currency: 'EUR', motivo: 'sin-tasas' });
    expect(mockDocGet).not.toHaveBeenCalled();
    expect(mockCollectionGet).not.toHaveBeenCalled();
  });

  it.each([
    ['un fin de semana', '2026-11-07', '2026-11-08'],
    ['un rango anterior a la historia', '2024-06-03', '2024-09-30'],
  ])('%s: sin datos suficientes, como en USD, sin leer los días guardados (Escenario 4, QA-04)', async (_caso, desde, hasta) => {
    mockObtenerDerivado.mockResolvedValue(derivado());

    const r = await fetchOverallPerformanceData('uid-A', desde, hasta, 'EUR');

    expect(r).toBeNull();
    expect(mockCollectionGet).not.toHaveBeenCalled();
    expect(mockDocGet).not.toHaveBeenCalled();
  });

  it('un rango de 1 día se calcula desde la serie derivada (QA-04, límite)', async () => {
    mockObtenerDerivado.mockResolvedValue(derivado());

    const r = await fetchOverallPerformanceData('uid-A', '2026-11-13', '2026-11-13', 'EUR');

    expect(r.dailyData).toEqual([{ date: '2026-11-13', return: 1.1 / 100, value: 18240.57 * 0.8625 }]);
  });

  it('una cuenta: un error no cae a la ruta legacy (QA-15)', async () => {
    mockObtenerDerivado.mockRejectedValue(new Error('finance-query caído'));

    await expect(fetchAccountPerformanceData('uid-A', 'acc-us', '2026-01-02', '2026-11-13', 'EUR')).rejects.toThrow('finance-query caído');
    expect(mockCollectionGet).not.toHaveBeenCalled();
  });

  it('varias cuentas: las que no tienen días en el rango se omiten, sin ruta legacy', async () => {
    mockObtenerDerivado.mockImplementation(async (db, { accountId }) => derivado(accountId === 'acc-co' ? [] : serie));

    const r = await aggregateMultiAccountData('uid-A', ['acc-us', 'acc-co'], '2026-11-01', '2026-11-13', 'EUR');

    expect(r.metadata.accountsIncluded).toEqual(['acc-us']);
    expect(r.dailyData).toHaveLength(2);
    expect(mockCollectionGet).not.toHaveBeenCalled();
  });

  it('varias cuentas: si una no tiene con qué derivar, el aviso para todas', async () => {
    mockObtenerDerivado.mockImplementation(async (db, { accountId }) => (accountId === 'acc-co'
      ? { snapshot: null, metodo: 'no-derivable', motivo: 'tasa-invalida-2026-10-15' }
      : derivado()));

    const error = await aggregateMultiAccountData('uid-A', ['acc-us', 'acc-co'], '2026-11-01', '2026-11-13', 'EUR').catch((e) => e);

    expect(error.details.motivo).toBe('tasa-invalida-2026-10-15');
    expect(mockCollectionGet).not.toHaveBeenCalled();
  });

  it('en la moneda principal (COP) un rango sin días sigue cayendo a la ruta legacy, como antes (Escenario 6, QA-11)', async () => {
    mockObtenerDerivado.mockResolvedValue({ snapshot: null, metodo: 'no-aplica' });
    mockDocGet.mockResolvedValue({ exists: true, data: () => ({ timeline: serie }) });
    mockCollectionGet.mockResolvedValue({ empty: true, docs: [] });

    const r = await fetchOverallPerformanceData('uid-P', '2026-11-07', '2026-11-08', 'COP');

    expect(r).toBeNull();
    expect(mockCollectionGet).toHaveBeenCalledTimes(1);
  });

  it('con RARE_CURRENCY_SCREENS_FROM_USD=false, sin derivado se lee el guardado como en la 6.9.2 (Escenario 7, QA-18)', async () => {
    process.env.RARE_CURRENCY_SCREENS_FROM_USD = 'false';
    mockObtenerDerivado.mockResolvedValue({ snapshot: null, metodo: 'no-derivable', motivo: 'sin-tasas' });

    const r = await fetchOverallPerformanceData('uid-A', '2026-11-01', '2026-11-13', 'EUR');

    expect(mockDocGet).toHaveBeenCalledTimes(1);
    expect(r.currentValue).toBe(1111111111.11);
  });
});
