/**
 * HU 6.11.3 — Métricas de riesgo en una moneda poco usada: el aviso y las semanas
 * rentables desde la serie derivada del USD.
 *
 * - Sin con qué calcular responde `CURRENCY_UNAVAILABLE` con la moneda, no un
 *   error de cálculo (Escenario 3, RN-6.11-K; QA-09). Gana a "sin datos" (QA-09).
 * - Las semanas rentables salen de la serie, con la regla del job semanal, sin
 *   leer `metrics[EUR]` (RN-6.11-J, decisión 3 del refinamiento; QA-03, QA-15).
 * - En USD y en la moneda principal se leen como antes (Escenario 6, QA-11).
 *
 * @see platform-docs/stories/6.11.3-activo-comparacion-riesgo-usd/refinamiento.md (T8c)
 */

const { DateTime } = require('luxon');

const mockDocGet = jest.fn();
const mockDb = { doc: jest.fn(() => ({ get: mockDocGet })) };
jest.mock('../../firebaseAdmin', () => ({ firestore: () => mockDb }));

jest.mock('../benchmarkCache', () => ({
  getMarketReturns: jest.fn(async () => []),
  getDynamicRiskFreeRate: jest.fn(async () => ({ rate: 0.04, source: 'test' })),
}));

const mockAgregar = jest.fn();
const mockLeerResumen = jest.fn();
jest.mock('../multiAccountAggregator', () => ({
  aggregateMultiAccountData: (...args) => mockAgregar(...args),
  determineStrategy: (ids) => (!ids || ids.length === 0 ? 'overall' : ids.length === 1 ? 'single' : 'multi'),
  leerResumen: (...args) => mockLeerResumen(...args),
}));

jest.mock('../../snapshots/rareCurrencyScreensService', () => ({
  calculaDesdeUsd: async (db, userId, currency) => !['USD', 'COP'].includes(currency),
}));

const { calculateRiskMetrics, semanasRentablesDeLaSerie } = require('../riskMetricsService');
const { HttpsError } = require('firebase-functions/v2/https');

const HASTA = DateTime.fromISO('2026-11-14T12:00:00', { zone: 'America/New_York' });

/** Una semana de mercado (lunes a viernes) con el valor de su primer y último día. */
const semana = (lunes, inicio, fin, dias = 5) => Array.from({ length: dias }, (_, i) => ({
  d: DateTime.fromISO(lunes).plus({ days: i }).toISODate(),
  v: i === dias - 1 ? fin : inicio,
}));

/** 70 días de mercado con valor creciente: suficientes para las métricas. */
const diasDeMercado = Array.from({ length: 70 }, (_, i) => ({
  date: DateTime.fromISO('2026-08-03').plus({ days: i }).toISODate(),
  return: 0.001 * ((i % 3) - 1),
  value: 15000 + i * 10,
}));

const agregado = () => ({
  strategy: 'overall',
  accountsProcessed: 1,
  accountsRequested: 0,
  dailyReturns: diasDeMercado.map((d) => d.return),
  dailyData: diasDeMercado,
  totalValue: 15690,
  metadata: { accountsIncluded: ['overall'], aggregationMethod: 'pre-aggregated' },
});

beforeEach(() => {
  jest.clearAllMocks();
  mockDocGet.mockResolvedValue({ exists: true, data: () => ({ metrics: { EUR: { profitableWeeksPercentage: 99 }, USD: { profitableWeeksPercentage: 61 } } }) });
});

describe('HU 6.11.3: semanasRentablesDeLaSerie — la regla del job semanal', () => {
  it('cuenta las semanas de al menos 3 días; rentable si el último día supera al primero', () => {
    const timeline = [
      ...semana('2026-10-26', 100, 110), // rentable
      ...semana('2026-11-02', 110, 105), // no rentable
      ...semana('2026-11-09', 105, 120, 2), // solo 2 días: no cuenta
    ];

    expect(semanasRentablesDeLaSerie(timeline, HASTA)).toBe(50);
    expect(semanasRentablesDeLaSerie([...semana('2026-10-26', 100, 110), ...semana('2026-11-02', 110, 111)], HASTA)).toBe(100);
  });

  it('solo las 52 semanas hasta el último día', () => {
    const timeline = [...semana('2025-10-06', 100, 90), ...semana('2026-11-02', 100, 120)];

    expect(semanasRentablesDeLaSerie(timeline, HASTA)).toBe(100);
  });

  it('sin semanas que contar, el mismo 50 que el valor guardado por defecto', () => {
    expect(semanasRentablesDeLaSerie([], HASTA)).toBe(50);
    expect(semanasRentablesDeLaSerie(undefined, HASTA)).toBe(50);
  });
});

describe('HU 6.11.3: calculateRiskMetrics en una moneda poco usada', () => {
  it('sin con qué calcular responde CURRENCY_UNAVAILABLE con la moneda y el motivo (QA-09)', async () => {
    mockAgregar.mockRejectedValue(new HttpsError('unavailable', 'La cifra no está disponible en EUR por ahora',
      { code: 'CURRENCY_UNAVAILABLE', currency: 'EUR', motivo: 'sin-tasas' }));

    const r = await calculateRiskMetrics('uid-P', { period: 'YTD', currency: 'EUR' });

    expect(r).toMatchObject({ success: false, error: 'CURRENCY_UNAVAILABLE', currency: 'EUR', motivo: 'sin-tasas' });
  });

  it('un error cualquiera sigue siendo CALCULATION_ERROR', async () => {
    mockAgregar.mockRejectedValue(new Error('boom'));

    const r = await calculateRiskMetrics('uid-P', { period: 'YTD', currency: 'EUR' });

    expect(r.error).toBe('CALCULATION_ERROR');
  });

  it('las semanas rentables salen de la serie derivada, sin leer metrics[EUR] (QA-03, QA-15)', async () => {
    mockAgregar.mockResolvedValue(agregado());
    mockLeerResumen.mockResolvedValue({ timeline: [...semana('2026-10-26', 100, 110), ...semana('2026-11-02', 110, 105)] });

    const r = await calculateRiskMetrics('uid-P', { period: 'YTD', currency: 'EUR' });

    expect(r.success).toBe(true);
    expect(r.profitableWeeks).toBe(50);
    expect(mockLeerResumen).toHaveBeenCalledWith('uid-P', 'overall', 'EUR');
    expect(mockDocGet).not.toHaveBeenCalled();
  });

  it.each([['USD', 61], ['COP', 50]])('en %s las semanas rentables se leen como antes (Escenario 6, QA-11)', async (moneda, esperado) => {
    mockAgregar.mockResolvedValue(agregado());

    const r = await calculateRiskMetrics('uid-P', { period: 'YTD', currency: moneda });

    expect(r.profitableWeeks).toBe(esperado);
    expect(mockDocGet).toHaveBeenCalledTimes(1);
    expect(mockLeerResumen).not.toHaveBeenCalled();
  });
});
