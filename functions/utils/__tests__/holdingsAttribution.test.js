/**
 * Tests para utils/holdingsAttribution (FIX-ATTR-HOLDINGS-001)
 *
 * Atribución por tenencias diarias: contribución = Σ peso(t-1) × retorno(t),
 * encadenada. Los casos de regresión reproducen lo medido en producción el
 * 2026-11-10: con aportes que multiplicaban el portafolio, el cálculo anterior
 * mostraba pesos de 118 % y contribuciones de -22 pp con 5 % de peso.
 */

const {
  computeHoldingsAttribution,
  mergeAccountDays,
  holdingsByTicker,
  typeFromAssetKey,
  MAX_CARRY_DAYS,
} = require('../holdingsAttribution');

/** Doc con assetPerformance a partir de { llave: [valor, unidades] } */
const day = (date, holdings) => ({
  date,
  currencyData: {
    assetPerformance: Object.fromEntries(
      Object.entries(holdings).map(([k, [v, u]]) => [k, { totalValue: v, units: u }])
    ),
  },
});

const byTicker = (result) => Object.fromEntries(result.assets.map((a) => [a.ticker, a]));
const sumContrib = (result) => result.assets.reduce((s, a) => s + a.contribution, 0);

describe('computeHoldingsAttribution', () => {
  it('la suma de contribuciones es exactamente el TWR encadenado', () => {
    const days = [
      day('2026-01-05', { A_stock: [600, 6], B_stock: [400, 4] }),
      day('2026-01-06', { A_stock: [660, 6], B_stock: [380, 4] }), // A +10 %, B -5 %
      day('2026-01-07', { A_stock: [726, 6], B_stock: [380, 4] }), // A +10 %, B 0 %
    ];
    const r = computeHoldingsAttribution(days);
    // R1 = 0.6·10 % + 0.4·(-5 %) = 4 % ; R2 = (660/1040)·10 % ; TWR = 1.04·(1+R2) - 1
    const r2 = (660 / 1040) * 0.1;
    const twr = (1.04 * (1 + r2) - 1) * 100;
    expect(r.twr).toBeCloseTo(twr, 10);
    expect(sumContrib(r)).toBeCloseTo(r.twr, 10);
    const a = byTicker(r);
    expect(a.A.contribution).toBeCloseTo((0.06 + r2 * 1.04) * 100, 10);
    expect(a.B.contribution).toBeCloseTo(-2, 10);
    expect(a.A.returnPct).toBeCloseTo(21, 10);
    expect(a.B.returnPct).toBeCloseTo(-5, 10);
  });

  it('un aporte que multiplica el portafolio no genera rendimiento ni infla la contribución', () => {
    const days = [
      day('2026-01-05', { A_stock: [100, 1] }),
      day('2026-01-06', { A_stock: [1000, 10] }), // compra 9 unidades al mismo precio
      day('2026-01-07', { A_stock: [1100, 10] }), // +10 %
    ];
    const r = computeHoldingsAttribution(days);
    expect(r.twr).toBeCloseTo(10, 10);
    // Ganancia $100 sobre un valor inicial de $100 daba 100 pp en el cálculo anterior
    expect(byTicker(r).A.contribution).toBeCloseTo(10, 10);
    expect(byTicker(r).A.dollarChange).toBeCloseTo(100, 10);
  });

  it('el peso de un vendido es su peso real mientras se tuvo, no el costo acumulado / valor inicial', () => {
    // B pesa 10 %, luego el portafolio crece ×10 por aportes a A y B se vende.
    const days = [
      day('2026-01-05', { A_stock: [90, 9], B_stock: [10, 1] }),
      day('2026-01-06', { A_stock: [900, 90], B_stock: [10, 1] }),
      day('2026-01-07', { A_stock: [900, 90] }),
      day('2026-01-08', { A_stock: [900, 90] }),
    ];
    const r = computeHoldingsAttribution(days);
    const b = byTicker(r).B;
    expect(b.weightStart).toBeCloseTo(0.1, 10);
    expect(b.avgWeight).toBeCloseTo((0.1 + 10 / 910) / 2, 10);
    expect(b.avgWeight).toBeLessThan(0.1);
    expect(b.valueEnd).toBe(0);
    expect(b.weightEnd).toBe(0);
    expect(b.heldSteps).toBe(2);
  });

  it('usa el retorno de mercado antes que el precio implícito del doc', () => {
    const days = [
      day('2026-01-05', { 'VUAA.L_etf': [1000, 10] }),
      day('2026-01-06', { 'VUAA.L_etf': [1107, 10] }), // implícito +10.7 % (error de datos)
    ];
    const r = computeHoldingsAttribution(days, { marketReturn: (t) => (t === 'VUAA.L' ? 1 : null) });
    expect(r.twr).toBeCloseTo(1, 10);
    expect(r.coverage.marketReturns).toBe(1);
    expect(r.coverage.impliedReturns).toBe(0);
  });

  it('sin precio de mercado usa el implícito y descarta saltos imposibles', () => {
    const days = [
      day('2026-01-05', { A_stock: [100, 1], B_stock: [100, 1] }),
      day('2026-01-06', { A_stock: [105, 1], B_stock: [300, 1] }), // B +200 %: dato roto
    ];
    const r = computeHoldingsAttribution(days);
    const a = byTicker(r);
    expect(a.A.contribution).toBeCloseTo(2.5, 10);
    expect(a.B.contribution).toBe(0);
    expect(r.coverage.discardedImpliedJumps).toBe(1);
    expect(r.coverage.missingTickers).toEqual(['B']);
  });

  it('suma los dividendos cobrados al retorno del día', () => {
    const days = [
      day('2026-01-05', { A_stock: [100, 1] }),
      day('2026-01-06', { A_stock: [100, 1] }),
    ];
    const r = computeHoldingsAttribution(days, { dividendCash: (t, d0, d1) => (t === 'A' && d1 === '2026-01-06' ? 2 : 0) });
    expect(r.twr).toBeCloseTo(2, 10);
    expect(byTicker(r).A.dollarChange).toBeCloseTo(2, 10);
    expect(r.coverage.dividends).toBeCloseTo(2, 10);
  });

  it('agrupa llaves del mismo ticker, ignora las anidadas y conserva la llave con tipo', () => {
    const base = day('2026-01-05', { 'VUAA.L_undefined': [100, 1], 'VUAA.L_etf': [100, 1] });
    base.currencyData.assetPerformance.VUAA = { L_etf: { totalValue: 999 } }; // anidada por un "."
    const days = [base, day('2026-01-06', { 'VUAA.L_etf': [220, 2] })];
    const r = computeHoldingsAttribution(days);
    expect(r.assets).toHaveLength(1);
    expect(r.assets[0].ticker).toBe('VUAA.L');
    expect(r.assets[0].assetKey).toBe('VUAA.L_etf');
    expect(r.assets[0].type).toBe('etf');
    expect(r.startValue).toBe(200);
    expect(r.twr).toBeCloseTo(10, 10);
  });

  it('un doc sin bloque por activo (backfill) se salta: el siguiente se compara contra el último válido', () => {
    const days = [
      day('2026-01-05', { A_stock: [100, 1] }),
      { date: '2026-01-06', currencyData: { totalValue: 0 } },
      day('2026-01-07', { A_stock: [110, 1] }),
    ];
    const r = computeHoldingsAttribution(days);
    expect(r.coverage.steps).toBe(1);
    expect(r.twr).toBeCloseTo(10, 10);
  });

  it('un bloque vacío sí es dato: tras liquidar todo no se siguen atribuyendo retornos', () => {
    const days = [
      day('2026-01-05', { A_stock: [100, 1] }),
      day('2026-01-06', {}),
      day('2026-01-07', {}),
    ];
    const r = computeHoldingsAttribution(days, { marketReturn: () => 5 });
    expect(r.coverage.steps).toBe(1);
    expect(r.twr).toBeCloseTo(5, 10);
    expect(r.endValue).toBe(0);
  });

  it('devuelve null si ningún doc trae datos por activo', () => {
    expect(computeHoldingsAttribution([{ date: '2026-01-05', currencyData: { totalValue: 5 } }])).toBeNull();
    expect(computeHoldingsAttribution([])).toBeNull();
  });
});

describe('mergeAccountDays', () => {
  it('suma las cuentas en la misma fecha y arrastra el último doc de una cuenta sin dato', () => {
    const acc1 = [
      day('2026-01-05', { A_stock: [100, 1] }),
      day('2026-01-06', { A_stock: [110, 1] }),
      day('2026-01-07', { A_stock: [120, 1] }),
    ];
    const acc2 = [day('2026-01-06', { B_stock: [50, 5] })]; // empieza después; falta el 07
    const merged = mergeAccountDays([acc1, acc2]);
    expect(merged.map((d) => d.date)).toEqual(['2026-01-05', '2026-01-06', '2026-01-07']);
    expect(merged[0].currencyData.totalValue).toBe(100); // acc2 aún no existía
    expect(merged[1].currencyData.totalValue).toBe(160);
    expect(merged[2].currencyData.totalValue).toBe(170); // B arrastrado
    expect(merged[2].currencyData.assetPerformance.B_stock).toEqual({ totalValue: 50, units: 5 });
  });

  it('suma valor y unidades de la misma llave en varias cuentas', () => {
    const merged = mergeAccountDays([
      [day('2026-01-05', { A_stock: [100, 1] })],
      [day('2026-01-05', { A_stock: [300, 3] })],
    ]);
    expect(merged[0].currencyData.assetPerformance.A_stock).toEqual({ totalValue: 400, units: 4 });
  });

  it(`no arrastra un doc más de ${MAX_CARRY_DAYS} días`, () => {
    const merged = mergeAccountDays([
      [day('2026-01-05', { A_stock: [100, 1] }), day('2026-01-20', { A_stock: [100, 1] })],
      [day('2026-01-05', { B_stock: [100, 1] })],
    ]);
    expect(merged[1].date).toBe('2026-01-20');
    expect(merged[1].currencyData.assetPerformance.B_stock).toBeUndefined();
  });

  it('una cuenta que lo vendió todo (bloque vacío) deja de aportar ese mismo día', () => {
    const merged = mergeAccountDays([
      [day('2026-01-05', { A_stock: [100, 1] }), day('2026-01-06', { A_stock: [100, 1] })],
      [day('2026-01-05', { B_stock: [50, 1] }), day('2026-01-06', {})],
    ]);
    expect(merged[1].currencyData.totalValue).toBe(100);
    expect(merged[1].currencyData.assetPerformance.B_stock).toBeUndefined();
  });
});

describe('helpers', () => {
  it('typeFromAssetKey reconoce stock/etf/crypto y rechaza el resto', () => {
    expect(typeFromAssetKey('VUAA.L_etf')).toBe('etf');
    expect(typeFromAssetKey('BTC-USD_crypto')).toBe('crypto');
    expect(typeFromAssetKey('VUAA.L_undefined')).toBeNull();
  });

  it('holdingsByTicker distingue bloque ausente (null) de bloque vacío (Map vacío)', () => {
    expect(holdingsByTicker(undefined)).toBeNull();
    expect(holdingsByTicker({}).size).toBe(0);
  });
});
