/**
 * HU 6.11.5 — La cabecera del día anterior sin descargar todas las monedas.
 *
 * - Escenario 4 / QA-08: USD y la moneda principal dan el mismo bloque que leía
 *   el navegador, y solo se pide la fecha y esa moneda.
 * - QA-09: en una moneda poco usada, USD × tasa de cierre del día, sin leer lo
 *   guardado en esa moneda (RN-6.11-J), incluido el día sin cierre de divisa.
 * - QA-10: sin tasa, con tasa inválida o con el servicio caído → no disponible,
 *   nunca la cifra guardada (RN-6.11-K).
 * - QA-12: el límite de los 5 días naturales (RN-6.11-L).
 * - RN-6-H: con DERIVED_CURRENCY_SNAPSHOTS=false se lee lo guardado, como antes.
 *
 * @see platform-docs/stories/6.11.5-recargar-portfolio-sin-lecturas/qa.md
 */

const mockGetExchangeRates = jest.fn();
jest.mock('../../financeQuery', () => ({
  getExchangeRates: (...args) => mockGetExchangeRates(...args),
}));

const { limpiarCachesDerivados } = require('../derivedCurrencySnapshotService');
const {
  obtenerCabeceraDiaAnterior,
  extraerCabecera,
  derivarCabecera,
  diferenciasDeCabecera,
} = require('../previousDayPerformanceService');

// ============================================================================
// FÁBRICAS
// ============================================================================

/**
 * Firestore en memoria: `userData/{uid}` y los documentos diarios de
 * `portfolioPerformance/{uid}/dates`, con la consulta que hace el servicio.
 */
function crearDb({ principal = 'USD', dias = [] } = {}) {
  const lecturas = [];
  const proyecciones = [];
  return {
    lecturas,
    proyecciones,
    collection: (ruta) => {
      if (ruta === 'userData') {
        return {
          doc: (id) => ({
            get: async () => {
              lecturas.push(`userData/${id}`);
              return { exists: true, data: () => ({ defaultCurrency: principal }) };
            },
          }),
        };
      }
      let filtro = () => true;
      let tope = Infinity;
      let campos = null;
      const consulta = {
        where: (campo, op, valor) => {
          expect([campo, op]).toEqual(['date', '<']);
          filtro = (d) => d.date < valor;
          return consulta;
        },
        orderBy: (campo, dir) => {
          expect([campo, dir]).toEqual(['date', 'desc']);
          return consulta;
        },
        limit: (n) => { tope = n; return consulta; },
        select: (...c) => { campos = c; return consulta; },
        get: async () => {
          proyecciones.push(campos);
          const docs = dias.filter(filtro).sort((a, b) => b.date.localeCompare(a.date)).slice(0, tope)
            .map((d) => {
              lecturas.push(`${ruta}/${d.date}`);
              const visible = campos ? Object.fromEntries(Object.entries(d).filter(([k]) => campos.includes(k))) : d;
              return { data: () => visible };
            });
          return { empty: docs.length === 0, docs };
        },
      };
      return consulta;
    },
  };
}

/** Un día del portafolio como lo escribe el cierre, con todas sus monedas. */
const dia = (date, monedas) => ({ date, userId: 'inv-u', ...monedas });

/** Bloque de una moneda del documento diario. */
const bloque = (overrides = {}) => ({
  totalValue: 239480,
  totalInvestment: 210000,
  factor: 1.1240,
  adjustedDailyChangePercent: 0.52,
  totalCashFlow: 0,
  doneProfitAndLoss: 0,
  unrealizedProfitAndLoss: 1245.30,
  ...overrides,
});

/** Lo guardado en COP y EUR, distinto de REF-0 a propósito (QA-10). */
const DIA_12 = dia('2026-11-12', {
  USD: bloque(),
  COP: bloque({ totalValue: 1, unrealizedProfitAndLoss: 1 }),
  EUR: bloque({ totalValue: 2, unrealizedProfitAndLoss: 1069.40 }),
});

const cierres = (moneda, porFecha) => ({ rates: { [moneda]: porFecha } });

const HOY = '2026-11-13';

beforeEach(() => {
  limpiarCachesDerivados();
  mockGetExchangeRates.mockReset();
  delete process.env.DERIVED_CURRENCY_SNAPSHOTS;
  jest.spyOn(console, 'log').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

// ============================================================================
// QA-08 / Escenario 6 — USD y la moneda principal
// ============================================================================

describe('obtenerCabeceraDiaAnterior — USD y moneda principal (QA-08, Escenario 6)', () => {
  test('en USD devuelve el bloque guardado del último día anterior a hoy, pidiendo solo la fecha y USD', async () => {
    const db = crearDb({ dias: [dia('2026-11-11', { USD: bloque({ totalValue: 1 }) }), DIA_12, dia('2026-11-13', { USD: bloque({ totalValue: 3 }) })] });

    const r = await obtenerCabeceraDiaAnterior(db, { userId: 'inv-u', currency: 'USD', hoy: HOY });

    expect(r).toEqual({ disponible: true, date: '2026-11-12', currency: 'USD', data: extraerCabecera(bloque()) });
    expect(db.proyecciones).toEqual([['date', 'USD']]);
    expect(db.lecturas).toEqual(['portfolioPerformance/inv-u/dates/2026-11-12']);
    expect(mockGetExchangeRates).not.toHaveBeenCalled();
  });

  test('en la moneda principal (COP de INV-C) devuelve su bloque guardado, sin derivar', async () => {
    const db = crearDb({ principal: 'COP', dias: [DIA_12] });

    const r = await obtenerCabeceraDiaAnterior(db, { userId: 'inv-c', currency: 'COP', hoy: HOY });

    expect(r.data).toEqual(extraerCabecera(DIA_12.COP));
    expect(r.derivedFrom).toBeUndefined();
    expect(db.proyecciones).toEqual([['date', 'COP']]);
    expect(mockGetExchangeRates).not.toHaveBeenCalled();
  });

  test('aplica los mismos valores por defecto que aplicaba el navegador', () => {
    expect(extraerCabecera({ totalValue: 10 })).toEqual({
      totalValue: 10,
      totalInvestment: 0,
      factor: 1,
      adjustedDailyChangePercent: 0,
      totalCashFlow: 0,
      doneProfitAndLoss: 0,
      unrealizedProfitAndLoss: 0,
    });
  });

  test('un inversor sin días cerrados recibe fecha vacía y sin datos', async () => {
    const db = crearDb({ dias: [] });
    await expect(obtenerCabeceraDiaAnterior(db, { userId: 'nuevo', currency: 'USD', hoy: HOY }))
      .resolves.toEqual({ disponible: true, date: '', currency: 'USD', data: null });
  });

  test('si el día no tiene esa moneda, no hay datos (como omitía el navegador)', async () => {
    const db = crearDb({ principal: 'COP', dias: [dia('2026-11-12', { USD: bloque() })] });
    const r = await obtenerCabeceraDiaAnterior(db, { userId: 'inv-c', currency: 'COP', hoy: HOY });
    expect(r).toEqual({ disponible: true, date: '2026-11-12', currency: 'COP', data: null });
  });
});

// ============================================================================
// QA-09 — Moneda poco usada: USD × tasa de cierre del día
// ============================================================================

describe('obtenerCabeceraDiaAnterior — moneda poco usada desde USD (QA-09, RN-6.11-J)', () => {
  test('INV-U en COP: importes = USD × 4.012,55, sin leer lo guardado en COP', async () => {
    mockGetExchangeRates.mockResolvedValue(cierres('COP', { '2026-11-12': 4012.55 }));
    const db = crearDb({ principal: 'USD', dias: [DIA_12] });

    const r = await obtenerCabeceraDiaAnterior(db, { userId: 'inv-u', currency: 'COP', hoy: HOY });

    expect(r.derivedFrom).toBe('USD');
    expect(r.data.unrealizedProfitAndLoss).toBe(1245.30 * 4012.55);
    // 4.996.828,515 exacto; en coma flotante es 4.996.828,51499…, así que al
    // presentar se ve ,51 y no la ,52 de qa.md (redondeo decimal hacia arriba).
    // Hallazgo anotado para el PO en dev-record.md.
    expect(r.data.unrealizedProfitAndLoss).toBeCloseTo(4996828.515, 6);
    expect(r.data.totalValue.toFixed(2)).toBe('960925474.00');
    // Del documento solo se pide USD: el bloque en COP no viaja (RN-6.11-J, Escenario 4)
    expect(db.proyecciones).toEqual([['date', 'USD']]);
    expect(mockGetExchangeRates).toHaveBeenCalledWith(['COP'], expect.any(String), '2026-11-12');
  });

  test('INV-U en EUR y INV-C en EUR (su principal es COP): cifras de qa.md al céntimo', async () => {
    mockGetExchangeRates.mockResolvedValue(cierres('EUR', { '2026-11-12': 0.8615 }));

    const u = await obtenerCabeceraDiaAnterior(crearDb({ principal: 'USD', dias: [DIA_12] }),
      { userId: 'inv-u', currency: 'EUR', hoy: HOY });
    expect(u.data.unrealizedProfitAndLoss.toFixed(2)).toBe('1072.83');
    expect(u.data.totalValue.toFixed(2)).toBe('206312.02');

    const diaC = dia('2026-11-12', { USD: bloque({ totalValue: 52300, unrealizedProfitAndLoss: -380.15 }), COP: bloque(), EUR: bloque() });
    const c = await obtenerCabeceraDiaAnterior(crearDb({ principal: 'COP', dias: [diaC] }),
      { userId: 'inv-c', currency: 'EUR', hoy: HOY });
    expect(c.data.unrealizedProfitAndLoss.toFixed(2)).toBe('-327.50');
    expect(c.data.totalValue.toFixed(2)).toBe('45056.45');
  });

  test('día sin cierre de divisa: toma el cierre hábil anterior, sin aviso', async () => {
    mockGetExchangeRates.mockResolvedValue(cierres('COP', { '2026-11-11': 4020.10 }));
    const r = await obtenerCabeceraDiaAnterior(crearDb({ dias: [DIA_12] }),
      { userId: 'inv-u', currency: 'COP', hoy: HOY });
    expect(r.disponible).toBe(true);
    expect(r.data.unrealizedProfitAndLoss.toFixed(2)).toBe('5006230.53');
  });

  test('solo se derivan los importes; factor y cambio porcentual no (D2)', () => {
    const d = derivarCabecera(bloque(), 2);
    expect(d).toEqual({
      totalValue: 239480 * 2,
      totalInvestment: 210000 * 2,
      totalCashFlow: 0,
      doneProfitAndLoss: 0,
      unrealizedProfitAndLoss: 1245.30 * 2,
    });
  });
});

// ============================================================================
// QA-10 / QA-12 — Sin con qué calcular
// ============================================================================

describe('obtenerCabeceraDiaAnterior — sin con qué calcular (QA-10, QA-12, RN-6.11-K)', () => {
  const pedirEur = (porFecha, dias = [DIA_12], hoy = HOY) => {
    mockGetExchangeRates.mockResolvedValue(porFecha === null ? null : cierres('EUR', porFecha));
    return obtenerCabeceraDiaAnterior(crearDb({ dias }), { userId: 'inv-u', currency: 'EUR', hoy });
  };

  test('sin tasa en los 5 días previos (última del 2026-11-02): no disponible, nunca lo guardado', async () => {
    const r = await pedirEur({ '2026-11-02': 0.86 });
    expect(r).toEqual({ disponible: false, date: '2026-11-12', currency: 'EUR', motivo: 'sin-tasa-del-dia' });
    expect(JSON.stringify(r)).not.toContain('1069.4');
  });

  test('servicio de tasas caído: no disponible', async () => {
    const r = await pedirEur(null);
    expect(r).toMatchObject({ disponible: false, motivo: 'sin-tasas' });
  });

  test.each([
    ['cero', 0],
    ['negativa', -0.8615],
    ['vacía', null],
  ])('tasa inválida (%s): no disponible, sin una cifra calculada con ella', async (_nombre, tasa) => {
    // Integración con la 6.11.1: la regla unificada distingue la tasa inválida
    // del día sin cierre; la cabecera responde "no disponible" en lugar de fallar.
    const r = await pedirEur({ '2026-11-12': tasa });
    expect(r).toMatchObject({ disponible: false, motivo: 'tasa-invalida' });
    expect(r.data).toBeUndefined();
  });

  test('QA-12: la tasa del 5.º día natural previo vale; la del 6.º no', async () => {
    const dia10 = [dia('2026-11-10', { USD: bloque() })];

    const dentro = await pedirEur({ '2026-11-05': 0.8602 }, dia10, '2026-11-11');
    expect(dentro.disponible).toBe(true);
    expect(dentro.data.totalValue).toBe(239480 * 0.8602);

    limpiarCachesDerivados();
    const fuera = await pedirEur({ '2026-11-04': 0.8602 }, dia10, '2026-11-11');
    expect(fuera).toMatchObject({ disponible: false, motivo: 'sin-tasa-del-dia' });
  });
});

// ============================================================================
// RN-6-H — Reversible
// ============================================================================

describe('obtenerCabeceraDiaAnterior — reversible (RN-6-H, QA-16)', () => {
  test('con DERIVED_CURRENCY_SNAPSHOTS=false lee el bloque guardado en la moneda, como antes', async () => {
    process.env.DERIVED_CURRENCY_SNAPSHOTS = 'false';
    const db = crearDb({ dias: [DIA_12] });

    const r = await obtenerCabeceraDiaAnterior(db, { userId: 'inv-u', currency: 'EUR', hoy: HOY });

    expect(r.data).toEqual(extraerCabecera(DIA_12.EUR));
    expect(db.proyecciones).toEqual([['date', 'EUR']]);
    expect(mockGetExchangeRates).not.toHaveBeenCalled();
  });
});

// ============================================================================
// QA-15 — La comparación previa al despliegue exige igualdad exacta
// ============================================================================

describe('diferenciasDeCabecera — igualdad exacta antes de publicar (QA-15, RN-6-B)', () => {
  test('cifras idénticas en todos sus decimales: sin diferencias, se puede publicar', () => {
    expect(diferenciasDeCabecera(extraerCabecera(bloque()), extraerCabecera(bloque()))).toEqual([]);
  });

  test('0,01 COP de diferencia en el P&G bloquea', () => {
    const ref = derivarCabecera(bloque(), 4012.55);
    const obtenida = { ...ref, unrealizedProfitAndLoss: ref.unrealizedProfitAndLoss + 0.01 };
    expect(diferenciasDeCabecera(obtenida, ref)).toHaveLength(1);
    expect(diferenciasDeCabecera(obtenida, ref)[0]).toMatch(/^unrealizedProfitAndLoss: /);
  });

  test('0,520001 % frente a 0,520000 % bloquea aunque en pantalla ambos sean 0,52 %', () => {
    const ref = extraerCabecera(bloque({ adjustedDailyChangePercent: 0.520000 }));
    const obtenida = extraerCabecera(bloque({ adjustedDailyChangePercent: 0.520001 }));
    expect(diferenciasDeCabecera(obtenida, ref)).toEqual(['adjustedDailyChangePercent: 0.520001 ≠ 0.52']);
  });

  test('un campo de más o de menos también es una diferencia', () => {
    expect(diferenciasDeCabecera({ totalValue: 1 }, { totalValue: 1, factor: 1 })).toEqual(['factor: undefined ≠ 1']);
  });

  test('sin datos en los dos lados no hay diferencia; en uno solo, sí', () => {
    expect(diferenciasDeCabecera(null, null)).toEqual([]);
    expect(diferenciasDeCabecera(null, { totalValue: 1 })).toHaveLength(1);
  });
});
