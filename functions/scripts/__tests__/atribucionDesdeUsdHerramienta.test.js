/**
 * HU 6.11.4 (T9f) — La herramienta previa a publicar
 * `scripts/atribucion-desde-usd.js` (raíz del proyecto).
 *
 * - QA-05: la comparación exige igualdad exacta: una diferencia de 0,01 COP
 *   en la contribución de PFBCOLOM se señala con su cifra exacta.
 * - QA-06: comparar dos veces lo mismo da el mismo resultado (función pura).
 * - REF-0: la regla de la tasa del día (5 días naturales, tasa inválida).
 * - QA-15: la vista de solo lectura cuenta los usos de lo guardado en la moneda
 *   (MED-MON) y, en REF-0, sintetiza el bloque desde USD sin leer el guardado.
 * - QA-14: el cuadro para el PO trae ANT, NUEVO y la diferencia.
 *
 * @see platform-docs/stories/6.11.4-atribucion-desde-usd/refinamiento.md (T8, T9f)
 */

const path = require('path');
const { DateTime } = require('luxon');

const herramienta = require(path.resolve(__dirname, '..', '..', '..', '..', '..', 'scripts', 'atribucion-desde-usd.js'));
const { parseArgs, cifras, diferencias, tasasPorDia, filasDelCuadro, crearVista } = herramienta;

const atribucion = (contribucionPfb) => ({
  success: true,
  summary: { portfolioReturn: 12.69, attributedReturn: 12.5 },
  assetAttributions: [
    { assetKey: 'PFBCOLOM_stock', ticker: 'PFBCOLOM', name: 'Bancolombia', contribution: contribucionPfb, valueEnd: 52430 },
    { assetKey: 'AAPL_stock', ticker: 'AAPL', name: 'Apple', contribution: 4.1, valueEnd: 22850 },
  ],
  waterfallData: [{ name: 'PFBCOLOM', value: 1.25 }],
  metadata: { calculatedAt: '2026-11-15T10:00:00Z', processingTimeMs: 840, diagnostics: { periodTWR: 12.69, coverage: { timingsMs: { a: 1 } } } },
});

describe('HU 6.11.4 — comparación exacta de cifras (QA-05, QA-06)', () => {
  it('sin diferencias cuando las cifras son idénticas; los tiempos y los nombres no cuentan', () => {
    const a = cifras(atribucion(1.25));
    const b = cifras({ ...atribucion(1.25), metadata: { calculatedAt: 'otro', processingTimeMs: 1, diagnostics: { periodTWR: 12.69, coverage: { timingsMs: { a: 9 } } } } });
    expect(diferencias(a, b)).toEqual([]);
    expect(diferencias(a, b)).toEqual(diferencias(a, b)); // relanzar da lo mismo
  });

  it('una diferencia de 0,01 COP en la contribución de PFBCOLOM se señala con su cifra exacta', () => {
    const lista = diferencias(cifras(atribucion(1.25)), cifras(atribucion(1.26)));
    expect(lista).toEqual([{ cifra: 'assetAttributions[PFBCOLOM_stock].contribution', a: 1.25, b: 1.26 }]);
  });

  it('una diferencia en el último decimal también cuenta: igualdad en todos los decimales', () => {
    expect(diferencias(cifras(atribucion(0.1 + 0.2)), cifras(atribucion(0.3)))).toHaveLength(1);
  });

  it('"no disponible" en los dos lados es igual aunque el motivo difiera; frente a cifras, es diferencia', () => {
    const nuevo = cifras({ currencyUnavailable: { currency: 'COP', motivo: 'sin-tasa-2026-09-17' } });
    const ref0 = cifras({ currencyUnavailable: { motivo: 'ref0-sin-tasa-2026-09-17' } });
    expect(diferencias(nuevo, ref0)).toEqual([]);
    expect(diferencias(nuevo, cifras(atribucion(1.25))).length).toBeGreaterThan(0);
  });
});

describe('HU 6.11.4 — REF-0: la tasa de cada día (RN-6.11-L, RN-6.11-K)', () => {
  const CIERRES = { '2026-09-11': 4010.8, '2026-09-21': 4002.15, '2026-10-30': 3912.4, '2026-11-02': 0 };

  it('cierre hábil anterior hasta 5 días naturales; el 6.º día sin tasa', () => {
    const tasas = tasasPorDia(CIERRES, '2026-09-11', '2026-09-21', 5, DateTime);
    expect(tasas['2026-09-16']).toBe(4010.8);
    expect(tasas['2026-09-17']).toBeUndefined();
    expect(tasas['2026-09-21']).toBe(4002.15);
  });

  it('una tasa cero deja sin tasa a ese día y a los que la usarían', () => {
    const tasas = tasasPorDia(CIERRES, '2026-10-30', '2026-11-03', 5, DateTime);
    expect(tasas['2026-11-01']).toBe(3912.4);
    expect(tasas['2026-11-02']).toBeUndefined();
    expect(tasas['2026-11-03']).toBeUndefined();
  });
});

describe('HU 6.11.4 — cuadro para el PO (QA-14)', () => {
  it('cada cifra con ANT, NUEVO y la diferencia', () => {
    const filas = filasDelCuadro(cifras(atribucion(1.25)), cifras(atribucion(1.5)));
    expect(filas).toContainEqual({ cifra: 'assetAttributions[PFBCOLOM_stock].contribution', ant: 1.25, nuevo: 1.5, dif: 0.25 });
    expect(filas).toContainEqual({ cifra: 'summary.portfolioReturn', ant: 12.69, nuevo: 12.69, dif: 0 });
  });

  it('argumentos de la línea de comandos', () => {
    expect(parseArgs(['comparar', '--uid', 'a', '--uid', 'b', '--moneda', 'EUR', '--salida', 'c.md']))
      .toEqual({ comando: 'comparar', uids: ['a', 'b'], moneda: 'EUR', salida: 'c.md' });
    expect(() => parseArgs(['medir', '--otro'])).toThrow('Argumento inesperado');
  });
});

describe('HU 6.11.4 — la vista de solo lectura: MED-MON y REF-0 (QA-15)', () => {
  /** Firestore "real" falso: registra qué campos se proyectaron. */
  function dbReal(docs) {
    const proyecciones = [];
    const query = (ruta) => {
      const q = {
        where: () => q, orderBy: () => q, limit: () => q,
        select: (...campos) => { proyecciones.push(campos); return q; },
        get: async () => ({ docs: (docs[ruta] || []).map((d) => ({ id: d.date, exists: true, data: () => d })) }),
      };
      return q;
    };
    return {
      proyecciones,
      collection: query,
      doc: (ruta) => ({ id: ruta.split('/').pop(), get: async () => ({ id: 'x', exists: !!docs[ruta], data: () => docs[ruta] }) }),
    };
  }
  const RUTA = 'portfolioPerformance/uid-inversor-M/dates';
  const DOCS = { [RUTA]: [{ date: '2026-11-03', USD: { totalValue: 52610 }, COP: { totalValue: 1 } }], 'performanceSnapshots/uid-inversor-M_COP': { returns: {} } };

  it('en modo real cuenta el bloque COP usado, la proyección COP y el resumen _COP', async () => {
    const estado = { modo: 'real-ant', moneda: 'COP', medicion: [] };
    const vista = crearVista(dbReal(DOCS), estado);

    const snap = await vista.collection(RUTA).where('date', '>=', 'x').select('date', 'COP.totalValue').get();
    snap.docs[0].data().COP; // eslint-disable-line no-unused-expressions
    snap.docs[0].data().USD; // eslint-disable-line no-unused-expressions
    await vista.doc('performanceSnapshots/uid-inversor-M_COP').get();
    await vista.doc('performanceSnapshots/uid-inversor-M_USD').get();

    expect(estado.medicion.map((m) => m.tipo)).toEqual(['proyeccion', 'bloque-diario', 'resumen']);
  });

  it('en REF-0 proyecta USD en lugar de COP, sintetiza el bloque y oculta el resumen guardado en COP', async () => {
    const real = dbReal(DOCS);
    const estado = {
      modo: 'ref0',
      moneda: 'COP',
      medicion: [],
      diarioRef0: (_ruta, datos) => ({ ...datos, COP: { totalValue: datos.USD.totalValue * 3905.1 } }),
    };
    const vista = crearVista(real, estado);

    const snap = await vista.collection(RUTA).select('date', 'COP.totalValue').get();
    const resumen = await vista.doc('performanceSnapshots/uid-inversor-M_COP').get();

    expect(real.proyecciones).toEqual([['date', 'USD']]);
    expect(snap.docs[0].data().COP.totalValue).toBe(52610 * 3905.1);
    expect(resumen.exists).toBe(false);
    expect(estado.medicion).toEqual([]);
  });

  it('las escrituras se descartan: la vista nunca escribe', async () => {
    const vista = crearVista(dbReal(DOCS), { modo: 'real-ant', moneda: 'COP', medicion: [] });
    await expect(vista.doc('performanceSnapshots/uid-inversor-M_COP').set({ x: 1 })).resolves.toBeUndefined();
    await expect(vista.runTransaction()).rejects.toThrow('solo lectura');
  });
});
