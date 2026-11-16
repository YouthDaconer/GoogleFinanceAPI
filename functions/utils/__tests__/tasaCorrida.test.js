/**
 * HU 6.14.2 — Los días guardados con la tasa del día siguiente se detectan.
 *
 * - Un día reescrito por la corrección en verano británico, cuya tasa deducida
 *   coincide con el cierre del día siguiente y no con el suyo, está afectado; el
 *   inversor se corrige desde el primero (RN-6.14.2-A, RN-6.14-B; QA-01).
 * - Invierno, sin coincidencia, cierres prácticamente iguales y el cierre diario
 *   no cuentan; los sin coincidencia se listan (RN-6.14.2-B, RN-6.14-C; QA-05).
 *   Los posteriores al primer día afectado se recalculan igual (decisión del PO).
 * - La detección no marca a nadie: marcar es un paso aparte, que no marca dos
 *   veces ni adelanta la fecha (RN-6.14.2-C, RN-6.14-F; QA-03, QA-08).
 * - Relanzarla tras la corrección da 0 (QA-08).
 *
 * Los cierres son los de `qa.md`: el jueves 2025-08-14 cerró en 4.021,75 y el
 * viernes 15 en 4.049,45; el lunes 11 en 4.043,14 y el martes 12 en 4.032,60. En
 * la serie desplazada, el jueves llevaba el cierre del viernes y el lunes el del
 * martes.
 *
 * @see platform-docs/stories/6.14.2-dias-tasa-corrida-recalculados/refinamiento.md (T6)
 */

jest.mock('../../services/financeQuery', () => ({ getExchangeRates: jest.fn() }));

const {
  UMBRAL_COINCIDE,
  CORTE_SERIE_CORREGIDA,
  cierreDelDia,
  cierreSiguiente,
  esVeranoBritanico,
  tasaDeducida,
  clasificarDia,
  detectarInversor,
  primerDiaDelInversor,
  decidirMarca,
} = require('../tasaCorrida');

/** Serie corregida COP=X (días hábiles). */
const SERIE = {
  '2025-06-02': 4155.25,
  '2025-06-03': 4130.1,
  '2025-07-08': 4020.0,
  '2025-07-09': 4020.002, // difiere 5·10⁻⁷ del 8: prácticamente iguales
  '2025-08-08': 4060.5,
  '2025-08-11': 4043.14,
  '2025-08-12': 4032.6,
  '2025-08-13': 4028.9,
  '2025-08-14': 4021.75,
  '2025-08-15': 4049.45,
  '2025-08-18': 4011.3,
  '2025-09-10': 3955.1,
  '2025-09-11': 3948.7,
  '2026-01-15': 3700.5,
  '2026-01-16': 3690.25,
};

/** La corrección del 2026-11-12 reescribió la historia con la serie desplazada. */
const REESCRITO_ANTES = '2026-11-12T07:03:11.512Z';
const REESCRITO_DESPUES = '2026-11-20T07:01:00.000Z';

/**
 * Un documento general del día con el valor en USD y en COP convertido a la tasa
 * dada, como lo escribe la corrección. Sin `lastUpdated`, como el cierre diario.
 */
const dia = (date, tasa, overrides = {}) => {
  const usd = overrides.usd ?? 125000.37;
  const doc = { date, USD: { totalValue: usd }, COP: { totalValue: usd * tasa } };
  if (overrides.lastUpdated !== null) doc.lastUpdated = overrides.lastUpdated ?? REESCRITO_ANTES;
  return doc;
};

const COP = { moneda: 'COP' };

describe('HU 6.14.2 — cierres de la serie corregida', () => {
  it('el cierre del día es el suyo o el hábil anterior; el siguiente de un viernes es el lunes', () => {
    expect(cierreDelDia(SERIE, '2025-08-14')).toEqual({ fecha: '2025-08-14', cierre: 4021.75 });
    expect(cierreDelDia(SERIE, '2025-08-17')).toEqual({ fecha: '2025-08-15', cierre: 4049.45 }); // domingo
    expect(cierreSiguiente(SERIE, '2025-08-14')).toEqual({ fecha: '2025-08-15', cierre: 4049.45 });
    expect(cierreSiguiente(SERIE, '2025-08-15')).toEqual({ fecha: '2025-08-18', cierre: 4011.3 });
  });

  it('un cierre inválido no cuenta: se sigue buscando (RN-6.14-D)', () => {
    const serie = { ...SERIE, '2025-08-14': 0, '2025-08-15': null };

    expect(cierreDelDia(serie, '2025-08-14')).toEqual({ fecha: '2025-08-13', cierre: 4028.9 });
    expect(cierreSiguiente(serie, '2025-08-14')).toEqual({ fecha: '2025-08-18', cierre: 4011.3 });
  });

  it('más de 5 días naturales sin cierre: no hay', () => {
    expect(cierreDelDia(SERIE, '2025-08-07')).toBeNull();
    expect(cierreSiguiente(SERIE, '2025-08-01')).toBeNull();
  });

  it('el verano británico va del último domingo de marzo al último domingo de octubre', () => {
    expect(esVeranoBritanico('2025-03-29')).toBe(false);
    expect(esVeranoBritanico('2025-03-30')).toBe(true);
    expect(esVeranoBritanico('2025-08-14')).toBe(true);
    expect(esVeranoBritanico('2025-10-25')).toBe(true);
    expect(esVeranoBritanico('2025-10-26')).toBe(false);
    expect(esVeranoBritanico('2026-01-15')).toBe(false);
  });

  it('la tasa deducida es el valor en la moneda entre el valor en USD; sin la moneda, no hay', () => {
    expect(tasaDeducida(dia('2025-08-14', 4049.45), 'COP')).toBeCloseTo(4049.45, 9);
    expect(tasaDeducida({ date: '2025-08-14', USD: { totalValue: 100 } }, 'MXN')).toBeNull();
    expect(tasaDeducida({ date: '2025-08-14', USD: { totalValue: 0 }, COP: { totalValue: 0 } }, 'COP')).toBeNull();
  });
});

describe('HU 6.14.2 — clasificar un día (RN-6.14.2-A, RN-6.14.2-B)', () => {
  it('D-AF2: el jueves 2025-08-14 con 4.049,45 (el cierre del viernes) está afectado (QA-01)', () => {
    expect(clasificarDia(dia('2025-08-14', 4049.45), SERIE, COP)).toMatchObject({
      fecha: '2025-08-14', motivo: 'afectado', cierreSuyo: 4021.75, cierreSiguiente: 4049.45,
    });
  });

  it('D-AF3: el lunes 2025-08-11 con 4.032,60 (el cierre del martes) está afectado (QA-01)', () => {
    expect(clasificarDia(dia('2025-08-11', 4032.6), SERIE, COP).motivo).toBe('afectado');
  });

  it('el viernes arrastraba la entrada del jueves, que era su propio cierre: coincide con su día', () => {
    expect(clasificarDia(dia('2025-08-15', 4049.45), SERIE, COP).motivo).toBe('suyo');
  });

  it('un domingo con el cierre del lunes está afectado: su cierre es el del viernes', () => {
    expect(clasificarDia(dia('2025-08-17', 4011.3), SERIE, COP).motivo).toBe('afectado');
  });

  it('D-INV: un día de invierno con el cierre siguiente no se marca (QA-05)', () => {
    expect(clasificarDia(dia('2026-01-15', 3690.25), SERIE, COP).motivo).toBe('invierno');
  });

  it('D-MAN: 3.950,00 no coincide con ningún cierre: queda para revisión, no afectado (QA-05)', () => {
    expect(clasificarDia(dia('2025-09-10', 3950), SERIE, COP)).toMatchObject({
      motivo: 'sin-coincidencia', tasa: expect.any(Number),
    });
  });

  it('D-IGU: los dos cierres difieren menos que U: ambiguo, no hay error que corregir (QA-05)', () => {
    expect(clasificarDia(dia('2025-07-08', 4020.002), SERIE, COP).motivo).toBe('ambiguo');
  });

  it('con los cierres casi iguales pero una tasa que no coincide con ninguno, va a revisión, no a ambiguo', () => {
    expect(clasificarDia(dia('2025-07-08', 3990), SERIE, COP).motivo).toBe('sin-coincidencia');
  });

  it(`U = ${UMBRAL_COINCIDE} relativo: una diferencia de 10⁻⁷ coincide; una de 10⁻⁴, no`, () => {
    expect(clasificarDia(dia('2025-08-14', 4049.45 * (1 + 1e-7)), SERIE, COP).motivo).toBe('afectado');
    expect(clasificarDia(dia('2025-08-14', 4049.45 * (1 + 1e-4)), SERIE, COP).motivo).toBe('sin-coincidencia');
  });

  it('un día del cierre diario (sin lastUpdated) no es candidato, aunque coincida con el siguiente', () => {
    expect(clasificarDia(dia('2025-08-14', 4049.45, { lastUpdated: null }), SERIE, COP).motivo).toBe('cierre-diario');
  });

  it(`un día reescrito desde el corte (${CORTE_SERIE_CORREGIDA}) ya usó la serie corregida`, () => {
    expect(clasificarDia(dia('2025-08-14', 4049.45, { lastUpdated: REESCRITO_DESPUES }), SERIE, COP).motivo)
      .toBe('posterior-al-corte');
  });

  it('lastUpdated como Timestamp de Firestore también vale', () => {
    const conTimestamp = { ...dia('2025-08-14', 4049.45), lastUpdated: { toDate: () => new Date(REESCRITO_ANTES) } };
    expect(clasificarDia(conTimestamp, SERIE, COP).motivo).toBe('afectado');
  });

  it('un día que no guarda la moneda queda sin tasa; uno sin cierres en la serie, sin cierre', () => {
    expect(clasificarDia({ date: '2025-08-14', lastUpdated: REESCRITO_ANTES, USD: { totalValue: 10 } }, SERIE, COP).motivo)
      .toBe('sin-tasa');
    expect(clasificarDia(dia('2025-08-01', 4060.5), SERIE, COP).motivo).toBe('sin-cierre');
  });
});

describe('HU 6.14.2 — detectar a un inversor (RN-6.14-B, RN-6-B)', () => {
  /** INV-G de `qa.md`, desordenado como puede llegar de Firestore. */
  const INV_G = [
    dia('2025-09-10', 3950), // D-MAN
    dia('2025-08-14', 4049.45), // D-AF2
    dia('2025-06-02', 4130.1), // D-AF1: primer día afectado
    dia('2025-07-08', 4020.002), // D-IGU
    dia('2025-08-15', 4049.45), // viernes, bien
    dia('2025-08-11', 4032.6), // D-AF3
    dia('2026-01-15', 3690.25), // D-INV
    dia('2026-01-16', 3690.25, { lastUpdated: null }), // del cierre diario
  ];

  it('QA-01: INV-G sale con D-AF1, D-AF2 y D-AF3 afectados y primer día afectado el 2025-06-02', () => {
    const r = detectarInversor(INV_G, SERIE, COP);

    expect(r).toMatchObject({
      moneda: 'COP',
      dias: 8,
      diasAfectados: 3,
      primerDiaAfectado: '2025-06-02',
      ultimoDiaAfectado: '2025-08-14',
      ambiguos: 1,
      sinCoincidencia: ['2025-09-10'],
    });
    expect(r.porMotivo).toEqual({ afectado: 3, ambiguo: 1, suyo: 1, 'sin-coincidencia': 1, invierno: 1, 'cierre-diario': 1 });
  });

  it('QA-05: lo que no encaja no cuenta como afectado, pero se recalcula por estar después del primer día afectado', () => {
    const { recalculados } = detectarInversor(INV_G, SERIE, COP);

    expect(recalculados.total).toBe(5);
    expect(recalculados.porMotivo).toEqual({ ambiguo: 1, suyo: 1, 'sin-coincidencia': 1, invierno: 1, 'cierre-diario': 1 });
    expect(recalculados.aRevisar).toEqual([
      { fecha: '2025-07-08', motivo: 'ambiguo' },
      { fecha: '2025-09-10', motivo: 'sin-coincidencia' },
      { fecha: '2026-01-15', motivo: 'invierno' },
      { fecha: '2026-01-16', motivo: 'cierre-diario' },
    ]);
  });

  it('QA-01: INV-H, con todos sus días del cierre diario, no aparece', () => {
    const invH = INV_G.map((d) => ({ ...d, lastUpdated: undefined }));
    const r = detectarInversor(invH, SERIE, COP);

    expect(r).toMatchObject({ diasAfectados: 0, primerDiaAfectado: null, recalculados: { total: 0, aRevisar: [] } });
  });

  it('QA-08: tras la corrección (cada día con el cierre de su día) relanzar la detección da 0', () => {
    const corregido = INV_G
      .filter((d) => d.lastUpdated)
      .map((d) => dia(d.date, cierreDelDia(SERIE, d.date).cierre, { lastUpdated: REESCRITO_DESPUES }));

    expect(detectarInversor(corregido, SERIE, COP)).toMatchObject({ diasAfectados: 0, primerDiaAfectado: null });
    // Aunque el corte no los apartara, ya coinciden con su día
    const sinCorte = detectarInversor(corregido, SERIE, { ...COP, corte: '2100-01-01T00:00:00Z' });
    expect(sinCorte).toMatchObject({ diasAfectados: 0, primerDiaAfectado: null });
  });

  it('el primer día del inversor es el más antiguo entre sus monedas', () => {
    expect(primerDiaDelInversor([{ primerDiaAfectado: '2025-08-11' }, { primerDiaAfectado: '2024-08-19' }]))
      .toBe('2024-08-19');
    expect(primerDiaDelInversor([{ primerDiaAfectado: null }])).toBeNull();
    expect(primerDiaDelInversor([])).toBeNull();
  });
});

describe('HU 6.14.2 — marcar es un paso aparte, sin efectos dobles (RN-6.14.2-C, RN-6.14-F; QA-03, QA-08)', () => {
  it('sin días afectados no hay nada que marcar', () => {
    expect(decidirMarca(null, null)).toBe('sin-afectados');
  });

  it('sin marca previa: se marca desde el primer día afectado', () => {
    expect(decidirMarca(null, '2025-06-02')).toBe('marcar');
  });

  it('QA-08: relanzar el marcado antes de la corrección nocturna no marca dos veces ni adelanta la fecha', () => {
    expect(decidirMarca({ since: '2025-06-02', desdeInicial: '2025-06-02' }, '2025-06-02')).toBe('ya-cubierto');
  });

  it('con la corrección a mitad (since ya avanzó) y los días que faltan aún afectados: ya cubierto', () => {
    expect(decidirMarca({ since: '2025-09-01', desdeInicial: '2025-06-02' }, '2025-09-03')).toBe('ya-cubierto');
  });

  it('con una corrección pendiente posterior (otra operación), se lleva al primer día afectado', () => {
    expect(decidirMarca({ since: '2026-03-10' }, '2025-06-02')).toBe('adelantar');
  });
});
