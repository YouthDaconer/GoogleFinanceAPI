/**
 * HU 6.9.3 — Interruptores y comparación de las lecturas acotadas.
 *
 * Defiende:
 * - RN-6-H (QA-10): con `SCOPED_READS_ENABLED=false` se usa la lectura de
 *   siempre y la acotada ni se intenta.
 * - RN-6-B (QA-09): con `SCOPED_READS_VERIFY=true` se comparan las dos; ante
 *   cualquier diferencia se usa la de siempre y el área queda bloqueada.
 * - Nunca un dato inventado: si la acotada falla, se usa la de siempre.
 *
 * @see platform-docs/stories/6.9.3-solo-periodo-y-ventas-del-dia/refinamiento.md (T1, T7a)
 */

const {
  AREAS,
  MODOS,
  leerInterruptores,
  ordenarPorId,
  compararPorId,
  leerAcotado,
  describirMedicion,
  estaBloqueada,
  _reiniciarBloqueos,
} = require('../scopedReads');

const doc = (id, valor = {}) => ({ id, valor });
const lectura = (ids, lecturas) => ({ docs: ids.map((id) => doc(id)), lecturas });

const ENTORNO_ORIGINAL = { ...process.env };

beforeEach(() => {
  delete process.env.SCOPED_READS_ENABLED;
  delete process.env.SCOPED_READS_VERIFY;
  _reiniciarBloqueos();
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  process.env = { ...ENTORNO_ORIGINAL };
  jest.restoreAllMocks();
});

describe('leerInterruptores (HU 6.9.3, RN-6-H, RN-6-B)', () => {
  it('por defecto lee acotado y sin verificar', () => {
    expect(leerInterruptores()).toEqual({ acotadas: true, verificar: false });
  });

  it('SCOPED_READS_ENABLED=false vuelve a la lectura de siempre; cualquier otro valor la deja encendida', () => {
    process.env.SCOPED_READS_ENABLED = 'false';
    expect(leerInterruptores().acotadas).toBe(false);
    process.env.SCOPED_READS_ENABLED = 'true';
    expect(leerInterruptores().acotadas).toBe(true);
  });

  it('sólo SCOPED_READS_VERIFY=true activa la comparación', () => {
    process.env.SCOPED_READS_VERIFY = 'true';
    expect(leerInterruptores().verificar).toBe(true);
    process.env.SCOPED_READS_VERIFY = '1';
    expect(leerInterruptores().verificar).toBe(false);
  });
});

describe('ordenarPorId / compararPorId', () => {
  it('ordena por identificador sin tocar la lista original', () => {
    const lista = [doc('tx-c'), doc('tx-a'), doc('tx-B'), doc('tx-b')];
    expect(ordenarPorId(lista).map((d) => d.id)).toEqual(['tx-B', 'tx-a', 'tx-b', 'tx-c']);
    expect(lista.map((d) => d.id)).toEqual(['tx-c', 'tx-a', 'tx-B', 'tx-b']);
  });

  it('dos listas con los mismos documentos en el mismo orden son iguales', () => {
    expect(compararPorId([doc('a'), doc('b')], [doc('a'), doc('b')])).toEqual({ iguales: true, faltan: [], sobran: [] });
  });

  it('dice qué falta en la acotada y qué sobra', () => {
    expect(compararPorId([doc('a'), doc('b'), doc('c')], [doc('a'), doc('d')]))
      .toEqual({ iguales: false, faltan: ['b', 'c'], sobran: ['d'] });
  });

  it('los mismos documentos en otro orden NO son iguales: el cálculo los sumaría en otro orden', () => {
    expect(compararPorId([doc('a'), doc('b')], [doc('b'), doc('a')])).toEqual({ iguales: false, faltan: [], sobran: [] });
  });
});

describe('leerAcotado — elige el camino (HU 6.9.3)', () => {
  it('por defecto usa la acotada y no toca la de siempre', async () => {
    const completa = jest.fn(async () => lectura(['a', 'b', 'c'], 4200));
    const acotada = jest.fn(async () => lectura(['a'], 4));

    const r = await leerAcotado({ area: AREAS.LOTES_VENDIDOS, acotada, completa });

    expect(r).toEqual({ docs: [doc('a')], lecturas: 4, modo: MODOS.ACOTADA });
    expect(completa).not.toHaveBeenCalled();
  });

  it('QA-10 — con el interruptor apagado usa la de siempre y la acotada ni se intenta (RN-6-H)', async () => {
    process.env.SCOPED_READS_ENABLED = 'false';
    const completa = jest.fn(async () => lectura(['a', 'b'], 310));
    const acotada = jest.fn(async () => lectura(['a'], 9));

    const r = await leerAcotado({ area: AREAS.DIVIDENDOS, acotada, completa });

    expect(r.modo).toBe(MODOS.COMPLETA);
    expect(r.lecturas).toBe(310);
    expect(acotada).not.toHaveBeenCalled();
  });

  it('si la acotada falla (p. ej. falta su índice) usa la de siempre y lo avisa', async () => {
    const completa = jest.fn(async () => lectura(['a'], 310));
    const acotada = jest.fn(async () => { throw new Error('9 FAILED_PRECONDITION: The query requires an index'); });

    const r = await leerAcotado({ area: AREAS.DIVIDENDOS, acotada, completa });

    expect(r.modo).toBe(MODOS.POR_ERROR);
    expect(r.docs).toEqual([doc('a')]);
    expect(r.error).toMatch(/FAILED_PRECONDITION/);
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('[scopedReads] dividendos: la lectura acotada falló'));
    // Un fallo no bloquea: la siguiente vuelve a intentar la acotada
    expect(estaBloqueada(AREAS.DIVIDENDOS)).toBe(false);
  });
});

describe('leerAcotado — verificación antes de aplicar (RN-6-B, QA-09)', () => {
  beforeEach(() => { process.env.SCOPED_READS_VERIFY = 'true'; });

  it('sin diferencias usa la acotada e informa la medición antes/después', async () => {
    const r = await leerAcotado({
      area: AREAS.LOTES_VENDIDOS,
      completa: async () => lectura(['a', 'b'], 4200),
      acotada: async () => lectura(['a', 'b'], 4),
    });

    expect(r.modo).toBe(MODOS.VERIFICADA);
    expect(r.lecturas).toBe(4);
    expect(r.lecturasCompleta).toBe(4200);
    expect(r.lecturasAcotada).toBe(4);
    expect(console.warn).not.toHaveBeenCalled();
  });

  it('compara sólo lo que el cálculo usa de la lectura de siempre (`comparable`)', async () => {
    const r = await leerAcotado({
      area: AREAS.DIVIDENDOS,
      completa: async () => ({ docs: [doc('d1', { date: '2026-08-05' }), doc('d2', { date: '2026-11-03' })], lecturas: 2 }),
      acotada: async () => ({ docs: [doc('d1', { date: '2026-08-05' })], lecturas: 1 }),
      comparable: (docs) => docs.filter((d) => d.valor.date <= '2026-10-31'),
    });

    expect(r.modo).toBe(MODOS.VERIFICADA);
    expect(r.docs.map((d) => d.id)).toEqual(['d1']);
  });

  it('una diferencia bloquea el cambio: usa la de siempre, lo avisa y no vuelve a usar la acotada', async () => {
    const completa = jest.fn(async () => lectura(['a', 'b', 'c'], 310));
    const acotada = jest.fn(async () => lectura(['a', 'c'], 9)); // le falta 'b' a propósito

    const r = await leerAcotado({ area: AREAS.DIVIDENDOS, acotada, completa });

    expect(r.modo).toBe(MODOS.POR_DIFERENCIA);
    expect(r.docs.map((d) => d.id)).toEqual(['a', 'b', 'c']);
    expect(r.lecturas).toBe(310);
    expect(r.diferencias).toEqual({ faltan: ['b'], sobran: [] });
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('[scopedReads] DIFERENCIA en dividendos: faltan [b]'));
    expect(estaBloqueada(AREAS.DIVIDENDOS)).toBe(true);

    // Aunque se apague la verificación, en esta instancia sigue la lectura de siempre
    delete process.env.SCOPED_READS_VERIFY;
    acotada.mockClear();
    const despues = await leerAcotado({ area: AREAS.DIVIDENDOS, acotada, completa });
    expect(despues.modo).toBe(MODOS.BLOQUEADA);
    expect(despues.lecturas).toBe(310);
    expect(acotada).not.toHaveBeenCalled();
  });

  it('el bloqueo es por área: una diferencia en dividendos no toca los lotes', async () => {
    await leerAcotado({
      area: AREAS.DIVIDENDOS,
      completa: async () => lectura(['a', 'b'], 2),
      acotada: async () => lectura(['a'], 1),
    });

    const lotes = await leerAcotado({
      area: AREAS.LOTES_VENDIDOS,
      completa: async () => lectura(['x'], 4200),
      acotada: async () => lectura(['x'], 1),
    });

    expect(estaBloqueada(AREAS.DIVIDENDOS)).toBe(true);
    expect(lotes.modo).toBe(MODOS.VERIFICADA);
  });

  it('si falla la lectura de siempre, falla todo, como antes de la historia', async () => {
    await expect(leerAcotado({
      area: AREAS.LOTES_VENDIDOS,
      completa: async () => { throw new Error('DEADLINE_EXCEEDED'); },
      acotada: async () => lectura(['a'], 1),
    })).rejects.toThrow('DEADLINE_EXCEEDED');
  });
});

describe('describirMedicion', () => {
  it('dice cuánto leyó y por qué camino; con verificación, también el antes y el después', () => {
    expect(describirMedicion({ lecturas: 4, modo: MODOS.ACOTADA })).toBe('4 documentos leídos (acotada)');
    expect(describirMedicion({ lecturas: 4, modo: MODOS.VERIFICADA, lecturasCompleta: 4200, lecturasAcotada: 4 }))
      .toBe('4 documentos leídos (acotada-verificada) — antes 4200, después 4');
  });
});
