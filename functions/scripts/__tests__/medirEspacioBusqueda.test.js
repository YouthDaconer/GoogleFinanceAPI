/**
 * HU 6.10.2 — Escenario 4: el ahorro se mide con la medición de la plataforma (QA-11).
 *
 * @see platform-docs/stories/6.10.2-indices-solo-donde-se-busca/refinamiento.md (T8)
 */

const {
  parseArgs,
  valorVigente,
  compararMediciones,
  formatearComparacion,
} = require('../medirEspacioBusqueda');

/** Un punto por minuto de la serie de Monitoring, ya en orden. */
const punto = (fin, valor) => ({
  inicio: new Date(Date.parse(fin) - 60 * 1000).toISOString(),
  fin,
  valor,
});

/** `antes` se mide el día de publicar, a mediodía; la métrica se había actualizado a las 05:00. */
const medicion = (overrides = {}) => ({
  bytes: 330241944,
  medidoEn: '2026-11-18T12:00:00.000Z',
  vigenteDesde: '2026-11-18T05:00:00.000Z',
  ...overrides,
});

describe('valorVigente — la métrica cambia una vez al día', () => {
  it('toma el valor más reciente y desde cuándo está vigente', () => {
    const puntos = [
      punto('2026-11-18T04:58:00.000Z', 340000000),
      punto('2026-11-18T04:59:00.000Z', 340000000),
      punto('2026-11-18T05:00:00.000Z', 330241944),
      punto('2026-11-18T05:01:00.000Z', 330241944),
    ];

    expect(valorVigente(puntos)).toEqual({
      bytes: 330241944,
      instante: '2026-11-18T05:01:00.000Z',
      vigenteDesde: '2026-11-18T04:59:00.000Z',
    });
  });

  it('sin un cambio en la ventana, no inventa desde cuándo está vigente', () => {
    const puntos = [punto('2026-11-18T05:00:00.000Z', 1), punto('2026-11-18T05:01:00.000Z', 1)];

    expect(valorVigente(puntos).vigenteDesde).toBeNull();
  });

  it('falla de forma explícita si Monitoring no devuelve puntos', () => {
    expect(() => valorVigente([])).toThrow('ningún punto de almacenamiento');
  });
});

describe('compararMediciones — la diferencia, sin mínimo (decisión 4)', () => {
  it('cuenta la bajada en bytes y en fracción', () => {
    const c = compararMediciones({
      antes: medicion({ bytes: 330000000, vigenteDesde: '2026-11-18T05:00:00.000Z' }),
      despues: medicion({ bytes: 264000000, vigenteDesde: '2026-11-20T05:00:00.000Z' }),
    });

    expect(c.diferenciaBytes).toBe(66000000);
    expect(c.reduccion).toBeCloseTo(0.2, 10);
    expect(c.baja).toBe(true);
    expect(c.actualizada).toBe(true);
    expect(c).not.toHaveProperty('cumple');
  });

  it('si no baja, lo dice sin tratarlo como fallo', () => {
    const c = compararMediciones({
      antes: medicion({ bytes: 330000000 }),
      despues: medicion({ bytes: 331000000, vigenteDesde: '2026-11-20T05:00:00.000Z' }),
    });

    expect(c.baja).toBe(false);
    expect(formatearComparacion(c, { antes: medicion(), despues: medicion() })).toContain('no se revierte');
  });

  it('avisa si la métrica aún no se actualizó tras publicar', () => {
    const antes = medicion();
    const despues = medicion({ medidoEn: '2026-11-18T20:00:00.000Z', vigenteDesde: '2026-11-18T05:00:00.000Z' });
    const c = compararMediciones({ antes, despues });

    expect(c.actualizada).toBe(false);
    expect(formatearComparacion(c, { antes, despues })).toContain('espera y vuelve a medir');
  });

  it('un valor que no cambió en toda la ventana no cuenta como actualizado', () => {
    const despues = medicion({ medidoEn: '2026-11-20T12:00:00.000Z', vigenteDesde: null });
    const c = compararMediciones({ antes: medicion(), despues });

    expect(c.actualizada).toBe(false);
    expect(formatearComparacion(c, { antes: medicion(), despues })).toContain('antes de la ventana leída');
  });

  it('relanzar la medición da el mismo resultado (QA-11)', () => {
    const m = {
      antes: medicion({ bytes: 330000000 }),
      despues: medicion({ bytes: 264000000, vigenteDesde: '2026-11-20T05:00:00.000Z' }),
    };

    expect(compararMediciones(m)).toEqual(compararMediciones(m));
  });
});

describe('parseArgs', () => {
  it('lee el comando y la etiqueta', () => {
    expect(parseArgs(['medir', '--etiqueta', 'antes'])).toEqual({ comando: 'medir', etiqueta: 'antes', help: false });
  });

  it('pide ayuda ante un argumento de más', () => {
    const error = jest.spyOn(console, 'error').mockImplementation(() => {});

    expect(parseArgs(['medir', 'otra']).help).toBe(true);
    expect(error).toHaveBeenCalledWith('[medirEspacioBusqueda] Argumento no reconocido: otra');
    error.mockRestore();
  });
});
