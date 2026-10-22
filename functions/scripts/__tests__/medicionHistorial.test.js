/**
 * HU 6.2 — El veredicto de la medición del historial.
 *
 * Defiende quién decide si el cambio cumplió: el ahorro de espacio, que ninguna
 * cifra se movió y que las consultas por fecha no empeoraron.
 *
 * @see platform-docs/stories/6.2-dejar-pagar-busqueda-sin-uso/historia.md
 * @see platform-docs/stories/6.2-dejar-pagar-busqueda-sin-uso/refinamiento.md (T7)
 */

const {
  EPSILON,
  BYTES_POR_GIB,
  mediana,
  compararEspacio,
  compararLatencia,
  compararCifras,
  formatearVeredicto,
} = require('../medicionHistorial');

// === Fábricas locales ===

const gib = (valor) => valor * BYTES_POR_GIB;

const metricas = (overrides = {}) => ({
  totalInvestment: 45000,
  totalValue: 52300,
  totalROI: 16.22,
  dailyReturn: 0.045,
  unrealizedProfitAndLoss: 6100,
  ...overrides,
});

const diaDeCifras = (overrides = {}) => ({
  date: '2026-10-19',
  USD: { ...metricas(), assetPerformance: { AAPL_stock: metricas({ totalValue: 18500 }) } },
  COP: metricas({ totalValue: 210000000 }),
  ...overrides,
});

const volcado = (overrides = {}) => ({
  hashes: { '2026-10-18': 'a1b2c3d4e5f60718', '2026-10-19': 'f0e1d2c3b4a59687' },
  muestra: { '2026-10-19': diaDeCifras() },
  dias: 2,
  ...overrides,
});

// === Tests ===

describe('mediana — RN-6.2-N habla del valor típico, no del promedio', () => {
  it('devuelve el valor central de una muestra impar', () => {
    expect(mediana([30, 10, 20])).toBe(20);
  });

  it('promedia los dos centrales en una muestra par', () => {
    expect(mediana([10, 20, 30, 40])).toBe(25);
  });

  it('no se deja arrastrar por una corrida lenta aislada', () => {
    expect(mediana([100, 102, 101, 103, 5000])).toBe(102);
  });

  it('devuelve null sin muestras y descarta valores no finitos', () => {
    expect(mediana([])).toBeNull();
    expect(mediana([NaN, Infinity])).toBeNull();
    expect(mediana([10, NaN, 30])).toBe(20);
  });
});

describe('compararEspacio — Escenario 1: baja a menos de la tercera parte', () => {
  it('cumple cuando 2,79 GiB bajan a 0,25 GiB, el valor que espera la historia', () => {
    const r = compararEspacio({ bytesAntes: gib(2.79), bytesDespues: gib(0.25) });

    expect(r.cumple).toBe(true);
    expect(r.gibAntes).toBeCloseTo(2.79, 2);
    expect(r.gibDespues).toBeCloseTo(0.25, 2);
    expect(r.fraccion).toBeCloseTo(0.0896, 3);
  });

  it('NO cumple justo en un tercio: el criterio es "menos de", no "hasta"', () => {
    const r = compararEspacio({ bytesAntes: gib(3), bytesDespues: gib(1) });

    expect(r.fraccion).toBeCloseTo(1 / 3, 10);
    expect(r.cumple).toBe(false);
  });

  it('cumple un pelo por debajo de un tercio', () => {
    expect(compararEspacio({ bytesAntes: gib(3), bytesDespues: gib(0.999) }).cumple).toBe(true);
  });

  it('no cumple si el espacio no se movió', () => {
    const r = compararEspacio({ bytesAntes: gib(2.79), bytesDespues: gib(2.79) });

    expect(r.cumple).toBe(false);
    expect(r.reduccion).toBe(0);
  });
});

describe('compararCifras — Escenario 2: idénticas al céntimo (RN-6-A)', () => {
  it('da por idénticos dos volcados iguales', () => {
    const r = compararCifras({ antes: volcado(), despues: volcado() });

    expect(r.identicasExactamente).toBe(true);
    expect(r.cumple).toBe(true);
    expect(r.diasComparados).toBe(2);
    expect(r.hashesDistintos).toEqual([]);
  });

  it('detecta un día cuyo hash cambió, aunque no esté en la muestra íntegra', () => {
    const r = compararCifras({
      antes: volcado(),
      despues: volcado({ hashes: { '2026-10-18': 'OTRO000000000000', '2026-10-19': 'f0e1d2c3b4a59687' } }),
    });

    expect(r.cumple).toBe(false);
    expect(r.hashesDistintos).toHaveLength(1);
    expect(r.hashesDistintos[0].dia).toBe('2026-10-18');
  });

  it('un céntimo de diferencia en una métrica incumple el criterio', () => {
    const despues = volcado();
    despues.muestra['2026-10-19'].USD.totalValue = 52300.01;

    const r = compararCifras({ antes: volcado(), despues });

    expect(r.identicasExactamente).toBe(false);
    expect(r.cumple).toBe(false);
    expect(r.masAllaDelCentimo).toHaveLength(1);
    expect(r.masAllaDelCentimo[0].ruta).toBe('2026-10-19.USD.totalValue');
  });

  it('una diferencia por debajo del céntimo no incumple, pero se informa', () => {
    const despues = volcado();
    despues.muestra['2026-10-19'].USD.totalValue = 52300 + EPSILON / 2;

    const r = compararCifras({ antes: volcado(), despues });

    expect(r.cumple).toBe(true);
    expect(r.identicasExactamente).toBe(false);
    expect(r.diferencias).toHaveLength(1);
    expect(r.masAllaDelCentimo).toEqual([]);
  });

  it('baja hasta el detalle por activo dentro de assetPerformance', () => {
    const despues = volcado();
    despues.muestra['2026-10-19'].USD.assetPerformance.AAPL_stock.totalROI = 23.34;

    const r = compararCifras({ antes: volcado(), despues });

    expect(r.cumple).toBe(false);
    expect(r.masAllaDelCentimo[0].ruta).toBe('2026-10-19.USD.assetPerformance.AAPL_stock.totalROI');
  });

  it('señala una moneda que desaparece de un día', () => {
    const despues = volcado();
    delete despues.muestra['2026-10-19'].COP;

    const r = compararCifras({ antes: volcado(), despues });

    expect(r.cumple).toBe(false);
    expect(r.soloEnAntes).toContain('2026-10-19.COP');
  });

  it('trata dos NaN como el mismo valor: Firestore los persiste en los doubles', () => {
    const antes = volcado();
    const despues = volcado();
    antes.muestra['2026-10-19'].USD.totalROI = NaN;
    despues.muestra['2026-10-19'].USD.totalROI = NaN;

    expect(compararCifras({ antes, despues }).cumple).toBe(true);
  });

  it('NO cuenta como diferencia los días que añadió el cierre diario entre las dos tomas', () => {
    // Es el caso real de la medición a 7 días: aparecen ~5 días nuevos y la
    // ventana de muestra se desplaza entera.
    const despues = volcado({
      hashes: {
        '2026-10-18': 'a1b2c3d4e5f60718',
        '2026-10-19': 'f0e1d2c3b4a59687',
        '2026-10-26': '0011223344556677',
        '2026-10-27': '8899aabbccddeeff',
      },
      muestra: { '2026-10-27': diaDeCifras({ date: '2026-10-27' }) },
      dias: 4,
    });

    const r = compararCifras({ antes: volcado(), despues });

    expect(r.cumple).toBe(true);
    expect(r.identicasExactamente).toBe(true);
    expect(r.diasNuevos).toEqual(['2026-10-26', '2026-10-27']);
    expect(r.diasComparados).toBe(2);
  });

  it('SÍ cuenta como incumplimiento un día que estaba y ya no está (RN-6.2-J)', () => {
    const r = compararCifras({
      antes: volcado(),
      despues: volcado({ hashes: { '2026-10-19': 'f0e1d2c3b4a59687' }, dias: 1 }),
    });

    expect(r.cumple).toBe(false);
    expect(r.diasPerdidos).toEqual(['2026-10-18']);
  });

  it('no compara la muestra de un día que solo está en una de las dos tomas', () => {
    const despues = volcado({ muestra: { '2026-10-18': diaDeCifras({ date: '2026-10-18' }) } });

    const r = compararCifras({ antes: volcado(), despues });

    expect(r.diferencias).toEqual([]);
    expect(r.cumple).toBe(true);
  });
});

describe('compararLatencia — Escenario 3: no empeora más del 10 % (RN-6.2-N)', () => {
  const muestra = (ms) => [ms, ms, ms];

  it('cumple si la mediana no se mueve', () => {
    const r = compararLatencia({
      antes: { 'ultimo dia': muestra(100) },
      despues: { 'ultimo dia': muestra(100) },
    });

    expect(r.cumple).toBe(true);
    expect(r.consultas[0].variacion).toBe(0);
  });

  it('cumple justo en +10 %: el criterio es "no más de"', () => {
    const r = compararLatencia({
      antes: { 'ultimo dia': muestra(100) },
      despues: { 'ultimo dia': muestra(110) },
    });

    expect(r.consultas[0].variacion).toBeCloseTo(0.1, 10);
    expect(r.cumple).toBe(true);
  });

  it('no cumple a partir de +10,1 %', () => {
    const r = compararLatencia({
      antes: { 'ultimo dia': muestra(100) },
      despues: { 'ultimo dia': muestra(110.1) },
    });

    expect(r.cumple).toBe(false);
  });

  it('una consulta que mejora no tapa a otra que empeora', () => {
    const r = compararLatencia({
      antes: { rango: muestra(100), completo: muestra(100) },
      despues: { rango: muestra(50), completo: muestra(200) },
    });

    expect(r.cumple).toBe(false);
    expect(r.consultas.find((c) => c.nombre === 'rango').cumple).toBe(true);
    expect(r.consultas.find((c) => c.nombre === 'completo').cumple).toBe(false);
  });

  it('no da por buena una consulta que falta en una de las dos tomas', () => {
    const r = compararLatencia({ antes: { rango: muestra(100) }, despues: {} });

    expect(r.cumple).toBe(false);
    expect(r.sinMedir).toEqual(['rango']);
    expect(r.consultas[0].cumple).toBeNull();
  });
});

describe('formatearVeredicto — el informe dice qué falló', () => {
  it('marca con ❌ el criterio incumplido y nombra la cifra que cambió', () => {
    const despues = volcado();
    despues.muestra['2026-10-19'].USD.totalValue = 52400;

    const texto = formatearVeredicto({
      espacio: compararEspacio({ bytesAntes: gib(2.79), bytesDespues: gib(0.25) }),
      cifras: compararCifras({ antes: volcado(), despues }),
      latencia: compararLatencia({ antes: { x: [100] }, despues: { x: [100] } }),
    });

    expect(texto).toContain('✅ Escenario 1');
    expect(texto).toContain('❌ Escenario 2');
    expect(texto).toContain('2026-10-19.USD.totalValue');
    expect(texto).toContain('✅ Escenario 3');
  });
});
