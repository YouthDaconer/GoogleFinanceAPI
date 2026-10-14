/**
 * HU 5.6 — Los datos duros salen de la plataforma, no del modelo.
 *
 * Lo que defiende este archivo es la razón de ser de la historia: que el
 * periodo de una cifra sea **el que declara `finance-query`** y no el que el
 * modelo dé por bueno (Escenario 6), que un activo sin cobertura no se quede
 * sin análisis (Escenario 4), que la plataforma caída degrade en vez de
 * cancelar (Escenario 5) y que las etiquetas estén en el idioma del usuario
 * (Escenario 8).
 *
 * @see services/copilot/copilotFundamentals.js
 */

jest.mock('../../config', () => ({
  FINANCE_QUERY_API_URL: 'https://api.ejemplo/v1',
  getServiceHeaders: (extra) => ({ 'x-service-token': 'token', ...extra }),
}));

jest.mock('../../financeQuery', () => ({ getHistoricalPrices: jest.fn() }));

const fundamentals = require('../copilotFundamentals');

// ============================================================================
// FÁBRICAS
// ============================================================================

/** Un trimestre del estado de resultados. */
const crearTrimestre = (overrides = {}) => ({
  endDate: '2026-06-30',
  totalRevenue: 70000000000,
  operatingIncome: 31500000000,
  netIncome: 25000000000,
  ...overrides,
});

const crearIncome = (overrides = {}) => ({
  symbol: 'MSFT',
  frequency: 'quarterly',
  currency: 'USD',
  // Medido contra produccion el 13/10/2026: la plataforma devuelve CUATRO
  // trimestres, asi que el mismo trimestre del año pasado NO esta en la serie.
  statements: [
    crearTrimestre(),
    crearTrimestre({ endDate: '2026-03-31', totalRevenue: 68000000000 }),
    crearTrimestre({ endDate: '2025-12-31', totalRevenue: 66000000000 }),
    crearTrimestre({ endDate: '2025-09-30', totalRevenue: 65000000000 }),
  ],
  ...overrides,
});

const crearAnual = (overrides = {}) => ({
  symbol: 'MSFT',
  frequency: 'annual',
  currency: 'USD',
  statements: [
    crearTrimestre({ endDate: '2026-06-30', totalRevenue: 281000000000 }),
    crearTrimestre({ endDate: '2025-06-30', totalRevenue: 245000000000 }),
  ],
  ...overrides,
});

const crearQuote = (overrides = {}) => ({
  symbol: 'MSFT',
  pe: '34.50',
  marketCap: '3.1T',
  exDividend: '2026-08-21',
  ...overrides,
});

const crearAnalisis = (overrides = {}) => ({
  symbol: 'MSFT',
  recommendationKey: 'strong_buy',
  targetPrice: { current: 400, mean: 520, numberOfAnalysts: 42 },
  earningsEstimates: [{ period: '0q', endDate: '2026-10-28', avg: 3.1 }],
  ...overrides,
});

const crearNoticias = () => ([
  { title: 'Microsoft sube tras resultados', link: 'https://reuters.com/a', source: 'Reuters' },
  { title: 'Azure acelera', link: 'https://ft.com/b', source: 'FT' },
]);

const crearHolding = (overrides = {}) => ({
  symbol: 'MSFT',
  name: 'Microsoft',
  asset_type: 'stock',
  ...overrides,
});

const fetchersQueResponden = (overrides = {}) => ({
  fetchIncome: jest.fn().mockImplementation(
    async (_s, frequency) => (frequency === 'annual' ? crearAnual() : crearIncome()),
  ),
  fetchQuotes: jest.fn().mockResolvedValue([crearQuote()]),
  fetchAnalysis: jest.fn().mockResolvedValue(crearAnalisis()),
  fetchNews: jest.fn().mockResolvedValue(crearNoticias()),
  ...overrides,
});

/** El 30/06/2026 cerró el trimestre de las fábricas: "hoy" es poco después. */
const HOY = new Date('2026-10-13T12:00:00Z');

const etiquetasDe = (findings) => findings.map((f) => f.label);
const buscar = (findings, label) => findings.find((f) => f.label === label);

// ============================================================================

describe('el periodo de una cifra es el que declara la plataforma (Escenario 6)', () => {
  it('toma el trimestre más reciente, no el primero de la lista', () => {
    const desordenado = crearIncome({
      statements: [
        crearTrimestre({ endDate: '2024-06-30', totalRevenue: 50000000000 }),
        crearTrimestre({ endDate: '2026-06-30' }),
        crearTrimestre({ endDate: '2025-06-30' }),
      ],
    });

    const ingresos = buscar(fundamentals.hallazgosDeIngresos(desordenado, 'es', HOY), 'Ingresos');

    expect(ingresos.period).toBe('Q2 2026');
    expect(ingresos.publishedAt).toBe('2026-06-30');
  });

  it('una cifra de hace más de un año no se inyecta: el activo cae a la web', () => {
    const viejo = crearIncome({ statements: [crearTrimestre({ endDate: '2024-06-30' })] });

    expect(fundamentals.hallazgosDeIngresos(viejo, 'es', HOY)).toEqual([]);
  });

  it('sin estados financieros no hay hallazgos, y no lanza', () => {
    expect(fundamentals.hallazgosDeIngresos(null, 'es', HOY)).toEqual([]);
    expect(fundamentals.hallazgosDeIngresos({ statements: [] }, 'es', HOY)).toEqual([]);
  });
});

describe('las cifras se calculan, no se copian', () => {
  it('el margen operativo sale de los enteros crudos', () => {
    // 31.500M / 70.000M = 45,0 %
    const margen = buscar(
      fundamentals.hallazgosDeIngresos(crearIncome(), 'es', HOY), 'Margen operativo',
    );

    expect(margen.value).toBe('45.0%');
  });

  it('con el trimestre del año pasado en la serie, el crecimiento es interanual', () => {
    const conInteranual = crearIncome({
      statements: [crearTrimestre(), crearTrimestre({ endDate: '2025-06-30', totalRevenue: 64000000000 })],
    });

    // 70.000M frente a 64.000M = +9,4 %
    const crecimiento = buscar(
      fundamentals.hallazgosDeIngresos(conInteranual, 'es', HOY),
      'Crecimiento interanual de ingresos',
    );

    expect(crecimiento.value).toBe('+9.4%');
    expect(crecimiento.period).toBe('Q2 2026 vs Q2 2025');
  });

  it('sin ese trimestre cae al crecimiento ANUAL, y lo dice', () => {
    // Es el caso real: la plataforma devuelve cuatro trimestres y el de hace
    // un año no esta. Comparar contra el trimestre anterior y llamarlo
    // interanual seria un numero correcto con una etiqueta falsa.
    // 281.000M frente a 245.000M = +14,7 %
    const crecimiento = buscar(
      fundamentals.hallazgosDeIngresos(crearIncome(), 'es', HOY, crearAnual()),
      'Crecimiento anual de ingresos',
    );

    expect(crecimiento.value).toBe('+14.7%');
    expect(crecimiento.period).toBe('FY 2026 vs FY 2025');
  });

  it('sin serie anual tampoco se inventa el crecimiento', () => {
    expect(etiquetasDe(fundamentals.hallazgosDeIngresos(crearIncome(), 'es', HOY)))
      .not.toContain('Crecimiento anual de ingresos');
  });

  it('con un solo año no hay con qué comparar', () => {
    const unAno = crearAnual({ statements: [crearTrimestre({ endDate: '2026-06-30' })] });

    expect(etiquetasDe(fundamentals.hallazgosDeIngresos(crearIncome(), 'es', HOY, unAno)))
      .not.toContain('Crecimiento anual de ingresos');
  });

  it('el crecimiento negativo lleva su signo', () => {
    const cayendo = crearAnual({
      statements: [
        crearTrimestre({ endDate: '2026-06-30', totalRevenue: 200000000000 }),
        crearTrimestre({ endDate: '2025-06-30', totalRevenue: 250000000000 }),
      ],
    });

    expect(buscar(fundamentals.hallazgosDeIngresos(crearIncome(), 'es', HOY, cayendo),
                  'Crecimiento anual de ingresos').value).toBe('-20.0%');
  });

  it('los importes grandes se presentan en miles de millones', () => {
    expect(fundamentals.formatearImporte(70000000000, 'USD')).toBe('70.00 B USD');
    expect(fundamentals.formatearImporte(-2500000, 'EUR')).toBe('-2.5 M EUR');
  });
});

describe('lo que trae cada origen de la plataforma', () => {
  it('la valoración sale del quote y no lleva periodo: es el valor de hoy', () => {
    const hallazgos = fundamentals.hallazgosDeValoracion(crearQuote(), 'es');

    expect(etiquetasDe(hallazgos)).toEqual([
      'P/E (TTM)', 'Capitalización de mercado', 'Fecha ex-dividendo',
    ]);
    expect(hallazgos.every((f) => f.period === null)).toBe(true);
  });

  it('un quote sin P/E no inventa la etiqueta', () => {
    const hallazgos = fundamentals.hallazgosDeValoracion(crearQuote({ pe: null }), 'es');

    expect(etiquetasDe(hallazgos)).not.toContain('P/E (TTM)');
  });

  it('el consenso y la próxima fecha de resultados salen de /analysis', () => {
    const hallazgos = fundamentals.hallazgosDeAnalistas(crearAnalisis(), 'es');

    expect(buscar(hallazgos, 'Consenso de analistas').value).toBe('strongBuy');
    expect(buscar(hallazgos, 'Precio objetivo medio').value).toBe('520 (42 analistas)');
    // `endDate` es el cierre del trimestre estimado, no el día de publicación:
    // etiquetarlo como "próxima fecha" anunciaba fechas ya pasadas.
    const bpa = buscar(hallazgos, 'Estimación de BPA del trimestre en curso');
    expect(bpa.value).toBe('3.1');
    expect(bpa.period).toBe('Q4 2026');
    expect(etiquetasDe(hallazgos)).not.toContain('Próxima fecha de resultados');
  });

  it('los titulares citan el enlace real del medio, no la plataforma', () => {
    const hallazgos = fundamentals.hallazgosDeTitulares(crearNoticias(), 'es');

    expect(hallazgos[0].sourceUrl).toBe('https://reuters.com/a');
    expect(hallazgos[0].sourceTitle).toBe('Reuters');
    expect(hallazgos[0].verified).toBe(true);
  });

  it('un titular sin enlace no se publica: sin fuente no hay hallazgo (RN-5-E)', () => {
    const hallazgos = fundamentals.hallazgosDeTitulares(
      [{ title: 'Sin enlace', source: 'X' }], 'es',
    );

    expect(hallazgos).toEqual([]);
  });
});

describe('la procedencia de un dato de la plataforma se conoce (RN-5.6-C)', () => {
  it('todo hallazgo de cifras llega marcado como verificado', () => {
    const hallazgos = fundamentals.hallazgosDeIngresos(crearIncome(), 'es', HOY);

    expect(hallazgos.every((f) => f.verified === true)).toBe(true);
  });

  it('se atribuye a la plataforma, con una sola fuente para todas', () => {
    const hallazgos = fundamentals.hallazgosDeIngresos(crearIncome(), 'es', HOY);
    const urls = new Set(hallazgos.map((f) => f.sourceUrl));

    expect(urls).toEqual(new Set([fundamentals.FUENTE_PLATAFORMA.url]));
    expect(hallazgos[0].sourceTitle).toContain('Portastock');
  });
});

describe('las etiquetas están en el idioma del usuario (Escenario 8)', () => {
  it('en inglés', () => {
    const ingresos = fundamentals.hallazgosDeIngresos(crearIncome(), 'en', HOY);
    const valoracion = fundamentals.hallazgosDeValoracion(crearQuote(), 'en');
    const analistas = fundamentals.hallazgosDeAnalistas(crearAnalisis(), 'en');

    expect(etiquetasDe(ingresos)).toContain('Revenue');
    expect(etiquetasDe(ingresos)).toContain('Operating margin');
    expect(etiquetasDe(valoracion)).toContain('Market cap');
    expect(etiquetasDe(analistas)).toContain('Mean target price');
  });

  it('un idioma desconocido cae a español, no a una etiqueta vacía', () => {
    const hallazgos = fundamentals.hallazgosDeIngresos(crearIncome(), 'pt', HOY);

    expect(etiquetasDe(hallazgos)).toContain('Ingresos');
  });
});

describe('a quién se le pide qué (RN-5.6-E, Escenario 4)', () => {
  it('a un ETF no se le piden estados financieros: no tiene 10-Q', async () => {
    const fetchers = fetchersQueResponden();

    await fundamentals.obtenerDatosDePlataforma(
      [crearHolding({ symbol: 'VUAA.L', asset_type: 'etf' })], { language: 'es' }, fetchers, HOY,
    );

    expect(fetchers.fetchIncome).not.toHaveBeenCalled();
  });

  it('a una cripto tampoco', async () => {
    const fetchers = fetchersQueResponden();

    await fundamentals.obtenerDatosDePlataforma(
      [crearHolding({ symbol: 'BTC-USD', asset_type: 'crypto' })], { language: 'es' }, fetchers, HOY,
    );

    expect(fetchers.fetchIncome).not.toHaveBeenCalled();
  });

  it('a una acción de bolsa local sí: el 404 es información, no una excusa', async () => {
    const fetchers = fetchersQueResponden({ fetchIncome: jest.fn().mockResolvedValue(null) });

    const datos = await fundamentals.obtenerDatosDePlataforma(
      [crearHolding({ symbol: 'FALABELLA.SN' })], { language: 'es' }, fetchers, HOY,
    );

    expect(fetchers.fetchIncome).toHaveBeenCalledWith('FALABELLA.SN');
    expect(datos.get('FALABELLA.SN').cubierto).toBe(false);
  });

  it('un ETF sigue trayendo lo que la plataforma sí sabe de él', async () => {
    const fetchers = fetchersQueResponden({
      fetchQuotes: jest.fn().mockResolvedValue([crearQuote({ symbol: 'VUAA.L' })]),
    });

    const datos = await fundamentals.obtenerDatosDePlataforma(
      [crearHolding({ symbol: 'VUAA.L', asset_type: 'etf' })], { language: 'es' }, fetchers, HOY,
    );

    expect(etiquetasDe(datos.get('VUAA.L').findings)).toContain('Capitalización de mercado');
    // Pero sus cifras no están cubiertas: se las sigue buscando la web.
    expect(datos.get('VUAA.L').cubierto).toBe(false);
  });

  it('a una acción se le piden las dos series: trimestral y anual', async () => {
    const fetchers = fetchersQueResponden();

    await fundamentals.obtenerDatosDePlataforma(
      [crearHolding()], { language: 'es' }, fetchers, HOY,
    );

    expect(fetchers.fetchIncome).toHaveBeenCalledWith('MSFT');
    expect(fetchers.fetchIncome).toHaveBeenCalledWith('MSFT', 'annual');
  });

  it('los quotes se piden con UNA sola llamada para toda la tanda', async () => {
    const fetchers = fetchersQueResponden({
      fetchQuotes: jest.fn().mockResolvedValue([crearQuote(), crearQuote({ symbol: 'AAPL' })]),
    });

    await fundamentals.obtenerDatosDePlataforma(
      [crearHolding(), crearHolding({ symbol: 'AAPL', name: 'Apple' })],
      { language: 'es' }, fetchers, HOY,
    );

    expect(fetchers.fetchQuotes).toHaveBeenCalledTimes(1);
    expect(fetchers.fetchQuotes).toHaveBeenCalledWith(['MSFT', 'AAPL']);
  });
});

describe('la plataforma caída degrada, no cancela (Escenario 5, RN-5.6-D)', () => {
  it('con todos los orígenes caídos, el activo queda sin cubrir y no se lanza', async () => {
    const fetchers = {
      fetchIncome: jest.fn().mockResolvedValue(null),
      fetchQuotes: jest.fn().mockResolvedValue(null),
      fetchAnalysis: jest.fn().mockResolvedValue(null),
      fetchNews: jest.fn().mockResolvedValue(null),
    };

    const datos = await fundamentals.obtenerDatosDePlataforma(
      [crearHolding()], { language: 'es' }, fetchers, HOY,
    );

    expect(datos.get('MSFT')).toEqual({ findings: [], cubierto: false });
  });

  it('un origen caído no se lleva por delante a los demás', async () => {
    const fetchers = fetchersQueResponden({ fetchAnalysis: jest.fn().mockResolvedValue(null) });

    const { findings } = (await fundamentals.obtenerDatosDePlataforma(
      [crearHolding()], { language: 'es' }, fetchers, HOY,
    )).get('MSFT');

    expect(etiquetasDe(findings)).toContain('Ingresos');
    expect(etiquetasDe(findings)).not.toContain('Consenso de analistas');
  });

  it('un activo cubierto lo declara, para que no se le pregunte al modelo', async () => {
    const datos = await fundamentals.obtenerDatosDePlataforma(
      [crearHolding()], { language: 'es' }, fetchersQueResponden(), HOY,
    );

    expect(datos.get('MSFT').cubierto).toBe(true);
  });

  it('todos los activos vuelven en el mapa, también los que no trajeron nada', async () => {
    const fetchers = fetchersQueResponden({ fetchIncome: jest.fn().mockResolvedValue(null) });

    const datos = await fundamentals.obtenerDatosDePlataforma(
      [crearHolding(), crearHolding({ symbol: 'BTC-USD', asset_type: 'crypto' })],
      { language: 'es' }, fetchers, HOY,
    );

    expect([...datos.keys()]).toEqual(['MSFT', 'BTC-USD']);
  });
});

describe('no se satura a finance-query (medido el 14/10/2026)', () => {
  it('nunca hay más de ACTIVOS_EN_PARALELO activos en vuelo', async () => {
    // Cada activo son cuatro peticiones: siete de golpe eran 28 conexiones, y
    // de 28 volvían 18 con HTTP 503. Un 503 aquí no se ve — el activo se queda
    // sin cifras y cae a la web, que es lo que esta historia vino a evitar.
    let enVuelo = 0;
    let maximo = 0;
    const vigilado = async (valor) => {
      enVuelo += 1;
      maximo = Math.max(maximo, enVuelo);
      await new Promise((r) => setTimeout(r, 3));
      enVuelo -= 1;
      return valor;
    };

    const fetchers = {
      fetchIncome: jest.fn().mockImplementation(async (_s, f) =>
        vigilado(f === 'annual' ? crearAnual() : crearIncome())),
      fetchQuotes: jest.fn().mockResolvedValue([crearQuote()]),
      fetchAnalysis: jest.fn().mockImplementation(async () => vigilado(crearAnalisis())),
      fetchNews: jest.fn().mockImplementation(async () => vigilado(crearNoticias())),
    };

    const siete = ['A', 'B', 'C', 'D', 'E', 'F', 'G'].map((s) => crearHolding({ symbol: s }));
    await fundamentals.obtenerDatosDePlataforma(siete, { language: 'es' }, fetchers, HOY);

    // 2 activos × 4 peticiones = 8 conexiones, el orden del resto del proyecto.
    expect(maximo).toBeLessThanOrEqual(fundamentals.ACTIVOS_EN_PARALELO * 4);
  });

  it('los siete activos se consultan igual, sólo que por tandas', async () => {
    const fetchers = fetchersQueResponden();
    const siete = ['A', 'B', 'C', 'D', 'E', 'F', 'G'].map((s) => crearHolding({ symbol: s }));

    const datos = await fundamentals.obtenerDatosDePlataforma(
      siete, { language: 'es' }, fetchers, HOY,
    );

    expect([...datos.keys()]).toEqual(['A', 'B', 'C', 'D', 'E', 'F', 'G']);
    expect(fetchers.fetchAnalysis).toHaveBeenCalledTimes(7);
  });
});
