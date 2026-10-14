/**
 * HU 5.3 — Las fuentes del análisis (RN-5-E, Escenario 11).
 *
 * Lo que de verdad defiende este archivo es la deuda que dejó la prueba de la
 * HU 5.1 (§9.1): los enlaces del *grounding* de Gemini son redirecciones
 * efímeras y **15 de 63 ya no resolvían**. La HU 5.4 guarda los análisis para
 * compararlos: si se guarda el redirect, el análisis archivado queda con
 * enlaces muertos.
 *
 * @see services/copilot/copilotSources.js
 */

const fetchOriginal = global.fetch;

const {
  esRedirectDeGrounding,
  resolverDestino,
  dominio,
  dominiosRespaldados,
  construirListaDeFuentes,
  construirIndice,
  _cacheDestino,
} = require('../copilotSources');

const REDIRECT = 'https://vertexaisearch.cloud.google.com/grounding-api-redirect/abc123';
const DESTINO = 'https://www.reuters.com/business/microsoft-q3';

/**
 * Un `fetch` de mentira que resuelve cada redirección a su destino.
 *
 * @param {Object<string,string>} mapa
 * @param {Set<string>} [rotos] - Los que fallan.
 */
function montarFetch(mapa, rotos = new Set()) {
  global.fetch = jest.fn(async (url) => {
    if (rotos.has(url)) throw new Error('404');
    return { url: mapa[url] || url };
  });
}

beforeEach(() => {
  _cacheDestino.clear();
  jest.clearAllMocks();
});

afterAll(() => {
  global.fetch = fetchOriginal;
});

describe('esRedirectDeGrounding', () => {
  it('reconoce la redirección efímera de Google', () => {
    expect(esRedirectDeGrounding(REDIRECT)).toBe(true);
  });

  it('un enlace normal no lo es', () => {
    expect(esRedirectDeGrounding(DESTINO)).toBe(false);
    expect(esRedirectDeGrounding(null)).toBe(false);
  });
});

describe('resolverDestino', () => {
  it('sigue la redirección y devuelve el destino', async () => {
    montarFetch({ [REDIRECT]: DESTINO });

    expect(await resolverDestino(REDIRECT)).toEqual({ url: DESTINO });
  });

  it('un enlace que no es del grounding no se toca', async () => {
    montarFetch({});

    expect(await resolverDestino(DESTINO)).toEqual({ url: DESTINO });
    expect(global.fetch).not.toHaveBeenCalled();
  });

  it('si no resuelve, conserva el original', async () => {
    montarFetch({}, new Set([REDIRECT]));

    // Un enlace que quizá funcione es mejor que ninguno.
    expect(await resolverDestino(REDIRECT)).toEqual({ url: REDIRECT });
  });

  it('ya no decide si la fuente está verificada', async () => {
    // Devolvía `verificada: true` para cualquier URL que no fuera una
    // redirección del grounding — es decir, para todas las que escribe el
    // modelo. Las daba por verificadas sin comprobar nada (RN-5-E).
    montarFetch({});

    expect(await resolverDestino(DESTINO)).not.toHaveProperty('verificada');
  });

  it('cachea el destino: la misma redirección no se sigue dos veces', async () => {
    montarFetch({ [REDIRECT]: DESTINO });

    await resolverDestino(REDIRECT);
    await resolverDestino(REDIRECT);

    expect(global.fetch).toHaveBeenCalledTimes(1);
  });
});

describe('construirListaDeFuentes', () => {
  it('numera las fuentes desde 1, en orden de aparición', async () => {
    montarFetch({});

    const lista = await construirListaDeFuentes([
      { title: 'Reuters', url: 'https://a' },
      { title: 'Bloomberg', url: 'https://b' },
    ]);

    expect(lista.map((f) => [f.n, f.title])).toEqual([[1, 'Reuters'], [2, 'Bloomberg']]);
  });

  it('dos redirecciones al mismo artículo son UNA fuente', async () => {
    const otroRedirect = `${REDIRECT}-bis`;
    montarFetch({ [REDIRECT]: DESTINO, [otroRedirect]: DESTINO });

    const lista = await construirListaDeFuentes([
      { title: 'Reuters', url: REDIRECT },
      { title: 'Reuters', url: otroRedirect },
    ]);

    // El usuario no debería ver la misma fuente dos veces con dos números.
    expect(lista).toHaveLength(1);
    expect(lista[0].url).toBe(DESTINO);
  });

  it('guarda el destino, no el redirect (HU 5.1 §9.1)', async () => {
    montarFetch({ [REDIRECT]: DESTINO });

    const [fuente] = await construirListaDeFuentes(
      [{ title: 'Reuters', url: REDIRECT }], new Set([dominio(DESTINO)]),
    );

    expect(fuente.url).toBe(DESTINO);
    expect(fuente.verificada).toBe(true);
  });

  it('conserva la fecha de publicación cuando la hay', async () => {
    montarFetch({});

    const [fuente] = await construirListaDeFuentes([
      { title: 'Reuters', url: 'https://a', date: '2026-10-01' },
    ]);

    expect(fuente.date).toBe('2026-10-01');
  });

  it('ignora las entradas sin enlace', async () => {
    montarFetch({});

    expect(await construirListaDeFuentes([{ title: 'Sin enlace', url: '' }])).toEqual([]);
  });
});

describe('construirIndice — para que el informe pueda citar [n]', () => {
  it('lleva de la URL original al número de la fuente ya deduplicada', async () => {
    montarFetch({ [REDIRECT]: DESTINO });

    const crudas = [{ title: 'Reuters', url: REDIRECT }, { title: 'Bloomberg', url: 'https://b' }];
    const lista = await construirListaDeFuentes(crudas);
    const indice = await construirIndice(crudas, lista);

    expect(indice.get(REDIRECT)).toBe(1);
    expect(indice.get('https://b')).toBe(2);
  });
});

describe('RN-5-E — una fuente verificada es la que el grounding visitó', () => {
  it('lo que el grounding respalda, va marcado como verificado', async () => {
    montarFetch({});

    const [fuente] = await construirListaDeFuentes(
      [{ title: 'Morningstar', url: 'https://www.morningstar.com/stocks/msft' }],
      new Set(['morningstar.com']),
    );

    expect(fuente.verificada).toBe(true);
  });

  it('lo que sólo cita el modelo, NO', async () => {
    // Medido: el 45 % de los hallazgos citan dominios que el grounding nunca
    // visitó, y entre ellos salen reuters.com o nasdaq.com. Son indistinguibles
    // de uno inventado, y presentarlos como comprobados es lo que RN-5-E
    // prohíbe.
    montarFetch({});

    const [fuente] = await construirListaDeFuentes(
      [{ title: 'Reuters', url: 'https://www.reuters.com/markets/x' }],
      new Set(['morningstar.com']),
    );

    expect(fuente.verificada).toBe(false);
  });

  it('sin respaldo ninguno, ninguna fuente se da por verificada', async () => {
    montarFetch({});

    const lista = await construirListaDeFuentes(
      [{ title: 'A', url: 'https://a.com/1' }, { title: 'B', url: 'https://b.com/2' }],
    );

    expect(lista.every((f) => f.verificada === false)).toBe(true);
  });

  it('se compara por dominio, no por URL exacta', async () => {
    // El modelo cita la página que leyó y el grounding indexó otra ruta del
    // mismo medio; exigir igualdad exacta descartaría fuentes buenas.
    montarFetch({});

    const [fuente] = await construirListaDeFuentes(
      [{ title: 'CNBC', url: 'https://www.cnbc.com/2026/10/13/msft.html?utm_source=x' }],
      new Set(['cnbc.com']),
    );

    expect(fuente.verificada).toBe(true);
  });

  it('los dominios respaldados salen de resolver las redirecciones', async () => {
    // El enlace del grounding apunta a vertexaisearch, no al medio: sin
    // resolverlo no se sabe contra qué contrastar.
    montarFetch({ [REDIRECT]: DESTINO });

    const dominios = await dominiosRespaldados([{ url: REDIRECT }]);

    expect([...dominios]).toEqual([dominio(DESTINO)]);
  });

  it('una URL ilegible no revienta ni se cuela como respaldada', async () => {
    montarFetch({});

    expect(dominio('no-es-una-url')).toBe('');
    const dominios = await dominiosRespaldados([{ url: 'no-es-una-url' }]);
    expect(dominios.has('')).toBe(false);
  });
});

describe('HU 5.6 — una fuente puede llegar verificada por construcción (RN-5.6-C)', () => {
  const PLATAFORMA = 'https://api.portastock.net';

  it('lo que sirve la plataforma no se le pregunta al grounding', async () => {
    montarFetch({});

    const lista = await construirListaDeFuentes(
      [{ title: 'Portastock', url: PLATAFORMA, date: null, verified: true }],
      new Set(['reuters.com']),
    );

    expect(lista[0].verificada).toBe(true);
  });

  it('lo que el modelo cita por su cuenta sigue sin verificar', async () => {
    montarFetch({});

    const lista = await construirListaDeFuentes(
      [{ title: 'Inventado', url: 'https://medio-raro.test/x', date: null }],
      new Set(['reuters.com']),
    );

    expect(lista[0].verificada).toBe(false);
  });

  it('las dos conviven en la misma lista, cada una con su marca', async () => {
    montarFetch({ [REDIRECT]: DESTINO });

    const lista = await construirListaDeFuentes(
      [
        { title: 'Portastock', url: PLATAFORMA, date: null, verified: true },
        { title: 'Reuters', url: REDIRECT, date: null },
        { title: 'Inventado', url: 'https://medio-raro.test/x', date: null },
      ],
      new Set(['reuters.com']),
    );

    expect(lista.map((f) => f.verificada)).toEqual([true, true, false]);
  });

  it('un titular de la plataforma conserva el enlace real del medio', async () => {
    // Viene de `/v1/news`, así que su procedencia se conoce, pero la URL que se
    // publica es la del artículo: es la que el lector puede abrir.
    montarFetch({});

    const lista = await construirListaDeFuentes(
      [{ title: 'Reuters', url: 'https://reuters.com/articulo', date: null, verified: true }],
      new Set(),
    );

    expect(lista[0].url).toBe('https://reuters.com/articulo');
    expect(lista[0].verificada).toBe(true);
  });
});
