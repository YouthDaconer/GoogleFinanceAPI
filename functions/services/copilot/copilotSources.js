/**
 * HU 5.3 — Las fuentes de un análisis (RN-5-E).
 *
 * Dos trabajos:
 *
 * 1. **Resolver el redirect del *grounding*.** Los enlaces que devuelve la
 *    búsqueda nativa de Gemini son URL efímeras de
 *    `vertexaisearch.cloud.google.com/grounding-api-redirect/…`. En la prueba
 *    real de la HU 5.1, **15 de 63 ya no resolvían**; §9.1 lo llama "la deuda
 *    más concreta que deja esta prueba". Y la HU 5.4 guarda los análisis para
 *    compararlos: si se guarda el redirect, el análisis archivado queda con
 *    enlaces muertos. Se sigue una vez y se guarda el destino.
 *
 * 2. **Numerarlas y deduplicarlas**, para que el informe pueda citar `[1]` y
 *    `[2]` y la sección Fuentes sea una lista estable.
 *
 * @module services/copilot/copilotSources
 * @see platform-docs/stories/5.3-analista-investiga-recomienda/refinamiento.md (T8)
 */

/** Dominio de las redirecciones del grounding de Google. */
const GROUNDING_HOST = 'vertexaisearch.cloud.google.com';

/** Resolver un enlace es una comodidad, no una necesidad: se le da poco. */
const RESOLVE_TIMEOUT_MS = 5000;

/** Resoluciones simultáneas. Son peticiones triviales, pero son decenas. */
const RESOLVE_CONCURRENCY = 8;

/** Las redirecciones no cambian de destino: se cachean entre análisis. */
const cacheDestino = new Map();

/**
 * El dominio de una URL, sin `www.` y en minúsculas.
 *
 * El respaldo se comprueba por dominio y no por URL exacta porque el modelo
 * cita la página concreta que leyó y el *grounding* devuelve la que indexó: el
 * mismo medio con distinta ruta, distinto `utm_`, o la versión AMP. Exigir
 * igualdad exacta descartaría fuentes buenas.
 *
 * @param {string} url
 * @returns {string} Vacío si la URL no es legible.
 */
function dominio(url) {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return host.startsWith('www.') ? host.slice(4) : host;
  } catch (error) {
    return '';
  }
}

/**
 * Los dominios que el *grounding* visitó de verdad (RN-5-E).
 *
 * Es la lista contra la que se contrasta lo que el modelo dice haber leído.
 * Hay que resolver cada redirección para saberlo: el enlace que entrega el
 * *grounding* apunta a `vertexaisearch`, no al medio.
 *
 * @param {Array<{url: string}>} groundingSources
 * @returns {Promise<Set<string>>}
 */
async function dominiosRespaldados(groundingSources) {
  const resueltas = await enTandas(groundingSources || [], (fuente) => resolverDestino(fuente.url));

  const dominios = new Set();
  for (const resuelta of resueltas) {
    const d = dominio(resuelta.url);
    if (d) dominios.add(d);
  }

  return dominios;
}

/**
 * ¿Este enlace es una redirección efímera del grounding?
 *
 * @param {string} url
 * @returns {boolean}
 */
function esRedirectDeGrounding(url) {
  return typeof url === 'string' && url.includes(GROUNDING_HOST);
}

/**
 * Sigue la redirección y devuelve la URL final.
 *
 * Si no resuelve —caducó, no hay red, el destino responde mal— se conserva la
 * original: un enlace que quizá funcione es mejor que ninguno.
 *
 * Ya **no** decide si la fuente está verificada. Antes devolvía
 * `verificada: true` para cualquier URL que no fuera una redirección del
 * *grounding*, que son justamente las que escribe el modelo: se daban por
 * verificadas sin verificar nada. Eso ahora lo decide `dominiosRespaldados`.
 *
 * @param {string} url
 * @returns {Promise<{url: string}>}
 */
async function resolverDestino(url) {
  if (!esRedirectDeGrounding(url)) return { url };

  if (cacheDestino.has(url)) return cacheDestino.get(url);

  let resultado = { url };

  try {
    const response = await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: AbortSignal.timeout(RESOLVE_TIMEOUT_MS),
    });
    if (response.url && !esRedirectDeGrounding(response.url)) {
      resultado = { url: response.url };
    }
  } catch (error) {
    console.error(`[copilotSources] No se pudo resolver ${url}:`, error.message);
  }

  cacheDestino.set(url, resultado);
  return resultado;
}

/**
 * Ejecuta en tandas, para no abrir decenas de conexiones a la vez.
 *
 * @param {Array<*>} items
 * @param {function(*): Promise<*>} fn
 * @param {number} [limite]
 * @returns {Promise<Array<*>>}
 */
async function enTandas(items, fn, limite = RESOLVE_CONCURRENCY) {
  const salida = [];
  for (let i = 0; i < items.length; i += limite) {
    salida.push(...(await Promise.all(items.slice(i, i + limite).map(fn))));
  }
  return salida;
}

/**
 * Convierte las fuentes crudas en la lista numerada del informe.
 *
 * Deduplica por URL **ya resuelta**: dos redirecciones distintas del grounding
 * pueden llevar al mismo artículo, y el usuario no debería ver la misma fuente
 * dos veces con dos números.
 *
 * Cada fuente dice si está **verificada** (RN-5-E): si su dominio está entre
 * los que el *grounding* visitó. Lo que el modelo cite por su cuenta llega
 * marcado como no verificado, y el informe lo señala — ni se oculta ni se
 * presenta como comprobado.
 *
 * Una fuente puede llegar **ya verificada** (HU 5.6, RN-5.6-C). Es el caso de
 * lo que sirve la propia plataforma: preguntarle al *grounding* si visitó
 * `finance-query` no tiene sentido, porque su procedencia se conoce por
 * construcción. La marca "procedencia no verificada" existe para distinguir lo
 * que el modelo citó sin respaldo, y esto no lo citó el modelo.
 *
 * @param {Array<{title?: string, url: string, date?: string, verified?: boolean}>} crudas
 * @param {Set<string>} [respaldados] - Dominios que el grounding visitó.
 * @returns {Promise<Array<{n: number, title: string, url: string, date: string|null, verificada: boolean}>>}
 */
async function construirListaDeFuentes(crudas, respaldados = new Set()) {
  const resueltas = await enTandas(crudas, async (fuente) => {
    const destino = await resolverDestino(fuente.url);
    return {
      ...fuente,
      ...destino,
      verificada: fuente.verified === true || respaldados.has(dominio(destino.url)),
    };
  });

  const porUrl = new Map();
  for (const fuente of resueltas) {
    if (!fuente.url) continue;
    if (porUrl.has(fuente.url)) continue;
    porUrl.set(fuente.url, {
      n: porUrl.size + 1,
      title: fuente.title || fuente.url,
      url: fuente.url,
      date: fuente.date || null,
      verificada: fuente.verificada,
    });
  }

  return [...porUrl.values()];
}

/**
 * Índice de URL original → número de fuente, para que los hallazgos puedan
 * citar `[n]` después de que la lista se haya deduplicado y renumerado.
 *
 * @param {Array<{url: string}>} crudas - En el mismo orden con el que se
 *   construyó la lista.
 * @param {Array<{n: number, url: string}>} lista
 * @returns {Promise<Map<string, number>>}
 */
async function construirIndice(crudas, lista) {
  const porDestino = new Map(lista.map((f) => [f.url, f.n]));
  const indice = new Map();

  for (const fuente of crudas) {
    if (!fuente.url || indice.has(fuente.url)) continue;
    const { url } = await resolverDestino(fuente.url);
    const n = porDestino.get(url);
    if (n !== undefined) indice.set(fuente.url, n);
  }

  return indice;
}

module.exports = {
  GROUNDING_HOST,
  RESOLVE_TIMEOUT_MS,
  dominio,
  dominiosRespaldados,
  esRedirectDeGrounding,
  resolverDestino,
  construirListaDeFuentes,
  construirIndice,
  _cacheDestino: cacheDestino,
};
