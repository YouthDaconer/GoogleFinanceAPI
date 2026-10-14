/**
 * HU 5.3 — La redacción final.
 *
 * Recibe lo investigado **ya filtrado** por `copilotFindings` y lo convierte en
 * las seis secciones del Escenario 14. No busca en internet: separar investigar
 * de redactar es lo que impide que la síntesis introduzca una cifra o un enlace
 * que ninguna etapa de investigación obtuvo.
 *
 * El resumen ejecutivo se devuelve además **aparte**, porque la tarjeta del
 * historial tiene que mostrarlo (Escenario 14) y hoy recorta los primeros 150
 * caracteres del markdown.
 *
 * @module services/copilot/copilotComposer
 * @see platform-docs/stories/5.3-analista-investiga-recomienda/refinamiento.md (T12)
 */

const engineClient = require('./copilotEngineClient');
const sources = require('./copilotSources');
const { RESEARCH_OUTCOME, parsearRespuesta } = require('./copilotFindings');

/**
 * Encabezados con los que arranca el resumen ejecutivo, en los dos idiomas. Se
 * usan para recortarlo del markdown sin volver a preguntarle nada al modelo.
 */
const ENCABEZADOS_RESUMEN = ['## Resumen ejecutivo', '## Executive summary'];

/** HU 5.4 - Los estados que la seccion 2 puede dejarle a una recomendacion anterior. */
const ESTADOS_DE_RECOMENDACION = ['vigente', 'ajustada', 'no_vigente'];

/** Y a una tesis anterior. */
const ESTADOS_DE_TESIS = ['confirmada', 'invalidada', 'sin_cambio'];

/** Lo que se devuelve cuando no hay seccion 2 que leer. */
const SEGUIMIENTO_VACIO = { recommendations: [], theses: [] };

/**
 * El resumen ejecutivo, recortado del informe.
 *
 * @param {string} markdown
 * @returns {string} Vacío si el informe no trae la sección — el markdown
 *   completo sigue siendo válido, sólo que la tarjeta caerá a su recorte.
 */
function extraerResumenEjecutivo(markdown) {
  if (typeof markdown !== 'string') return '';

  for (const encabezado of ENCABEZADOS_RESUMEN) {
    const inicio = markdown.indexOf(encabezado);
    if (inicio === -1) continue;

    const desde = inicio + encabezado.length;
    const siguiente = markdown.indexOf('\n## ', desde);
    return markdown.slice(desde, siguiente === -1 ? undefined : siguiente).trim();
  }

  return '';
}

/**
 * Arma lo que ve la redacción: sólo hechos verificados y fuentes numeradas.
 *
 * Cada hallazgo lleva el número de su fuente ya resuelto, para que el informe
 * pueda citar `[3]` y la sección Fuentes coincida.
 *
 * @param {object} entrada
 * @param {Array<{ticker: string, outcome: string, findings: object[], notes: string[], conflicts: object[], groundingSources?: object[], motivo?: string, variacionSemanal?: number, name?: string}>} entrada.investigaciones
 * @param {object|null} entrada.macro
 * @param {object|null} entrada.resto
 * @param {Array<{ticker: string, motivo: string}>} entrada.excluidos
 * @param {object} entrada.portafolio
 * @param {object|null} [entrada.comparacion] - HU 5.4: el análisis anterior, su
 *   estado y sus recomendaciones ya cerradas de entrada
 * @returns {Promise<{context: object, fuentes: object[]}>}
 */
async function construirContexto({
  investigaciones, macro, resto, excluidos, portafolio, comparacion = null,
}) {
  const todas = [...investigaciones, ...(macro ? [macro] : [])];

  const crudas = [];
  for (const inv of todas) {
    for (const f of inv.findings) {
      crudas.push({
        title: f.sourceTitle || '',
        url: f.sourceUrl,
        date: f.publishedAt,
        // HU 5.6 (RN-5.6-C): un dato de la plataforma llega con su procedencia
        // conocida. Sin arrastrar la marca hasta aquí, la lista de fuentes
        // volvería a preguntarle al *grounding* por algo que no buscó, y la
        // cifra buena saldría en el informe como "procedencia no verificada".
        verified: f.verified === true,
      });
    }
    for (const c of inv.conflicts) {
      for (const v of c.versions) crudas.push({ title: '', url: v.sourceUrl, date: null });
    }
  }

  // RN-5-E: los dominios que el *grounding* visitó de verdad. Contra esto se
  // contrasta lo que el modelo dice haber leído. Hasta ahora `groundingSources`
  // se guardaba en cada investigación y **no lo leía nadie**: las fuentes del
  // informe salían de los `sourceUrl` que el propio modelo escribía, que es
  // justo lo que la regla quiere impedir.
  const grounding = [];
  for (const inv of todas) {
    for (const fuente of inv.groundingSources || []) grounding.push(fuente);
  }
  const respaldados = await sources.dominiosRespaldados(grounding);

  const fuentes = await sources.construirListaDeFuentes(crudas, respaldados);
  const indice = await sources.construirIndice(crudas, fuentes);

  const conCita = (inv) => ({
    ticker: inv.ticker,
    name: inv.name || inv.ticker,
    // Cuánto pesa hoy en la cartera. El redactor lo necesita para que una
    // recomendación de rebalanceo tenga un punto de partida: sin esto decía
    // "reduce un 2%" sin saber de cuánto.
    weightPct: typeof inv.weight === 'number' ? Math.round(inv.weight * 1000) / 10 : null,
    // El motivo sólo se declara cuando no es el peso (RN-5.3-N): un activo
    // relevante por tamaño no necesita explicación.
    includedBecause: inv.motivo === 'variacion'
      ? { reason: 'variacion', weeklyChangePct: inv.variacionSemanal }
      : null,
    researched: inv.outcome !== RESEARCH_OUTCOME.FALLO,
    findings: inv.findings.map((f) => ({
      label: f.label,
      value: f.value,
      period: f.period,
      publishedAt: f.publishedAt,
      source: indice.get(f.sourceUrl) || null,
    })),
    notes: inv.notes,
    conflicts: inv.conflicts.map((c) => ({
      about: c.about,
      versions: c.versions.map((v) => ({ value: v.value, source: indice.get(v.sourceUrl) || null })),
    })),
  });

  return {
    context: {
      portfolio: portafolio,
      excluded: excluidos,
      assets: investigaciones.map(conCita),
      rest: resto,
      macro: macro ? conCita(macro) : null,
      // HU 5.4: la sección 2 la escribe este mismo composer, en la misma
      // llamada que la 4. Si se generaran por separado podrían contradecirse
      // (Escenario 4).
      comparison: comparacion,
      // `verified` viaja al prompt: el informe marca lo que no pudo comprobarse
      // en vez de presentarlo como verificado (RN-5-E).
      sources: fuentes.map((f) => ({
        n: f.n, title: f.title, url: f.url, date: f.date, verified: f.verificada,
      })),
    },
    fuentes,
  };
}

/**
 * HU 5.4 — Extrae del informe ya escrito sus recomendaciones y sus tesis.
 *
 * Es lo que hace comparable un análisis: sin esto, el siguiente tendría que
 * acordarse de lo que dijo el anterior leyendo su prosa, y el Escenario 2
 * ("las N, ninguna omitida, con su enunciado original") no sería verificable.
 *
 * **Nunca lanza.** Un fallo aquí no puede llevarse por delante un informe que
 * ya está escrito: se guarda sin estructura, y el siguiente análisis lo tratará
 * como formato antiguo — que es exactamente lo que es, uno sin recomendaciones
 * comparables.
 *
 * Devuelve dos cosas que no hay que confundir: `recommendations`/`theses` son
 * las de **este** informe, lo que hará comparable al siguiente; `followUp` son
 * las del **anterior**, con el estado en que la sección 2 las dejó. Sin esto
 * último, el estado final se quedaría escrito sólo dentro del markdown y el
 * recuento de la tarjeta saldría siempre en ceros (Escenario 15).
 *
 * @param {string} markdown - El informe ya redactado.
 * @param {object} entorno - `{ userId, secret, language }`.
 * @returns {Promise<{recommendations: object[], theses: object[], followUp: object, tokensUsed: number}>}
 */
async function extraerEstructura(markdown, { userId, secret, language }) {
  const vacio = {
    recommendations: [], theses: [], followUp: { ...SEGUIMIENTO_VACIO }, tokensUsed: 0,
  };

  try {
    const salida = await engineClient.redactar(userId, secret, {
      task: 'extract',
      language,
      markdown,
    });

    const datos = parsearRespuesta(salida.text);
    if (!datos) {
      console.error('[copilotComposer] La extracción no devolvió JSON legible');
      return vacio;
    }

    return {
      recommendations: normalizarRecomendaciones(datos.recommendations),
      theses: normalizarTesis(datos.theses),
      followUp: normalizarSeguimiento(datos.followUp),
      tokensUsed: salida.tokensUsed,
    };
  } catch (error) {
    console.error('[copilotComposer] No se pudo extraer la estructura:', error.message);
    return vacio;
  }
}

/**
 * Deja cada recomendación con la forma que la HU 5.4 va a comparar.
 *
 * Lo que el modelo no entregue queda a `null`, nunca deducido: una zona de
 * precio inventada convertiría un "sigue vigente" en una afirmación falsa.
 *
 * @param {Array<object>} crudas
 * @returns {object[]}
 */
function normalizarRecomendaciones(crudas) {
  const salida = [];

  for (const rec of crudas || []) {
    if (!rec || typeof rec !== 'object') continue;
    const statement = String(rec.statement || '').trim();
    if (!statement) continue;

    salida.push({
      statement,
      asset: rec.asset || null,
      type: rec.type === 'rebalanceo_riesgo' ? 'rebalanceo_riesgo' : 'accion_concreta',
      conviction: ['alta', 'media', 'baja'].includes(rec.conviction) ? rec.conviction : null,
      targetPct: typeof rec.targetPct === 'number' && Number.isFinite(rec.targetPct)
        ? rec.targetPct : null,
      priceZone: rec.priceZone || null,
      horizon: rec.horizon || null,
      isNewAsset: rec.isNewAsset === true,
    });
  }

  return salida;
}

/**
 * HU 5.4 — Los estados en que la sección 2 dejó al análisis anterior.
 *
 * Es la única vía por la que ese estado sale del markdown y vuelve a ser un
 * dato. Lo que no traiga un estado de la lista se descarta en vez de contarse
 * como vigente: un recuento inventado es peor que uno corto, porque la tarjeta
 * lo presenta como un hecho.
 *
 * @param {object} crudo - El `followUp` tal como lo devolvió el modelo.
 * @returns {{recommendations: Array<{statement: string, asset: string|null, state: string}>,
 *   theses: Array<{statement: string, asset: string|null, state: string}>}}
 */
function normalizarSeguimiento(crudo) {
  const limpiar = (lista, permitidos) => {
    const salida = [];

    for (const item of Array.isArray(lista) ? lista : []) {
      if (!item || typeof item !== 'object') continue;
      const statement = String(item.statement || '').trim();
      if (!statement || !permitidos.includes(item.state)) continue;
      salida.push({ statement, asset: item.asset || null, state: item.state });
    }

    return salida;
  };

  if (!crudo || typeof crudo !== 'object') return { ...SEGUIMIENTO_VACIO };

  return {
    recommendations: limpiar(crudo.recommendations, ESTADOS_DE_RECOMENDACION),
    theses: limpiar(crudo.theses, ESTADOS_DE_TESIS),
  };
}

/**
 * Las tesis, con su enunciado literal.
 *
 * @param {Array<object>} crudas
 * @returns {object[]}
 */
function normalizarTesis(crudas) {
  const salida = [];

  for (const tesis of crudas || []) {
    if (!tesis || typeof tesis !== 'object') continue;
    const statement = String(tesis.statement || '').trim();
    if (!statement) continue;
    salida.push({ statement, asset: tesis.asset || null });
  }

  return salida;
}

/**
 * Escribe el informe.
 *
 * @param {object} entrada - Lo mismo que `construirContexto`.
 * @param {object} entorno - `{ userId, secret, language }`.
 * @returns {Promise<{markdown: string, executiveSummary: string, tokensUsed: number, modelId: string, sources: object[]}>}
 * @throws {CopilotEngineError} Si el motor no responde: sin informe no hay
 *   análisis, y eso sí es un fallo (RN-5-C).
 */
async function redactarInforme(entrada, { userId, secret, language }) {
  const { context, fuentes } = await construirContexto(entrada);

  const salida = await engineClient.redactar(userId, secret, { language, context });

  // HU 5.4: con el informe escrito, se extrae su estructura para que el
  // análisis siguiente pueda compararse contra él.
  const estructura = await extraerEstructura(salida.text, { userId, secret, language });

  return {
    markdown: salida.text,
    executiveSummary: extraerResumenEjecutivo(salida.text),
    tokensUsed: salida.tokensUsed + estructura.tokensUsed,
    modelId: salida.modelId,
    sources: fuentes,
    recommendations: estructura.recommendations,
    theses: estructura.theses,
    // Los estados que la sección 2 le dio al análisis anterior: de aquí sale
    // el recuento de la tarjeta del historial.
    followUp: estructura.followUp,
  };
}

module.exports = {
  ENCABEZADOS_RESUMEN,
  ESTADOS_DE_RECOMENDACION,
  ESTADOS_DE_TESIS,
  SEGUIMIENTO_VACIO,
  extraerResumenEjecutivo,
  extraerEstructura,
  normalizarRecomendaciones,
  normalizarTesis,
  normalizarSeguimiento,
  construirContexto,
  redactarInforme,
};
