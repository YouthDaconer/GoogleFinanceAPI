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
 * HU 5.7.3: antes de redactar, el modelo **decide** (una llamada que devuelve
 * JSON), el código corrige esa decisión y calcula los estados del seguimiento,
 * y solo entonces se redacta, con todo ya decidido. Así el informe y lo que se
 * guarda coinciden por construcción, y lo que se puede comprobar con datos no
 * depende de que el modelo lo obedezca (RN-5.7-P a RN-5.7-T).
 *
 * @module services/copilot/copilotComposer
 * @see platform-docs/stories/5.3-analista-investiga-recomienda/refinamiento.md (T12)
 * @see platform-docs/stories/5.7.3-seguimiento-con-precio-y-criterio/refinamiento.md (T6)
 */

const { DateTime } = require('luxon');
const engineClient = require('./copilotEngineClient');
const sources = require('./copilotSources');
const { RESEARCH_OUTCOME, parsearRespuesta } = require('./copilotFindings');
const { FUENTE_PLATAFORMA } = require('./copilotFundamentals');
const { idioma } = require('./copilotAssetCardService');
const { accionesPermitidas } = require('./copilotDecisionGuard');

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

/** HU 5.7.3 — Por qué camino se llegó a las recomendaciones guardadas. */
const RUTA_DE_DECISION = {
  /** El modelo decidió, el código corrigió y el informe se redactó con eso. */
  DECIDIDA: 'decidida',
  /**
   * La decisión no llegó legible: se redactó como antes de la 5.7.3 y se
   * corrigió lo extraído. El informe puede no reflejar las correcciones.
   */
  DEGRADADA: 'degradada',
};

/** HU 5.7.1 (RN-5.7-K) — El pilar que decide una recomendación. */
const PILARES = ['fundamentales', 'valoracion', 'tecnico', 'noticias'];

/** HU 5.7.1 (RN-5.7-K) — Las acciones posibles de una recomendación. */
const ACCIONES = ['vender', 'reducir', 'mantener', 'aumentar', 'comprar'];

/** Las únicas acciones que llevan nivel de la jerarquía (RN-5.7-D). */
const ACCIONES_CON_NIVEL = ['vender', 'reducir'];

/** Los niveles de la jerarquía de RN-5.7-D. */
const NIVELES = [1, 2, 3];

/** RN-5.7-G — Una compra por tramos se reparte en 2 o 3. */
const TRAMOS_MIN = 2;
const TRAMOS_MAX = 3;

/** Encabezados de la sección de fuentes, la única que lleva la marca de procedencia. */
const ENCABEZADOS_FUENTES = ['\n## Fuentes', '\n## Sources'];

/** La marca que el prompt pone a una fuente `verified: false`, en los dos idiomas. */
const MARCA_NO_VERIFICADA = /\s*—\s*(?:procedencia no verificada|unverified provenance)/g;

/**
 * Quita la marca de procedencia del cuerpo del informe y la deja en Fuentes.
 *
 * La regla 13 del prompt lo pide y el modelo no lo cumple: el 2026-11-20 un
 * informe la escribió 33 veces en el cuerpo ("[4 — procedencia no
 * verificada]"). Lo que se puede hacer en código no se le pide al modelo.
 *
 * @param {string} markdown
 * @returns {string}
 */
function limpiarMarcasDelCuerpo(markdown) {
  if (typeof markdown !== 'string') return markdown;
  const corte = ENCABEZADOS_FUENTES
    .map((e) => markdown.indexOf(e))
    .filter((i) => i !== -1)
    .reduce((min, i) => Math.min(min, i), markdown.length);
  return markdown.slice(0, corte).replace(MARCA_NO_VERIFICADA, '') + markdown.slice(corte);
}

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
 * @param {string} [entrada.language] - HU 5.7.1: el idioma del título de la
 *   fuente de la plataforma que respalda la ficha.
 * @param {Date} [entrada.now] - HU 5.7.1: el "hoy" que se le dice al redactor.
 * @returns {Promise<{context: object, fuentes: object[]}>}
 */
async function construirContexto({
  investigaciones, macro, resto, excluidos, portafolio, comparacion = null,
  language = 'es', now = new Date(),
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
    // HU 5.7.1: la ficha es un dato de la plataforma (RN-5.6-C): cita la misma
    // fuente, verificada por construcción. Un ETF o una cripto sin hallazgos de
    // la plataforma la estrenan aquí.
    if (inv.card) {
      crudas.push({
        title: FUENTE_PLATAFORMA.titulo[idioma(language)],
        url: FUENTE_PLATAFORMA.url,
        date: inv.card.asOf,
        verified: true,
      });
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
  const verificadaPorNumero = new Map(fuentes.map((f) => [f.n, f.verificada === true]));

  // HU 5.7.3 (RN-5.7-T): el Nivel 1 es un hecho que toca la tesis con una
  // fuente que el sistema pudo comprobar. No está en la ficha porque depende de
  // lo investigado; se decide aquí, con la lista de fuentes ya verificada.
  const nivelUnoPosible = (inv) => inv.findings.some((f) => f.impact === 'tesis'
    && verificadaPorNumero.get(indice.get(f.sourceUrl)) === true);

  const conAcciones = (card, level1Eligible) => {
    if (!card.signals) return card.signals;
    const permitidas = accionesPermitidas(card.signals, { clase: card.kind, level1Eligible });
    return {
      ...card.signals,
      level1Eligible,
      allowedActions: permitidas.allowed,
      holdMaxConviction: permitidas.holdMaxConviction,
    };
  };

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
      // HU 5.7.1 (RN-5.7-I): si el hecho toca la tesis o es ruido.
      impact: f.impact || null,
    })),
    // HU 5.7.1: la ficha de cuatro pilares, con las señales ya evaluadas.
    // HU 5.7.3: y las acciones que permite, ya con el Nivel 1.
    card: inv.card
      ? {
        ...inv.card,
        source: indice.get(FUENTE_PLATAFORMA.url) || null,
        signals: conAcciones(inv.card, nivelUnoPosible(inv)),
      }
      : null,
    notes: inv.notes,
    conflicts: inv.conflicts.map((c) => ({
      about: c.about,
      versions: c.versions.map((v) => ({ value: v.value, source: indice.get(v.sourceUrl) || null })),
    })),
  });

  return {
    context: {
      // HU 5.7.1 (RN-5.7-J): la ventana de 30 días se mide desde aquí.
      today: DateTime.fromJSDate(now, { zone: 'America/New_York' }).toISODate(),
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
 * HU 5.7.3 — Las decisiones de hoy, antes de redactar.
 *
 * Recibe el mismo contexto que la redacción y devuelve JSON: las
 * recomendaciones de hoy con sus parámetros, sus tesis y el estado que el
 * modelo da a las anteriores (por `id`). Sustituye a la extracción: lo que se
 * guarda ya no se lee de la prosa.
 *
 * **Nunca lanza.** Si el motor falla o no devuelve un JSON con recomendaciones,
 * devuelve `null` y el informe sigue por la ruta degradada. Si el motor está
 * caído de verdad, la redacción lo dirá al fallar, como hasta ahora (RN-5-C).
 *
 * @param {object} context - El de `construirContexto`.
 * @param {object} entorno - `{ userId, secret, language }`.
 * @returns {Promise<{recommendations: object[], theses: object[], followUp: object,
 *   tokensUsed: number}|null>}
 */
async function decidir(context, { userId, secret, language }) {
  try {
    const salida = await engineClient.redactar(userId, secret, { task: 'decide', language, context });
    const decision = normalizarDecision(parsearRespuesta(salida.text));
    if (!decision) {
      console.error('[copilotComposer] Decisión no legible: ruta degradada');
      return null;
    }
    return { ...decision, tokensUsed: salida.tokensUsed };
  } catch (error) {
    console.error('[copilotComposer] No se pudo decidir, ruta degradada:', error.message);
    return null;
  }
}

/**
 * La decisión del modelo con la forma que se compara y se guarda.
 *
 * @param {*} datos - El JSON tal como llegó.
 * @returns {{recommendations: object[], theses: object[], followUp: object}|null}
 *   `null` si no trae una lista de recomendaciones: sin ella no hay decisión.
 */
function normalizarDecision(datos) {
  if (!datos || typeof datos !== 'object' || !Array.isArray(datos.recommendations)) return null;

  return {
    recommendations: normalizarRecomendaciones(datos.recommendations),
    theses: normalizarTesis(datos.theses),
    followUp: normalizarSeguimiento(datos.followUp),
  };
}

/**
 * HU 5.7.1 — Los tramos de una compra por tramos (RN-5.7-G).
 *
 * Valen de 2 a 3, cada uno con su zona de precio y un porcentaje positivo. Si
 * alguno no cuadra, o son uno o más de tres, no hay tramos: `[]`. No se arreglan
 * a medias, porque un reparto incompleto se leería como el reparto completo.
 *
 * @param {*} crudos
 * @returns {Array<{zone: string, pct: number}>}
 */
function normalizarTramos(crudos) {
  if (!Array.isArray(crudos) || crudos.length < TRAMOS_MIN || crudos.length > TRAMOS_MAX) return [];

  const tramos = [];
  for (const tramo of crudos) {
    if (!tramo || typeof tramo !== 'object') return [];
    const zone = String(tramo.zone || '').trim();
    const { pct } = tramo;
    if (!zone || typeof pct !== 'number' || !Number.isFinite(pct) || pct <= 0) return [];
    tramos.push({ zone, pct });
  }

  return tramos;
}

/**
 * HU 5.7.1 — Lo que el criterio de decisión añade a una recomendación.
 *
 * @param {object} rec - La recomendación tal como la extrajo el modelo.
 * @returns {{action: string|null, pillar: string|null, level: number|null,
 *   tranches: Array<{zone: string, pct: number}>}}
 */
function camposDelCriterio(rec) {
  const action = ACCIONES.includes(rec.action) ? rec.action : null;
  // Sin acción legible no se puede decir que el nivel sobre: se conserva si es
  // válido. Con una acción que no reduce ni vende, el nivel no existe.
  const admiteNivel = action === null || ACCIONES_CON_NIVEL.includes(action);

  return {
    action,
    pillar: PILARES.includes(rec.pillar) ? rec.pillar : null,
    level: admiteNivel && NIVELES.includes(rec.level) ? rec.level : null,
    tranches: normalizarTramos(rec.tranches),
  };
}

/**
 * Deja cada recomendación con la forma que la HU 5.4 va a comparar.
 *
 * Lo que el modelo no entregue queda a `null`, nunca deducido: una zona de
 * precio inventada convertiría un "sigue vigente" en una afirmación falsa.
 *
 * HU 5.7.1 (RN-5.7-K, QA-22): `action`, `pillar`, `level` y `tranches`. El
 * nivel sólo existe en lo que reduce o vende: en mantener, aumentar o comprar
 * queda `null` aunque el modelo lo escriba.
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
      ...camposDelCriterio(rec),
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
 * HU 5.7.3: cada estado puede venir con el `id` del elemento anterior (la
 * decisión los devuelve así); entonces el enunciado sobra, porque se empareja
 * por `id` (RN-5.7-S).
 *
 * @param {object} crudo - El `followUp` tal como lo devolvió el modelo.
 * @returns {{recommendations: Array<{id: string|null, statement: string, asset: string|null, state: string}>,
 *   theses: Array<{id: string|null, statement: string, asset: string|null, state: string}>}}
 */
function normalizarSeguimiento(crudo) {
  const limpiar = (lista, permitidos) => {
    const salida = [];

    for (const item of Array.isArray(lista) ? lista : []) {
      if (!item || typeof item !== 'object') continue;
      const statement = String(item.statement || '').trim();
      const id = item.id !== undefined && item.id !== null && String(item.id).trim()
        ? String(item.id).trim()
        : null;
      if ((!statement && !id) || !permitidos.includes(item.state)) continue;
      salida.push({
        id, statement, asset: item.asset || null, state: item.state,
      });
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
 * Lo que hace el seguimiento cuando no se le pasa nada: deja la decisión tal
 * cual. Lo usan los llamadores que no comparan (y los tests del composer).
 */
const SEGUIMIENTO_SIN_CAMBIOS = {
  antesDeDecidir: (context) => context,
  alDecidir: (decision, context) => ({
    recommendations: decision.recommendations,
    theses: decision.theses,
    comparison: context.comparison || null,
    followUp: decision.followUp,
    corrections: [],
  }),
};

/**
 * Escribe el informe.
 *
 * HU 5.7.3 — Decidir → corregir → redactar:
 * 1. `seguimiento.antesDeDecidir(context)`: el llamador cierra lo que ya se
 *    sabe sin el modelo (las reducciones que la ficha no permite, RN-5.7-P).
 * 2. `decidir`: el modelo decide en JSON.
 * 3. `seguimiento.alDecidir(decision, context)`: el llamador corrige las
 *    recomendaciones y calcula los estados de las anteriores.
 * 4. Se redacta con `context.decisions` y la comparación ya resuelta.
 *
 * Si la decisión no llega legible (ruta degradada), se redacta como antes, se
 * extrae la estructura del informe y `alDecidir` corrige lo extraído: lo
 * guardado cumple las reglas aunque el informe no las refleje.
 *
 * @param {object} entrada - Lo mismo que `construirContexto`.
 * @param {object} entorno - `{ userId, secret, language }`.
 * @param {object} [seguimiento] - `{ antesDeDecidir, alDecidir }`.
 * @returns {Promise<{markdown: string, executiveSummary: string, tokensUsed: number, modelId: string,
 *   sources: object[], recommendations: object[], theses: object[], followUp: object,
 *   corrections: object[], decisionPath: string}>}
 * @throws {CopilotEngineError} Si el motor no responde al redactar: sin informe
 *   no hay análisis, y eso sí es un fallo (RN-5-C).
 */
async function redactarInforme(entrada, { userId, secret, language }, seguimiento = {}) {
  const { antesDeDecidir, alDecidir } = { ...SEGUIMIENTO_SIN_CAMBIOS, ...seguimiento };
  const entorno = { userId, secret, language };

  const construido = await construirContexto({ language, ...entrada });
  const context = antesDeDecidir(construido.context);

  const decision = await decidir(context, entorno);

  if (decision) {
    const resuelto = alDecidir(decision, context);
    const salida = await engineClient.redactar(userId, secret, {
      language,
      context: {
        ...context,
        comparison: resuelto.comparison,
        decisions: { recommendations: resuelto.recommendations, theses: resuelto.theses },
      },
    });
    const markdown = limpiarMarcasDelCuerpo(salida.text);

    return {
      markdown,
      executiveSummary: extraerResumenEjecutivo(markdown),
      tokensUsed: decision.tokensUsed + salida.tokensUsed,
      modelId: salida.modelId,
      sources: construido.fuentes,
      recommendations: resuelto.recommendations,
      theses: resuelto.theses,
      // Los estados de las anteriores, ya calculados: de aquí sale el recuento
      // de la tarjeta del historial.
      followUp: resuelto.followUp,
      corrections: resuelto.corrections,
      decisionPath: RUTA_DE_DECISION.DECIDIDA,
    };
  }

  const salida = await engineClient.redactar(userId, secret, { language, context });
  const markdown = limpiarMarcasDelCuerpo(salida.text);

  // HU 5.4: con el informe escrito, se extrae su estructura para que el
  // análisis siguiente pueda compararse contra él.
  const estructura = await extraerEstructura(markdown, entorno);
  const resuelto = alDecidir(estructura, context);

  return {
    markdown,
    executiveSummary: extraerResumenEjecutivo(markdown),
    tokensUsed: salida.tokensUsed + estructura.tokensUsed,
    modelId: salida.modelId,
    sources: construido.fuentes,
    recommendations: resuelto.recommendations,
    theses: resuelto.theses,
    followUp: resuelto.followUp,
    corrections: resuelto.corrections,
    decisionPath: RUTA_DE_DECISION.DEGRADADA,
  };
}

module.exports = {
  ENCABEZADOS_RESUMEN,
  ESTADOS_DE_RECOMENDACION,
  ESTADOS_DE_TESIS,
  SEGUIMIENTO_VACIO,
  RUTA_DE_DECISION,
  PILARES,
  ACCIONES,
  NIVELES,
  extraerResumenEjecutivo,
  limpiarMarcasDelCuerpo,
  extraerEstructura,
  decidir,
  normalizarDecision,
  normalizarRecomendaciones,
  normalizarTramos,
  normalizarTesis,
  normalizarSeguimiento,
  construirContexto,
  redactarInforme,
};
