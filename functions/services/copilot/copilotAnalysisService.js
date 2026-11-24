/**
 * HU 5.3 — El análisis, de principio a fin.
 *
 * Depurar → seleccionar → investigar activo por activo → resto → macro →
 * redactar. Cada tramo es un paso del plan que el usuario ve avanzar, y cada
 * paso es trabajo real: el progreso de esta historia no se puede fingir, porque
 * cada etapa se cierra con lo que de verdad obtuvo.
 *
 * El ciclo de vida —lanzar, avisar, fallar sin cobrar, dar por colgado— lo puso
 * la HU 5.2 y aquí no se toca: lo único que cambia es lo que ocurre entre el
 * lanzamiento y el cierre.
 *
 * Cuándo falla el análisis entero (RN-5-C):
 * - tras depurar no queda ninguna posición válida (Escenario 13);
 * - **no se obtiene ni un solo dato en vivo**, tampoco el macro (Escenario 15).
 *   Si fallan todos los activos pero el macro responde, el análisis se entrega
 *   (QA-31): el fallo de uno o varios activos nunca tumba el análisis
 *   (Escenario 19).
 *
 * @module services/copilot/copilotAnalysisService
 * @see platform-docs/stories/5.3-analista-investiga-recomienda/refinamiento.md (T13)
 */

const admin = require('../firebaseAdmin');
const runRepository = require('./copilotRunRepository');
const payloadBuilder = require('./copilotPayloadBuilder');
const fundamentals = require('./copilotFundamentals');
const cleanser = require('./copilotDataCleanser');
const relevance = require('./copilotRelevance');
const restOfPortfolio = require('./copilotRestOfPortfolio');
const research = require('./copilotResearch');
const composer = require('./copilotComposer');
const followUp = require('./copilotFollowUp');
const guard = require('./copilotDecisionGuard');
const { cierresDeLaSerie } = require('./copilotAssetCardService');
const { RESEARCH_OUTCOME } = require('./copilotFindings');
const notifications = require('./copilotNotifications');
const quota = require('./copilotQuota');
const {
  STEP_KIND,
  FAILURE_CODE,
  COPILOT_ANALYSES_SUBCOLLECTION,
  CRITERIO_DE_DECISION,
} = require('./copilotConstants');

// HU 5.6: `CONCURRENCIA_ACTIVOS` desapareció. Existía porque diez activos en
// serie, a 45-60 s por llamada, no cabían en los 540 s de una función v2 por
// eventos. Con una sola llamada para todos los activos y el macro no hay nada
// que paralelizar: lo que decide el reloj es esa única llamada.

/**
 * Identificador del análisis, derivado del **momento del lanzamiento**.
 *
 * RN-5.2-R: el análisis retrata el portafolio de cuando se lanzó, y su fecha en
 * el historial es esa, no la del momento en que el motor terminó de escribir.
 *
 * @param {string} startedAt - ISO 8601.
 * @returns {string} `YYYYMMDDTHHMMSSZ`.
 */
function buildAnalysisId(startedAt) {
  return new Date(startedAt).toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');
}

/**
 * El resumen que las tarjetas del historial muestran sin abrir el análisis.
 *
 * Conserva los metadatos de referencia (β, YTD, valor, nº de activos): RN-5-D
 * se aplica al contenido del análisis, no a la tarjeta (confirmado por el PO).
 *
 * @param {object} payload
 * @param {number} analizados - Activos que quedaron tras depurar.
 * @returns {object}
 */
function buildPayloadSummary(payload, analizados) {
  return {
    portfolioBeta: payload.portfolioBeta,
    riskCategory: payload.riskCategory,
    ytdReturn: payload.ytdReturn,
    totalPortfolioValue: payload.totalPortfolioValue,
    currency: payload.userCurrency,
    totalAssets: analizados,
    isLive: payload.isLive,
  };
}

/**
 * El plan de pasos que el usuario verá avanzar (Escenario 6).
 *
 * @param {object[]} relevantes
 * @param {boolean} hayResto
 * @returns {Array<object>}
 */
function construirPlan(relevantes, hayResto) {
  const pasos = relevantes.map((h) => ({
    kind: STEP_KIND.ASSET,
    ticker: h.symbol,
    reason: h.motivo,
    ...(h.motivo === 'variacion' ? { variation: h.variacionSemanal } : {}),
  }));

  if (hayResto) pasos.push({ kind: STEP_KIND.REST });
  pasos.push({ kind: STEP_KIND.MACRO });
  pasos.push({ kind: STEP_KIND.COMPOSING });

  return pasos;
}

/**
 * Lo que se escribe al lado de una etapa completada ("resultados Q3, consenso,
 * noticias"). Un paso fallido no lleva resumen: lo cierra como fallido.
 *
 * @param {object} investigacion
 * @returns {string|null}
 */
function resumenDePaso(investigacion) {
  if (investigacion.outcome === RESEARCH_OUTCOME.FALLO) return null;
  if (investigacion.findings.length === 0) return investigacion.notes[0] || null;

  return investigacion.findings
    .slice(0, 3)
    .map((f) => f.label)
    .join(', ');
}

/**
 * HU 6.11.2 — La nota con que empieza el informe cuando la moneda pedida no
 * estaba disponible y el análisis se armó en USD (Escenario 5, acordada con el
 * PO el 2026-11-14).
 *
 * Es un texto fijo, no lo redacta el modelo: así el informe **siempre** lo
 * dice, sin depender de que el redactor lo mencione.
 */
const NOTA_MONEDA_NO_DISPONIBLE = {
  es: (moneda) => `> Este análisis está en USD porque ${moneda} no está disponible por ahora.`,
  en: (moneda) => `> This analysis is in USD because ${moneda} isn't available right now.`,
};

/**
 * @param {string} markdown - El informe redactado
 * @param {string|null|undefined} monedaNoDisponible - La moneda pedida que no se pudo usar
 * @param {string} language
 * @returns {string} El informe, con la nota delante si hace falta
 */
function conNotaDeMoneda(markdown, monedaNoDisponible, language) {
  if (!monedaNoDisponible) return markdown;
  const nota = (NOTA_MONEDA_NO_DISPONIBLE[language] || NOTA_MONEDA_NO_DISPONIBLE.es)(monedaNoDisponible);
  return `${nota}\n\n${markdown}`;
}

/**
 * Guarda el análisis terminado.
 *
 * @param {string} userId
 * @param {string} analysisId
 * @param {string} createdAt
 * @param {object} informe
 * @param {object} payload
 * @param {object} meta
 * @returns {Promise<void>}
 */
async function persistAnalysis(userId, analysisId, createdAt, informe, payload, meta) {
  // HU 5.7.3: los estados ya vienen calculados por el código (RN-5.7-P), así
  // que se cuentan tal cual.
  const seguimiento = informe.followUp || { recommendations: [], theses: [] };

  await admin
    .firestore()
    .collection('userData')
    .doc(userId)
    .collection(COPILOT_ANALYSES_SUBCOLLECTION)
    .doc(analysisId)
    .set({
      id: analysisId,
      createdAt,
      status: 'completed',
      analysisMarkdown: informe.markdown,
      // HU 5.3: la tarjeta del historial muestra el resumen ejecutivo, no los
      // primeros 150 caracteres del markdown.
      executiveSummary: informe.executiveSummary,
      // HU 5.4: lo que hace comparable este análisis para el siguiente. Si la
      // extracción falló, va vacío — y el siguiente lo tratará como un análisis
      // sin recomendaciones comparables, que es lo que es.
      recommendations: informe.recommendations || [],
      theses: informe.theses || [],
      // HU 5.4: la comparación es una fotografía (RN-5.4-V). Se guarda hecha y
      // no se recalcula al reabrir el análisis ni al borrar su referencia.
      comparison: meta.comparacion
        ? {
          state: meta.comparacion.estado,
          referenceId: meta.comparacion.referencia ? meta.comparacion.referencia.id : null,
          referenceDate: meta.comparacion.referencia
            ? meta.comparacion.referencia.createdAt : null,
          // El estado final de cada una no está en `meta.comparacion`, que
          // solo lleva los cierres de entrada: lo calculó el código al decidir
          // (HU 5.7.3) y vuelve en `informe.followUp`.
          followUp: followUp.contarSeguimiento(seguimiento.recommendations, seguimiento.theses),
        }
        : null,
      payloadSummary: buildPayloadSummary(payload, meta.analizados),
      tokensUsed: meta.tokensUsed,
      modelId: informe.modelId,
      feedback: null,
      // HU 5.7.1: el criterio de decisión con que se redactó. La 5.7.3 lo lee
      // para saber qué recomendaciones anteriores se emitieron sin él; un
      // análisis anterior a esta historia no lo tiene (QA-23).
      decisionCriteria: CRITERIO_DE_DECISION,
      _meta: {
        promptVersion: 'v5',
        researchedAssets: meta.investigados,
        failedAssets: meta.fallidos,
        excludedPositions: meta.excluidos,
        sources: informe.sources.length,
        // HU 5.7.3: por qué camino se decidió y qué corrigió el código. Es lo
        // que se lee en producción para verificar RN-5.7-Q, R y T.
        decisionPath: informe.decisionPath || null,
        guardCorrections: informe.corrections || [],
      },
    });
}

/**
 * HU 5.4 — Lo que el redactor ve de la comparación.
 *
 * Sólo lo que necesita para escribir la sección 2: el estado, la línea de
 * referencia ya construida (la fecha y el tiempo transcurrido son datos, no
 * una redacción) y las recomendaciones con las que ya vienen cerradas marcadas
 * como tales. El resto —qué análisis era, sus metadatos— no le hace falta.
 *
 * HU 5.7.3: cada anterior lleva su `id` (el estado vuelve por él, RN-5.7-S),
 * su acción y sus tramos (con ellos se compara con la de hoy, RN-5.7-P) y su
 * precio de emisión, el de hoy y la línea ya escrita (RN-5.7-L).
 *
 * @param {object} comparacion - La salida de `prepararComparacion`.
 * @param {object} [precios]
 * @param {Map<string, object|null>} [precios.series] - Activo → serie de 1 año.
 * @param {Map<string, number>} [precios.preciosHoy] - Activo → precio de hoy.
 * @param {string} [precios.language]
 * @returns {object|null} `null` cuando no hay nada que decir.
 */
function construirComparacionParaElInforme(comparacion, {
  series = new Map(), preciosHoy = new Map(), language = 'es',
} = {}) {
  if (!comparacion) return null;
  const emitidoEn = comparacion.referencia ? comparacion.referencia.createdAt : null;

  return {
    state: comparacion.estado,
    referenceLine: comparacion.referenceLine,
    criterionChanged: Boolean(comparacion.criterionChanged),
    recommendations: comparacion.recomendaciones.map((rec) => ({
      id: rec.id || null,
      statement: rec.statement,
      asset: rec.asset,
      action: guard.accionDe(rec),
      conviction: rec.conviction,
      targetPct: rec.targetPct,
      priceZone: rec.priceZone,
      horizon: rec.horizon,
      tranches: Array.isArray(rec.tranches) ? rec.tranches : [],
      // Las que ya vienen decididas no las juzga el modelo: se respetan.
      closedState: rec.closedState,
      closedReason: rec.closedReason,
      closedAt: rec.closedAt || null,
      price: emitidoEn
        ? followUp.precioDeRecomendacion(rec, {
          emitidoEn,
          serie: rec.asset ? series.get(rec.asset) || null : null,
          precioHoy: rec.asset && preciosHoy.has(rec.asset) ? preciosHoy.get(rec.asset) : null,
          language,
        })
        : null,
    })),
    theses: comparacion.tesis.map((t) => ({ id: t.id || null, statement: t.statement, asset: t.asset })),
  };
}

/**
 * HU 5.7.3 — El último cierre de una serie, o `null`.
 *
 * @param {object|null} serie
 * @returns {number|null}
 */
function ultimoCierre(serie) {
  const cierres = cierresDeLaSerie(serie);
  return cierres.length > 0 ? cierres[cierres.length - 1].cierre : null;
}

/**
 * HU 5.7.3 (RN-5.7-L) — El precio de hoy de cada activo, y la serie de 1 año
 * de los que tienen una recomendación anterior.
 *
 * El precio de hoy es el del portafolio (el mismo que ve el usuario); si no lo
 * trae, el último cierre de la serie. Los activos de una recomendación
 * anterior que hoy no tienen ficha (vendidos, nuevos) piden su serie aparte.
 *
 * **Nunca lanza**: sin precios, el seguimiento dice "no disponible" (RN-5-H).
 *
 * @param {object} entrada
 * @param {object[]} entrada.holdings
 * @param {Map<string, object>} entrada.priceMap
 * @param {Map<string, object>} entrada.datosDePlataforma
 * @param {object} entrada.comparacion
 * @returns {Promise<{preciosHoy: Map<string, number>, series: Map<string, object|null>}>}
 */
async function preciosDelSeguimiento({
  holdings, priceMap, datosDePlataforma, comparacion,
}) {
  const preciosHoy = new Map();
  const series = new Map();

  try {
    for (const h of holdings) {
      const info = priceMap && typeof priceMap.get === 'function' ? priceMap.get(h.symbol) : null;
      if (info && typeof info.price === 'number' && Number.isFinite(info.price) && info.price > 0) {
        preciosHoy.set(h.symbol, info.price);
      }
    }

    for (const [symbol, datos] of datosDePlataforma) {
      series.set(symbol, (datos && datos.serie) || null);
      if (!preciosHoy.has(symbol) && datos && datos.card && typeof datos.card.price === 'number') {
        preciosHoy.set(symbol, datos.card.price);
      }
    }

    const faltan = (comparacion.recomendaciones || [])
      .map((r) => r.asset)
      .filter((a) => a && !series.has(a));
    if (faltan.length > 0) {
      const extra = await fundamentals.seriesDeActivos(faltan);
      for (const [symbol, serie] of extra) {
        series.set(symbol, serie);
        const cierre = ultimoCierre(serie);
        if (!preciosHoy.has(symbol) && cierre !== null) preciosHoy.set(symbol, cierre);
      }
    }
  } catch (error) {
    console.error('[copilotAnalysisService] Sin precios para el seguimiento:', error.message);
  }

  return { preciosHoy, series };
}

/**
 * HU 5.7.3 — Lo que el código hace antes y después de que el modelo decida.
 *
 * - `antesDeDecidir`: cierra las reducciones o ventas anteriores que la ficha
 *   de hoy no permite (RN-5.7-P), para que el modelo las reciba ya marcadas.
 * - `alDecidir`: corrige las recomendaciones de hoy (RN-5.7-Q, R y T), les
 *   pone su precio de emisión (RN-5.7-L) y calcula el estado de las anteriores
 *   comparándolas con ellas (RN-5.7-P).
 *
 * @param {object} entrada
 * @param {Map<string, number>} entrada.preciosHoy
 * @param {Map<string, number>} entrada.pesos - Activo → peso en %.
 * @param {string} entrada.language
 * @returns {{antesDeDecidir: Function, alDecidir: Function}}
 */
function crearSeguimiento({ preciosHoy, pesos, language }) {
  const permitidasDe = (context) => {
    const permitidas = new Map();
    for (const activo of context.assets || []) {
      const signals = activo.card && activo.card.signals;
      if (signals && Array.isArray(signals.allowedActions)) {
        permitidas.set(activo.ticker, signals.allowedActions);
      }
    }
    return permitidas;
  };

  const fichasDe = (context) => new Map((context.assets || [])
    .filter((a) => a.card)
    .map((a) => [a.ticker, a.card]));

  return {
    antesDeDecidir(context) {
      const comparison = context.comparison;
      if (!comparison || !Array.isArray(comparison.recommendations)) return context;

      return {
        ...context,
        comparison: {
          ...comparison,
          recommendations: followUp.cerrarPorCriterio(comparison.recommendations, permitidasDe(context)),
        },
      };
    },

    alDecidir(decision, context) {
      const { recomendaciones, correcciones } = guard.aplicarGuarda(decision.recommendations, {
        fichas: fichasDe(context), pesos, language,
      });

      // RN-5.7-L: cada recomendación guarda el precio de su activo al emitirse.
      const recommendations = recomendaciones.map((rec) => ({
        ...rec,
        priceAtIssue: rec.asset && preciosHoy.has(rec.asset) ? preciosHoy.get(rec.asset) : null,
      }));

      const comparison = context.comparison || null;
      const declarados = decision.followUp || { recommendations: [], theses: [] };
      if (!comparison || comparison.state !== followUp.COMPARISON_STATE.COMPARED) {
        return {
          recommendations,
          theses: decision.theses,
          comparison,
          followUp: { recommendations: [], theses: [] },
          corrections: correcciones,
        };
      }

      const estados = followUp.estadosCalculados(
        comparison.recommendations, recommendations, declarados.recommendations,
        { preciosHoy, permitidas: permitidasDe(context) },
      );
      const tesis = followUp.estadosDeTesis(comparison.theses, declarados.theses);

      return {
        recommendations,
        theses: decision.theses,
        comparison: { ...comparison, recommendations: estados, theses: tesis },
        followUp: { recommendations: estados, theses: tesis },
        corrections: correcciones,
      };
    },
  };
}

/**
 * Da el análisis por fallido y avisa.
 *
 * @param {object} run
 * @param {string} errorCode
 * @returns {Promise<void>}
 */
async function marcarFallido(run, errorCode) {
  const cerrado = await runRepository.failRun(run.userId, run.runId, errorCode);
  if (!cerrado) {
    console.log(`[copilotAnalysisService] Fallo tardío descartado (${errorCode}) para ${run.runId}`);
    return;
  }
  await notifications.notifyAnalysisFailed(run.userId, { language: run.language });
}

/**
 * Corre el análisis completo.
 *
 * @param {object} run - El documento de `copilotRuns` recién creado.
 * @param {{ marketDataTokenSecret: string }} entorno
 * @returns {Promise<{ desenlace: 'completed'|'failed'|'abandoned', analysisId?: string, errorCode?: string }>}
 */
async function runAnalysis(run, { marketDataTokenSecret }) {
  const { userId, runId, language } = run;
  const motor = { userId, secret: marketDataTokenSecret, language };

  /** Abandona en silencio si el análisis dejó de ser el vigente. */
  const abandonado = { desenlace: 'abandoned' };

  try {
    // ---- Paso 1: depurando los datos del portafolio ------------------------
    if (!(await runRepository.startStep(userId, runId, 0))) return abandonado;

    const { sinPosiciones, payload, estado } = await payloadBuilder.buildBasePayload(userId, run);
    if (sinPosiciones) {
      await marcarFallido(run, FAILURE_CODE.NO_VALID_POSITIONS);
      return { desenlace: 'failed', errorCode: FAILURE_CODE.NO_VALID_POSITIONS };
    }

    const { validos, excluidos } = cleanser.cleanse(estado.assets, estado.priceMap);

    // Escenario 13: si tras depurar no queda nada, el análisis no se entrega y
    // el crédito no se consume.
    if (validos.length === 0) {
      await runRepository.finishStep(userId, runId, 0, { ok: false });
      await marcarFallido(run, FAILURE_CODE.NO_VALID_POSITIONS);
      return { desenlace: 'failed', errorCode: FAILURE_CODE.NO_VALID_POSITIONS };
    }

    // Los pesos se recalculan sobre lo depurado: una posición excluida deja su
    // lugar entre los de mayor peso (QA-20).
    const holdings = payloadBuilder.buildUserHoldings(validos, estado.priceMap, estado.rates);

    // HU 5.4: el análisis anterior se consulta ANTES de decidir qué investigar,
    // porque lo que recomendó amplía la lista (RN-5.4-X). Si no se puede
    // consultar, el análisis sigue su curso sin comparación (Escenario 13).
    const comparacion = await followUp.prepararComparacion(userId, {
      tickersEnCartera: new Set(holdings.map((h) => h.symbol)),
      language,
    });

    const { relevantes, resto } = await relevance.seleccionarRelevantes(
      holdings,
      new Date(),
      comparacion.activosARevisar,
    );

    await runRepository.finishStep(userId, runId, 0, {
      ok: true,
      summary: `${holdings.length}|${excluidos.length}`,
    });

    // ---- El plan, ya que se sabe qué se va a investigar --------------------
    const plan = construirPlan(relevantes, resto.length > 0);
    if (!(await runRepository.setSteps(userId, runId, plan))) return abandonado;

    // HU 5.6 (RN-5.6-A): los datos duros los pone la plataforma, no el modelo.
    // Esto va ANTES de investigar porque decide qué se le pregunta a la web: un
    // activo cubierto sólo necesita que le interpreten lo que pasó.
    //
    // El try/catch no es decorativo: RN-5.6-D dice que un proveedor de datos
    // caído **degrada** el análisis, nunca lo cancela. Sin él, cualquier fallo
    // inesperado aquí caería en el catch general y marcaría el análisis como
    // fallido — justo lo contrario de lo que la regla ordena.
    let datosDePlataforma = new Map();
    try {
      datosDePlataforma = await fundamentals.obtenerDatosDePlataforma(relevantes, { language });
    } catch (error) {
      console.error('[copilotAnalysisService] Sin datos de la plataforma:', error.message);
    }

    // HU 5.7.3 (RN-5.7-L): los precios de emisión y de hoy del seguimiento.
    const { preciosHoy, series } = await preciosDelSeguimiento({
      holdings, priceMap: estado.priceMap, datosDePlataforma, comparacion,
    });

    // ---- Los pasos por activo, todos a la vez ------------------------------
    //
    // Una sola llamada cubre todos los activos y el macro (Escenario 7), así
    // que todos sus pasos están realmente en curso a la vez. El plan que ve el
    // usuario no cambia —la misma lista, en el mismo orden—; lo que cambia es
    // que ya no se cierran de cuatro en cuatro, sino cuando la tanda responde.
    const arrancados = await Promise.all(
      relevantes.map((_, i) => runRepository.startStep(userId, runId, i + 1)),
    );
    if (arrancados.some((ok) => !ok)) return abandonado;

    const tanda = await research.investigarTanda(relevantes, motor, datosDePlataforma);

    const investigaciones = relevantes.map((holding, i) => ({
      ...tanda.investigaciones[i],
      name: holding.name || holding.symbol,
      // Sin el peso, el redactor recomienda magnitudes a ciegas: "reduce un
      // 2%" sin saber si se parte del 3% o del 12%.
      weight: holding.weight,
      motivo: holding.motivo,
      variacionSemanal: holding.variacionSemanal,
      // HU 5.7.1: la ficha de cuatro pilares, si la plataforma pudo armarla.
      card: (datosDePlataforma.get(holding.symbol) || {}).card || null,
    }));

    await Promise.all(investigaciones.map((investigacion, i) => runRepository.finishStep(
      userId, runId, i + 1,
      {
        ok: investigacion.outcome !== RESEARCH_OUTCOME.FALLO,
        summary: resumenDePaso(investigacion),
      },
    )));

    let indice = relevantes.length + 1;

    // ---- El resto del portafolio, sin investigar ---------------------------
    let resumenResto = null;
    if (resto.length > 0) {
      if (!(await runRepository.startStep(userId, runId, indice))) return abandonado;
      resumenResto = restOfPortfolio.resumirResto(resto);
      await runRepository.finishStep(userId, runId, indice, {
        ok: true,
        summary: String(resto.length),
      });
      indice += 1;
    }

    // ---- El contexto macro -------------------------------------------------
    //
    // HU 5.6: ya no es una llamada propia — viajó dentro de la tanda. El paso
    // sigue existiendo porque el usuario lo ve, y se cierra con lo que la tanda
    // entregó bajo su clave, que es la verdad de lo que se obtuvo.
    if (!(await runRepository.startStep(userId, runId, indice))) return abandonado;
    const { macro } = tanda;
    await runRepository.finishStep(userId, runId, indice, {
      ok: macro.outcome !== RESEARCH_OUTCOME.FALLO,
      summary: resumenDePaso(macro),
    });
    indice += 1;

    // ---- Escenario 15: ni un solo dato en vivo ------------------------------
    //
    // HU 5.6: las cifras de la plataforma **son** datos en vivo, y no dependen
    // del *grounding*. Como `conDatosDePlataforma` deja en `ok` a todo activo
    // que las traiga, un análisis con la búsqueda web caída pero con datos de
    // `finance-query` se entrega — algo que antes de esta historia no ocurría.
    const algoEnVivo = investigaciones.some((i) => i.outcome !== RESEARCH_OUTCOME.FALLO)
      || macro.outcome !== RESEARCH_OUTCOME.FALLO;

    if (!algoEnVivo) {
      await marcarFallido(run, FAILURE_CODE.RESEARCH_UNAVAILABLE);
      return { desenlace: 'failed', errorCode: FAILURE_CODE.RESEARCH_UNAVAILABLE };
    }

    // ---- Redactando recomendaciones ----------------------------------------
    if (!(await runRepository.startStep(userId, runId, indice))) return abandonado;

    const informe = await composer.redactarInforme(
      {
        investigaciones,
        macro: macro.outcome === RESEARCH_OUTCOME.FALLO ? null : macro,
        resto: resumenResto,
        excluidos,
        portafolio: {
          currency: payload.userCurrency,
          totalAssets: holdings.length,
          // HU 6.11.2: el redactor sabe que la moneda pedida no estaba disponible.
          ...(payload.unavailableCurrency ? { unavailableCurrency: payload.unavailableCurrency } : {}),
        },
        comparacion: construirComparacionParaElInforme(comparacion, { series, preciosHoy, language }),
      },
      motor,
      crearSeguimiento({
        preciosHoy,
        pesos: new Map(holdings.map((h) => [h.symbol, Math.round(h.weight * 1000) / 10])),
        language,
      }),
    );
    informe.markdown = conNotaDeMoneda(informe.markdown, payload.unavailableCurrency, language);

    await runRepository.finishStep(userId, runId, indice, { ok: true, summary: null });

    // ---- Cierre ------------------------------------------------------------
    const analysisId = buildAnalysisId(run.startedAt);
    // HU 5.6: el consumo de la investigación es el de **la llamada**, no la suma
    // de sus claves. Sumarlo por activo contaría el mismo prompt seis veces y
    // haría irreconocible el ahorro que mide el Escenario 7.
    const tokensUsed = tanda.tokensUsed + informe.tokensUsed;

    await persistAnalysis(userId, analysisId, run.startedAt, informe, payload, {
      analizados: holdings.length,
      investigados: investigaciones.filter((i) => i.outcome !== RESEARCH_OUTCOME.FALLO).length,
      fallidos: investigaciones.filter((i) => i.outcome === RESEARCH_OUTCOME.FALLO).map((i) => i.ticker),
      excluidos: excluidos.map((e) => e.ticker),
      tokensUsed,
      comparacion,
    });

    const cerrado = await runRepository.completeRun(userId, runId, analysisId);
    if (!cerrado) {
      await borrarAnalisisDescartado(userId, analysisId);
      console.log(`[copilotAnalysisService] Resultado tardío descartado para ${runId}`);
      return abandonado;
    }

    await quota.consumeQuota(userId);
    await notifications.notifyAnalysisReady(userId, { language, analysisId });

    return { desenlace: 'completed', analysisId };
  } catch (error) {
    const errorCode = error.code || FAILURE_CODE.UNEXPECTED;
    console.error(`[copilotAnalysisService] Análisis ${runId} fallido (${errorCode}):`, error);
    await marcarFallido(run, errorCode);
    return { desenlace: 'failed', errorCode };
  }
}

/**
 * Retira el análisis que llegó tarde.
 *
 * @param {string} userId
 * @param {string} analysisId
 * @returns {Promise<void>}
 */
async function borrarAnalisisDescartado(userId, analysisId) {
  try {
    await admin
      .firestore()
      .collection('userData')
      .doc(userId)
      .collection(COPILOT_ANALYSES_SUBCOLLECTION)
      .doc(analysisId)
      .delete();
  } catch (error) {
    console.error('[copilotAnalysisService] No se pudo retirar el análisis descartado:', error);
  }
}

module.exports = {
  construirComparacionParaElInforme,
  preciosDelSeguimiento,
  crearSeguimiento,
  buildAnalysisId,
  buildPayloadSummary,
  construirPlan,
  resumenDePaso,
  conNotaDeMoneda,
  persistAnalysis,
  runAnalysis,
};
