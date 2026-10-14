/**
 * HU 5.6 — Investigar todo el portafolio en una sola llamada.
 *
 * La HU 5.3 hacía una llamada por activo, más una para el macro. Era lo que
 * permitía que cada hallazgo trajera su fuente, que cada etapa del progreso se
 * cerrara con lo que obtuvo y que el fallo de un activo dejara vivo al resto.
 * Las tres siguen siendo ciertas, y ninguna necesitaba una llamada por activo:
 *
 * - **Las fuentes** las sigue poniendo el *grounding*, que es por llamada.
 * - **El progreso** se cierra por activo leyendo su clave de la respuesta.
 * - **El fallo aislado** lo da el reparto: una clave que el modelo omita queda
 *   como fallida sin arrastrar a las demás.
 *
 * Lo que sí dependía de ello era el coste. Cada búsqueda con *grounding* se
 * factura **por llamada**, no por activo ni por cuántas búsquedas haga dentro,
 * así que seis activos y el macro en una tanda cuestan lo que antes costaba
 * uno: de ~$0.20 a ~$0.045 por análisis (Escenario 7).
 *
 * Y lo que se le pregunta encogió: los datos estructurados los pone ahora
 * `copilotFundamentals` desde `finance-query` (RN-5.6-A), así que a la web sólo
 * le queda interpretar — qué está pasando y por qué se mueve el precio.
 *
 * @module services/copilot/copilotResearch
 * @see platform-docs/stories/5.6-cifras-duras-desde-la-plataforma/refinamiento.md (T5)
 */

const engineClient = require('./copilotEngineClient');
const findings = require('./copilotFindings');
const cache = require('./copilotResearchCache');
const { claseDeActivo } = require('./copilotRelevance');

/**
 * Una consulta que falla y se recupera dentro del mismo análisis **no es un
 * fallo** (QA-26). Un reintento: si el segundo intento tampoco entrega, lo que
 * no llegó se declara como no investigado y el análisis sigue.
 */
const REINTENTOS = 1;

/**
 * Hasta cuándo merece la pena reintentar la tanda.
 *
 * El reintento existe para un caso concreto y **rápido**: que el modelo
 * devuelva el JSON envuelto en prosa. Eso se sabe en cuanto responde.
 *
 * Desde la HU 5.6 la tanda puede tardar hasta los 300 s que dura la Lambda, y
 * un segundo intento de otros 300 s se saldría de los 540 s que dura la función
 * que orquesta: el análisis moriría entero, sin informe, por intentar salvar
 * una respuesta mal formada. Si el primer intento ya consumió este presupuesto,
 * no hay segundo — igual que con la cuota agotada, el segundo intento
 * encontraría lo mismo y se llevaría por delante lo que sí funcionó.
 */
const PRESUPUESTO_PARA_REINTENTO_MS = 150000;

/** La clave bajo la que viaja el contexto macro dentro de la tanda. */
const CLAVE_MACRO = 'macro';

/**
 * La exposición del portafolio que el contexto macro tiene que mirar.
 *
 * El Escenario 2 de la HU 5.3 prohíbe "un párrafo macro genérico que valdría
 * igual para cualquier portafolio", así que lo que se le manda al modelo son
 * **las divisas y los sectores de este portafolio**, con su peso.
 *
 * @param {object[]} holdings - Posiciones válidas, agregadas por ticker.
 * @returns {{divisas: object[], sectores: object[]}}
 */
function construirExposicion(holdings) {
  const acumular = (clave) => {
    const mapa = new Map();
    for (const h of holdings) {
      const nombre = h[clave] || 'Otros';
      mapa.set(nombre, (mapa.get(nombre) || 0) + h.weight * 100);
    }
    return [...mapa.entries()]
      .map(([nombre, peso]) => ({ nombre, codigo: nombre, peso: Math.round(peso * 10) / 10 }))
      .sort((a, b) => b.peso - a.peso);
  };

  return { divisas: acumular('currency'), sectores: acumular('sector') };
}

/**
 * Una investigación vacía, con el desenlace de lo que no se pudo investigar.
 *
 * @param {string} ticker
 * @returns {object}
 */
function fallida(ticker) {
  return { ...findings.normalizarInvestigacion(ticker, null), tokensUsed: 0, groundingSources: [] };
}

/**
 * Le añade a una investigación los datos que puso la plataforma.
 *
 * Van **delante** de lo que encontró el modelo: son los que no pueden estar mal
 * y los que el informe cita primero. Y un activo que sólo tenga datos de
 * plataforma deja de estar "fallido": tiene hechos verificables, que es lo que
 * la palabra significa (RN-5.6-D).
 *
 * @param {object} investigacion
 * @param {object[]} deLaPlataforma
 * @returns {object}
 */
function conDatosDePlataforma(investigacion, deLaPlataforma) {
  if (!deLaPlataforma || deLaPlataforma.length === 0) return investigacion;

  const findingsUnidos = [...deLaPlataforma, ...investigacion.findings];

  return {
    ...investigacion,
    findings: findingsUnidos,
    outcome: findingsUnidos.length > 0 ? findings.RESEARCH_OUTCOME.OK : investigacion.outcome,
  };
}

/**
 * Investiga todos los activos relevantes y el macro, en **una** llamada.
 *
 * Nunca lanza: lo que no se pudo investigar vuelve con `outcome: 'fallo'`, que
 * es información para el informe y no un error del análisis (RN-5.3-R).
 *
 * @param {object[]} holdings - Las posiciones relevantes, con `symbol`,
 *   `asset_type` y `name`.
 * @param {object} entorno - `{ userId, secret, language }`.
 * @param {Map<string, {findings: object[], cubierto: boolean}>} [datosDePlataforma] -
 *   Lo que `copilotFundamentals` consiguió para cada ticker.
 * @returns {Promise<{investigaciones: object[], macro: object, tokensUsed: number}>}
 *   Las investigaciones van en el **mismo orden** que `holdings`.
 */
async function investigarTanda(holdings, { userId, secret, language }, datosDePlataforma = new Map()) {
  const plataformaDe = (symbol) => (datosDePlataforma.get(symbol) || { findings: [], cubierto: false });

  // Lo que ya se investigó hace poco no se vuelve a pagar. Son hechos públicos
  // sobre una empresa, así que el caché se comparte entre usuarios — y sale de
  // la tanda, que es lo que la abarata todavía más.
  const cacheadas = new Map();
  await Promise.all(holdings.map(async (h) => {
    try {
      const cacheada = await cache.leer(h.symbol, language);
      if (cacheada) {
        console.log(`[copilotResearch] ${h.symbol}: servido del caché`);
        cacheadas.set(h.symbol, cacheada);
      }
    } catch (error) {
      // Un caché que falla es un caché que no acierta, no un análisis roto
      // (RN-5-C). Se investiga como si no hubiera nada guardado.
      console.error(`[copilotResearch] el caché de ${h.symbol} no se pudo leer:`, error.message);
    }
  }));

  // RN-5.6-A: el activo cuyas cifras puso la plataforma va a la tanda con el
  // salto puesto, y al modelo sólo se le pregunta qué está pasando con él.
  const porInvestigar = holdings
    .filter((h) => !cacheadas.has(h.symbol))
    .map((h) => ({ ...h, skipDatos: plataformaDe(h.symbol).cubierto }));

  const exposicion = construirExposicion(holdings);
  const { porClave, tokensUsed } = await pedirLaTanda(
    porInvestigar, exposicion, { userId, secret, language },
  );

  const investigaciones = holdings.map((holding) => {
    const base = cacheadas.get(holding.symbol)
      || porClave.get(holding.symbol)
      || fallida(holding.symbol);
    return conDatosDePlataforma(base, plataformaDe(holding.symbol).findings);
  });

  return {
    investigaciones,
    macro: porClave.get(CLAVE_MACRO) || fallida(CLAVE_MACRO),
    tokensUsed,
  };
}

/**
 * La llamada en sí, con su reintento, y el reparto de lo que devolvió.
 *
 * @param {object[]} porInvestigar - Los que no estaban en caché, con `skipDatos`.
 * @param {object} exposicion
 * @param {object} entorno - `{ userId, secret, language }`.
 * @returns {Promise<{porClave: Map<string, object>, tokensUsed: number}>}
 */
async function pedirLaTanda(porInvestigar, exposicion, { userId, secret, language }) {
  const claves = [...porInvestigar.map((h) => h.symbol), CLAVE_MACRO];
  const vacio = { porClave: new Map(), tokensUsed: 0 };

  const peticion = {
    task: 'assets',
    language,
    exposure: exposicion,
    assets: porInvestigar.map((h) => ({
      ticker: h.symbol,
      name: h.name || h.symbol,
      assetKind: claseDeActivo(h),
      // RN-5.6-A: si la plataforma puso las cifras, no se le piden al modelo.
      skipDatos: h.skipDatos === true,
    })),
  };

  // Sin activos que investigar —todos en caché— la tanda sigue haciendo falta
  // para el macro, pero el motor exige al menos un activo en `assets`. Se pide
  // el macro por su camino de siempre, que para eso sigue existiendo.
  const soloMacro = porInvestigar.length === 0;
  const arranque = Date.now();

  /** Si queda plazo para un segundo intento sin arriesgar el análisis entero. */
  const quedaPlazo = () => Date.now() - arranque < PRESUPUESTO_PARA_REINTENTO_MS;

  for (let intento = 0; intento <= REINTENTOS; intento++) {
    try {
      const salida = soloMacro
        ? await engineClient.investigar(userId, secret, { task: 'macro', language, exposure: exposicion })
        : await engineClient.investigar(userId, secret, peticion);

      const porClave = soloMacro
        ? new Map([[CLAVE_MACRO, findings.normalizarInvestigacion(CLAVE_MACRO, salida.text)]])
        : findings.normalizarInvestigacionLote(claves, salida.text);

      // Un JSON ilegible en el primer intento merece otro turno: el modelo a
      // veces envuelve la respuesta en prosa. En el último, se da por fallida.
      const todoFallido = [...porClave.values()]
        .every((inv) => inv.outcome === findings.RESEARCH_OUTCOME.FALLO);
      if (todoFallido && intento < REINTENTOS && quedaPlazo()) {
        console.warn('[copilotResearch] respuesta ilegible de la tanda, reintentando');
        continue;
      }

      // Las fuentes del *grounding* son de la llamada entera, así que respaldan
      // a todas sus claves por igual; el consumo también es de la llamada y se
      // devuelve aparte para no sumarlo una vez por activo.
      for (const inv of porClave.values()) inv.groundingSources = salida.sources || [];
      await guardarEnCache(porInvestigar, porClave, language);

      return { porClave, tokensUsed: salida.tokensUsed || 0 };
    } catch (error) {
      // La cuota agotada no se reintenta: el segundo intento la encontraría
      // igual de agotada y gastaría el plazo del análisis.
      const esCuota = error.code === 'ENGINE_QUOTA_EXHAUSTED';
      if (esCuota || intento === REINTENTOS || !quedaPlazo()) {
        console.error('[copilotResearch] la tanda no se pudo investigar:', error.message);
        return vacio;
      }
      console.warn(`[copilotResearch] ${error.message}. Reintentando la tanda`);
    }
  }

  return vacio;
}

/**
 * Guarda en caché lo que salió bien, activo por activo.
 *
 * El macro no se cachea: es del portafolio concreto que lo pidió, no un hecho
 * público sobre una empresa.
 *
 * @param {object[]} porInvestigar
 * @param {Map<string, object>} porClave
 * @param {string} language
 * @returns {Promise<void>}
 */
async function guardarEnCache(porInvestigar, porClave, language) {
  await Promise.all(porInvestigar.map((h) => {
    const investigacion = porClave.get(h.symbol);
    return investigacion ? cache.guardar(h.symbol, language, investigacion) : Promise.resolve();
  }));
}

module.exports = {
  REINTENTOS,
  CLAVE_MACRO,
  construirExposicion,
  conDatosDePlataforma,
  investigarTanda,
};
