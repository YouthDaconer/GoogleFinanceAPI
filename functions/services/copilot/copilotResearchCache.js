/**
 * La investigación de un activo, reutilizable durante unas horas.
 *
 * Cada búsqueda con *grounding* se factura aparte (~$0.014) y es el grueso del
 * coste de un análisis. Investigar MSFT dos veces el mismo día —porque el
 * usuario relanza, o porque otro usuario también lo tiene— es pagar dos veces
 * por la misma respuesta.
 *
 * El caché es **por activo e idioma, no por usuario**: lo que se guarda son
 * hechos públicos sobre una empresa, no nada del portafolio de nadie. Por eso
 * se puede compartir entre usuarios sin filtrar información.
 *
 * @module services/copilot/copilotResearchCache
 */

const admin = require('../firebaseAdmin');

/** Colección raíz del caché. El id del documento es `TICKER_idioma`. */
const COLECCION = 'copilotResearchCache';

/**
 * Cuánto vale una investigación antes de repetirla.
 *
 * Ocho horas: los fundamentales de una empresa no cambian en una jornada —un
 * trimestre se publica cada tres meses—, y lo que sí se mueve a diario (el
 * precio, el peso en la cartera) no sale de aquí, sino del portafolio depurado.
 * Es el plazo que hace que relanzar un análisis el mismo día no cueste casi
 * nada sin llegar a servir noticias de ayer.
 */
const HORAS_DE_VIDA = 8;

/**
 * Lo que se guarda de una investigación.
 *
 * Sólo lo que el informe consume. `tokensUsed` queda fuera a propósito: lo que
 * costó la primera vez no es un dato de la segunda, y sumarlo otra vez
 * falsearía el consumo que se le atribuye al análisis.
 *
 * @param {object} investigacion
 * @returns {object}
 */
function paraGuardar(investigacion) {
  return {
    outcome: investigacion.outcome,
    findings: investigacion.findings || [],
    notes: investigacion.notes || [],
    conflicts: investigacion.conflicts || [],
    groundingSources: investigacion.groundingSources || [],
  };
}

/**
 * El id del documento.
 *
 * Lleva el idioma porque los hallazgos vienen redactados: reutilizar los de un
 * análisis en español para uno en inglés metería textos en el idioma
 * equivocado dentro del informe.
 *
 * @param {string} ticker
 * @param {string} language
 * @returns {string}
 */
function clave(ticker, language) {
  return `${String(ticker).toUpperCase()}_${language === 'en' ? 'en' : 'es'}`;
}

/**
 * La investigación guardada, si sigue vigente.
 *
 * Nunca lanza: un caché que falla es un caché que no acierta, no un análisis
 * roto (RN-5-C).
 *
 * @param {string} ticker
 * @param {string} language
 * @param {Date} [now]
 * @returns {Promise<object|null>}
 */
async function leer(ticker, language, now = new Date()) {
  try {
    const doc = await admin.firestore().collection(COLECCION).doc(clave(ticker, language)).get();
    if (!doc.exists) return null;

    const datos = doc.data();
    const guardadoEn = Date.parse(datos.at);
    if (Number.isNaN(guardadoEn)) return null;

    const horas = (now.getTime() - guardadoEn) / (1000 * 60 * 60);
    if (horas >= HORAS_DE_VIDA) return null;

    return { ...datos.research, ticker, tokensUsed: 0, desdeCache: true };
  } catch (error) {
    console.error(`[copilotResearchCache] No se pudo leer ${ticker}:`, error.message);
    return null;
  }
}

/**
 * Guarda una investigación para las próximas horas.
 *
 * **Sólo las que salieron bien.** Guardar un fallo lo serviría durante ocho
 * horas a todo el mundo, convirtiendo un tropiezo puntual en una avería larga.
 *
 * @param {string} ticker
 * @param {string} language
 * @param {object} investigacion
 * @param {Date} [now]
 * @returns {Promise<void>}
 */
async function guardar(ticker, language, investigacion, now = new Date()) {
  if (!investigacion || investigacion.outcome === 'fallo') return;
  if (!(investigacion.findings || []).length) return;

  try {
    await admin.firestore().collection(COLECCION).doc(clave(ticker, language)).set({
      at: now.toISOString(),
      ticker: String(ticker).toUpperCase(),
      language,
      research: paraGuardar(investigacion),
    });
  } catch (error) {
    console.error(`[copilotResearchCache] No se pudo guardar ${ticker}:`, error.message);
  }
}

module.exports = {
  COLECCION,
  HORAS_DE_VIDA,
  clave,
  paraGuardar,
  leer,
  guardar,
};
