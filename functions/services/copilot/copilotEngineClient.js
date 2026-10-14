/**
 * HU 5.3 — Hablar con el motor.
 *
 * El motor es **Gemini 2.5 Flash en capa gratuita**, el que el PO aprobó en la
 * HU 5.1, y sigue viviendo en la Lambda `portastock-lambda-ai-analyzer`. Lo que
 * cambia con 5.3 es que el análisis dejó de ser una llamada:
 *
 * - `investigar()` busca en internet y devuelve **también las fuentes** que
 *   respaldan lo que encontró;
 * - `redactar()` escribe el informe **sin buscar**, viendo sólo lo que la
 *   investigación entregó y el código validó.
 *
 * El modo `generate` de la HU 5.2 —una sola llamada que devolvía el análisis
 * entero— desapareció con esta historia: un análisis sin investigar ya no es un
 * análisis válido (RN-5-E), así que mantenerlo sería mantener una salida de
 * emergencia hacia algo que la historia prohíbe.
 *
 * @module services/copilot/copilotEngineClient
 * @see platform-docs/stories/5.3-analista-investiga-recomienda/refinamiento.md (T6)
 */

const { COPILOT_ANALYZER_URL } = require('../config');
const { generateToken } = require('../marketDataTokenService');
const { FAILURE_CODE } = require('./copilotConstants');

/**
 * Audiencia que sólo pueden firmar los backends.
 *
 * `getMarketDataToken` únicamente emite `finance-query-api`, así que un usuario
 * con sesión no puede fabricarse un token de esta audiencia y gastar la cuota
 * gratuita del motor.
 */
const INTERNAL_TOKEN_AUDIENCE = 'copilot-analyzer-internal';

/** Vida del token de servicio: lo justo para una llamada. */
const INTERNAL_TOKEN_TTL_SECONDS = 300;

/**
 * Lo que tarda una llamada. En la HU 5.1 se midieron 45 s de mediana sin
 * buscar; investigar añade la búsqueda.
 *
 * HU 5.6: desde que una sola llamada busca por todos los activos y el macro, la
 * Lambda se corta a los 300 s. Este margen sigue existiendo por lo mismo que
 * antes: que el error lo responda ella, con su código, y no un `AbortSignal`
 * ciego que sólo sabe decir "no respondió".
 */
const ENGINE_TIMEOUT_MS = 330000;

/**
 * Error del motor, con el código que el orquestador registra en el análisis
 * fallido. El usuario nunca ve estos códigos (Escenario 8 de la HU 5.2).
 */
class CopilotEngineError extends Error {
  /**
   * @param {string} code - Un valor de `FAILURE_CODE`.
   * @param {string} message
   */
  constructor(code, message) {
    super(message);
    this.name = 'CopilotEngineError';
    this.code = code;
  }
}

/**
 * Una llamada al motor.
 *
 * @param {string} userId
 * @param {string} secret - `MARKET_DATA_TOKEN_SECRET`.
 * @param {'research'|'compose'} mode
 * @param {object} peticion - La tarea, no el prompt: la Lambda lo construye
 *   ella (ahí vive el contenido del análisis, en un solo sitio).
 *   Investigar un activo: `{task:'asset', language, ticker, name, assetKind, hints}`.
 *   Investigar el macro: `{task:'macro', language, exposure}`.
 *   Redactar: `{language, context}`.
 *   Extraer la estructura (HU 5.4): `{task:'extract', language, markdown}`.
 * @returns {Promise<{text: string, tokensUsed: number, sources: object[], modelId: string}>}
 * @throws {CopilotEngineError} Si el motor no responde, devuelve error o agotó
 *   su cuota gratuita.
 */
async function llamar(userId, secret, mode, peticion) {
  if (!secret) {
    throw new CopilotEngineError(
      FAILURE_CODE.ENGINE_FAILED,
      'Falta MARKET_DATA_TOKEN_SECRET para firmar la llamada al motor',
    );
  }

  const { token } = generateToken(
    userId,
    secret,
    INTERNAL_TOKEN_TTL_SECONDS,
    INTERNAL_TOKEN_AUDIENCE,
  );

  let response;
  try {
    response = await fetch(COPILOT_ANALYZER_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-market-token': token,
        'x-copilot-mode': mode,
      },
      body: JSON.stringify(peticion),
      signal: AbortSignal.timeout(ENGINE_TIMEOUT_MS),
    });
  } catch (error) {
    throw new CopilotEngineError(
      FAILURE_CODE.ENGINE_FAILED,
      `El motor no respondió (${mode}): ${error.message}`,
    );
  }

  if (!response.ok) {
    const detalle = await leerDetalle(response);
    // 429 del proveedor = cuota gratuita agotada. Para el usuario es el mismo
    // desenlace que cualquier otro fallo, pero para el soporte no.
    const code = response.status === 429
      ? FAILURE_CODE.ENGINE_QUOTA_EXHAUSTED
      : FAILURE_CODE.ENGINE_FAILED;
    throw new CopilotEngineError(code, `El motor respondió ${response.status} (${mode}): ${detalle}`);
  }

  const resultado = await response.json();

  if (!resultado || typeof resultado.text !== 'string' || !resultado.text.trim()) {
    throw new CopilotEngineError(FAILURE_CODE.ENGINE_FAILED, `El motor respondió vacío (${mode})`);
  }

  return {
    text: resultado.text,
    tokensUsed: resultado.tokensUsed || 0,
    sources: resultado.sources || [],
    modelId: resultado.modelId || '',
  };
}

/**
 * Investiga: busca en internet y devuelve lo encontrado con sus fuentes.
 *
 * @param {string} userId
 * @param {string} secret
 * @param {object} peticion - `{task:'asset'|'macro', language, …}`; ver `llamar`.
 * @returns {Promise<{text: string, tokensUsed: number, sources: object[]}>}
 */
function investigar(userId, secret, peticion) {
  return llamar(userId, secret, 'research', peticion);
}

/**
 * Redacta el informe a partir de lo ya investigado. **No busca.**
 *
 * @param {string} userId
 * @param {string} secret
 * @param {object} peticion - `{language, context}` para el informe, o
 *   `{task:'extract', language, markdown}` para extraer su estructura.
 * @returns {Promise<{text: string, tokensUsed: number, modelId: string}>}
 */
function redactar(userId, secret, peticion) {
  return llamar(userId, secret, 'compose', peticion);
}

/**
 * El cuerpo del error del motor, acotado: nunca se le muestra al usuario, pero
 * va al log y un volcado entero no ayuda a nadie.
 *
 * @param {Response} response
 * @returns {Promise<string>}
 */
async function leerDetalle(response) {
  try {
    const texto = await response.text();
    return texto.slice(0, 300);
  } catch (error) {
    return 'sin cuerpo';
  }
}

module.exports = {
  INTERNAL_TOKEN_AUDIENCE,
  INTERNAL_TOKEN_TTL_SECONDS,
  ENGINE_TIMEOUT_MS,
  CopilotEngineError,
  investigar,
  redactar,
};
