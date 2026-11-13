/**
 * HU 7 — Cada inversor solo consulta sus propias cifras.
 *
 * **Por qué existe.** La function pública `app` respondía la atribución, el
 * riesgo y las posiciones cerradas de **cualquier** inversor con solo poner su
 * `userId` en la URL (verificado el 2026-11-12: 200 sin credenciales). Este
 * guardia decide **de quién son las cifras** que una petición puede recibir, y
 * lo decide la credencial, nunca la URL.
 *
 * **Dos identidades, y nada más:**
 *
 * - **Persona** (`Authorization: Bearer <ID token de Firebase>`): solo ve lo de
 *   su propio uid (RN-7-A). Sin `userId` en la petición, se responden sus cifras;
 *   con un `userId` distinto se rechaza — nunca se sustituye por el propio. No hay
 *   excepción por rol: el claim `admin` no abre nada (RN-7-B).
 * - **Sistema** (`x-service-token` = secreto `CF_SERVICE_TOKEN`): Cloud
 *   Functions, Lambdas y demás procesos de la plataforma consultan a **cualquier**
 *   inversor, diciendo siempre de cuál (RN-7-I, decisión del PO del 2026-11-12).
 *
 * Todo rechazo responde lo mismo — 401 «No autorizado» — sea cual sea el motivo
 * (RN-7-H), y sale **antes** de tocar Firestore (RN-7-C): verificar el token es
 * criptografía local. El motivo solo va al registro, que es una línea de Cloud
 * Logging: ni lecturas ni escrituras, para que nadie pueda convertir rechazos en
 * factura (RN-7-D). De ese registro sale el aviso a operación
 * (`scripts/investorAccessAlerts.js`).
 *
 * El módulo no lee el entorno ni inicializa nada por sí solo: `crearGuardiaInversor`
 * recibe sus dependencias y es lo que prueban los tests. `httpApi.js` lo compone
 * con `DEPENDENCIAS_PRODUCCION` y con la lista de rutas abiertas
 * (`config/publicHttpRoutes.js`).
 *
 * @module utils/investorSession
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/refinamiento.md (T1, T2)
 */

const crypto = require('crypto');

// ============================================================================
// CONSTANTES
// ============================================================================

/** El único mensaje con el que se rechaza, sea cual sea el motivo (RN-7-H). */
const MENSAJE_RECHAZO = 'No autorizado';

/** Cuerpo de todo rechazo. Mismo código y mismo cuerpo para todos los motivos (RN-7-H). */
const RESPUESTA_RECHAZO = Object.freeze({
  success: false,
  error: 'UNAUTHORIZED',
  message: MENSAJE_RECHAZO,
});

/** Evento del registro del que cuelga la métrica de los avisos. No cambiar sin cambiar el aviso. */
const EVENTO_RECHAZO = 'investor_data_access_denied';

/** Evento de auditoría de cada consulta hecha por el sistema (RN-7-I). Sin aviso. */
const EVENTO_ACCESO_SERVICIO = 'investor_data_service_access';

/** Header con el que el sistema se identifica. Es el mismo que ya usa contra finance-query. */
const HEADER_SERVICIO = 'x-service-token';

/**
 * Motivos internos del rechazo. **Solo van al registro**; la respuesta nunca los
 * dice (RN-7-H).
 */
const MOTIVOS = Object.freeze({
  SIN_SESION: 'sin_sesion',
  SESION_INVALIDA: 'sesion_invalida',
  CIFRAS_AJENAS: 'cifras_ajenas',
  SERVICIO_INVALIDO: 'servicio_invalido',
});

/**
 * Tope de lo que se copia al registro desde la petición. El `userId` y los
 * headers los escribe quien llama: sin tope, un atacante decide el tamaño de
 * cada línea del registro.
 */
const MAX_LARGO_CAMPO = 128;

// ============================================================================
// FUNCIONES PURAS
// ============================================================================

/**
 * Recorta un valor externo para el registro.
 *
 * @param {*} valor
 * @returns {string|null} El valor como texto, recortado; `null` si no viene.
 */
function recortar(valor) {
  if (valor === undefined || valor === null || valor === '') return null;
  const texto = Array.isArray(valor) ? valor.join(',') : String(valor);
  return texto.length > MAX_LARGO_CAMPO ? texto.slice(0, MAX_LARGO_CAMPO) : texto;
}

/**
 * Compara dos secretos en tiempo constante, también cuando tienen distinto
 * largo: se comparan sus resúmenes SHA-256, que siempre miden lo mismo.
 *
 * @param {string} recibido
 * @param {string} esperado
 * @returns {boolean} `false` si cualquiera de los dos está vacío.
 */
function secretosIguales(recibido, esperado) {
  if (typeof recibido !== 'string' || typeof esperado !== 'string') return false;
  if (!recibido || !esperado) return false;
  const a = crypto.createHash('sha256').update(recibido).digest();
  const b = crypto.createHash('sha256').update(esperado).digest();
  return crypto.timingSafeEqual(a, b);
}

/**
 * Extrae el ID token de `Authorization: Bearer <token>`.
 *
 * @param {string|undefined} header
 * @returns {string|null}
 */
function extraerBearer(header) {
  if (typeof header !== 'string') return null;
  const coincidencia = header.match(/^Bearer\s+(\S+)\s*$/i);
  return coincidencia ? coincidencia[1] : null;
}

/**
 * Lee el `userId` que trae la petición.
 *
 * Un `userId` repetido (`?userId=a&userId=b`) llega como arreglo: no es un
 * inversor, y se trata como uno que no coincide con nadie.
 *
 * @param {object} query - `req.query`
 * @returns {{ presente: boolean, valor: string|null, valido: boolean }}
 */
function leerUserIdSolicitado(query) {
  const crudo = query ? query.userId : undefined;
  if (crudo === undefined || crudo === '') return { presente: false, valor: null, valido: true };
  if (typeof crudo !== 'string') return { presente: true, valor: recortar(crudo), valido: false };
  return { presente: true, valor: crudo, valido: true };
}

/**
 * Dirección de origen de la petición.
 *
 * En Cloud Run, el balanceador de Google **añade** la IP real del cliente al
 * final de `X-Forwarded-For`. Lo que haya antes lo escribió el propio cliente y
 * no vale como origen: tomar el primero (lo que hace `req.ip` con `trust proxy`)
 * dejaría a un atacante cambiar de «origen» en cada intento y esquivar el aviso
 * por origen del Escenario 6.
 *
 * @param {object} req
 * @returns {string|null}
 */
function origenDe(req) {
  const xff = req.headers ? req.headers['x-forwarded-for'] : undefined;
  if (typeof xff === 'string' && xff.trim()) {
    const partes = xff.split(',').map((p) => p.trim()).filter(Boolean);
    if (partes.length) return recortar(partes[partes.length - 1]);
  }
  const remota = req.socket ? req.socket.remoteAddress : undefined;
  return recortar(remota || req.ip);
}

/**
 * Construye la línea del registro. Una sola línea JSON por evento: Cloud
 * Logging la guarda como `jsonPayload` y la métrica del aviso extrae de ahí
 * `origin` y `targetUserId`.
 *
 * @param {object} campos
 * @returns {object}
 */
function lineaRegistro({ severity, message, event, req, actorUid, targetUserId, reason, ahora }) {
  return {
    severity,
    message,
    event,
    actorUid: actorUid || null,
    targetUserId: recortar(targetUserId),
    origin: origenDe(req),
    forwardedFor: recortar(req.headers ? req.headers['x-forwarded-for'] : null),
    route: recortar(req.path),
    method: req.method,
    ...(reason ? { reason } : {}),
    at: ahora.toISOString(),
  };
}

// ============================================================================
// GUARDIA
// ============================================================================

/**
 * Crea el middleware de Express que decide de quién son las cifras.
 *
 * Si deja pasar, fija:
 * - `req.investorUid` — el inversor cuyas cifras se responden. **Las rutas leen
 *   esto, nunca `req.query.userId`.**
 * - `req.investorAccess` — `{ tipo: 'persona'|'servicio', actorUid }`.
 *
 * @param {object} deps
 * @param {(token: string) => Promise<{uid: string}>} deps.verificarIdToken - Verifica un ID token de Firebase.
 * @param {() => string} deps.obtenerTokenServicio - Devuelve el secreto `CF_SERVICE_TOKEN` vigente.
 * @param {(linea: object) => void} [deps.registrar] - Escribe una línea del registro.
 * @param {(req: object) => boolean} [deps.esRutaRegistrada] - ¿La petición va a una ruta que
 *   existe? Las que no existen se rechazan igual, pero **no se registran**: los
 *   barridos de bots por rutas al azar (`/wp-login.php`…) no son intentos de leer
 *   cifras y dispararían el aviso por origen. Si esta comprobación fallara, lo que
 *   se pierde es el registro, nunca la protección.
 * @param {() => Date} [deps.reloj]
 * @returns {Function} Middleware `(req, res, next)` asíncrono.
 */
function crearGuardiaInversor({
  verificarIdToken,
  obtenerTokenServicio,
  registrar = registrarEnConsola,
  esRutaRegistrada = () => true,
  reloj = () => new Date(),
}) {
  /**
   * Rechaza con la respuesta única y deja la línea del registro.
   */
  function rechazar(req, res, { motivo, actorUid, targetUserId }) {
    if (esRutaRegistrada(req)) registrar(lineaRegistro({
      severity: 'WARNING',
      message: '[investorSession] Acceso denegado a cifras de un inversor',
      event: EVENTO_RECHAZO,
      req,
      actorUid,
      targetUserId,
      reason: motivo,
      ahora: reloj(),
    }));
    return res.status(401).json(RESPUESTA_RECHAZO);
  }

  return async function guardiaInversor(req, res, next) {
    const solicitado = leerUserIdSolicitado(req.query);
    const headerServicio = req.headers ? req.headers[HEADER_SERVICIO] : undefined;

    // ── Sistema (RN-7-I) ──────────────────────────────────────────────────
    // Si se presenta como servicio, se juzga como servicio: un token de servicio
    // falso no cae a la vía de persona, se rechaza.
    if (headerServicio !== undefined) {
      if (!secretosIguales(String(headerServicio).trim(), obtenerTokenServicio())) {
        return rechazar(req, res, {
          motivo: MOTIVOS.SERVICIO_INVALIDO,
          actorUid: null,
          targetUserId: solicitado.valor,
        });
      }
      if (!solicitado.presente || !solicitado.valido) {
        // El sistema siempre dice de quién pide. No es un rechazo de acceso:
        // es una petición mal formada de un llamador legítimo.
        return res.status(400).json({
          success: false,
          error: 'MISSING_PARAM',
          message: 'Una consulta del sistema debe indicar el parámetro userId',
        });
      }
      registrar(lineaRegistro({
        severity: 'INFO',
        message: '[investorSession] Consulta del sistema a cifras de un inversor',
        event: EVENTO_ACCESO_SERVICIO,
        req,
        actorUid: null,
        targetUserId: solicitado.valor,
        ahora: reloj(),
      }));
      req.investorUid = solicitado.valor;
      req.investorAccess = { tipo: 'servicio', actorUid: null };
      return next();
    }

    // ── Persona ───────────────────────────────────────────────────────────
    const token = extraerBearer(req.headers ? req.headers.authorization : undefined);
    if (!token) {
      return rechazar(req, res, {
        motivo: MOTIVOS.SIN_SESION,
        actorUid: null,
        targetUserId: solicitado.valor,
      });
    }

    let uid;
    try {
      const decodificado = await verificarIdToken(token);
      uid = decodificado && decodificado.uid;
    } catch (error) {
      uid = null;
    }
    if (!uid) {
      return rechazar(req, res, {
        motivo: MOTIVOS.SESION_INVALIDA,
        actorUid: null,
        targetUserId: solicitado.valor,
      });
    }

    // RN-7-A: el identificador de la petición nunca decide de quién son los
    // datos. Si no coincide, se rechaza; no se responde con las cifras propias.
    // RN-7-B: no se mira ningún rol ni claim.
    if (solicitado.presente && (!solicitado.valido || solicitado.valor !== uid)) {
      return rechazar(req, res, {
        motivo: MOTIVOS.CIFRAS_AJENAS,
        actorUid: uid,
        targetUserId: solicitado.valor,
      });
    }

    req.investorUid = uid;
    req.investorAccess = { tipo: 'persona', actorUid: uid };
    return next();
  };
}

/**
 * Escribe la línea como JSON en una sola línea: en Cloud Run (gen2) eso la
 * convierte en `jsonPayload` con la severidad indicada.
 *
 * @param {object} linea
 */
function registrarEnConsola(linea) {
  const texto = JSON.stringify(linea);
  if (linea.severity === 'WARNING') console.warn(texto);
  else console.log(texto);
}

// ============================================================================
// INSTANCIA DE PRODUCCIÓN
// ============================================================================

/**
 * Dependencias de producción: el Admin SDK y el secreto de servicio.
 *
 * Se resuelven en cada petición, no al cargar el módulo: en Functions v2 los
 * secretos llegan a `process.env` después de cargar los módulos
 * (FIX-SECRET-001, `services/config.js`), y así los tests no inicializan Firebase.
 */
const DEPENDENCIAS_PRODUCCION = Object.freeze({
  verificarIdToken: (token) => require('../services/firebaseAdmin').auth().verifyIdToken(token),
  obtenerTokenServicio: () => require('../services/config').getServiceTokenSecret(),
});

module.exports = {
  crearGuardiaInversor,
  DEPENDENCIAS_PRODUCCION,
  // Para tests y para el aviso
  MENSAJE_RECHAZO,
  RESPUESTA_RECHAZO,
  EVENTO_RECHAZO,
  EVENTO_ACCESO_SERVICIO,
  HEADER_SERVICIO,
  MOTIVOS,
  secretosIguales,
  origenDe,
  leerUserIdSolicitado,
};
