/**
 * HU 6.10.1 — La limpieza diaria de datos vencidos.
 *
 * Corre **todos los días a las 06:00 ET**, fuera del horario del cierre
 * (00:05–00:45) y de la corrección (02:00–≈02:10) nocturnos (RN-6.10.1-J).
 * Diaria, decisión del PO del 2026-11-15:
 * - lo vencido nunca pasa más de un día de su plazo;
 * - una primera limpieza grande se reparte sola en varias mañanas, porque
 *   cada una se detiene a los 420 s y la siguiente continúa donde quedó.
 *
 * Un tipo que operación no ha activado solo se cuenta (ensayo en seco,
 * RN-6.10-O). La activación es `scripts/plazosConservacion.js --activar`.
 *
 * `maxInstances: 1`: dos limpiezas a la vez se repartirían las mismas tandas
 * y duplicarían la constancia. `retryCount: 0`: reintentar no aporta, la
 * ejecución del día siguiente retoma lo que falte.
 *
 * @module services/retention/retentionScheduled
 * @see platform-docs/stories/6.10.1-plazos-conservacion-datos/refinamiento.md (T3)
 */

const { onSchedule } = require('firebase-functions/v2/scheduler');
const admin = require('../firebaseAdmin');
const { medirAlmacenamiento } = require('../consumption/consumptionMetricsService');
const { ejecutarLimpieza } = require('./retentionService');

const FUNCTION_CONFIG = {
  schedule: '0 6 * * *',
  timeZone: 'America/New_York',
  timeoutSeconds: 540,
  memory: '512MiB',
  maxInstances: 1,
  minInstances: 0,
  retryCount: 0,
  labels: { component: 'hu-6-10-1', purpose: 'retention' },
};

/**
 * El almacenamiento que la plataforma mide ahora mismo (RN-6-I): el último
 * punto de `data_and_index_storage_bytes`, con la credencial dada.
 *
 * @param {Object} params
 * @param {Function} params.obtenerToken - Devuelve un token de Cloud Monitoring
 * @param {Function} [params.ahora]
 * @returns {Function} `() => Promise<number|null>`
 */
function crearMedidorDeEspacio({ obtenerToken, ahora = () => new Date() }) {
  return async () => {
    const token = await obtenerToken();
    const puntos = await medirAlmacenamiento({ token, hasta: ahora() });
    return puntos.length > 0 ? puntos[puntos.length - 1].valor : null;
  };
}

/**
 * Token de Monitoring con la credencial del runtime (ADC), como
 * `consumptionWatchScheduled.js`.
 *
 * @returns {Promise<string>}
 */
async function tokenDelRuntime() {
  const { access_token: token } = await admin.credential.applicationDefault().getAccessToken();
  return token;
}

const scheduledDataRetention = onSchedule(FUNCTION_CONFIG, async () => {
  const db = admin.firestore();
  await ejecutarLimpieza(db, {
    medirEspacio: crearMedidorDeEspacio({ obtenerToken: tokenDelRuntime }),
    origen: 'programada',
  });
});

module.exports = {
  scheduledDataRetention,
  crearMedidorDeEspacio,
  tokenDelRuntime,
  FUNCTION_CONFIG,
};
