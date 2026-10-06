/**
 * alertRetention.js
 *
 * BUG-ALERT-002: Retención de los históricos del sistema de alertas.
 *
 * `userNotifications` y `alertHistory` sólo crecen: el evaluador inserta un
 * documento en cada una por disparo y nada los borra nunca. El panel de
 * notificaciones muestra como mucho los 20 más recientes, así que todo lo
 * anterior al periodo de retención es coste de almacenamiento sin lector.
 *
 * Dos vectores:
 *   Vector 1: purgar userNotifications con más de NOTIFICATION_RETENTION_DAYS
 *   Vector 2: purgar alertHistory con más de HISTORY_RETENTION_DAYS
 *
 * `alertHistory` es la traza de auditoría (qué se notificó y por qué canal),
 * así que se conserva más tiempo que las notificaciones de la campana.
 *
 * Ambas consultas filtran por un único campo de fecha, que Firestore indexa
 * automáticamente: no hacen falta índices compuestos.
 *
 * @see docs/architecture/FEAT-ALERT-001-price-alerts-system-design.md
 */

const { onSchedule } = require('firebase-functions/v2/scheduler');
const { getFirestore } = require('firebase-admin/firestore');
// Inicializa la app de admin antes de pedir el cliente de Firestore.
require('./firebaseAdmin');

const db = getFirestore();

// ============================================================================
// CONSTANTS
// ============================================================================

const NOTIFICATION_RETENTION_DAYS = 90;
const HISTORY_RETENTION_DAYS = 365;
const FIRESTORE_BATCH_LIMIT = 500;
/** Tope de documentos por ejecución, para no agotar el timeout de 540s. */
const MAX_DELETES_PER_RUN = 10000;

// ============================================================================
// CORE
// ============================================================================

/**
 * Borra en lotes los documentos de `collection` cuyo `dateField` es anterior
 * al corte.
 *
 * Nota: si algún documento antiguo guardó la fecha como cadena en vez de
 * Timestamp, la comparación con un Date no lo alcanza (Firestore ordena
 * primero por tipo). Es el lado seguro del fallo: se queda sin purgar, nunca
 * se borra de más.
 *
 * @param {FirebaseFirestore.Firestore} firestore
 * @param {string} collection
 * @param {string} dateField
 * @param {number} retentionDays
 * @returns {Promise<{deleted: number, truncated: boolean}>}
 */
async function purgeOlderThan(firestore, collection, dateField, retentionDays) {
  const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000);
  let deleted = 0;

  for (;;) {
    if (deleted >= MAX_DELETES_PER_RUN) {
      console.warn(
        `[alertRetention] ${collection}: alcanzado el tope de ${MAX_DELETES_PER_RUN} borrados, ` +
        'el resto se purgará en la siguiente ejecución'
      );
      return { deleted, truncated: true };
    }

    const snapshot = await firestore
      .collection(collection)
      .where(dateField, '<', cutoff)
      .limit(FIRESTORE_BATCH_LIMIT)
      .get();

    if (snapshot.empty) break;

    const batch = firestore.batch();
    snapshot.docs.forEach((doc) => batch.delete(doc.ref));
    await batch.commit();
    deleted += snapshot.size;

    // Última página: el lote no venía lleno.
    if (snapshot.size < FIRESTORE_BATCH_LIMIT) break;
  }

  console.log(
    `[alertRetention] ${collection}: ${deleted} documentos anteriores a ` +
    `${cutoff.toISOString()} (retención ${retentionDays}d)`
  );
  return { deleted, truncated: false };
}

/**
 * @param {FirebaseFirestore.Firestore} firestore
 * @returns {Promise<{notifications: number, history: number}>}
 */
async function purgeAlertHistory(firestore) {
  const notifications = await purgeOlderThan(
    firestore, 'userNotifications', 'createdAt', NOTIFICATION_RETENTION_DAYS
  );
  const history = await purgeOlderThan(
    firestore, 'alertHistory', 'triggeredAt', HISTORY_RETENTION_DAYS
  );

  return { notifications: notifications.deleted, history: history.deleted };
}

// ============================================================================
// SCHEDULED FUNCTION
// ============================================================================

const scheduledAlertRetention = onSchedule({
  // Semanal, domingo 05:00 ET — después de scheduledSnapshotCleanup (03:00).
  schedule: '0 5 * * 0',
  timeZone: 'America/New_York',
  timeoutSeconds: 540,
  memory: '256MiB',
  retryCount: 1,
  labels: { component: 'feat-alert', purpose: 'retention' },
}, async () => {
  const result = await purgeAlertHistory(db);
  console.log(
    `[alertRetention] Purga completada: ${result.notifications} notificaciones, ` +
    `${result.history} entradas de historial`
  );
});

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  purgeOlderThan,
  purgeAlertHistory,
  scheduledAlertRetention,
  NOTIFICATION_RETENTION_DAYS,
  HISTORY_RETENTION_DAYS,
  FIRESTORE_BATCH_LIMIT,
  MAX_DELETES_PER_RUN,
};
