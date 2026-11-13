/**
 * reconcileStalePerformance.js
 *
 * Cloud Function scheduled that recalculates portfolioPerformance
 * for users with stale data markers.
 *
 * Schedule: 02:00 ET, Tuesday through Saturday
 *
 * HU 6.8.1: la corrección avanza por tramos de `DIAS_POR_TRAMO` días hábiles,
 * desde el más antiguo, y recuerda dónde quedó en `_stale.since` (el primer
 * día aún sin corregir). Un rango largo ya no se recorta ni agota la noche:
 * toma varias noches y avanza en cada una.
 *
 * @see LATE-REG-003
 * @see docs/architecture/LATE-REGISTRATION-001-retroactive-transactions-analysis.md
 * @see platform-docs/stories/6.8.1-historial-corregido-por-tramos/historia.md
 */

const { onSchedule } = require('firebase-functions/v2/scheduler');
const { defineSecret } = require('firebase-functions/params');
const { getFirestore, FieldValue, FieldPath } = require('firebase-admin/firestore');
const { DateTime } = require('luxon');
const {
  generateTradingDays,
  backfillUserPerformance,
  clearHistoricalPriceCache,
} = require('./backfillCoreModule');
const { consolidateMonth, consolidateYear } = require('./periodConsolidationScheduled');
const { invalidatePerformanceCache } = require('./cacheInvalidationService');
const { deleteInSmallBatches } = require('../utils/firestoreDeletes');

const db = getFirestore();

/**
 * SEC-TOKEN-001: precios y tasas vienen del API de mercado, que exige
 * `x-service-token`. Sin el binding la función corría con el token vacío:
 * todo respondía 401, ningún día se podía valorar y la reconciliación "salía
 * bien" con 0 días escritos (DDeR8P, 2026-11-06 y 2026-11-10).
 */
const cfServiceToken = defineSecret('CF_SERVICE_TOKEN');

// ============================================================================
// LIMITS AND CONFIGURATION
// ============================================================================

const LIMITS = {
  /** Usuarios por consulta; se siguen pidiendo mientras quede tiempo */
  USERS_PER_PAGE: 20,

  /**
   * RN-6.8.1-K: días hábiles por tramo, un trimestre. Un tramo tiene que caber
   * en el margen entre TIME_BUDGET_MS y el timeout (120 s): medido el
   * 2026-11-12, ≈1,5 s por día con todo incluido.
   */
  DIAS_POR_TRAMO: 63,

  /** Maximum retry attempts before skipping user */
  MAX_RETRY_COUNT: 3,

  /**
   * No empezar otro tramo pasado este tiempo: deja margen al timeout de la
   * función para terminar el que está en curso y propagarlo.
   */
  TIME_BUDGET_MS: 420 * 1000,
};

// ============================================================================
// HELPERS
// ============================================================================

/**
 * Último día que ya cerró el EOD: ayer en Nueva York. La corrida es a las
 * 02:00 ET y el día en curso no tiene cierre todavía; recalcularlo escribía un
 * doc con el precio del día anterior que el EOD tenía que pisar después.
 */
function getLastClosedDate(now = DateTime.now().setZone('America/New_York')) {
  return now.minus({ days: 1 }).toISODate();
}

/**
 * Meses (YYYY-MM) ya cerrados entre `firstDay` y el mes anterior al actual.
 * Sus consolidados se generaron una sola vez y el V2 de rendimientos los lee
 * en lugar de los docs diarios, así que hay que rehacerlos.
 */
function closedMonthsSince(firstDay, now = DateTime.now().setZone('America/New_York')) {
  const currentMonth = now.toFormat('yyyy-MM');
  const months = [];
  let month = DateTime.fromISO(firstDay).startOf('month');
  while (month.toFormat('yyyy-MM') < currentMonth) {
    months.push(month.toFormat('yyyy-MM'));
    month = month.plus({ months: 1 });
  }
  return months;
}

/** Día de calendario siguiente a `date` (YYYY-MM-DD), en Nueva York. */
function nextCalendarDay(date) {
  return DateTime.fromISO(date, { zone: 'America/New_York' }).plus({ days: 1 }).toISODate();
}

/**
 * Hace llegar el recálculo a todo lo que se deriva de los docs diarios:
 * consolidados de meses y años cerrados, snapshots, caché de rendimientos y la
 * señal que invalida las cachés del cliente. Sin esto los docs quedaban bien
 * pero el usuario seguía viendo los números viejos.
 *
 * HU 6.8.1: con `lastDay` solo rehace los meses y años cerrados que tocan
 * `[firstDay, lastDay]` (el tramo). El mes de frontera se vuelve a rehacer con
 * el tramo siguiente. Sin `lastDay`, hasta el último mes cerrado.
 *
 * @param {string} userId
 * @param {string} firstDay - YYYY-MM-DD
 * @param {string[]} accountIds
 * @param {string|null} [lastDay] - YYYY-MM-DD
 * @param {DateTime} [now]
 */
async function propagateReconciledRange(
  userId,
  firstDay,
  accountIds,
  lastDay = null,
  now = DateTime.now().setZone('America/New_York'),
) {
  const scopes = [null, ...accountIds];
  const lastMonth = lastDay ? lastDay.slice(0, 7) : null;
  const months = closedMonthsSince(firstDay, now).filter((m) => !lastMonth || m <= lastMonth);
  const closedYears = [...new Set(months.map((m) => m.slice(0, 4)))].filter((y) => Number(y) < now.year);

  for (const periodKey of months) {
    for (const accountId of scopes) {
      await consolidateMonth(userId, accountId, periodKey);
    }
  }
  for (const yearKey of closedYears) {
    for (const accountId of scopes) {
      await consolidateYear(userId, accountId, yearKey);
    }
  }

  // Snapshots del usuario (portafolio, cuentas y activos): se regeneran al
  // leerlos o en el próximo EOD. Actualizarlos en sitio no sirve, el
  // incremental del EOD sólo añade el último día sobre la historia vieja.
  // Solo las referencias (`select()`): ahora se borran en cada tramo y son
  // docs grandes.
  const snapshots = db.collection('performanceSnapshots');
  const prefix = `${userId}_`;
  const snapshotDocs = await snapshots
    .where(FieldPath.documentId(), '>=', snapshots.doc(prefix))
    .where(FieldPath.documentId(), '<', snapshots.doc(`${prefix}`))
    .select()
    .get();
  // En lotes pequeños: un batch de 400 snapshots grandes falla con
  // "Transaction too big" (DDeR8P, 261 snapshots, 2026-11-05 en adelante).
  await deleteInSmallBatches(db, snapshotDocs.docs.map((d) => d.ref));

  await invalidatePerformanceCache(userId);

  const signal = { lastSnapshotUpdate: new Date().toISOString() };
  await db.collection('portfolioPerformance').doc(userId).set(signal, { merge: true });
  await db.collection('userData').doc(userId).set(signal, { merge: true });

  console.log(`[reconcile] User ${userId}: propagado — ${months.length} meses, ${closedYears.length} años, ` +
    `${snapshotDocs.size} snapshots borrados`);
}

/** RN-6-I: una línea por tramo, lo que se observa en los logs de cada noche. */
function logTramo(userId, { desde, hasta, days, ms, status, detail, since }) {
  const rango = desde ? `${desde}→${hasta} (${days} días, ${ms} ms)` : `sin días pendientes desde ${since}`;
  const line = `[reconcile] TRAMO ${userId} ${rango}: ${status} — ${detail}`;
  if (status === 'failed') console.error(line);
  else console.log(line);
}

/**
 * Deja constancia de un tramo fallido en la marca. RN-6.8.1-L: si el inversor
 * ya avanzó esta noche, el fallo no suma intento.
 */
async function registrarFallo(ref, lastError, { yaAvanzo, now }) {
  const update = {
    '_stale.lastError': lastError,
    '_stale.lastAttempt': new Date(now()).toISOString(),
  };
  if (!yaAvanzo) update['_stale.retryCount'] = FieldValue.increment(1);
  await ref.update(update);
}

/**
 * Corrige UN tramo de un inversor marcado: los primeros `DIAS_POR_TRAMO` días
 * hábiles desde `_stale.since`, sin recortar nada (RN-6.8.1-K, RN-6.8-K).
 *
 * Avanzar (RN-6.8.1-L) es completar el tramo entero (calculado, validado,
 * escrito y propagado) y mover `since` al día siguiente a su último día, en la
 * transacción que comprueba que nadie movió la marca. Con avance,
 * `retryCount` vuelve a 0. Un fallo en una noche en la que el inversor ya
 * avanzó no suma intento.
 *
 * Si otra operación movió la marca durante el tramo, no se avanza: se sigue
 * desde la fecha más antigua afectada (RN-6.8.1-O).
 *
 * @param {FirebaseFirestore.DocumentSnapshot} doc - portfolioPerformance/{uid}
 * @param {Object} [options]
 * @param {boolean} [options.yaAvanzo=false] - El inversor ya avanzó esta noche
 * @param {string} [options.lastClosedDate] - Último día cerrado (YYYY-MM-DD)
 * @param {Function} [options.now=Date.now] - Reloj en ms
 * @returns {Promise<{status: 'reconciled'|'advanced'|'moved'|'failed', days: number,
 *   desde?: string, hasta?: string, errors?: string[]}>}
 */
async function corregirTramo(doc, { yaAvanzo = false, lastClosedDate = getLastClosedDate(), now = Date.now } = {}) {
  const userId = doc.id;
  const stale = doc.data()._stale;
  const startedAt = now();
  const isoNow = () => new Date(now()).toISOString();

  const pendientes = await generateTradingDays(stale.since, lastClosedDate);
  const tramo = pendientes.slice(0, LIMITS.DIAS_POR_TRAMO);
  const desde = tramo[0];
  const hasta = tramo[tramo.length - 1];
  const esUltimo = tramo.length === pendientes.length;

  let result = { success: true, daysProcessed: 0, errors: [] };
  if (tramo.length > 0) {
    result = await backfillUserPerformance(userId, tramo, { rangeStart: stale.since });
  }

  if (!result.success) {
    // Los 3 primeros errores bastan para diagnosticar
    await registrarFallo(doc.ref, result.errors.slice(0, 3).join('; '), { yaAvanzo, now });
    logTramo(userId, {
      desde, hasta, days: tramo.length, ms: now() - startedAt, status: 'failed',
      detail: `${result.errors[0] || 'sin detalle'}${yaAvanzo ? ' (ya avanzó esta noche: no suma intento)' : ''}`,
    });
    return { status: 'failed', days: 0, desde, hasta, errors: result.errors };
  }

  if (tramo.length > 0) {
    const accountIds = (await db.collection('portfolioAccounts').where('userId', '==', userId).get())
      .docs.map((d) => d.id);
    await propagateReconciledRange(userId, desde, accountIds, hasta,
      DateTime.fromMillis(now(), { zone: 'America/New_York' }));
  }

  const siguiente = esUltimo ? null : nextCalendarDay(hasta);
  const { status, currentSince } = await db.runTransaction(async (tx) => {
    const fresh = await tx.get(doc.ref);
    const current = fresh.data()?._stale;
    if (!current || current.registeredAt !== stale.registeredAt || current.since !== stale.since) {
      return { status: 'moved', currentSince: current?.since || null };
    }
    if (esUltimo) {
      tx.update(doc.ref, { _stale: FieldValue.delete() });
      return { status: 'reconciled' };
    }
    const at = isoNow();
    tx.update(doc.ref, {
      '_stale.since': siguiente,
      '_stale.retryCount': 0,
      '_stale.lastError': FieldValue.delete(),
      '_stale.lastAttempt': at,
      '_stale.lastAdvance': at,
    });
    return { status: 'advanced' };
  });

  const detail = {
    reconciled: 'marca retirada',
    advanced: `queda desde ${siguiente}`,
    moved: `la marca cambió durante el tramo; sigue desde ${currentSince || 'sin marca'}`,
  }[status];
  logTramo(userId, { desde, hasta, days: tramo.length, ms: now() - startedAt, status, detail, since: stale.since });
  return { status, days: status === 'moved' ? 0 : result.daysProcessed, desde, hasta };
}

/** Abandona un usuario que agotó los reintentos y lo deja para revisión manual. */
async function abandonUser(doc) {
  const userId = doc.id;
  const stale = doc.data()._stale;
  console.error(`[reconcile] ALERT: User ${userId} exceeded max retries (${stale.retryCount}). Cleaning up stale marker.`);
  try {
    await db.collection('reconcileFailures').doc(userId).set({
      userId,
      staleData: stale,
      abandonedAt: new Date().toISOString(),
      reason: `Exceeded MAX_RETRY_COUNT (${LIMITS.MAX_RETRY_COUNT})`,
      // HU 6.8.1: `since` es el primer día aún sin corregir; lo anterior ya quedó corregido
      affectedRange: { since: stale.since },
      // Una revisión resuelta antes no oculta esta: el aviso lee `resolvedAt`
      resolvedAt: FieldValue.delete(),
    }, { merge: true });

    // Clean up the _stale marker to prevent infinite skip loop
    await doc.ref.update({ _stale: FieldValue.delete() });
    console.warn(`[reconcile] User ${userId}: Stale marker cleaned. Failure logged to reconcileFailures collection.`);
  } catch (cleanupError) {
    console.error(`[reconcile] Failed to cleanup stale for ${userId}:`, cleanupError.message);
  }
}

/**
 * Recorre los usuarios marcados en páginas hasta vaciar la cola o agotar el
 * tiempo. Antes eran 20 usuarios por corrida y cinco corridas por semana: con
 * más usuarios la cola crecía más rápido de lo que se vaciaba.
 *
 * HU 6.8.1 (RN-6.8.1-M): la noche va por vueltas. En la primera, un tramo por
 * inversor en el orden de siempre; en las siguientes, mientras quede tiempo,
 * un tramo más a cada uno que sigue pendiente. Un rango largo nunca toma un
 * segundo tramo mientras otro espera el primero.
 *
 * @param {Object} [options]
 * @param {number} [options.timeBudgetMs] - No se empieza un tramo pasado este tiempo
 * @param {Function} [options.now=Date.now] - Reloj en ms; también fija la fecha de la noche
 */
async function runReconciliation({ timeBudgetMs = LIMITS.TIME_BUDGET_MS, now = Date.now } = {}) {
  const startTime = now();
  const hayTiempo = () => now() - startTime < timeBudgetMs;
  const lastClosedDate = getLastClosedDate(DateTime.fromMillis(startTime, { zone: 'America/New_York' }));
  const seen = new Set();
  const avanzaron = new Set();
  const totals = { rounds: 0, tramos: 0, reconciled: 0, advanced: 0, moved: 0, failed: 0, skipped: 0, days: 0 };

  // Una corrida, una caché de precios: se comparte entre usuarios
  clearHistoricalPriceCache();

  /** Un tramo; devuelve si el inversor sigue en la cola de la vuelta siguiente. */
  const corregir = async (doc) => {
    const yaAvanzo = avanzaron.has(doc.id);
    totals.tramos++;
    try {
      const { status, days } = await corregirTramo(doc, { yaAvanzo, lastClosedDate, now });
      totals[status]++;
      totals.days += days;
      if (status === 'advanced') avanzaron.add(doc.id);
      return status === 'advanced' || status === 'moved';
    } catch (userError) {
      console.error(`[reconcile] User ${doc.id}: Exception -`, userError.message);
      try {
        await registrarFallo(doc.ref, userError.message, { yaAvanzo, now });
      } catch (updateError) {
        console.error(`[reconcile] Failed to update stale for ${doc.id}:`, updateError.message);
      }
      totals.failed++;
      return false;
    }
  };

  // Vuelta 1: la cola de siempre, un tramo por inversor
  totals.rounds = 1;
  let cola = [];
  while (hayTiempo()) {
    // Los reconciliados salen de la consulta; los demás siguen marcados y
    // ocupan sitio, por eso el límite crece con los ya vistos.
    const limit = LIMITS.USERS_PER_PAGE + seen.size;
    const page = await db.collection('portfolioPerformance')
      .where('_stale', '!=', null)
      .orderBy('_stale.since', 'desc') // Most recent first
      .limit(limit)
      .get();

    const pending = page.docs.filter((d) => !seen.has(d.id));
    if (pending.length === 0) break;

    for (const doc of pending) {
      if (!hayTiempo()) break;
      seen.add(doc.id);

      if ((doc.data()._stale.retryCount || 0) >= LIMITS.MAX_RETRY_COUNT) {
        await abandonUser(doc);
        totals.skipped++;
        continue;
      }

      if (await corregir(doc)) cola.push(doc.ref);
    }

    // Página incompleta: no queda nadie más en la cola
    if (page.size < limit) break;
  }

  // Vueltas siguientes: un tramo más a cada uno que sigue pendiente
  while (cola.length > 0 && hayTiempo()) {
    totals.rounds++;
    const siguiente = [];
    for (const ref of cola) {
      if (!hayTiempo()) break;
      const doc = await ref.get();
      if (!doc.exists || !doc.data()?._stale) continue;
      if (await corregir(doc)) siguiente.push(ref);
    }
    cola = siguiente;
  }

  const duration = ((now() - startTime) / 1000).toFixed(1);
  console.log(`[reconcile] DONE: ${totals.rounds} vuelta(s), ${totals.tramos} tramo(s) — ` +
    `${totals.reconciled} completados, ${totals.advanced} avanzaron, ${totals.moved} con la marca movida, ` +
    `${totals.failed} fallidos, ${totals.skipped} abandonados; ${totals.days} días en ${duration}s` +
    `${hayTiempo() ? '' : ' (tiempo agotado)'}`);

  if (totals.failed > 5) {
    console.error(`[reconcile] ALERT: High failure rate - ${totals.failed} failures in this run`);
  }
  return totals;
}

// ============================================================================
// MAIN SCHEDULED FUNCTION
// ============================================================================

/**
 * Corrección nocturna del rendimiento de los inversores marcados.
 *
 * Corre a las 02:00 ET, de martes a sábado:
 * 1. Busca los inversores con la marca `_stale`
 * 2. Recalcula su rendimiento de a un tramo (HU 6.8.1)
 * 3. Propaga cada tramo y avanza o retira la marca
 */
const reconcileStalePerformance = onSchedule({
  schedule: '0 2 * * 2-6',    // 02:00 ET, Tuesday to Saturday
  timeZone: 'America/New_York',
  memory: '512MiB',
  timeoutSeconds: 540,        // 9 minutes; TIME_BUDGET_MS deja margen
  maxInstances: 1,            // una corrida a la vez: comparten la cola
  secrets: [cfServiceToken],
  labels: {
    component: 'late-reg',
    purpose: 'reconcile-stale',
  },
}, async () => {
  console.log('[reconcile] Starting stale performance reconciliation...');
  try {
    await runReconciliation();
  } catch (error) {
    console.error('[reconcile] Fatal error:', error);
    throw error; // Re-throw so Cloud Functions records it as a failure
  }
});

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  reconcileStalePerformance,
  runReconciliation,
  corregirTramo,
  propagateReconciledRange,
  getLastClosedDate,
  closedMonthsSince,
  LIMITS, // Export for testing
};
