/**
 * reconcileStalePerformance.js
 *
 * Corrección nocturna del rendimiento de los inversores marcados con `_stale`
 * (operaciones registradas con fecha pasada).
 *
 * Horario: 02:00 ET, de martes a sábado.
 *
 * HU 6.8.1: la corrección avanza por tramos de `DIAS_POR_TRAMO` días hábiles,
 * desde el más antiguo, y recuerda dónde quedó en `_stale.since` (el primer
 * día aún sin corregir). Un rango largo ya no se recorta ni agota la noche:
 * toma varias noches y avanza en cada una.
 *
 * HU 6.8.2: ningún inversor bloquea a los demás.
 * - Cada tramo corre aislado, con su propio tope de memoria y de tiempo: si se
 *   pasa, cae ese tramo y la noche sigue (Escenario 1).
 * - El intento se cuenta **antes** de empezar: aunque el proceso muera, cuenta.
 *   Tres noches seguidas sin avance llevan a revisión de operación, con el
 *   motivo de cada una (Escenario 2, RN-6.8.2-J).
 * - Se atiende primero al que más lleva esperando (Escenario 3, RN-6.8-P).
 * - Al terminar queda el estado de la noche y, si hay atraso, el aviso a
 *   operación (Escenarios 4 y 5, `reconcileNightReportService.js`).
 *
 * @see LATE-REG-003
 * @see platform-docs/stories/6.8.1-historial-corregido-por-tramos/historia.md
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md
 */

const path = require('path');
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
const { crearEjecutorAislado, crearEjecutorEnProceso } = require('../utils/ejecutorAislado');
const {
  esperaDe,
  porEspera,
  nocheDe,
  calcularEstadoNoche,
  publicarEstadoNoche,
  PREFIJO_ALERTA,
} = require('./reconcileNightReportService');

const db = getFirestore();

/**
 * SEC-TOKEN-001: precios y tasas vienen del API de mercado, que exige
 * `x-service-token`. Sin el binding la función corría con el token vacío:
 * todo respondía 401, ningún día se podía valorar y la reconciliación "salía
 * bien" con 0 días escritos (DDeR8P, 2026-11-06 y 2026-11-10).
 */
const cfServiceToken = defineSecret('CF_SERVICE_TOKEN');

// ============================================================================
// LÍMITES
// ============================================================================

const LIMITS = {
  /**
   * RN-6.8.1-K: días hábiles por tramo, un trimestre. Medido el 2026-11-12,
   * ≈1,5 s por día con todo incluido: ≈95 s, dentro de TIEMPO_MAX_TRAMO_MS.
   */
  DIAS_POR_TRAMO: 63,

  /** RN-6.8-L: intentos sin avance seguidos antes de pasar a revisión */
  MAX_RETRY_COUNT: 3,

  /**
   * No se empieza un tramo pasado este tiempo. Con TIEMPO_MAX_TRAMO_MS, el
   * último tramo termina como tarde a los 520 s: el límite de 540 s de la
   * función ya no corta a nadie (HU 6.8.2).
   */
  TIME_BUDGET_MS: 400 * 1000,

  /** HU 6.8.2 (vacío 7): tope de cada tramo; al pasarlo, cuenta como `tiempo` */
  TIEMPO_MAX_TRAMO_MS: 120 * 1000,

  /**
   * HU 6.8.2: tope de memoria del worker de cada tramo. La función tiene
   * 1 GiB: el principal (con todo `index.js` cargado) y el worker caben con
   * margen, y un tramo que se pase muere solo.
   */
  MEMORIA_MAX_TRAMO_MB: 512,
};

/** Motivo provisional del intento: si nadie lo cambia, el proceso murió con él en curso. */
const MOTIVO_INTERRUMPIDA = 'interrumpida';

// ============================================================================
// FECHAS
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

/** Noche (YYYY-MM-DD en Nueva York) de un instante ISO, o `null`. */
function nocheDeIso(iso) {
  return iso ? DateTime.fromISO(iso).setZone('America/New_York').toISODate() : null;
}

// ============================================================================
// EL TRABAJO DE UN TRAMO (corre en el worker)
// ============================================================================

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

/**
 * El trabajo de UN tramo: recalcularlo con el motor y, si corrigió algo,
 * propagarlo. No toca la marca: eso lo hace el proceso principal.
 *
 * HU 6.8.2: es lo que corre dentro del worker aislado
 * (`reconcileTramoWorker.js`), así que recibe y devuelve datos que se pueden
 * clonar entre hilos.
 *
 * "Corregir algún día" (RN-6.8.2-J) es escribir **o borrar** al menos un doc
 * diario: borrar los docs de un inversor que lo vendió todo también corrige.
 *
 * @param {Object} params
 * @param {string} params.userId
 * @param {string[]} params.tramo - Días hábiles del tramo (YYYY-MM-DD)
 * @param {string} params.rangeStart - Inicio de calendario (`_stale.since`)
 * @param {number} params.nowMs - Reloj de la corrida
 * @returns {Promise<{success: boolean, daysProcessed: number, docsBorrados: number, errors: string[]}>}
 */
async function ejecutarTramo({ userId, tramo, rangeStart, nowMs }) {
  const result = await backfillUserPerformance(userId, tramo, { rangeStart });
  if (!result.success) {
    return { success: false, daysProcessed: 0, docsBorrados: 0, errors: result.errors || [] };
  }

  const docsBorrados = (result.staleDocs || []).length;
  if (result.daysProcessed + docsBorrados > 0) {
    const accountIds = (await db.collection('portfolioAccounts').where('userId', '==', userId).get())
      .docs.map((d) => d.id);
    await propagateReconciledRange(userId, tramo[0], accountIds, tramo[tramo.length - 1],
      DateTime.fromMillis(nowMs, { zone: 'America/New_York' }));
  }
  return { success: true, daysProcessed: result.daysProcessed, docsBorrados, errors: [] };
}

// ============================================================================
// INTENTOS (proceso principal)
// ============================================================================

/** RN-6-I: una línea por tramo, lo que se observa en los logs de cada noche. */
function logTramo(userId, { desde, hasta, days, ms, status, detail, since }) {
  const rango = desde ? `${desde}→${hasta} (${days} días, ${ms} ms)` : `sin días pendientes desde ${since}`;
  const line = `[reconcile] TRAMO ${userId} ${rango}: ${status} — ${detail}`;
  if (status === 'failed') console.error(line);
  else console.log(line);
}

/**
 * HU 6.8.2 (RN-6.8.2-J): abre el intento de esta noche **antes** de empezar el
 * tramo. Si el proceso entero muere con este inversor en curso, el intento ya
 * contó y queda con el motivo `interrumpida`: es lo que hacía que el mismo
 * inversor se reintentara cada noche para siempre, bloqueando a los demás.
 *
 * Un intento por noche: un relanzamiento el mismo día no suma otro, reemplaza
 * el de esa noche (vacío 6).
 *
 * @param {FirebaseFirestore.DocumentReference} ref
 * @param {Object} params
 * @param {string} params.noche - YYYY-MM-DD
 * @param {Function} params.now - Reloj en ms
 * @returns {Promise<{stale: Object, previo: Object|null}|null>} La marca con el intento
 *   abierto y el intento de esta misma noche que reemplazó (`null` si sumó uno), o
 *   `null` si ya no hay marca
 */
async function abrirIntento(ref, { noche, now }) {
  return db.runTransaction(async (tx) => {
    const fresh = await tx.get(ref);
    const stale = fresh.exists ? fresh.data()._stale : null;
    if (!stale) return null;

    const at = new Date(now()).toISOString();
    const previos = Array.isArray(stale.intentos) ? stale.intentos : [];
    const ultimo = previos[previos.length - 1];
    const nuevo = {
      noche,
      motivo: MOTIVO_INTERRUMPIDA,
      detalle: 'el proceso de la noche se cortó con este inversor en curso',
      at,
    };
    const mismaNoche = Boolean(ultimo && ultimo.noche === noche);
    const intentos = mismaNoche ? [...previos.slice(0, -1), nuevo] : [...previos, nuevo];
    const retryCount = mismaNoche ? Math.max(stale.retryCount || 0, previos.length) : (stale.retryCount || 0) + 1;

    tx.update(ref, { '_stale.intentos': intentos, '_stale.retryCount': retryCount, '_stale.enCurso': at });
    return { stale: { ...stale, intentos, retryCount, enCurso: at }, previo: mismaNoche ? ultimo : null };
  });
}

/**
 * Deja constancia de un tramo fallido. Si el intento de esta noche ya estaba
 * abierto (`contado`), su motivo provisional pasa a ser el real. Si el
 * inversor ya avanzó esta noche, el fallo no suma (RN-6.8.1-L).
 *
 * @param {FirebaseFirestore.DocumentReference} ref
 * @param {{motivo: string, detalle: string}} fallo
 * @param {Object} params
 * @param {boolean} params.contado - Se abrió el intento de esta noche
 * @param {string} params.noche
 * @param {Function} params.now
 * @returns {Promise<{retryCount: number}>}
 */
async function registrarFallo(ref, fallo, { contado, noche, now }) {
  return db.runTransaction(async (tx) => {
    const fresh = await tx.get(ref);
    const stale = fresh.exists ? fresh.data()._stale : null;
    if (!stale) return { retryCount: 0 };

    const at = new Date(now()).toISOString();
    const update = {
      '_stale.lastError': `${fallo.motivo}: ${fallo.detalle}`,
      '_stale.lastAttempt': at,
    };
    if (contado) {
      const previos = Array.isArray(stale.intentos) ? stale.intentos : [];
      const ultimo = previos[previos.length - 1];
      const entrada = { noche, motivo: fallo.motivo, detalle: fallo.detalle, at };
      // Si una marca del código anterior borró los intentos, se vuelve a anotar el de esta noche
      update['_stale.intentos'] = ultimo && ultimo.noche === noche
        ? [...previos.slice(0, -1), entrada]
        : [...previos, entrada];
      update['_stale.enCurso'] = FieldValue.delete();
    }
    tx.update(ref, update);
    return { retryCount: stale.retryCount || 0 };
  });
}

/**
 * Corrige UN tramo de un inversor marcado: los primeros `DIAS_POR_TRAMO` días
 * hábiles desde `_stale.since`, sin recortar nada (RN-6.8.1-K, RN-6.8-K).
 *
 * Avanzar (RN-6.8.1-L) es completar el tramo entero (calculado, validado,
 * escrito y propagado) y mover `since` al día siguiente a su último día, en la
 * transacción que comprueba que nadie movió la marca. Con avance, la cuenta de
 * intentos vuelve a 0 (RN-6.8.2-J: los intentos tienen que ser seguidos).
 *
 * HU 6.8.2:
 * - el intento se abre antes del tramo (`abrirIntento`), salvo que el inversor
 *   ya avanzara esta noche;
 * - el tramo lo ejecuta `ejecutor` (aislado en producción); sin memoria, sin
 *   tiempo o con error, es un intento fallido con ese motivo;
 * - terminar sin error pero sin corregir ningún día también es un intento
 *   fallido (`sin_avance`), y la marca no se mueve;
 * - si otra operación movió la marca durante el tramo, el intento se deshace:
 *   llegó información nueva, no falló nada (RN-6.8.1-O).
 *
 * @param {FirebaseFirestore.DocumentSnapshot} doc - portfolioPerformance/{uid}
 * @param {Object} [options]
 * @param {boolean} [options.yaAvanzo=false] - El inversor ya avanzó esta noche
 * @param {string} [options.lastClosedDate] - Último día cerrado (YYYY-MM-DD)
 * @param {Function} [options.now=Date.now] - Reloj en ms
 * @param {string} [options.noche] - Noche de la corrida (YYYY-MM-DD)
 * @param {Object} [options.ejecutor] - Ejecuta `ejecutarTramo`
 * @returns {Promise<{status: 'reconciled'|'advanced'|'moved'|'failed'|'gone', days: number,
 *   desde?: string, hasta?: string, motivo?: string, retryCount?: number, sinceInicial?: string}>}
 */
async function corregirTramo(doc, {
  yaAvanzo = false,
  lastClosedDate = getLastClosedDate(),
  now = Date.now,
  noche = nocheDe(now()),
  ejecutor = crearEjecutorEnProceso(ejecutarTramo),
} = {}) {
  const userId = doc.id;
  const startedAt = now();

  let stale = doc.data()._stale;
  let previo = null;
  const contado = !yaAvanzo;
  if (contado) {
    const abierto = await abrirIntento(doc.ref, { noche, now });
    if (!abierto) return { status: 'gone', days: 0 };
    ({ stale, previo } = abierto);
  }
  const sinceInicial = stale.since;

  let desde;
  let hasta;
  let dias = 0;
  try {
    const pendientes = await generateTradingDays(stale.since, lastClosedDate);
    const tramo = pendientes.slice(0, LIMITS.DIAS_POR_TRAMO);
    desde = tramo[0];
    hasta = tramo[tramo.length - 1];
    dias = tramo.length;
    const esUltimo = tramo.length === pendientes.length;

    let fallo = null;
    let daysProcessed = 0;
    if (tramo.length > 0) {
      const salida = await ejecutor.ejecutar({ userId, tramo, rangeStart: stale.since, nowMs: now() });
      if (!salida.ok) {
        fallo = { motivo: salida.motivo, detalle: salida.detalle };
      } else if (!salida.resultado.success) {
        // Los 3 primeros errores bastan para diagnosticar
        fallo = { motivo: 'error', detalle: salida.resultado.errors.slice(0, 3).join('; ') || 'sin detalle' };
      } else if (salida.resultado.daysProcessed + salida.resultado.docsBorrados === 0) {
        fallo = { motivo: 'sin_avance', detalle: 'terminó sin error pero sin corregir ningún día' };
      }
      daysProcessed = salida.ok ? salida.resultado.daysProcessed : 0;
    }

    if (fallo) {
      const { retryCount } = await registrarFallo(doc.ref, fallo, { contado, noche, now });
      logTramo(userId, {
        desde, hasta, days: dias, ms: now() - startedAt, status: 'failed',
        detail: `${fallo.motivo}: ${fallo.detalle}` +
          `${contado ? ` (intento ${retryCount} de ${LIMITS.MAX_RETRY_COUNT})` : ' (ya avanzó esta noche: no suma intento)'}`,
      });
      return { status: 'failed', days: 0, desde, hasta, motivo: fallo.motivo, retryCount, sinceInicial, contado };
    }

    const siguiente = esUltimo ? null : nextCalendarDay(hasta);
    const { status, currentSince } = await db.runTransaction(async (tx) => {
      const fresh = await tx.get(doc.ref);
      const current = fresh.data()?._stale;
      if (!current || current.registeredAt !== stale.registeredAt || current.since !== stale.since) {
        // Deshacer el intento de esta noche, si la marca nueva lo conservó: se
        // quita, o vuelve el de un relanzamiento anterior de la misma noche
        if (current && contado && current.enCurso === stale.enCurso) {
          const sinEste = (current.intentos || []).filter((i) => i.noche !== noche);
          tx.update(doc.ref, {
            '_stale.intentos': previo ? [...sinEste, previo] : sinEste,
            '_stale.retryCount': previo ? (current.retryCount || 0) : Math.max(0, (current.retryCount || 0) - 1),
            '_stale.enCurso': FieldValue.delete(),
          });
        }
        return { status: 'moved', currentSince: current?.since || null };
      }
      if (esUltimo) {
        tx.update(doc.ref, { _stale: FieldValue.delete() });
        return { status: 'reconciled' };
      }
      const at = new Date(now()).toISOString();
      tx.update(doc.ref, {
        '_stale.since': siguiente,
        '_stale.retryCount': 0,
        '_stale.intentos': FieldValue.delete(),
        '_stale.enCurso': FieldValue.delete(),
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
    logTramo(userId, { desde, hasta, days: dias, ms: now() - startedAt, status, detail, since: stale.since });
    return { status, days: status === 'moved' ? 0 : daysProcessed, desde, hasta, sinceInicial };
  } catch (error) {
    // Un fallo de la propia corrida (Firestore, calendario) también es un intento sin avance
    const fallo = { motivo: 'error', detalle: error.message };
    let retryCount = stale.retryCount || 0;
    try {
      ({ retryCount } = await registrarFallo(doc.ref, fallo, { contado, noche, now }));
    } catch (updateError) {
      console.error(`[reconcile] No se pudo registrar el fallo de ${userId}: ${updateError.message}`);
    }
    logTramo(userId, { desde, hasta, days: dias, ms: now() - startedAt, status: 'failed', detail: `error: ${error.message}` });
    return { status: 'failed', days: 0, desde, hasta, motivo: 'error', retryCount, sinceInicial, contado };
  }
}

/**
 * Pasa a revisión de operación a un inversor que agotó sus intentos: escribe
 * `reconcileFailures/{uid}` y retira la marca, para que deje de ocupar sitio
 * en la cola (Escenario 2).
 *
 * HU 6.8.2: guarda el motivo y la fecha de cada intento y desde cuándo
 * esperaba. Operación lo libera con `liberarRevision`.
 *
 * @param {FirebaseFirestore.DocumentReference} ref
 * @param {Object} [params]
 * @param {Function} [params.now=Date.now]
 * @returns {Promise<{userId: string, intentos: Array, pendienteDesde: string}|null>}
 */
async function abandonUser(ref, { now = Date.now } = {}) {
  const userId = ref.id;
  try {
    const fresh = await ref.get();
    const stale = fresh.exists ? fresh.data()._stale : null;
    if (!stale) return null;

    const intentos = Array.isArray(stale.intentos) ? stale.intentos : [];
    const pendienteDesde = esperaDe(stale);
    console.error(`[reconcile] ALERT: User ${userId} pasa a revisión: ${stale.retryCount} intentos sin avance seguidos ` +
      `(${intentos.map((i) => `${i.motivo} ${i.noche}`).join(', ') || 'motivos no registrados'})`);

    await db.collection('reconcileFailures').doc(userId).set({
      userId,
      staleData: stale,
      intentos,
      pendienteDesde,
      abandonedAt: new Date(now()).toISOString(),
      reason: `${stale.retryCount} intentos sin avance seguidos (máximo ${LIMITS.MAX_RETRY_COUNT})`,
      // HU 6.8.1: `since` es el primer día aún sin corregir; lo anterior ya quedó corregido
      affectedRange: { since: stale.since },
      // Una revisión resuelta antes no oculta esta: el aviso lee `resolvedAt`
      resolvedAt: FieldValue.delete(),
      liberadoAt: FieldValue.delete(),
    }, { merge: true });

    // Fuera de la cola hasta que operación lo libere
    await ref.update({ _stale: FieldValue.delete() });
    return { userId, intentos, pendienteDesde };
  } catch (cleanupError) {
    console.error(`[reconcile] No se pudo pasar a revisión a ${userId}:`, cleanupError.message);
    return null;
  }
}

/**
 * HU 6.8.2 (Escenario 2): operación libera a un inversor en revisión y vuelve
 * a la cola.
 *
 * - La cuenta de intentos vuelve a 0 y conserva su espera original: es el que
 *   más espera, así que irá de los primeros (vacío 1).
 * - `since` es el más antiguo entre lo que quedó sin corregir y la marca
 *   actual, si entró otra operación mientras estaba en revisión.
 * - Liberar dos veces no hace nada la segunda: vuelve a la cola una sola vez.
 *
 * @param {string} userId
 * @param {Object} [params]
 * @param {boolean} [params.aplicar=true] - Sin aplicar, solo dice lo que haría
 * @param {Function} [params.now=Date.now]
 * @returns {Promise<{estado: 'liberado'|'simulado'|'ya-liberado'|'sin-revision'|'sin-rendimiento',
 *   since?: string, pendienteDesde?: string}>}
 */
async function liberarRevision(userId, { aplicar = true, now = Date.now } = {}) {
  const failRef = db.collection('reconcileFailures').doc(userId);
  const perfRef = db.collection('portfolioPerformance').doc(userId);

  return db.runTransaction(async (tx) => {
    const revision = await tx.get(failRef);
    const perf = await tx.get(perfRef);
    if (!revision.exists) return { estado: 'sin-revision' };
    const datos = revision.data();
    if (datos.resolvedAt) return { estado: 'ya-liberado', resolvedAt: datos.resolvedAt };
    if (!perf.exists) return { estado: 'sin-rendimiento' };

    const at = new Date(now()).toISOString();
    const previa = datos.staleData || {};
    const actual = perf.data()._stale || null;
    const since = [actual && actual.since, (datos.affectedRange && datos.affectedRange.since) || previa.since]
      .filter(Boolean).sort()[0];
    const pendienteDesde = [datos.pendienteDesde, esperaDe(previa), actual && esperaDe(actual)]
      .filter(Boolean).sort()[0] || at;

    if (aplicar) {
      tx.update(perfRef, {
        _stale: {
          since,
          reason: 'liberado_por_operacion',
          transactionType: previa.transactionType || 'unknown',
          portfolioAccount: previa.portfolioAccount || null,
          registeredAt: (actual && actual.registeredAt) || at,
          pendienteDesde,
          retryCount: 0,
          intentos: [],
        },
      });
      tx.update(failRef, { resolvedAt: at, liberadoAt: at });
    }
    return { estado: aplicar ? 'liberado' : 'simulado', since, pendienteDesde };
  });
}

// ============================================================================
// LA NOCHE
// ============================================================================

/**
 * La cola, en el orden en que se atiende: primero el que más lleva esperando
 * (RN-6.8-P). Una sola lectura con la consulta y el índice de siempre, solo
 * con la marca (`select`); antes se releía por páginas y las lecturas crecían
 * con el cuadrado de la cola.
 *
 * @returns {Promise<Array<{id: string, ref: FirebaseFirestore.DocumentReference, stale: Object}>>}
 */
async function leerCola() {
  const snap = await db.collection('portfolioPerformance')
    .where('_stale', '!=', null)
    .orderBy('_stale.since', 'desc')
    .select('_stale')
    .get();
  return snap.docs
    .map((d) => ({ id: d.id, ref: d.ref, stale: d.data()._stale }))
    .filter((e) => e.stale)
    .sort(porEspera);
}

/** Inversores en revisión de operación sin liberar. */
async function contarEnRevision() {
  const snap = await db.collection('reconcileFailures').select('resolvedAt').get();
  return snap.docs.filter((d) => !d.data().resolvedAt).length;
}

/**
 * Una noche de corrección.
 *
 * HU 6.8.1 (RN-6.8.1-M): por vueltas. En la primera, un tramo por inversor; en
 * las siguientes, mientras quede tiempo, un tramo más a cada uno que sigue
 * pendiente. Un rango largo nunca toma un segundo tramo mientras otro espera
 * el primero.
 *
 * HU 6.8.2: la primera vuelta va por espera (RN-6.8-P); un inversor que llega
 * con 3 intentos (el proceso murió en el tercero) pasa a revisión sin
 * atenderlo, y el que falla el tercero esta noche, en el momento. Al terminar,
 * el estado de la noche y, si hay atraso, el aviso (RN-6.8.2-K, RN-6.8.2-L).
 *
 * @param {Object} [options]
 * @param {number} [options.timeBudgetMs] - No se empieza un tramo pasado este tiempo
 * @param {Function} [options.now=Date.now] - Reloj en ms; también fija la fecha de la noche
 * @param {Object} [options.ejecutor] - Ejecuta cada tramo; por omisión, en este proceso
 * @returns {Promise<Object>} Recuentos de la noche y su `estado`
 */
async function runReconciliation({
  timeBudgetMs = LIMITS.TIME_BUDGET_MS,
  now = Date.now,
  ejecutor = crearEjecutorEnProceso(ejecutarTramo),
} = {}) {
  const startTime = now();
  const hayTiempo = () => now() - startTime < timeBudgetMs;
  const noche = nocheDe(startTime);
  const lastClosedDate = getLastClosedDate(DateTime.fromMillis(startTime, { zone: 'America/New_York' }));
  const avanzaron = new Set();
  const atendidos = new Map(); // uid → { sinceInicial, avanzo }
  const revisionesNuevas = [];
  const totals = { rounds: 0, tramos: 0, reconciled: 0, advanced: 0, moved: 0, failed: 0, skipped: 0, days: 0 };

  // Una corrida, una caché de precios: se comparte entre usuarios
  clearHistoricalPriceCache();

  const aRevision = async (ref) => {
    const entrada = await abandonUser(ref, { now });
    if (entrada) revisionesNuevas.push(entrada);
    totals.skipped++;
  };

  /** Un tramo; devuelve si el inversor sigue en la cola de la vuelta siguiente. */
  const corregir = async (doc) => {
    const stale = doc.data()._stale;
    const yaAvanzo = avanzaron.has(doc.id) || nocheDeIso(stale.lastAdvance) === noche;
    totals.tramos++;
    const r = await corregirTramo(doc, { yaAvanzo, lastClosedDate, now, noche, ejecutor });
    if (r.status === 'gone') {
      totals.tramos--;
      return false;
    }
    if (!atendidos.has(doc.id)) atendidos.set(doc.id, { sinceInicial: r.sinceInicial, avanzo: false });
    totals[r.status]++;
    totals.days += r.days;
    if (r.status === 'advanced' || r.status === 'reconciled') {
      avanzaron.add(doc.id);
      atendidos.get(doc.id).avanzo = true;
    }
    if (r.status === 'failed' && r.contado && r.retryCount >= LIMITS.MAX_RETRY_COUNT) {
      // Escenario 2: "cuando termina el tercero", no la noche siguiente
      await aRevision(doc.ref);
      return false;
    }
    return r.status === 'advanced' || r.status === 'moved';
  };

  // Vuelta 1: toda la cola, por espera, un tramo por inversor
  totals.rounds = 1;
  let cola = [];
  for (const { ref } of await leerCola()) {
    if (!hayTiempo()) break;
    const doc = await ref.get(); // la marca puede haber cambiado desde que se leyó la cola
    const stale = doc.exists ? doc.data()._stale : null;
    if (!stale) continue;

    if ((stale.retryCount || 0) >= LIMITS.MAX_RETRY_COUNT) {
      // El proceso murió en su tercer intento: nadie pudo pasarlo a revisión
      await aRevision(ref);
      continue;
    }
    if (await corregir(doc)) cola.push(ref);
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

  const tiempoAgotado = !hayTiempo();
  const duration = ((now() - startTime) / 1000).toFixed(1);
  console.log(`[reconcile] DONE: ${totals.rounds} vuelta(s), ${totals.tramos} tramo(s) — ` +
    `${totals.reconciled} completados, ${totals.advanced} avanzaron, ${totals.moved} con la marca movida, ` +
    `${totals.failed} fallidos, ${totals.skipped} a revisión; ${totals.days} días en ${duration}s` +
    `${tiempoAgotado ? ' (tiempo agotado)' : ''}`);

  // Escenarios 4 y 5: el estado de la noche y, si hace falta, el aviso
  let estado = null;
  try {
    estado = calcularEstadoNoche({
      noche,
      inicioMs: startTime,
      finMs: now(),
      lastClosedDate,
      totals,
      atendidos,
      colaFinal: await leerCola(),
      revisionesNuevas,
      totalEnRevision: await contarEnRevision(),
      tiempoAgotado,
    });
    await publicarEstadoNoche(db, estado);
  } catch (error) {
    // Sin estado no hay aviso: con el prefijo, el fallo sí llega a operación
    console.error(`${PREFIJO_ALERTA} ${noche}: no se pudo calcular el estado de la noche (${error.message}). ` +
      'Revisar los registros [reconcile] de la ejecución');
  }

  return { ...totals, estado };
}

// ============================================================================
// FUNCIÓN PROGRAMADA
// ============================================================================

/**
 * Corrección nocturna del rendimiento de los inversores marcados.
 *
 * Corre a las 02:00 ET, de martes a sábado (se puede relanzar a mano forzando
 * el job de Cloud Scheduler):
 * 1. Lee la cola de inversores marcados y la ordena por espera.
 * 2. Corrige un tramo de cada uno en un worker aislado (HU 6.8.1 y 6.8.2).
 * 3. Deja el estado de la noche y avisa a operación si hay atraso.
 *
 * RN-6.8-N: una sola ejecución, de duración acotada. 1 GiB de memoria para
 * que el principal y el worker del tramo quepan (HU 6.8.2, decisión 9).
 */
const reconcileStalePerformance = onSchedule({
  schedule: '0 2 * * 2-6',    // 02:00 ET, de martes a sábado
  timeZone: 'America/New_York',
  memory: '1GiB',
  timeoutSeconds: 540,        // 9 minutos; TIME_BUDGET_MS + TIEMPO_MAX_TRAMO_MS = 520 s
  maxInstances: 1,            // una corrida a la vez: comparten la cola
  secrets: [cfServiceToken],
  labels: {
    component: 'late-reg',
    purpose: 'reconcile-stale',
  },
}, async () => {
  console.log('[reconcile] Starting stale performance reconciliation...');
  const ejecutor = crearEjecutorAislado({
    script: path.join(__dirname, 'reconcileTramoWorker.js'),
    maxMemoriaMb: LIMITS.MEMORIA_MAX_TRAMO_MB,
    tiempoMaxMs: LIMITS.TIEMPO_MAX_TRAMO_MS,
    etiqueta: 'reconcile',
  });
  try {
    await runReconciliation({ ejecutor });
  } catch (error) {
    console.error('[reconcile] Fatal error:', error);
    throw error; // Cloud Functions lo registra como fallo
  } finally {
    await ejecutor.cerrar();
  }
});

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  reconcileStalePerformance,
  runReconciliation,
  corregirTramo,
  ejecutarTramo,
  abrirIntento,
  abandonUser,
  liberarRevision,
  propagateReconciledRange,
  getLastClosedDate,
  closedMonthsSince,
  LIMITS,
  MOTIVO_INTERRUMPIDA,
};
