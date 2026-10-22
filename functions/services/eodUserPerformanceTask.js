/**
 * HU 6.3 — El cierre nocturno de un solo inversor.
 *
 * Hasta ahora el cierre era **un proceso para todos**: cargaba en memoria los
 * activos de todo el mundo, los recorría de cinco en cinco y tenía un tope de
 * nueve minutos. El problema no era que fallara, era que fallaba callado: al
 * agotarse el tiempo, los inversores que quedaban en la cola simplemente no
 * tenían su día calculado, el proceso terminaba sin error y nadie se enteraba
 * hasta que alguien veía un hueco en su gráfica.
 *
 * Esta función es la unidad del reparto: **una tarea, un inversor**, con su
 * propio tiempo, su propio reintento y su propio resultado. Un inversor que
 * falla falla solo (Escenario 3).
 *
 * **Por qué un disparador de cola.** Los estándares (§4 regla 7) listan
 * `onCall`, `onRequest`, `onSchedule` y `beforeUserCreated`. Ninguno sirve para
 * trabajo que se reparte en miles de unidades con reintento propio: un
 * `onSchedule` volvería a ser un proceso único con tope de tiempo, que es
 * justo lo que esta historia viene a quitar. `copilotWorker.js` ya sentó el
 * precedente de ampliar esa lista declarándolo en el plan; esto hace lo mismo
 * con `onTaskDispatched`, que es v2, que es lo que la regla exige. No es una
 * dependencia nueva: `firebase-functions@7` ya expone
 * `firebase-functions/v2/tasks`.
 *
 * **Lo que NO hace:** no cambia ni una fórmula. Llama a `processUserPerformance`
 * tal cual, con los mismos datos que le pasaba el proceso único, para que las
 * cifras sean idénticas al céntimo a las del cierre actual (RN-6-A, Escenario 7).
 *
 * @module services/eodUserPerformanceTask
 * @see platform-docs/stories/6.3-cierre-nocturno-termina-siempre/refinamiento.md (T3, T8)
 */

const { onTaskDispatched } = require('firebase-functions/v2/tasks');
const { defineSecret } = require('firebase-functions/params');
const { FieldPath } = require('firebase-admin/firestore');
const admin = require('./firebaseAdmin');

const { leerSnapshotDelDia } = require('./eodMarketSnapshot');
const progreso = require('./eodProgressService');
const {
  processUserPerformance,
  PerformanceDataCache,
  markInconsistentUsersAsStale,
  setModuleLogger,
} = require('./unifiedMarketDataUpdate');
const { chunkArray } = require('./backfillCoreModule');
const { crearLoggerDeConsola } = require('../utils/logger');

/** SEC-TOKEN-001: el cálculo no llama al API, pero sí los módulos que arrastra. */
const cfServiceToken = defineSecret('CF_SERVICE_TOKEN');

/** Tope del operador `in` de Firestore. */
const TAMANO_LOTE_IN = 30;

/** Intentos totales de una tarea antes de darla por perdida (RN-6-E). */
const MAX_INTENTOS = 3;

/**
 * 120 s por inversor. El inversor más pesado de producción tarda ~8 s hoy; el
 * margen cubre un arranque en frío y una cartera varias veces mayor sin dejar
 * que una tarea atascada ocupe una instancia durante minutos.
 *
 * `maxInstances: 40` con `concurrency: 1` es el paralelismo real del reparto:
 * 40 inversores a la vez contra Firestore, ocho veces lo que permitía el
 * `p-limit(5)` del proceso único, y por debajo del techo de escrituras por
 * segundo que ya calibraba SCALE-001.
 *
 * **40 no es una elección de diseño: es el techo del proyecto.** `us-central1`
 * tiene una cuota `CpuAllocPerProjectRegion` de 40.000 milinúcleos, y cada
 * instancia de 512 MiB consume un núcleo, así que pedir 50 hace **fallar el
 * despliegue** con «Max instances must be set to 40 or fewer» (comprobado el
 * 2026-10-21). Subir de aquí exige pedir ampliación de cuota a GCP; cambiar el
 * número sin hacerlo rompe el despliegue, no lo mejora.
 */
const FUNCTION_CONFIG = {
  retryConfig: {
    maxAttempts: MAX_INTENTOS,
    minBackoffSeconds: 10,
    maxBackoffSeconds: 300,
    maxDoublings: 3,
  },
  rateLimits: {
    maxConcurrentDispatches: 40,
    maxDispatchesPerSecond: 40,
  },
  memory: '512MiB',
  timeoutSeconds: 120,
  maxInstances: 40,
  minInstances: 0,
  concurrency: 1,
  secrets: [cfServiceToken],
  labels: {
    status: 'active',
    purpose: 'eod-user-performance',
  },
};

/**
 * Adaptador de logging para el módulo del orquestador (ver `setModuleLogger`).
 * Estándares §7: el código nuevo usa `console.*` con prefijo `[unidad]`.
 */
const loggerDeTarea = crearLoggerDeConsola('eodUserPerformanceTask');

/**
 * Borra los snapshots del inversor para que el reintento los rehaga.
 *
 * Sin esto, RN-6.3-L se cumple en la forma pero no en el valor:
 * `updateSnapshotIncremental` **salta** si el día ya está en el timeline
 * (`snapshotIncrementalService.js:202`), así que un reintento sobre un inversor
 * que se interrumpió a mitad dejaría el snapshot malo intacto. Borrarlos antes
 * fuerza la reconstrucción completa — es el mismo remedio que ya aplica
 * `reconcileStalePerformance.js` al propagar un recálculo.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} userId
 * @returns {Promise<number>} Snapshots borrados
 */
async function borrarSnapshotsDelInversor(db, userId) {
  const snapshots = db.collection('performanceSnapshots');
  const prefijo = `${userId}_`;

  // `` es el último carácter del plano básico: cierra el rango de todos
  // los IDs que empiezan por el prefijo.
  const docs = await snapshots
    .where(FieldPath.documentId(), '>=', snapshots.doc(prefijo))
    .where(FieldPath.documentId(), '<', snapshots.doc(`${prefijo}`))
    .get();

  for (let i = 0; i < docs.docs.length; i += 400) {
    const batch = db.batch();
    docs.docs.slice(i, i + 400).forEach((d) => batch.delete(d.ref));
    await batch.commit();
  }

  if (docs.size > 0) {
    console.log(`[eodUserPerformanceTask] ${userId}: ${docs.size} snapshots borrados antes de rehacer`);
  }
  return docs.size;
}

/**
 * Reúne, **sólo para este inversor**, lo que el proceso único cargaba de golpe
 * para todos.
 *
 * Es el intercambio consciente que la historia acepta: cada tarea lee sus
 * propios datos en vez de aprovechar una lectura conjunta, lo que multiplica
 * las lecturas por unas cinco veces. Se paga a cambio de que nadie quede fuera.
 *
 * El resultado tiene exactamente la forma que espera `processUserPerformance`,
 * y restringirlo al inversor no cambia ningún número: la función ya filtraba
 * por cuentas del usuario todo lo que recibía.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} userId
 * @param {string} formattedDate - `YYYY-MM-DD`
 * @returns {Promise<Object|null>} `null` si el inversor no tiene cuentas activas
 */
async function cargarDatosDelInversor(db, userId, formattedDate) {
  const cuentasSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();

  const accounts = cuentasSnapshot.docs.map((d) => ({ id: d.id, ...d.data() }));
  if (accounts.length === 0) return null;

  const accountIds = accounts.map((a) => a.id);
  const lotes = chunkArray(accountIds, TAMANO_LOTE_IN);

  // FIX-TIMESTAMP-002: el campo `date` puede traer timestamp completo, así que
  // el día se consulta por rango, igual que en el proceso único.
  const dateRangeStart = `${formattedDate}T00:00:00.000Z`;
  const dateRangeEnd = `${formattedDate}T23:59:59.999Z`;

  const todaysTransactions = [];
  for (const lote of lotes) {
    const snapshot = await db.collection('transactions')
      .where('portfolioAccountId', 'in', lote)
      .where('date', '>=', dateRangeStart)
      .where('date', '<=', dateRangeEnd)
      .get();
    snapshot.docs.forEach((d) => todaysTransactions.push({ id: d.id, ...d.data() }));
  }

  const sellTransactions = todaysTransactions.filter((t) => t.type === 'sell');
  const assetIdsInSellTransactions = [...new Set(sellTransactions.map((t) => t.assetId).filter(Boolean))];

  const activeAssets = [];
  for (const lote of lotes) {
    const snapshot = await db.collection('assets')
      .where('portfolioAccount', 'in', lote)
      .where('isActive', '==', true)
      .get();
    snapshot.docs.forEach((d) => activeAssets.push({ id: d.id, ...d.data() }));
  }

  const inactiveAssets = [];
  if (assetIdsInSellTransactions.length > 0) {
    for (const lote of lotes) {
      const snapshot = await db.collection('assets')
        .where('portfolioAccount', 'in', lote)
        .where('isActive', '==', false)
        .get();
      snapshot.docs
        .map((d) => ({ id: d.id, ...d.data() }))
        .filter((asset) => assetIdsInSellTransactions.includes(asset.id))
        .forEach((asset) => inactiveAssets.push(asset));
    }
  }

  const allAssets = [...activeAssets, ...inactiveAssets];

  const sellTransactionsByAccount = sellTransactions.reduce((acc, transaction) => {
    if (!acc[transaction.portfolioAccountId]) acc[transaction.portfolioAccountId] = [];
    acc[transaction.portfolioAccountId].push(transaction);
    return acc;
  }, {});

  const inactiveAssetsWithSellTransactions = new Set(
    inactiveAssets
      .filter((asset) => sellTransactions.some((tx) => tx.assetId === asset.id))
      .map((asset) => asset.id)
  );

  const assetsToInclude = [
    ...activeAssets,
    ...inactiveAssets.filter((asset) => inactiveAssetsWithSellTransactions.has(asset.id)),
  ];

  // El caché precarga sólo a este inversor: es el mismo componente, acotado a
  // un portafolio en vez de a todos.
  const cache = new PerformanceDataCache();
  await cache.preloadHistoricalData(db, formattedDate, { [userId]: accounts }, sellTransactions);

  return {
    accounts,
    accountIds,
    cache,
    allAssets,
    assetsToInclude,
    sellTransactions,
    sellTransactionsByAccount,
    inactiveAssets,
    activeAssets,
    todaysTransactions,
  };
}

/**
 * Calcula el rendimiento del día de un inversor. Es el cuerpo de la tarea,
 * extraído para poder probarlo sin la envoltura de Cloud Tasks.
 *
 * @param {Object} params
 * @param {FirebaseFirestore.Firestore} params.db
 * @param {string} params.userId
 * @param {string} params.date - Día que se cierra, `YYYY-MM-DD`
 * @param {number} [params.intento] - Entregas previas de esta tarea (0 en la primera)
 * @returns {Promise<{userId: string, date: string, estado: 'completado'|'omitido-ya-procesado'|'omitido-sin-cuentas'}>}
 * @throws Si el día no está materializado o el cálculo falla: Cloud Tasks reintenta.
 */
async function procesarInversor({ db, userId, date, intento = 0 }) {
  // RN-6.3-L: un reintento sobre quien ya terminó no vuelve a escribir nada.
  if (await progreso.yaProcesado(db, date, userId)) {
    console.log(`[eodUserPerformanceTask] ${userId} ${date}: ya procesado, no se repite`);
    return { userId, date, estado: 'omitido-ya-procesado' };
  }

  // RN-6.3-M: los precios del día salen del documento del día, nunca del
  // mercado en vivo. Si no está, la tarea falla y se reintenta: calcular con
  // precios de otro momento sería peor que no calcular.
  const snapshot = await leerSnapshotDelDia(db, date);
  if (!snapshot) {
    throw new Error(`No hay snapshot de mercado materializado para ${date}`);
  }

  const datos = await cargarDatosDelInversor(db, userId, date);
  if (!datos) {
    console.log(`[eodUserPerformanceTask] ${userId} ${date}: sin cuentas activas, nada que calcular`);
    await progreso.marcarCompletado(db, date, userId, { intento });
    return { userId, date, estado: 'omitido-sin-cuentas' };
  }

  // Escenario 5 + RN-6.3-L: si una entrega anterior se interrumpió a mitad, los
  // snapshots pueden haber quedado con el día ya en el timeline. Borrarlos es
  // lo que hace que el reintento rehaga en lugar de saltar.
  if (intento > 0) {
    await borrarSnapshotsDelInversor(db, userId);
  }

  // OPT-DEMAND-500 / SCALE-004: la misma validación de consistencia que hacía
  // el proceso único antes de calcular, ahora acotada a este inversor.
  const consistencia = datos.cache.validateDataConsistency(userId, datos.accountIds);
  if (!consistencia.isConsistent) {
    await markInconsistentUsersAsStale(db, [{ userId, ...consistencia }]);
  }

  setModuleLogger(loggerDeTarea);
  const resultado = await processUserPerformance({
    db,
    userId,
    accounts: datos.accounts,
    currentPrices: snapshot.prices,
    currencies: snapshot.currencies,
    cache: datos.cache,
    allAssets: datos.allAssets,
    assetsToInclude: datos.assetsToInclude,
    sellTransactions: datos.sellTransactions,
    sellTransactionsByAccount: datos.sellTransactionsByAccount,
    inactiveAssets: datos.inactiveAssets,
    activeAssets: datos.activeAssets,
    todaysTransactions: datos.todaysTransactions,
    formattedDate: date,
  });

  if (!resultado.success) {
    throw new Error(resultado.error || 'Fallo desconocido calculando el rendimiento');
  }

  // El inversor se da por terminado **después** de sus snapshots, no antes: ese
  // orden es lo que hace que una interrupción a mitad se rehaga entera
  // (Escenario 5).
  await progreso.marcarCompletado(db, date, userId, {
    durationMs: resultado.durationMs,
    intento,
  });

  console.log(`[eodUserPerformanceTask] ${userId} ${date}: completado en ${resultado.durationMs}ms (intento ${intento + 1})`);
  return { userId, date, estado: 'completado' };
}

/**
 * HU 6.3 (T3) — El handler de la cola.
 *
 * Cloud Tasks entrega `{ userId, date }` y nada más (D2). Si el handler lanza,
 * Cloud Tasks reintenta hasta `MAX_INTENTOS`; en el último intento el inversor
 * se marca como fallido para que el cierre póstumo lo nombre en el aviso
 * (RN-6.3-N, Escenario 4).
 */
const eodUserPerformanceTask = onTaskDispatched(FUNCTION_CONFIG, async (request) => {
  const { userId, date } = request.data || {};
  const intento = request.retryCount || 0;

  if (!userId || !date) {
    // Una tarea mal formada no se arregla reintentándola: se registra y se
    // descarta devolviendo normalmente.
    console.error(`[eodUserPerformanceTask] Tarea inválida, falta userId o date: ${JSON.stringify(request.data)}`);
    return;
  }

  const db = admin.firestore();

  try {
    await procesarInversor({ db, userId, date, intento });
  } catch (error) {
    const esUltimoIntento = intento + 1 >= MAX_INTENTOS;
    console.error(`[eodUserPerformanceTask] ${userId} ${date}: fallo en el intento ${intento + 1}/${MAX_INTENTOS} — ${error.message}`);

    if (esUltimoIntento) {
      // Agotados los reintentos, el inversor entra en la lista del aviso. Y se
      // marca `_stale` para que la reconciliación de las 02:00 —la red que ya
      // existe— lo recoja esta misma noche.
      await progreso.marcarFallido(db, date, userId, error.message, intento);
      try {
        await db.collection('portfolioPerformance').doc(userId).set({
          _stale: {
            since: date,
            reason: 'eod-task-exhausted-retries',
            source: 'eodUserPerformanceTask',
            retryCount: 0,
            lastAttempt: new Date().toISOString(),
          },
        }, { merge: true });
      } catch (staleError) {
        console.error(`[eodUserPerformanceTask] ${userId}: no se pudo marcar _stale — ${staleError.message}`);
      }
      // No se relanza: el intento se agotó y el fallo ya quedó visible. Volver
      // a lanzar sólo produciría ruido de entregas muertas en la cola.
      return;
    }

    throw error;
  }
});

module.exports = {
  eodUserPerformanceTask,
  procesarInversor,
  cargarDatosDelInversor,
  borrarSnapshotsDelInversor,
  FUNCTION_CONFIG,
  MAX_INTENTOS,
};
