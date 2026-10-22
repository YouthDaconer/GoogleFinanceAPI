/**
 * HU 6.3 — El punto de reunión: nadie queda fuera en silencio.
 *
 * Al repartir el cierre en una tarea por inversor aparece un hueco que antes no
 * existía: **nadie sabe cuándo terminaron todos**. Los pasos globales del
 * cierre —riesgo del portafolio, histórico de índices, caché, benchmarks y la
 * señal de `systemStatus`— dependían de la lista de usuarios que el proceso
 * único acababa de calcular en memoria. Con el reparto esa lista ya no existe
 * en ningún sitio hasta que la última tarea termina.
 *
 * Esta función es ese punto de reunión, y al mismo tiempo el **único aviso a
 * operación que la plataforma tiene**. Hasta ahora, un inversor que fallaba
 * sólo dejaba un marcador `_stale` y una entrada en `reconcileFailures` que
 * nadie lee. El Escenario 4 pide lo contrario: que quede visible cuántos,
 * cuáles y por qué (RN-6.3-N, RN-6-E).
 *
 * **Qué hace, en orden:**
 * 1. Lee la cuenta del día (`eodProgress/{fecha}`).
 * 2. Si están todos: ejecuta los pasos globales y cierra el día como completo.
 * 3. Si falta alguien: escribe `eodFailures/{fecha}` con cuántos, cuáles y por
 *    qué, y emite un `console.error` con el prefijo `[EOD-INCOMPLETO]` que
 *    engancha la política de alertas de GCP. **Los pasos globales se ejecutan
 *    igual** con los que sí terminaron: que falte un inversor no es motivo para
 *    dejar a los demás sin riesgo ni sin índices del día.
 *
 * **Por qué no se inventa un canal de correo** (D6): el `emailService` del
 * proyecto es transaccional de suscripciones; meterle alertas de operación
 * sería usarlo para lo que no es. El canal es la política de alertas de GCP
 * sobre el log, que ya vigila el cierre.
 *
 * @module services/eodFinalizationScheduled
 * @see platform-docs/stories/6.3-cierre-nocturno-termina-siempre/refinamiento.md (T6)
 */

const { onSchedule } = require('firebase-functions/v2/scheduler');
const { defineSecret } = require('firebase-functions/params');
const { DateTime } = require('luxon');
const { FieldValue } = require('firebase-admin/firestore');
const admin = require('./firebaseAdmin');
const { crearLoggerDeConsola } = require('../utils/logger');

const progreso = require('./eodProgressService');
const { leerSnapshotDelDia } = require('./eodMarketSnapshot');
const { calculatePortfolioRisk } = require('./calculatePortfolioRisk');
const { invalidatePerformanceCacheBatch } = require('./cacheInvalidationService');
const { updateBenchmarkSnapshots } = require('./benchmarkSnapshotService');
const { saveIndicesHistoryDataInternal } = require('./marketDataScheduled');
const { refreshIndexCacheInternal } = require('./indexHistoryService');

/** SEC-TOKEN-001: los pasos globales llaman al API de datos de mercado. */
const cfServiceToken = defineSecret('CF_SERVICE_TOKEN');

/** Colección donde queda por escrito lo que no se pudo procesar. */
const COLECCION_FALLOS = 'eodFailures';

/**
 * Prefijo del log que engancha la política de alertas de GCP. Cambiarlo sin
 * cambiar `alert-policies.yaml` deja el aviso mudo.
 */
const PREFIJO_ALERTA = '[EOD-INCOMPLETO]';

/**
 * Tope de inversores nombrados en el documento del aviso. Con la meta de
 * 5.000 inversores (RN-6.3-J), un fallo masivo llenaría el documento: se
 * nombran los primeros y el recuento total siempre es exacto.
 */
const MAX_NOMBRADOS = 200;

const FUNCTION_CONFIG = {
  // 00:45 ET Ma-Sa: **cuarenta minutos** después de que el orquestador encola a
  // las 00:05, es decir, pasada la ventana de 30 minutos que promete RN-6.3-J.
  //
  // La hora no es arbitraria y corregirla importa: el diagnóstico de marzo
  // proponía las 00:20, pero eso son quince minutos, y 5.000 inversores entre
  // 40 en paralelo tardan ~21-25 min. Llegar a los quince declararía el día
  // incompleto y dispararía `[EOD-INCOMPLETO]` por gente que simplemente no
  // había terminado todavía — un falso positivo del único aviso que esta
  // historia existe para dar. Comprobado al desplegar el 2026-10-21.
  //
  // Si a los cuarenta minutos falta alguien, es un fallo de verdad: la meta ya
  // se incumplió.
  schedule: '45 0 * * 2-6',
  timeZone: 'America/New_York',
  memory: '512MiB',
  timeoutSeconds: 540,
  maxInstances: 1,
  minInstances: 0,
  secrets: [cfServiceToken],
  labels: {
    status: 'active',
    purpose: 'eod-finalization',
  },
};

/**
 * Deja por escrito lo que quedó fuera y lo grita al log (Escenario 4).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha
 * @param {Object} resumen
 * @param {number} resumen.totalUsers
 * @param {number} resumen.completedUsers
 * @param {Array<{userId: string, error: string|null, intentos: number}>} resumen.fallidos
 * @param {string[]} resumen.sinRastro
 * @returns {Promise<void>}
 */
async function avisarInversoresFuera(db, fecha, { totalUsers, completedUsers, fallidos, sinRastro }) {
  const totalFuera = fallidos.length + sinRastro.length;

  await db.collection(COLECCION_FALLOS).doc(fecha).set({
    date: fecha,
    totalUsers,
    completedUsers,
    totalFuera,
    // «Cuáles y por qué»: el que falló trae su error; el que nunca dejó rastro
    // trae por qué no lo hay, que es un diagnóstico distinto y más grave.
    fallidos: fallidos.slice(0, MAX_NOMBRADOS),
    sinRastro: sinRastro.slice(0, MAX_NOMBRADOS),
    truncado: totalFuera > MAX_NOMBRADOS,
    detectadoAt: new Date().toISOString(),
  });

  const muestra = [...fallidos.map((f) => f.userId), ...sinRastro].slice(0, 10).join(', ');
  console.error(
    `${PREFIJO_ALERTA} ${fecha}: ${totalFuera} de ${totalUsers} inversores quedaron sin rendimiento ` +
    `(${fallidos.length} fallaron tras sus reintentos, ${sinRastro.length} nunca se ejecutaron). ` +
    `Primeros: ${muestra}. Detalle en ${COLECCION_FALLOS}/${fecha}`
  );
}

/**
 * Los pasos que necesitan que los inversores ya estén calculados.
 *
 * Son exactamente los que hacía el orquestador al final, movidos aquí sin
 * cambiarlos: cada uno es best-effort y su fallo no tumba el cierre, igual que
 * antes.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha
 * @param {string[]} userIdsProcesados
 * @returns {Promise<{cachesDeleted: number}>}
 */
async function ejecutarPasosGlobales(db, fecha, userIdsProcesados) {
  const snapshotMercado = await leerSnapshotDelDia(db, fecha);
  if (!snapshotMercado) {
    console.error(`${PREFIJO_ALERTA} ${fecha}: no hay snapshot de mercado; los pasos globales no se pueden ejecutar`);
    return { cachesDeleted: 0 };
  }

  const { prices: currentPrices, currencies } = snapshotMercado;

  const assetsSnapshot = await db.collection('assets').where('isActive', '==', true).get();
  const allAssets = assetsSnapshot.docs.map((d) => ({ id: d.id, ...d.data() }));

  const cuentasSnapshot = await db.collection('portfolioAccounts').where('isActive', '==', true).get();
  const userPortfolios = cuentasSnapshot.docs.reduce((acc, doc) => {
    const cuenta = { id: doc.id, ...doc.data() };
    if (!cuenta.userId) return acc;
    if (!acc[cuenta.userId]) acc[cuenta.userId] = [];
    acc[cuenta.userId].push(cuenta);
    return acc;
  }, {});

  // Riesgo: sólo de los que terminaron. Los que no, están marcados `_stale` y
  // la reconciliación de las 02:00 los recoge.
  try {
    const currentPricesMap = {};
    currentPrices.forEach((quote) => {
      currentPricesMap[quote.symbol] = { beta: quote.beta ?? 1.0, price: quote.price || 0 };
    });
    await calculatePortfolioRisk({
      allAssets,
      userPortfolios,
      userIds: userIdsProcesados,
      currentPricesMap,
      currencies,
      calculationDate: fecha,
    });
    console.log(`[eodFinalization] ${fecha}: riesgo calculado para ${userIdsProcesados.length} inversores`);
  } catch (riskError) {
    console.error(`[eodFinalization] ${fecha}: fallo calculando riesgo — ${riskError.message}`);
  }

  try {
    const indices = await saveIndicesHistoryDataInternal({ formattedDate: fecha, skipCacheInvalidation: true });
    console.log(`[eodFinalization] ${fecha}: histórico de índices guardado (${indices.count})`);
  } catch (indicesError) {
    console.warn(`[eodFinalization] ${fecha}: saveIndicesHistoryData falló (no crítico) — ${indicesError.message}`);
  }

  try {
    const cache = await refreshIndexCacheInternal();
    console.log(`[eodFinalization] ${fecha}: caché de índices refrescada (${cache.refreshed})`);
  } catch (indexCacheError) {
    console.warn(`[eodFinalization] ${fecha}: refreshIndexCache falló (no crítico) — ${indexCacheError.message}`);
  }

  let cachesDeleted = 0;
  if (userIdsProcesados.length > 0) {
    try {
      const resultado = await invalidatePerformanceCacheBatch(userIdsProcesados);
      cachesDeleted = resultado.cachesDeleted;
    } catch (cacheError) {
      console.warn(`[eodFinalization] ${fecha}: invalidación de caché falló (no crítico) — ${cacheError.message}`);
    }
  }

  try {
    // `updateBenchmarkSnapshots` registra con `logger.*` sin comprobar que
    // exista; se le pasa el adaptador sobre `console.*`, que es el patrón
    // vigente de los estándares (§7).
    await updateBenchmarkSnapshots(db, currentPrices, currencies, DateTime.fromISO(fecha), crearLoggerDeConsola('eodFinalization'));
  } catch (benchmarkError) {
    console.warn(`[eodFinalization] ${fecha}: benchmarkSnapshots falló (no crítico) — ${benchmarkError.message}`);
  }

  return { cachesDeleted };
}

/**
 * El cierre póstumo de un día. Extraído del disparador para poder probarlo.
 *
 * @param {Object} [params]
 * @param {FirebaseFirestore.Firestore} [params.db]
 * @param {string} [params.fecha] - `YYYY-MM-DD`; por omisión, el día anterior en Nueva York
 * @returns {Promise<{date: string, estado: string, totalUsers: number, completedUsers: number, totalFuera: number}>}
 */
async function finalizarCierre({ db = admin.firestore(), fecha = null } = {}) {
  const fechaCierre = fecha || DateTime.now().setZone('America/New_York').minus({ days: 1 }).toISODate();

  const estado = await progreso.leerProgreso(db, fechaCierre);
  if (!estado) {
    // No hay cuenta del día: o no fue día de trading, o el reparto está
    // apagado y el cierre lo hizo el proceso único. Ninguna de las dos es un
    // fallo, y confundirlas con uno llenaría de ruido la alerta.
    console.log(`[eodFinalization] ${fechaCierre}: no hay cierre repartido que finalizar`);
    return { date: fechaCierre, estado: 'sin-cierre-repartido', totalUsers: 0, completedUsers: 0, totalFuera: 0 };
  }

  if (estado.estado === progreso.ESTADO_DIA.COMPLETO) {
    console.log(`[eodFinalization] ${fechaCierre}: ya estaba finalizado`);
    return {
      date: fechaCierre,
      estado: estado.estado,
      totalUsers: estado.totalUsers,
      completedUsers: estado.completedUsers,
      totalFuera: 0,
    };
  }

  const fallidos = await progreso.listarFallidos(db, fechaCierre);
  const sinRastro = await progreso.listarSinRastro(db, fechaCierre, estado.userIds);
  const totalFuera = fallidos.length + sinRastro.length;

  const fuera = new Set([...fallidos.map((f) => f.userId), ...sinRastro]);
  const userIdsProcesados = estado.userIds.filter((userId) => !fuera.has(userId));

  const { cachesDeleted } = await ejecutarPasosGlobales(db, fechaCierre, userIdsProcesados);

  if (totalFuera > 0) {
    await avisarInversoresFuera(db, fechaCierre, {
      totalUsers: estado.totalUsers,
      completedUsers: estado.completedUsers,
      fallidos,
      sinRastro,
    });
  } else {
    console.log(`[eodFinalization] ${fechaCierre}: los ${estado.totalUsers} inversores quedaron procesados`);
  }

  const veredicto = totalFuera > 0 ? progreso.ESTADO_DIA.INCOMPLETO : progreso.ESTADO_DIA.COMPLETO;
  await progreso.cerrarProgreso(db, fechaCierre, veredicto);

  try {
    await db.collection('systemStatus').doc('marketData').set({
      lastCompleteUpdate: FieldValue.serverTimestamp(),
      lastUpdateDate: DateTime.now().toISO(),
      source: 'eod-fanout',
      performanceCalculated: userIdsProcesados.length,
      failedUsers: totalFuera,
      cachesInvalidated: cachesDeleted,
      marketOpen: false,
    }, { merge: true });
  } catch (statusError) {
    console.warn(`[eodFinalization] ${fechaCierre}: systemStatus no se pudo actualizar — ${statusError.message}`);
  }

  return {
    date: fechaCierre,
    estado: veredicto,
    totalUsers: estado.totalUsers,
    completedUsers: estado.completedUsers,
    totalFuera,
  };
}

/**
 * HU 6.3 (T6) — El cierre póstumo, a las 00:20 ET de martes a sábado.
 */
const scheduledEodFinalization = onSchedule(FUNCTION_CONFIG, async () => {
  console.log('[eodFinalization] Iniciando cierre póstumo del EOD repartido');
  try {
    const resultado = await finalizarCierre();
    console.log(`[eodFinalization] Terminado: ${JSON.stringify(resultado)}`);
  } catch (error) {
    console.error('[eodFinalization] Error fatal en el cierre póstumo:', error);
    throw error;
  }
});

module.exports = {
  scheduledEodFinalization,
  finalizarCierre,
  ejecutarPasosGlobales,
  avisarInversoresFuera,
  FUNCTION_CONFIG,
  COLECCION_FALLOS,
  PREFIJO_ALERTA,
  MAX_NOMBRADOS,
};
