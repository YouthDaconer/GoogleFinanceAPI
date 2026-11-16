/**
 * HU 6.12 — Tras corregir una operación pasada, los resúmenes se rehacen solo
 * desde la fecha afectada.
 *
 * Antes, la corrección nocturna borraba todos los resúmenes del inversor y cada
 * pantalla (y el cierre siguiente) volvía a leer toda su historia. Aquí, al
 * terminar la corrección, cada resumen que ya existía se rehace **desde la fecha
 * afectada**:
 *
 * - Los días anteriores a la fecha no se leen (RN-6.12-B): se reconstruyen desde
 *   lo que el propio resumen ya guarda.
 *   - Portafolio o cuenta: `timeline`, las listas `cashFlows`/`realizedPnL` de
 *     la 6.9.2 y el inicio y cierre de cada mes (`monthlyCompound`).
 *   - Activo en USD: sus días (`assetDays`, 6.11.6).
 * - Los días desde la fecha se leen **una vez por alcance** (portafolio y cada
 *   cuenta) y sirven para todos sus resúmenes.
 * - Todo pasa por **las mismas funciones** del cálculo desde cero
 *   (`armarResumenDePortafolio`, `armarResumenDeActivo`): la cifra es la del
 *   recálculo completo por construcción (RN-6-A).
 *
 * Cada resumen termina de una de tres formas:
 * - `rehecho`.
 * - `sin-cambios`: su último día es anterior a la fecha, así que ninguno de sus
 *   días cambió. El lector lo pone al día al abrirlo.
 * - `como-hoy`: no se puede rehacer y se borra, como antes de esta HU. Solo ese
 *   resumen, con su motivo (RN-6.12-F, Escenario 3).
 *
 * Solo se tocan los resúmenes que existían: no se crea ninguno (RN-6.12-G).
 *
 * `REBUILD_SNAPSHOTS_FROM_DATE=false` devuelve el comportamiento de siempre
 * (RN-6-H): la corrección vuelve a borrar todo en cada tramo.
 *
 * @module services/snapshots/snapshotRebuildService
 * @see platform-docs/stories/6.12-correccion-desde-fecha-afectada/refinamiento.md (T2)
 */

const { FieldPath } = require('firebase-admin/firestore');
const {
  armarResumenDePortafolio,
  armarResumenDeActivo,
  fetchAllDailyDocs,
} = require('../snapshotGenerator');
const { SCHEMA_VERSION_INCREMENTAL, MAX_DAILY_POINTS } = require('../snapshotIncrementalService');
const { docsDesdeDiasDelActivo, extraerDiasDelActivo } = require('../../utils/assetDays');
const { deleteInSmallBatches } = require('../../utils/firestoreDeletes');

const COLECCION = 'performanceSnapshots';

// ============================================================================
// INTERRUPTOR
// ============================================================================

/**
 * Se lee en cada tramo, como los interruptores de la 6.4 y la 6.9.2: basta con
 * cambiar la variable y desplegar para volver atrás (RN-6-H).
 *
 * @returns {boolean} `true` salvo `REBUILD_SNAPSHOTS_FROM_DATE=false`
 */
function resumenesRehechosDesdeLaFecha() {
  return process.env.REBUILD_SNAPSHOTS_FROM_DATE !== 'false';
}

// ============================================================================
// RECONSTRUIR LOS DÍAS ANTERIORES A LA FECHA (puro)
// ============================================================================

/** `{ "2026": { "10": entrada } }` → la entrada del mes de `fecha` (YYYY-MM-DD). */
function entradaDelMes(monthlyCompound, fecha) {
  const anio = fecha.substring(0, 4);
  const mes = String(parseInt(fecha.substring(5, 7), 10));
  return monthlyCompound?.[anio]?.[mes] || null;
}

/**
 * Los documentos diarios anteriores a `antesDe` de un resumen de portafolio o de
 * cuenta, reconstruidos desde lo que el resumen guarda, con los campos que lee
 * el cálculo desde cero:
 *
 * - todos los días: valor y cambio (`timeline`), flujo (`cashFlows`) y P&G
 *   realizado (`realizedPnL`); los dos últimos solo si el día los tiene, como
 *   los escribe `extraerListasDiarias`;
 * - el primer día de cada mes: la inversión con que empezó el mes;
 * - el último día de cada mes: la inversión y el P&G no realizado con que cerró.
 *
 * El cambio se pone como ajustado y como diario, igual que la derivación de la
 * 6.9.2 (`derivarResumenDeMoneda`). El desde cero lee el ajustado y la serie
 * guarda `ajustado ?? diario`.
 *
 * @param {Object} snapshot - Resumen con `timeline`, `cashFlows`, `realizedPnL` y `monthlyCompound`
 * @param {string} antesDe - YYYY-MM-DD: solo los días anteriores
 * @param {string|null} [fechaSiguiente] - Primer día leído desde la fecha: decide
 *   si el último día reconstruido cerró su mes
 * @returns {Object[]|null} Documentos `{ date, [moneda]: {...} }` en orden, o
 *   `null` si a un mes le falta su entrada en `monthlyCompound`
 */
function docsDesdeResumen(snapshot, antesDe, fechaSiguiente = null) {
  const moneda = snapshot.currency;
  const flujos = new Map((snapshot.cashFlows || []).map((x) => [x.d, x.f]));
  const realizados = new Map((snapshot.realizedPnL || []).map((x) => [x.d, x.p]));
  const dias = (snapshot.timeline || []).filter((p) => p.d < antesDe);

  const docs = [];
  for (let i = 0; i < dias.length; i++) {
    const p = dias[i];
    const mes = p.d.substring(0, 7);
    const anterior = dias[i - 1]?.d;
    const siguiente = i + 1 < dias.length ? dias[i + 1].d : fechaSiguiente;

    const bloque = { totalValue: p.v, adjustedDailyChangePercentage: p.c, dailyChangePercentage: p.c };
    if (flujos.has(p.d)) bloque.totalCashFlow = flujos.get(p.d);
    if (realizados.has(p.d)) bloque.doneProfitAndLoss = realizados.get(p.d);

    const primeroDelMes = !anterior || anterior.substring(0, 7) !== mes;
    const ultimoDelMes = !siguiente || siguiente.substring(0, 7) !== mes;
    if (primeroDelMes || ultimoDelMes) {
      const entrada = entradaDelMes(snapshot.monthlyCompound, p.d);
      if (!entrada) return null;
      if (primeroDelMes) bloque.totalInvestment = entrada.startTotalInvestment;
      if (ultimoDelMes) {
        bloque.totalInvestment = entrada.endTotalInvestment;
        bloque.unrealizedProfitAndLoss = entrada.unrealizedProfitAndLoss;
      }
    }
    docs.push({ date: p.d, [moneda]: bloque });
  }
  return docs;
}

// ============================================================================
// REHACER UN RESUMEN (puro)
// ============================================================================

/**
 * Rehace un resumen desde `desde` con los días leídos desde esa fecha, sin leer
 * ni escribir nada.
 *
 * @param {Object|null} snapshot - El resumen tal como está en Firestore
 * @param {Array} docsDesde - Documentos diarios del alcance con `date >= desde`, en orden
 * @param {string} desde - YYYY-MM-DD, la fecha afectada
 * @returns {{metodo: 'rehecho'|'sin-cambios'|'como-hoy', motivo?: string, snapshot?: Object}}
 */
function rehacerResumenDesde(snapshot, docsDesde, desde) {
  const comoHoy = (motivo) => ({ metodo: 'como-hoy', motivo });

  if (!snapshot || (snapshot.schemaVersion || 0) < SCHEMA_VERSION_INCREMENTAL ||
      !Array.isArray(snapshot.timeline) || !snapshot.lastDateInTimeline) {
    return comoHoy('esquema-viejo');
  }
  // Ninguno de sus días es de la fecha en adelante: nada de lo que guarda cambió.
  if (snapshot.lastDateInTimeline < desde) return { metodo: 'sin-cambios' };
  // Escenario 3 / QA-04: la fecha es anterior a su inicio.
  if (snapshot.startDate && desde < snapshot.startDate) return comoHoy('fecha-anterior-al-inicio');
  // Sin días desde la fecha (se borraron): el último día ya no es el que guarda.
  if (docsDesde.length === 0) return comoHoy('sin-dias-desde-la-fecha');

  const { userId, accountId, currency } = snapshot;
  // Sin alcance no se sabe de qué días sale: rehacerlo con los del portafolio mezclaría
  if (!accountId) return comoHoy('sin-alcance');

  if (snapshot.type === 'asset') {
    // Decisión 1 del refinamiento: solo el resumen en USD guarda sus días.
    if (currency !== 'USD' || !Array.isArray(snapshot.assetDays)) return comoHoy('sin-dias');
    const { ticker, assetType } = snapshot;
    const previos = docsDesdeDiasDelActivo(snapshot.assetDays.filter((r) => r.d < desde), `${ticker}_${assetType}`);
    const docs = [...previos, ...docsDesde];
    const rehecho = armarResumenDeActivo(docs, { userId, accountId, currency, ticker, assetType });
    if (!rehecho) return comoHoy('sin-datos');
    rehecho.assetDays = extraerDiasDelActivo(docs, currency, ticker, assetType);
    return { metodo: 'rehecho', snapshot: rehecho };
  }

  if (!Array.isArray(snapshot.cashFlows) || !Array.isArray(snapshot.realizedPnL)) return comoHoy('sin-listas');
  // Por encima de este tope el incremental comprime lo más viejo a meses.
  if (snapshot.timeline.length > MAX_DAILY_POINTS) return comoHoy('serie-comprimida');

  const primero = docsDesde[0];
  const fechaSiguiente = (typeof primero.data === 'function' ? primero.data() : primero).date;
  const previos = docsDesdeResumen(snapshot, desde, fechaSiguiente);
  if (!previos) return comoHoy('sin-mes');

  const rehecho = armarResumenDePortafolio([...previos, ...docsDesde], { userId, accountId, currency });
  if (!rehecho) return comoHoy('sin-datos');
  return { metodo: 'rehecho', snapshot: rehecho };
}

// ============================================================================
// REHACER LOS RESÚMENES DE UN INVERSOR
// ============================================================================

/**
 * Los resúmenes del inversor, solo con los campos pedidos (son documentos
 * grandes). Misma búsqueda por prefijo de ID que el borrado de la corrección y
 * `retirarResumenesSinTenencia` (6.4).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} userId
 * @param {string[]} campos
 * @returns {Promise<Array<{id: string, ref: FirebaseFirestore.DocumentReference}>>} Con los campos pedidos
 */
async function listarResumenes(db, userId, campos) {
  const coleccion = db.collection(COLECCION);
  const prefijo = `${userId}_`;
  const snap = await coleccion
    .where(FieldPath.documentId(), '>=', coleccion.doc(prefijo))
    .where(FieldPath.documentId(), '<', coleccion.doc(`${prefijo}`))
    .select(...campos)
    .get();
  return snap.docs.map((d) => ({ id: d.id, ref: d.ref, ...d.data() }));
}

/**
 * Rehace desde `desde` todos los resúmenes que el inversor ya tenía.
 *
 * - Los que terminan después de la fecha se agrupan por alcance. Los días del
 *   alcance desde la fecha se leen una vez y se procesan uno a uno, para no
 *   tener varios resúmenes grandes en memoria.
 * - Un error al rehacer uno lo manda a "como hoy" y la noche sigue con los demás
 *   (RN-6.12-F).
 * - Los que van "como hoy" se borran al final.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} params
 * @param {string} params.userId
 * @param {string} params.desde - YYYY-MM-DD, la fecha más antigua de la corrección
 * @returns {Promise<{rehechos: string[], sinCambios: string[], comoHoy: Array<{id: string, motivo: string}>,
 *   diasLeidos: number, ms: number}>}
 */
async function rehacerResumenesDelInversor(db, { userId, desde }) {
  const inicio = Date.now();
  const resultado = { rehechos: [], sinCambios: [], comoHoy: [], diasLeidos: 0, ms: 0 };
  const aBorrar = [];

  const porAlcance = new Map();
  for (const r of await listarResumenes(db, userId, ['accountId', 'lastDateInTimeline'])) {
    if (r.lastDateInTimeline && r.lastDateInTimeline < desde) {
      resultado.sinCambios.push(r.id);
      continue;
    }
    const alcance = r.accountId || 'overall';
    if (!porAlcance.has(alcance)) porAlcance.set(alcance, []);
    porAlcance.get(alcance).push(r);
  }

  for (const [alcance, resumenes] of porAlcance) {
    const docsDesde = await fetchAllDailyDocs(db, userId, alcance, { desde });
    resultado.diasLeidos += docsDesde.length;

    for (const { id, ref } of resumenes) {
      try {
        const doc = await ref.get();
        if (!doc.exists) continue; // lo borró otro proceso mientras tanto
        const paso = rehacerResumenDesde(doc.data(), docsDesde, desde);
        if (paso.metodo === 'rehecho') {
          await ref.set(paso.snapshot);
          resultado.rehechos.push(id);
        } else if (paso.metodo === 'sin-cambios') {
          resultado.sinCambios.push(id);
        } else {
          resultado.comoHoy.push({ id, motivo: paso.motivo });
          aBorrar.push(ref);
        }
      } catch (error) {
        resultado.comoHoy.push({ id, motivo: `error: ${error.message}` });
        aBorrar.push(ref);
      }
    }
  }

  if (aBorrar.length > 0) await deleteInSmallBatches(db, aBorrar);
  resultado.ms = Date.now() - inicio;

  const motivos = resultado.comoHoy.map((x) => `${x.id} (${x.motivo})`).join(', ');
  console.log(`[snapshotRebuild] ${userId} desde ${desde}: ${resultado.rehechos.length} rehechos, ` +
    `${resultado.sinCambios.length} sin cambios, ${resultado.comoHoy.length} como hoy` +
    `${motivos ? `: ${motivos}` : ''}; ${resultado.diasLeidos} días leídos, ${resultado.ms} ms`);
  return resultado;
}

module.exports = {
  resumenesRehechosDesdeLaFecha,
  docsDesdeResumen,
  rehacerResumenDesde,
  rehacerResumenesDelInversor,
  listarResumenes,
};
