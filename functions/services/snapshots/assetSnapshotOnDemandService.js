/**
 * HU 6.4 — Resúmenes por activo solo cuando se piden.
 *
 * El cierre nocturno deja de actualizar los resúmenes por activo de todos los
 * activos de cada inversor. Cada resumen se prepara la primera vez que alguien
 * abre el activo y, a partir de ahí, se pone al día **al abrirlo**, leyendo
 * solo los días que le faltan y con **la misma función** que usaba la noche
 * (RN-6.4-K, RN-6.4-N).
 *
 * Tres piezas:
 * - `resumenesPorActivoBajoDemanda()`: el interruptor (RN-6-H).
 *   `ASSET_SNAPSHOTS_ON_DEMAND=false` devuelve el comportamiento de siempre.
 * - `ponerAlDiaResumenDeActivo()`: la puesta al día al abrir.
 * - `retirarResumenesSinTenencia()`: retira los resúmenes de activos que el
 *   inversor ya no tiene (RN-6.4-M).
 *
 * Las correcciones del historial (RN-6.4-L) no pasan por aquí: la corrección
 * nocturna y el reintento del cierre borran todos los resúmenes del inversor,
 * y el primero que se abre después se rehace con los datos corregidos.
 *
 * @module services/snapshots/assetSnapshotOnDemandService
 * @see platform-docs/stories/6.4-resumenes-activo-bajo-demanda/refinamiento.md (T2, T3, T6)
 */

const { DateTime } = require('luxon');
const { FieldPath } = require('firebase-admin/firestore');
const {
  buildSnapshotDocId,
  generateAssetSnapshot,
  fetchAssetDailyDocs,
} = require('../snapshotGenerator');
const {
  aplicarDiaAResumenDeActivo,
  SCHEMA_VERSION_INCREMENTAL,
} = require('../snapshotIncrementalService');
const { deleteInSmallBatches } = require('../../utils/firestoreDeletes');

const ZONA = 'America/New_York';

/** Tope de valores de un filtro `in` de Firestore. */
const TAMANO_LOTE_IN = 30;

// ============================================================================
// INTERRUPTOR (T2)
// ============================================================================

/**
 * Se lee en cada llamada, como `utils/scopedReads.js#leerInterruptores`: así
 * basta con cambiar la variable y desplegar para volver atrás (RN-6-H).
 *
 * @returns {boolean} `true` salvo `ASSET_SNAPSHOTS_ON_DEMAND=false`
 */
function resumenesPorActivoBajoDemanda() {
  return process.env.ASSET_SNAPSHOTS_ON_DEMAND !== 'false';
}

/**
 * Monedas en que el cierre mantiene los resúmenes del inversor (PERF-SNAP-026):
 * USD y su moneda principal.
 *
 * @param {string|null|undefined} defaultCurrency
 * @returns {string[]}
 */
function monedasDelCierre(defaultCurrency) {
  return defaultCurrency && defaultCurrency !== 'USD' ? ['USD', defaultCurrency] : ['USD'];
}

// ============================================================================
// PONER AL DÍA AL ABRIR (T3)
// ============================================================================

/**
 * Pone al día el resumen de un activo que la señal del inversor dio por viejo.
 *
 * Solo hace incremental lo que mantenía la noche: el portafolio completo en USD
 * o en la moneda principal. Lo demás (una cuenta, otra moneda) ya se preparaba
 * al abrirlo rehaciéndose entero, y sigue así para no mover cifras (RN-6-A).
 *
 * Lee **todos** los días posteriores a `lastDateInTimeline`, sin el tope de 7
 * días de la noche: al abrir, un hueco largo es lo normal, y añadir los días que
 * faltan siempre lee menos que rehacer (decisión 5 del refinamiento).
 *
 * El "ahora" de los periodos es el instante del cierre (`senal`), el mismo con
 * que la noche habría hecho el cálculo: la cifra sale idéntica, no parecida.
 *
 * Rehace entero, como la noche, si no hay fecha o el esquema es viejo, si el
 * activo no aparece algún día (lo vendió: la noche nunca le añadía días) o si
 * falta la moneda en un documento.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} params
 * @param {string} params.userId
 * @param {string} params.accountId
 * @param {string} params.currency
 * @param {string} params.ticker
 * @param {string} params.assetType
 * @param {Object|null} params.snapshot - El resumen viejo tal como está en Firestore
 * @param {string|null} params.senal - `lastSnapshotUpdate` del inversor (ISO)
 * @returns {Promise<{snapshot: Object|null, metodo: string, motivo?: string, diasLeidos: number|null}>}
 *   `metodo`: `incremental` · `sin-dias-nuevos` · `rehecho`
 */
async function ponerAlDiaResumenDeActivo(db, params) {
  const { userId, accountId, currency, ticker, assetType, snapshot, senal } = params;
  const snapshotId = buildSnapshotDocId(userId, accountId, currency, ticker, assetType);
  const assetKey = `${ticker}_${assetType}`;
  const inicio = Date.now();

  const terminar = (resultado) => {
    const dias = resultado.diasLeidos === null ? 'historia completa' : `${resultado.diasLeidos} días leídos`;
    const motivo = resultado.motivo ? ` (${resultado.motivo})` : '';
    console.log(`[assetSnapshotOnDemand] ${snapshotId}: ${resultado.metodo}${motivo}, ${dias}, ${Date.now() - inicio} ms`);
    return resultado;
  };

  const rehacer = async (motivo) => {
    const rehecho = await generateAssetSnapshot(db, userId, accountId, currency, ticker, assetType);
    return terminar({ snapshot: rehecho || null, metodo: 'rehecho', motivo, diasLeidos: null });
  };

  if (accountId !== 'overall') return rehacer('cuenta');

  const userDataDoc = await db.collection('userData').doc(userId).get();
  const defaultCurrency = userDataDoc.exists ? userDataDoc.data()?.defaultCurrency : null;
  if (!monedasDelCierre(defaultCurrency).includes(currency)) return rehacer('moneda-no-mantenida');

  if (!snapshot || (snapshot.schemaVersion || 0) < SCHEMA_VERSION_INCREMENTAL || !snapshot.lastDateInTimeline) {
    return rehacer('sin-fecha-o-esquema-viejo');
  }

  const dias = await fetchAssetDailyDocs(db, userId, accountId, currency, ticker, assetType, {
    despuesDe: snapshot.lastDateInTimeline,
  });

  const now = senal ? DateTime.fromISO(senal).setZone(ZONA) : DateTime.now().setZone(ZONA);
  let actual = snapshot;

  for (const doc of dias) {
    const data = doc.data();
    const currencyData = data[currency];
    if (!currencyData) return rehacer(`falta-moneda-${data.date}`);

    const assetData = currencyData.assetPerformance?.[assetKey];
    if (!assetData) return rehacer(`sin-posicion-${data.date}`);

    const paso = aplicarDiaAResumenDeActivo(actual, assetData, data.date, { now, sinTopeDeHueco: true });
    if (paso.method === 'skipped') continue;
    if (paso.method !== 'incremental') return rehacer(paso.reason);
    actual = paso.snapshot;
  }

  // Sin días que añadir (un cierre relanzado el mismo día): solo se marca como
  // revisado, para que la señal no lo vuelva a dar por viejo.
  const metodo = actual === snapshot ? 'sin-dias-nuevos' : 'incremental';
  if (actual === snapshot) {
    actual = { ...snapshot, lastUpdated: new Date().toISOString() };
  }

  await db.collection('performanceSnapshots').doc(snapshotId).set(actual);
  return terminar({ snapshot: actual, metodo, diasLeidos: dias.length });
}

// ============================================================================
// RETIRADA DE SOBRANTES (T6)
// ============================================================================

/**
 * Activos que el inversor tiene: los de sus lotes activos en sus cuentas
 * activas. Es la misma fuente con la que el cierre arma `assetPerformance`, con
 * la misma clave `${name}_${assetType}`. `assets.userId` no sirve: solo está en
 * los lotes importados.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} userId
 * @returns {Promise<Set<string>>}
 */
async function activosQueTiene(db, userId) {
  const cuentas = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  const ids = cuentas.docs.map((d) => d.id);

  const claves = new Set();
  for (let i = 0; i < ids.length; i += TAMANO_LOTE_IN) {
    const lotes = await db.collection('assets')
      .where('portfolioAccount', 'in', ids.slice(i, i + TAMANO_LOTE_IN))
      .where('isActive', '==', true)
      .get();
    lotes.docs.forEach((d) => {
      const { name, assetType } = d.data();
      if (name && assetType) claves.add(`${name}_${assetType}`);
    });
  }
  return claves;
}

/**
 * Retira los resúmenes por activo de los activos que el inversor **ya no
 * tiene**, de cualquier cuenta y moneda (RN-6.4-M, Escenario 6).
 *
 * Los de activos que conserva se quedan aunque nunca los haya abierto: están al
 * día y los va a usar en cuanto entre. Relanzarlo es seguro: la segunda vez no
 * encuentra sobrantes. Sin `aplicar`, solo cuenta.
 *
 * Los resúmenes se buscan por prefijo de ID (`{userId}_`), como el borrado de la
 * corrección nocturna, y se leen solo los campos que hacen falta: son
 * documentos grandes.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} params
 * @param {string} params.userId
 * @param {boolean} [params.aplicar=false]
 * @returns {Promise<{revisados: number, sobrantes: string[], retirados: number, conservados: number}>}
 */
async function retirarResumenesSinTenencia(db, { userId, aplicar = false }) {
  const tiene = await activosQueTiene(db, userId);

  const snapshots = db.collection('performanceSnapshots');
  const prefijo = `${userId}_`;
  const docs = await snapshots
    .where(FieldPath.documentId(), '>=', snapshots.doc(prefijo))
    .where(FieldPath.documentId(), '<', snapshots.doc(`${prefijo}`))
    .select('type', 'ticker', 'assetType')
    .get();

  const deActivo = docs.docs.filter((d) => d.data().type === 'asset');
  const sobrantes = deActivo.filter((d) => {
    const { ticker, assetType } = d.data();
    return !tiene.has(`${ticker}_${assetType}`);
  });

  const retirados = aplicar && sobrantes.length > 0
    ? await deleteInSmallBatches(db, sobrantes.map((d) => d.ref))
    : 0;

  const resultado = {
    revisados: deActivo.length,
    sobrantes: sobrantes.map((d) => d.id),
    retirados,
    conservados: deActivo.length - sobrantes.length,
  };
  console.log(`[assetSnapshotOnDemand] ${userId}: ${resultado.revisados} resúmenes por activo, ` +
    `${sobrantes.length} de activos que ya no tiene, ${retirados} retirados` +
    `${aplicar ? '' : ' (sin aplicar)'}`);
  return resultado;
}

module.exports = {
  resumenesPorActivoBajoDemanda,
  monedasDelCierre,
  ponerAlDiaResumenDeActivo,
  activosQueTiene,
  retirarResumenesSinTenencia,
};
