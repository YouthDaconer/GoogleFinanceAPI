/**
 * HU 6.9.3 — El cierre busca sólo los lotes de las ventas del día.
 *
 * Cada lote es un documento de `assets`: al venderse entero queda con
 * `isActive: false`, y la venta guarda su `assetId`. Para valorar las ventas de
 * la noche, el cierre necesita esos lotes ya cerrados.
 *
 * **Antes**, en cuanto había una sola venta, el cierre leía **todos** los lotes
 * vendidos (de toda la plataforma en el proceso único y de todo el inversor en
 * el reparto de la HU 6.3) y se quedaba en memoria con los de esas ventas. Esa
 * lectura crecía con las ventas históricas de todos: cada inversor nuevo
 * encarecía la noche de los demás.
 *
 * **Ahora** se leen por identificador sólo los lotes de las ventas del día
 * (RN-6.9.3-J, RN-6.9-M). Sin ventas no se lee ninguno (Escenario 5). El
 * resultado es exactamente el de antes, con los mismos documentos y en el mismo
 * orden (RN-6-A).
 *
 * La lectura de siempre se conserva aquí, movida tal cual desde los dos
 * cierres, detrás de los interruptores de `utils/scopedReads`.
 *
 * @module services/eod/soldLotsService
 * @see platform-docs/stories/6.9.3-solo-periodo-y-ventas-del-dia/refinamiento.md (T2, T3)
 */

const { AREAS, leerAcotado, ordenarPorId, describirMedicion } = require('../../utils/scopedReads');

/** Documentos por llamada a `getAll`: acota el tamaño de cada petición. */
const TAMANO_GETALL = 100;

/** Modo de una noche sin ventas: no se lee ningún lote (Escenario 5). */
const MODO_SIN_VENTAS = 'sin-ventas';

/**
 * Identificadores con los que se puede pedir un documento. Un `assetId` que no
 * es texto, o que trae `/`, nunca coincidía con un lote en la lectura de
 * siempre (que compara con el id del documento), así que descartarlo no cambia
 * el resultado y evita que `doc()` lance.
 */
function idsValidos(assetIds) {
  return [...new Set((assetIds || []).filter((id) => typeof id === 'string' && id.length > 0 && !id.includes('/')))];
}

/** Forma que consume el cierre. Igual que antes: los campos del doc pisan el id. */
const comoActivo = (doc) => ({ id: doc.id, ...doc.data() });

/**
 * Orden de la lectura de siempre: por identificador y, en el reparto, grupo de
 * cuentas por grupo de cuentas (cada grupo era una consulta).
 */
function enOrdenDeSiempre(docs, lotesDeCuentas) {
  if (!lotesDeCuentas) return ordenarPorId(docs);
  return lotesDeCuentas.flatMap((lote) => ordenarPorId(docs.filter((d) => lote.includes(d.valor.portfolioAccount))));
}

/**
 * La lectura acotada: los lotes de las ventas, por identificador.
 *
 * @returns {Promise<import('../utils/scopedReads').Lectura>}
 */
async function leerLotesDeLasVentas(db, ids, vendidos, lotesDeCuentas) {
  const coleccion = db.collection('assets');
  const docs = [];
  // Troceo local: `backfillCoreModule#chunkArray` arrastraría ese módulo (y la
  // inicialización de Admin) al cierre de un solo proceso sólo para esto.
  for (let i = 0; i < ids.length; i += TAMANO_GETALL) {
    const grupo = ids.slice(i, i + TAMANO_GETALL);
    const snapshots = await db.getAll(...grupo.map((id) => coleccion.doc(id)));
    for (const snapshot of snapshots) {
      if (!snapshot.exists) continue;
      const activo = comoActivo(snapshot);
      if (activo.isActive !== false || !vendidos.has(activo.id)) continue;
      docs.push({ id: snapshot.id, valor: activo });
    }
  }
  // Firestore cobra cada documento pedido, exista o no.
  return { docs: enOrdenDeSiempre(docs, lotesDeCuentas), lecturas: ids.length };
}

/**
 * La lectura de siempre, movida sin cambios desde los dos cierres: todos los
 * lotes vendidos (de la plataforma, o de las cuentas del inversor) y en memoria
 * los de las ventas.
 *
 * @returns {Promise<import('../utils/scopedReads').Lectura>}
 */
async function leerTodosLosVendidos(db, vendidos, lotesDeCuentas) {
  const docs = [];
  let lecturas = 0;
  const consultas = lotesDeCuentas
    ? lotesDeCuentas.map((lote) => db.collection('assets').where('portfolioAccount', 'in', lote).where('isActive', '==', false))
    : [db.collection('assets').where('isActive', '==', false)];

  for (const consulta of consultas) {
    const snapshot = await consulta.get();
    lecturas += snapshot.docs.length;
    for (const doc of snapshot.docs) {
      const activo = comoActivo(doc);
      if (vendidos.has(activo.id)) docs.push({ id: doc.id, valor: activo });
    }
  }
  return { docs, lecturas };
}

/**
 * Los lotes vendidos de las ventas de la ventana del cierre.
 *
 * @param {Object} params
 * @param {FirebaseFirestore.Firestore} params.db
 * @param {string[]} params.assetIds - `assetId` de las ventas de la ventana
 * @param {string[][]|null} [params.lotesDeCuentas] - Grupos de cuentas del
 *   inversor (reparto de la HU 6.3). Sin ellos, el ámbito es la plataforma
 *   (proceso único).
 * @param {string} [params.etiqueta] - Quién pregunta, para el registro
 * @returns {Promise<{activos: Object[], lecturas: number, modo: string,
 *   lecturasCompleta?: number, lecturasAcotada?: number, diferencias?: Object}>}
 *   `activos`: los lotes vendidos con la forma `{ id, ...data }` que espera el
 *   cálculo, en el orden de siempre. Los tres últimos campos sólo llegan en
 *   verificación.
 */
async function cargarLotesVendidos({ db, assetIds, lotesDeCuentas = null, etiqueta = 'cierre' }) {
  const ids = idsValidos(assetIds);
  if (ids.length === 0) return { activos: [], lecturas: 0, modo: MODO_SIN_VENTAS };

  const vendidos = new Set(ids);
  const resultado = await leerAcotado({
    area: AREAS.LOTES_VENDIDOS,
    acotada: () => leerLotesDeLasVentas(db, ids, vendidos, lotesDeCuentas),
    completa: () => leerTodosLosVendidos(db, vendidos, lotesDeCuentas),
  });

  const activos = resultado.docs.map((d) => d.valor);
  console.log(`[soldLotsService] ${etiqueta}: ${activos.length} lotes vendidos para ${ids.length} activos con venta, `
    + describirMedicion(resultado));

  return {
    activos,
    lecturas: resultado.lecturas,
    modo: resultado.modo,
    // Sólo en verificación: la medición antes/después y lo que difería (RN-6-I)
    lecturasCompleta: resultado.lecturasCompleta,
    lecturasAcotada: resultado.lecturasAcotada,
    diferencias: resultado.diferencias,
  };
}

module.exports = {
  cargarLotesVendidos,
  TAMANO_GETALL,
  MODO_SIN_VENTAS,
};
