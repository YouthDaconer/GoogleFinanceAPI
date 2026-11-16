/**
 * HU 6.11.2 — Un resumen que falta o quedó viejo se prepara una sola vez.
 *
 * Preparar un resumen de portafolio o de cuenta (`generatePerformanceSnapshot`)
 * lee toda la historia del alcance. Si llegan a la vez varias consultas del
 * mismo resumen viejo —el inversor abre el gráfico mientras el Copiloto prepara
 * su análisis, o dos pestañas— cada una lo preparaba por su cuenta y cada una
 * leía la historia entera (Escenario 3, QA-11).
 *
 * Aquí las peticiones simultáneas del mismo resumen esperan **la misma**
 * preparación. Al terminar (bien o mal) se olvida: la consulta siguiente ya lo
 * encuentra escrito o en la caché en memoria de quien la pidió, y si la
 * preparación falló puede volver a intentarse.
 *
 * Límite: vale dentro de una instancia. Dos instancias distintas no se ven
 * entre sí; con la concurrencia por instancia de las funciones v2, las
 * peticiones simultáneas de un inversor caen casi siempre en la misma.
 *
 * HU 6.11.6 (T3): el mecanismo se separa en `prepararUnaVez` para que el resumen
 * por activo en USD, del que sale el detalle en una moneda poco usada, también
 * se prepare o se complete una sola vez (RN-6.11.6-J).
 *
 * @module services/snapshots/snapshotPreparationService
 * @see platform-docs/stories/6.11.2-una-cuenta-y-copiloto-sin-recorrer/refinamiento.md (T2)
 * @see platform-docs/stories/6.11.6-detalle-activo-desde-resumen-usd/refinamiento.md (T3)
 */

const { buildSnapshotDocId, generatePerformanceSnapshot } = require('../snapshotGenerator');

/** Preparaciones en curso, por id de resumen. */
const enVuelo = new Map();

/**
 * Corre `preparar`, o espera a la preparación que ya está en curso con el mismo
 * id. Al terminar (bien o mal) se olvida.
 *
 * @template T
 * @param {string} id - Id del resumen (o de lo que se prepara)
 * @param {() => Promise<T>} preparar
 * @returns {Promise<T>}
 */
function prepararUnaVez(id, preparar) {
  const enCurso = enVuelo.get(id);
  if (enCurso) {
    console.log(`[snapshotPreparation] ${id}: ya se está preparando, se espera esa preparación`);
    return enCurso;
  }

  const preparacion = (async () => {
    try {
      return await preparar();
    } finally {
      enVuelo.delete(id);
    }
  })();
  enVuelo.set(id, preparacion);
  return preparacion;
}

/**
 * Prepara el resumen de un alcance, o espera a la preparación que ya está en
 * curso para ese mismo resumen.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} userId
 * @param {string} accountId - `'overall'` o el id de la cuenta
 * @param {string} currency
 * @returns {Promise<Object|null>} El resumen preparado (o `null` si no hay historia)
 */
function prepararResumenUnaVez(db, userId, accountId, currency) {
  return prepararUnaVez(
    buildSnapshotDocId(userId, accountId, currency),
    () => generatePerformanceSnapshot(db, userId, accountId, currency),
  );
}

/** Solo para tests: cuántas preparaciones siguen en curso. */
function preparacionesEnCurso() {
  return enVuelo.size;
}

module.exports = {
  prepararUnaVez,
  prepararResumenUnaVez,
  preparacionesEnCurso,
};
