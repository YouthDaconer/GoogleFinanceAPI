/**
 * HU 6.9.3 — Lecturas acotadas: se lee sólo lo que el cálculo usa.
 *
 * Dos lecturas crecían con lo acumulado y no con lo que se pedía:
 * - la búsqueda nocturna de lotes vendidos leía **todos** los lotes vendidos de
 *   la plataforma cada noche en que alguien vendía (RN-6.9.3-J);
 * - la atribución leía **todos** los dividendos de cada cuenta, fuera cual fuera
 *   el periodo consultado (RN-6.9.3-K).
 *
 * Cada una tiene ahora dos caminos: la lectura **acotada** (la nueva) y la
 * lectura **de siempre**, que se conserva intacta. Este módulo decide cuál se
 * usa y, si se pide, compara las dos antes de fiarse de la nueva. El cálculo no
 * cambia: los dos caminos entregan los mismos documentos y en el mismo orden
 * (RN-6-A).
 *
 * Interruptores. Se leen en cada llamada y no al cargar el módulo, para que los
 * tests puedan cambiarlos; en producción vienen de `functions/.env` y cambian al
 * volver a publicar la función:
 * - `SCOPED_READS_ENABLED=false` — vuelve a las lecturas de siempre (RN-6-H).
 * - `SCOPED_READS_VERIFY=true` — hace las dos lecturas y las compara documento
 *   por documento. Ante cualquier diferencia usa la de siempre, lo avisa con
 *   `[scopedReads] DIFERENCIA` y no vuelve a usar la acotada de esa área en la
 *   instancia (RN-6-B).
 *
 * Es el mismo patrón de interruptor y verificación de la HU 6.9.1
 * (`portafolio-inversiones/lib/closedPositionsSourceCache.ts`), llevado al
 * servidor.
 *
 * @module utils/scopedReads
 * @see platform-docs/stories/6.9.3-solo-periodo-y-ventas-del-dia/refinamiento.md (T1)
 */

/** Áreas con lectura acotada. El bloqueo por una diferencia es por área. */
const AREAS = Object.freeze({
  LOTES_VENDIDOS: 'lotes-vendidos',
  DIVIDENDOS: 'dividendos',
});

/** Qué camino se usó, para los registros y la medición (RN-6-I). */
const MODOS = Object.freeze({
  /** Lectura acotada. */
  ACOTADA: 'acotada',
  /** Lectura acotada, comparada con la de siempre sin diferencias. */
  VERIFICADA: 'acotada-verificada',
  /** Interruptor apagado: la lectura de siempre (RN-6-H). */
  COMPLETA: 'completa',
  /** Una diferencia anterior bloqueó el área en esta instancia (RN-6-B). */
  BLOQUEADA: 'completa-bloqueada',
  /** La comparación encontró una diferencia ahora mismo (RN-6-B). */
  POR_DIFERENCIA: 'completa-por-diferencia',
  /** La lectura acotada falló (p. ej. su índice aún no existe). */
  POR_ERROR: 'completa-por-error',
});

/** Áreas bloqueadas en esta instancia por una diferencia. */
const bloqueos = new Set();

/**
 * @returns {{acotadas: boolean, verificar: boolean}}
 */
function leerInterruptores() {
  return {
    acotadas: process.env.SCOPED_READS_ENABLED !== 'false',
    verificar: process.env.SCOPED_READS_VERIFY === 'true',
  };
}

/**
 * Ordena por identificador de documento.
 *
 * Firestore devuelve una consulta con sólo igualdades en orden de identificador.
 * Una consulta por rango de fecha (ordena por fecha) o una lectura por
 * identificadores (respeta el orden pedido) no. Reordenar hace que el cálculo
 * reciba las entradas en el mismo orden que antes, y con eso la cifra sale
 * idéntica en todos sus decimales, no sólo al céntimo.
 *
 * @template T
 * @param {Array<T & {id: string}>} docs
 * @returns {Array<T & {id: string}>} Copia ordenada
 */
function ordenarPorId(docs) {
  return docs.slice().sort((a, b) => {
    if (a.id < b.id) return -1;
    if (a.id > b.id) return 1;
    return 0;
  });
}

/**
 * Compara dos listas de documentos por identificador y en orden.
 *
 * @param {Array<{id: string}>} esperado - Lo que entrega la lectura de siempre
 * @param {Array<{id: string}>} obtenido - Lo que entrega la acotada
 * @returns {{iguales: boolean, faltan: string[], sobran: string[]}} `faltan`:
 *   en la de siempre y no en la acotada; `sobran`: al revés. Con los mismos
 *   documentos en otro orden, `iguales` es false y las dos listas vienen vacías.
 */
function compararPorId(esperado, obtenido) {
  const idsEsperados = esperado.map((d) => d.id);
  const idsObtenidos = obtenido.map((d) => d.id);
  const enObtenido = new Set(idsObtenidos);
  const enEsperado = new Set(idsEsperados);
  const iguales = idsEsperados.length === idsObtenidos.length
    && idsEsperados.every((id, i) => id === idsObtenidos[i]);
  return {
    iguales,
    faltan: idsEsperados.filter((id) => !enObtenido.has(id)),
    sobran: idsObtenidos.filter((id) => !enEsperado.has(id)),
  };
}

/** Lista corta de identificadores para un aviso: no se vuelcan miles. */
function resumirIds(ids, maximo = 10) {
  if (ids.length <= maximo) return `[${ids.join(', ')}]`;
  return `[${ids.slice(0, maximo).join(', ')}, … y ${ids.length - maximo} más]`;
}

/**
 * @typedef {Object} Lectura
 * @property {Array<{id: string, valor: *}>} docs - Documentos que entrega, con
 *   el identificador del documento y el valor que consume el llamador
 * @property {number} lecturas - Documentos que leyó de Firestore (la medición)
 */

/**
 * Elige el camino de una lectura según los interruptores.
 *
 * @param {Object} params
 * @param {string} params.area - Una de `AREAS`
 * @param {() => Promise<Lectura>} params.acotada - La lectura nueva
 * @param {() => Promise<Lectura>} params.completa - La lectura de siempre
 * @param {(docs: Array<{id: string, valor: *}>) => Array<{id: string, valor: *}>} [params.comparable]
 *   - Restringe la lectura de siempre a lo que el cálculo usa antes de comparar.
 *   Hace falta cuando la de siempre traía documentos que nunca movían una cifra
 *   (los dividendos posteriores al fin de un rango).
 * @returns {Promise<Lectura & {modo: string, lecturasCompleta?: number, lecturasAcotada?: number,
 *   diferencias?: {faltan: string[], sobran: string[]}, error?: string}>}
 */
async function leerAcotado({ area, acotada, completa, comparable = (docs) => docs }) {
  const { acotadas, verificar } = leerInterruptores();

  if (!acotadas) return { ...(await completa()), modo: MODOS.COMPLETA };
  if (bloqueos.has(area)) return { ...(await completa()), modo: MODOS.BLOQUEADA };

  if (!verificar) {
    try {
      return { ...(await acotada()), modo: MODOS.ACOTADA };
    } catch (error) {
      console.warn(`[scopedReads] ${area}: la lectura acotada falló (${error.message}); se usa la de siempre`);
      return { ...(await completa()), modo: MODOS.POR_ERROR, error: error.message };
    }
  }

  // Verificación: las dos lecturas a la vez. Si la de siempre falla, falla todo,
  // como antes de la historia.
  const [deSiempre, nueva] = await Promise.allSettled([completa(), acotada()]);
  if (deSiempre.status === 'rejected') throw deSiempre.reason;
  if (nueva.status === 'rejected') {
    console.warn(`[scopedReads] ${area}: la lectura acotada falló (${nueva.reason.message}); se usa la de siempre`);
    return { ...deSiempre.value, modo: MODOS.POR_ERROR, error: nueva.reason.message };
  }

  const comparacion = compararPorId(comparable(deSiempre.value.docs), nueva.value.docs);
  const medicion = { lecturasCompleta: deSiempre.value.lecturas, lecturasAcotada: nueva.value.lecturas };

  if (!comparacion.iguales) {
    bloqueos.add(area);
    console.warn(`[scopedReads] DIFERENCIA en ${area}: faltan ${resumirIds(comparacion.faltan)}, `
      + `sobran ${resumirIds(comparacion.sobran)}${comparacion.faltan.length + comparacion.sobran.length === 0 ? ' (otro orden)' : ''}. `
      + 'Se usa la lectura de siempre y la acotada queda bloqueada en esta instancia (RN-6-B)');
    return {
      ...deSiempre.value,
      ...medicion,
      modo: MODOS.POR_DIFERENCIA,
      diferencias: { faltan: comparacion.faltan, sobran: comparacion.sobran },
    };
  }

  return { ...nueva.value, ...medicion, modo: MODOS.VERIFICADA };
}

/** Texto de la medición para los registros: «antes X, después Y» cuando se verificó. */
function describirMedicion(resultado) {
  const base = `${resultado.lecturas} documentos leídos (${resultado.modo})`;
  if (resultado.lecturasCompleta === undefined) return base;
  return `${base} — antes ${resultado.lecturasCompleta}, después ${resultado.lecturasAcotada}`;
}

module.exports = {
  AREAS,
  MODOS,
  leerInterruptores,
  ordenarPorId,
  compararPorId,
  leerAcotado,
  describirMedicion,
  estaBloqueada: (area) => bloqueos.has(area),
  // Para tests: el bloqueo vive lo que vive la instancia
  _reiniciarBloqueos: () => bloqueos.clear(),
};
