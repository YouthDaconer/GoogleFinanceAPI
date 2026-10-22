/**
 * HU 6.3 — Los precios y las tasas del día, escritos una vez y leídos por todos.
 *
 * El cierre nocturno pasa de ser un proceso único a una tarea por inversor. En
 * cuanto el trabajo se reparte, cada tarea necesita los precios del día que se
 * está cerrando, y ninguna puede volver a pedírselos al mercado: obtendría el
 * precio **vivo del momento**, no el cierre del día objetivo, y dos inversores
 * procesados con diez minutos de diferencia tendrían cifras no comparables.
 * Eso es exactamente lo que prohíbe RN-6.3-M (Escenario 8).
 *
 * Por eso el orquestador materializa aquí, **una sola vez y antes de encolar
 * nada**, los precios y las tasas con que se va a cerrar el día. Las tareas
 * —y sus reintentos, a la hora que lleguen— leen este documento.
 *
 * **Por qué no viajan dentro de la tarea** (D2): Cloud Tasks acota el cuerpo de
 * la tarea, los precios son ~100 KB que se duplicarían por cada inversor, y un
 * dato serializado en la tarea es un dato rancio en el reintento.
 *
 * **Por qué en trozos** (D1): un documento de Firestore no pasa de 1 MiB. Con
 * los pocos cientos de símbolos de hoy sobra uno, pero la meta de la historia
 * son 5.000 inversores (RN-6.3-J) y ahí el catálogo de símbolos crece. El
 * troceado es siempre el mismo camino de código para que el caso grande no
 * estrene lógica el día que llegue.
 *
 * Inmutable: una vez el documento raíz queda en `listo`, no se sobrescribe.
 * Un relanzamiento del día lee lo que ya estaba. Si una corrida anterior murió
 * a mitad de la escritura (hay trozos pero no raíz), la siguiente sí la
 * completa: lo que se protege es un cierre terminado, no uno roto.
 *
 * @module services/eodMarketSnapshot
 * @see platform-docs/stories/6.3-cierre-nocturno-termina-siempre/refinamiento.md (T1)
 */

const COLECCION_RAIZ = 'marketData';
const DOC_EOD = 'eod';
const SUBCOLECCION_FECHAS = 'dates';
const SUBCOLECCION_TROZOS = 'chunks';

/**
 * Cotizaciones por trozo. Una cotización normalizada pesa ~350 B, así que
 * 1.500 son ~525 KB: la mitad del tope de 1 MiB de Firestore, con margen de
 * sobra para que un símbolo con metadatos largos no reviente el documento.
 */
const COTIZACIONES_POR_TROZO = 1500;

/** El documento del día está completo y se puede leer. */
const ESTADO_LISTO = 'listo';

/**
 * Referencia al documento del día: `marketData/eod/dates/{YYYY-MM-DD}`.
 *
 * Es la misma forma que `portfolioPerformance/{userId}/dates/{fecha}` ya usa en
 * el proyecto, y el único camino válido en Firestore para una ruta de documento
 * (un número par de segmentos).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha - `YYYY-MM-DD`
 * @returns {FirebaseFirestore.DocumentReference}
 */
function refDelDia(db, fecha) {
  return db
    .collection(COLECCION_RAIZ)
    .doc(DOC_EOD)
    .collection(SUBCOLECCION_FECHAS)
    .doc(fecha);
}

/**
 * Parte un arreglo en trozos de tamaño máximo.
 *
 * @param {Array} items
 * @param {number} tamano
 * @returns {Array[]}
 */
function trocear(items, tamano) {
  const trozos = [];
  for (let i = 0; i < items.length; i += tamano) {
    trozos.push(items.slice(i, i + tamano));
  }
  return trozos;
}

/**
 * Guarda los precios y las tasas con que se cierra un día (RN-6.3-M).
 *
 * Escribe primero los trozos y **al final** el documento raíz con
 * `estado: 'listo'`: hasta que esa última escritura ocurre, `leerSnapshotDelDia`
 * devuelve `null` y ninguna tarea trabaja con datos a medias.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha - Día que se cierra, `YYYY-MM-DD`
 * @param {Object} datos
 * @param {Object[]} datos.prices - Cotizaciones tal como las devuelve `getPricesFromApi`
 * @param {Object[]} datos.currencies - Divisas con tasa, tal como las devuelve `getCurrencyRatesFromApi`
 * @param {string} [datos.sourceRunId] - Identificador de la corrida que lo materializó
 * @returns {Promise<{creado: boolean, trozos: number, simbolos: number, divisas: number}>}
 *   `creado: false` si el día ya estaba cerrado y no se tocó nada.
 */
async function guardarSnapshotDelDia(db, fecha, { prices = [], currencies = [], sourceRunId = null } = {}) {
  const ref = refDelDia(db, fecha);
  const existente = await ref.get();

  if (existente.exists && existente.data().estado === ESTADO_LISTO) {
    const datos = existente.data();
    console.log(`[eodMarketSnapshot] ${fecha} ya estaba materializado (${datos.simbolos} símbolos); no se sobrescribe`);
    return {
      creado: false,
      trozos: datos.trozos || 0,
      simbolos: datos.simbolos || 0,
      divisas: (datos.currencies || []).length,
    };
  }

  const trozos = trocear(prices, COTIZACIONES_POR_TROZO);
  const trozosRef = ref.collection(SUBCOLECCION_TROZOS);

  for (let i = 0; i < trozos.length; i++) {
    await trozosRef.doc(String(i)).set({ indice: i, prices: trozos[i] });
  }

  // La raíz se escribe de última: es la señal de que el día está completo.
  await ref.set({
    date: fecha,
    estado: ESTADO_LISTO,
    trozos: trozos.length,
    simbolos: prices.length,
    currencies,
    createdAt: new Date().toISOString(),
    sourceRunId,
  });

  console.log(`[eodMarketSnapshot] ${fecha} materializado: ${prices.length} símbolos en ${trozos.length} trozo(s), ${currencies.length} divisas`);

  return { creado: true, trozos: trozos.length, simbolos: prices.length, divisas: currencies.length };
}

/**
 * Lee los precios y las tasas del día que se está cerrando.
 *
 * Lo llama cada tarea de inversor. Un reintento a otra hora lee **este mismo
 * documento**, no el mercado en vivo: es lo que hace que las cifras de todos
 * los inversores del día sean comparables entre sí (RN-6.3-M, Escenario 8).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha - `YYYY-MM-DD`
 * @returns {Promise<{prices: Object[], currencies: Object[], createdAt: string, sourceRunId: string|null}|null>}
 *   `null` si el día no se materializó o quedó a medias.
 */
async function leerSnapshotDelDia(db, fecha) {
  const ref = refDelDia(db, fecha);
  const raiz = await ref.get();

  if (!raiz.exists || raiz.data().estado !== ESTADO_LISTO) {
    return null;
  }

  const datos = raiz.data();
  const trozosSnapshot = await ref.collection(SUBCOLECCION_TROZOS).get();

  // Los trozos llegan sin orden garantizado y el índice es el que fija cuál va
  // antes. El orden importa: `currentPrices` se recorre con `find(symbol)`, y
  // un orden distinto entre dos lecturas no cambia el resultado, pero sí hace
  // irreproducible cualquier comparación posterior.
  const prices = trozosSnapshot.docs
    .map((d) => d.data())
    .sort((a, b) => (a.indice ?? 0) - (b.indice ?? 0))
    .flatMap((d) => d.prices || []);

  if (prices.length !== datos.simbolos) {
    console.warn(`[eodMarketSnapshot] ${fecha}: se esperaban ${datos.simbolos} símbolos y se leyeron ${prices.length}`);
  }

  return {
    prices,
    currencies: datos.currencies || [],
    createdAt: datos.createdAt,
    sourceRunId: datos.sourceRunId || null,
  };
}

module.exports = {
  guardarSnapshotDelDia,
  leerSnapshotDelDia,
  refDelDia,
  COTIZACIONES_POR_TROZO,
  ESTADO_LISTO,
};
