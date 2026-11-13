/**
 * HU 6.8.2 — Operación ve cada mañana el estado de la corrección nocturna, y
 * el atraso le llega como aviso (Escenarios 4 y 5).
 *
 * Hasta ahora lo único que quedaba de cada noche era una línea `DONE` en los
 * logs: no constaba cuántos esperaban, desde cuándo, ni quién quedó en
 * revisión. Este módulo calcula ese estado al terminar la corrida y lo deja
 * donde operación lo encuentra, con el mismo mecanismo que el aviso del cierre
 * nocturno de la HU 6.3 (`eodFinalizationScheduled.js`):
 *
 * 1. un documento por corrida en `reconcileNights/{noche}_{HHmmss}` con los
 *    recuentos exactos y los inversores nombrados (hasta `MAX_NOMBRADOS`);
 * 2. una línea `[reconcile] ESTADO …` en el log, siempre;
 * 3. si hay atrasados o alguien pasó a revisión, un `console.error` con el
 *    prefijo `[CORRECCION-ATRASO]`, que engancha la política de alertas con el
 *    canal de operación (`scripts/correccionNocturnaAlerts.js`).
 *
 * RN-6.8.2-L: el atraso **solo se avisa**. Nada de aquí lanza otra ejecución
 * ni alarga la noche.
 *
 * @module services/reconcileNightReportService
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md (T7)
 */

const { DateTime } = require('luxon');

/**
 * Prefijo del log que engancha la política de alertas. Cambiarlo sin cambiar
 * `scripts/correccionNocturnaAlerts.js` deja el aviso mudo (lo vigila un test).
 */
const PREFIJO_ALERTA = '[CORRECCION-ATRASO]';

/** Colección con el estado de cada corrida. El cliente no la puede leer. */
const COLECCION_NOCHES = 'reconcileNights';

/** Tope de inversores nombrados por lista; los recuentos son siempre exactos. */
const MAX_NOMBRADOS = 200;

/** RN-6.8.2-K: una corrección es corta si su rango es de hasta 6 meses. */
const MESES_CORRECCION_CORTA = 6;

const ZONA = 'America/New_York';

/**
 * Desde cuándo espera un inversor (RN-6.8-P, vacío 2): la primera vez que
 * quedó pendiente. Las marcas puestas por el código anterior a esta HU no
 * tienen `pendienteDesde`; para ellas vale su `registeredAt`.
 *
 * @param {Object} stale - `portfolioPerformance/{uid}._stale`
 * @returns {string} ISO 8601, o `''` si no hay ninguna fecha
 */
function esperaDe(stale) {
  return (stale && (stale.pendienteDesde || stale.registeredAt)) || '';
}

/**
 * Orden de la cola: primero el que más lleva esperando; el empate lo resuelve
 * el `uid`, para que el orden sea siempre el mismo.
 *
 * @param {{id: string, stale: Object}} a
 * @param {{id: string, stale: Object}} b
 * @returns {number}
 */
function porEspera(a, b) {
  const ea = esperaDe(a.stale);
  const eb = esperaDe(b.stale);
  if (ea !== eb) {
    if (!ea) return 1; // sin fecha, al final
    if (!eb) return -1;
    return ea < eb ? -1 : 1;
  }
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

/**
 * Fecha de la corrida en Nueva York: la "noche" con la que se cuentan los
 * intentos y se nombra el estado.
 *
 * @param {number} ms
 * @returns {string} YYYY-MM-DD
 */
function nocheDe(ms) {
  return DateTime.fromMillis(ms, { zone: ZONA }).toISODate();
}

/**
 * El estado de una corrida, sin tocar la base: todo lo que necesita llega por
 * parámetro.
 *
 * Atrasado (RN-6.8.2-K): un inversor que **ya esperaba al empezar la corrida**
 * y sigue en la cola al terminar. Si su corrección era corta (hasta 6 meses
 * contados desde el último día cerrado), lo está siempre: no quedó lista a la
 * mañana siguiente. Si era larga, solo si esa noche no avanzó ningún tramo. Lo
 * corto o largo se mide con su `since` **al empezar la noche**: un rango largo
 * que avanzó no se vuelve corto a mitad de la cuenta.
 *
 * Los que están en revisión ya no ocupan sitio en la cola: no cuentan entre los
 * pendientes ni entre los atrasados, y figuran aparte (vacío 3).
 *
 * @param {Object} params
 * @param {string} params.noche - YYYY-MM-DD
 * @param {number} params.inicioMs
 * @param {number} params.finMs
 * @param {string} params.lastClosedDate - Último día cerrado (YYYY-MM-DD)
 * @param {Object} params.totals - Recuentos de la corrida (`runReconciliation`)
 * @param {Map<string, {sinceInicial: string, avanzo: boolean}>} params.atendidos
 * @param {Array<{id: string, stale: Object}>} params.colaFinal - La cola al terminar
 * @param {Array<{userId: string, intentos: Array, pendienteDesde: string}>} params.revisionesNuevas
 * @param {number} params.totalEnRevision
 * @param {boolean} params.tiempoAgotado
 * @returns {Object} El estado, tal como se guarda
 */
function calcularEstadoNoche({
  noche, inicioMs, finMs, lastClosedDate, totals, atendidos, colaFinal,
  revisionesNuevas, totalEnRevision, tiempoAgotado,
}) {
  const corte = DateTime.fromISO(lastClosedDate, { zone: ZONA }).minus({ months: MESES_CORRECCION_CORTA }).toISODate();
  const inicio = new Date(inicioMs).toISOString();

  const pendientes = [...colaFinal].sort(porEspera).map(({ id, stale }) => {
    const pendienteDesde = esperaDe(stale);
    const atendido = atendidos.get(id);
    const sinceInicial = atendido ? atendido.sinceInicial : stale.since;
    const tipo = sinceInicial >= corte ? 'corta' : 'larga';
    const yaEsperaba = Boolean(pendienteDesde) && pendienteDesde < inicio;
    const avanzo = Boolean(atendido && atendido.avanzo);
    return {
      userId: id,
      pendienteDesde,
      since: stale.since,
      tipo,
      avanzo,
      atrasado: yaEsperaba && (tipo === 'corta' || !avanzo),
    };
  });
  const atrasados = pendientes.filter((p) => p.atrasado);
  const sinAtrasado = ({ atrasado, ...resto }) => resto;

  return {
    noche,
    inicio,
    fin: new Date(finMs).toISOString(),
    duracionMs: finMs - inicioMs,
    tiempoAgotado,
    tramos: totals.tramos,
    vueltas: totals.rounds,
    corregidos: totals.reconciled,
    avanzaron: totals.advanced,
    fallidos: totals.failed,
    pendientes: {
      total: pendientes.length,
      masAntiguoDesde: pendientes.length > 0 ? pendientes[0].pendienteDesde : null,
      lista: pendientes.slice(0, MAX_NOMBRADOS).map(sinAtrasado),
    },
    atrasados: {
      total: atrasados.length,
      masAntiguoDesde: atrasados.length > 0 ? atrasados[0].pendienteDesde : null,
      lista: atrasados.slice(0, MAX_NOMBRADOS).map(sinAtrasado),
    },
    revision: {
      nuevos: revisionesNuevas.slice(0, MAX_NOMBRADOS),
      totalNuevos: revisionesNuevas.length,
      totalEnRevision,
    },
    truncado: Math.max(pendientes.length, revisionesNuevas.length) > MAX_NOMBRADOS,
  };
}

/** Fecha corta para el log: `2026-11-05` de un ISO, o `—` si no hay. */
function fechaCorta(iso) {
  return iso ? DateTime.fromISO(iso, { zone: ZONA }).toISODate() : '—';
}

/**
 * Guarda el estado, lo deja en el log y, si hace falta, avisa a operación.
 * Si el documento no se puede guardar, el aviso sale igual: el atraso nunca
 * pasa en silencio (RN-6-E).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} estado - Salida de `calcularEstadoNoche`
 * @returns {Promise<{docId: string, avisado: boolean}>}
 */
async function publicarEstadoNoche(db, estado) {
  const hora = DateTime.fromISO(estado.inicio, { zone: ZONA }).toFormat('HHmmss');
  const docId = `${estado.noche}_${hora}`;
  let detalle = `${COLECCION_NOCHES}/${docId}`;

  try {
    await db.collection(COLECCION_NOCHES).doc(docId).set(estado);
  } catch (error) {
    console.error(`[reconcile] No se pudo guardar el estado de la noche ${docId}: ${error.message}`);
    detalle = 'no se pudo guardar el detalle';
  }

  console.log(`[reconcile] ESTADO ${estado.noche}: ${estado.corregidos} corregidos, ` +
    `${estado.pendientes.total} pendientes (el más antiguo desde ${fechaCorta(estado.pendientes.masAntiguoDesde)}), ` +
    `${estado.atrasados.total} atrasados, ${estado.revision.totalNuevos} pasaron a revisión ` +
    `(${estado.revision.totalEnRevision} en revisión); ${estado.tramos} tramos en ${(estado.duracionMs / 1000).toFixed(1)} s. ` +
    `Detalle en ${detalle}`);

  const avisado = estado.atrasados.total > 0 || estado.revision.totalNuevos > 0;
  if (avisado) {
    const nombres = estado.revision.nuevos.map((r) => r.userId).slice(0, 10).join(', ');
    console.error(`${PREFIJO_ALERTA} ${estado.noche}: ${estado.atrasados.total} inversor(es) atrasado(s)` +
      `${estado.atrasados.total > 0 ? `, el más antiguo pendiente desde ${fechaCorta(estado.atrasados.masAntiguoDesde)}` : ''}; ` +
      `${estado.revision.totalNuevos} pasaron a revisión${nombres ? ` (${nombres})` : ''}. ` +
      `No se amplió la corrección. Detalle en ${detalle}`);
  }
  return { docId, avisado };
}

module.exports = {
  PREFIJO_ALERTA,
  COLECCION_NOCHES,
  MAX_NOMBRADOS,
  MESES_CORRECCION_CORTA,
  esperaDe,
  porEspera,
  nocheDe,
  calcularEstadoNoche,
  publicarEstadoNoche,
};
