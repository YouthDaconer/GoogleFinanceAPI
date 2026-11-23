/**
 * HU 6.10.1 — Cada dato operativo tiene su plazo de conservación.
 *
 * El catálogo de **qué** se retira, **dónde** vive y **cuánto** se conserva.
 * Es puro (sin Firestore, sin red): lo usan la limpieza diaria
 * (`retentionService.js`), el borde de operación (`scripts/plazosConservacion.js`)
 * y los tests, y por eso los plazos acordados con el PO viven en un solo sitio.
 *
 * **Lo que NO está aquí, a propósito:**
 * - Los plazos que ya existían (RN de la HU: «se mantienen como están»):
 *   `userNotifications` y `alertHistory` (`alertRetention.js`) y los resúmenes
 *   de activos vendidos (`snapshotCleanup.js`). `_backfillSnapshots` (TTL de
 *   30 días) se retiró el 2026-11-23: el respaldo nunca llegó a guardarse.
 * - Lo que nunca se retira (RN-6.10-K, RN-6.10-L): pagos y eventos de
 *   suscripción, historiales de mercado y los datos de origen de la historia
 *   del inversor. Están listados en `COLECCIONES_PROTEGIDAS`, y un test
 *   comprueba que ningún tipo apunta a una de ellas.
 *
 * **La antigüedad** se cuenta en días naturales de Nueva York (estándar §4,
 * regla 10; vacío 1 de qa.md). El corte de un tipo es la fecha de la limpieza
 * menos su plazo, a las 00:00 NY, y se retira lo **anterior** al corte: con
 * la limpieza del 2026-11-12 y 90 días, el corte es el 2026-08-14, así que el
 * 2026-08-13 (91 días) se retira y el 2026-08-14 (90 días) se conserva.
 *
 * @module services/retention/retentionPolicies
 * @see platform-docs/stories/6.10.1-plazos-conservacion-datos/refinamiento.md (T1)
 */

const { DateTime } = require('luxon');
const { COLECCION_PROGRESO, SUBCOLECCION_USUARIOS } = require('../eodProgressService');
const { COLECCION_NOCHES } = require('../reconcileNightReportService');
const {
  COLECCION_RAIZ: COLECCION_MERCADO,
  DOC_EOD,
  SUBCOLECCION_FECHAS,
  SUBCOLECCION_TROZOS,
} = require('../eodMarketSnapshot');

const ZONA = 'America/New_York';

/**
 * `eodFinalizationScheduled.COLECCION_FALLOS`. No se importa de allí porque
 * ese módulo arrastra secretos y los cálculos del cierre; un test vigila que
 * los dos literales no se separen.
 */
const COLECCION_FALLOS_CIERRE = 'eodFailures';

/** Fallos de la corrección nocturna, uno por inversor (`reconcileStalePerformance.js#abandonUser`). */
const COLECCION_FALLOS_CORRECCION = 'reconcileFailures';

/** Constancia de cada limpieza (RN-6.10-N). Ella misma es un registro operativo. */
const COLECCION_CONSTANCIAS = 'dataRetentionRuns';

/** Configuración de la limpieza: qué tipos activó operación y sus últimos ensayos. */
const COLECCION_CONFIG = 'dataRetentionPolicies';
const DOC_CONFIG = 'config';

/**
 * RN-6.10.1-J: franja del cierre (00:05–00:45 ET) y de la corrección
 * (02:00 ET, hasta ≈ 02:10). Ninguna tanda arranca dentro de ella.
 */
const HORARIO_NOCTURNO = Object.freeze({ desdeHora: 0, hastaHora: 3 });

/** Plazos acordados (RN-6.10-J). */
const PLAZO = Object.freeze({
  OPERATIVO: Object.freeze({ days: 90 }),
  BRIEFINGS: Object.freeze({ years: 1 }),
  RESUMENES_ARCHIVADOS: Object.freeze({ years: 2 }),
});

/**
 * Los tipos de dato con plazo nuevo, en el orden en que se procesan: primero
 * los pequeños, para que un tipo con mucho volumen (el progreso del cierre
 * repartido) no deje a los demás sin turno dentro del presupuesto del día.
 *
 * - `ruta`: colección (o ruta de colección) donde vive.
 * - `fecha.porId`: el id del documento empieza por `YYYY-MM-DD`.
 * - `fecha.campo`: campo ISO 8601 que decide la antigüedad.
 * - `subcolecciones`: se retiran **antes** que su padre. Si la tanda se corta,
 *   el padre vencido sigue ahí y la siguiente ejecución lo retoma (RN-6-H).
 * - `registros`: qué se cuenta como «registro» en la constancia.
 *   `'subcoleccion'` cuenta los de la primera subcolección (un registro por
 *   inversor y por día, Escenario 7).
 * - `listarDocumentos`: el día se escribe con los trozos **antes** que la raíz
 *   (`eodMarketSnapshot.js`). Un día que quedó a medias no tiene raíz, y una
 *   consulta no lo devolvería. `listDocuments()` sí lo devuelve.
 */
const TIPOS = Object.freeze([
  Object.freeze({
    id: 'fallosCierre',
    descripcion: 'Fallos del cierre nocturno',
    ruta: COLECCION_FALLOS_CIERRE,
    plazo: PLAZO.OPERATIVO,
    fecha: Object.freeze({ porId: true }),
    subcolecciones: Object.freeze([]),
    registros: 'documento',
  }),
  Object.freeze({
    // RN-6.10.1-K: un inversor en revisión no tiene `resolvedAt`, así que la
    // consulta `resolvedAt < corte` nunca lo alcanza. Al liberarlo, sus 90 días
    // cuentan desde `resolvedAt`, la hora en que operación lo liberó.
    id: 'fallosCorreccion',
    descripcion: 'Fallos de la corrección nocturna (desde que operación libera al inversor)',
    ruta: COLECCION_FALLOS_CORRECCION,
    plazo: PLAZO.OPERATIVO,
    fecha: Object.freeze({ campo: 'resolvedAt' }),
    subcolecciones: Object.freeze([]),
    registros: 'documento',
  }),
  Object.freeze({
    id: 'metricasCorreccion',
    descripcion: 'Métricas internas de cada noche de corrección',
    ruta: COLECCION_NOCHES,
    plazo: PLAZO.OPERATIVO,
    fecha: Object.freeze({ porId: true }),
    subcolecciones: Object.freeze([]),
    registros: 'documento',
  }),
  Object.freeze({
    id: 'constancias',
    descripcion: 'Constancias de la propia limpieza',
    ruta: COLECCION_CONSTANCIAS,
    plazo: PLAZO.OPERATIVO,
    fecha: Object.freeze({ porId: true }),
    subcolecciones: Object.freeze([]),
    registros: 'documento',
  }),
  Object.freeze({
    id: 'briefings',
    descripcion: 'Historial de briefings enviados',
    ruta: 'briefingHistory',
    plazo: PLAZO.BRIEFINGS,
    fecha: Object.freeze({ campo: 'sentAt' }),
    subcolecciones: Object.freeze([]),
    registros: 'documento',
  }),
  Object.freeze({
    // RN-6.10.1-L: los 2 años cuentan desde que se archivó, no desde la última
    // entrada del inversor. Retirarlo no borra su historia (RN-6.10-L): su
    // resumen se prepara de nuevo desde `portfolioPerformance` al volver.
    id: 'resumenesArchivados',
    descripcion: 'Resúmenes archivados de inversores inactivos',
    ruta: 'archivedSnapshots',
    plazo: PLAZO.RESUMENES_ARCHIVADOS,
    fecha: Object.freeze({ campo: 'archivedAt' }),
    subcolecciones: Object.freeze([]),
    registros: 'documento',
  }),
  Object.freeze({
    id: 'preciosCierre',
    descripcion: 'Precios y tasas congelados del cierre de cada noche',
    ruta: `${COLECCION_MERCADO}/${DOC_EOD}/${SUBCOLECCION_FECHAS}`,
    plazo: PLAZO.OPERATIVO,
    fecha: Object.freeze({ porId: true }),
    subcolecciones: Object.freeze([SUBCOLECCION_TROZOS]),
    registros: 'documento',
    listarDocumentos: true,
  }),
  Object.freeze({
    id: 'progresoCierre',
    descripcion: 'Progreso del cierre repartido, por inversor y por día',
    ruta: COLECCION_PROGRESO,
    plazo: PLAZO.OPERATIVO,
    fecha: Object.freeze({ porId: true }),
    subcolecciones: Object.freeze([SUBCOLECCION_USUARIOS]),
    registros: 'subcoleccion',
  }),
]);

/**
 * RN-6.10-K y RN-6.10-L: nunca se retiran, tengan la antigüedad que tengan.
 * Pagos y suscripciones viven en `userData.subscription` y `subscriptionEvents`;
 * los historiales de mercado, en `indexHistories`, `sectorsHistory`,
 * `currentPrices`, `indexCache` y `benchmarks`; los datos de origen de la
 * historia del inversor, en `portfolioPerformance`, `transactions`, `assets`
 * y `portfolioAccounts`.
 */
const COLECCIONES_PROTEGIDAS = Object.freeze([
  'subscriptionEvents',
  'userData',
  'indexHistories',
  'sectorsHistory',
  'currentPrices',
  'indexCache',
  'benchmarks',
  'portfolioPerformance',
  'transactions',
  'assets',
  'portfolioAccounts',
]);

/**
 * @param {string} id
 * @returns {Object|undefined} El tipo del catálogo con ese id
 */
function tipoPorId(id) {
  return TIPOS.find((t) => t.id === id);
}

/**
 * El corte de un tipo: se retira lo anterior a él.
 *
 * @param {Object} tipo - Un elemento de `TIPOS`
 * @param {Date} ahora - Hora de la limpieza
 * @returns {{fecha: string, iso: string}} `fecha` (`YYYY-MM-DD`) para los ids,
 *   `iso` (00:00 NY en UTC) para los campos de fecha
 */
function corteDe(tipo, ahora) {
  const corte = DateTime.fromJSDate(ahora, { zone: ZONA }).startOf('day').minus(tipo.plazo);
  return { fecha: corte.toISODate(), iso: corte.toUTC().toISO() };
}

/**
 * RN-6.10.1-J: ¿cae `ahora` en la franja del cierre y la corrección?
 *
 * @param {Date} ahora
 * @returns {boolean}
 */
function enHorarioNocturno(ahora) {
  const hora = DateTime.fromJSDate(ahora, { zone: ZONA }).hour;
  return hora >= HORARIO_NOCTURNO.desdeHora && hora < HORARIO_NOCTURNO.hastaHora;
}

/**
 * Tramo de antigüedad del conteo (vacío 7 de qa.md): el mes, en NY.
 *
 * @param {string} valor - Id que empieza por `YYYY-MM-DD`, o fecha ISO 8601
 * @returns {string} `YYYY-MM`, o `'sin-fecha'`
 */
function tramoDe(valor) {
  if (typeof valor !== 'string' || valor.length < 7) return 'sin-fecha';
  if (!valor.includes('T')) return valor.slice(0, 7);
  const fecha = DateTime.fromISO(valor, { zone: ZONA });
  return fecha.isValid ? fecha.toFormat('yyyy-MM') : 'sin-fecha';
}

/**
 * El valor que decide la antigüedad de un documento.
 *
 * @param {Object} tipo
 * @param {string} id
 * @param {Object} [datos]
 * @returns {string|undefined}
 */
function fechaDelDocumento(tipo, id, datos) {
  return tipo.fecha.porId ? id : (datos || {})[tipo.fecha.campo];
}

/**
 * Id de una constancia: `{fecha}_{HHmmss}` en NY, como `reconcileNights`.
 *
 * @param {Date} ahora
 * @returns {string}
 */
function idDeConstancia(ahora) {
  return DateTime.fromJSDate(ahora, { zone: ZONA }).toFormat('yyyy-MM-dd_HHmmss');
}

module.exports = {
  TIPOS,
  PLAZO,
  COLECCIONES_PROTEGIDAS,
  COLECCION_FALLOS_CIERRE,
  COLECCION_FALLOS_CORRECCION,
  COLECCION_CONSTANCIAS,
  COLECCION_CONFIG,
  DOC_CONFIG,
  HORARIO_NOCTURNO,
  ZONA,
  tipoPorId,
  corteDe,
  enHorarioNocturno,
  tramoDe,
  fechaDelDocumento,
  idDeConstancia,
};
