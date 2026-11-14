/**
 * HU 6.7 — La medición de los cuatro consumos vigilados.
 *
 * - **Lecturas, escrituras y almacenamiento** salen de las métricas que
 *   Firestore ya publica en Cloud Monitoring (`timeSeries.list`). No hay código
 *   nuevo de medición: se pide la serie por minuto del día de Colombia y se
 *   suma (RN-6-I: el consumo se mide, no se supone).
 * - **Los análisis de IA** no son una métrica: salen de Firestore. Los
 *   candidatos son los usuarios con `lastCopilotAnalysis` reciente (lo escribe
 *   `copilotQuota.consumeQuota` al cerrar cada análisis completado), y de cada
 *   uno se leen sus análisis de los últimos 8 días. Así no hace falta un índice
 *   de grupo de colecciones.
 *
 * El token de Monitoring llega por parámetro: en la función es la credencial
 * del runtime (ADC); en los scripts, el de `gcloud`.
 *
 * @module services/consumption/consumptionMetricsService
 * @see platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md (T2)
 */

const { COPILOT_ANALYSES_SUBCOLLECTION, DIAS_ENTRE_ANALISIS } = require('../copilot/copilotConstants');
const { tieneAcceso } = require('../copilot/copilotQuota');
const { CONSUMO, VENTANA_CANDIDATOS_HORAS, intervaloDelDia } = require('./consumptionAlertService');

/** Proyecto de GCP. Solo hay producción. */
const PROYECTO = 'portafolio-inversiones';

const URL_SERIES = `https://monitoring.googleapis.com/v3/projects/${PROYECTO}/timeSeries`;

/** Métricas de Firestore en Cloud Monitoring. */
const METRICAS = Object.freeze({
  [CONSUMO.LECTURAS]: 'firestore.googleapis.com/document/read_count',
  [CONSUMO.ESCRITURAS]: 'firestore.googleapis.com/document/write_count',
  [CONSUMO.ALMACENAMIENTO]: 'firestore.googleapis.com/storage/data_and_index_storage_bytes',
});

/** Un punto por minuto: la resolución con la que se calcula la hora del cruce. */
const PERIODO_SEGUNDOS = 60;

/** El almacenamiento se mira en las últimas 24 h para saber desde cuándo está así. */
const HORAS_ALMACENAMIENTO = 24;

const MS_HORA = 60 * 60 * 1000;

/**
 * Los parámetros de `timeSeries.list` para una métrica: una sola serie (suma de
 * todas) con un punto por minuto.
 *
 * @param {object} p
 * @param {string} p.metrica - Tipo de la métrica
 * @param {string} p.inicio - ISO
 * @param {string} p.fin - ISO
 * @param {'ALIGN_SUM'|'ALIGN_MEAN'} p.alineador - Suma para contadores, media para el almacenamiento
 * @param {string} [p.pageToken]
 * @returns {string} Query string
 */
function consultaSerie({ metrica, inicio, fin, alineador, pageToken }) {
  const params = new URLSearchParams({
    filter: `metric.type="${metrica}"`,
    'interval.startTime': inicio,
    'interval.endTime': fin,
    'aggregation.alignmentPeriod': `${PERIODO_SEGUNDOS}s`,
    'aggregation.perSeriesAligner': alineador,
    'aggregation.crossSeriesReducer': 'REDUCE_SUM',
  });
  if (pageToken) params.set('pageToken', pageToken);
  return params.toString();
}

/**
 * Convierte la respuesta de Monitoring en una serie en orden. Monitoring
 * devuelve los puntos del más nuevo al más viejo; si llegara más de una serie,
 * se suman por minuto.
 *
 * @param {Array<object>} series - `timeSeries` de la respuesta
 * @returns {Array<{inicio: string, fin: string, valor: number}>}
 */
function puntosDeSeries(series) {
  const porMinuto = new Map();
  for (const serie of series || []) {
    for (const punto of serie.points || []) {
      const fin = punto.interval.endTime;
      const v = punto.value || {};
      const valor = Number(v.int64Value !== undefined ? v.int64Value : v.doubleValue || 0);
      porMinuto.set(fin, (porMinuto.get(fin) || 0) + valor);
    }
  }
  return [...porMinuto.entries()]
    .map(([fin, valor]) => ({
      inicio: new Date(Date.parse(fin) - PERIODO_SEGUNDOS * 1000).toISOString(),
      fin: new Date(Date.parse(fin)).toISOString(),
      valor,
    }))
    .sort((a, b) => Date.parse(a.fin) - Date.parse(b.fin));
}

/**
 * Lee una serie de Monitoring, con todas sus páginas.
 *
 * @param {object} p
 * @param {string} p.token
 * @param {string} p.metrica
 * @param {string} p.inicio
 * @param {string} p.fin
 * @param {'ALIGN_SUM'|'ALIGN_MEAN'} p.alineador
 * @param {Function} [p.fetchFn]
 * @returns {Promise<Array<{inicio: string, fin: string, valor: number}>>}
 */
async function leerSerie({ token, metrica, inicio, fin, alineador, fetchFn = fetch }) {
  const series = [];
  let pageToken;
  do {
    const respuesta = await fetchFn(`${URL_SERIES}?${consultaSerie({ metrica, inicio, fin, alineador, pageToken })}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const texto = await respuesta.text();
    if (!respuesta.ok) {
      throw new Error(`Monitoring ${metrica} → ${respuesta.status}: ${texto.slice(0, 300)}`);
    }
    const json = texto ? JSON.parse(texto) : {};
    series.push(...(json.timeSeries || []));
    pageToken = json.nextPageToken;
  } while (pageToken);
  return puntosDeSeries(series);
}

/**
 * Lecturas o escrituras de un día de Colombia, minuto a minuto, hasta `hasta`
 * (por defecto, hasta el final del día).
 *
 * @param {object} p
 * @param {string} p.token
 * @param {'lecturas'|'escrituras'} p.consumo
 * @param {string} p.dia - `YYYY-MM-DD` de Colombia
 * @param {Date} [p.hasta]
 * @param {Function} [p.fetchFn]
 * @returns {Promise<Array<{inicio: string, fin: string, valor: number}>>}
 */
async function medirConteoDelDia({ token, consumo, dia, hasta, fetchFn }) {
  const metrica = METRICAS[consumo];
  if (!metrica || consumo === CONSUMO.ALMACENAMIENTO) throw new Error(`No es un conteo diario: ${consumo}`);
  const { inicio, fin } = intervaloDelDia(dia);
  const finReal = hasta && hasta.getTime() < Date.parse(fin) ? hasta.toISOString() : fin;
  // Al filo de la medianoche el día aún no tiene ni un minuto: Monitoring
  // rechaza un intervalo vacío.
  if (Date.parse(finReal) - Date.parse(inicio) < PERIODO_SEGUNDOS * 1000) return [];
  return leerSerie({ token, metrica, inicio, fin: finReal, alineador: 'ALIGN_SUM', fetchFn });
}

/**
 * Almacenamiento en bytes de las últimas 24 h hasta `hasta`, minuto a minuto.
 *
 * @param {object} p
 * @param {string} p.token
 * @param {Date} p.hasta
 * @param {number} [p.horas]
 * @param {Function} [p.fetchFn]
 * @returns {Promise<Array<{inicio: string, fin: string, valor: number}>>}
 */
async function medirAlmacenamiento({ token, hasta, horas = HORAS_ALMACENAMIENTO, fetchFn }) {
  return leerSerie({
    token,
    metrica: METRICAS[CONSUMO.ALMACENAMIENTO],
    inicio: new Date(hasta.getTime() - horas * MS_HORA).toISOString(),
    fin: hasta.toISOString(),
    alineador: 'ALIGN_MEAN',
    fetchFn,
  });
}

/**
 * Los usuarios con análisis recientes y sus análisis de la ventana del cupo.
 *
 * Solo trae lo que el aviso necesita: el identificador, si tiene acceso al
 * Copilot al evaluar (D2) y la fecha de cada análisis. Ningún otro dato del
 * usuario sale de aquí (RN-6.7-O).
 *
 * @param {object} p
 * @param {FirebaseFirestore.Firestore} p.db
 * @param {Date} p.ahora
 * @param {number} [p.horas] - Ventana de candidatos (por defecto 26 h)
 * @returns {Promise<Array<{uid: string, conAcceso: boolean, analisis: Array<{id: string, createdAt: string}>}>>}
 */
async function leerUsuariosIa({ db, ahora, horas = VENTANA_CANDIDATOS_HORAS }) {
  const desdeCandidatos = new Date(ahora.getTime() - horas * MS_HORA).toISOString();
  // Un análisis de hace `horas` necesita mirar los 7 días anteriores a él.
  const desdeAnalisis = new Date(ahora.getTime() - (horas + DIAS_ENTRE_ANALISIS * 24) * MS_HORA).toISOString();
  const hastaIso = ahora.toISOString();

  const candidatos = await db.collection('userData')
    .where('lastCopilotAnalysis', '>=', desdeCandidatos)
    .get();

  const usuarios = [];
  for (const doc of candidatos.docs) {
    const snap = await doc.ref.collection(COPILOT_ANALYSES_SUBCOLLECTION)
      .where('createdAt', '>=', desdeAnalisis)
      .get();
    const analisis = snap.docs
      .map((a) => ({ id: a.id, createdAt: a.data().createdAt }))
      .filter((a) => typeof a.createdAt === 'string' && a.createdAt <= hastaIso);
    usuarios.push({ uid: doc.id, conAcceso: tieneAcceso(doc.data(), ahora), analisis });
  }
  return usuarios;
}

module.exports = {
  PROYECTO,
  METRICAS,
  PERIODO_SEGUNDOS,
  consultaSerie,
  puntosDeSeries,
  leerSerie,
  medirConteoDelDia,
  medirAlmacenamiento,
  leerUsuariosIa,
};
