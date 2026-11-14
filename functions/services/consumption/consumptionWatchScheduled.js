/**
 * HU 6.7 — El vigilante del consumo: un aviso el mismo día, antes de que llegue
 * a la factura.
 *
 * Cada 15 minutos mide los cuatro consumos vigilados, los compara con su umbral
 * (RN-6.7-J) y, si alguno lo cruzó y no se avisó todavía hoy, escribe una línea
 * de log `[CONSUMO-AVISO]`. Una política de Cloud Monitoring basada en logs, una
 * por tipo de aviso, la convierte en un correo al destinatario del aviso de
 * presupuesto y al buzón de operación (`scripts/consumoAlerts.js`). Es el
 * mecanismo de las HU 6.3 y 6.8.2.
 *
 * **Por qué un evaluador y no políticas de umbral nativas**: la ventana de
 * Monitoring es móvil, no un día de calendario. Con 450.000 lecturas el día D1,
 * la suma de las últimas 24 h seguiría por encima el D2 a las 11:00, el
 * incidente seguiría abierto y el aviso del D2 no llegaría (QA-05). Y el cupo de
 * IA por usuario no es una métrica.
 *
 * **Primero el log y después el estado.** Si guardar el estado falla, el aviso
 * se repite en la ejecución siguiente; nunca se pierde en silencio. Si no se
 * puede medir un consumo, no se marca nada y se reintenta 15 min después
 * (Vacío 6).
 *
 * Estado en Firestore (solo de servidor):
 * - `consumptionAlerts/{YYYY-MM-DD}` (día de Colombia): qué se avisó hoy de
 *   lecturas, escrituras e IA, con la lista completa de usuarios de IA (D5).
 * - `consumptionAlerts/estado`: si el almacenamiento está por encima, desde
 *   cuándo y el último día de cruce y de normalización.
 *
 * @module services/consumption/consumptionWatchScheduled
 * @see platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md (T3)
 */

const { onSchedule } = require('firebase-functions/v2/scheduler');
const admin = require('../firebaseAdmin');
const reglas = require('./consumptionAlertService');
const metricas = require('./consumptionMetricsService');

const { CONSUMO, COLECCION_AVISOS, DOC_ESTADO } = reglas;

const PREFIJO_LOG = '[consumptionWatch]';

const MS_DIA = 24 * 60 * 60 * 1000;

/** Ventana al empezar el día en que se cierra el total del día anterior. */
const MS_CIERRE_DIA_ANTERIOR = 30 * 60 * 1000;

const FUNCTION_CONFIG = {
  // Cada 15 min: el peor caso de RN-6.7-K son ~25 min (hasta 4 de retraso de
  // las métricas de Firestore, 15 hasta la siguiente ejecución y ~5 de la
  // política de log). El horario no depende de la zona; el día del aviso se
  // cuenta en Colombia dentro de las reglas.
  schedule: '*/15 * * * *',
  timeZone: 'America/New_York',
  memory: '256MiB',
  timeoutSeconds: 120,
  maxInstances: 1,
  minInstances: 0,
  labels: {
    status: 'active',
    purpose: 'consumption-watch',
  },
};

/**
 * Escribe la línea del aviso. `console.warn` con JSON: Cloud Logging la guarda
 * como `jsonPayload` con severidad WARNING.
 *
 * @param {object} aviso
 * @returns {object} La línea escrita
 */
function emitirAviso(aviso) {
  const linea = reglas.lineaAviso(aviso);
  console.warn(JSON.stringify(linea));
  return linea;
}

/**
 * Token de Monitoring con la credencial del runtime (ADC).
 *
 * @returns {Promise<string>}
 */
async function tokenDelRuntime() {
  const { access_token: token } = await admin.credential.applicationDefault().getAccessToken();
  return token;
}

/**
 * Lee un documento; `{}` si no existe.
 *
 * @param {FirebaseFirestore.DocumentReference} ref
 * @returns {Promise<object>}
 */
async function leerDoc(ref) {
  const snap = await ref.get();
  return snap.exists ? snap.data() || {} : {};
}

/**
 * Una ejecución del vigilante.
 *
 * @param {object} p
 * @param {FirebaseFirestore.Firestore} p.db
 * @param {string} p.token - Token de Monitoring
 * @param {Date} [p.ahora]
 * @param {object} [p.medicion] - Inyectable en tests (por defecto, `consumptionMetricsService`)
 * @param {Function} [p.emitir] - Inyectable en tests (por defecto, `emitirAviso`)
 * @returns {Promise<{dia: string, medidos: object, avisos: string[], fallos: string[]}>}
 */
async function vigilarConsumo({ db, token, ahora = new Date(), medicion = metricas, emitir = emitirAviso }) {
  const hoy = reglas.diaColombia(ahora);
  const coleccion = db.collection(COLECCION_AVISOS);
  const refHoy = coleccion.doc(hoy);
  const estadoHoy = await leerDoc(refHoy);
  const resultado = { dia: hoy, medidos: {}, avisos: [], fallos: [] };

  // Lecturas y escrituras: total del día de Colombia (Vacío 2). En la primera
  // media hora del día se cierra también el anterior: la ejecución de las 23:45
  // solo ve hasta ~23:41 (retraso de las métricas), y un cruce en los últimos
  // minutos del día se perdería al reiniciarse el total.
  const dias = [{ dia: hoy, ref: refHoy, estado: estadoHoy }];
  const ayer = reglas.diaColombia(new Date(ahora.getTime() - MS_DIA));
  let estadoAyer = null;
  const leerAyer = async () => {
    if (!estadoAyer) estadoAyer = await leerDoc(coleccion.doc(ayer));
    return estadoAyer;
  };
  if (ahora.getTime() - Date.parse(reglas.intervaloDelDia(hoy).inicio) < MS_CIERRE_DIA_ANTERIOR) {
    dias.push({ dia: ayer, ref: coleccion.doc(ayer), estado: await leerAyer() });
  }

  for (const consumo of [CONSUMO.LECTURAS, CONSUMO.ESCRITURAS]) {
    for (const { dia, ref, estado } of dias) {
      try {
        const puntos = await medicion.medirConteoDelDia({ token, consumo, dia, hasta: ahora });
        if (dia === hoy) resultado.medidos[consumo] = reglas.totalDe(puntos);
        const aviso = reglas.evaluarConteoDiario({ consumo, puntos, yaAvisado: Boolean(estado[consumo]) });
        if (aviso) {
          emitir(aviso);
          resultado.avisos.push(consumo);
          await ref.set({ [consumo]: { valor: aviso.valor, desde: aviso.desde, avisadoEn: ahora.toISOString() } }, { merge: true });
        }
      } catch (error) {
        resultado.fallos.push(consumo);
        console.error(`${PREFIJO_LOG} No se pudo vigilar ${consumo} del ${dia}: ${error.message}`);
      }
    }
  }

  // Almacenamiento: cruce y vuelta a la normalidad (Vacíos 3 y 4)
  try {
    const puntos = await medicion.medirAlmacenamiento({ token, hasta: ahora });
    if (puntos.length > 0) resultado.medidos[CONSUMO.ALMACENAMIENTO] = puntos[puntos.length - 1].valor;
    const refEstado = coleccion.doc(DOC_ESTADO);
    const estado = await leerDoc(refEstado);
    const { aviso, estadoNuevo } = reglas.evaluarAlmacenamiento({ puntos, estado: estado.almacenamiento, hoy });
    if (aviso) {
      emitir(aviso);
      resultado.avisos.push(aviso.consumo);
    }
    if (estadoNuevo) await refEstado.set({ almacenamiento: estadoNuevo }, { merge: true });
  } catch (error) {
    resultado.fallos.push(CONSUMO.ALMACENAMIENTO);
    console.error(`${PREFIJO_LOG} No se pudo vigilar el almacenamiento: ${error.message}`);
  }

  // Análisis de IA fuera de cupo (RN-6.7-O, Vacíos 1, 3 y 7)
  try {
    const usuarios = await medicion.leerUsuariosIa({ db, ahora });
    resultado.medidos[CONSUMO.IA] = usuarios.length;
    if (usuarios.length > 0) {
      const { aviso, iaNuevo } = reglas.evaluarIa({ usuarios, ahora, iaHoy: estadoHoy.ia, iaAyer: (await leerAyer()).ia });
      if (aviso) {
        emitir(aviso);
        resultado.avisos.push(CONSUMO.IA);
      }
      if (iaNuevo) await refHoy.set({ ia: iaNuevo }, { merge: true });
    }
  } catch (error) {
    resultado.fallos.push(CONSUMO.IA);
    console.error(`${PREFIJO_LOG} No se pudo vigilar los análisis de IA: ${error.message}`);
  }

  return resultado;
}

const scheduledConsumptionWatch = onSchedule(FUNCTION_CONFIG, async () => {
  const token = await tokenDelRuntime();
  const r = await vigilarConsumo({ db: admin.firestore(), token });
  const almacenamiento = r.medidos[CONSUMO.ALMACENAMIENTO];
  console.log(`${PREFIJO_LOG} ${r.dia}: lecturas=${r.medidos.lecturas ?? '-'} escrituras=${r.medidos.escrituras ?? '-'} ` +
    `almacenamiento=${almacenamiento !== undefined ? reglas.formatoGib(almacenamiento) : '-'} ` +
    `candidatosIa=${r.medidos.ia ?? '-'} avisos=[${r.avisos.join(',')}] fallos=[${r.fallos.join(',')}]`);
});

module.exports = {
  FUNCTION_CONFIG,
  PREFIJO_LOG,
  emitirAviso,
  vigilarConsumo,
  scheduledConsumptionWatch,
};
