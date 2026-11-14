/**
 * HU 6.7 — Qué avisos de consumo habrían salido en los últimos N días, con el
 * consumo real y las mismas reglas del vigilante. **Solo lee.**
 *
 *   node scripts/simularAvisosConsumo.js            → últimos 42 días (lo que guarda Monitoring)
 *   node scripts/simularAvisosConsumo.js --dias 7   → últimos 7
 *
 * Es la medición previa del Escenario 4 (RN-6-I: el consumo se mide, no se
 * supone): con el consumo habitual, cero avisos. Recorre cada día de Colombia:
 * - lecturas y escrituras: el total del día contra su umbral;
 * - almacenamiento: evalúa cada 15 min, como el vigilante, arrastrando el estado
 *   de un día a otro;
 * - IA: los análisis fuera de cupo de cada día. El plan de cada usuario es el de
 *   hoy (D2: no hay histórico de suscripciones).
 *
 * Usa las credenciales de `gcloud` para Monitoring y las de
 * `services/firebaseAdmin` (key.json local o ADC) para Firestore.
 *
 * @see platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md (T7)
 */

const { DateTime } = require('luxon');
const { tokenDeAcceso } = require('./gcpMonitoringApi');
const reglas = require('../services/consumption/consumptionAlertService');
const metricas = require('../services/consumption/consumptionMetricsService');

const { CONSUMO, ZONA_AVISO } = reglas;

/** Monitoring guarda las métricas de GCP 6 semanas. */
const DIAS_MAXIMOS = 42;

const PASO_MS = 15 * 60 * 1000;
const MS_DIA = 24 * 60 * 60 * 1000;

function printUsage() {
  console.log(`
Simula los avisos de consumo de los últimos N días con el consumo real (HU 6.7). Solo lee.

  --dias N   Días hacia atrás, incluido hoy (1 a ${DIAS_MAXIMOS}; por defecto ${DIAS_MAXIMOS}).
  --help     Esta ayuda.
`);
}

function parseArgs(argv) {
  const args = { dias: DIAS_MAXIMOS, help: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg === '--dias') {
      args.dias = Number(argv[i + 1]);
      i += 1;
    } else {
      console.error(`Argumento no reconocido: ${arg}`);
      args.help = true;
    }
  }
  if (!Number.isInteger(args.dias) || args.dias < 1 || args.dias > DIAS_MAXIMOS) {
    console.error(`--dias debe ser un entero entre 1 y ${DIAS_MAXIMOS}`);
    args.help = true;
  }
  return args;
}

/**
 * Almacenamiento evaluado cada 15 min del día, como el vigilante.
 *
 * @param {Array<{fin: string, valor: number}>} puntos - Desde 24 h antes del día hasta su fin
 * @param {string} dia
 * @param {object} estado - Se arrastra de un día al siguiente
 * @param {Date} hasta
 * @returns {{avisos: object[], estado: object, maximo: number|null}}
 */
function simularAlmacenamiento(puntos, dia, estado, hasta) {
  const { inicio, fin } = reglas.intervaloDelDia(dia);
  const limite = Math.min(Date.parse(fin), hasta.getTime());
  const avisos = [];
  let actual = estado;
  for (let t = Date.parse(inicio) + PASO_MS; t <= limite; t += PASO_MS) {
    const ventana = puntos.filter((p) => Date.parse(p.fin) <= t && Date.parse(p.fin) > t - MS_DIA);
    const { aviso, estadoNuevo } = reglas.evaluarAlmacenamiento({ puntos: ventana, estado: actual, hoy: dia });
    if (aviso) avisos.push(aviso);
    if (estadoNuevo) actual = estadoNuevo;
  }
  const delDia = puntos.filter((p) => Date.parse(p.fin) > Date.parse(inicio) && Date.parse(p.fin) <= Date.parse(fin)).map((p) => p.valor);
  return { avisos, estado: actual, maximo: delDia.length > 0 ? Math.max(...delDia) : null };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printUsage();
    return 2;
  }

  const ahora = new Date();
  const token = tokenDeAcceso();
  const hoy = DateTime.fromJSDate(ahora).setZone(ZONA_AVISO).startOf('day');
  const dias = Array.from({ length: args.dias }, (_, i) => hoy.minus({ days: args.dias - 1 - i }).toISODate());

  // IA: todos los usuarios con análisis en la ventana, una sola lectura
  const admin = require('../services/firebaseAdmin');
  const horasVentana = (args.dias + 1) * 24;
  const usuariosIa = await metricas.leerUsuariosIa({ db: admin.firestore(), ahora, horas: horasVentana });

  console.log('día        | lecturas | escrituras | almacenamiento máx | IA fuera de cupo | avisos');
  console.log('-----------|----------|------------|--------------------|------------------|-------');
  let estadoAlmacenamiento = {};
  const todos = [];
  for (const dia of dias) {
    const avisos = [];
    const totales = {};
    for (const consumo of [CONSUMO.LECTURAS, CONSUMO.ESCRITURAS]) {
      const puntos = await metricas.medirConteoDelDia({ token, consumo, dia, hasta: ahora });
      totales[consumo] = reglas.totalDe(puntos);
      const aviso = reglas.evaluarConteoDiario({ consumo, puntos, yaAvisado: false });
      if (aviso) avisos.push(aviso);
    }

    const { inicio, fin } = reglas.intervaloDelDia(dia);
    const hastaDia = new Date(Math.min(Date.parse(fin), ahora.getTime()));
    const puntosAlm = await metricas.medirAlmacenamiento({
      token, hasta: hastaDia, horas: (hastaDia.getTime() - Date.parse(inicio)) / 3600000 + 24,
    });
    const alm = simularAlmacenamiento(puntosAlm, dia, estadoAlmacenamiento, ahora);
    estadoAlmacenamiento = alm.estado;
    avisos.push(...alm.avisos);

    const fueraDeCupo = usuariosIa.filter((u) => reglas.analisisFueraDeCupo(u.analisis, u.conAcceso)
      .some((a) => reglas.diaColombia(a.createdAt) === dia));
    if (fueraDeCupo.length > 0) avisos.push({ consumo: CONSUMO.IA, usuarios: fueraDeCupo.length });

    todos.push(...avisos.map((a) => ({ dia, ...a })));
    console.log([
      dia,
      reglas.formatoEntero(totales.lecturas).padStart(8),
      reglas.formatoEntero(totales.escrituras).padStart(10),
      (alm.maximo !== null ? reglas.formatoGib(alm.maximo) : '-').padStart(18),
      String(fueraDeCupo.length).padStart(16),
      avisos.map((a) => a.consumo).join(', ') || '-',
    ].join(' | '));
  }

  console.log('');
  console.log(`Avisos que habrían salido en ${args.dias} días: ${todos.length}`);
  for (const aviso of todos) {
    if (aviso.consumo === CONSUMO.IA) {
      console.log(`  ${aviso.dia} ia: ${aviso.usuarios} usuario(s) fuera de cupo (plan de hoy)`);
    } else {
      console.log(`  ${aviso.dia} ${reglas.lineaAviso(aviso).message}`);
    }
  }
  return 0;
}

if (require.main === module) {
  (async () => {
    try {
      process.exit(await main());
    } catch (error) {
      console.error('');
      console.error('ERROR:', error.message);
      process.exit(1);
    }
  })();
}

module.exports = { simularAlmacenamiento, parseArgs };
