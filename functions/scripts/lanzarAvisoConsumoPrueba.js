/**
 * HU 6.7 — El aviso de prueba (Escenario 5): comprueba que el correo llega sin
 * esperar un incidente.
 *
 *   node scripts/lanzarAvisoConsumoPrueba.js           → mide y muestra lo que enviaría
 *   node scripts/lanzarAvisoConsumoPrueba.js --enviar  → lo envía
 *
 * Mide los cuatro consumos de hoy con el mismo servicio del vigilante y escribe
 * en Cloud Logging una línea `consumo: "prueba"`. La política «PRUEBA — aviso de
 * consumo» la convierte en correo por los mismos canales que los avisos reales,
 * marcado como PRUEBA en el asunto y en el cuerpo.
 *
 * **No toca el estado del vigilante** (Vacío 5): no cuenta para el «uno por
 * día» ni silencia un aviso real. Se puede relanzar; dos lanzamientos con menos
 * de 5 min entre sí llegan como un solo correo (el mínimo de Monitoring).
 *
 * Usa las credenciales de `gcloud` para Monitoring y Logging, y las de
 * `services/firebaseAdmin` (key.json local o ADC) para leer los análisis de IA.
 *
 * @see platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md (T6)
 */

const { tokenDeAcceso, llamar } = require('./gcpMonitoringApi');
const { PROYECTO, LOG_PRUEBA } = require('./consumoAlerts');
const reglas = require('../services/consumption/consumptionAlertService');
const metricas = require('../services/consumption/consumptionMetricsService');

function printUsage() {
  console.log(`
Lanza un aviso de consumo de PRUEBA (HU 6.7, Escenario 5).

  --enviar   Escribe la línea en Cloud Logging y llega el correo. Sin esto, solo mide y muestra.
  --help     Esta ayuda.
`);
}

function parseArgs(argv) {
  const args = { enviar: false, help: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg === '--enviar') args.enviar = true;
    else {
      console.error(`Argumento no reconocido: ${arg}`);
      args.help = true;
    }
  }
  return args;
}

/** Mide un consumo; `null` si no se pudo (el correo lo dice, la prueba sigue). */
async function medir(nombre, fn) {
  try {
    return await fn();
  } catch (error) {
    console.error(`No se pudo medir ${nombre}: ${error.message}`);
    return null;
  }
}

/**
 * Cuántos usuarios tienen hoy análisis fuera de cupo.
 *
 * @param {Date} ahora
 * @returns {Promise<number>}
 */
async function usuariosIaFueraDeCupoHoy(ahora) {
  const admin = require('../services/firebaseAdmin');
  const hoy = reglas.diaColombia(ahora);
  const usuarios = await metricas.leerUsuariosIa({ db: admin.firestore(), ahora });
  return usuarios.filter((u) => reglas.analisisFueraDeCupo(u.analisis, u.conAcceso)
    .some((a) => reglas.diaColombia(a.createdAt) === hoy)).length;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printUsage();
    return 2;
  }

  const ahora = new Date();
  const hoy = reglas.diaColombia(ahora);
  const token = tokenDeAcceso();

  const lecturas = await medir('las lecturas', async () => reglas.totalDe(
    await metricas.medirConteoDelDia({ token, consumo: reglas.CONSUMO.LECTURAS, dia: hoy, hasta: ahora })));
  const escrituras = await medir('las escrituras', async () => reglas.totalDe(
    await metricas.medirConteoDelDia({ token, consumo: reglas.CONSUMO.ESCRITURAS, dia: hoy, hasta: ahora })));
  const almacenamientoBytes = await medir('el almacenamiento', async () => {
    const puntos = await metricas.medirAlmacenamiento({ token, hasta: ahora, horas: 1 });
    return puntos.length > 0 ? puntos[puntos.length - 1].valor : null;
  });
  const usuariosIa = await medir('los análisis de IA', () => usuariosIaFueraDeCupoHoy(ahora));

  const linea = reglas.lineaAviso({
    consumo: reglas.CONSUMO.PRUEBA,
    valor: reglas.valorPrueba({ lecturas, escrituras, almacenamientoBytes, usuariosIa }),
    desde: ahora.toISOString(),
  });
  console.log(`Línea: ${JSON.stringify(linea, null, 2)}`);

  if (!args.enviar) {
    console.log('');
    console.log('Simulación: no se envió nada. Repite con --enviar para que llegue el correo.');
    return 0;
  }

  await llamar(token, 'POST', 'https://logging.googleapis.com/v2/entries:write', {
    entries: [
      {
        logName: `projects/${PROYECTO}/logs/${LOG_PRUEBA}`,
        resource: { type: 'global', labels: { project_id: PROYECTO } },
        severity: 'WARNING',
        jsonPayload: linea,
      },
    ],
  });
  console.log(`✓ Aviso de prueba enviado a las ${reglas.horaColombia(ahora)}. El correo llega en unos minutos.`);
  return 0;
}

(async () => {
  try {
    process.exit(await main());
  } catch (error) {
    console.error('');
    console.error('ERROR:', error.message);
    process.exit(1);
  }
})();
