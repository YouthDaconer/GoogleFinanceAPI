/**
 * HU 6.7 — Crea o actualiza en producción los correos del aviso de consumo: el
 * canal hacia quien administra la facturación, el panel de consumo y las seis
 * políticas.
 *
 *   node scripts/aplicarAvisosConsumo.js            → solo muestra lo que haría
 *   node scripts/aplicarAvisosConsumo.js --aplicar  → lo aplica
 *
 * Idempotente: canales, panel y políticas se reconocen y se actualizan; nunca se
 * duplican. Usa las credenciales de `gcloud` de quien lo ejecuta (Monitoring
 * sobre el proyecto y lectura de la política IAM de la cuenta de facturación).
 * No hay entorno de pruebas en GCP: `--aplicar` escribe en producción.
 *
 * **Solo lee la facturación, nunca la toca**: el aviso de presupuesto queda
 * intacto (RN-6.7-N, QA-12). Si cambia quien administra la facturación, hay que
 * volver a ejecutarlo.
 *
 * La definición vive en `scripts/consumoAlerts.js`, con tests; esto es solo el
 * borde.
 *
 * @see platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md (T5)
 */

const { tokenDeAcceso, llamar } = require('./gcpMonitoringApi');
const {
  PROYECTO,
  CORREO_OPERACION,
  AVISOS,
  definicionPolitica,
  politicaExistente,
  correosFacturacion,
  definicionCanalFacturacion,
  definicionPanel,
  panelExistente,
  enlacePanel,
  elegirCanal,
  enmascarar,
} = require('./consumoAlerts');

const MONITORING = `https://monitoring.googleapis.com/v3/projects/${PROYECTO}`;
const PANELES = `https://monitoring.googleapis.com/v1/projects/${PROYECTO}/dashboards`;
const FACTURACION = 'https://cloudbilling.googleapis.com/v1';

function printUsage() {
  console.log(`
Crea o actualiza los avisos de consumo (HU 6.7): canal de facturación, panel de
consumo y las seis políticas de log.

  --aplicar   Escribe en producción. Sin esto, solo muestra lo que haría.
  --help      Esta ayuda.
`);
}

function parseArgs(argv) {
  const args = { aplicar: false, help: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg === '--aplicar') args.aplicar = true;
    else {
      console.error(`Argumento no reconocido: ${arg}`);
      args.help = true;
    }
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printUsage();
    return 2;
  }

  const token = tokenDeAcceso();

  // 1. Quién administra la facturación: el destinatario del aviso de presupuesto (RN-6.7-M)
  const facturacion = await llamar(token, 'GET', `${FACTURACION}/projects/${PROYECTO}/billingInfo`);
  const cuenta = facturacion.json.billingAccountName;
  if (!cuenta) throw new Error(`${PROYECTO} no tiene cuenta de facturación`);
  const iam = await llamar(token, 'GET', `${FACTURACION}/${cuenta}:getIamPolicy`);
  const correos = correosFacturacion(iam.json);
  if (correos.length === 0) throw new Error(`Nadie tiene roles/billing.admin en ${cuenta}`);
  console.log(`Administradores de la facturación (${cuenta}): ${correos.map(enmascarar).join(', ')}`);

  // 2. Canales: operación (ya existe) y uno por cada administrador de la facturación
  const listaCanales = (await llamar(token, 'GET', `${MONITORING}/notificationChannels`)).json.notificationChannels;
  const canalOperacion = elegirCanal(listaCanales, CORREO_OPERACION);
  if (!canalOperacion) throw new Error(`Ningún canal de correo activo de ${PROYECTO} envía a ${CORREO_OPERACION}`);
  console.log(`Canal de operación: ${canalOperacion} (${CORREO_OPERACION})`);

  const planesCanal = correos.map((correo) => ({ correo, existente: elegirCanal(listaCanales, correo) }));
  for (const plan of planesCanal) {
    console.log(`Canal de facturación ${enmascarar(plan.correo)}: ${plan.existente ? `ya existe (${plan.existente})` : 'crear'}`);
  }

  // 3. Panel de consumo
  const paneles = (await llamar(token, 'GET', PANELES)).json.dashboards;
  const panel = panelExistente(paneles);
  console.log(`Panel «${definicionPanel().displayName}»: ${panel ? `actualizar (${panel.name})` : 'crear'}`);

  // 4. Políticas
  const politicas = (await llamar(token, 'GET', `${MONITORING}/alertPolicies`)).json.alertPolicies;
  for (const clave of Object.keys(AVISOS)) {
    const existente = politicaExistente(politicas, clave);
    console.log(`Política «${AVISOS[clave].titulo}»: ${existente ? 'actualizar' : 'crear'}`);
  }

  if (!args.aplicar) {
    console.log('');
    console.log('Simulación: no se escribió nada. Repite con --aplicar para aplicarlo.');
    return 0;
  }

  const canales = [canalOperacion];
  for (const plan of planesCanal) {
    let nombre = plan.existente;
    if (!nombre) {
      nombre = (await llamar(token, 'POST', `${MONITORING}/notificationChannels`, definicionCanalFacturacion(plan.correo))).json.name;
      console.log(`✓ Canal de facturación ${enmascarar(plan.correo)}: ${nombre}`);
    }
    if (!canales.includes(nombre)) canales.push(nombre);
  }

  let nombrePanel;
  if (panel) {
    await llamar(token, 'PATCH', `https://monitoring.googleapis.com/v1/${panel.name}`, { ...definicionPanel(), name: panel.name, etag: panel.etag });
    nombrePanel = panel.name;
  } else {
    nombrePanel = (await llamar(token, 'POST', PANELES, definicionPanel())).json.name;
  }
  const enlace = enlacePanel(nombrePanel);
  console.log(`✓ Panel: ${enlace}`);

  for (const clave of Object.keys(AVISOS)) {
    const existente = politicaExistente(politicas, clave);
    const cuerpo = definicionPolitica(clave, canales, enlace);
    if (existente) {
      await llamar(token, 'PATCH', `https://monitoring.googleapis.com/v3/${existente.name}`, cuerpo);
    } else {
      await llamar(token, 'POST', `${MONITORING}/alertPolicies`, cuerpo);
    }
    console.log(`✓ Política «${cuerpo.displayName}»`);
  }
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
