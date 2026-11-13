/**
 * Llamadas a las APIs de Google Cloud (Logging y Monitoring) desde los scripts
 * de operación que crean avisos. Usa las credenciales de `gcloud` de quien los
 * ejecuta.
 *
 * Salió del borde de la HU 7 (`aplicarAvisosAccesoInversor.js`) cuando la HU
 * 6.8.2 necesitó lo mismo (`aplicarAvisosCorreccionNocturna.js`): una sola
 * copia.
 *
 * @module scripts/gcpMonitoringApi
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md (T9)
 */

const { execSync } = require('child_process');

/** Token de acceso de quien ejecuta el script. */
function tokenDeAcceso() {
  return execSync('gcloud auth print-access-token', { encoding: 'utf8' }).trim();
}

/**
 * Llama a una API REST de Google Cloud.
 *
 * @param {string} token
 * @param {'GET'|'POST'|'PUT'|'PATCH'|'DELETE'} metodo
 * @param {string} url
 * @param {object} [cuerpo]
 * @returns {Promise<{status: number, json: object}>}
 */
async function llamar(token, metodo, url, cuerpo) {
  const respuesta = await fetch(url, {
    method: metodo,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: cuerpo ? JSON.stringify(cuerpo) : undefined,
  });
  const texto = await respuesta.text();
  const json = texto ? JSON.parse(texto) : {};
  // Un 404 solo es una respuesta válida en una lectura («no existe todavía»). En
  // una escritura es un fallo: así se perdió en silencio la alerta por origen el
  // 2026-11-12 (Monitoring aún no veía la métrica recién creada y respondió 404).
  const ausenciaEsperada = metodo === 'GET' && respuesta.status === 404;
  if (!respuesta.ok && !ausenciaEsperada) {
    const mensaje = json.error ? json.error.message : texto;
    throw new Error(`${metodo} ${url} → ${respuesta.status}: ${mensaje}`);
  }
  return { status: respuesta.status, json };
}

const esperar = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

module.exports = { tokenDeAcceso, llamar, esperar };
