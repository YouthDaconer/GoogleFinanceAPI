/**
 * Token de servicio para los scripts de operación y diagnóstico.
 *
 * El token NUNCA se escribe en el código: este repositorio es público. Se lee de
 * la variable de entorno `CF_SERVICE_TOKEN` (el mismo secreto que usan las
 * Cloud Functions). Si falta, el script se detiene con un mensaje claro en vez de
 * llamar a la API sin autenticar.
 *
 * Uso en PowerShell: `$env:CF_SERVICE_TOKEN = '<token>'; node scripts/<script>.js`
 *
 * @returns {string} El token de servicio
 */
function serviceTokenFromEnv() {
  const token = (process.env.CF_SERVICE_TOKEN || '').trim();
  if (!token) {
    console.error('[serviceTokenFromEnv] Falta CF_SERVICE_TOKEN en el entorno. ' +
      'Defínelo antes de ejecutar el script; nunca lo escribas en el código.');
    process.exit(1);
  }
  return token;
}

module.exports = { serviceTokenFromEnv };
