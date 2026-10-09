/**
 * Firebase Admin SDK — Inicialización centralizada
 * 
 * SEC-AUDIT-002: Migrado a Application Default Credentials (ADC).
 * 
 * En Cloud Functions / Cloud Run:
 *   admin.initializeApp() usa ADC automáticamente (service account del runtime).
 *   NO requiere key.json — las credenciales son inyectadas por GCP.
 * 
 * En desarrollo local:
 *   Si existe key.json, se usa para autenticar.
 *   Si no existe, se usa ADC (gcloud auth application-default login).
 * 
 * @see docs/architecture/SEC-AUDIT-002-security-vulnerabilities-report.md
 * @module services/firebaseAdmin
 */
const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

if (!admin.apps.length) {
  const keyPath = path.resolve(__dirname, '../key.json');
  const isCloudFunctions = !!process.env.FUNCTION_TARGET || !!process.env.K_SERVICE;

  if (!isCloudFunctions && fs.existsSync(keyPath)) {
    // Desarrollo local: usar key.json si existe
    const serviceAccount = require(keyPath);
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL: "https://portafolio-inversiones-default-rtdb.firebaseio.com"
    });
  } else {
    // Cloud Functions / Cloud Run: usar Application Default Credentials
    admin.initializeApp({
      databaseURL: "https://portafolio-inversiones-default-rtdb.firebaseio.com"
    });
  }
}

module.exports = admin;