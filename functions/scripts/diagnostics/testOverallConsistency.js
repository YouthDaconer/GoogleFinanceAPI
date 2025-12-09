/**
 * Test de Regresión: Verificar que el cálculo de OVERALL sea correcto
 * 
 * Este script verifica que después de cada ejecución del scheduler,
 * el adjustedDailyChangePercentage de OVERALL coincida con el promedio
 * ponderado de las cuentas individuales (usando valor pre-cambio).
 * 
 * USO:
 *   node testOverallConsistency.js                  # Verificar últimos 7 días
 *   node testOverallConsistency.js --days=30       # Verificar últimos N días
 * 
 * Este test puede ejecutarse como parte del CI/CD para detectar regresiones.
 */

const admin = require('firebase-admin');

const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  CURRENCY: 'USD',
  TOLERANCE: 0.1 // 0.1% de tolerancia
};

function parseArgs() {
  const args = process.argv.slice(2);
  let days = 7;
  
  args.forEach(arg => {
    if (arg.startsWith('--days=')) days = parseInt(arg.split('=')[1]);
  });
  
  return { days };
}

async function getRecentDocs(path, days) {
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);
  const startDateStr = startDate.toISOString().split('T')[0];
  
  const snapshot = await db.collection(path)
    .where('date', '>=', startDateStr)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

async function getAccountIds(userId) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  return snapshot.docs.map(d => d.id);
}

function calculateExpectedChange(accountsData, currency) {
  let totalPreChange = 0;
  let weightedChange = 0;
  
  for (const data of Object.values(accountsData)) {
    if (!data || !data[currency]) continue;
    
    const value = data[currency].totalValue || 0;
    const change = data[currency].adjustedDailyChangePercentage || 0;
    
    if (value <= 0) continue;
    
    const preChange = change !== 0 ? value / (1 + change / 100) : value;
    totalPreChange += preChange;
    weightedChange += preChange * change;
  }
  
  return totalPreChange > 0 ? weightedChange / totalPreChange : 0;
}

async function main() {
  const { days } = parseArgs();
  
  console.log('');
  console.log('═'.repeat(80));
  console.log('  TEST DE CONSISTENCIA: OVERALL vs Cuentas Individuales');
  console.log('═'.repeat(80));
  console.log('');
  console.log(`  Verificando últimos ${days} días`);
  console.log('');

  // Obtener datos de OVERALL
  const overallPath = `portfolioPerformance/${CONFIG.USER_ID}/dates`;
  const overallDocs = await getRecentDocs(overallPath, days);
  
  // Obtener IDs de cuentas
  const accountIds = await getAccountIds(CONFIG.USER_ID);
  
  // Obtener datos de cada cuenta
  const accountsDataByDate = new Map();
  
  for (const accountId of accountIds) {
    const accountPath = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`;
    const accountDocs = await getRecentDocs(accountPath, days);
    
    accountDocs.forEach(doc => {
      if (!accountsDataByDate.has(doc.date)) {
        accountsDataByDate.set(doc.date, {});
      }
      accountsDataByDate.get(doc.date)[accountId] = doc;
    });
  }
  
  // Verificar cada día
  let errors = 0;
  let warnings = 0;
  let passed = 0;
  
  console.log('  Fecha       | OVERALL   | Esperado  | Diff      | Estado');
  console.log('  ' + '-'.repeat(65));
  
  for (const overallDoc of overallDocs) {
    const date = overallDoc.date;
    const overallChange = overallDoc[CONFIG.CURRENCY]?.adjustedDailyChangePercentage || 0;
    const accountsData = accountsDataByDate.get(date) || {};
    const expectedChange = calculateExpectedChange(accountsData, CONFIG.CURRENCY);
    const diff = Math.abs(overallChange - expectedChange);
    
    let status;
    if (diff <= CONFIG.TOLERANCE) {
      status = '✅';
      passed++;
    } else if (diff <= CONFIG.TOLERANCE * 5) {
      status = '⚠️';
      warnings++;
    } else {
      status = '❌';
      errors++;
    }
    
    console.log(
      `  ${date} | ` +
      `${overallChange.toFixed(2).padStart(8)}% | ` +
      `${expectedChange.toFixed(2).padStart(8)}% | ` +
      `${diff.toFixed(2).padStart(8)}% | ` +
      `${status}`
    );
  }
  
  console.log('  ' + '-'.repeat(65));
  console.log('');
  console.log(`  Resultados: ${passed} ✅ pasaron, ${warnings} ⚠️ warnings, ${errors} ❌ errores`);
  
  if (errors > 0) {
    console.log('');
    console.log('  ❌ TEST FALLIDO: Se encontraron inconsistencias significativas');
    console.log('     Ejecute fixOverallAdjustedChange.js para corregir');
    process.exit(1);
  } else if (warnings > 0) {
    console.log('');
    console.log('  ⚠️ TEST PASÓ CON WARNINGS: Pequeñas inconsistencias encontradas');
    process.exit(0);
  } else {
    console.log('');
    console.log('  ✅ TEST PASÓ: OVERALL es consistente con cuentas individuales');
    process.exit(0);
  }
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
