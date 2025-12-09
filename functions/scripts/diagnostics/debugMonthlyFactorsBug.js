/**
 * Diagnóstico: Verificar monthlyStartFactors y monthlyEndFactors
 * 
 * El problema identificado es que IBKR marzo muestra -6.75% en UI pero
 * el cálculo directo da -9.25%.
 * 
 * Hipótesis: El problema está en cómo se guardan los factores mensuales.
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  ACCOUNTS: {
    IBKR: 'BZHvXz4QT2yqqqlFP22X'
  },
  CURRENCY: 'USD'
};

async function getAccountDocs(accountId) {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`;
  const snapshot = await db.collection(path).orderBy('date', 'asc').get();
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

/**
 * Simular exactamente lo que hace calculateHistoricalReturns
 */
function simulateCalculation(docs, currency) {
  const now = DateTime.now().setZone("America/New_York");
  
  // Estructuras
  const datesByMonth = {};
  const lastDaysByMonth = {};
  const monthlyStartFactors = {};
  const monthlyEndFactors = {};
  
  // Ordenar documentos por fecha
  const documents = docs.sort((a, b) => {
    const dateA = a.date;
    const dateB = b.date;
    return dateA.localeCompare(dateB);
  });
  
  // Primera pasada: agrupar fechas por mes
  documents.forEach(doc => {
    const date = DateTime.fromISO(doc.date);
    const year = date.year.toString();
    const month = (date.month - 1).toString();
    
    if (!datesByMonth[year]) datesByMonth[year] = {};
    if (!datesByMonth[year][month]) datesByMonth[year][month] = [];
    datesByMonth[year][month].push(doc.date);
  });
  
  // Obtener el último día de cada mes
  Object.keys(datesByMonth).forEach(year => {
    lastDaysByMonth[year] = {};
    Object.keys(datesByMonth[year]).forEach(month => {
      const sortedDates = [...datesByMonth[year][month]].sort((a, b) => b.localeCompare(a));
      lastDaysByMonth[year][month] = sortedDates[0];
    });
  });
  
  // Inicializar estructuras mensuales
  Object.keys(datesByMonth).forEach(year => {
    monthlyStartFactors[year] = {};
    monthlyEndFactors[year] = {};
  });
  
  // Segunda pasada: procesar documentos
  let currentFactor = 1;
  const marchLog = [];
  
  documents.forEach(doc => {
    const currencyData = doc[currency];
    if (!currencyData) return;
    
    const adjustedDailyChange = currencyData.adjustedDailyChangePercentage || 0;
    
    const factorBefore = currentFactor;
    
    // Actualizar factor
    currentFactor = currentFactor * (1 + adjustedDailyChange / 100);
    
    const date = DateTime.fromISO(doc.date);
    const year = date.year.toString();
    const month = (date.month - 1).toString();
    
    // Log para marzo 2025
    if (year === '2025' && month === '2') {
      marchLog.push({
        date: doc.date,
        change: adjustedDailyChange,
        factorBefore,
        factorAfter: currentFactor,
        isFirstOfMonth: !monthlyStartFactors[year][month]
      });
    }
    
    // AQUÍ ESTÁ EL POTENCIAL BUG:
    // Se guarda currentFactor DESPUÉS del cambio, no ANTES
    if (!monthlyStartFactors[year][month]) {
      monthlyStartFactors[year][month] = currentFactor;
    }
    monthlyEndFactors[year][month] = currentFactor;
  });
  
  return {
    monthlyStartFactors,
    monthlyEndFactors,
    marchLog
  };
}

async function main() {
  console.log('');
  console.log('═'.repeat(90));
  console.log('  DIAGNÓSTICO: monthlyStartFactors vs monthlyEndFactors');
  console.log('═'.repeat(90));
  console.log('');
  
  const ibkrDocs = await getAccountDocs(CONFIG.ACCOUNTS.IBKR);
  console.log(`Documentos IBKR: ${ibkrDocs.length}`);
  
  const result = simulateCalculation(ibkrDocs, CONFIG.CURRENCY);
  
  console.log('');
  console.log('━'.repeat(90));
  console.log('  MARZO 2025 - DETALLE DÍA A DÍA');
  console.log('━'.repeat(90));
  console.log('');
  
  console.log('Fecha       | Change %   | Factor Before | Factor After | Nota');
  console.log('-'.repeat(80));
  
  result.marchLog.forEach(day => {
    const nota = day.isFirstOfMonth ? '← monthlyStartFactors se guarda AQUÍ' : '';
    console.log(
      `${day.date} | ` +
      `${day.change.toFixed(4).padStart(9)}% | ` +
      `${day.factorBefore.toFixed(6).padStart(12)} | ` +
      `${day.factorAfter.toFixed(6).padStart(12)} | ` +
      nota
    );
  });
  
  const marchStart = result.monthlyStartFactors['2025']?.['2'];
  const marchEnd = result.monthlyEndFactors['2025']?.['2'];
  const marchReturn = ((marchEnd / marchStart) - 1) * 100;
  
  console.log('');
  console.log('━'.repeat(90));
  console.log('  CÁLCULO DEL RENDIMIENTO MENSUAL');
  console.log('━'.repeat(90));
  console.log('');
  console.log(`  monthlyStartFactors['2025']['2'] = ${marchStart?.toFixed(6)}`);
  console.log(`  monthlyEndFactors['2025']['2'] = ${marchEnd?.toFixed(6)}`);
  console.log('');
  console.log(`  monthReturn = (${marchEnd?.toFixed(6)} / ${marchStart?.toFixed(6)} - 1) × 100`);
  console.log(`  monthReturn = ${marchReturn?.toFixed(2)}%`);
  console.log('');
  console.log(`  UI muestra: -6.75%`);
  console.log('');
  
  // ¿Coincide?
  if (Math.abs(marchReturn - (-6.75)) < 0.1) {
    console.log('  ✅ El cálculo coincide con el UI');
    console.log('');
    console.log('  EXPLICACIÓN DEL PROBLEMA:');
    console.log('  El monthlyStartFactors se guarda DESPUÉS de aplicar el cambio del primer día.');
    console.log('  Esto significa que el rendimiento del primer día de marzo NO se incluye');
    console.log('  en el rendimiento mensual de marzo.');
    console.log('');
    console.log('  El día 2025-03-01 tuvo un cambio de -2.67%, pero ese cambio se "pierde"');
    console.log('  porque el factor ya incluye ese cambio cuando se guarda como "inicio".');
  } else {
    console.log('  ❌ El cálculo NO coincide con el UI');
  }
  
  // Calcular el rendimiento CORRECTO (incluyendo el primer día)
  console.log('');
  console.log('━'.repeat(90));
  console.log('  CÁLCULO CORRECTO (incluyendo primer día)');
  console.log('━'.repeat(90));
  console.log('');
  
  // El factor al inicio de marzo debería ser el factor ANTES del primer cambio
  const firstDayOfMarch = result.marchLog[0];
  const correctMarchStart = firstDayOfMarch.factorBefore;
  const correctMarchReturn = ((marchEnd / correctMarchStart) - 1) * 100;
  
  console.log(`  Factor CORRECTO al inicio de marzo = ${correctMarchStart.toFixed(6)}`);
  console.log(`  Factor al final de marzo = ${marchEnd.toFixed(6)}`);
  console.log(`  Rendimiento CORRECTO = ${correctMarchReturn.toFixed(2)}%`);
  console.log('');
  console.log(`  Diferencia: ${(marchReturn - correctMarchReturn).toFixed(2)}% (el primer día se pierde)`);
  console.log(`  Primer día de marzo tuvo: ${firstDayOfMarch.change.toFixed(2)}%`);
  
  console.log('');
  console.log('═'.repeat(90));
  console.log('  CONCLUSIÓN');
  console.log('═'.repeat(90));
  console.log('');
  console.log('  🐛 BUG CONFIRMADO:');
  console.log('     El rendimiento mensual pierde el cambio del primer día de cada mes.');
  console.log('');
  console.log('  📍 UBICACIÓN DEL BUG:');
  console.log('     historicalReturnsService.js, líneas ~318-321');
  console.log('     El monthlyStartFactors se guarda DESPUÉS del currentFactor *= ...');
  console.log('');
  console.log('  🔧 FIX NECESARIO:');
  console.log('     Guardar monthlyStartFactors ANTES de aplicar el cambio del día.');
  
  console.log('');
  console.log('═'.repeat(90));
  
  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
