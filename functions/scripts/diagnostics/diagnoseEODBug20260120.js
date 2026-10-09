/**
 * Script de diagnóstico para investigar la caída de -10.9% en portfolioPerformance
 * Fecha problemática: 2026-01-20
 * 
 * Propósito:
 * 1. Comparar valores entre 2026-01-16 y 2026-01-20
 * 2. Identificar qué causó la diferencia
 * 3. Verificar si fue un día de trading válido
 * 
 * @see OPT-DEMAND-400-FIX
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

async function diagnose() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  const problematicDate = '2026-01-20';
  const previousDate = '2026-01-16'; // El viernes 17 parece no tener datos
  
  console.log('='.repeat(90));
  console.log('DIAGNÓSTICO: Caída de -10.9% en portfolioPerformance');
  console.log('='.repeat(90));
  console.log(`Usuario: ${userId}`);
  console.log(`Fecha problemática: ${problematicDate}`);
  console.log(`Fecha anterior válida: ${previousDate}`);
  console.log('');
  
  // 1. Verificar festivos de NYSE
  console.log('─'.repeat(90));
  console.log('1. VERIFICACIÓN DE FESTIVOS NYSE');
  console.log('─'.repeat(90));
  
  const holidaysDoc = await db.collection('marketHolidays').doc('US').get();
  if (holidaysDoc.exists) {
    const holidays = holidaysDoc.data().holidays || {};
    console.log('Festivos de enero 2026:', Object.entries(holidays)
      .filter(([date]) => date.startsWith('2026-01'))
      .map(([date, name]) => `${date}: ${name}`)
      .join(', ') || 'Ninguno encontrado');
    
    // Verificar si 2026-01-20 es festivo
    if (holidays['2026-01-20']) {
      console.log(`\n⚠️ ${problematicDate} ES UN FESTIVO: ${holidays['2026-01-20']}`);
      console.log('   La función NO debería haber guardado datos para este día');
    } else if (holidays['2026-01-19']) {
      console.log(`\n📅 ${holidays['2026-01-19']} es el 2026-01-19, no el 2026-01-20`);
    }
  } else {
    console.log('❌ marketHolidays/US no encontrado');
  }
  console.log('');
  
  // 2. Obtener datos de portfolioPerformance para ambas fechas
  console.log('─'.repeat(90));
  console.log('2. COMPARACIÓN DE DATOS: portfolioPerformance/dates');
  console.log('─'.repeat(90));
  
  const [prev, current] = await Promise.all([
    db.doc(`portfolioPerformance/${userId}/dates/${previousDate}`).get(),
    db.doc(`portfolioPerformance/${userId}/dates/${problematicDate}`).get()
  ]);
  
  if (!prev.exists || !current.exists) {
    console.log(`❌ Datos faltantes: prev=${prev.exists}, current=${current.exists}`);
    return;
  }
  
  const prevData = prev.data();
  const currentData = current.data();
  
  // Mostrar USD (la moneda principal)
  console.log('\n📊 Datos en USD:');
  console.log('Fecha           | totalValue | adjustedDailyChange% | totalCashFlow | dailyChange%');
  console.log('-'.repeat(95));
  
  const prevUSD = prevData.USD || {};
  const currUSD = currentData.USD || {};
  
  console.log(`${previousDate}    | $${(prevUSD.totalValue || 0).toFixed(2).padStart(10)} | ${((prevUSD.adjustedDailyChangePercentage || 0) * 100).toFixed(4).padStart(12)}% | $${(prevUSD.totalCashFlow || 0).toFixed(2).padStart(10)} | ${((prevUSD.dailyChangePercentage || 0) * 100).toFixed(4)}%`);
  console.log(`${problematicDate}    | $${(currUSD.totalValue || 0).toFixed(2).padStart(10)} | ${((currUSD.adjustedDailyChangePercentage || 0) * 100).toFixed(4).padStart(12)}% | $${(currUSD.totalCashFlow || 0).toFixed(2).padStart(10)} | ${((currUSD.dailyChangePercentage || 0) * 100).toFixed(4)}%`);
  
  // Calcular la diferencia real
  const valueDiff = (currUSD.totalValue || 0) - (prevUSD.totalValue || 0);
  const valueDiffPercent = prevUSD.totalValue > 0 
    ? (valueDiff / prevUSD.totalValue) * 100 
    : 0;
  
  console.log('');
  console.log(`📉 Diferencia en totalValue: $${valueDiff.toFixed(2)} (${valueDiffPercent.toFixed(2)}%)`);
  console.log(`   adjustedDailyChangePercentage reportado: ${((currUSD.adjustedDailyChangePercentage || 0) * 100).toFixed(4)}%`);
  
  // 3. Verificar datos por cuenta
  console.log('');
  console.log('─'.repeat(90));
  console.log('3. DESGLOSE POR CUENTA');
  console.log('─'.repeat(90));
  
  const accountsSnap = await db.collection(`portfolioPerformance/${userId}/accounts`).get();
  const accountIds = accountsSnap.docs.map(d => d.id);
  
  console.log(`Cuentas encontradas: ${accountIds.length}`);
  console.log('');
  
  for (const accountId of accountIds) {
    const [accPrev, accCurr] = await Promise.all([
      db.doc(`portfolioPerformance/${userId}/accounts/${accountId}/dates/${previousDate}`).get(),
      db.doc(`portfolioPerformance/${userId}/accounts/${accountId}/dates/${problematicDate}`).get()
    ]);
    
    // Obtener nombre de la cuenta
    const accountDoc = await db.collection('portfolioAccounts').doc(accountId).get();
    const accountName = accountDoc.exists ? accountDoc.data().name : accountId;
    
    if (!accPrev.exists && !accCurr.exists) {
      console.log(`📂 ${accountName}: Sin datos para ambas fechas`);
      continue;
    }
    
    const accPrevData = accPrev.exists ? accPrev.data() : {};
    const accCurrData = accCurr.exists ? accCurr.data() : {};
    
    const accPrevUSD = accPrevData.USD || {};
    const accCurrUSD = accCurrData.USD || {};
    
    console.log(`📂 ${accountName} (${accountId}):`);
    console.log(`   ${previousDate}: $${(accPrevUSD.totalValue || 0).toFixed(2)}, adjDailyChange: ${((accPrevUSD.adjustedDailyChangePercentage || 0) * 100).toFixed(4)}%`);
    console.log(`   ${problematicDate}: $${(accCurrUSD.totalValue || 0).toFixed(2)}, adjDailyChange: ${((accCurrUSD.adjustedDailyChangePercentage || 0) * 100).toFixed(4)}%`);
    
    // Verificar si la cuenta tiene datos inconsistentes
    const accValueDiff = (accCurrUSD.totalValue || 0) - (accPrevUSD.totalValue || 0);
    if (Math.abs(accValueDiff) > 100) {
      console.log(`   ⚠️ Diferencia significativa: $${accValueDiff.toFixed(2)}`);
    }
    console.log('');
  }
  
  // 4. Verificar si hay días faltantes
  console.log('─'.repeat(90));
  console.log('4. ANÁLISIS DE DÍAS FALTANTES');
  console.log('─'.repeat(90));
  
  const datesSnap = await db.collection(`portfolioPerformance/${userId}/dates`)
    .where('date', '>=', '2026-01-13')
    .orderBy('date', 'asc')
    .get();
  
  const dates = datesSnap.docs.map(d => d.data().date);
  console.log('Fechas encontradas:', dates.join(', '));
  
  // Verificar si faltan días laborales
  const expectedWorkdays = ['2026-01-13', '2026-01-14', '2026-01-15', '2026-01-16', '2026-01-17'];
  const missingWorkdays = expectedWorkdays.filter(d => !dates.includes(d));
  
  if (missingWorkdays.length > 0) {
    console.log(`⚠️ Días laborales faltantes: ${missingWorkdays.join(', ')}`);
  }
  
  // 5. Verificar precios de activos para ambas fechas
  console.log('');
  console.log('─'.repeat(90));
  console.log('5. COMPARACIÓN DE PRECIOS POR ACTIVO (assetPerformance)');
  console.log('─'.repeat(90));
  
  const prevAssets = prevUSD.assetPerformance || {};
  const currAssets = currUSD.assetPerformance || {};
  
  const allAssetKeys = [...new Set([...Object.keys(prevAssets), ...Object.keys(currAssets)])];
  
  console.log('Activo                      | Prev Value  | Curr Value  | Diff      | Prev Precio | Curr Precio');
  console.log('-'.repeat(105));
  
  for (const assetKey of allAssetKeys.slice(0, 15)) { // Mostrar primeros 15
    const prevAsset = prevAssets[assetKey] || {};
    const currAsset = currAssets[assetKey] || {};
    
    const prevVal = prevAsset.totalValue || 0;
    const currVal = currAsset.totalValue || 0;
    const diff = currVal - prevVal;
    const prevPrice = prevAsset.currentPrice || 0;
    const currPrice = currAsset.currentPrice || 0;
    
    const diffIndicator = diff < -10 ? '⚠️' : (diff > 10 ? '📈' : '  ');
    
    console.log(`${diffIndicator} ${assetKey.padEnd(25).substring(0, 25)} | $${prevVal.toFixed(2).padStart(9)} | $${currVal.toFixed(2).padStart(9)} | $${diff.toFixed(2).padStart(8)} | $${prevPrice.toFixed(2).padStart(9)} | $${currPrice.toFixed(2).padStart(9)}`);
  }
  
  // 6. Verificar qué cálculo utilizó el caché anterior
  console.log('');
  console.log('─'.repeat(90));
  console.log('6. ANÁLISIS DE CAUSA RAÍZ');
  console.log('─'.repeat(90));
  
  // Calcular cuál debería ser el valor anterior para el cálculo del 2026-01-20
  // El caché busca: date < formattedDate ORDER BY date DESC LIMIT 1
  // Para 2026-01-20, debería encontrar 2026-01-16 (ya que 2026-01-17 no existe)
  
  console.log('El caché de unifiedMarketDataUpdate busca:');
  console.log('  WHERE date < formattedDate ORDER BY date DESC LIMIT 1');
  console.log(`  Para fecha ${problematicDate}, debería usar: ${previousDate}`);
  console.log('');
  
  // Verificar si 2026-01-17 o 2026-01-19 existen
  const [date17, date19] = await Promise.all([
    db.doc(`portfolioPerformance/${userId}/dates/2026-01-17`).get(),
    db.doc(`portfolioPerformance/${userId}/dates/2026-01-19`).get()
  ]);
  
  console.log(`2026-01-17 (viernes) existe: ${date17.exists}`);
  console.log(`2026-01-19 (domingo) existe: ${date19.exists}`);
  console.log('');
  
  // HIPÓTESIS:
  console.log('📋 HIPÓTESIS:');
  console.log('');
  
  if (!dates.includes('2026-01-17')) {
    console.log('1. ❌ NO hay datos para 2026-01-17 (viernes)');
    console.log('   - El mercado estuvo ABIERTO el viernes 17 de enero');
    console.log('   - unifiedMarketDataUpdate debería haber corrido el sábado 18/01 a las 00:05 ET');
    console.log('   - PERO no hay datos guardados');
    console.log('');
    console.log('2. El 2026-01-19 es Martin Luther King Jr. Day (festivo)');
    console.log('   - Pero el código dice 2026-01-19 en NYSE_HOLIDAYS_FALLBACK');
    console.log('   - ¿Puede haber un error de un día?');
  }
  
  // Verificar el adjustedDailyChangePercentage directamente
  console.log('');
  console.log('─'.repeat(90));
  console.log('7. VERIFICACIÓN DEL CÁLCULO adjustedDailyChangePercentage');
  console.log('─'.repeat(90));
  
  // El adjustedDailyChange se calcula como:
  // (currentValue - previousValue - cashFlow) / (previousValue + cashFlow)
  
  const prevValue = prevUSD.totalValue || 0;
  const currValue = currUSD.totalValue || 0;
  const cashFlow = currUSD.totalCashFlow || 0;
  
  // Calcular adjustedDailyChange esperado usando fórmula MWR
  const preChangeValue = prevValue + cashFlow;
  const expectedAdjChange = preChangeValue !== 0 
    ? (currValue - preChangeValue) / Math.abs(preChangeValue)
    : 0;
  
  console.log(`Valores para calcular adjustedDailyChangePercentage:`);
  console.log(`  previousValue (${previousDate}): $${prevValue.toFixed(2)}`);
  console.log(`  currentValue (${problematicDate}): $${currValue.toFixed(2)}`);
  console.log(`  totalCashFlow: $${cashFlow.toFixed(2)}`);
  console.log(`  preChangeValue (prev + cashFlow): $${preChangeValue.toFixed(2)}`);
  console.log('');
  console.log(`  adjustedDailyChange esperado: ${(expectedAdjChange * 100).toFixed(4)}%`);
  console.log(`  adjustedDailyChange guardado: ${((currUSD.adjustedDailyChangePercentage || 0) * 100).toFixed(4)}%`);
  
  // CONCLUSIÓN
  console.log('');
  console.log('='.repeat(90));
  console.log('CONCLUSIONES Y RECOMENDACIONES');
  console.log('='.repeat(90));
  
  if (!dates.includes('2026-01-17')) {
    console.log('');
    console.log('🔴 PROBLEMA IDENTIFICADO:');
    console.log('   El día 2026-01-17 (viernes) no tiene datos en portfolioPerformance');
    console.log('   Esto causa que el cálculo del 2026-01-20 use datos muy antiguos');
    console.log('');
    console.log('🔍 POSIBLES CAUSAS:');
    console.log('   1. La función unifiedMarketDataUpdate no se ejecutó el sábado 18/01');
    console.log('   2. Hubo un error durante la ejecución que no guardó los datos');
    console.log('   3. Problema con la verificación de festivos (confusión con MLK Day)');
  }
  
  console.log('');
  console.log('📝 ACCIONES RECOMENDADAS:');
  console.log('   1. Verificar logs de Cloud Functions para 2026-01-18 00:05 ET');
  console.log('   2. Recalcular datos para 2026-01-17 manualmente');
  console.log('   3. Eliminar o corregir el documento 2026-01-20');
  console.log('   4. Verificar que la lista de festivos esté correcta');
}

diagnose().catch(console.error).finally(() => process.exit(0));
