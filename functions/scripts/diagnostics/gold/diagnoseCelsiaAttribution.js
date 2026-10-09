/**
 * Diagnóstico de CELSIA.CL en cuenta Trii (COP) - Atribución
 * 
 * Verifica el cálculo de contribución, peso al vender y P&L realizada
 * para un activo vendido en moneda COP que se está mostrando como USD.
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// Usuario y cuenta Trii
const USER_EMAIL = 'ccaicedousman@gmail.com';
const TRII_ACCOUNT_ID = 'ggM52GimbLL7jwvegc9o'; // De diagnoseTriiAssets.js

async function main() {
  console.log('═'.repeat(80));
  console.log('DIAGNÓSTICO CELSIA.CL - ATRIBUCIÓN (COP vs USD)');
  console.log('═'.repeat(80));
  
  // 1. Obtener userId desde las transacciones de Trii (ya conocemos la cuenta)
  console.log('\n🔍 PASO 1: Obtener userId desde cuenta Trii');
  const txCheckSnap = await db.collection('transactions')
    .where('portfolioAccountId', '==', TRII_ACCOUNT_ID)
    .limit(1)
    .get();
  
  if (txCheckSnap.empty) {
    console.log('   ❌ Transacciones de Trii no encontradas');
    process.exit(1);
  }
  
  const firstTx = txCheckSnap.docs[0].data();
  const userId = firstTx.userId;
  console.log(`   ✅ userId: ${userId}`);
  console.log(`   ✅ email (from tx): ${firstTx.userEmail || 'N/A'}`);
  
  // 2. Listar TODAS las cuentas del usuario
  console.log('\n🔍 PASO 2: Cuentas del usuario');
  const accountsSnap = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  console.log(`   Total cuentas activas: ${accountsSnap.size}`);
  const accountIds = [];
  accountsSnap.docs.forEach(doc => {
    const a = doc.data();
    console.log(`   - ${doc.id}: ${a.name} (currency: ${a.currency || 'N/A'})`);
    accountIds.push(doc.id);
  });
  
  // 3. Buscar CELSIA en TODAS las cuentas (assets)
  console.log('\n🔍 PASO 3: Buscar CELSIA en TODAS las cuentas (assets)');
  let celsiaAssetsFound = [];
  for (const accId of accountIds) {
    const assetsSnap = await db.collection('assets')
      .where('userId', '==', userId)
      .where('portfolioAccount', '==', accId)
      .where('name', '==', 'CELSIA')
      .get();
    
    assetsSnap.docs.forEach(doc => {
      const a = doc.data();
      celsiaAssetsFound.push({ accId, ...a });
      console.log(`   ✅ ${accId}: ${a.name} - units: ${a.units}, currency: ${a.currency}, isActive: ${a.isActive}`);
    });
  }
  
  if (celsiaAssetsFound.length === 0) {
    console.log('   ℹ️  CELSIA no encontrado en assets — buscando en portfolioPerformance...');
  }
  
  // 3.5 Buscar transacciones de CELSIA en TODAS las cuentas
  console.log('\n🔍 PASO 3.5: Buscar transacciones de CELSIA en TODAS las cuentas');
  let celsiaTxFound = [];
  for (const accId of accountIds) {
    const txSnap = await db.collection('transactions')
      .where('userId', '==', userId)
      .where('portfolioAccountId', '==', accId)
      .get();
    
    txSnap.docs.forEach(doc => {
      const t = doc.data();
      // Buscar por nombre, assetName o ticker
      const nameMatch = (t.name || '').includes('CELSIA');
      const assetNameMatch = (t.assetName || '').includes('CELSIA');
      const tickerMatch = (t.ticker || '').includes('CELSIA');
      if ((nameMatch || assetNameMatch || tickerMatch)) {
        celsiaTxFound.push({ accId, ...t });
        console.log(`   ✅ ${accId}: ${t.type} de CELSIA el ${t.date} - amount: ${t.amount}, price: ${t.price} ${t.currency}, valuePnL: ${t.valuePnL}`);
      }
    });
  }
  
  if (celsiaTxFound.length === 0) {
    console.log('   ℹ️  No se encontraron transacciones de CELSIA — buscando todas las ventas de Trii');
    // Buscar TODAS las ventas de Trii en junio
    const allTriiTx = await db.collection('transactions')
      .where('userId', '==', userId)
      .where('portfolioAccountId', '==', 'ggM52GimbLL7jwvegc9o')
      .where('date', '>=', '2026-06-01')
      .where('date', '<=', '2026-06-03')
      .get();
    
    allTriiTx.docs.forEach(doc => {
      const t = doc.data();
      console.log(`   📋 ${t.type}: ${t.name || t.assetName} el ${t.date} - amount: ${t.amount}, price: ${t.price} ${t.currency}, valuePnL: ${t.valuePnL}, dollarPriceToDate: ${t.dollarPriceToDate}`);
    });
  }
  
  // 4. Buscar CELSIA en portfolioPerformance de TODAS las cuentas
  console.log('\n🔍 PASO 4: Buscar CELSIA en portfolioPerformance de TODAS las cuentas');
  
  for (const accId of accountIds) {
    console.log(`\n   📊 Cuenta: ${accId}`);
    
    // Obtener las últimas 3 fechas disponibles
    let datesPath = `portfolioPerformance/${userId}/dates`;
    if (accId !== 'overall') {
      datesPath = `portfolioPerformance/${userId}/accounts/${accId}/dates`;
    }
    
    try {
      const recentDatesSnap = await db.collection(datesPath)
        .orderBy('date', 'desc')
        .limit(3)
        .get();
      
      for (const dateDoc of recentDatesSnap.docs) {
        const dateStr = dateDoc.id;
        const data = dateDoc.data();
        
        // Buscar en COP
        const copAssets = data.COP?.assetPerformance || {};
        const celsiaCOP = copAssets['CELSIA_stock'] || copAssets['CELSIA.CL_stock'];
        
        // Buscar en USD
        const usdAssets = data.USD?.assetPerformance || {};
        const celsiaUSD = usdAssets['CELSIA_stock'] || usdAssets['CELSIA.CL_stock'];
        
        if (celsiaCOP || celsiaUSD) {
          console.log(`     ${dateStr}:`);
          if (celsiaCOP) {
            console.log(`       COP: units=${celsiaCOP.units}, totalValue=${celsiaCOP.totalValue}, totalInvestment=${celsiaCOP.totalInvestment}, pnl=${celsiaCOP.unrealizedProfitAndLoss}`);
          }
          if (celsiaUSD) {
            console.log(`       USD: units=${celsiaUSD.units}, totalValue=${celsiaUSD.totalValue}, totalInvestment=${celsiaUSD.totalInvestment}, pnl=${celsiaUSD.unrealizedProfitAndLoss}`);
          }
        }
      }
    } catch (e) {
      console.log(`     Error accediendo ${datesPath}: ${e.message}`);
    }
  }
  
  // 5. Verificar si CELSIA aparece en el overall
  console.log('\n🔍 PASO 5: CELSIA en documento OVERALL');
  try {
    const overallDatesSnap = await db.collection(`portfolioPerformance/${userId}/dates`)
      .orderBy('date', 'desc')
      .limit(3)
      .get();
    
    for (const doc of overallDatesSnap.docs) {
      const dateStr = doc.id;
      const data = doc.data();
      const copAssets = data.COP?.assetPerformance || {};
      const usdAssets = data.USD?.assetPerformance || {};
      
      const celsiaCOP = copAssets['CELSIA_stock'] || copAssets['CELSIA.CL_stock'];
      const celsiaUSD = usdAssets['CELSIA_stock'] || usdAssets['CELSIA.CL_stock'];
      
      if (celsiaCOP || celsiaUSD) {
        console.log(`   ${dateStr}:`);
        if (celsiaCOP) {
          console.log(`     CELSIA en COP: units=${celsiaCOP.units}, totalValue=${celsiaCOP.totalValue}, totalInvestment=${celsiaCOP.totalInvestment}`);
        }
        if (celsiaUSD) {
          console.log(`     CELSIA en USD: units=${celsiaUSD.units}, totalValue=${celsiaUSD.totalValue}, totalInvestment=${celsiaUSD.totalInvestment}`);
        }
      }
    }
  } catch (e) {
    console.log(`   Error: ${e.message}`);
  }
  
  // 6. ANÁLISIS DEL PROBLEMA DE CONVERSIÓN
  console.log('\n🔍 PASO 6: ANÁLISIS DEL PROBLEMA DE CONVERSIÓN');
  console.log('   ═══════════════════════════════════════════');
  
  // Datos reales de CELSIA:
  // Compra: 5 unidades @ 4,805 COP el 2026-03-04
  // Venta: 5 unidades @ 9,002 COP el 2026-06-03
  // valuePnL: 20,985 COP (esto es CORRECTO: (9002-4805)*5 = 20,985)
  // 
  // En portfolioPerformance 2026-06-02 (antes de venta):
  //   COP: units=5, totalValue=23,900 COP, totalInvestment=24,025 COP, pnl=-125 COP
  //   USD: units=5, totalValue=6.69 USD, totalInvestment=6.41 USD, pnl=0.27 USD
  // 
  // En portfolioPerformance 2026-06-03 (después de venta):
  //   COP: units=0, totalValue=0, totalInvestment=0
  //   USD: units=0, totalValue=0, totalInvestment=0
  
  console.log('   DATOS REALES DE CELSIA.CL:');
  console.log('   ──────────────────────────');
  console.log('   Compra: 5 unidades @ 4,805 COP (2026-03-04)');
  console.log('   Venta:  5 unidades @ 9,002 COP  (2026-06-03)');
  console.log('   valuePnL de transacción: 20,985 COP');
  console.log('   ');
  console.log('   portfolioPerformance 2026-06-02 (antes de venta):');
  console.log('     COP: units=5, totalValue=$23,900, totalInvestment=$24,025, pnl=-$125');
  console.log('     USD: units=5, totalValue=$6.69, totalInvestment=$6.41, pnl=+$0.27');
  console.log('   ');
  console.log('   portfolioPerformance 2026-06-03 (después de venta):');
  console.log('     COP: units=0 (vendido completamente)');
  console.log('     USD: units=0 (vendido completamente)');
  console.log('');
  
  // Calcular lo que debería pasar
  const exchangeRate = 23900 / 6.6852211857176815;
  console.log('   TIPO DE CAMBIO IMPLÍCITO (2026-06-02):');
  console.log(`   $${exchangeRate.toFixed(2)} COP/USD`);
  console.log('');
  
  // Simular el cálculo del backend
  console.log('   SIMULACIÓN DEL CÁLCULO DE ATRIBUCIÓN:');
  console.log('   ─────────────────────────────────────');
  
  // Escenario 1: Backend usa datos COP directamente
  console.log('   Escenario A: Backend usa datos COP (correcto):');
  console.log('     assetValueStart = $24,025 COP');
  console.log('     assetValueEnd = $0 COP (vendido)');
  console.log('     realizedPnL = $20,985 COP (de transacción)');
  console.log('     unrealizedPnLAtStart = -$125 COP');
  console.log('     totalChange = $20,985 - (-$125) = $21,110 COP');
  console.log('     contribution = $21,110 / startTotalValue_COP * 100');
  console.log('     ✅ Esto es CORRECTO si startTotalValue está en COP');
  console.log('');
  
  // Escenario 2: Backend mezcla COP con USD
  console.log('   Escenario B: Backend mezcla COP con USD (INCORRECTO):');
  console.log('     assetValueStart = $24,025 COP (de datos COP)');
  console.log('     realizedPnL = $20,985 COP (de transacción en COP)');
  console.log('     startTotalValue = $XX,XXX USD (del documento overall en USD)');
  console.log('     contribution = $21,110 / $XX,XXX_USD * 100');
  console.log('     ❌ Esto mezcla monedas! El resultado está mal por ~factor 4000');
  console.log('');
  
  // Escenario 3: El frontend pasa currency=USD
  console.log('   Escenario C: Frontend pasa currency=USD:');
  console.log('     Backend busca CELSIA en data.USD.assetPerformance:');
  console.log('       assetValueStart = $6.41 USD');
  console.log('       assetValueEnd = $0 USD');
  console.log('     Backend busca transacciones y obtiene:');
  console.log('       realizedPnL = $20,985 COP (¡NO convertido a USD!)');
  console.log('     totalChange = $20,985 - $0.27 = $20,984.73 ❌');
  console.log('     contribution = $20,984.73 / startTotalValue_USD * 100');
  console.log('     ❌ ¡El valuePnL de la transacción está en COP pero se usa como USD!');
  console.log('     ❌ Esto infla la contribución por un factor de ~4000x');
  console.log('');
  
  // Verificar cuál escenario aplica
  console.log('   VERIFICACIÓN: ¿El valuePnL de CELSIA está en COP o USD?');
  console.log('   valuePnL de transacción = 20,985 (esto es COP, confirmado)');
  console.log('   Si se usara como USD: contribution = 20985 / 10000 * 100 = 209.85pp');
  console.log('   Si se convierte a COP: contribution = 21110 / startTotalValue_COP * 100');
  console.log('');
  
  // Calcular el startTotalValue de la cuenta Trii en COP
  console.log('   CÁLCULO DEL IMPACTO:');
  console.log('   ──────────────────');
  
  // Necesitamos el startTotalValue de la cuenta Trii en COP para YTD
  // Vamos a obtener los datos del 2026-01-01
  console.log('   Obteniendo startTotalValue de Trii en COP para YTD 2026...');
  
  const ytdStartCOP = await db.doc(`portfolioPerformance/${userId}/accounts/ggM52GimbLL7jwvegc9o/dates/2026-01-01`).get();
  if (ytdStartCOP.exists) {
    const copStart = ytdStartCOP.data().COP || {};
    console.log(`   startTotalValue (COP) = $${copStart.totalValue?.toLocaleString() || 'N/A'}`);
  } else {
    // Buscar el primer documento disponible
    const firstDateSnap = await db.doc(`portfolioPerformance/${userId}/accounts/ggM52GimbLL7jwvegc9o/dates`)
      .where('date', '>=', '2026-01-01')
      .orderBy('date', 'asc')
      .limit(1)
      .get();
    
    if (!firstDateSnap.empty) {
      const firstDate = firstDateSnap.docs[0].data();
      const copStart = firstDate.COP || {};
      console.log(`   Primer documento: ${firstDateSnap.docs[0].id}`);
      console.log(`   startTotalValue (COP) = $${copStart.totalValue?.toLocaleString() || 'N/A'}`);
    } else {
      console.log('   No hay datos desde 2026-01-01 para Trii');
    }
  }
  
  console.log('\n' + '═'.repeat(80));
  console.log('   CONCLUSIÓN:');
  console.log('   ═══════════');
  console.log('   El bug es ESCENARIO C:');
  console.log('   1. Frontend solicita atribución con currency=USD');
  console.log('   2. Backend obtiene CELSIA de data.USD (valueStart=$6.41, investment=$6.41)');
  console.log('   3. Backend obtiene transacciones: valuePnL=$20,985 COP');
  console.log('   4. Backend suma valuePnL directamente sin convertir a USD');
  console.log('   5. Resultado: contribución inflada ~4000x');
  console.log('');
  console.log('   SOLUCIÓN: Convertir valuePnL de la transacción a la moneda');
  console.log('   de solicitud usando dollarPriceToDate antes de calcular.');
  console.log('═'.repeat(80));
  
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
