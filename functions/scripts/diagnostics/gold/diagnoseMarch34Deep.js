/**
 * Diagnóstico profundo: Datos del 4 de marzo y transacciones
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();
const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';

async function main() {
  console.log('═'.repeat(80));
  console.log('DIAGNÓSTICO PROFUNDO: Transacciones y Cuentas');
  console.log('═'.repeat(80));
  
  // 1. Buscar cuentas directamente en portfolioPerformance/accounts
  console.log('\n📋 Cuentas en portfolioPerformance:');
  const accountsRef = db.collection(`portfolioPerformance/${userId}/accounts`);
  const accountsSnap = await accountsRef.get();
  console.log(`   Total: ${accountsSnap.size}`);
  
  const accountIds = [];
  accountsSnap.docs.forEach(doc => {
    accountIds.push(doc.id);
    console.log(`   - ${doc.id}`);
  });

  // 2. Obtener nombres de las cuentas
  console.log('\n📋 Nombres de cuentas (users/{userId}/portfolioAccounts):');
  const userAccountsSnap = await db.collection(`users/${userId}/portfolioAccounts`).get();
  const accountNames = {};
  userAccountsSnap.docs.forEach(doc => {
    accountNames[doc.id] = doc.data().name || doc.id;
    console.log(`   - ${doc.id}: ${doc.data().name}`);
  });

  // 3. Datos por cuenta para 03-03 y 03-04
  console.log('\n' + '─'.repeat(80));
  console.log('DATOS POR CUENTA');
  console.log('─'.repeat(80));
  
  for (const accId of accountIds) {
    const accName = accountNames[accId] || accId;
    console.log(`\n  ── ${accName} (${accId}) ──`);
    
    for (const date of ['2026-03-02', '2026-03-03', '2026-03-04']) {
      const docSnap = await db.doc(`portfolioPerformance/${userId}/accounts/${accId}/dates/${date}`).get();
      if (!docSnap.exists) {
        console.log(`     ${date}: NO EXISTE`);
        continue;
      }
      const data = docSnap.data();
      const usd = data.USD || {};
      console.log(`     ${date}: val=${usd.totalValue?.toFixed(2)}, inv=${usd.totalInvestment?.toFixed(2)}, cf=${usd.totalCashFlow?.toFixed(2)}, adj%=${usd.adjustedDailyChangePercentage?.toFixed(4)}%`);
    }
  }

  // 4. TODAS las transacciones de febrero y marzo 2026
  console.log('\n' + '─'.repeat(80));
  console.log('TRANSACCIONES (Feb-Mar 2026)');
  console.log('─'.repeat(80));
  
  const txSnap = await db.collection('transactions')
    .where('userId', '==', userId)
    .where('date', '>=', '2026-02-01')
    .orderBy('date', 'asc')
    .get();
  
  console.log(`\n  Total: ${txSnap.size}`);
  txSnap.docs.forEach(doc => {
    const tx = doc.data();
    const accName = accountNames[tx.portfolioAccountId] || tx.portfolioAccountId;
    console.log(`     ${tx.date} | ${tx.type.padEnd(5)} | ${(tx.name || tx.ticker || '').substring(0,15).padEnd(15)} | ${tx.quantity}x @ ${tx.price} ${tx.currency} | ${accName}`);
  });

  // 5. Verificar assets de la cuenta Trii
  console.log('\n' + '─'.repeat(80));
  console.log('ASSETS EN CUENTA TRII');
  console.log('─'.repeat(80));
  
  // Encontrar ID de Trii
  let triiId = null;
  for (const [id, name] of Object.entries(accountNames)) {
    if (name.toLowerCase().includes('trii')) {
      triiId = id;
      break;
    }
  }
  
  if (triiId) {
    console.log(`\n  Cuenta Trii ID: ${triiId}`);
    
    const assetsSnap = await db.collection('assets')
      .where('userId', '==', userId)
      .where('portfolioAccountId', '==', triiId)
      .where('isActive', '==', true)
      .get();
    
    console.log(`  Assets activos: ${assetsSnap.size}`);
    assetsSnap.docs.forEach(doc => {
      const a = doc.data();
      console.log(`     - ${a.name}: ${a.quantity}x @ ${a.averagePurchasePrice} ${a.currency}`);
    });
  }

  // 6. Revisar el cálculo en detalle para el 4 de marzo
  console.log('\n' + '─'.repeat(80));
  console.log('ANÁLISIS DETALLADO DEL CÁLCULO 04-03');
  console.log('─'.repeat(80));
  
  // Sumar los valores de cada cuenta para verificar si cuadra con overall
  let sumValue = 0;
  let sumInvestment = 0;
  let sumCashFlow = 0;
  
  console.log('\n  Verificando suma de cuentas vs OVERALL:');
  for (const accId of accountIds) {
    const docSnap = await db.doc(`portfolioPerformance/${userId}/accounts/${accId}/dates/2026-03-04`).get();
    if (docSnap.exists) {
      const usd = docSnap.data().USD || {};
      sumValue += usd.totalValue || 0;
      sumInvestment += usd.totalInvestment || 0;
      sumCashFlow += usd.totalCashFlow || 0;
      console.log(`     ${accountNames[accId] || accId}: val=${usd.totalValue?.toFixed(2)}, inv=${usd.totalInvestment?.toFixed(2)}, cf=${usd.totalCashFlow?.toFixed(2)}`);
    }
  }
  
  console.log(`\n  Suma de cuentas: val=${sumValue.toFixed(2)}, inv=${sumInvestment.toFixed(2)}, cf=${sumCashFlow.toFixed(2)}`);
  
  const overallSnap = await db.doc(`portfolioPerformance/${userId}/dates/2026-03-04`).get();
  if (overallSnap.exists) {
    const usd = overallSnap.data().USD || {};
    console.log(`  OVERALL:         val=${usd.totalValue?.toFixed(2)}, inv=${usd.totalInvestment?.toFixed(2)}, cf=${usd.totalCashFlow?.toFixed(2)}`);
    
    const diffVal = Math.abs(sumValue - usd.totalValue);
    const diffInv = Math.abs(sumInvestment - usd.totalInvestment);
    if (diffVal > 1 || diffInv > 1) {
      console.log(`  ⚠️ Discrepancia: val diff=${diffVal.toFixed(2)}, inv diff=${diffInv.toFixed(2)}`);
    } else {
      console.log('  ✅ Las sumas cuadran');
    }
  }

  // 7. Verificar si hay ventas que expliquen la reducción de inversión
  console.log('\n' + '─'.repeat(80));
  console.log('POSIBLES VENTAS (type=sell)');
  console.log('─'.repeat(80));
  
  const sellsSnap = await db.collection('transactions')
    .where('userId', '==', userId)
    .where('type', '==', 'sell')
    .where('date', '>=', '2026-03-01')
    .get();
  
  console.log(`\n  Ventas en marzo 2026: ${sellsSnap.size}`);
  sellsSnap.docs.forEach(doc => {
    const tx = doc.data();
    console.log(`     ${tx.date}: ${tx.name || tx.ticker} - ${tx.quantity}x @ ${tx.price} ${tx.currency}`);
  });

  console.log('\n' + '═'.repeat(80));
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
