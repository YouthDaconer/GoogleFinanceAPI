/**
 * Diagnóstico: Cambio de rendimientos entre 3 y 4 de marzo 2026
 * Verifica coherencia de los cálculos de unifiedMarketDataUpdate
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
const DATES_TO_CHECK = ['2026-03-02', '2026-03-03', '2026-03-04'];

async function main() {
  console.log('═'.repeat(80));
  console.log('DIAGNÓSTICO: Rendimientos 3-4 Marzo 2026');
  console.log('═'.repeat(80));
  
  // 1. Obtener cuentas activas
  const accountsSnap = await db.collection(`users/${userId}/portfolioAccounts`)
    .where('isActive', '==', true)
    .get();
  
  const accounts = [];
  accountsSnap.docs.forEach(doc => {
    accounts.push({ id: doc.id, ...doc.data() });
  });
  console.log(`\n📋 Cuentas activas: ${accounts.length}`);
  accounts.forEach(a => console.log(`   - ${a.name} (${a.id})`));

  // 2. Revisar OVERALL
  console.log('\n' + '─'.repeat(80));
  console.log('OVERALL (portfolioPerformance/{userId}/dates)');
  console.log('─'.repeat(80));
  
  for (const date of DATES_TO_CHECK) {
    const docSnap = await db.doc(`portfolioPerformance/${userId}/dates/${date}`).get();
    if (!docSnap.exists) {
      console.log(`  ${date}: NO EXISTE`);
      continue;
    }
    const data = docSnap.data();
    const usd = data.USD || {};
    console.log(`\n  📅 ${date}:`);
    console.log(`     totalValue:        ${usd.totalValue?.toFixed(2)} USD`);
    console.log(`     totalInvestment:   ${usd.totalInvestment?.toFixed(2)} USD`);
    console.log(`     totalCashFlow:     ${usd.totalCashFlow?.toFixed(2)} USD`);
    console.log(`     dailyChange%:      ${usd.dailyChangePercentage?.toFixed(4)}%`);
    console.log(`     adjDailyChange%:   ${usd.adjustedDailyChangePercentage?.toFixed(4)}%`);
    console.log(`     updatedAt:         ${data.updatedAt ? new Date(data.updatedAt).toISOString() : 'N/A'}`);
  }

  // 3. Revisar cada cuenta individual
  for (const account of accounts) {
    console.log('\n' + '─'.repeat(80));
    console.log(`${account.name} (${account.id})`);
    console.log('─'.repeat(80));
    
    for (const date of DATES_TO_CHECK) {
      const docSnap = await db.doc(`portfolioPerformance/${userId}/accounts/${account.id}/dates/${date}`).get();
      if (!docSnap.exists) {
        console.log(`  ${date}: NO EXISTE`);
        continue;
      }
      const data = docSnap.data();
      const usd = data.USD || {};
      const cop = data.COP || {};
      
      console.log(`\n  📅 ${date}:`);
      console.log(`     USD: totalValue=${usd.totalValue?.toFixed(2)}, totalInvestment=${usd.totalInvestment?.toFixed(2)}, cashFlow=${usd.totalCashFlow?.toFixed(2)}, adjChange=${usd.adjustedDailyChangePercentage?.toFixed(4)}%`);
      if (cop.totalValue) {
        console.log(`     COP: totalValue=${cop.totalValue?.toFixed(0)}, totalInvestment=${cop.totalInvestment?.toFixed(0)}, cashFlow=${cop.totalCashFlow?.toFixed(0)}, adjChange=${cop.adjustedDailyChangePercentage?.toFixed(4)}%`);
      }
    }
  }

  // 4. Buscar transacciones del 4 de marzo
  console.log('\n' + '─'.repeat(80));
  console.log('TRANSACCIONES del 4 de marzo 2026');
  console.log('─'.repeat(80));
  
  const txSnap = await db.collection('transactions')
    .where('userId', '==', userId)
    .where('date', '>=', '2026-03-04')
    .where('date', '<=', '2026-03-04')
    .get();
  
  console.log(`\n  Transacciones encontradas: ${txSnap.size}`);
  txSnap.docs.forEach(doc => {
    const tx = doc.data();
    console.log(`     ${tx.type} - ${tx.name || tx.ticker} - ${tx.quantity}x @ ${tx.price} ${tx.currency} - cuenta: ${tx.portfolioAccountId}`);
  });

  // 5. Calcular el cambio esperado TWR (Time-Weighted Return)
  console.log('\n' + '─'.repeat(80));
  console.log('ANÁLISIS DE COHERENCIA TWR');
  console.log('─'.repeat(80));
  
  // Obtener datos de 03-03 y 03-04 para calcular
  const mar03 = await db.doc(`portfolioPerformance/${userId}/dates/2026-03-03`).get();
  const mar04 = await db.doc(`portfolioPerformance/${userId}/dates/2026-03-04`).get();
  
  if (mar03.exists && mar04.exists) {
    const d03 = mar03.data().USD || {};
    const d04 = mar04.data().USD || {};
    
    console.log('\n  Datos OVERALL (USD):');
    console.log(`     03-03: totalValue=${d03.totalValue?.toFixed(2)}, totalInvestment=${d03.totalInvestment?.toFixed(2)}`);
    console.log(`     04-03: totalValue=${d04.totalValue?.toFixed(2)}, totalInvestment=${d04.totalInvestment?.toFixed(2)}`);
    
    // Calcular ganancia real del día
    const cashFlowDay = (d04.totalCashFlow || 0) - (d03.totalCashFlow || 0);
    const investmentChange = (d04.totalInvestment || 0) - (d03.totalInvestment || 0);
    const valueChangeRaw = (d04.totalValue || 0) - (d03.totalValue || 0);
    
    // Ganancia ajustada = cambio en valor - nuevos cash flows del día
    const adjustedGain = valueChangeRaw - cashFlowDay;
    
    // Base para cálculo TWR: valor al inicio + mitad del cash flow (aproximación)
    const twrBase = d03.totalValue + (cashFlowDay / 2);
    const twrReturn = twrBase > 0 ? (adjustedGain / twrBase) * 100 : 0;
    
    console.log('\n  Cálculo TWR manual:');
    console.log(`     Cash flow nuevo del día:    ${cashFlowDay.toFixed(2)} USD`);
    console.log(`     Cambio en inversión total:  ${investmentChange.toFixed(2)} USD`);
    console.log(`     Cambio en valor bruto:      ${valueChangeRaw.toFixed(2)} USD`);
    console.log(`     Ganancia ajustada (excl CF): ${adjustedGain.toFixed(2)} USD`);
    console.log(`     Base TWR (inicio + CF/2):   ${twrBase.toFixed(2)} USD`);
    console.log(`     TWR calculado manualmente:  ${twrReturn.toFixed(4)}%`);
    console.log(`     TWR guardado en Firestore:  ${d04.adjustedDailyChangePercentage?.toFixed(4)}%`);
    
    const diff = Math.abs(twrReturn - (d04.adjustedDailyChangePercentage || 0));
    if (diff < 0.1) {
      console.log('     ✅ Los valores son coherentes (diff < 0.1%)');
    } else {
      console.log(`     ⚠️ Diferencia significativa: ${diff.toFixed(4)}%`);
    }
  }

  // 6. Verificar si la conversión COP->USD afectó los cálculos
  console.log('\n' + '─'.repeat(80));
  console.log('ANÁLISIS DE IMPACTO DE CUENTA TRII (COP)');
  console.log('─'.repeat(80));
  
  // Buscar la cuenta Trii
  const triiAccount = accounts.find(a => a.name.toLowerCase().includes('trii'));
  if (triiAccount) {
    console.log(`\n  Cuenta Trii encontrada: ${triiAccount.id}`);
    
    for (const date of ['2026-03-03', '2026-03-04']) {
      const triiSnap = await db.doc(`portfolioPerformance/${userId}/accounts/${triiAccount.id}/dates/${date}`).get();
      if (triiSnap.exists) {
        const triiData = triiSnap.data();
        const usd = triiData.USD || {};
        const cop = triiData.COP || {};
        console.log(`\n  ${date}:`);
        console.log(`     COP - totalValue: ${cop.totalValue?.toFixed(0)}, totalInvestment: ${cop.totalInvestment?.toFixed(0)}`);
        console.log(`     USD - totalValue: ${usd.totalValue?.toFixed(2)}, totalInvestment: ${usd.totalInvestment?.toFixed(2)}`);
        console.log(`     USD adjChange%: ${usd.adjustedDailyChangePercentage?.toFixed(4)}%`);
      }
    }
  }

  // 7. Verificar tasa de cambio USD/COP usada
  console.log('\n' + '─'.repeat(80));
  console.log('TASA DE CAMBIO COP/USD');
  console.log('─'.repeat(80));
  
  // La tasa se almacena generalmente en currencies o se obtiene del API
  // Vamos a calcular la tasa implícita a partir de los datos guardados
  if (triiAccount) {
    for (const date of ['2026-03-03', '2026-03-04']) {
      const triiSnap = await db.doc(`portfolioPerformance/${userId}/accounts/${triiAccount.id}/dates/${date}`).get();
      if (triiSnap.exists) {
        const triiData = triiSnap.data();
        const usd = triiData.USD || {};
        const cop = triiData.COP || {};
        if (cop.totalValue && usd.totalValue) {
          const implicitRate = cop.totalValue / usd.totalValue;
          console.log(`  ${date}: COP/USD implícito = ${implicitRate.toFixed(2)}`);
        }
      }
    }
  }

  console.log('\n' + '═'.repeat(80));
  console.log('FIN DEL DIAGNÓSTICO');
  console.log('═'.repeat(80));
  
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
