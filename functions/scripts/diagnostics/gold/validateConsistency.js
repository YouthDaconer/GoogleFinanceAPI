/**
 * Script de validación de consistencia entre Overall y Multi-cuenta
 * Simula exactamente las consultas que hace el frontend
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

async function validateConsistency() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  const ibkrId = 'BZHvXz4QT2yqqqlFP22X';
  const xtbId = 'Z3gnboYgRlTvSZNGSu8j';
  const binanceId = 'zHZCvwpQeA2HoYMxDtPF';
  const currency = 'USD';
  
  console.log('='.repeat(80));
  console.log('VALIDACIÓN DE CONSISTENCIA - Simulando consultas del frontend');
  console.log('='.repeat(80));
  console.log('');
  
  // Simula getMultiAccountHistoricalReturns
  async function simulateMultiAccountReturns(accountIds, startDate = null) {
    const accountsData = {};
    
    for (const accId of accountIds) {
      let query = db.collection(`portfolioPerformance/${userId}/accounts/${accId}/dates`);
      if (startDate) {
        query = query.where('date', '>=', startDate);
      }
      const snap = await query.orderBy('date', 'asc').get();
      accountsData[accId] = {};
      snap.docs.forEach(doc => { accountsData[accId][doc.data().date] = doc.data()[currency]; });
    }
    
    const allDates = [...new Set(Object.values(accountsData).flatMap(data => Object.keys(data)))].sort();
    let factor = 1;
    
    allDates.forEach(date => {
      const contributions = [];
      
      accountIds.forEach(accId => {
        const data = accountsData[accId][date];
        if (data) {
          contributions.push({
            totalValue: data.totalValue || 0,
            adjustedDailyChangePercentage: data.adjustedDailyChangePercentage || 0,
            totalCashFlow: data.totalCashFlow || 0
          });
        }
      });
      
      if (contributions.length === 0) return;
      
      // Excluir cuentas nuevas
      const existingAccounts = contributions.filter(acc => {
        const isNewAccount = acc.adjustedDailyChangePercentage === 0 && acc.totalCashFlow < 0 && acc.totalValue > 0;
        return !isNewAccount;
      });
      
      // Calcular preChangeValue con la fórmula corregida
      const withPreValue = existingAccounts.map(acc => {
        const change = acc.adjustedDailyChangePercentage || 0;
        const currentValue = acc.totalValue || 0;
        const cashFlow = acc.totalCashFlow || 0;
        const preChangeValue = change !== 0 ? (currentValue + cashFlow) / (1 + change / 100) : currentValue + cashFlow;
        return { ...acc, preChangeValue: Math.max(0, preChangeValue) };
      });
      
      const totalWeight = withPreValue.reduce((sum, acc) => sum + acc.preChangeValue, 0);
      const totalCashFlow = contributions.reduce((sum, acc) => sum + acc.totalCashFlow, 0);
      const totalCurrentValue = contributions.reduce((sum, acc) => sum + acc.totalValue, 0);
      
      let change = 0;
      if (totalWeight > 0) {
        change = ((totalCurrentValue - totalWeight + totalCashFlow) / totalWeight) * 100;
      }
      
      factor *= (1 + change / 100);
    });
    
    return (factor - 1) * 100;
  }
  
  // Simula getHistoricalReturns para overall
  async function simulateOverallReturns(startDate = null) {
    let query = db.collection(`portfolioPerformance/${userId}/dates`);
    if (startDate) {
      query = query.where('date', '>=', startDate);
    }
    const snap = await query.orderBy('date', 'asc').get();
    
    let factor = 1;
    snap.docs.forEach(doc => {
      const data = doc.data()[currency];
      if (data) {
        factor *= (1 + (data.adjustedDailyChangePercentage || 0) / 100);
      }
    });
    
    return (factor - 1) * 100;
  }
  
  // ============================================================================
  // VALIDACIÓN 1Y (desde 2025-01-05)
  // ============================================================================
  console.log('VALIDACIÓN 1Y (desde 2025-01-05)');
  console.log('-'.repeat(60));
  console.log('');
  
  const startDate1Y = '2025-01-05';
  
  const [overall1Y, ibkrXtb1Y, allThree1Y] = await Promise.all([
    simulateOverallReturns(startDate1Y),
    simulateMultiAccountReturns([ibkrId, xtbId], startDate1Y),
    simulateMultiAccountReturns([ibkrId, xtbId, binanceId], startDate1Y)
  ]);
  
  console.log('RESULTADOS 1Y:');
  console.log('');
  console.log('  "Todas las cuentas" (overall):   ' + overall1Y.toFixed(2) + '%');
  console.log('  "IBKR + XTB" (2 cuentas):        ' + ibkrXtb1Y.toFixed(2) + '%');
  console.log('  "3 cuentas seleccionadas":       ' + allThree1Y.toFixed(2) + '%');
  console.log('');
  
  const diff1Y = Math.abs(overall1Y - allThree1Y);
  console.log('CONSISTENCIA 1Y:');
  console.log('  Overall vs 3 cuentas: ' + diff1Y.toFixed(4) + '% diferencia');
  console.log('  ' + (diff1Y < 0.1 ? '✅ CONSISTENTE' : '⚠️ REVISAR'));
  console.log('');
  
  // ============================================================================
  // COMPARACIÓN CON VALORES DE LA UI
  // ============================================================================
  console.log('='.repeat(80));
  console.log('COMPARACIÓN CON VALORES DE LA UI');
  console.log('='.repeat(80));
  console.log('');
  console.log('UI muestra:');
  console.log('  IBKR + XTB:        28.25%');
  console.log('  Todas las cuentas: 29.29%');
  console.log('');
  console.log('Backend calcula:');
  console.log('  IBKR + XTB:        ' + ibkrXtb1Y.toFixed(2) + '%');
  console.log('  Overall:           ' + overall1Y.toFixed(2) + '%');
  console.log('  3 cuentas:         ' + allThree1Y.toFixed(2) + '%');
  console.log('');
  
  const diffUIIbkrXtb = Math.abs(28.25 - ibkrXtb1Y);
  const diffUIOverall = Math.abs(29.29 - overall1Y);
  
  console.log('Diferencias UI vs Backend:');
  console.log('  IBKR+XTB: ' + diffUIIbkrXtb.toFixed(2) + '% ' + (diffUIIbkrXtb < 0.5 ? '✅' : '⚠️'));
  console.log('  Overall:  ' + diffUIOverall.toFixed(2) + '% ' + (diffUIOverall < 0.5 ? '✅' : '⚠️'));
  console.log('');
  
  if (diffUIIbkrXtb > 0.5 || diffUIOverall > 0.5) {
    console.log('NOTA: Las diferencias mayores pueden deberse a cache del frontend.');
    console.log('Refresca la página después de invalidar el cache para ver valores actualizados.');
  }
  
  process.exit(0);
}

validateConsistency().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
