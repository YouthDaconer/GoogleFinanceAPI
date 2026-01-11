/**
 * Script para comparar Overall vs Multi-cuenta calculado
 * Identifica diferencias entre el overall almacenado y el calculado con la fórmula corregida
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

async function compare() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  const ibkrId = 'BZHvXz4QT2yqqqlFP22X';
  const xtbId = 'Z3gnboYgRlTvSZNGSu8j';
  const binanceId = 'zHZCvwpQeA2HoYMxDtPF';
  const startDate = '2025-01-05';
  
  console.log('='.repeat(80));
  console.log('COMPARACIÓN: Overall vs Multi-cuenta calculado');
  console.log('='.repeat(80));
  console.log('');
  
  // Obtener overall
  const overallSnap = await db.collection(`portfolioPerformance/${userId}/dates`)
    .where('date', '>=', startDate)
    .orderBy('date', 'asc')
    .get();
  
  const overallData = {};
  overallSnap.docs.forEach(doc => {
    overallData[doc.data().date] = doc.data().USD;
  });
  
  // Obtener cuentas individuales
  const [ibkrSnap, xtbSnap, binanceSnap] = await Promise.all([
    db.collection(`portfolioPerformance/${userId}/accounts/${ibkrId}/dates`).where('date', '>=', startDate).orderBy('date', 'asc').get(),
    db.collection(`portfolioPerformance/${userId}/accounts/${xtbId}/dates`).where('date', '>=', startDate).orderBy('date', 'asc').get(),
    db.collection(`portfolioPerformance/${userId}/accounts/${binanceId}/dates`).where('date', '>=', startDate).orderBy('date', 'asc').get()
  ]);
  
  const ibkrData = {};
  ibkrSnap.docs.forEach(doc => { ibkrData[doc.data().date] = doc.data().USD; });
  
  const xtbData = {};
  xtbSnap.docs.forEach(doc => { xtbData[doc.data().date] = doc.data().USD; });
  
  const binanceData = {};
  binanceSnap.docs.forEach(doc => { binanceData[doc.data().date] = doc.data().USD; });
  
  const allDates = Object.keys(overallData).sort();
  
  console.log('Documentos overall:', Object.keys(overallData).length);
  console.log('Documentos IBKR:', Object.keys(ibkrData).length);
  console.log('Documentos XTB:', Object.keys(xtbData).length);
  console.log('Documentos Binance:', Object.keys(binanceData).length);
  console.log('');
  
  let diffCount = 0;
  const diffs = [];
  
  allDates.forEach(date => {
    const overall = overallData[date];
    
    const contributions = [];
    if (ibkrData[date]) {
      contributions.push({
        name: 'IBKR',
        totalValue: ibkrData[date].totalValue || 0,
        adjustedDailyChangePercentage: ibkrData[date].adjustedDailyChangePercentage || 0,
        totalCashFlow: ibkrData[date].totalCashFlow || 0
      });
    }
    if (xtbData[date]) {
      contributions.push({
        name: 'XTB',
        totalValue: xtbData[date].totalValue || 0,
        adjustedDailyChangePercentage: xtbData[date].adjustedDailyChangePercentage || 0,
        totalCashFlow: xtbData[date].totalCashFlow || 0
      });
    }
    if (binanceData[date]) {
      contributions.push({
        name: 'Binance',
        totalValue: binanceData[date].totalValue || 0,
        adjustedDailyChangePercentage: binanceData[date].adjustedDailyChangePercentage || 0,
        totalCashFlow: binanceData[date].totalCashFlow || 0
      });
    }
    
    // Excluir cuentas nuevas
    const existingAccounts = contributions.filter(acc => {
      const isNewAccount = acc.adjustedDailyChangePercentage === 0 && acc.totalCashFlow < 0 && acc.totalValue > 0;
      return !isNewAccount;
    });
    
    // Calcular preChangeValue
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
    
    let calcChange = 0;
    if (totalWeight > 0) {
      calcChange = ((totalCurrentValue - totalWeight + totalCashFlow) / totalWeight) * 100;
    }
    
    const storedChange = overall.adjustedDailyChangePercentage || 0;
    const diff = Math.abs(storedChange - calcChange);
    
    if (diff > 0.01) {
      diffCount++;
      diffs.push({
        date,
        stored: storedChange,
        calculated: calcChange,
        diff,
        contributions: contributions.map(c => c.name).join('+'),
        totalCashFlow
      });
    }
  });
  
  console.log('-'.repeat(80));
  console.log('DIFERENCIAS ENCONTRADAS (> 0.01%)');
  console.log('-'.repeat(80));
  console.log('');
  
  if (diffs.length === 0) {
    console.log('✅ No se encontraron diferencias significativas');
  } else {
    console.log(`⚠️ ${diffs.length} días con diferencias`);
    console.log('');
    console.log('Fecha       | Overall | Calculado | Diff    | Cuentas      | CashFlow');
    console.log('------------|---------|-----------|---------|--------------|----------');
    
    diffs.slice(0, 20).forEach(d => {
      console.log(
        `${d.date} | ` +
        `${d.stored.toFixed(4).padStart(7)}% | ` +
        `${d.calculated.toFixed(4).padStart(9)}% | ` +
        `${d.diff.toFixed(4).padStart(7)}% | ` +
        `${d.contributions.padEnd(12)} | ` +
        `${d.totalCashFlow.toFixed(2)}`
      );
    });
    
    if (diffs.length > 20) {
      console.log(`... y ${diffs.length - 20} más`);
    }
  }
  
  // Calcular impacto en TWR
  console.log('');
  console.log('-'.repeat(80));
  console.log('IMPACTO EN TWR ACUMULADO');
  console.log('-'.repeat(80));
  console.log('');
  
  let factorOverall = 1;
  let factorCalc = 1;
  
  allDates.forEach(date => {
    factorOverall *= (1 + (overallData[date]?.adjustedDailyChangePercentage || 0) / 100);
    
    // Recalcular
    const contributions = [];
    if (ibkrData[date]) contributions.push({ totalValue: ibkrData[date].totalValue || 0, adjustedDailyChangePercentage: ibkrData[date].adjustedDailyChangePercentage || 0, totalCashFlow: ibkrData[date].totalCashFlow || 0 });
    if (xtbData[date]) contributions.push({ totalValue: xtbData[date].totalValue || 0, adjustedDailyChangePercentage: xtbData[date].adjustedDailyChangePercentage || 0, totalCashFlow: xtbData[date].totalCashFlow || 0 });
    if (binanceData[date]) contributions.push({ totalValue: binanceData[date].totalValue || 0, adjustedDailyChangePercentage: binanceData[date].adjustedDailyChangePercentage || 0, totalCashFlow: binanceData[date].totalCashFlow || 0 });
    
    const existingAccounts = contributions.filter(acc => !(acc.adjustedDailyChangePercentage === 0 && acc.totalCashFlow < 0 && acc.totalValue > 0));
    const withPreValue = existingAccounts.map(acc => {
      const change = acc.adjustedDailyChangePercentage || 0;
      const preChangeValue = change !== 0 ? (acc.totalValue + acc.totalCashFlow) / (1 + change / 100) : acc.totalValue + acc.totalCashFlow;
      return { ...acc, preChangeValue: Math.max(0, preChangeValue) };
    });
    
    const totalWeight = withPreValue.reduce((sum, acc) => sum + acc.preChangeValue, 0);
    const totalCashFlow = contributions.reduce((sum, acc) => sum + acc.totalCashFlow, 0);
    const totalCurrentValue = contributions.reduce((sum, acc) => sum + acc.totalValue, 0);
    
    let calcChange = 0;
    if (totalWeight > 0) calcChange = ((totalCurrentValue - totalWeight + totalCashFlow) / totalWeight) * 100;
    
    factorCalc *= (1 + calcChange / 100);
  });
  
  console.log(`TWR Overall (Firestore): ${((factorOverall - 1) * 100).toFixed(4)}%`);
  console.log(`TWR Calculado (3 cuentas): ${((factorCalc - 1) * 100).toFixed(4)}%`);
  console.log(`Diferencia: ${(((factorCalc - 1) - (factorOverall - 1)) * 100).toFixed(4)}%`);
  
  process.exit(0);
}

compare().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
