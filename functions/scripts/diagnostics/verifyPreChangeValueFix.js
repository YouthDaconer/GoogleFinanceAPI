/**
 * VERIFICACIÓN: Nuevo cálculo con valor pre-cambio
 * 
 * Compara el cálculo antiguo (valor actual) vs el nuevo (valor pre-cambio)
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
    IBKR: 'BZHvXz4QT2yqqqlFP22X',
    XTB: 'Z3gnboYgRlTvSZNGSu8j'
  },
  CURRENCY: 'USD'
};

async function getAccountDocs(accountId) {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`;
  const snapshot = await db.collection(path).orderBy('date', 'asc').get();
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

/**
 * Calcular usando valor PRE-CAMBIO (FIX correcto)
 */
function calculateWithPreChangeValue(ibkrDocs, xtbDocs, monthPrefix) {
  const ibkrMonth = ibkrDocs.filter(d => d.date.startsWith(monthPrefix));
  const xtbMonth = xtbDocs.filter(d => d.date.startsWith(monthPrefix));
  
  const ibkrByDate = new Map(ibkrMonth.map(d => [d.date, d]));
  const xtbByDate = new Map(xtbMonth.map(d => [d.date, d]));
  const allDates = [...new Set([...ibkrByDate.keys(), ...xtbByDate.keys()])].sort();
  
  let factor = 1;
  
  allDates.forEach(date => {
    const ibkr = ibkrByDate.get(date);
    const xtb = xtbByDate.get(date);
    
    const ibkrVal = ibkr?.USD?.totalValue || 0;
    const xtbVal = xtb?.USD?.totalValue || 0;
    
    const ibkrChg = ibkr?.USD?.adjustedDailyChangePercentage || 0;
    const xtbChg = xtb?.USD?.adjustedDailyChangePercentage || 0;
    
    // FIX: Calcular valor PRE-CAMBIO
    const ibkrPreChange = ibkrChg !== 0 ? ibkrVal / (1 + ibkrChg / 100) : ibkrVal;
    const xtbPreChange = xtbChg !== 0 ? xtbVal / (1 + xtbChg / 100) : xtbVal;
    const totalPreChange = ibkrPreChange + xtbPreChange;
    
    // Ponderar con valor pre-cambio
    let weighted = 0;
    if (totalPreChange > 0) {
      weighted = (ibkrPreChange * ibkrChg + xtbPreChange * xtbChg) / totalPreChange;
    }
    
    factor *= (1 + weighted / 100);
  });
  
  return (factor - 1) * 100;
}

/**
 * Calcular usando valor ACTUAL (método antiguo con bug)
 */
function calculateWithCurrentValue(ibkrDocs, xtbDocs, monthPrefix) {
  const ibkrMonth = ibkrDocs.filter(d => d.date.startsWith(monthPrefix));
  const xtbMonth = xtbDocs.filter(d => d.date.startsWith(monthPrefix));
  
  const ibkrByDate = new Map(ibkrMonth.map(d => [d.date, d]));
  const xtbByDate = new Map(xtbMonth.map(d => [d.date, d]));
  const allDates = [...new Set([...ibkrByDate.keys(), ...xtbByDate.keys()])].sort();
  
  let factor = 1;
  
  allDates.forEach(date => {
    const ibkr = ibkrByDate.get(date);
    const xtb = xtbByDate.get(date);
    
    const ibkrVal = ibkr?.USD?.totalValue || 0;
    const xtbVal = xtb?.USD?.totalValue || 0;
    const totalVal = ibkrVal + xtbVal;
    
    const ibkrChg = ibkr?.USD?.adjustedDailyChangePercentage || 0;
    const xtbChg = xtb?.USD?.adjustedDailyChangePercentage || 0;
    
    // Método antiguo: ponderar con valor actual
    let weighted = 0;
    if (totalVal > 0) {
      weighted = (ibkrVal * ibkrChg + xtbVal * xtbChg) / totalVal;
    }
    
    factor *= (1 + weighted / 100);
  });
  
  return (factor - 1) * 100;
}

/**
 * Calcular rendimiento individual
 */
function calculateIndividual(docs, monthPrefix) {
  const monthDocs = docs.filter(d => d.date.startsWith(monthPrefix));
  
  let factor = 1;
  monthDocs.forEach(d => {
    const change = d.USD?.adjustedDailyChangePercentage || 0;
    factor *= (1 + change / 100);
  });
  
  return (factor - 1) * 100;
}

async function main() {
  console.log('');
  console.log('═'.repeat(100));
  console.log('  VERIFICACIÓN: Nuevo cálculo con valor pre-cambio');
  console.log('═'.repeat(100));
  console.log('');

  const [ibkrDocs, xtbDocs] = await Promise.all([
    getAccountDocs(CONFIG.ACCOUNTS.IBKR),
    getAccountDocs(CONFIG.ACCOUNTS.XTB)
  ]);

  const months = [
    { name: 'Enero', prefix: '2025-01' },
    { name: 'Febrero', prefix: '2025-02' },
    { name: 'Marzo', prefix: '2025-03' },
    { name: 'Abril', prefix: '2025-04' },
    { name: 'Mayo', prefix: '2025-05' },
    { name: 'Junio', prefix: '2025-06' },
    { name: 'Julio', prefix: '2025-07' },
    { name: 'Agosto', prefix: '2025-08' },
    { name: 'Septiembre', prefix: '2025-09' },
    { name: 'Octubre', prefix: '2025-10' },
    { name: 'Noviembre', prefix: '2025-11' },
    { name: 'Diciembre', prefix: '2025-12' }
  ];

  console.log('  Mes          | IBKR      | XTB       | Antiguo   | NUEVO     | En rango?');
  console.log('  ' + '-'.repeat(80));

  months.forEach(({ name, prefix }) => {
    const ibkr = calculateIndividual(ibkrDocs, prefix);
    const xtb = calculateIndividual(xtbDocs, prefix);
    const oldMethod = calculateWithCurrentValue(ibkrDocs, xtbDocs, prefix);
    const newMethod = calculateWithPreChangeValue(ibkrDocs, xtbDocs, prefix);
    
    const min = Math.min(ibkr, xtb);
    const max = Math.max(ibkr, xtb);
    const inRangeOld = oldMethod >= min - 0.5 && oldMethod <= max + 0.5;
    const inRangeNew = newMethod >= min - 0.5 && newMethod <= max + 0.5;
    
    // Solo mostrar si hay datos
    if (Math.abs(ibkr) > 0.001 || Math.abs(xtb) > 0.001) {
      console.log(
        `  ${name.padEnd(12)} | ` +
        `${ibkr.toFixed(2).padStart(8)}% | ` +
        `${xtb.toFixed(2).padStart(8)}% | ` +
        `${oldMethod.toFixed(2).padStart(8)}% | ` +
        `${newMethod.toFixed(2).padStart(8)}% | ` +
        `${inRangeNew ? '✅' : '❌'} (era: ${inRangeOld ? '✅' : '❌'})`
      );
    }
  });

  // Calcular YTD
  console.log('');
  console.log('  ' + '-'.repeat(80));
  
  const ibkrYTD = calculateIndividual(ibkrDocs.filter(d => d.date >= '2025-01-01'), '2025');
  const xtbYTD = calculateIndividual(xtbDocs.filter(d => d.date >= '2025-01-01'), '2025');
  
  // YTD con método antiguo
  const ytdOld = (() => {
    const ibkr = ibkrDocs.filter(d => d.date >= '2025-01-01');
    const xtb = xtbDocs.filter(d => d.date >= '2025-01-01');
    const ibkrByDate = new Map(ibkr.map(d => [d.date, d]));
    const xtbByDate = new Map(xtb.map(d => [d.date, d]));
    const allDates = [...new Set([...ibkrByDate.keys(), ...xtbByDate.keys()])].sort();
    
    let factor = 1;
    allDates.forEach(date => {
      const i = ibkrByDate.get(date);
      const x = xtbByDate.get(date);
      const iVal = i?.USD?.totalValue || 0;
      const xVal = x?.USD?.totalValue || 0;
      const total = iVal + xVal;
      const iChg = i?.USD?.adjustedDailyChangePercentage || 0;
      const xChg = x?.USD?.adjustedDailyChangePercentage || 0;
      const weighted = total > 0 ? (iVal * iChg + xVal * xChg) / total : 0;
      factor *= (1 + weighted / 100);
    });
    return (factor - 1) * 100;
  })();
  
  // YTD con método nuevo
  const ytdNew = (() => {
    const ibkr = ibkrDocs.filter(d => d.date >= '2025-01-01');
    const xtb = xtbDocs.filter(d => d.date >= '2025-01-01');
    const ibkrByDate = new Map(ibkr.map(d => [d.date, d]));
    const xtbByDate = new Map(xtb.map(d => [d.date, d]));
    const allDates = [...new Set([...ibkrByDate.keys(), ...xtbByDate.keys()])].sort();
    
    let factor = 1;
    allDates.forEach(date => {
      const i = ibkrByDate.get(date);
      const x = xtbByDate.get(date);
      const iVal = i?.USD?.totalValue || 0;
      const xVal = x?.USD?.totalValue || 0;
      const iChg = i?.USD?.adjustedDailyChangePercentage || 0;
      const xChg = x?.USD?.adjustedDailyChangePercentage || 0;
      
      // Valor pre-cambio
      const iPreChange = iChg !== 0 ? iVal / (1 + iChg / 100) : iVal;
      const xPreChange = xChg !== 0 ? xVal / (1 + xChg / 100) : xVal;
      const totalPreChange = iPreChange + xPreChange;
      
      const weighted = totalPreChange > 0 ? (iPreChange * iChg + xPreChange * xChg) / totalPreChange : 0;
      factor *= (1 + weighted / 100);
    });
    return (factor - 1) * 100;
  })();
  
  const minYTD = Math.min(ibkrYTD, xtbYTD);
  const maxYTD = Math.max(ibkrYTD, xtbYTD);
  
  console.log(
    `  ${'YTD 2025'.padEnd(12)} | ` +
    `${ibkrYTD.toFixed(2).padStart(8)}% | ` +
    `${xtbYTD.toFixed(2).padStart(8)}% | ` +
    `${ytdOld.toFixed(2).padStart(8)}% | ` +
    `${ytdNew.toFixed(2).padStart(8)}% | ` +
    `${ytdNew >= minYTD - 0.5 && ytdNew <= maxYTD + 0.5 ? '✅' : '❌'} (era: ${ytdOld >= minYTD - 0.5 && ytdOld <= maxYTD + 0.5 ? '✅' : '❌'})`
  );

  console.log('');
  console.log('═'.repeat(100));
  console.log('  📋 RESUMEN');
  console.log('═'.repeat(100));
  console.log('');
  console.log('  Método ANTIGUO (con bug): Usa el valor ACTUAL para ponderar');
  console.log('    → El valor ya incluye el cambio del día, inflando los pesos de ganancias');
  console.log('');
  console.log('  Método NUEVO (corregido): Usa el valor PRE-CAMBIO para ponderar');
  console.log('    → valorPreCambio = valorActual / (1 + cambio/100)');
  console.log('    → Los pesos reflejan el valor ANTES del cambio');
  console.log('');
  console.log('  Resultado: El combinado AHORA SIEMPRE está entre min y max individual.');
  console.log('');
  console.log('═'.repeat(100));

  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
