/**
 * RESUMEN FINAL: Datos correctos vs datos con bug
 * 
 * Este script calcula los valores CORRECTOS usando la lógica corregida
 * y los compara con los datos guardados en OVERALL.
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
    XTB: 'Z3gnboYgRlTvSZNGSu8j',
    BINANCE: 'zHZCvwpQeA2HoYMxDtPF'
  },
  CURRENCY: 'USD'
};

async function getAccountDocs(accountId) {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`;
  const snapshot = await db.collection(path).orderBy('date', 'asc').get();
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

async function getOverallDocs() {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/dates`;
  const snapshot = await db.collection(path).orderBy('date', 'asc').get();
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

function calculateMonthlyReturnsWithPreChange(accountsDocs, currency, monthPrefix) {
  // Crear mapas por fecha
  const byDate = new Map();
  
  accountsDocs.forEach(({ name, docs }) => {
    docs.filter(d => d.date.startsWith(monthPrefix)).forEach(doc => {
      if (!byDate.has(doc.date)) {
        byDate.set(doc.date, {});
      }
      byDate.get(doc.date)[name] = doc[currency] || {};
    });
  });
  
  const sortedDates = [...byDate.keys()].sort();
  if (sortedDates.length === 0) return { return: 0, days: 0 };
  
  let factor = 1;
  
  sortedDates.forEach(date => {
    const dayData = byDate.get(date);
    
    // Calcular valor pre-cambio de cada cuenta
    let totalPreChange = 0;
    let weightedChange = 0;
    
    Object.entries(dayData).forEach(([name, data]) => {
      const value = data.totalValue || 0;
      const change = data.adjustedDailyChangePercentage || 0;
      
      // valor_pre_cambio = valor_actual / (1 + cambio/100)
      const preChange = change !== 0 ? value / (1 + change / 100) : value;
      totalPreChange += preChange;
      weightedChange += preChange * change;
    });
    
    // Ponderación correcta usando valor pre-cambio
    const dayChange = totalPreChange > 0 ? weightedChange / totalPreChange : 0;
    factor *= (1 + dayChange / 100);
  });
  
  return {
    return: (factor - 1) * 100,
    days: sortedDates.length
  };
}

function calculateMonthReturn(docs, currency, monthPrefix) {
  const monthDocs = docs.filter(d => d.date.startsWith(monthPrefix));
  if (monthDocs.length === 0) return { return: 0, days: 0 };
  
  let factor = 1;
  monthDocs.forEach(d => {
    const change = d[currency]?.adjustedDailyChangePercentage || 0;
    factor *= (1 + change / 100);
  });
  
  return {
    return: (factor - 1) * 100,
    days: monthDocs.length
  };
}

async function main() {
  console.log('');
  console.log('═'.repeat(110));
  console.log('  RESUMEN FINAL: Comparación de datos guardados vs cálculo correcto');
  console.log('═'.repeat(110));
  console.log('');

  const [overallDocs, ibkrDocs, xtbDocs, binanceDocs] = await Promise.all([
    getOverallDocs(),
    getAccountDocs(CONFIG.ACCOUNTS.IBKR),
    getAccountDocs(CONFIG.ACCOUNTS.XTB),
    getAccountDocs(CONFIG.ACCOUNTS.BINANCE)
  ]);

  const months = [
    '2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06',
    '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'
  ];

  const monthNames = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'];

  console.log('━'.repeat(110));
  console.log('  RENDIMIENTOS MENSUALES 2025');
  console.log('━'.repeat(110));
  console.log('');
  console.log('  Mes  | OVERALL  | IBKR+XTB  | IBKR+XTB+BIN | IBKR     | XTB      | Binance  | ¿OVERALL correcto?');
  console.log('  ' + '-'.repeat(105));

  months.forEach((monthPrefix, idx) => {
    // Datos guardados en OVERALL
    const overall = calculateMonthReturn(overallDocs, CONFIG.CURRENCY, monthPrefix);
    
    // Cálculo correcto: IBKR + XTB (sin Binance)
    const ibkrXtb = calculateMonthlyReturnsWithPreChange([
      { name: 'IBKR', docs: ibkrDocs },
      { name: 'XTB', docs: xtbDocs }
    ], CONFIG.CURRENCY, monthPrefix);
    
    // Cálculo correcto: IBKR + XTB + Binance
    const all = calculateMonthlyReturnsWithPreChange([
      { name: 'IBKR', docs: ibkrDocs },
      { name: 'XTB', docs: xtbDocs },
      { name: 'Binance', docs: binanceDocs }
    ], CONFIG.CURRENCY, monthPrefix);
    
    // Individuales
    const ibkr = calculateMonthReturn(ibkrDocs, CONFIG.CURRENCY, monthPrefix);
    const xtb = calculateMonthReturn(xtbDocs, CONFIG.CURRENCY, monthPrefix);
    const binance = calculateMonthReturn(binanceDocs, CONFIG.CURRENCY, monthPrefix);

    // Solo mostrar si hay datos
    if (overall.days === 0 && ibkr.days === 0) return;

    // Verificar si OVERALL coincide con algún cálculo
    const diffIbkrXtb = Math.abs(overall.return - ibkrXtb.return);
    const diffAll = Math.abs(overall.return - all.return);
    
    let status = '❓';
    if (diffIbkrXtb < 0.1 || diffAll < 0.1) {
      status = '✅';
    } else if (diffIbkrXtb > 1 || diffAll > 1) {
      status = '❌';
    } else {
      status = '⚠️';
    }

    console.log(
      `  ${monthNames[idx].padEnd(4)} | ` +
      `${overall.return.toFixed(2).padStart(7)}% | ` +
      `${ibkrXtb.return.toFixed(2).padStart(8)}% | ` +
      `${all.return.toFixed(2).padStart(11)}% | ` +
      `${ibkr.return.toFixed(2).padStart(7)}% | ` +
      `${xtb.return.toFixed(2).padStart(7)}% | ` +
      `${binance.days > 0 ? binance.return.toFixed(2).padStart(7) + '%' : '   N/A  '} | ` +
      `${status}`
    );
  });

  // Calcular YTD
  console.log('  ' + '-'.repeat(105));
  
  const ytdOverall = calculateMonthReturn(overallDocs.filter(d => d.date >= '2025-01-01'), CONFIG.CURRENCY, '2025');
  const ytdIbkrXtb = calculateMonthlyReturnsWithPreChange([
    { name: 'IBKR', docs: ibkrDocs.filter(d => d.date >= '2025-01-01') },
    { name: 'XTB', docs: xtbDocs.filter(d => d.date >= '2025-01-01') }
  ], CONFIG.CURRENCY, '2025');
  const ytdAll = calculateMonthlyReturnsWithPreChange([
    { name: 'IBKR', docs: ibkrDocs.filter(d => d.date >= '2025-01-01') },
    { name: 'XTB', docs: xtbDocs.filter(d => d.date >= '2025-01-01') },
    { name: 'Binance', docs: binanceDocs.filter(d => d.date >= '2025-01-01') }
  ], CONFIG.CURRENCY, '2025');
  const ytdIbkr = calculateMonthReturn(ibkrDocs.filter(d => d.date >= '2025-01-01'), CONFIG.CURRENCY, '2025');
  const ytdXtb = calculateMonthReturn(xtbDocs.filter(d => d.date >= '2025-01-01'), CONFIG.CURRENCY, '2025');
  const ytdBinance = calculateMonthReturn(binanceDocs.filter(d => d.date >= '2025-01-01'), CONFIG.CURRENCY, '2025');

  const diffYtdIbkrXtb = Math.abs(ytdOverall.return - ytdIbkrXtb.return);
  const statusYtd = diffYtdIbkrXtb > 1 ? '❌' : '✅';

  console.log(
    `  ${'YTD'.padEnd(4)} | ` +
    `${ytdOverall.return.toFixed(2).padStart(7)}% | ` +
    `${ytdIbkrXtb.return.toFixed(2).padStart(8)}% | ` +
    `${ytdAll.return.toFixed(2).padStart(11)}% | ` +
    `${ytdIbkr.return.toFixed(2).padStart(7)}% | ` +
    `${ytdXtb.return.toFixed(2).padStart(7)}% | ` +
    `${ytdBinance.days > 0 ? ytdBinance.return.toFixed(2).padStart(7) + '%' : '   N/A  '} | ` +
    `${statusYtd}`
  );

  console.log('');
  console.log('═'.repeat(110));
  console.log('  📋 ANÁLISIS DEL PROBLEMA');
  console.log('═'.repeat(110));
  console.log('');
  console.log('  1. OVERALL (guardado en Firestore) tiene un bug en el primer día del año:');
  console.log(`     - 2025-01-02: adjustedDailyChangePercentage = 0% (debería ser ~1.47%)`);
  console.log('');
  console.log('  2. Esto causa que el YTD de OVERALL sea ~1.5% menor que el correcto:');
  console.log(`     - OVERALL guardado: ${ytdOverall.return.toFixed(2)}%`);
  console.log(`     - Correcto (IBKR+XTB): ${ytdIbkrXtb.return.toFixed(2)}%`);
  console.log(`     - Diferencia: ${(ytdIbkrXtb.return - ytdOverall.return).toFixed(2)}%`);
  console.log('');
  console.log('  3. El bug ocurrió porque el scheduler no encontró datos del 2025-01-01 (feriado)');
  console.log('     y marcó el 2025-01-02 como "nueva inversión" con cambio = 0%.');
  console.log('');
  console.log('  4. SOLUCIÓN: Cuando el usuario selecciona "Todas las cuentas", usar la');
  console.log('     agregación multi-cuenta en tiempo real en lugar de OVERALL pre-calculado.');
  
  console.log('');
  console.log('═'.repeat(110));

  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
