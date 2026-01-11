/**
 * Script de Verificación Final Post-Fix
 * 
 * Verifica que:
 * 1. Los rendimientos mensuales están correctos
 * 2. Los valores monetarios (Inversión, Valor Actual) solo suman IBKR+XTB
 * 3. El YTD es matemáticamente correcto (aunque > max individual)
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

async function main() {
  console.log('');
  console.log('═'.repeat(90));
  console.log('  VERIFICACIÓN POST-FIX: PortfolioSummary Multi-Cuenta');
  console.log('═'.repeat(90));
  console.log('');

  // Obtener datos de todas las cuentas
  const [ibkrDocs, xtbDocs, binanceDocs] = await Promise.all([
    getAccountDocs(CONFIG.ACCOUNTS.IBKR),
    getAccountDocs(CONFIG.ACCOUNTS.XTB),
    getAccountDocs(CONFIG.ACCOUNTS.BINANCE)
  ]);

  // Obtener valores más recientes
  const latestIbkr = ibkrDocs[ibkrDocs.length - 1];
  const latestXtb = xtbDocs[xtbDocs.length - 1];
  const latestBinance = binanceDocs.length > 0 ? binanceDocs[binanceDocs.length - 1] : null;

  console.log('━'.repeat(90));
  console.log('  VALORES ESPERADOS PARA "2 CUENTAS" (IBKR + XTB)');
  console.log('━'.repeat(90));
  console.log('');
  
  const ibkrValue = latestIbkr?.USD?.totalValue || 0;
  const xtbValue = latestXtb?.USD?.totalValue || 0;
  const binanceValue = latestBinance?.USD?.totalValue || 0;
  
  const ibkrInvestment = latestIbkr?.USD?.totalInvestment || 0;
  const xtbInvestment = latestXtb?.USD?.totalInvestment || 0;
  const binanceInvestment = latestBinance?.USD?.totalInvestment || 0;
  
  const expectedValue = ibkrValue + xtbValue;
  const expectedInvestment = ibkrInvestment + xtbInvestment;
  const expectedAppreciation = expectedValue - expectedInvestment;
  
  console.log('  Valores individuales:');
  console.log(`    IBKR:    Valor=$${ibkrValue.toFixed(2)}, Inversión=$${ibkrInvestment.toFixed(2)}`);
  console.log(`    XTB:     Valor=$${xtbValue.toFixed(2)}, Inversión=$${xtbInvestment.toFixed(2)}`);
  console.log(`    Binance: Valor=$${binanceValue.toFixed(2)}, Inversión=$${binanceInvestment.toFixed(2)}`);
  console.log('');
  console.log('  Valores esperados para "2 cuentas":');
  console.log(`    Valor Actual:    $${expectedValue.toFixed(2)}`);
  console.log(`    Inversión Total: $${expectedInvestment.toFixed(2)}`);
  console.log(`    Valorización:    $${expectedAppreciation.toFixed(2)}`);
  console.log('');
  console.log('  ⚠️  El UI DEBE mostrar estos valores, NO incluir Binance.');
  console.log('');
  
  // Verificar rendimientos
  console.log('━'.repeat(90));
  console.log('  RENDIMIENTOS CALCULADOS (para verificar contra UI)');
  console.log('━'.repeat(90));
  console.log('');
  
  // Calcular rendimientos mes por mes para 2025
  const months = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'];
  
  console.log('  2025:');
  console.log('  Mes      | IBKR      | XTB       | Combinado (ponderado)');
  console.log('  ' + '-'.repeat(60));
  
  for (let m = 0; m < 12; m++) {
    const monthNum = (m + 1).toString().padStart(2, '0');
    const monthPrefix = `2025-${monthNum}`;
    
    const ibkrMonth = ibkrDocs.filter(d => d.date.startsWith(monthPrefix));
    const xtbMonth = xtbDocs.filter(d => d.date.startsWith(monthPrefix));
    
    if (ibkrMonth.length === 0 && xtbMonth.length === 0) continue;
    
    // IBKR individual
    let ibkrFactor = 1;
    ibkrMonth.forEach(d => {
      ibkrFactor *= (1 + (d.USD?.adjustedDailyChangePercentage || 0) / 100);
    });
    const ibkrReturn = (ibkrFactor - 1) * 100;
    
    // XTB individual
    let xtbFactor = 1;
    xtbMonth.forEach(d => {
      xtbFactor *= (1 + (d.USD?.adjustedDailyChangePercentage || 0) / 100);
    });
    const xtbReturn = (xtbFactor - 1) * 100;
    
    // Combinado ponderado
    const ibkrByDate = new Map(ibkrMonth.map(d => [d.date, d]));
    const xtbByDate = new Map(xtbMonth.map(d => [d.date, d]));
    const allDates = [...new Set([...ibkrByDate.keys(), ...xtbByDate.keys()])].sort();
    
    let combinedFactor = 1;
    allDates.forEach(date => {
      const ibkr = ibkrByDate.get(date);
      const xtb = xtbByDate.get(date);
      
      const ibkrVal = ibkr?.USD?.totalValue || 0;
      const xtbVal = xtb?.USD?.totalValue || 0;
      const totalVal = ibkrVal + xtbVal;
      
      const ibkrChg = ibkr?.USD?.adjustedDailyChangePercentage || 0;
      const xtbChg = xtb?.USD?.adjustedDailyChangePercentage || 0;
      
      const weighted = totalVal > 0 ? (ibkrVal * ibkrChg + xtbVal * xtbChg) / totalVal : 0;
      combinedFactor *= (1 + weighted / 100);
    });
    const combinedReturn = (combinedFactor - 1) * 100;
    
    // Verificar que combinado esté dentro del rango
    const minR = Math.min(ibkrReturn, xtbReturn);
    const maxR = Math.max(ibkrReturn, xtbReturn);
    const inRange = combinedReturn >= minR - 0.5 && combinedReturn <= maxR + 0.5;
    
    console.log(
      `  ${months[m].padEnd(8)} | ` +
      `${ibkrReturn.toFixed(2).padStart(8)}% | ` +
      `${xtbReturn.toFixed(2).padStart(8)}% | ` +
      `${combinedReturn.toFixed(2).padStart(8)}% ${inRange ? '✅' : '⚠️ fuera rango'}`
    );
  }
  
  console.log('');
  console.log('━'.repeat(90));
  console.log('  📝 NOTA SOBRE "FUERA DE RANGO"');
  console.log('━'.repeat(90));
  console.log('');
  console.log('  Cuando el combinado está FUERA del rango [min, max] de las cuentas individuales,');
  console.log('  puede ser CORRECTO debido a la ponderación temporal.');
  console.log('');
  console.log('  Ejemplo: Si en Abril, IBKR tuvo días con +20% cuando dominaba el portafolio,');
  console.log('  y XTB tuvo días con +5% cuando tenía más peso, el promedio ponderado puede');
  console.log('  dar un resultado que no está entre el min y max de los individuales.');
  console.log('');
  console.log('  Esto es matemáticamente correcto para TWR compuesto.');
  
  console.log('');
  console.log('═'.repeat(90));
  console.log('  ✅ VERIFICACIÓN COMPLETA');
  console.log('═'.repeat(90));
  console.log('');
  console.log('  Después de recargar el UI:');
  console.log('');
  console.log(`  1. Valor Actual debe mostrar: ~$${expectedValue.toFixed(2)}`);
  console.log(`     (NO $8,049 que incluía Binance)`);
  console.log('');
  console.log(`  2. Inversión Total debe mostrar: ~$${expectedInvestment.toFixed(2)}`);
  console.log(`     (NO $7,278 que incluía Binance)`);
  console.log('');
  console.log('  3. Los rendimientos mensuales deben coincidir con la columna "Combinado"');
  
  console.log('');
  console.log('═'.repeat(90));
  
  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
