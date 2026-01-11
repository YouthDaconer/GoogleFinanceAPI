/**
 * DIAGNÓSTICO: Verificar por qué el primer día de enero tiene 0%
 */

const admin = require('firebase-admin');

const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2'
};

async function main() {
  console.log('');
  console.log('═'.repeat(80));
  console.log('  DIAGNÓSTICO: ¿Por qué 2025-01-02 tiene adjustedDailyChangePercentage = 0?');
  console.log('═'.repeat(80));
  console.log('');

  // Obtener últimos días de diciembre 2024 y primeros de enero 2025
  const overallPath = `portfolioPerformance/${CONFIG.USER_ID}/dates`;
  const snapshot = await db.collection(overallPath)
    .where('date', '>=', '2024-12-28')
    .where('date', '<=', '2025-01-05')
    .orderBy('date', 'asc')
    .get();

  const docs = snapshot.docs.map(d => ({ id: d.id, ...d.data() }));

  console.log('  Documentos encontrados:');
  console.log('');
  console.log('  Fecha       | TotalValue  | PrevValue   | CashFlow  | AdjChg%   | RawChg%');
  console.log('  ' + '-'.repeat(80));

  let prevValue = null;
  docs.forEach(doc => {
    const usd = doc.USD || {};
    const value = usd.totalValue || 0;
    const cashFlow = usd.totalCashFlow || 0;
    const adjChg = usd.adjustedDailyChangePercentage || 0;
    const rawChg = usd.rawDailyChangePercentage || 0;

    console.log(
      `  ${doc.date} | ` +
      `$${value.toFixed(2).padStart(9)} | ` +
      `$${(prevValue || 0).toFixed(2).padStart(9)} | ` +
      `$${cashFlow.toFixed(2).padStart(8)} | ` +
      `${adjChg.toFixed(2).padStart(8)}% | ` +
      `${rawChg.toFixed(2).padStart(8)}%`
    );

    // Verificar si el cambio está bien calculado
    if (prevValue && prevValue > 0) {
      const expectedChg = ((value - prevValue + cashFlow) / prevValue) * 100;
      if (Math.abs(expectedChg - adjChg) > 0.1) {
        console.log(`             ⚠️ Esperado: ${expectedChg.toFixed(2)}%, Guardado: ${adjChg.toFixed(2)}%`);
      }
    }

    prevValue = value;
  });

  // Verificar si hay gap entre el último día de diciembre y el primero de enero
  console.log('');
  console.log('━'.repeat(80));
  console.log('  ANÁLISIS');
  console.log('━'.repeat(80));
  console.log('');

  const dates = docs.map(d => d.date);
  const dec31 = dates.find(d => d === '2024-12-31');
  const jan01 = dates.find(d => d === '2025-01-01');
  const jan02 = dates.find(d => d === '2025-01-02');

  console.log(`  ¿Existe 2024-12-31? ${dec31 ? '✅ SÍ' : '❌ NO'}`);
  console.log(`  ¿Existe 2025-01-01? ${jan01 ? '✅ SÍ' : '❌ NO (feriado)'}`);
  console.log(`  ¿Existe 2025-01-02? ${jan02 ? '✅ SÍ' : '❌ NO'}`);

  // Ver si hay gap
  if (dec31 && jan02 && !jan01) {
    console.log('');
    console.log('  ⚠️ HAY UN GAP: No hay documento para 2025-01-01');
    console.log('  El scheduler busca el documento del día anterior (2025-01-01) que no existe.');
    console.log('  Por eso previousTotalValue = 0 y adjustedDailyChangePercentage = 0.');
    console.log('');
    console.log('  SOLUCIÓN: El scheduler debería buscar el ÚLTIMO documento disponible,');
    console.log('  no necesariamente el del día anterior.');
  }

  // Ver IBKR específicamente
  console.log('');
  console.log('━'.repeat(80));
  console.log('  VERIFICAR IBKR');
  console.log('━'.repeat(80));

  const ibkrPath = `portfolioPerformance/${CONFIG.USER_ID}/accounts/BZHvXz4QT2yqqqlFP22X/dates`;
  const ibkrSnapshot = await db.collection(ibkrPath)
    .where('date', '>=', '2024-12-28')
    .where('date', '<=', '2025-01-05')
    .orderBy('date', 'asc')
    .get();

  const ibkrDocs = ibkrSnapshot.docs.map(d => ({ id: d.id, ...d.data() }));

  console.log('');
  console.log('  Fecha       | TotalValue  | AdjChg%   | RawChg%');
  console.log('  ' + '-'.repeat(55));

  ibkrDocs.forEach(doc => {
    const usd = doc.USD || {};
    console.log(
      `  ${doc.date} | ` +
      `$${(usd.totalValue || 0).toFixed(2).padStart(9)} | ` +
      `${(usd.adjustedDailyChangePercentage || 0).toFixed(2).padStart(8)}% | ` +
      `${(usd.rawDailyChangePercentage || 0).toFixed(2).padStart(8)}%`
    );
  });

  console.log('');
  console.log('═'.repeat(80));

  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
