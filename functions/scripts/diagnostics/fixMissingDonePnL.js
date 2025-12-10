/**
 * Script para corregir doneProfitAndLoss faltante en días con ventas
 * 
 * PROPÓSITO:
 * Agregar doneProfitAndLoss a documentos donde hubo ventas pero el asset
 * no existe en assetPerformance (porque se vendió todo y quedó con units=0)
 * 
 * USO:
 *   node fixMissingDonePnL.js --analyze     # Solo analiza
 *   node fixMissingDonePnL.js --dry-run     # Muestra cambios sin aplicar
 *   node fixMissingDonePnL.js --fix         # Aplica los cambios
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
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
};

function parseArgs() {
  const args = process.argv.slice(2);
  let mode = 'analyze';
  
  args.forEach(arg => {
    if (arg === '--analyze') mode = 'analyze';
    else if (arg === '--dry-run') mode = 'dry-run';
    else if (arg === '--fix') mode = 'fix';
  });
  
  return { mode };
}

/**
 * Derivar tipos de cambio desde un documento existente
 */
function deriveExchangeRatesFromDoc(docData) {
  const rates = { USD: 1 };
  const usdTotal = docData.USD?.totalValue || 0;
  
  if (usdTotal <= 0) return rates;
  
  CONFIG.CURRENCIES.forEach(currency => {
    if (currency === 'USD') return;
    const currencyTotal = docData[currency]?.totalValue || 0;
    if (currencyTotal > 0) {
      rates[currency] = currencyTotal / usdTotal;
    }
  });
  
  return rates;
}

async function main() {
  const options = parseArgs();
  
  console.log('');
  console.log('='.repeat(100));
  console.log('CORRECCIÓN DE doneProfitAndLoss FALTANTE');
  console.log('='.repeat(100));
  console.log(`Modo: ${options.mode.toUpperCase()}`);
  console.log('');

  // Casos específicos a corregir
  const casesToFix = [
    { asset: 'AAPL_stock', date: '2025-04-25', assetName: 'AAPL' },
    { asset: 'NVO_stock', date: '2025-05-16', assetName: 'NVO' },
    { asset: 'UNH_stock', date: '2025-04-04', assetName: 'UNH' },
    { asset: 'MSFT_stock', date: '2025-05-29', assetName: 'MSFT' },
    { asset: 'DIS_stock', date: '2025-04-17', assetName: 'DIS' },
    { asset: 'NU_stock', date: '2025-03-12', assetName: 'NU' },
    { asset: 'PEP_stock', date: '2025-02-27', assetName: 'PEP' },
    { asset: 'PFE_stock', date: '2025-04-21', assetName: 'PFE' },
  ];

  // Obtener cuentas del usuario
  const accountsSnap = await db.collection('portfolioAccounts')
    .where('userId', '==', CONFIG.USER_ID)
    .get();
  const userAccountIds = accountsSnap.docs.map(d => d.id);

  const updates = [];
  const issues = [];

  for (const caseItem of casesToFix) {
    console.log(`\n📋 Analizando ${caseItem.asset} en ${caseItem.date}...`);
    
    // 1. Buscar las transacciones de venta de ese día para ese asset
    const txSnap = await db.collection('transactions')
      .where('assetName', '==', caseItem.assetName)
      .where('date', '==', caseItem.date)
      .where('type', '==', 'sell')
      .get();
    
    const sales = txSnap.docs
      .map(d => ({ id: d.id, ...d.data() }))
      .filter(tx => userAccountIds.includes(tx.portfolioAccountId));
    
    if (sales.length === 0) {
      console.log(`   ⚠️ No hay ventas de ${caseItem.assetName} el ${caseItem.date}`);
      continue;
    }
    
    // 2. Calcular doneProfitAndLoss usando valuePnL de las transacciones
    let totalDonePnL = 0;
    const salesByAccount = new Map();
    
    sales.forEach(sale => {
      const pnl = sale.valuePnL !== undefined ? sale.valuePnL : 0;
      totalDonePnL += pnl;
      
      if (!salesByAccount.has(sale.portfolioAccountId)) {
        salesByAccount.set(sale.portfolioAccountId, 0);
      }
      salesByAccount.set(sale.portfolioAccountId, salesByAccount.get(sale.portfolioAccountId) + pnl);
      
      console.log(`   📊 Venta: ${sale.amount.toFixed(4)} @ $${sale.price.toFixed(2)} -> valuePnL: $${pnl.toFixed(2)}`);
    });
    
    console.log(`   💰 Total doneProfitAndLoss: $${totalDonePnL.toFixed(2)}`);
    
    // 3. Verificar documento OVERALL
    const overallDocRef = db.doc(`portfolioPerformance/${CONFIG.USER_ID}/dates/${caseItem.date}`);
    const overallDoc = await overallDocRef.get();
    
    if (!overallDoc.exists) {
      console.log(`   ❌ Documento OVERALL ${caseItem.date} NO EXISTE`);
      issues.push({ type: 'missing_doc', asset: caseItem.asset, date: caseItem.date, level: 'OVERALL' });
      continue;
    }
    
    const overallData = overallDoc.data();
    const assetPerfExists = overallData.USD?.assetPerformance?.[caseItem.asset] !== undefined;
    const existingDonePnL = overallData.USD?.assetPerformance?.[caseItem.asset]?.doneProfitAndLoss;
    
    console.log(`   📄 OVERALL: assetPerformance.${caseItem.asset} ${assetPerfExists ? 'EXISTE' : 'NO EXISTE'}`);
    if (assetPerfExists) {
      console.log(`      doneProfitAndLoss: ${existingDonePnL !== undefined ? '$' + existingDonePnL.toFixed(2) : 'NO EXISTE'}`);
    }
    
    // Obtener tipos de cambio del documento
    const rates = deriveExchangeRatesFromDoc(overallData);
    
    // 4. Preparar update para OVERALL
    if (!assetPerfExists || existingDonePnL === undefined) {
      const updateData = {};
      
      CONFIG.CURRENCIES.forEach(currency => {
        const rate = rates[currency] || 1;
        const pnlInCurrency = currency === 'USD' ? totalDonePnL : totalDonePnL * rate;
        
        if (assetPerfExists) {
          // Solo agregar doneProfitAndLoss al asset existente
          updateData[`${currency}.assetPerformance.${caseItem.asset}.doneProfitAndLoss`] = pnlInCurrency;
        } else {
          // Crear el asset con solo doneProfitAndLoss (el resto será 0 porque vendió todo)
          updateData[`${currency}.assetPerformance.${caseItem.asset}`] = {
            totalValue: 0,
            totalInvestment: 0,
            totalROI: 0,
            dailyChangePercentage: 0,
            adjustedDailyChangePercentage: 0,
            rawDailyChangePercentage: 0,
            totalCashFlow: 0,
            units: 0,
            unrealizedProfitAndLoss: 0,
            doneProfitAndLoss: pnlInCurrency,
            dailyReturn: 0,
            monthlyReturn: 0,
            annualReturn: 0,
          };
        }
      });
      
      // También actualizar el doneProfitAndLoss total del documento si no existe
      const existingDocDonePnL = overallData.USD?.doneProfitAndLoss;
      if (existingDocDonePnL === undefined || existingDocDonePnL === null) {
        CONFIG.CURRENCIES.forEach(currency => {
          const rate = rates[currency] || 1;
          updateData[`${currency}.doneProfitAndLoss`] = currency === 'USD' ? totalDonePnL : totalDonePnL * rate;
        });
      }
      
      updates.push({
        ref: overallDocRef,
        path: `OVERALL/${caseItem.date}`,
        asset: caseItem.asset,
        donePnL: totalDonePnL,
        data: updateData,
      });
      
      console.log(`   ✅ Preparado update para OVERALL`);
    } else {
      console.log(`   ⏭️ OVERALL ya tiene doneProfitAndLoss correcto`);
    }
    
    // 5. Verificar y preparar updates para cada cuenta con ventas
    for (const [accountId, accountPnL] of salesByAccount) {
      const accountDocRef = db.doc(`portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates/${caseItem.date}`);
      const accountDoc = await accountDocRef.get();
      
      if (!accountDoc.exists) {
        console.log(`   ❌ Documento cuenta ${accountId}/${caseItem.date} NO EXISTE`);
        issues.push({ type: 'missing_doc', asset: caseItem.asset, date: caseItem.date, level: `account/${accountId}` });
        continue;
      }
      
      const accountData = accountDoc.data();
      const accountAssetExists = accountData.USD?.assetPerformance?.[caseItem.asset] !== undefined;
      const accountExistingPnL = accountData.USD?.assetPerformance?.[caseItem.asset]?.doneProfitAndLoss;
      
      console.log(`   📄 Cuenta ${accountId}: assetPerformance.${caseItem.asset} ${accountAssetExists ? 'EXISTE' : 'NO EXISTE'}`);
      
      if (!accountAssetExists || accountExistingPnL === undefined) {
        const accountRates = deriveExchangeRatesFromDoc(accountData);
        const accountUpdateData = {};
        
        CONFIG.CURRENCIES.forEach(currency => {
          const rate = accountRates[currency] || rates[currency] || 1;
          const pnlInCurrency = currency === 'USD' ? accountPnL : accountPnL * rate;
          
          if (accountAssetExists) {
            accountUpdateData[`${currency}.assetPerformance.${caseItem.asset}.doneProfitAndLoss`] = pnlInCurrency;
          } else {
            accountUpdateData[`${currency}.assetPerformance.${caseItem.asset}`] = {
              totalValue: 0,
              totalInvestment: 0,
              totalROI: 0,
              dailyChangePercentage: 0,
              adjustedDailyChangePercentage: 0,
              rawDailyChangePercentage: 0,
              totalCashFlow: 0,
              units: 0,
              unrealizedProfitAndLoss: 0,
              doneProfitAndLoss: pnlInCurrency,
              dailyReturn: 0,
              monthlyReturn: 0,
              annualReturn: 0,
            };
          }
        });
        
        // También actualizar el doneProfitAndLoss total del documento si no existe
        const accountExistingDocPnL = accountData.USD?.doneProfitAndLoss;
        if (accountExistingDocPnL === undefined || accountExistingDocPnL === null) {
          CONFIG.CURRENCIES.forEach(currency => {
            const rate = accountRates[currency] || rates[currency] || 1;
            accountUpdateData[`${currency}.doneProfitAndLoss`] = currency === 'USD' ? accountPnL : accountPnL * rate;
          });
        }
        
        updates.push({
          ref: accountDocRef,
          path: `account/${accountId}/${caseItem.date}`,
          asset: caseItem.asset,
          donePnL: accountPnL,
          data: accountUpdateData,
        });
        
        console.log(`   ✅ Preparado update para cuenta ${accountId}`);
      } else {
        console.log(`   ⏭️ Cuenta ${accountId} ya tiene doneProfitAndLoss correcto`);
      }
    }
  }

  // Resumen
  console.log('');
  console.log('='.repeat(100));
  console.log('RESUMEN');
  console.log('='.repeat(100));
  console.log(`Updates a aplicar: ${updates.length}`);
  console.log(`Issues (documentos faltantes): ${issues.length}`);
  
  if (issues.length > 0) {
    console.log('');
    console.log('Documentos faltantes (requieren backfill):');
    issues.forEach(i => console.log(`  - ${i.asset} ${i.date} (${i.level})`));
  }

  if (updates.length === 0) {
    console.log('');
    console.log('✅ No hay cambios que aplicar');
    process.exit(0);
  }

  // Modo analyze
  if (options.mode === 'analyze') {
    console.log('');
    console.log('📋 Modo ANALYZE: Solo se muestra el análisis');
    console.log('   Ejecuta con --dry-run para ver los cambios propuestos');
    console.log('   Ejecuta con --fix para aplicar los cambios');
    process.exit(0);
  }

  // Modo dry-run
  if (options.mode === 'dry-run') {
    console.log('');
    console.log('='.repeat(100));
    console.log('MODO DRY-RUN: Cambios propuestos (no aplicados)');
    console.log('='.repeat(100));
    
    updates.forEach((update, idx) => {
      console.log(`\n${idx + 1}. ${update.path}`);
      console.log(`   Asset: ${update.asset}`);
      console.log(`   doneProfitAndLoss: $${update.donePnL.toFixed(2)}`);
    });
    
    console.log('');
    console.log('⚠️ Modo DRY-RUN: No se aplicaron cambios');
    console.log('   Ejecuta con --fix para aplicar los cambios');
    process.exit(0);
  }

  // Modo fix
  if (options.mode === 'fix') {
    console.log('');
    console.log('='.repeat(100));
    console.log('MODO FIX: Aplicando correcciones');
    console.log('='.repeat(100));
    
    const batch = db.batch();
    
    updates.forEach(update => {
      batch.update(update.ref, update.data);
    });
    
    await batch.commit();
    
    console.log('');
    console.log(`✅ Se aplicaron ${updates.length} correcciones exitosamente`);
    
    // Verificación
    console.log('');
    console.log('Verificando cambios...');
    
    for (const update of updates.slice(0, 3)) {
      const doc = await update.ref.get();
      const pnl = doc.data()?.USD?.assetPerformance?.[update.asset]?.doneProfitAndLoss;
      console.log(`  ${update.path}: doneProfitAndLoss = $${pnl?.toFixed(2) || 'N/A'}`);
    }
    
    if (updates.length > 3) {
      console.log(`  ... y ${updates.length - 3} más`);
    }
  }

  process.exit(0);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
