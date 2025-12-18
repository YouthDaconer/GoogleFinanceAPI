/**
 * Script de diagnóstico: Verificar cálculos de atribución del portafolio
 * 
 * Compara los cálculos realizados en usePortfolioAttribution con los datos en Firestore
 * para validar coherencia y precisión antes de migrar al back-end.
 * 
 * @see usePortfolioAttribution.ts
 * @see TopContributorsTable.tsx
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// ============================================================================
// CONFIGURACIÓN
// ============================================================================
const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const CURRENCY = 'USD';
const PERIOD = 'YTD'; // Año actual

// ============================================================================
// FUNCIONES AUXILIARES
// ============================================================================

function formatPercent(value) {
  if (value === null || value === undefined) return 'N/A';
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}%`;
}

function formatCurrency(value, currency = 'USD') {
  if (value === null || value === undefined) return 'N/A';
  return new Intl.NumberFormat('en-US', { 
    style: 'currency', 
    currency: currency,
    minimumFractionDigits: 2 
  }).format(value);
}

function formatPP(value) {
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}pp`;
}

// ============================================================================
// OBTENER DATOS DEL PORTAFOLIO
// ============================================================================

async function getPortfolioPerformanceData() {
  console.log('\n📊 1. OBTENIENDO DATOS DEL PORTAFOLIO DESDE FIRESTORE...\n');
  
  // Obtener documento más reciente (overall)
  const latestSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/dates`)
    .orderBy('date', 'desc')
    .limit(1)
    .get();
  
  // Obtener documento de inicio de año (YTD)
  const ytdStartDoc = await db.doc(`portfolioPerformance/${USER_ID}/dates/2025-01-02`).get();
  
  if (latestSnapshot.empty) {
    console.log('❌ No hay datos de performance');
    return null;
  }
  
  const latestDoc = latestSnapshot.docs[0];
  const latestData = latestDoc.data();
  const ytdStartData = ytdStartDoc.exists ? ytdStartDoc.data() : null;
  
  console.log(`📅 Fecha más reciente: ${latestDoc.id}`);
  console.log(`📅 Fecha inicio YTD: 2025-01-02 ${ytdStartDoc.exists ? '✅' : '❌ No encontrado'}`);
  console.log('');
  
  // Extraer métricas en USD
  const usdCurrent = latestData[CURRENCY] || {};
  const usdStart = ytdStartData?.[CURRENCY] || {};
  
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  RESUMEN DEL PORTAFOLIO (OVERALL)');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('');
  console.log(`📈 Valor actual:      ${formatCurrency(usdCurrent.totalValue)}`);
  console.log(`💰 Inversión:         ${formatCurrency(usdCurrent.totalInvestment)}`);
  console.log(`📊 Cash Flow:         ${formatCurrency(usdCurrent.totalCashFlow)}`);
  console.log(`✅ P&L No Realizada:  ${formatCurrency(usdCurrent.unrealizedProfitAndLoss)}`);
  console.log(`💵 P&L Realizada:     ${formatCurrency(usdCurrent.doneProfitAndLoss)}`);
  console.log(`📉 ROI Total:         ${formatPercent(usdCurrent.totalROI)}`);
  console.log('');
  console.log(`📅 Valor inicio YTD:  ${formatCurrency(usdStart.totalValue)}`);
  console.log('');
  
  return {
    current: usdCurrent,
    ytdStart: usdStart,
    latestDate: latestDoc.id
  };
}

// ============================================================================
// OBTENER ASSETS Y PRECIOS
// ============================================================================

async function getAssetsAndPrices() {
  console.log('\n📊 2. OBTENIENDO ASSETS Y PRECIOS ACTUALES...\n');
  
  // Obtener accounts del usuario para filtrar por userId
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', USER_ID)
    .get();
  
  const accountIds = accountsSnapshot.docs.map(d => d.id);
  console.log(`📦 Cuentas del usuario: ${accountIds.length}`);
  accountIds.forEach(id => console.log(`   - ${id}`));
  
  // Obtener assets activos del usuario (están en colección raíz, filtrados por portfolioAccount)
  let allAssetsDocs = [];
  for (const accountId of accountIds) {
    const accountAssets = await db.collection('assets')
      .where('portfolioAccount', '==', accountId)
      .where('isActive', '==', true)
      .get();
    allAssetsDocs = allAssetsDocs.concat(accountAssets.docs);
  }
  
  const assetsSnapshot = { docs: allAssetsDocs };
  
  console.log(`📦 Assets activos encontrados: ${assetsSnapshot.docs.length}`);
  
  const assets = [];
  const tickerSet = new Set();
  
  for (const doc of assetsSnapshot.docs) {
    const data = doc.data();
    assets.push({
      id: doc.id,
      ticker: data.name,
      units: data.units || 0,
      unitValue: data.unitValue || 0,
      acquisitionDollarValue: data.acquisitionDollarValue || 1,
      portfolioAccount: data.portfolioAccount,
      isActive: data.isActive,
      currency: data.currency || 'USD'
    });
    tickerSet.add(data.name);
  }
  
  // Obtener precios actuales
  const tickers = Array.from(tickerSet);
  console.log(`🔖 Tickers únicos: ${tickers.length}`);
  
  const pricesMap = new Map();
  
  // Obtener precios en batches (Firestore limita a 30 items en 'in' query)
  for (let i = 0; i < tickers.length; i += 30) {
    const batch = tickers.slice(i, i + 30);
    const pricesSnapshot = await db.collection('currentPrices')
      .where('symbol', 'in', batch)
      .get();
    
    for (const doc of pricesSnapshot.docs) {
      const data = doc.data();
      pricesMap.set(data.symbol, {
        symbol: data.symbol,
        price: data.price || 0,
        name: data.name,
        sector: data.sector,
        type: data.type,
        currency: data.currency || 'USD',
        ytdReturn: data.ytdReturn,
        yearReturn: data.yearReturn,
        threeMonthReturn: data.threeMonthReturn,
        sixMonthReturn: data.sixMonthReturn
      });
    }
  }
  
  console.log(`💹 Precios encontrados: ${pricesMap.size}`);
  
  return { assets, pricesMap };
}

// ============================================================================
// OBTENER ASSET PERFORMANCE DEL DOCUMENTO DE FIRESTORE
// ============================================================================

async function getAssetPerformanceFromFirestore() {
  console.log('\n📊 3. OBTENIENDO ASSET PERFORMANCE DE FIRESTORE...\n');
  
  // Documento más reciente
  const latestSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/dates`)
    .orderBy('date', 'desc')
    .limit(1)
    .get();
  
  if (latestSnapshot.empty) return null;
  
  const latestData = latestSnapshot.docs[0].data();
  const assetPerformance = latestData[CURRENCY]?.assetPerformance || {};
  
  console.log(`📋 Assets en assetPerformance: ${Object.keys(assetPerformance).length}`);
  
  // Mostrar algunos ejemplos
  const assetKeys = Object.keys(assetPerformance).slice(0, 5);
  console.log('\n📝 Ejemplo de estructura assetPerformance:\n');
  
  for (const key of assetKeys) {
    const asset = assetPerformance[key];
    console.log(`  ${key}:`);
    console.log(`    - totalValue: ${formatCurrency(asset.totalValue)}`);
    console.log(`    - totalInvestment: ${formatCurrency(asset.totalInvestment)}`);
    console.log(`    - totalROI: ${formatPercent(asset.totalROI)}`);
    console.log(`    - dailyChangePercentage: ${formatPercent(asset.dailyChangePercentage)}`);
    console.log('');
  }
  
  return assetPerformance;
}

// ============================================================================
// CALCULAR ATRIBUCIÓN (SIMULAR usePortfolioAttribution)
// ============================================================================

async function calculateAttribution(assets, pricesMap, assetPerformance, portfolioData) {
  console.log('\n📊 4. CALCULANDO ATRIBUCIÓN (SIMULANDO usePortfolioAttribution)...\n');
  
  // Agrupar assets por ticker
  const assetGroups = new Map();
  let totalPortfolioValue = 0;
  let totalPortfolioInvestment = 0;
  
  for (const asset of assets) {
    const priceData = pricesMap.get(asset.ticker);
    if (!priceData) {
      console.log(`   ⚠️ Sin precio para: ${asset.ticker}`);
      continue;
    }
    
    const price = priceData.price || 0;
    const units = asset.units || 0;
    const portfolioValue = units * price;
    // FIX: La inversión debe ser units * unitValue en USD
    // acquisitionDollarValue es el tipo de cambio de la moneda en la que compró
    const investment = units * asset.unitValue; // Inversión en USD
    
    totalPortfolioValue += portfolioValue;
    totalPortfolioInvestment += investment;
    
    const ticker = asset.ticker;
    const assetType = asset.assetType || 'stock';
    const key = `${ticker}_${assetType}`;
    
    const existing = assetGroups.get(ticker);
    if (existing) {
      existing.totalUnits += units;
      existing.totalValue += portfolioValue;
      existing.totalInvestment += investment;
    } else {
      assetGroups.set(ticker, {
        ticker,
        assetType,
        key,
        totalUnits: units,
        totalValue: portfolioValue,
        totalInvestment: investment,
        priceData
      });
    }
  }
  
  console.log(`📊 Valor total calculado: ${formatCurrency(totalPortfolioValue)}`);
  console.log(`📊 Inversión total calculada: ${formatCurrency(totalPortfolioInvestment)}`);
  console.log(`📊 Valor en Firestore: ${formatCurrency(portfolioData.current.totalValue)}`);
  console.log(`📊 Inversión en Firestore: ${formatCurrency(portfolioData.current.totalInvestment)}`);
  console.log(`📊 Diferencia Valor: ${formatCurrency(totalPortfolioValue - portfolioData.current.totalValue)}`);
  console.log(`📊 Diferencia Inversión: ${formatCurrency(totalPortfolioInvestment - portfolioData.current.totalInvestment)}`);
  console.log('');
  
  // Calcular atribución por activo - MÉTODO ACTUAL (usePortfolioAttribution)
  const attributionsCurrentMethod = [];
  // Calcular atribución por activo - MÉTODO PROPUESTO (usando assetPerformance)
  const attributionsProposed = [];
  
  for (const [ticker, group] of assetGroups) {
    const weight = totalPortfolioValue > 0 ? group.totalValue / totalPortfolioValue : 0;
    
    // MÉTODO ACTUAL: Obtener retorno del activo (YTD) desde CurrentPrice
    const ytdReturnStr = group.priceData.ytdReturn || '0';
    const assetReturnFromPrice = parseFloat(ytdReturnStr.toString().replace(/[%,]/g, '').trim()) || 0;
    
    // Contribución ACTUAL = peso × retorno del activo del mercado
    const contributionCurrent = weight * assetReturnFromPrice;
    
    // Datos de Firestore para comparación
    const firestoreData = assetPerformance[group.key] || 
                          assetPerformance[`${ticker}_stock`] || 
                          assetPerformance[`${ticker}_etf`] || 
                          assetPerformance[ticker] || {};
    
    // MÉTODO PROPUESTO: Usar totalROI del assetPerformance (retorno personal)
    const assetReturnFromFS = firestoreData.totalROI || 0;
    const contributionProposed = weight * assetReturnFromFS;
    
    // ROI calculado manualmente
    const calculatedROI = group.totalInvestment > 0 
      ? ((group.totalValue - group.totalInvestment) / group.totalInvestment) * 100 
      : 0;
    
    const attrBase = {
      ticker,
      name: group.priceData.name || ticker,
      sector: group.priceData.sector || 'Unknown',
      type: group.priceData.type || 'stock',
      weight: weight * 100,
      valueEnd: group.totalValue,
      investment: group.totalInvestment,
      firestoreValue: firestoreData.totalValue,
      firestoreROI: firestoreData.totalROI,
      calculatedROI
    };
    
    attributionsCurrentMethod.push({
      ...attrBase,
      returnPercent: assetReturnFromPrice,
      contribution: contributionCurrent,
      method: 'CurrentPrice.ytdReturn'
    });
    
    attributionsProposed.push({
      ...attrBase,
      returnPercent: assetReturnFromFS,
      contribution: contributionProposed,
      method: 'assetPerformance.totalROI'
    });
  }
  
  // Ordenar por contribución descendente
  attributionsCurrentMethod.sort((a, b) => b.contribution - a.contribution);
  attributionsProposed.sort((a, b) => b.contribution - a.contribution);
  
  return { 
    attributionsCurrentMethod, 
    attributionsProposed, 
    totalPortfolioValue, 
    totalPortfolioInvestment 
  };
}

// ============================================================================
// COMPARAR Y MOSTRAR RESULTADOS
// ============================================================================

function displayAttributionResults(attributionsCurrentMethod, attributionsProposed, totalValue, portfolioData) {
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  COMPARACIÓN DE MÉTODOS DE ATRIBUCIÓN');
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  console.log('📋 MÉTODO ACTUAL (usePortfolioAttribution): Usa CurrentPrice.ytdReturn');
  console.log('   - Este es el retorno YTD del ACTIVO en el mercado');
  console.log('   - NO considera cuándo el usuario compró el activo');
  console.log('');
  console.log('📋 MÉTODO PROPUESTO: Usa assetPerformance.totalROI');
  console.log('   - Este es el ROI PERSONAL del usuario');
  console.log('   - Considera fecha de compra y precio de adquisición');
  console.log('');
  
  // TABLA COMPARATIVA TOP 10
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  TOP 10 CONTRIBUYENTES - COMPARACIÓN');
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  const top10Current = attributionsCurrentMethod.slice(0, 10);
  const top10Proposed = attributionsProposed.slice(0, 10);
  
  console.log('Rank  Ticker      Peso'.padEnd(30) + '| Método Actual (YTD)'.padEnd(25) + '| Método Propuesto (ROI)'.padEnd(25) + '| Diferencia');
  console.log('─'.repeat(110));
  
  let sumCurrent = 0;
  let sumProposed = 0;
  
  for (let i = 0; i < Math.max(top10Current.length, top10Proposed.length); i++) {
    const curr = top10Current[i];
    const prop = attributionsProposed.find(a => a.ticker === curr?.ticker);
    
    if (!curr) continue;
    
    sumCurrent += curr.contribution;
    if (prop) sumProposed += prop.contribution;
    
    const diff = prop ? curr.contribution - prop.contribution : 0;
    const diffSymbol = Math.abs(diff) > 1 ? (diff > 0 ? '⬆️' : '⬇️') : '≈';
    
    console.log(
      `${(i + 1).toString().padStart(2)}.   ` +
      curr.ticker.padEnd(12) +
      `${curr.weight.toFixed(1)}%`.padEnd(8) +
      `| ROI: ${formatPercent(curr.returnPercent).padEnd(8)} → ${formatPP(curr.contribution).padEnd(10)}` +
      `| ROI: ${formatPercent(prop?.returnPercent || 0).padEnd(8)} → ${formatPP(prop?.contribution || 0).padEnd(10)}` +
      `| ${diffSymbol} ${formatPP(diff)}`
    );
  }
  
  // Suma total de todos los activos
  const totalSumCurrent = attributionsCurrentMethod.reduce((sum, a) => sum + a.contribution, 0);
  const totalSumProposed = attributionsProposed.reduce((sum, a) => sum + a.contribution, 0);
  
  console.log('─'.repeat(110));
  console.log(`${'SUMA TOTAL'.padEnd(25)} | ${formatPP(totalSumCurrent).padEnd(25)} | ${formatPP(totalSumProposed).padEnd(25)} | Δ ${formatPP(totalSumCurrent - totalSumProposed)}`);
  
  // BOTTOM 5 COMPARACIÓN
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  BOTTOM 5 CONTRIBUYENTES - COMPARACIÓN');
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  const bottom5Current = attributionsCurrentMethod.slice(-5).reverse();
  
  console.log('Rank  Ticker      Peso'.padEnd(30) + '| Método Actual (YTD)'.padEnd(25) + '| Método Propuesto (ROI)'.padEnd(25) + '| Diferencia');
  console.log('─'.repeat(110));
  
  for (let i = 0; i < bottom5Current.length; i++) {
    const curr = bottom5Current[i];
    const prop = attributionsProposed.find(a => a.ticker === curr.ticker);
    
    const diff = prop ? curr.contribution - prop.contribution : 0;
    const diffSymbol = Math.abs(diff) > 1 ? (diff > 0 ? '⬆️' : '⬇️') : '≈';
    
    console.log(
      `${(attributionsCurrentMethod.length - 4 + i).toString().padStart(2)}.   ` +
      curr.ticker.padEnd(12) +
      `${curr.weight.toFixed(1)}%`.padEnd(8) +
      `| ROI: ${formatPercent(curr.returnPercent).padEnd(8)} → ${formatPP(curr.contribution).padEnd(10)}` +
      `| ROI: ${formatPercent(prop?.returnPercent || 0).padEnd(8)} → ${formatPP(prop?.contribution || 0).padEnd(10)}` +
      `| ${diffSymbol} ${formatPP(diff)}`
    );
  }
}

// ============================================================================
// VERIFICAR COHERENCIA DE LA SUMA DE CONTRIBUCIONES
// ============================================================================

function verifyContributionSum(attributionsCurrentMethod, attributionsProposed, portfolioData) {
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  VERIFICACIÓN DE COHERENCIA MATEMÁTICA');
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  const sumCurrentMethod = attributionsCurrentMethod.reduce((sum, a) => sum + a.contribution, 0);
  const sumProposedMethod = attributionsProposed.reduce((sum, a) => sum + a.contribution, 0);
  
  // ROI Total de Firestore (el que debería coincidir)
  const expectedROI = portfolioData.current.totalROI || 0;
  
  // Calcular YTD return basado en cambio de valor (esto incluye cashflows)
  const ytdStart = portfolioData.ytdStart?.totalValue || portfolioData.current.totalValue;
  const ytdEnd = portfolioData.current.totalValue;
  const ytdReturnByValue = ((ytdEnd - ytdStart) / ytdStart) * 100;
  
  console.log('📊 COMPARACIÓN DE SUMAS DE CONTRIBUCIONES:\n');
  console.log(`   Método Actual (CurrentPrice.ytdReturn):  ${formatPP(sumCurrentMethod)}`);
  console.log(`   Método Propuesto (assetPerformance.ROI): ${formatPP(sumProposedMethod)}`);
  console.log('');
  console.log('📊 VALORES DE REFERENCIA:\n');
  console.log(`   ROI Total (Firestore):                   ${formatPercent(expectedROI)}`);
  console.log(`   YTD Return (cambio de valor):            ${formatPercent(ytdReturnByValue)}`);
  console.log('');
  
  // Análisis de discrepancias
  const diffCurrent = Math.abs(sumCurrentMethod - expectedROI);
  const diffProposed = Math.abs(sumProposedMethod - expectedROI);
  
  console.log('📊 ANÁLISIS DE DISCREPANCIAS:\n');
  console.log(`   Método Actual vs ROI Firestore:    ${formatPP(diffCurrent)} ${diffCurrent < 3 ? '✅' : '❌'}`);
  console.log(`   Método Propuesto vs ROI Firestore: ${formatPP(diffProposed)} ${diffProposed < 3 ? '✅' : '❌'}`);
  console.log('');
  
  // Recomendación
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  RECOMENDACIÓN');
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  if (diffProposed < diffCurrent) {
    console.log('✅ El MÉTODO PROPUESTO (usar assetPerformance.totalROI) es más preciso.');
    console.log('');
    console.log('   Razón: El ROI de assetPerformance considera la fecha de compra');
    console.log('   y precio de adquisición personal del usuario, mientras que');
    console.log('   CurrentPrice.ytdReturn es el retorno del activo en el mercado');
    console.log('   desde el 1 de enero, independiente de cuándo el usuario compró.');
    console.log('');
    console.log('   ACCIÓN SUGERIDA: Modificar usePortfolioAttribution para usar');
    console.log('   datos de assetPerformance de Firestore en lugar de CurrentPrice.');
  } else if (diffCurrent < diffProposed) {
    console.log('⚠️ El MÉTODO ACTUAL (CurrentPrice.ytdReturn) es más preciso.');
    console.log('   Esto puede indicar que assetPerformance tiene datos inconsistentes.');
  } else {
    console.log('ℹ️ Ambos métodos tienen precisión similar.');
  }
  
  console.log('');
  console.log('   NOTA: La discrepancia con YTD por valor (' + formatPercent(ytdReturnByValue) + ')');
  console.log('   se debe a que el usuario ha realizado aportes durante el año.');
  console.log('   El ROI de Firestore ajusta por cashflows (TWR/MWR).');
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  VERIFICACIÓN DE CÁLCULOS DE ATRIBUCIÓN DEL PORTAFOLIO');
  console.log('  User ID: ' + USER_ID);
  console.log('  Moneda: ' + CURRENCY);
  console.log('  Período: ' + PERIOD);
  console.log('═══════════════════════════════════════════════════════════════');
  
  try {
    // 1. Obtener datos del portafolio
    const portfolioData = await getPortfolioPerformanceData();
    if (!portfolioData) {
      console.log('❌ No se pudieron obtener datos del portafolio');
      process.exit(1);
    }
    
    // 2. Obtener assets y precios
    const { assets, pricesMap } = await getAssetsAndPrices();
    
    // 3. Obtener assetPerformance de Firestore
    const assetPerformance = await getAssetPerformanceFromFirestore();
    
    // 4. Calcular atribución con ambos métodos
    const { attributionsCurrentMethod, attributionsProposed, totalPortfolioValue, totalPortfolioInvestment } = 
      await calculateAttribution(assets, pricesMap, assetPerformance, portfolioData);
    
    // 5. Mostrar resultados comparativos
    displayAttributionResults(attributionsCurrentMethod, attributionsProposed, totalPortfolioValue, portfolioData);
    
    // 6. Verificar coherencia
    verifyContributionSum(attributionsCurrentMethod, attributionsProposed, portfolioData);
    
    console.log('\n✅ Diagnóstico completado\n');
    
  } catch (error) {
    console.error('❌ Error:', error);
    process.exit(1);
  }
  
  process.exit(0);
}

main();
