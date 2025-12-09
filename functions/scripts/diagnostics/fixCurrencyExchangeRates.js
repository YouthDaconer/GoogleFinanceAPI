/**
 * Script para corregir documentos con tipos de cambio incorrectos
 * 
 * Este script corrige documentos donde todas las monedas tienen el mismo valor
 * (porque los tipos de cambio no se aplicaron correctamente)
 * 
 * USO:
 *   node fixCurrencyExchangeRates.js --dry-run    # Ver cambios sin aplicar
 *   node fixCurrencyExchangeRates.js --fix        # Aplicar cambios a Firestore
 */

const admin = require('firebase-admin');
const fetch = require('node-fetch');

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
  BATCH_SIZE: 10,
};

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    mode: args.includes('--fix') ? 'fix' : 'dry-run'
  };
}

function log(level, message) {
  const colors = {
    INFO: '\x1b[36m',
    SUCCESS: '\x1b[32m',
    WARN: '\x1b[33m',
    ERROR: '\x1b[31m',
    PROGRESS: '\x1b[35m',
  };
  const reset = '\x1b[0m';
  const color = colors[level] || '';
  console.log(`${color}[${level}]${reset} ${message}`);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Obtener tipo de cambio histórico (con fallback a días anteriores para fines de semana)
 */
async function fetchExchangeRate(currency, dateStr, maxRetries = 5) {
  if (currency === 'USD') return 1;
  
  let currentDate = new Date(dateStr + 'T12:00:00Z');
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      let symbol;
      if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
        symbol = currency + 'USD';
      } else {
        symbol = 'USD' + currency;
      }
      
      const timestamp = Math.floor(currentDate.getTime() / 1000);
      const nextDay = timestamp + 86400;
      
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}%3DX?period1=${timestamp}&period2=${nextDay}&interval=1d`;
      
      const response = await fetch(url);
      const data = await response.json();
      
      if (data.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.[0]) {
        const rate = data.chart.result[0].indicators.quote[0].close[0];
        
        // Para EUR/GBP etc., necesitamos invertir (son XXX/USD, necesitamos USD/XXX)
        if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
          return 1 / rate;
        }
        return rate;
      }
      
      // Si no hay datos, intentar con el día anterior
      currentDate.setDate(currentDate.getDate() - 1);
      await sleep(50);
      
    } catch (error) {
      // Si hay error, intentar con el día anterior
      currentDate.setDate(currentDate.getDate() - 1);
      await sleep(50);
    }
  }
  
  log('WARN', `No se pudo obtener tipo de cambio para ${currency} cerca de ${dateStr} después de ${maxRetries} intentos`);
  return null;
}

async function fixCurrencyRates() {
  const options = parseArgs();
  
  console.log('═'.repeat(80));
  console.log('  CORRECCIÓN DE TIPOS DE CAMBIO EN DOCUMENTOS');
  console.log('═'.repeat(80));
  console.log('');
  log('INFO', `Modo: ${options.mode.toUpperCase()}`);
  console.log('');
  
  // 1. Identificar documentos corruptos (donde COP/USD ratio < 1000)
  const allDocs = await db.collection(`portfolioPerformance/${CONFIG.USER_ID}/dates`)
    .orderBy('date', 'asc')
    .get();
  
  const corruptDocs = [];
  
  allDocs.docs.forEach(doc => {
    const data = doc.data();
    const usdValue = data.USD?.totalValue || 0;
    const copValue = data.COP?.totalValue || 0;
    
    if (usdValue > 0 && copValue > 0) {
      const ratio = copValue / usdValue;
      if (ratio < 1000) { // Debería ser ~4000+
        corruptDocs.push({ ref: doc.ref, data, date: data.date });
      }
    }
  });
  
  log('INFO', `Documentos corruptos encontrados: ${corruptDocs.length}`);
  
  if (corruptDocs.length === 0) {
    log('SUCCESS', 'No hay documentos para corregir');
    process.exit(0);
  }
  
  // 2. Obtener tipos de cambio para cada fecha
  log('PROGRESS', 'Obteniendo tipos de cambio históricos...');
  
  const exchangeRatesByDate = new Map();
  
  for (const { date } of corruptDocs) {
    if (!exchangeRatesByDate.has(date)) {
      const rates = { USD: 1 };
      
      for (const currency of CONFIG.CURRENCIES.filter(c => c !== 'USD')) {
        const rate = await fetchExchangeRate(currency, date);
        if (rate) {
          rates[currency] = rate;
        } else {
          log('WARN', `No se pudo obtener tipo de cambio para ${currency} en ${date}`);
        }
        await sleep(100); // Rate limiting
      }
      
      exchangeRatesByDate.set(date, rates);
      log('INFO', `  ${date}: COP=${rates.COP?.toFixed(2) || 'N/A'}, EUR=${rates.EUR?.toFixed(4) || 'N/A'}`);
    }
  }
  
  // 3. Corregir cada documento
  log('PROGRESS', 'Corrigiendo documentos...');
  
  const documentsToFix = [];
  
  for (const { ref, data, date } of corruptDocs) {
    const rates = exchangeRatesByDate.get(date);
    if (!rates) continue;
    
    const usdData = data.USD;
    if (!usdData) continue;
    
    const fixes = {};
    
    // Recalcular cada moneda basándose en USD
    CONFIG.CURRENCIES.filter(c => c !== 'USD').forEach(currency => {
      const rate = rates[currency];
      if (!rate) return;
      
      const currencyData = data[currency] || {};
      
      // Recalcular valores a nivel de portfolio
      fixes[currency] = {
        ...currencyData,
        totalValue: (usdData.totalValue || 0) * rate,
        totalInvestment: (usdData.totalInvestment || 0) * rate,
        totalCashFlow: (usdData.totalCashFlow || 0) * rate,
        unrealizedProfitAndLoss: (usdData.unrealizedProfitAndLoss || 0) * rate,
        doneProfitAndLoss: (usdData.doneProfitAndLoss || 0) * rate,
        // Mantener porcentajes igual que USD
        totalROI: usdData.totalROI || 0,
        dailyChangePercentage: usdData.dailyChangePercentage || 0,
        rawDailyChangePercentage: usdData.rawDailyChangePercentage || 0,
        adjustedDailyChangePercentage: usdData.adjustedDailyChangePercentage || 0,
        dailyReturn: usdData.dailyReturn || 0,
        monthlyReturn: usdData.monthlyReturn || 0,
        annualReturn: usdData.annualReturn || 0,
      };
      
      // Recalcular assetPerformance
      if (usdData.assetPerformance) {
        fixes[currency].assetPerformance = {};
        
        Object.entries(usdData.assetPerformance).forEach(([assetKey, usdAsset]) => {
          fixes[currency].assetPerformance[assetKey] = {
            ...usdAsset,
            totalValue: (usdAsset.totalValue || 0) * rate,
            totalInvestment: (usdAsset.totalInvestment || 0) * rate,
            totalCashFlow: (usdAsset.totalCashFlow || 0) * rate,
            unrealizedProfitAndLoss: (usdAsset.unrealizedProfitAndLoss || 0) * rate,
            doneProfitAndLoss: (usdAsset.doneProfitAndLoss || 0) * rate,
            // Mantener porcentajes igual que USD
            totalROI: usdAsset.totalROI || 0,
            dailyChangePercentage: usdAsset.dailyChangePercentage || 0,
            rawDailyChangePercentage: usdAsset.rawDailyChangePercentage || 0,
            adjustedDailyChangePercentage: usdAsset.adjustedDailyChangePercentage || 0,
            dailyReturn: usdAsset.dailyReturn || 0,
            monthlyReturn: usdAsset.monthlyReturn || 0,
            annualReturn: usdAsset.annualReturn || 0,
          };
        });
      }
    });
    
    if (Object.keys(fixes).length > 0) {
      documentsToFix.push({ ref, date, fixes });
    }
  }
  
  log('INFO', `Documentos a corregir: ${documentsToFix.length}`);
  
  // 4. Mostrar muestra
  if (documentsToFix.length > 0) {
    log('PROGRESS', 'Muestra de correcciones:');
    const sample = documentsToFix[0];
    console.log(`  ${sample.date}:`);
    console.log(`    COP totalValue: ${sample.fixes.COP?.totalValue?.toFixed(2)}`);
    console.log(`    EUR totalValue: ${sample.fixes.EUR?.totalValue?.toFixed(2)}`);
  }
  
  // 5. Aplicar correcciones
  if (options.mode === 'fix') {
    log('PROGRESS', 'Aplicando correcciones...');
    
    for (let i = 0; i < documentsToFix.length; i += CONFIG.BATCH_SIZE) {
      const batch = db.batch();
      const chunk = documentsToFix.slice(i, i + CONFIG.BATCH_SIZE);
      
      chunk.forEach(({ ref, fixes }) => {
        batch.update(ref, fixes);
      });
      
      await batch.commit();
      log('SUCCESS', `Batch ${Math.floor(i / CONFIG.BATCH_SIZE) + 1} completado (${chunk.length} docs)`);
    }
    
    log('SUCCESS', `✅ Corrección completada: ${documentsToFix.length} documentos`);
  } else {
    log('WARN', 'Modo dry-run: No se aplicaron cambios');
    log('INFO', 'Ejecuta con --fix para aplicar las correcciones');
  }
  
  process.exit(0);
}

fixCurrencyRates().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
