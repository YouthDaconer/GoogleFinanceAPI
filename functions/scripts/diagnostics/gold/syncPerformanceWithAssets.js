/**
 * Script para sincronizar portfolioPerformance con los assets actuales
 * 
 * Este script recalcula el totalValue usando los precios históricos
 * y assets actuales en lugar de las transacciones históricas.
 * 
 * Uso: 
 *   node syncPerformanceWithAssets.js                    # Dry-run para hoy
 *   node syncPerformanceWithAssets.js --fix              # Aplicar para hoy
 *   node syncPerformanceWithAssets.js --date=2026-01-08  # Dry-run para fecha específica
 *   node syncPerformanceWithAssets.js --date=2026-01-08 --fix  # Aplicar para fecha específica
 */

const admin = require('firebase-admin');
const path = require('path');
const fetch = require('node-fetch');

const keyPath = path.join(__dirname, '../../../key.json');
const serviceAccount = require(keyPath);

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const FIX_MODE = process.argv.includes('--fix');
// Permitir especificar una fecha con --date=YYYY-MM-DD
const dateArg = process.argv.find(arg => arg.startsWith('--date='));
const targetDate = dateArg ? dateArg.split('=')[1] : new Date().toISOString().split('T')[0];
const isToday = targetDate === new Date().toISOString().split('T')[0];

// API de precios históricos
const HISTORICAL_API_BASE = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

/**
 * Obtener precios históricos de un símbolo
 */
async function fetchHistoricalPrices(symbol) {
  try {
    const url = `${HISTORICAL_API_BASE}/historical?symbol=${encodeURIComponent(symbol)}&range=1mo&interval=1d`;
    const response = await fetch(url);
    
    if (!response.ok) return {};
    
    const data = await response.json();
    const priceMap = {};
    Object.entries(data).forEach(([date, ohlcv]) => {
      priceMap[date] = ohlcv.close;
    });
    
    return priceMap;
  } catch (error) {
    return {};
  }
}

/**
 * Obtener tipo de cambio histórico de Yahoo Finance
 */
async function fetchHistoricalExchangeRate(currency, date) {
  if (currency === 'USD') return 1;
  
  try {
    let symbol;
    if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
      symbol = `${currency}USD`;
    } else {
      symbol = `USD${currency}`;
    }
    
    const dateObj = new Date(date + 'T12:00:00Z');
    const timestamp = Math.floor(dateObj.getTime() / 1000);
    const nextDay = timestamp + 86400;
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}%3DX?period1=${timestamp}&period2=${nextDay}&interval=1d`;
    
    const response = await fetch(url);
    const data = await response.json();
    
    if (data.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.[0]) {
      let rate = data.chart.result[0].indicators.quote[0].close[0];
      
      if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
        rate = 1 / rate;
      }
      
      return rate;
    }
    
    return null;
  } catch (error) {
    return null;
  }
}

async function syncPerformanceWithAssets() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  const today = targetDate;

  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║     SINCRONIZACIÓN DE PERFORMANCE CON ASSETS ACTUALES          ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');
  console.log(`Fecha: ${today}`);
  console.log(`Modo: ${FIX_MODE ? '🔧 FIX (modificará datos)' : '👀 DRY-RUN (solo lectura)'}`);
  console.log(`Precios: ${isToday ? 'Actuales (currentPrices)' : 'Históricos (API)'}\n`);

  // Obtener cuentas del usuario
  const accountsSnap = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  const accounts = accountsSnap.docs.map(d => ({ id: d.id, name: d.data().name }));
  console.log(`Cuentas: ${accounts.map(a => a.name).join(', ')}\n`);

  // Obtener assets activos para identificar símbolos únicos
  const allAssets = [];
  const symbolsSet = new Set();
  
  for (const account of accounts) {
    const assetsSnap = await db.collection('assets')
      .where('isActive', '==', true)
      .where('portfolioAccount', '==', account.id)
      .get();
    
    assetsSnap.docs.forEach(doc => {
      const asset = { id: doc.id, ...doc.data(), accountId: account.id };
      allAssets.push(asset);
      symbolsSet.add(asset.name);
    });
  }

  // Obtener precios (actuales o históricos según la fecha)
  let priceMap = {};
  
  if (isToday) {
    // Usar precios actuales
    const pricesSnap = await db.collection('currentPrices').get();
    pricesSnap.docs.forEach(d => {
      const data = d.data();
      priceMap[data.symbol] = { 
        price: data.price, 
        currency: data.currency || 'USD' 
      };
    });
  } else {
    // Obtener precios históricos para cada símbolo
    console.log(`📡 Obteniendo precios históricos para ${symbolsSet.size} símbolos...`);
    
    // También necesitamos la moneda de cada símbolo desde currentPrices
    const pricesSnap = await db.collection('currentPrices').get();
    const currencyBySymbol = {};
    pricesSnap.docs.forEach(d => {
      const data = d.data();
      currencyBySymbol[data.symbol] = data.currency || 'USD';
    });
    
    for (const symbol of symbolsSet) {
      const historicalPrices = await fetchHistoricalPrices(symbol);
      const priceForDate = historicalPrices[today];
      
      if (priceForDate) {
        priceMap[symbol] = {
          price: priceForDate,
          currency: currencyBySymbol[symbol] || 'USD'
        };
      } else {
        // Fallback: buscar el día anterior más cercano
        const sortedDates = Object.keys(historicalPrices).sort().reverse();
        for (const d of sortedDates) {
          if (d < today) {
            priceMap[symbol] = {
              price: historicalPrices[d],
              currency: currencyBySymbol[symbol] || 'USD'
            };
            break;
          }
        }
      }
    }
    console.log(`✅ Precios históricos obtenidos\n`);
  }

  // Obtener tasas de cambio (actuales o históricos)
  let rates = {};
  
  if (isToday) {
    const currenciesSnap = await db.collection('currencies').where('isActive', '==', true).get();
    currenciesSnap.docs.forEach(d => {
      rates[d.data().code] = d.data().exchangeRate;
    });
  } else {
    console.log(`📡 Obteniendo tipos de cambio históricos...`);
    rates = { USD: 1 };
    
    for (const currency of ['COP', 'EUR', 'GBP', 'MXN', 'BRL', 'CAD']) {
      const rate = await fetchHistoricalExchangeRate(currency, today);
      if (rate) rates[currency] = rate;
    }
    console.log(`✅ Tipos de cambio: COP=${rates.COP?.toFixed(2)}, EUR=${rates.EUR?.toFixed(4)}\n`);
  }

  const updates = [];
  let overallTotalValue = 0;
  let overallTotalInvestment = 0;

  for (const account of accounts) {
    console.log(`\n═══ Cuenta: ${account.name} (${account.id}) ═══`);

    // Filtrar assets de esta cuenta (ya los cargamos antes)
    const accountAssets = allAssets.filter(a => a.accountId === account.id);

    // Calcular valores desde assets
    let totalValue = 0;
    let totalInvestment = 0;
    const assetPerformance = {};

    for (const asset of accountAssets) {
      const priceData = priceMap[asset.name];
      
      if (!priceData) continue;

      // Calcular valor en USD
      const priceCurrency = priceData.currency || 'USD';
      let priceInUSD = priceData.price;
      if (priceCurrency !== 'USD' && rates[priceCurrency]) {
        priceInUSD = priceData.price / rates[priceCurrency];
      }
      const valueUSD = asset.units * priceInUSD;

      // Calcular inversión en USD
      let investmentUSD;
      if (asset.currency === 'USD') {
        investmentUSD = asset.unitValue * asset.units;
      } else {
        const rate = asset.acquisitionDollarValue || rates[asset.currency] || 1;
        investmentUSD = (asset.unitValue * asset.units) / rate;
      }

      totalValue += valueUSD;
      totalInvestment += investmentUSD;

      // Agrupar por nombre + tipo
      const key = `${asset.name}_${asset.assetType || 'stock'}`;
      if (!assetPerformance[key]) {
        assetPerformance[key] = {
          totalValue: 0,
          totalInvestment: 0,
          units: 0
        };
      }
      assetPerformance[key].totalValue += valueUSD;
      assetPerformance[key].totalInvestment += investmentUSD;
      assetPerformance[key].units += asset.units;
    }

    // Obtener documento actual de performance
    const perfDoc = await db.doc(`portfolioPerformance/${userId}/accounts/${account.id}/dates/${today}`).get();
    const currentValue = perfDoc.data()?.USD?.totalValue || 0;

    console.log(`  Desde assets (USD): $${totalValue.toFixed(2)}`);
    console.log(`  En portfolioPerformance: $${currentValue.toFixed(2)}`);
    console.log(`  Diferencia: $${(currentValue - totalValue).toFixed(2)}`);

    if (Math.abs(currentValue - totalValue) > 1) {
      console.log(`  ⚠️ Diferencia significativa detectada`);
      
      if (FIX_MODE) {
        // Preparar actualización para TODAS las monedas
        const perfData = perfDoc.data() || {};
        const updateData = {};
        
        // Lista de monedas a actualizar
        const currenciesToUpdate = ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'];
        
        for (const currency of currenciesToUpdate) {
          const currencyRate = rates[currency] || 1;
          const currencyData = perfData[currency] || {};
          
          // Calcular valores en esta moneda
          const totalValueInCurrency = totalValue * currencyRate;
          const totalInvestmentInCurrency = totalInvestment * currencyRate;
          
          // Actualizar assetPerformance en esta moneda
          const assetPerfInCurrency = {};
          for (const [key, perf] of Object.entries(assetPerformance)) {
            assetPerfInCurrency[key] = {
              ...(currencyData.assetPerformance?.[key] || {}),
              totalValue: perf.totalValue * currencyRate,
              totalInvestment: perf.totalInvestment * currencyRate,
              units: perf.units
            };
          }
          
          updateData[currency] = {
            ...currencyData,
            totalValue: totalValueInCurrency,
            totalInvestment: totalInvestmentInCurrency,
            assetPerformance: assetPerfInCurrency
          };
        }

        updates.push({
          path: `portfolioPerformance/${userId}/accounts/${account.id}/dates/${today}`,
          data: updateData
        });
      }
    } else {
      console.log(`  ✅ Valores consistentes`);
    }

    overallTotalValue += totalValue;
    overallTotalInvestment += totalInvestment;
  }

  // Actualizar documento OVERALL
  console.log(`\n═══ OVERALL ═══`);
  const overallDoc = await db.doc(`portfolioPerformance/${userId}/dates/${today}`).get();
  const currentOverallValue = overallDoc.data()?.USD?.totalValue || 0;

  console.log(`  Desde assets: $${overallTotalValue.toFixed(2)}`);
  console.log(`  En portfolioPerformance: $${currentOverallValue.toFixed(2)}`);
  console.log(`  Diferencia: $${(currentOverallValue - overallTotalValue).toFixed(2)}`);

  if (Math.abs(currentOverallValue - overallTotalValue) > 1) {
    console.log(`  ⚠️ Diferencia significativa detectada`);
    
    if (FIX_MODE) {
      const overallData = overallDoc.data() || {};
      const updateData = {};
      
      // Lista de monedas a actualizar
      const currenciesToUpdate = ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'];
      
      for (const currency of currenciesToUpdate) {
        const currencyRate = rates[currency] || 1;
        const currencyData = overallData[currency] || {};
        
        updateData[currency] = {
          ...currencyData,
          totalValue: overallTotalValue * currencyRate,
          totalInvestment: overallTotalInvestment * currencyRate
        };
      }
      
      updates.push({
        path: `portfolioPerformance/${userId}/dates/${today}`,
        data: updateData
      });
    }
  } else {
    console.log(`  ✅ Valores consistentes`);
  }

  // Aplicar actualizaciones
  if (FIX_MODE && updates.length > 0) {
    console.log(`\n═══ APLICANDO ${updates.length} ACTUALIZACIONES ═══`);
    
    for (const update of updates) {
      await db.doc(update.path).set(update.data, { merge: true });
      console.log(`  ✅ Actualizado: ${update.path}`);
    }
    
    console.log(`\n✅ SINCRONIZACIÓN COMPLETADA`);
  } else if (!FIX_MODE) {
    console.log(`\n📋 Para aplicar correcciones, ejecuta: node syncPerformanceWithAssets.js --fix`);
  } else {
    console.log(`\n✅ No se requieren correcciones`);
  }

  process.exit(0);
}

syncPerformanceWithAssets().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
