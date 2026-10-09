/**
 * Script específico para corregir la corrupción de ECOPETROL.CL
 * 
 * PROBLEMA:
 * El 2026-01-08 se compraron 20 unidades de ECOPETROL.CL a 2005 COP cada uno.
 * Un bug guardó el cashflow como 40100 USD en vez de 40100 COP.
 * Esto corrompió el totalInvestment a ~150 millones USD.
 * 
 * SOLUCIÓN:
 * Recalcular los valores correctos para ECOPETROL.CL_stock en los días afectados.
 * 
 * USO:
 *   node fixEcopetrolCorruption.js --analyze     # Solo muestra los datos
 *   node fixEcopetrolCorruption.js --dry-run    # Muestra cambios sin aplicar
 *   node fixEcopetrolCorruption.js --fix        # Aplica las correcciones
 */

const admin = require('firebase-admin');
const fetch = require('node-fetch');

// Inicializar Firebase Admin
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

const CONFIG = {
  // Lambda API para precios históricos
  LAMBDA_API_BASE: 'https://354sdh5hcrnztw5vquw6sxiduu0gigku.lambda-url.us-east-1.on.aws/v1',
  
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  ASSET_KEY: 'ECOPETROL.CL_stock',
  ASSET_SYMBOL: 'ECOPETROL.CL',
  ASSET_CURRENCY: 'COP', // Cotiza en pesos colombianos (BVC)
  
  // Datos de la transacción original (correctos)
  PURCHASE_DATE: '2026-01-08',
  UNITS: 20,
  UNIT_PRICE_COP: 2005,
  TOTAL_COP: 40100, // 20 * 2005
  
  // Días afectados (donde el totalInvestment está corrupto)
  AFFECTED_DATES: ['2026-01-12', '2026-01-13', '2026-01-14', '2026-01-15', '2026-01-16'],
  
  // Cuenta donde está el activo
  ACCOUNT_ID: 'ggM52GimbLL7jwvegc9o', // Trii
  
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
};

// ============================================================================
// UTILIDADES
// ============================================================================

function log(level, message, data = null) {
  const timestamp = new Date().toISOString();
  const prefix = { INFO: '📘', WARN: '⚠️', ERROR: '❌', SUCCESS: '✅', DEBUG: '🔍' }[level] || '📝';
  console.log(`${prefix} [${timestamp}] ${message}`);
  if (data) console.log(JSON.stringify(data, null, 2));
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================================
// OBTENCIÓN DE DATOS
// ============================================================================

/**
 * Obtener precios históricos de un símbolo usando Lambda API
 * Basado en fetchHistoricalPrices de backfillPortfolioPerformance.js
 * @param {string} symbol - Ticker del activo
 * @param {string} startDate - Fecha de inicio para determinar el rango
 * @returns {Object} Map de fecha -> precio de cierre
 */
async function fetchHistoricalPrices(symbol, startDate = null) {
  try {
    // Determinar el rango basado en la fecha de inicio
    let range = 'ytd';
    if (startDate) {
      const start = new Date(startDate);
      const now = new Date();
      const monthsAgo = (now.getFullYear() - start.getFullYear()) * 12 + (now.getMonth() - start.getMonth());
      
      if (monthsAgo > 12) {
        range = '2y';
      } else if (monthsAgo > 6) {
        range = '1y';
      }
    }
    
    const url = `${CONFIG.LAMBDA_API_BASE}/historical?symbol=${encodeURIComponent(symbol)}&range=${range}&interval=1d`;
    log('DEBUG', `Fetching historical prices from: ${url}`);
    
    const response = await fetch(url);
    
    if (!response.ok) {
      log('WARNING', `No se pudieron obtener precios para ${symbol}: ${response.status}`);
      return {};
    }
    
    const data = await response.json();
    
    // Convertir a map fecha -> close price
    const priceMap = {};
    Object.entries(data).forEach(([date, ohlcv]) => {
      priceMap[date] = ohlcv.close;
    });
    
    log('INFO', `Obtenidos ${Object.keys(priceMap).length} precios históricos para ${symbol}`);
    return priceMap;
  } catch (error) {
    log('ERROR', `Error obteniendo precios históricos para ${symbol}`, { error: error.message });
    return {};
  }
}

/**
 * Obtener precio para una fecha específica del mapa de precios
 * Con fallback al día anterior más cercano
 */
function getPriceForDate(priceMap, targetDate) {
  // Primero intentar la fecha exacta
  if (priceMap[targetDate]) {
    return priceMap[targetDate];
  }
  
  // Si no hay precio exacto, buscar el día anterior más cercano
  const sortedDates = Object.keys(priceMap).sort().reverse();
  for (const date of sortedDates) {
    if (date < targetDate) {
      log('DEBUG', `No price for ${targetDate}, using ${date} price`);
      return priceMap[date];
    }
  }
  
  return null;
}

/**
 * Obtener tipo de cambio USD/COP para una fecha específica
 * Usa Yahoo Finance directamente
 */
async function fetchUSDCOPRate(date) {
  try {
    const targetDate = new Date(date);
    const timestamp = Math.floor(targetDate.getTime() / 1000);
    const nextDay = timestamp + 86400;
    
    // Endpoint directo de Yahoo Finance para COP=X (USD/COP)
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/COP%3DX?period1=${timestamp}&period2=${nextDay}&interval=1d`;
    
    log('DEBUG', `Fetching USD/COP rate from: ${url}`);
    
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    
    const data = await response.json();
    
    // Extraer el precio de cierre
    const closePrice = data.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.[0];
    
    if (closePrice) {
      log('DEBUG', `USD/COP rate for ${date}: ${closePrice}`);
      return closePrice;
    }
    
    // Si no hay datos para ese día específico, buscar en un rango más amplio
    log('WARN', `No rate found for exact date ${date}, trying wider range...`);
    
    const weekAgo = timestamp - (7 * 86400);
    const weekUrl = `https://query1.finance.yahoo.com/v8/finance/chart/COP%3DX?period1=${weekAgo}&period2=${nextDay}&interval=1d`;
    
    const weekResponse = await fetch(weekUrl);
    const weekData = await weekResponse.json();
    
    const prices = weekData.chart?.result?.[0]?.indicators?.quote?.[0]?.close || [];
    const timestamps = weekData.chart?.result?.[0]?.timestamp || [];
    
    // Buscar el precio más cercano a la fecha objetivo
    let closestRate = null;
    let minDiff = Infinity;
    
    for (let i = 0; i < timestamps.length; i++) {
      if (prices[i]) {
        const diff = Math.abs(timestamps[i] - timestamp);
        if (diff < minDiff) {
          minDiff = diff;
          closestRate = prices[i];
        }
      }
    }
    
    return closestRate;
  } catch (error) {
    log('ERROR', `Error fetching USD/COP rate for ${date}: ${error.message}`);
    return null;
  }
}

/**
 * Obtener documento de performance de una fecha
 */
async function getPerformanceDoc(date, accountId = null) {
  const path = accountId 
    ? `portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates/${date}`
    : `portfolioPerformance/${CONFIG.USER_ID}/dates/${date}`;
  
  const doc = await db.doc(path).get();
  return doc.exists ? { id: doc.id, path, data: doc.data() } : null;
}

/**
 * Derivar tipos de cambio desde un documento existente
 */
function deriveExchangeRates(docData) {
  const rates = { USD: 1 };
  const usdValue = docData.USD?.totalValue;
  
  if (!usdValue || usdValue === 0) return rates;
  
  for (const currency of CONFIG.CURRENCIES) {
    if (currency === 'USD') continue;
    const currencyValue = docData[currency]?.totalValue;
    if (currencyValue && currencyValue > 0) {
      rates[currency] = currencyValue / usdValue;
    }
  }
  
  return rates;
}

// ============================================================================
// ANÁLISIS Y CORRECCIÓN
// ============================================================================

async function analyzeCorruption() {
  log('INFO', '=== ANÁLISIS DE CORRUPCIÓN ECOPETROL.CL ===');
  
  // Mostrar datos de la transacción original
  log('INFO', 'Transacción original:', {
    fecha: CONFIG.PURCHASE_DATE,
    unidades: CONFIG.UNITS,
    precioUnitario: `${CONFIG.UNIT_PRICE_COP} COP`,
    total: `${CONFIG.TOTAL_COP} COP`
  });
  
  // Obtener tasa de cambio del día de compra
  const purchaseRate = await fetchUSDCOPRate(CONFIG.PURCHASE_DATE);
  const correctInvestmentUSD = purchaseRate ? CONFIG.TOTAL_COP / purchaseRate : null;
  
  log('INFO', 'Valores correctos:', {
    tasaCambioCompra: purchaseRate,
    inversionCorrectaUSD: correctInvestmentUSD?.toFixed(2)
  });
  
  // Analizar cada día afectado
  log('INFO', '\n=== DOCUMENTOS AFECTADOS ===');
  
  for (const date of CONFIG.AFFECTED_DATES) {
    log('INFO', `\n--- ${date} ---`);
    
    // Overall
    const overallDoc = await getPerformanceDoc(date);
    if (overallDoc) {
      const assetData = overallDoc.data.USD?.assetPerformance?.[CONFIG.ASSET_KEY];
      if (assetData) {
        log('INFO', 'OVERALL USD:', {
          totalInvestment: assetData.totalInvestment,
          totalValue: assetData.totalValue,
          totalROI: assetData.totalROI,
          corrupto: assetData.totalInvestment > 1000000
        });
      }
    }
    
    // Cuenta específica
    const accountDoc = await getPerformanceDoc(date, CONFIG.ACCOUNT_ID);
    if (accountDoc) {
      const assetData = accountDoc.data.USD?.assetPerformance?.[CONFIG.ASSET_KEY];
      if (assetData) {
        log('INFO', `ACCOUNT ${CONFIG.ACCOUNT_ID} USD:`, {
          totalInvestment: assetData.totalInvestment,
          totalValue: assetData.totalValue,
          totalROI: assetData.totalROI,
          corrupto: assetData.totalInvestment > 1000000
        });
      }
    }
  }
}

async function fixCorruption(dryRun = true) {
  log('INFO', `=== ${dryRun ? 'DRY RUN' : 'APLICANDO'} CORRECCIÓN ECOPETROL.CL ===`);
  
  // Obtener tasa de cambio del día de compra para calcular inversión correcta
  const purchaseRate = await fetchUSDCOPRate(CONFIG.PURCHASE_DATE);
  if (!purchaseRate) {
    log('ERROR', 'No se pudo obtener la tasa de cambio del día de compra');
    return;
  }
  
  const correctInvestmentUSD = CONFIG.TOTAL_COP / purchaseRate;
  log('INFO', `Inversión correcta: ${CONFIG.TOTAL_COP} COP / ${purchaseRate} = $${correctInvestmentUSD.toFixed(2)} USD`);
  
  // Obtener precios históricos de ECOPETROL usando Lambda API
  log('INFO', 'Obteniendo precios históricos de ECOPETROL.CL...');
  const historicalPrices = await fetchHistoricalPrices(CONFIG.ASSET_SYMBOL, CONFIG.PURCHASE_DATE);
  
  if (Object.keys(historicalPrices).length === 0) {
    log('ERROR', 'No se pudieron obtener precios históricos de ECOPETROL.CL');
    return;
  }
  
  // Mapear precios para cada fecha afectada
  const priceData = {};
  for (const date of CONFIG.AFFECTED_DATES) {
    priceData[date] = getPriceForDate(historicalPrices, date);
  }
  
  // Obtener tasas de cambio para cada día
  const ratesData = {};
  for (const date of CONFIG.AFFECTED_DATES) {
    ratesData[date] = await fetchUSDCOPRate(date);
    await sleep(100);
  }
  
  log('INFO', 'Datos obtenidos:', { precios: priceData, tasas: ratesData });
  
  const batch = db.batch();
  let updateCount = 0;
  
  for (const date of CONFIG.AFFECTED_DATES) {
    const priceCOP = priceData[date];
    const usdcopRate = ratesData[date];
    
    if (!priceCOP || !usdcopRate) {
      log('WARN', `Datos faltantes para ${date}, saltando...`);
      continue;
    }
    
    // Calcular valor actual en USD
    const totalValueUSD = (CONFIG.UNITS * priceCOP) / usdcopRate;
    const totalROI = ((totalValueUSD - correctInvestmentUSD) / correctInvestmentUSD) * 100;
    const unrealizedPnL = totalValueUSD - correctInvestmentUSD;
    
    log('INFO', `\n${date}:`, {
      precioCOP: priceCOP,
      tasaUSDCOP: usdcopRate,
      totalValueUSD: totalValueUSD.toFixed(2),
      totalInvestmentUSD: correctInvestmentUSD.toFixed(2),
      totalROI: totalROI.toFixed(2) + '%',
      unrealizedPnL: unrealizedPnL.toFixed(2)
    });
    
    // Preparar datos corregidos para el asset
    const correctedAssetData = {
      units: CONFIG.UNITS,
      totalValue: totalValueUSD,
      totalInvestment: correctInvestmentUSD,
      totalROI: totalROI,
      unrealizedProfitAndLoss: unrealizedPnL,
      doneProfitAndLoss: 0,
      dailyChangePercent: 0, // Se podría calcular pero no es crítico
      dailyChangeValue: 0
    };
    
    // Actualizar OVERALL
    const overallDoc = await getPerformanceDoc(date);
    if (overallDoc) {
      const rates = deriveExchangeRates(overallDoc.data);
      const updates = {};
      
      for (const currency of CONFIG.CURRENCIES) {
        const rate = rates[currency] || 1;
        const currencyPath = `${currency}.assetPerformance.${CONFIG.ASSET_KEY}`;
        
        updates[`${currencyPath}.units`] = CONFIG.UNITS;
        updates[`${currencyPath}.totalValue`] = totalValueUSD * rate;
        updates[`${currencyPath}.totalInvestment`] = correctInvestmentUSD * rate;
        updates[`${currencyPath}.totalROI`] = totalROI;
        updates[`${currencyPath}.unrealizedProfitAndLoss`] = unrealizedPnL * rate;
      }
      
      // También corregir los totales del documento
      const currentTotalInvestment = overallDoc.data.USD?.totalInvestment || 0;
      const currentAssetInvestment = overallDoc.data.USD?.assetPerformance?.[CONFIG.ASSET_KEY]?.totalInvestment || 0;
      const correctedTotalInvestment = currentTotalInvestment - currentAssetInvestment + correctInvestmentUSD;
      
      for (const currency of CONFIG.CURRENCIES) {
        const rate = rates[currency] || 1;
        updates[`${currency}.totalInvestment`] = correctedTotalInvestment * rate;
        
        // Recalcular totalROI del documento
        const totalValue = overallDoc.data[currency]?.totalValue || 0;
        const newTotalROI = correctedTotalInvestment > 0 
          ? ((totalValue / rate - correctedTotalInvestment) / correctedTotalInvestment) * 100
          : 0;
        updates[`${currency}.totalROI`] = newTotalROI;
      }
      
      if (!dryRun) {
        batch.update(db.doc(overallDoc.path), updates);
      }
      log('SUCCESS', `OVERALL ${date}: ${Object.keys(updates).length} campos a actualizar`);
      updateCount++;
    }
    
    // Actualizar cuenta específica
    const accountDoc = await getPerformanceDoc(date, CONFIG.ACCOUNT_ID);
    if (accountDoc) {
      const rates = deriveExchangeRates(accountDoc.data);
      const updates = {};
      
      for (const currency of CONFIG.CURRENCIES) {
        const rate = rates[currency] || 1;
        const currencyPath = `${currency}.assetPerformance.${CONFIG.ASSET_KEY}`;
        
        updates[`${currencyPath}.units`] = CONFIG.UNITS;
        updates[`${currencyPath}.totalValue`] = totalValueUSD * rate;
        updates[`${currencyPath}.totalInvestment`] = correctInvestmentUSD * rate;
        updates[`${currencyPath}.totalROI`] = totalROI;
        updates[`${currencyPath}.unrealizedProfitAndLoss`] = unrealizedPnL * rate;
      }
      
      // También corregir los totales del documento de la cuenta
      const currentTotalInvestment = accountDoc.data.USD?.totalInvestment || 0;
      const currentAssetInvestment = accountDoc.data.USD?.assetPerformance?.[CONFIG.ASSET_KEY]?.totalInvestment || 0;
      const correctedTotalInvestment = currentTotalInvestment - currentAssetInvestment + correctInvestmentUSD;
      
      for (const currency of CONFIG.CURRENCIES) {
        const rate = rates[currency] || 1;
        updates[`${currency}.totalInvestment`] = correctedTotalInvestment * rate;
        
        const totalValue = accountDoc.data[currency]?.totalValue || 0;
        const newTotalROI = correctedTotalInvestment > 0 
          ? ((totalValue / rate - correctedTotalInvestment) / correctedTotalInvestment) * 100
          : 0;
        updates[`${currency}.totalROI`] = newTotalROI;
      }
      
      if (!dryRun) {
        batch.update(db.doc(accountDoc.path), updates);
      }
      log('SUCCESS', `ACCOUNT ${date}: ${Object.keys(updates).length} campos a actualizar`);
      updateCount++;
    }
  }
  
  if (!dryRun && updateCount > 0) {
    await batch.commit();
    log('SUCCESS', `✅ ${updateCount} documentos actualizados correctamente`);
  } else if (dryRun) {
    log('INFO', `🔍 DRY RUN: ${updateCount} documentos se actualizarían`);
  }
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const args = process.argv.slice(2);
  
  if (args.includes('--analyze')) {
    await analyzeCorruption();
  } else if (args.includes('--dry-run')) {
    await fixCorruption(true);
  } else if (args.includes('--fix')) {
    await fixCorruption(false);
  } else {
    console.log(`
USO:
  node fixEcopetrolCorruption.js --analyze     # Solo muestra los datos
  node fixEcopetrolCorruption.js --dry-run    # Muestra cambios sin aplicar
  node fixEcopetrolCorruption.js --fix        # Aplica las correcciones
    `);
  }
  
  process.exit(0);
}

main().catch(err => {
  console.error('Error fatal:', err);
  process.exit(1);
});
