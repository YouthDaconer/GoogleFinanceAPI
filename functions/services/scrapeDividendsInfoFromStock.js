/**
 * Scraper de Información de Dividendos
 * 
 * OPT-DEMAND-CLEANUP: Refactorizado para:
 * - Leer símbolos de `assets` en lugar de `currentPrices`
 * - NO escribir a Firestore (retornar datos en memoria)
 * - Los datos de dividendos vienen del API directamente
 * 
 * SEC-CF-001: Migrado a Cloudflare Tunnel
 * 
 * @module scrapeDividendsInfoFromStock
 * @see docs/architecture/OPT-DEMAND-CLEANUP-phase4-closure-subplan.md
 * @see docs/architecture/SEC-CF-001-cloudflare-tunnel-migration-plan.md
 */

const axios = require("axios");
const { DateTime } = require("luxon");
const admin = require("firebase-admin");
const { FINANCE_QUERY_API_URL } = require('./config');

// SEC-CF-001: API URL via Cloudflare Tunnel
const LAMBDA_API_BASE_URL = FINANCE_QUERY_API_URL;

// FastAPI con StockEvents scraper (fuente primaria - datos más actualizados)
const FASTAPI_BASE_URL = FINANCE_QUERY_API_URL;

/**
 * Obtiene información de dividendos para un símbolo desde StockEvents via FastAPI
 * Esta es la fuente primaria - datos más actualizados
 * @param {string} symbol - El símbolo del ETF o acción
 * @returns {object} Objeto con la información de dividendos o null si no hay datos
 */
async function getDividendInfoFromStockEvents(symbol) {
  try {
    const url = `${FASTAPI_BASE_URL}/dividends?symbols=${encodeURIComponent(symbol)}`;
    console.log(`[StockEvents] Consultando dividendos para ${symbol}`);
    
    const { data } = await axios.get(url, { timeout: 15000 });
    
    if (!data || !data.dividendDate) {
      console.log(`[StockEvents] No hay datos de dividendos para ${symbol}`);
      return null;
    }
    
    // La API devuelve:
    // - dividendYield: porcentaje (número)
    // - dividend: valor del dividendo
    // - lastDividend: valor del último dividendo por acción
    // - exDividend: fecha ex-dividend (ej: "Dec 05, 2025")
    // - dividendDate: fecha de pago (ej: "Jan 06, 2026")
    
    return {
      yield: data.dividendYield || null,
      dividend: data.dividend || null,
      lastDividend: data.lastDividend || null,
      exDividend: data.exDividend || null,
      dividendDate: data.dividendDate || null,
      source: 'stockevents'
    };
  } catch (error) {
    console.error(`[StockEvents] Error obteniendo dividendos para ${symbol}:`, error.message);
    return null;
  }
}

/**
 * Obtiene información de dividendos para un símbolo desde nuestra API Lambda
 * Esta es la fuente de fallback
 * @param {string} symbol - El símbolo del ETF o acción
 * @returns {object} Objeto con la información de dividendos o null si no hay datos
 */
async function getDividendInfoFromLambda(symbol) {
  try {
    const url = `${LAMBDA_API_BASE_URL}/quotes?symbols=${encodeURIComponent(symbol)}`;
    console.log(`[Lambda API] Consultando dividendos para ${symbol}`);
    
    const { data: responseData } = await axios.get(url, { timeout: 10000 });
    
    // La API siempre devuelve un array, incluso para un solo símbolo
    const data = Array.isArray(responseData) ? responseData[0] : responseData;
    
    if (!data || !data.dividendDate) {
      console.log(`[Lambda API] No hay datos de dividendos para ${symbol}`);
      return null;
    }
    
    // La API devuelve:
    // - dividend: valor anual del dividendo (string)
    // - yield: porcentaje (ej: "4.07%")
    // - exDividend: fecha ex-dividend (ej: "Dec 05, 2025")
    // - dividendDate: fecha de pago (ej: "Jan 06, 2026")
    // - lastDividend: valor del último dividendo por acción (string)
    
    const yieldValue = data.yield ? parseFloat(data.yield.replace('%', '')) : null;
    const dividendValue = data.dividend ? parseFloat(data.dividend) : null;
    const lastDividendValue = data.lastDividend ? parseFloat(data.lastDividend) : null;
    
    return {
      yield: yieldValue,
      dividend: dividendValue,
      lastDividend: lastDividendValue,
      exDividend: data.exDividend || null,
      dividendDate: data.dividendDate || null,
      source: 'lambda'
    };
  } catch (error) {
    console.error(`[Lambda API] Error obteniendo dividendos para ${symbol}:`, error.message);
    return null;
  }
}

/**
 * Obtiene información de dividendos para un símbolo
 * Intenta StockEvents primero (datos más actualizados), luego Lambda como fallback
 * @param {string} symbol - El símbolo del ETF o acción
 * @returns {object} Objeto con la información de dividendos o null si no hay datos
 */
async function getDividendInfo(symbol) {
  // Primero intentar StockEvents (datos más frescos)
  let result = await getDividendInfoFromStockEvents(symbol);
  
  // Si falla, usar Lambda como fallback
  if (!result) {
    console.log(`[DividendService] StockEvents falló, usando Lambda como fallback para ${symbol}`);
    result = await getDividendInfoFromLambda(symbol);
  }
  
  return result;
}

/**
 * @deprecated Usar getDividendInfo en lugar de este método
 */
async function scrapeDividendInfo(symbol) {
  return getDividendInfo(symbol);
}

/**
 * OPT-DEMAND-CLEANUP: Obtiene información de dividendos para activos del usuario.
 * 
 * CAMBIOS:
 * - Lee símbolos de `assets` en lugar de `currentPrices`
 * - NO escribe a Firestore (retorna datos en memoria)
 * - Los datos son usados por `processDividendPayments`
 * 
 * @returns {Promise<Map<string, object>>} Mapa de símbolo a datos de dividendos
 */
async function scrapeDividendsInfoFromStockEvents() {
  const db = admin.firestore();
  const dividendDataMap = new Map();
  
  try {
    // OPT-DEMAND-CLEANUP: Leer símbolos de assets activos (no de currentPrices)
    const assetsSnapshot = await db.collection('assets')
      .where('isActive', '==', true)
      .get();
    
    if (assetsSnapshot.empty) {
      console.log('[scrapeDividendsInfoFromStockEvents] No se encontraron assets activos');
      return dividendDataMap;
    }
    
    // Extraer símbolos únicos de ETFs y stocks
    const symbolsSet = new Set();
    assetsSnapshot.docs.forEach(doc => {
      const data = doc.data();
      const assetType = data.assetType?.toLowerCase();
      if ((assetType === 'etf' || assetType === 'stock') && data.name) {
        symbolsSet.add(data.name);
      }
    });
    
    const symbols = Array.from(symbolsSet);
    
    if (symbols.length === 0) {
      console.log('[scrapeDividendsInfoFromStockEvents] No hay ETFs o acciones activos');
      return dividendDataMap;
    }
    
    console.log(`[scrapeDividendsInfoFromStockEvents] Consultando dividendos para ${symbols.length} símbolos únicos`);
    
    let stockEventsCount = 0;
    let lambdaFallbackCount = 0;
    let noDataCount = 0;
    
    // Intentar StockEvents batch primero (más actualizado)
    let stockEventsResults = [];
    try {
      const symbolsParam = symbols.join(',');
      const url = `${FASTAPI_BASE_URL}/dividends/batch?symbols=${encodeURIComponent(symbolsParam)}`;
      console.log(`[StockEvents] Intentando batch request para ${symbols.length} símbolos`);
      
      const { data: batchResponse } = await axios.get(url, { timeout: 60000 });
      
      if (batchResponse && batchResponse.success) {
        stockEventsResults = batchResponse.success;
        console.log(`[StockEvents] Batch exitoso: ${stockEventsResults.length} símbolos con datos`);
      }
    } catch (error) {
      console.log(`[StockEvents] Batch falló, usando Lambda como fallback:`, error.message);
    }
    
    // Crear set de símbolos que obtuvimos de StockEvents
    const stockEventsSymbols = new Set(stockEventsResults.map(r => r.symbol));
    
    // Procesar resultados de StockEvents (guardar en memoria, no en Firestore)
    for (const quote of stockEventsResults) {
      if (!quote || !quote.symbol) continue;
      
      if (quote.dividendDate || quote.exDividend || quote.dividend) {
        dividendDataMap.set(quote.symbol, {
          yield: quote.dividendYield || null,
          dividend: quote.dividend || null,
          lastDividend: quote.lastDividend || null,
          exDividend: quote.exDividend || null,
          dividendDate: quote.dividendDate || null,
          source: 'stockevents'
        });
        stockEventsCount++;
      } else {
        noDataCount++;
      }
    }
    
    // Para símbolos que no obtuvimos de StockEvents, usar Lambda API
    const missingSymbols = symbols.filter(s => !stockEventsSymbols.has(s));
    
    if (missingSymbols.length > 0) {
      console.log(`[Lambda API] Procesando ${missingSymbols.length} símbolos faltantes`);
      
      // Procesar en chunks de 50 para Lambda
      const CHUNK_SIZE = 50;
      
      for (let i = 0; i < missingSymbols.length; i += CHUNK_SIZE) {
        const chunk = missingSymbols.slice(i, i + CHUNK_SIZE);
        const symbolsParam = chunk.join(',');
        
        try {
          const url = `${LAMBDA_API_BASE_URL}/quotes?symbols=${encodeURIComponent(symbolsParam)}`;
          const { data: apiResponse } = await axios.get(url, { timeout: 30000 });
          
          const quotes = Array.isArray(apiResponse) ? apiResponse : [apiResponse];
          
          for (const quote of quotes) {
            if (!quote || !quote.symbol) continue;
            
            if (quote.dividendDate || quote.exDividend || quote.dividend) {
              const yieldValue = quote.yield ? parseFloat(String(quote.yield).replace('%', '')) : null;
              
              dividendDataMap.set(quote.symbol, {
                yield: yieldValue,
                dividend: quote.dividend || null,
                lastDividend: quote.lastDividend || null,
                exDividend: quote.exDividend || null,
                dividendDate: quote.dividendDate || null,
                source: 'lambda'
              });
              lambdaFallbackCount++;
            } else {
              noDataCount++;
            }
          }
          
          // Pequeña pausa entre chunks
          if (i + CHUNK_SIZE < missingSymbols.length) {
            await new Promise(resolve => setTimeout(resolve, 500));
          }
          
        } catch (error) {
          console.error(`Error procesando chunk Lambda:`, error.message);
        }
      }
    }
    
    console.log(`[scrapeDividendsInfoFromStockEvents] Completado: ${dividendDataMap.size} símbolos con datos (${stockEventsCount} StockEvents, ${lambdaFallbackCount} Lambda), ${noDataCount} sin datos`);
    
    return dividendDataMap;
    
  } catch (error) {
    console.error('[scrapeDividendsInfoFromStockEvents] Error:', error);
    return dividendDataMap;
  }
}

module.exports = {
  scrapeDividendInfo,
  getDividendInfo,
  getDividendInfoFromLambda,
  getDividendInfoFromStockEvents,
  scrapeDividendsInfoFromStockEvents
};