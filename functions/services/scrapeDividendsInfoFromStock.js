const axios = require("axios");
const { DateTime } = require("luxon");
const admin = require("firebase-admin");

// API Lambda para datos de mercado (fallback)
const LAMBDA_API_BASE_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

// FastAPI con StockEvents scraper (fuente primaria - datos más actualizados)
const FASTAPI_BASE_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

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
 * Actualiza la información de dividendos para todos los ETFs y acciones en la colección currentPrices
 * Usa StockEvents via FastAPI como fuente primaria, con Lambda como fallback
 */
async function scrapeDividendsInfoFromStockEvents() {
  const db = admin.firestore();
  
  try {
    // Hacer una única consulta eficiente para obtener todos los ETFs y acciones
    const snapshot = await db.collection('currentPrices')
      .where('type', 'in', ['etf', 'stock'])
      .get();
    
    if (snapshot.empty) {
      console.log('No se encontraron ETFs o acciones en la colección currentPrices');
      return;
    }
    
    console.log(`Actualizando información de dividendos para ${snapshot.docs.length} activos (ETFs y acciones)`);
    
    // Crear mapa de símbolos a referencias de documentos
    const symbols = snapshot.docs.map(doc => doc.data().symbol);
    const symbolToDocRef = new Map();
    snapshot.docs.forEach(doc => {
      symbolToDocRef.set(doc.data().symbol, doc.ref);
    });
    
    // Primero intentar obtener datos via StockEvents (endpoint FastAPI /dividends/batch)
    // Si falla, usar Lambda API como fallback
    let batch = db.batch();
    let updatesCount = 0;
    let noDataCount = 0;
    let stockEventsCount = 0;
    let lambdaFallbackCount = 0;
    const MAX_BATCH_SIZE = 400; // Firestore limit is 500
    
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
    
    // Procesar resultados de StockEvents
    for (const quote of stockEventsResults) {
      if (!quote || !quote.symbol) continue;
      
      const docRef = symbolToDocRef.get(quote.symbol);
      if (!docRef) continue;
      
      if (quote.dividendDate || quote.exDividend || quote.dividend) {
        const updateData = {
          lastDividendUpdate: admin.firestore.FieldValue.serverTimestamp(),
          dividendSource: 'stockevents'
        };
        
        if (quote.dividendYield !== null && !isNaN(quote.dividendYield)) updateData.yield = quote.dividendYield;
        if (quote.dividend) updateData.dividend = quote.dividend;
        if (quote.lastDividend) updateData.lastDividend = quote.lastDividend;
        if (quote.exDividend) updateData.exDividend = quote.exDividend;
        if (quote.dividendDate) updateData.dividendDate = quote.dividendDate;
        
        batch.update(docRef, updateData);
        updatesCount++;
        stockEventsCount++;
        
        if (updatesCount % MAX_BATCH_SIZE === 0) {
          await batch.commit();
          console.log(`Lote de ${MAX_BATCH_SIZE} actualizaciones guardado`);
          batch = db.batch();
        }
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
            
            const docRef = symbolToDocRef.get(quote.symbol);
            if (!docRef) continue;
            
            if (quote.dividendDate || quote.exDividend || quote.dividend) {
              const yieldValue = quote.yield ? parseFloat(quote.yield.replace('%', '')) : null;
              
              const updateData = {
                lastDividendUpdate: admin.firestore.FieldValue.serverTimestamp(),
                dividendSource: 'lambda'
              };
              
              if (yieldValue !== null && !isNaN(yieldValue)) updateData.yield = yieldValue;
              if (quote.dividend) updateData.dividend = quote.dividend;
              if (quote.lastDividend) updateData.lastDividend = quote.lastDividend;
              if (quote.exDividend) updateData.exDividend = quote.exDividend;
              if (quote.dividendDate) updateData.dividendDate = quote.dividendDate;
              
              batch.update(docRef, updateData);
              updatesCount++;
              lambdaFallbackCount++;
              
              if (updatesCount % MAX_BATCH_SIZE === 0) {
                await batch.commit();
                console.log(`Lote de ${MAX_BATCH_SIZE} actualizaciones guardado`);
                batch = db.batch();
              }
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
    
    // Guardar actualizaciones pendientes
    if (updatesCount % MAX_BATCH_SIZE !== 0 && updatesCount > 0) {
      await batch.commit();
      console.log(`Lote final guardado`);
    }
    
    console.log(`Proceso completado: ${updatesCount} activos actualizados (${stockEventsCount} de StockEvents, ${lambdaFallbackCount} de Lambda), ${noDataCount} sin datos de dividendos`);
    
  } catch (error) {
    console.error('Error al actualizar información de dividendos:', error);
  }
}

module.exports = {
  scrapeDividendInfo,
  getDividendInfo,
  getDividendInfoFromLambda,
  getDividendInfoFromStockEvents,
  scrapeDividendsInfoFromStockEvents
};