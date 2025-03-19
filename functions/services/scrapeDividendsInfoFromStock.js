const axios = require("axios");
const cheerio = require("cheerio");
const { DateTime } = require("luxon");
const admin = require("firebase-admin");

/**
 * Obtiene información de dividendos para un símbolo específico desde StockEvents
 * @param {string} symbol - El símbolo del ETF o acción
 * @returns {object} Objeto con la información de dividendos o null si no hay datos
 */
async function scrapeDividendInfo(symbol) {
  try {
    const url = `https://stockevents.app/en/stock/${symbol}/dividends`;
    console.log(`Consultando información de dividendos para ${symbol} en ${url}`);
    
    const { data } = await axios.get(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
      }
    });
    
    const $ = cheerio.load(data);
    
    // Buscar utilizando el texto dentro de dt y luego obteniendo el dd correspondiente
    const yieldText = $('dt:contains("Dividend Yield")').closest('div').find('dd').text().trim();
    const dividendText = $('dt:contains("Dividend amount")').closest('div').find('dd').text().trim();
    const exDividendText = $('dt:contains("Last ex-date")').closest('div').find('dd').text().trim();
    const dividendDateText = $('dt:contains("Last pay date")').closest('div').find('dd').text().trim();
    
    // Si no hay datos de dividendos, retornar null
    if (!yieldText && !dividendText && !exDividendText && !dividendDateText) {
      console.log(`No se encontró información de dividendos para ${symbol}`);
      return null;
    }
    
    // Procesar yield (quitar el símbolo % y convertir a número)
    const yield = yieldText ? parseFloat(yieldText.replace('%', '')) : null;
    
    // Procesar dividend (quitar el símbolo $ y convertir a número)
    const dividend = dividendText ? parseFloat(dividendText.replace('$', '')) : null;
    
    // Formatear fechas
    let exDividend = null;
    let dividendDate = null;
    
    if (exDividendText) {
      try {
        const datePattern = /(\w{3})\s+(\d{1,2}),\s+(\d{4})/;
        const exMatch = exDividendText.match(datePattern);
        
        if (exMatch) {
          const month = exMatch[1];
          const day = exMatch[2];
          const year = exMatch[3];
          
          const dateStr = `${month} ${day}, ${year}`;
          exDividend = DateTime.fromFormat(dateStr, 'LLL d, yyyy', { locale: 'en' }).toFormat('MMM d, yyyy');
        }
      } catch (error) {
        console.error(`Error al procesar la fecha ex-dividendo para ${symbol}:`, error);
      }
    }
    
    if (dividendDateText) {
      try {
        const datePattern = /(\w{3})\s+(\d{1,2}),\s+(\d{4})/;
        const payMatch = dividendDateText.match(datePattern);
        
        if (payMatch) {
          const month = payMatch[1];
          const day = payMatch[2];
          const year = payMatch[3];
          
          const dateStr = `${month} ${day}, ${year}`;
          dividendDate = DateTime.fromFormat(dateStr, 'LLL d, yyyy', { locale: 'en' }).toFormat('MMM d, yyyy');
        }
      } catch (error) {
        console.error(`Error al procesar la fecha de pago para ${symbol}:`, error);
      }
    }
    
    return {
      yield: yield,
      dividend: dividend * 4,
      exDividend: exDividend,
      dividendDate: dividendDate
    };
  } catch (error) {
    console.error(`Error al obtener información de dividendos para ${symbol}:`, error.message);
    return null;
  }
}

/**
 * Actualiza la información de dividendos para todos los ETFs en la colección currentPrices
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
    
    // Filtrar en memoria aquellos sin información de dividendos
    const assetsToUpdate = snapshot.docs.filter(doc => {
      const data = doc.data();
      // Verificar si los campos no existen o son null
      return data.dividend === undefined || data.dividend === null || 
             data.dividendDate === undefined || data.dividendDate === null;
    });
    
    if (assetsToUpdate.length === 0) {
      console.log('Todos los activos ya tienen información de dividendos');
      return;
    }
    
    console.log(`Actualizando información de dividendos para ${assetsToUpdate.length} activos (ETFs y acciones)`);
    
    // Control de flujo para evitar bloqueos
    let batch = db.batch();
    let updatesCount = 0;
    let errorsCount = 0;
    let waitTime = 3000; // Comenzar con 3 segundos para ser conservadores
    const MAX_BATCH_SIZE = 20;
    const MAX_ASSETS_PER_RUN = 50; // Reducir a 50 para ser más conservadores
    
    // Procesar solo un subconjunto de activos por ejecución
    const assetsToProcess = assetsToUpdate.slice(0, MAX_ASSETS_PER_RUN);
    
    for (const doc of assetsToProcess) {
      const data = doc.data();
      const symbol = data.symbol;
      
      try {
        console.log(`Procesando ${symbol} (${data.type})...`);
        
        // Esperar entre solicitudes para prevenir bloqueos
        await new Promise(resolve => setTimeout(resolve, waitTime));
        
        const dividendInfo = await scrapeDividendInfo(symbol);
        
        if (dividendInfo) {
          batch.update(doc.ref, {
            yield: dividendInfo.yield,
            dividend: dividendInfo.dividend,
            exDividend: dividendInfo.exDividend,
            dividendDate: dividendInfo.dividendDate,
            lastDividendUpdate: admin.firestore.FieldValue.serverTimestamp()
          });
          
          console.log(`Actualizada información de dividendos para ${symbol} (${data.type})`);
          updatesCount++;
          
          // Commit batch cada cierto número de actualizaciones
          if (updatesCount % MAX_BATCH_SIZE === 0) {
            await batch.commit();
            console.log(`Lote de ${MAX_BATCH_SIZE} actualizaciones guardado`);
            batch = db.batch();
            
            // Pausa adicional entre lotes
            await new Promise(resolve => setTimeout(resolve, 2000));
          }
        } else {
          console.log(`No se encontró información de dividendos para ${symbol} (${data.type})`);
        }
      } catch (error) {
        console.error(`Error procesando ${symbol}:`, error);
        errorsCount++;
      }
    }
    
    // Guardar actualizaciones pendientes
    if (updatesCount % MAX_BATCH_SIZE !== 0 && updatesCount > 0) {
      await batch.commit();
      console.log(`Lote final de ${updatesCount % MAX_BATCH_SIZE} actualizaciones guardado`);
    }
    
    console.log(`Proceso completado: ${updatesCount} activos actualizados, ${errorsCount} errores`);
    
    if (assetsToUpdate.length > MAX_ASSETS_PER_RUN) {
      console.log(`Atención: Quedan ${assetsToUpdate.length - MAX_ASSETS_PER_RUN} activos pendientes por actualizar.`);
    }
    
  } catch (error) {
    console.error('Error al actualizar información de dividendos:', error);
  }
}

module.exports = {
  scrapeDividendInfo,
  scrapeDividendsInfoFromStockEvents
};