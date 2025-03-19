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
    // Obtener todos los ETFs de la colección currentPrices
    const etfsSnapshot = await db.collection('currentPrices')
      .where('type', '==', 'etf')
      .get();
    
    if (etfsSnapshot.empty) {
      console.log('No se encontraron ETFs en la colección currentPrices');
      return;
    }
    
    console.log(`Actualizando información de dividendos para ${etfsSnapshot.size} ETFs`);
    
    let batch = db.batch();
    let updatesCount = 0;
    let errorsCount = 0;
    
    // Procesar cada ETF en lotes para evitar exceder límites de API
    for (const doc of etfsSnapshot.docs) {
      const etfData = doc.data();
      const symbol = etfData.symbol;
      
      try {
        // Esperar un tiempo entre solicitudes para evitar ser bloqueado
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        // Obtener información de dividendos
        const dividendInfo = await scrapeDividendInfo(symbol);
        
        if (dividendInfo) {
          // Actualizar documento en Firestore
          batch.update(doc.ref, {
            yield: dividendInfo.yield,
            dividend: dividendInfo.dividend,
            exDividend: dividendInfo.exDividend,
            dividendDate: dividendInfo.dividendDate,
            lastDividendUpdate: admin.firestore.FieldValue.serverTimestamp()
          });
          
          console.log(`Actualizada información de dividendos para ${symbol}`);
          updatesCount++;
          
          // Commit batch cada 20 actualizaciones para evitar exceder límites
          if (updatesCount % 20 === 0) {
            await batch.commit();
            console.log(`Lote de ${updatesCount} actualizaciones guardado`);
            // Crear un nuevo batch
            batch = db.batch();
          }
        }
      } catch (error) {
        console.error(`Error procesando ${symbol}:`, error);
        errorsCount++;
      }
    }
    
    // Guardar las actualizaciones restantes
    if (updatesCount % 20 !== 0) {
      await batch.commit();
    }
    
    console.log(`Proceso completado: ${updatesCount} ETFs actualizados, ${errorsCount} errores`);
    
  } catch (error) {
    console.error('Error al actualizar información de dividendos:', error);
  }
}

module.exports = {
  scrapeDividendInfo,
  scrapeDividendsInfoFromStockEvents
};