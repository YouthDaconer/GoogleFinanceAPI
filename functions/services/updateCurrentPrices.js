const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require('firebase-admin');
const { getQuotes } = require('./financeQuery');
const axios = require('axios'); // Asegúrate de tener esta dependencia instalada

// Importar generador de logos
const { generateLogoUrl } = require('../utils/logoGenerator');

const API_BASE_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

// Función para obtener el ISIN de una acción
async function getStockIsin(symbol) {
  try {
    const response = await axios.get(`${API_BASE_URL}/stock/${symbol}`);
    if (response.data && response.data.isin) {
      return response.data.isin;
    }
    return null;
  } catch (error) {
    console.error(`Error al obtener ISIN para la acción ${symbol}:`, error.message);
    return null;
  }
}

// Función para obtener el ISIN de un ETF
async function getEtfIsin(symbol) {
  try {
    const response = await axios.get(`${API_BASE_URL}/etf/${symbol}/basic`);
    
    // Verificar que el ETF sea de tipo 'equity'
    if (response.data && 
        response.data.financial_info && 
        response.data.financial_info.asset_class === 'equity' && 
        response.data.isin) {
      return response.data.isin;
    }
    return null;
  } catch (error) {
    console.error(`Error al obtener ISIN para el ETF ${symbol}:`, error.message);
    return null;
  }
}

async function updateCurrentPrices() {
  const db = admin.firestore();
  const currentPricesRef = db.collection('currentPrices');

  try {
    const snapshot = await currentPricesRef.get();
    const batch = db.batch();
    let updatesCount = 0;
    let isinsUpdated = 0;

    const symbols = snapshot.docs.map(doc => doc.data().symbol);
    const batchSize = 50;
    for (let i = 0; i < symbols.length; i += batchSize) {
      const symbolBatch = symbols.slice(i, i + batchSize).join(',');

      // Obtener cotizaciones para el lote de símbolos
      const quotes = await getQuotes(symbolBatch);

      if (quotes) {
        const quotesMap = new Map(quotes.map(quote => [quote.symbol, quote]));

        for (const doc of snapshot.docs) {
          const docData = doc.data();
          const { symbol, type } = docData;

          // Verificar si necesitamos obtener el ISIN
          if (!docData.isin) {
            let isin = null;
            
            // Obtener ISIN según el tipo de instrumento
            if (type === 'stock') {
              isin = await getStockIsin(symbol);
              if (isin) {
                console.log(`ISIN obtenido para acción ${symbol}: ${isin}`);
              }
            } else if (type === 'etf') {
              isin = await getEtfIsin(symbol);
              if (isin) {
                console.log(`ISIN obtenido para ETF ${symbol}: ${isin}`);
              }
            }
            
            // Si obtuvimos un ISIN, añadirlo al objeto de actualización
            if (isin) {
              batch.update(doc.ref, { isin });
              isinsUpdated++;
            }
          }

          const quoteData = quotesMap.get(symbol);

          if (quoteData && quoteData.price) {
            // Normalizar el precio eliminando comas
            const normalizedPrice = quoteData.price.replace(/,/g, ''); 
            const newPrice = parseFloat(normalizedPrice);
            
            const updatedData = {
              symbol: symbol,
              price: newPrice,
              lastUpdated: Date.now(),
              name: quoteData.name || docData.name,
              change: quoteData.change,
              percentChange: quoteData.percentChange,
            };

            // Lista de campos adicionales a agregar si están en quoteData
            const optionalKeys = [
              'logo', 'website', 'open', 'high', 'low',
              'yearHigh', 'yearLow', 'volume', 'avgVolume',
              'marketCap', 'beta', 'pe', 'eps',
              'earningsDate', 'industry', 'sector', 'about', 'employees', 
              'dividend', 'exDividend', 'yield', 'dividendDate',
              'threeMonthReturn', 'sixMonthReturn', 'ytdReturn', 
              'threeYearReturn', 'yearReturn', 'fiveYearReturn', 
              'currency', 'currencySymbol', 'exchangeName', 'country', 'city'
            ];

            optionalKeys.forEach(key => {
              if (quoteData[key] !== null && quoteData[key] !== undefined) {
                updatedData[key] = quoteData[key];
              }
            });

            // Generar logo si no viene en la respuesta del API y no existe en el documento
            if (!updatedData.logo && !docData.logo) {
              const generatedLogo = generateLogoUrl(symbol, { 
                website: quoteData.website || docData.website, 
                assetType: type 
              });
              if (generatedLogo) {
                updatedData.logo = generatedLogo;
                console.log(`Logo generado para ${symbol}: ${generatedLogo}`);
              }
            }

            batch.update(doc.ref, updatedData);
            updatesCount++;
            console.log(`Actualizado precio para ${symbol}`);
          } else {
            console.warn(`No se pudo obtener el precio para ${symbol}`);
          }
        }
      }
    }

    if (updatesCount > 0 || isinsUpdated > 0) {
      await batch.commit();
      console.log(`${updatesCount} precios han sido actualizados`);
      if (isinsUpdated > 0) {
        console.log(`${isinsUpdated} ISINs han sido actualizados`);
      }
    } else {
      console.log('No se requirieron actualizaciones');
    }
  } catch (error) {
    console.error('Error al actualizar precios e ISINs:', error);
  }
}

// Programar la actualización a las 8:00 AM y 5:00 PM durante días laborables
exports.scheduledUpdatePrices = onSchedule({
  schedule: '0 8,17 * * 1-5',
  timeZone: 'America/New_York',
  retryCount: 3,
}, async (event) => {
  await updateCurrentPrices();
  return null;
});

// Nueva función que se ejecuta cada 2 minutos durante el horario de mercado
exports.scheduledUpdateQuotes = onSchedule({
  schedule: '*/2 9-17 * * 1-5',
  timeZone: 'America/New_York',
  retryCount: 3,
}, async (event) => {
  await updateMarketQuotes();
  return null;
});

// Función para actualizar precios usando el nuevo endpoint de cotizaciones múltiples
async function updateMarketQuotes() {
  const db = admin.firestore();
  const currentPricesRef = db.collection('currentPrices');

  try {
    const snapshot = await currentPricesRef.get();
    const batch = db.batch();
    let updatesCount = 0;

    // Obtener todos los símbolos
    const symbols = snapshot.docs.map(doc => doc.data().symbol);
    
    // Procesar en lotes de 50 símbolos
    const batchSize = 50;
    for (let i = 0; i < symbols.length; i += batchSize) {
      const symbolBatch = symbols.slice(i, i + batchSize);
      const symbolsParam = symbolBatch.join(',');
      
      try {
        // Consultar el nuevo endpoint
        const response = await axios.get(`${API_BASE_URL}/market-quotes?symbols=${symbolsParam}`);
        
        if (Array.isArray(response.data)) {
          // Crear un mapa para acceso rápido
          const quotesMap = new Map(response.data.map(quote => [quote.symbol, quote]));
          
          // Actualizar cada documento en el lote actual
          for (const symbol of symbolBatch) {
            const quote = quotesMap.get(symbol);
            
            if (quote && quote.regularMarketPrice) {
              // Buscar el documento correspondiente
              const matchingDocs = snapshot.docs.filter(doc => doc.data().symbol === symbol);
              
              if (matchingDocs.length > 0) {
                const doc = matchingDocs[0];
                const docData = doc.data();
                
                const updatedData = {
                  symbol: symbol,
                  price: quote.regularMarketPrice,
                  lastUpdated: Date.now(),
                  change: quote.regularMarketChange,
                  percentChange: quote.regularMarketChangePercent,
                  previousClose: quote.regularMarketPreviousClose,
                  currency: quote.currency,
                  marketState: quote.marketState,
                  quoteType: quote.quoteType,
                  exchange: quote.exchange,
                  fullExchangeName: quote.fullExchangeName
                };
                
                // Mantener campos existentes si no están en la nueva respuesta
                if (docData.name) {
                  updatedData.name = docData.name;
                }
                
                // Mantener el ISIN si existe
                if (docData.isin) {
                  updatedData.isin = docData.isin;
                }
                
                // Mantener el tipo si existe
                if (docData.type) {
                  updatedData.type = docData.type;
                }
                
                batch.update(doc.ref, updatedData);
                updatesCount++;
                console.log(`Actualizado precio para ${symbol} usando market-quotes: ${quote.regularMarketPrice} ${quote.currency}`);
              }
            } else {
              console.warn(`No se pudo obtener el precio para ${symbol} desde market-quotes`);
            }
          }
        } else {
          console.warn('Formato de respuesta inesperado:', response.data);
        }
      } catch (error) {
        console.error(`Error al obtener cotizaciones para el lote de símbolos:`, error.message);
      }
    }

    if (updatesCount > 0) {
      await batch.commit();
      console.log(`${updatesCount} precios han sido actualizados usando market-quotes`);
    } else {
      console.log('No se requirieron actualizaciones desde market-quotes');
    }
  } catch (error) {
    console.error('Error al actualizar precios desde market-quotes:', error);
  }
}