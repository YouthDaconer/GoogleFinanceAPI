const functions = require('firebase-functions');
const admin = require('firebase-admin');
const { scrapeSimpleQuote } = require('./scrapeQuote');
const NodeCache = require('node-cache');

admin.initializeApp();

// Inicializar caché con un tiempo de vida de 25 minutos
const cache = new NodeCache({ stdTTL: 1500 });

exports.updateCurrentPrices = functions.pubsub
  .schedule('every 30 minutes')
  .onRun(async (context) => {
    const db = admin.firestore();
    const currentPricesRef = db.collection('currentPrices');

    try {
      const snapshot = await currentPricesRef.get();
      const batch = db.batch();
      let updatesCount = 0;

      for (const doc of snapshot.docs) {
        const { symbol, market, price: lastPrice } = doc.data();
        const cacheKey = `${symbol}:${market}`;

        // Verificar si los datos están en caché
        const cachedData = cache.get(cacheKey);
        if (cachedData) {
          console.log(`Usando datos en caché para ${symbol}:${market}`);
          continue;
        }

        try {
          const quoteData = await scrapeSimpleQuote(symbol, market);
          
          if (quoteData && quoteData.current) {
            const newPrice = parseFloat(quoteData.current);
            
            // Actualizar solo si el precio ha cambiado significativamente (más del 0.1%)
            if (Math.abs(newPrice - lastPrice) / lastPrice > 0.001) {
              const updatedData = {
                symbol: symbol,
                market: market,
                price: newPrice,
                lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
                name: quoteData.name || doc.data().name,
                change: quoteData.change,
                percentChange: quoteData.percentChange
              };

              batch.update(doc.ref, updatedData);
              cache.set(cacheKey, updatedData);
              updatesCount++;
              console.log(`Actualizado precio para ${symbol}:${market}`);
            } else {
              console.log(`No se requiere actualización para ${symbol}:${market}`);
              cache.set(cacheKey, doc.data());
            }
          } else {
            console.warn(`No se pudo obtener el precio para ${symbol}:${market}`);
          }
        } catch (error) {
          console.error(`Error al obtener datos para ${symbol}:${market}:`, error);
        }
      }

      if (updatesCount > 0) {
        await batch.commit();
        console.log(`${updatesCount} precios han sido actualizados`);
      } else {
        console.log('No se requirieron actualizaciones');
      }
    } catch (error) {
      console.error('Error al actualizar precios:', error);
    }

    return null;
  });

// Función HTTP para pruebas locales (opcional)
/*exports.httpUpdateCurrentPrices = functions.https.onRequest(async (req, res) => {
  await exports.updateCurrentPrices.run();
  res.send('Actualización de precios completada');
});*/