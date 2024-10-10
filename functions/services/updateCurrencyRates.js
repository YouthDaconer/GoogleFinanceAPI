const functions = require('firebase-functions');
const admin = require('./firebaseAdmin');
const { scrapeSimpleCurrencie } = require('./scrapeCurrencies');
const NodeCache = require('node-cache');

const cache = new NodeCache({ stdTTL: 2100 });

exports.updateCurrencyRates = functions.pubsub
  .schedule('every 30 minutes')
  .onRun(async (context) => {
    const db = admin.firestore();
    const currenciesRef = db.collection('currencies');

    try {
      const snapshot = await currenciesRef.where('isActive', '==', true).get();
      const batch = db.batch();
      let updatesCount = 0;

      for (const doc of snapshot.docs) {
        const { code, name, symbol, exchangeRate: lastRate } = doc.data();
        const cacheKey = `USD:${code}`;

        // Verificar si los datos están en caché
        const cachedData = cache.get(cacheKey);
        if (cachedData) {
          console.log(`Usando datos en caché para USD:${code}`);
          continue;
        }

        try {
          const currencyData = await scrapeSimpleCurrencie('USD', code);

          if (currencyData && currencyData.current) {
            const newRate = parseFloat(currencyData.current);
            const updatedData = {
              code: code,
              name: name,
              symbol: symbol,
              exchangeRate: newRate,
              lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
              change: currencyData.change,
              percentChange: currencyData.percentChange
            };

            batch.update(doc.ref, updatedData);
            cache.set(cacheKey, updatedData);
            updatesCount++;
            console.log(`Actualizada tasa de cambio para USD:${code}`);
          } else {
            console.warn(`No se pudo obtener la tasa de cambio para USD:${code}`);
          }
        } catch (error) {
          console.error(`Error al obtener datos para USD:${code}:`, error);
        }
      }

      if (updatesCount > 0) {
        await batch.commit();
        console.log(`${updatesCount} tasas de cambio han sido actualizadas`);
      } else {
        console.log('No se requirieron actualizaciones');
      }
    } catch (error) {
      console.error('Error al actualizar tasas de cambio:', error);
    }

    return null;
  });

// Función HTTP para pruebas locales (opcional)
/*exports.httpUpdateCurrencyRates = functions.https.onRequest(async (req, res) => {
  await exports.updateCurrencyRates.run();
  res.send('Actualización de tasas de cambio completada');
});*/