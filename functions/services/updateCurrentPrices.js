const functions = require('firebase-functions');
const admin = require('firebase-admin');
const { scrapeSimpleQuote } = require('./scrapeQuote');
const NodeCache = require('node-cache');
const priceCache = new NodeCache({ stdTTL: 420 });
const marketHoursCache = new NodeCache({ stdTTL: 86400 });

async function isMarketOpen(market) {
  const now = new Date();
  const utcHour = now.getUTCHours() + now.getUTCMinutes() / 60;

  const marketHours = await getMarketHours();
  const hours = marketHours[market];
  if (!hours) {
    console.warn(`Horario no definido para el mercado: ${market}`);
    return { isOpen: false, isClosing: false };
  }

  const isOpen = utcHour >= hours.open && utcHour < hours.close;
  const isClosing = utcHour >= hours.close - 0.1 && utcHour < hours.close; // 6 minutos antes del cierre

  return { isOpen, isClosing };
}

async function updateCurrentPrices() {
  const db = admin.firestore();
  const currentPricesRef = db.collection('currentPrices');

  try {
    const snapshot = await currentPricesRef.get();
    const batch = db.batch();
    let updatesCount = 0;

    for (const doc of snapshot.docs) {
      const { symbol, market, price: lastPrice } = doc.data();
      const cacheKey = `${symbol}:${market}`;

      // Verificar si el mercado está abierto o a punto de cerrar
      const { isOpen, isClosing } = await isMarketOpen(market);
      if (!isOpen && !isClosing) {
        console.log(`Mercado cerrado para ${symbol}:${market}`);
        continue;
      }

      // Si el mercado está a punto de cerrar, forzamos la actualización ignorando la caché
      if (!isClosing) {
        // Verificar si los datos están en caché
        const cachedData = priceCache.get(cacheKey);
        if (cachedData) {
          console.log(`Usando datos en caché para ${symbol}:${market}`);
          continue;
        }
      }

      try {
        const quoteData = await scrapeSimpleQuote(symbol, market);

        if (quoteData && quoteData.current) {
          const newPrice = parseFloat(quoteData.current);
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
          priceCache.set(cacheKey, updatedData);
          updatesCount++;
          console.log(`Actualizado precio para ${symbol}:${market}${isClosing ? ' (cierre de mercado)' : ''}`);
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
}

// Función para ejecutar actualizaciones más frecuentes cerca del cierre
async function frequentUpdatesNearClose() {
  const marketHours = await getMarketHours();
  const now = new Date();
  const utcHour = now.getUTCHours() + now.getUTCMinutes() / 60 + now.getUTCSeconds() / 3600;

  for (const [market, hours] of Object.entries(marketHours)) {
    const timeToClose = hours.close - utcHour;
    if (timeToClose <= 0.0833 && timeToClose > 0) { // Últimos 5 minutos (0.0833 horas)
      console.log(`Ejecutando actualización frecuente para ${market} cerca del cierre`);
      await updateCurrentPrices();
      break;
    }
  }
}

exports.scheduledUpdatePrices = functions.pubsub
  .schedule('every 5 minutes')
  .onRun(async (context) => {
    await updateCurrentPrices();
    return null;
  });

exports.frequentUpdatesNearClose = functions.pubsub
  .schedule('every 1 minutes')
  .onRun(async (context) => {
    await frequentUpdatesNearClose();
    return null;
  });

exports.clearMarketHoursCache = functions.firestore
  .document('markets/{marketId}')
  .onWrite(async (change, context) => {
    marketHoursCache.del('marketHours');
    console.log('Caché de horarios de mercado limpiada debido a cambios en la colección markets');
    return null;
  });