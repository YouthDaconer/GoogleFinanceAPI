const functions = require('firebase-functions');
const admin = require('firebase-admin');
const { scrapeSimpleQuote } = require('./scrapeQuote');
const NodeCache = require('node-cache');


const priceCache = new NodeCache({ stdTTL: 900 });
const marketHoursCache = new NodeCache({ stdTTL: 86400 });

async function getMarketHours() {
  const cachedMarketHours = marketHoursCache.get('marketHours');
  if (cachedMarketHours) {
    return cachedMarketHours;
  }

  const db = admin.firestore();
  const marketsSnapshot = await db.collection('markets').get();
  const marketHours = {};
  marketsSnapshot.forEach(doc => {
    const data = doc.data();
    marketHours[data.code] = { open: data.open, close: data.close };
  });

  marketHoursCache.set('marketHours', marketHours);
  return marketHours;
}

async function isMarketOpen(market) {
  const now = new Date();
  const utcHour = now.getUTCHours() + now.getUTCMinutes() / 60;

  const marketHours = await getMarketHours();
  const hours = marketHours[market];
  if (!hours) {
    console.warn(`Horario no definido para el mercado: ${market}`);
    return false;
  }

  return utcHour >= hours.open && utcHour < hours.close;
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

      // Verificar si el mercado está abierto
      if (!(await isMarketOpen(market))) {
        console.log(`Mercado cerrado para ${symbol}:${market}`);
        continue;
      }

      // Verificar si los datos están en caché
      const cachedData = priceCache.get(cacheKey);
      if (cachedData) {
        console.log(`Usando datos en caché para ${symbol}:${market}`);
        continue;
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
          console.log(`Actualizado precio para ${symbol}:${market}`);
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

exports.scheduledUpdatePrices = functions.pubsub
  .schedule('every 10 minutes')
  .onRun(async (context) => {
    await updateCurrentPrices();
    return null;
  });

exports.clearMarketHoursCache = functions.firestore
  .document('markets/{marketId}')
  .onWrite(async (change, context) => {
    marketHoursCache.del('marketHours');
    console.log('Caché de horarios de mercado limpiada debido a cambios en la colección markets');
    return null;
  });