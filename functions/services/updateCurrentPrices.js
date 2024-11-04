const functions = require('firebase-functions');
const admin = require('firebase-admin');
const { scrapeSimpleQuote } = require('./scrapeQuote');
const NodeCache = require('node-cache');

const priceCache = new NodeCache({ stdTTL: 420 });
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

function isWeekday(date) {
  const day = date.getUTCDay();
  return day >= 1 && day <= 5; // 1 (Lunes) a 5 (Viernes)
}

async function isAnyMarketOpen() {
  const now = new Date();
  if (!isWeekday(now)) return false;

  const utcHour = now.getUTCHours() + now.getUTCMinutes() / 60;
  const marketHours = await getMarketHours();

  return Object.values(marketHours).some(hours =>
    utcHour >= hours.open && utcHour < hours.close
  );
}

async function isMarketOpen(market) {
  const now = new Date();
  if (!isWeekday(now)) return { isOpen: false, isClosing: false };

  const utcHour = now.getUTCHours() + now.getUTCMinutes() / 60;

  const marketHours = await getMarketHours();
  const hours = marketHours[market];
  if (!hours) {
    console.warn(`Horario no definido para el mercado: ${market}`);
    return { isOpen: false, isClosing: false };
  }

  const isOpen = utcHour >= hours.open && utcHour < hours.close;
  const isClosing = utcHour >= hours.close - 0.0833 && utcHour < hours.close; // 5 minutos antes del cierre

  return { isOpen, isClosing };
}

async function updateCurrentPrices() {
  if (!(await isAnyMarketOpen())) {
    console.log('Ningún mercado está abierto. No se realizarán actualizaciones.');
    return;
  }

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
            percentChange: quoteData.percentChange,
            currencySymbol: quoteData.currencySymbol,
            currencyCode: quoteData.currencyCode
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

function convertDecimalToTime(decimal) {
  const hours = Math.floor(decimal);
  const minutes = Math.round((decimal - hours) * 60);
  return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`;
}

async function getMarketOpenCloseTimes() {
  const marketHours = await getMarketHours();
  const times = new Set();
  Object.values(marketHours).forEach(hours => {
    times.add(typeof hours.open === 'number' ? hours.open : parseFloat(hours.open));
    times.add(typeof hours.close === 'number' ? hours.close : parseFloat(hours.close));
  });
  return Array.from(times).sort((a, b) => a - b);
}

async function scheduleMarketUpdates() {
  const times = await getMarketOpenCloseTimes();

  times.forEach(time => {
    const timeString = convertDecimalToTime(time);
    const [hours, minutes] = timeString.split(':').map(Number);
    const cronExpression = `${minutes} ${hours} * * 1-5`;

    exports[`updatePricesAt_${hours}_${minutes}`] = functions.pubsub
      .schedule(cronExpression)
      .timeZone('UTC')
      .onRun(async (context) => {
        await updateCurrentPrices();
        return null;
      });

    console.log(`Scheduled update for ${timeString} UTC`);
  });
}

// Programar la actualización cada 5 minutos durante el horario de mercado
exports.scheduledUpdatePrices = functions.pubsub
  .schedule('*/5 * * * 1-5')
  .timeZone('America/New_York')
  .onRun(async (context) => {
    await updateCurrentPrices();
    return null;
  });

exports.clearMarketHoursCache = functions.firestore
  .document('markets/{marketId}')
  .onWrite(async (change, context) => {
    marketHoursCache.del('marketHours');
    console.log('Caché de horarios de mercado limpiada debido a cambios en la colección markets');
    await scheduleMarketUpdates(); // Reprogramar las actualizaciones de mercado
    return null;
  });

// Inicializar las funciones programadas para las actualizaciones de mercado
scheduleMarketUpdates();