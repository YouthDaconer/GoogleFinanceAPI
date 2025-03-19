const functions = require('firebase-functions');
const admin = require('firebase-admin');
const { getQuotes } = require('./financeQuery'); 

async function updateCurrentPrices() {
  const db = admin.firestore();
  const currentPricesRef = db.collection('currentPrices');

  try {
    const snapshot = await currentPricesRef.get();
    const batch = db.batch();
    let updatesCount = 0;

    const symbols = snapshot.docs.map(doc => doc.data().symbol);
    const batchSize = 50;
    for (let i = 0; i < symbols.length; i += batchSize) {
      const symbolBatch = symbols.slice(i, i + batchSize).join(',');

      // Obtener cotizaciones para el lote de símbolos
      const quotes = await getQuotes(symbolBatch);

      if (quotes) {
        const quotesMap = new Map(quotes.map(quote => [quote.symbol, quote]));

        for (const doc of snapshot.docs) {
          const { symbol, market } = doc.data();

          const quoteData = quotesMap.get(symbol);

          if (quoteData && quoteData.price) {
            const newPrice = parseFloat(quoteData.price);
            const updatedData = {
              symbol: symbol,
              market: market,
              price: newPrice,
              lastUpdated: Date.now(),
              name: quoteData.name || doc.data().name,
              change: quoteData.change,
              percentChange: quoteData.percentChange,
            };

            // Lista de campos adicionales a agregar si están en quoteData
            const optionalKeys = [
              'logo', 'open', 'high', 'low',
              'yearHigh', 'yearLow', 'volume', 'avgVolume',
              'marketCap', 'beta', 'pe', 'eps',
              'earningsDate', 'industry', 'sector', 'about', 'employees', 
              'dividend', 'exDividend', 'yield', 'dividendDate',
              'threeMonthReturn', 'sixMonthReturn', 'ytdReturn', 
              'threeYearReturn', 'yearReturn', 'fiveYearReturn', 
              'currency', 'currencySymbol', 'exchangeName'
            ];

            optionalKeys.forEach(key => {
              if (quoteData[key] !== null && quoteData[key] !== undefined) {
                updatedData[key] = quoteData[key];
              }
            });

            batch.update(doc.ref, updatedData);
            updatesCount++;
            console.log(`Actualizado precio para ${symbol}:${market}`);
          } else {
            console.warn(`No se pudo obtener el precio para ${symbol}:${market}`);
          }
        }
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

// Programar la actualización cada 5 minutos
exports.scheduledUpdatePrices = functions.pubsub
  .schedule('*/2 9-17 * * 1-5')
  .timeZone('America/New_York')
  .onRun(async (context) => {
    await updateCurrentPrices();
    return null;
  });