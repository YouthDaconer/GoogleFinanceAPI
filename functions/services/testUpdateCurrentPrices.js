const admin = require('./firebaseAdmin');
const { getQuotes } = require('./financeQuery'); 

async function testUpdateCurrentPrices() {
  const db = admin.firestore();
  const currentPricesRef = db.collection('currentPrices');

  try {
    const snapshot = await currentPricesRef.get();
    const batch = db.batch();
    let updatesCount = 0;

    // Acumular símbolos de los documentos
    const symbols = snapshot.docs.map(doc => doc.data().symbol).join(',');

    // Obtener cotizaciones para todos los símbolos
    const quotes = await getQuotes(symbols);

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
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            name: quoteData.name || doc.data().name,
            change: quoteData.change,
            percentChange: quoteData.percentChange
          };

          batch.update(doc.ref, updatedData);
          updatesCount++;
          console.log(`Actualizado precio para ${symbol}:${market}`);
        } else {
          console.warn(`No se pudo obtener el precio para ${symbol}:${market}`);
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

// Llamar a la función de prueba
testUpdateCurrentPrices();
