const functions = require('firebase-functions');
const admin = require('./firebaseAdmin');
const scrapeIndicesByCountry = require('./scrapeIndicesByCountry');

exports.saveSp500HistoryData = functions.pubsub
  .schedule('0 17 * * 1-5')
  .timeZone('America/New_York')
  .onRun(async (context) => {
    try {
      // Realizar el scraping
      const indices = await scrapeIndicesByCountry('americas', 'US');

      // Encontrar el S&P 500 en los resultados
      const sp500 = indices.find(index => index.name === 'S&P 500');

      if (!sp500) {
        console.error('No se encontraron datos para el S&P 500');
        return null;
      }

      // Normalizar los datos
      const score = parseFloat(sp500.score.replace(/[,\s]/g, ''));
      const percentChange = parseFloat(sp500.percentChange.replace('%', ''));

      if (isNaN(score) || isNaN(percentChange)) {
        console.error('Error al convertir score o percentChange a número');
        return null;
      }

      // Obtener la fecha actual
      const now = new Date();
      const formattedDate = now.toISOString().split('T')[0]; // YYYY-MM-DD

      // Referencia al documento en Firestore
      const docRef = admin.firestore()
        .collection('indexHistories')
        .doc('.INX')
        .collection('dates')
        .doc(formattedDate);

      // Guardar los datos en Firestore
      await docRef.set({
        date: formattedDate,
        score: score,
        percentChange: percentChange
      }, { merge: true });

      console.log(`Datos del S&P 500 guardados exitosamente para la fecha ${formattedDate}`);
    } catch (error) {
      console.error('Error al guardar los datos del S&P 500:', error);
    }

    return null;
  });