const functions = require('firebase-functions');
const admin = require('./firebaseAdmin');
const requestIndicesFromFinance = require('./requestIndicesFromFinance');

exports.saveAllIndicesHistoryData = functions.pubsub
  .schedule('*/10 9-16 * * 1-5')
  .timeZone('America/New_York')
  .onRun(async (context) => {
    try {
      const indices = await requestIndicesFromFinance();

      if (!indices || indices.length === 0) {
        console.error('No se encontraron índices');
        return null;
      }

      const batch = admin.firestore().batch();
      const formattedDate = new Date().toISOString().split('T')[0];

      indices.forEach(index => {
        // Función de normalización
        const normalizeNumber = (value) => {
          if (!value) return null;
          return parseFloat(value.replace(/[%,]/g, ''));
        };

        // Datos a guardar en la colección principal
        const generalData = {
          name: index.name,
          code: index.code,
          region: index.region,
        };

        // Referencia al documento de información general
        const generalDocRef = admin.firestore()
          .collection('indexHistories')
          .doc(index.code);

        // Guardar información general
        batch.set(generalDocRef, generalData, { merge: true });

        // Datos específicos a guardar en la subcolección 'dates'
        const indexData = {
          score: index.value,
          change: index.change,
          percentChange: normalizeNumber(index.percentChange),
          date: formattedDate,
          timestamp: Date.now()
        };

        // Referencia al documento en la subcolección 'dates'
        const docRef = generalDocRef
          .collection('dates')
          .doc(formattedDate);

        batch.set(docRef, indexData, { merge: true });
      });

      await batch.commit();
      console.log(`Datos guardados: ${indices.length} índices - ${formattedDate}`);

    } catch (error) {
      console.error('Error:', error);
    }

    return null;
  });