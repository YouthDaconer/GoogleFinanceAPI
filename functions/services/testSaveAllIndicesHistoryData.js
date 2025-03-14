const admin = require('./firebaseAdmin');
const requestIndicesFromFinance = require('./requestIndicesFromFinance');

async function testSaveAllIndicesHistoryData () {
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
  
          // Datos a guardar
          const indexData = {
            name: index.name,
            code: index.code,
            region: index.region,
            score: index.value,  
            change: index.change,
            percentChange: normalizeNumber(index.percentChange),
            date: formattedDate,
            timestamp: admin.firestore.FieldValue.serverTimestamp()
          };
  
          // Referencia al documento
          const docRef = admin.firestore()
            .collection('indexHistories')
            .doc(index.code)
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
}

// Llamar a la función de prueba
testSaveAllIndicesHistoryData();
