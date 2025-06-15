const admin = require('../services/firebaseAdmin');
const { FieldValue } = admin.firestore;

async function forceDeleteLegacyKeys(userId) {
  const db = admin.firestore();
  const userRef = db.collection('portfolioPerformance').doc(userId);

  // ============== Eliminar en documentos principales ==============
  const datesSnapshot = await userRef.collection('dates')
    .where('date', '>=', '2024-11-05')
    .get();

  for (const dateDoc of datesSnapshot.docs) {
    const dateData = dateDoc.data();
    const updatePayload = {};

    // Iterar por cada moneda (COP, USD, etc.)
    Object.keys(dateData).forEach(currency => {
      const currencyData = dateData[currency];
      if (!currencyData?.assetPerformance) return;

      const legacyKeys = [];
      Object.keys(currencyData.assetPerformance).forEach(key => {
        if (key.split('_').length === 3) {
          legacyKeys.push(key);
        }
      });

      // Construir rutas Firestore para eliminación
      if (legacyKeys.length > 0) {
        legacyKeys.forEach(key => {
          updatePayload[`${currency}.assetPerformance.${key}`] = FieldValue.delete();
        });
      }
    });

    // Ejecutar actualización masiva si hay claves para eliminar
    if (Object.keys(updatePayload).length > 0) {
      await dateDoc.ref.update(updatePayload);
      console.log(`Documento ${dateDoc.id}: Eliminadas ${Object.keys(updatePayload).length} claves.`);
    }
  }

  // ============== Eliminar en cuentas individuales ==============
  const accountsSnapshot = await userRef.collection('accounts').get();

  for (const accountDoc of accountsSnapshot.docs) {
    const accountDatesSnapshot = await accountDoc.ref.collection('dates')
      .where('date', '>=', '2024-11-05')
      .get();

    for (const accountDateDoc of accountDatesSnapshot.docs) {
      const accountDateData = accountDateDoc.data();
      const accountUpdatePayload = {};

      Object.keys(accountDateData).forEach(currency => {
        const currencyData = accountDateData[currency];
        if (!currencyData?.assetPerformance) return;

        const legacyKeys = [];
        Object.keys(currencyData.assetPerformance).forEach(key => {
          if (key.split('_').length === 3) {
            legacyKeys.push(key);
          }
        });

        if (legacyKeys.length > 0) {
          legacyKeys.forEach(key => {
            accountUpdatePayload[`${currency}.assetPerformance.${key}`] = FieldValue.delete();
          });
        }
      });

      if (Object.keys(accountUpdatePayload).length > 0) {
        await accountDateDoc.ref.update(accountUpdatePayload);
        console.log(`Cuenta ${accountDoc.id} - Fecha ${accountDateDoc.id}: Eliminadas ${Object.keys(accountUpdatePayload).length} claves.`);
      }
    }
  }

  console.log(`✅ Eliminación forzada completada para ${userId}. Verifica Firestore.`);
}

// Ejecutar
forceDeleteLegacyKeys('kzLG47PS5DTSU0MgcB6o3jgdETb2')
  .catch(error => console.error("🔥 Error crítico:", error));

module.exports = { forceDeleteLegacyKeys };