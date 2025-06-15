const admin = require('../services/firebaseAdmin');
const { FieldValue } = admin.firestore;

async function updateVUAAKeys(userId) {
  const db = admin.firestore();
  const userRef = db.collection('portfolioPerformance').doc(userId);

  // ==================== Actualizar documentos principales ====================
  const datesSnapshot = await userRef.collection('dates')
    .where('date', '>=', '2024-10-25')
    .where('date', '<=', '2024-11-04')
    .get();

  for (const dateDoc of datesSnapshot.docs) {
    const dateData = dateDoc.data();
    let hasChanges = false;

    // Recorrer cada moneda
    Object.keys(dateData).forEach(currency => {
      const currencyData = dateData[currency];
      if (!currencyData?.assetPerformance) return;

      const assetPerf = currencyData.assetPerformance;
      const newAssetPerf = {};

      // Modificar las claves necesarias
      Object.keys(assetPerf).forEach(oldKey => {
        if (oldKey.startsWith('VUAA_')) {
          const newKey = oldKey.replace('VUAA_', 'VUAA.L_');
          newAssetPerf[newKey] = assetPerf[oldKey];
          hasChanges = true;
        } else {
          newAssetPerf[oldKey] = assetPerf[oldKey];
        }
      });

      // Reemplazar el objeto completo solo si hay cambios
      if (hasChanges) {
        dateData[currency].assetPerformance = newAssetPerf;
      }
    });

    // Actualizar el documento completo con los cambios
    if (hasChanges) {
      await dateDoc.ref.update(dateData);
      console.log(`Documento ${dateDoc.id}: Actualizadas las claves VUAA → VUAA.L`);
    }
  }

  // ==================== Actualizar cuentas individuales ====================
  const accountsSnapshot = await userRef.collection('accounts').get();

  for (const accountDoc of accountsSnapshot.docs) {
    const accountDatesSnapshot = await accountDoc.ref.collection('dates')
      .where('date', '>=', '2024-10-25')
      .where('date', '<=', '2024-11-04')
      .get();

    for (const accountDateDoc of accountDatesSnapshot.docs) {
      const accountDateData = accountDateDoc.data();
      let hasAccountChanges = false;

      Object.keys(accountDateData).forEach(currency => {
        const currencyData = accountDateData[currency];
        if (!currencyData?.assetPerformance) return;

        const assetPerf = currencyData.assetPerformance;
        const newAssetPerf = {};

        Object.keys(assetPerf).forEach(oldKey => {
          if (oldKey.startsWith('VUAA_')) {
            const newKey = oldKey.replace('VUAA_', 'VUAA.L_');
            newAssetPerf[newKey] = assetPerf[oldKey];
            hasAccountChanges = true;
          } else {
            newAssetPerf[oldKey] = assetPerf[oldKey];
          }
        });

        if (hasAccountChanges) {
          accountDateData[currency].assetPerformance = newAssetPerf;
        }
      });

      if (hasAccountChanges) {
        await accountDateDoc.ref.update(accountDateData);
        console.log(`Cuenta ${accountDoc.id} - Fecha ${accountDateDoc.id}: Actualizadas las claves VUAA → VUAA.L`);
      }
    }
  }

  console.log(`✅ VUAA → VUAA.L actualizado para ${userId}`);
}

// Ejecutar
updateVUAAKeys('DDeR8P5hYgfuN8gcU4RsQfdTJqx2')
  .catch(error => console.error("🔥 Error:", error));

module.exports = { updateVUAAKeys };