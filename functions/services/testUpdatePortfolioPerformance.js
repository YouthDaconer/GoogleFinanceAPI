const admin = require('./firebaseAdmin');

async function updatePortfolioPerformance(userId) {
  const db = admin.firestore();
  const userPerformanceRef = db.collection('portfolioPerformance').doc(userId);

  // ==================== Actualizar documentos principales ====================
  const datesSnapshot = await userPerformanceRef.collection('dates')
    .where('date', '>=', '2024-11-05')
    .get();

  for (const dateDoc of datesSnapshot.docs) {
    const dateData = dateDoc.data();
    const updatedData = { ...dateData }; // Conservar todos los campos

    // Iterar sobre cada moneda (COP, USD, etc.)
    Object.keys(dateData).forEach(currency => {
      if (typeof dateData[currency] === 'object' && dateData[currency].assetPerformance) {
        const currencyData = dateData[currency];
        updatedData[currency] = { ...currencyData }; // Mantener otros datos de la moneda
        updatedData[currency].assetPerformance = {};

        // Procesar cada activo en la moneda actual
        for (const [fullKey, assetData] of Object.entries(currencyData.assetPerformance)) {
          const parts = fullKey.split('_');
          if (parts.length < 3) continue;

          const market = parts.pop();
          const assetType = parts.pop();
          const name = parts.join('_');
          const newKey = `${name}_${assetType}`;

          // Sumar métricas numéricas
          if (!updatedData[currency].assetPerformance[newKey]) {
            updatedData[currency].assetPerformance[newKey] = { ...assetData };
          } else {
            Object.keys(assetData).forEach(metric => {
              if (typeof assetData[metric] === 'number') {
                updatedData[currency].assetPerformance[newKey][metric] = 
                  (updatedData[currency].assetPerformance[newKey][metric] || 0) + assetData[metric];
              }
            });
          }
        }
      }
    });

    await dateDoc.ref.set(updatedData, { merge: true });
  }

  // ==================== Actualizar cuentas individuales ====================
  const accountsSnapshot = await userPerformanceRef.collection('accounts').get();

  for (const accountDoc of accountsSnapshot.docs) {
    const accountDatesSnapshot = await accountDoc.ref.collection('dates')
      .where('date', '>=', '2024-11-05')
      .get();

    for (const accountDateDoc of accountDatesSnapshot.docs) {
      const accountDateData = accountDateDoc.data();
      const updatedAccountData = { ...accountDateData };

      // Iterar sobre cada moneda en la cuenta
      Object.keys(accountDateData).forEach(currency => {
        if (typeof accountDateData[currency] === 'object' && accountDateData[currency].assetPerformance) {
          const currencyData = accountDateData[currency];
          updatedAccountData[currency] = { ...currencyData };
          updatedAccountData[currency].assetPerformance = {};

          for (const [fullKey, assetData] of Object.entries(currencyData.assetPerformance)) {
            const parts = fullKey.split('_');
            if (parts.length < 3) continue;

            const market = parts.pop();
            const assetType = parts.pop();
            const name = parts.join('_');
            const newKey = `${name}_${assetType}`;

            if (!updatedAccountData[currency].assetPerformance[newKey]) {
              updatedAccountData[currency].assetPerformance[newKey] = { ...assetData };
            } else {
              Object.keys(assetData).forEach(metric => {
                if (typeof assetData[metric] === 'number') {
                  updatedAccountData[currency].assetPerformance[newKey][metric] = 
                    (updatedAccountData[currency].assetPerformance[newKey][metric] || 0) + assetData[metric];
                }
              });
            }
          }
        }
      });

      await accountDateDoc.ref.set(updatedAccountData, { merge: true });
    }
  }

  console.log(`Actualización completada para ${userId}`);
}

// Ejecutar
updatePortfolioPerformance('kzLG47PS5DTSU0MgcB6o3jgdETb2')
  .catch(error => console.error('Error:', error));

module.exports = { updatePortfolioPerformance };