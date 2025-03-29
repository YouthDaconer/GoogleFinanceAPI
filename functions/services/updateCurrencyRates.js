const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require('./firebaseAdmin');
const axios = require('axios');

// Horarios estáticos para NYSE (en UTC)
const NYSE_OPEN_HOUR = 13.5;  // 9:30 AM EST
const NYSE_CLOSE_HOUR = 20;   // 4:00 PM EST


/**
 * Obtiene la tasa de cambio actual de una moneda usando Yahoo Finance
 * @param {string} currencyCode - Código de la moneda a consultar
 * @return {Promise<number|null>} - Retorna la tasa de cambio o null si hay error
 */
async function getCurrencyRateFromYahoo(currencyCode) {
  try {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${currencyCode}=X?lang=en-US&region=US`;
    console.log(`Consultando tasa para ${currencyCode} en Yahoo Finance: ${url}`);
    
    const { data } = await axios.get(url);
    
    // Verificar si hay resultados y meta datos en la respuesta
    if (data?.chart?.result?.[0]?.meta) {
      const meta = data.chart.result[0].meta;
      return meta.regularMarketPrice || null;
    }
    
    return null;
  } catch (error) {
    console.error(`Error al obtener tasa para ${currencyCode} desde Yahoo Finance:`, error.message);
    return null;
  }
}

function isNYSEMarketOpen() {
  const now = new Date();
  const utcHour = now.getUTCHours() + now.getUTCMinutes() / 60;
  return utcHour >= NYSE_OPEN_HOUR && utcHour < NYSE_CLOSE_HOUR;
}

// Actualización para usar Firebase Functions v5+
exports.updateCurrencyRates = onSchedule({
  schedule: '*/2 9-17 * * 1-5',
  timeZone: 'America/New_York',
  retryCount: 3,
}, async (event) => {
  if (!isNYSEMarketOpen()) {
    console.log('El mercado NYSE está cerrado. No se actualizarán las tasas de cambio.');
    return null;
  }

  const db = admin.firestore();
  const currenciesRef = db.collection('currencies');

  try {
    const snapshot = await currenciesRef.where('isActive', '==', true).get();
    const batch = db.batch();
    let updatesCount = 0;

    for (const doc of snapshot.docs) {
      const { code, name, symbol } = doc.data();

      try {
        // Obtener tasa de cambio desde Yahoo Finance
        const newRate = await getCurrencyRateFromYahoo(code);

        if (newRate && !isNaN(newRate) && newRate > 0) {
          const updatedData = {
            code: code,
            name: name,
            symbol: symbol,
            exchangeRate: newRate,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp()
          };

          batch.update(doc.ref, updatedData);
          updatesCount++;
          console.log(`Actualizada tasa de cambio para USD:${code} a ${newRate}`);
        } else {
          console.warn(`Valor inválido para USD:${code}: ${newRate}`);
        }
      } catch (error) {
        console.error(`Error al obtener datos para USD:${code}:`, error);
      }
    }

    if (updatesCount > 0) {
      await batch.commit();
      console.log(`${updatesCount} tasas de cambio han sido actualizadas`);
    } else {
      console.log('No se requirieron actualizaciones');
    }
  } catch (error) {
    console.error('Error al actualizar tasas de cambio:', error);
  }

  return null;
});

// Función HTTP para pruebas locales (opcional)
/*exports.httpUpdateCurrencyRates = functions.https.onRequest(async (req, res) => {
  if (isNYSEMarketOpen()) {
    await exports.updateCurrencyRates.run();
    res.send('Actualización de tasas de cambio completada');
  } else {
    res.send('El mercado NYSE está cerrado. No se actualizaron las tasas de cambio.');
  }
});*/