const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require('./firebaseAdmin');
const requestIndicesFromFinance = require('./requestIndicesFromFinance');
const axios = require('axios');

const normalizeNumber = (value) => {
  if (!value) return null;
  return parseFloat(value.replace(/[%,]/g, ''));
};

exports.saveAllIndicesAndSectorsHistoryData = onSchedule({
  schedule: '*/10 9-17 * * 1-5',
  timeZone: 'America/New_York',
  retryCount: 3,
}, async (event) => {
  const batch = admin.firestore().batch();
  const formattedDate = new Date().toISOString().split('T')[0];
  let indicesCount = 0;
  let sectorsCount = 0;

  // Obtener y guardar índices
  try {
    const indices = await requestIndicesFromFinance();
  
    if (indices && indices.length > 0) {
      indices.forEach(index => {
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
      indicesCount = indices.length;
    } else {
      console.error('No se encontraron índices');
    }
  } catch (error) {
    console.error('Error obteniendo índices:', error.message);
  }

  // Obtener y guardar sectores (independiente de índices)
  try {
    const response = await axios.get('https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1/sectors');
    const sectors = response.data;

    // Mapeo de nombres de sectores a etfSectors
    const sectorMapping = {
      "Technology": "INFORMATION TECHNOLOGY",
      "Consumer Cyclical": "CONSUMER DISCRETIONARY",
      "Communication Services": "COMMUNICATION SERVICES",
      "Financial Services": "FINANCIALS",
      "Healthcare": "HEALTH CARE",
      "Energy": "ENERGY",
      "Consumer Defensive": "CONSUMER STAPLES",
      "Basic Materials": "MATERIALS",
      "Industrials": "INDUSTRIALS",
      "Utilities": "UTILITIES",
      "Real Estate": "REAL ESTATE"
    };

    if (sectors && sectors.length > 0) {
      sectors.forEach(sector => {
        const etfSectorName = sectorMapping[sector.sector] || sector.sector;

        const sectorData = {
          sector: sector.sector,
          dayReturn: normalizeNumber(sector.dayReturn),
          ytdReturn: normalizeNumber(sector.ytdReturn),
          yearReturn: normalizeNumber(sector.yearReturn),
          threeYearReturn: normalizeNumber(sector.threeYearReturn),
          fiveYearReturn: normalizeNumber(sector.fiveYearReturn),
          etfSectorName: etfSectorName
        };

        const sectorDocRef = admin.firestore()
          .collection('sectors')
          .doc(sector.sector);

        batch.set(sectorDocRef, sectorData, { merge: true });
      });
      sectorsCount = sectors.length;
    }
  } catch (error) {
    console.error('Error obteniendo sectores:', error.message);
  }

  // Solo hacer commit si hay datos para guardar
  if (indicesCount > 0 || sectorsCount > 0) {
    try {
      await batch.commit();
      console.log(`Datos guardados: ${indicesCount} índices y ${sectorsCount} sectores - ${formattedDate}`);
    } catch (error) {
      console.error('Error guardando en Firestore:', error.message);
    }
  } else {
    console.warn('No hay datos para guardar');
  }

  return null;
});