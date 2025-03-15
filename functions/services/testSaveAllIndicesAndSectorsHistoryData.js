const admin = require('./firebaseAdmin');
const requestIndicesFromFinance = require('./requestIndicesFromFinance');
const axios = require('axios');

const normalizeNumber = (value) => {
  if (!value) return null;
  return parseFloat(value.replace(/[%,]/g, ''));
};

async function testSaveAllIndicesAndSectorsHistoryData() {
  try {
    const indices = await requestIndicesFromFinance();

    if (!indices || indices.length === 0) {
      console.error('No se encontraron índices');
      return null;
    }

    // Obtener datos de sectores
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

    const batch = admin.firestore().batch();
    const formattedDate = new Date().toISOString().split('T')[0];

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

    // Guardar datos de sectores
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

    await batch.commit();
    console.log(`Datos guardados: ${indices.length} índices y ${sectors.length} sectores - ${formattedDate}`);

  } catch (error) {
    console.error('Error:', error);
  }

  return null;
}

// Llamar a la función de prueba
testSaveAllIndicesAndSectorsHistoryData();
