const fetch = require('node-fetch');

async function fetchHistoricalExchangeRate(currency, date) {
  const timestamp = Math.floor(date.getTime() / 1000);
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${currency}%3DX?period1=${timestamp}&period2=${timestamp}&interval=1d`;

  try {
    const response = await fetch(url);
    const data = await response.json();

    if (data.chart.result && data.chart.result[0].indicators.quote[0].close) {
      const closePrice = data.chart.result[0].indicators.quote[0].close[0];
      return closePrice;
    } else {
      console.warn(`No se pudo obtener el tipo de cambio para ${currency} en la fecha especificada`);
      return null;
    }
  } catch (error) {
    console.error('Error al obtener el tipo de cambio histórico desde la API:', error);
    return null;
  }
}

module.exports = fetchHistoricalExchangeRate;