/* eslint-disable require-jsdoc */
const axios = require("axios");
const puppeteer = require("puppeteer");
const NodeCache = require("node-cache");
const cheerio = require("cheerio");
const { createSimpleQuoteForYahoo } = require("../../models/quoteModel");

const cache = new NodeCache({ stdTTL: 3600 }); // TTL de 1 hora

/**
 * Scrapes the simple information for a quote from Google Finance.
 * @param {string} symbol - The stock symbol.
 * @see {@link createSimpleQuote}
 * @returns {object} The simple stock quote object with the current price.
 */
async function scrapeSimpleYahooQuote(symbol) {
  const urls = [
    `https://finance.yahoo.com/quote/${encodeURIComponent(symbol)}/`,
    `https://es-us.finanzas.yahoo.com/quote/${encodeURIComponent(symbol)}/`
  ];

  for (const url of urls) {
    try {
      const { data } = await axios.get(url);
      const $ = cheerio.load(data);

      // Extraer el valor actual del precio
      const current = $('fin-streamer[data-testid="qsp-price"]').attr('data-value');

      // Extraer el nombre del elemento h1 con la clase yf-xxbei9
      const name = $('h1.yf-xxbei9').text();

      // Extraer el código de la moneda
      const currencyCode = $('.exchange.yf-wk4yba span').last().text();

      // Extraer el mercado
      const market = $('.exchange.yf-wk4yba span').first().text().split("-")[0].trim();

      // Extraer el cierre anterior desde el span
      const change = $('fin-streamer[data-testid="qsp-price-change"] span').text();

      // Verificar si 'current' es un número válido
      if (isNaN(parseFloat(current))) {
        throw new Error(`El valor actual '${current}' no es un número válido.`);
      }

      // Calcular el cambio y el cambio porcentual
      const previousClose = (parseFloat(current) + parseFloat(change)).toFixed(2);
      const formattedPercentChange = `${parseFloat(change) >= 0 ? "+" : "-"}${(Math.abs(change) / previousClose * 100).toFixed(2)}%`;

      return createSimpleQuoteForYahoo(
        name,
        current,
        change,
        formattedPercentChange,
        currencyCode,
        market
      );
    } catch (error) {
      if (error.code !== 'ERR_BAD_REQUEST') {
        console.error("Error al obtener los datos:", error.message);
        throw error;
      }
      console.warn(`Error con la URL ${url}: ${error.message}. Intentando con la siguiente URL...`);
    }
  }

  throw new Error("No se pudo obtener los datos de ninguna de las URLs proporcionadas.");
}

async function getCookiesWithPuppeteer(url) {
  console.log("Iniciando Puppeteer para obtener cookies...");
  const browser = await puppeteer.launch({
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
    headless: true
  });
  const page = await browser.newPage();
  await page.goto(url, { waitUntil: 'networkidle2' });
  const cookies = await page.cookies();
  await browser.close();
  console.log("Cookies obtenidas y navegador cerrado.");
  return cookies;
}

async function fetchPriceFromYahooFinance(symbols) {
  try {
    let cookies = cache.get("yahooCookies");
    let crumbResponse = cache.get("yahooCrumb");

    if (!cookies) {
      console.log("Obteniendo nuevas cookies...");
      cookies = await getCookiesWithPuppeteer('https://finance.yahoo.com');
      cache.set("yahooCookies", cookies);
    }

    if (!crumbResponse) {
      console.log("Obteniendo nuevo crumb...");
      crumbResponse = await fetchCrumb(cookies);
      cache.set("yahooCrumb", crumbResponse);
    }

    const cookieHeader = cookies.map(cookie => `${cookie.name}=${cookie.value}`).join('; ');

    const symbolsString = symbols.join(',');
    const url = `https://query1.finance.yahoo.com/v7/finance/quote?&symbols=${symbolsString}&crumb=${crumbResponse}&formatted=false&region=US&lang=en-US`;

    console.log(`Realizando solicitud a: ${url}`);
    const response = await fetchWithHeaders(url, {
      'Cookie': cookieHeader,
      'authority': 'query1.finance.yahoo.com',
      'method': 'GET',
      'path': '/v7/finance/quote',
      'scheme': 'https',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
      'Accept-Encoding': 'gzip, deflate, br, zstd',
      'Accept-Language': 'es-US,es-419;q=0.9,es;q=0.8,en;q=0.7',
      'Cache-Control': 'max-age=0',
      'Sec-Ch-Ua': '"Chromium";v="130", "Google Chrome";v="130", "Not:A-Brand";v="99"',
      'Sec-Ch-Ua-Mobile': '?0',
      'Sec-Ch-Ua-Platform': '"Windows"',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'none',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    });

    if (response.finance && response.finance.error && response.finance.error.code === "Unauthorized") {
      console.warn('Crumb inválido, obteniendo uno nuevo...');
      crumbResponse = await fetchCrumb(cookies);
      cache.set("yahooCrumb", crumbResponse);
      return fetchPriceFromYahooFinance(symbols); // Reintentar con nuevo crumb
    }

    const data = response;

    if (data.quoteResponse && data.quoteResponse.result) {
      console.log("Datos recibidos correctamente.");
      return data.quoteResponse.result.map(quote => ({
        symbol: quote.symbol,
        market: quote.fullExchangeName,
        price: quote.regularMarketPrice,
        change: quote.regularMarketChange,
        percentChange: quote.regularMarketChangePercent,
        lastUpdated: new Date().toISOString(),
        currencyCode: quote.currency,
        name: quote.shortName || 'N/A'
      }));
    } else {
      console.warn('No se pudo obtener la información de los símbolos proporcionados');
      return null;
    }
  } catch (error) {
    if (error.response && (error.response.status === 401 || error.response.status === 400)) {
      console.warn('Error de autenticación, restableciendo cookies...');
      cache.del("yahooCookies");
      return fetchPriceFromYahooFinance(symbols); // Reintentar
    }
    console.error('Error fetching price from Yahoo Finance:', error);
    return null;
  }
}

async function fetchCrumb(cookies) {
  const cookieHeader = cookies.map(cookie => `${cookie.name}=${cookie.value}`).join('; ');
  const crumbResponse = await fetchWithHeaders('https://query2.finance.yahoo.com/v1/test/getcrumb', {
    'Cookie': cookieHeader,
    'authority': 'query2.finance.yahoo.com',
    'method': 'GET',
    'path': '/v1/test/getcrumb',
    'scheme': 'https',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'es-US,es-419;q=0.9,es;q=0.8,en;q=0.7',
    'Cache-Control': 'max-age=0',
    'Sec-Ch-Ua': '"Chromium";v="130", "Google Chrome";v="130", "Not:A-Brand";v="99"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
  });
  return crumbResponse;
}

async function fetchWithHeaders(url, headers) {
  try {
    const response = await axios.get(url, { headers });
    return response.data;
  } catch (error) {
    console.error('Error fetching data:', error);
    throw error;
  }
}

module.exports = { fetchPriceFromYahooFinance, scrapeSimpleYahooQuote };

//fetchPriceFromYahooFinance(["AAPL", "ATKR", "VUAA.L", "VUAA.DE"]);
