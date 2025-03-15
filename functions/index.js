require('dotenv').config();
const { onRequest } = require("firebase-functions/v2/https");
const express = require("express");
const rateLimit = require("express-rate-limit");
const YAML = require("yamljs");
const swaggerDocument = YAML.load("./swagger.yaml");
const swaggerUi = require("swagger-ui-express");
const scrapeIndices = require("./services/scrapeIndices");
const scrapeIndicesByCountry = require("./services/scrapeIndicesByCountry");
const cors = require('cors');
const { scrapeFullQuote, scrapeSimpleQuote, } = require("./services/scrapeQuote");
const { fetchPriceFromYahooFinance } = require("./services/yahoo/scrapeQuote");
const { scrapeActiveStock } = require("./services/scrapeActiveStock");
const { scrapeSimpleCurrencie } = require("./services/scrapeCurrencies");
const { scrapeGainers } = require("./services/scrapeGainers");
const { scrapeLosers } = require("./services/scrapeLosers");
const { scrapeNews } = require("./services/scrapeNews");
const { saveAllIndicesAndSectorsHistoryData } = require("./services/saveAllIndicesAndSectorsHistoryData");
const updateCurrencyRates = require('./services/updateCurrencyRates');
const calcDailyPortfolioPerf = require('./services/calculateDailyPortfolioPerformance');
const { scheduledUpdatePrices, clearMarketHoursCache } = require('./services/updateCurrentPrices');
const fetchHistoricalExchangeRate = require('./services/fetchHistoricalExchangeRate');
const { getQuotes, getSimpleQuotes } = require('./services/financeQuery');

const app = express();
const port = 3100;

corsOptions = {
  origin: ["https://portafolio-inversiones.web.app", "https://portafolio-inversiones.firebaseapp.com", "http://localhost:3000", "http://localhost:3001"],
  optionsSuccessStatus: 200
};

app.use(cors(corsOptions));

const demoApiKey = "demo";
const demoApiLimiter = rateLimit({
  windowMs: 60000,
  max: 10,
  message: "Too many requests from this IP, please try again after a minute"
});

app.use((req, res, next) => {
  let apiKey = req.get('X-API-Key');
  if (!apiKey) {
    apiKey = demoApiKey;
    req.headers['x-api-key'] = demoApiKey;
    demoApiLimiter(req, res, next);
  } else if (apiKey === process.env.API_KEY) {
    next();
  } else {
    demoApiLimiter(req, res, next);
  }
});

app.use("/api-docs", swaggerUi.serve, swaggerUi.setup(swaggerDocument));

app.get("/indices", async (req, res) => {
  const { region, country } = req.query;
  if (!region) {
    res.status(400).json({
      error: "Por favor, proporcione el parámetro de consulta de región (americas, europe-middle-east-africa, o asia-pacific)",
    });
    return;
  }
  try {
    let stockIndex;
    if (country) {
      stockIndex = await scrapeIndicesByCountry(region, country);
    } else {
      stockIndex = await scrapeIndices(region);
    }
    res.status(200).json(stockIndex);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "Ocurrió un error al raspar el sitio web: " + error.message,
    });
  }
});

app.get("/fullQuote", async (req, res) => {
  const { symbol, exchange } = req.query;
  try {
    if (!symbol || !exchange) {
      res.status(400).json({
        error: "Por favor, proporcione ambos parámetros de consulta: símbolo y bolsa",
      });
      return;
    }
    const fullQuote = await scrapeFullQuote(symbol, exchange);
    res.status(200).json(fullQuote);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "Ocurrió un error al buscar la acción: " + error.message,
    });
  }
});

app.get("/quote", async (req, res) => {
  const { symbol, exchange } = req.query;
  try {
    if (!symbol || !exchange) {
      res.status(400).json({
        error: "Por favor, proporcione ambos parámetros de consulta: símbolo y bolsa",
      });
      return;
    }
    const fullQuote = await scrapeSimpleQuote(symbol, exchange);
    res.status(200).json(fullQuote);
  } catch (error) {
    console.error(error);
    if (error.message.includes("no es un número válido")) {
      res.status(400).json({
        error: "Datos inválidos devueltos por el API: " + error.message,
      });
    } else {
      res.status(500).json({
        error: "Ocurrió un error al buscar la acción: " + error.message,
      });
    }
  }
});

app.get("/apiQuote", async (req, res) => {
  const { symbols } = req.query;
  try {
    if (!symbols) {
      res.status(400).json({
        error: "Por favor, proporcione el parámetro de consulta de símbolo",
      });
      return;
    }

    // Convertir symbols a un array de strings
    const symbolsArray = symbols.split(',').map(s => s.trim());

    // Verificar si el array es válido
    if (symbolsArray.length === 0 || symbolsArray.some(s => s === "")) {
      throw new Error("Formato de símbolos inválido");
    }

    const fullQuote = await fetchPriceFromYahooFinance(symbolsArray);
    res.status(200).json(fullQuote);
  } catch (error) {
    console.error(error);
    if (error.message === "Formato de símbolos inválido") {
      res.status(400).json({
        error: "Formato de símbolos inválido. Por favor, proporcione una lista de símbolos separados por comas.",
      });
    } else {
      res.status(500).json({
        error: "Ocurrió un error al buscar la acción: " + error.message,
      });
    }
  }
});

app.get("/currencie", async (req, res) => {
  const { origin, target } = req.query;
  try {
    if (!origin || !target) {
      res.status(400).json({
        error: "Por favor, proporcione ambos parámetros de consulta: origen y destino",
      });
      return;
    }
    const currencie = await scrapeSimpleCurrencie(origin, target);
    res.status(200).json(currencie);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "Ocurrió un error al buscar la moneda: " + error.message,
    });
  }
});

app.get("/active", async (req, res) => {
  try {
    const activeStocks = await scrapeActiveStock();
    res.status(200).json(activeStocks);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "Ocurrió un error al raspar el sitio web: " + error.message,
    });
  }
});

app.get("/gainers", async (req, res) => {
  try {
    const gainers = await scrapeGainers();
    res.status(200).json(gainers);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "Ocurrió un error al raspar el sitio web: " + error.message,
    });
  }
});

app.get("/losers", async (req, res) => {
  try {
    const losers = await scrapeLosers();
    res.status(200).json(losers);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "Ocurrió un error al raspar el sitio web: " + error.message,
    });
  }
});

app.get("/news", async (req, res) => {
  const { symbol, exchange } = req.query;
  try {
    if (!symbol || !exchange) {
      res.status(400).json({
        error: "Por favor, proporcione ambos parámetros de consulta: símbolo y bolsa",
      });
      return;
    }
    const news = await scrapeNews(symbol, exchange);
    res.status(200).json(news);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "Ocurrió un error al buscar la acción: " + error.message,
    });
  }
});

app.get("/api/historicalExchangeRate", async (req, res) => {
  const { currency, date } = req.query;

  if (!currency || !date) {
    res.status(400).json({
      error: 'Faltan parámetros requeridos: currency y date',
    });
    return;
  }

  try {
    const dateObj = new Date(date);
    const exchangeRate = await fetchHistoricalExchangeRate(currency, dateObj);

    if (exchangeRate !== null) {
      res.status(200).json({ exchangeRate });
    } else {
      res.status(404).json({
        error: `No se pudo obtener el tipo de cambio para ${currency} en la fecha especificada`,
      });
    }
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: 'Error al obtener el tipo de cambio histórico desde la API',
    });
  }
});

app.get('/quotes', async (req, res) => {
  const { symbols } = req.query;
  try {
    if (!symbols) {
      res.status(400).json({
        error: 'Por favor, proporcione el parámetro de consulta de símbolos',
      });
      return;
    }

    const quotes = await getQuotes(symbols);
    res.status(200).json(quotes);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: 'Ocurrió un error al obtener las cotizaciones: ' + error.message,
    });
  }
});

app.get('/simple-quotes', async (req, res) => {
  const { symbols } = req.query;
  try {
    if (!symbols) {
      res.status(400).json({
        error: 'Por favor, proporcione el parámetro de consulta de símbolos',
      });
      return;
    }

    const simpleQuotes = await getSimpleQuotes(symbols);
    res.status(200).json(simpleQuotes);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: 'Ocurrió un error al obtener las cotizaciones simplificadas: ' + error.message,
    });
  }
});

app.listen(port, () => {
  console.log("Server running on http://localhost:" + port);
});

exports.app = onRequest(app);
exports.updateCurrencyRates = updateCurrencyRates.updateCurrencyRates;
exports.scheduledUpdatePrices = scheduledUpdatePrices;
exports.clearMarketHoursCache = clearMarketHoursCache;
exports.saveAllIndicesAndSectorsHistoryData = saveAllIndicesAndSectorsHistoryData;
exports.calcDailyPortfolioPerf = calcDailyPortfolioPerf;