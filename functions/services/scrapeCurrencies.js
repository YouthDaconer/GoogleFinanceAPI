

/* eslint-disable require-jsdoc */
const axios = require("axios");
const cheerio = require("cheerio");
const { createSimpleQuote } = require("../models/quoteModel");


/**
 * Scrapes the simple information for a currencie from Google Finance.
 * @param {string} origin - The stock origin.
 * @param {string} target - The stock target.
 * @see {@link createSimpleQuote}
 * @returns {object} The simple stock currencie object with the current price.
 */
async function scrapeSimpleCurrencie(origin, target) {
  const url = `https://www.google.com/finance/quote/${origin}-${target}`;
  console.log("Probando " + url);
  const { data } = await axios.get(url);
  const $ = cheerio.load(data);
  const dataArray = $(".P6K39c").text().split("$");
  const name = $(".zzDege").text();
  const current = $(".YMlKec.fxKbKc").text().replace("$", "").split("$")[0];
  const previousClose = dataArray[1];
  
  // No procesamos 'current' aquí, lo devolvemos como string
  return createSimpleQuote(
    name,
    current, // Devolvemos el valor sin procesar
    null,    // Cambio a null porque no podemos calcularlo aquí
    null     // Porcentaje de cambio a null por la misma razón
  );
}

module.exports = { scrapeSimpleCurrencie };
