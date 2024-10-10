

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
  const url = `https://www.google.com/finance/quote/${origin}:${target}`;
  console.log("Probando " + url)
  const { data } = await axios.get(url);
  console.log(data)
  const $ = cheerio.load(data);
  const dataArray = $(".P6K39c").text().split("$");

  const name = $(".zzDege").text();
  const current = $(".YMlKec.fxKbKc").text().replace("$", "").split("$")[0];
  const previousClose = dataArray[1];
  const change = (current - previousClose).toFixed(2);
  const percentChange = `${change >= 0 ? "+" : "-"}${((Math.abs(change) / previousClose) * 100).toFixed(2)}%`;
  return createSimpleQuote(
    name,
    current,
    change,
    percentChange
  )
}

module.exports = { scrapeSimpleCurrencie };
