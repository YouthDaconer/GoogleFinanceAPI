

/* eslint-disable require-jsdoc */
const axios = require("axios");
const cheerio = require("cheerio");
const moment = require("moment");
const { createFullStockQuote, createSimpleQuote } = require("../models/quoteModel");

/**
 * Scrapes complex data for a quote from Google Finance.
 *  @param {string} symbol - The stock symbol.
 *  @param {string} exchange - The stock exchange.
 *  @see {@link createFullStockQuote}
 *  @returns {object} The full stock quote object with all the details.
 */

async function scrapeFullQuote(symbol, exchange) {
  const url = `https://www.google.com/finance/quote/${symbol}:${exchange}`;
  const { data } = await axios.get(url);
  const $ = cheerio.load(data);

  const values = $(".YMlKec.fxKbKc").text().replace("$", "").split("$");
  const dataArray = $(".P6K39c").text().split("$");

  const name = $(".zzDege").text();
  const current = values[0];
  let aftermarketValue = "N/A";
  if (moment().isAfter(moment().hour(16).minute(0).second(0))) {
    aftermarketValue = values[1];
  }
  const previousClose = dataArray[1];
  const change = (current - previousClose).toFixed(2);
  const percentChange = `${change >= 0 ? "+" : "-"}${((Math.abs(change) / previousClose) * 100).toFixed(2)}%`;
  const low = dataArray[2].split("-")[0].trim();
  const high = dataArray[3];
  const week52Low = dataArray[4].split("-")[0].trim();
  const week52High = dataArray[5].match(/\d+\.\d{2}/)[0];
  const marketCap = dataArray[5].split(week52High)[1].match(/\d+\.\d{2}(T|B|M) USD/)[0];
  const avgVolume = dataArray[5].match(/\d+\.\d{2}M/)[0];
  const peRatio = dataArray[5].split(avgVolume)[1].match(/\d+\.\d{2}/)[0];
  const dividendYield = dataArray[5].match(/(\b\d+\.\d{2}%\b)/)?.[0]?.substring(2) ?? "N/A";
  const employees = dataArray[5].match(/(\d+,?\d*)$/)[1];
  const about = $(".bLLb2d").text().replace(/\n/g, ' ').replace(/Wikipedia/g, '').trim();
  const quarter = $(".yNnsfe").text().match(/\(USD\)(\w+\s\d{4})/)[1];
  const quarterlyRevenue = $('tr:contains("Revenue") .QXDnM').text();
  const quarterlyNetIncome = $('tr:contains("Net income") .QXDnM').text().match(/(\d+\.\d+)([BM]?)?/g)[0];
  const eps = $('tr:contains("Earnings per share") .QXDnM').text();
  return createFullStockQuote(
    name,
    previousClose,
    change,
    percentChange,
    current,
    aftermarketValue,
    high,
    low,
    avgVolume,
    marketCap,
    peRatio,
    week52High,
    week52Low,
    dividendYield,
    change,
    percentChange,
    about,
    employees,
    quarter,
    quarterlyRevenue,
    quarterlyNetIncome,
    eps
  );
}

/**
 * Scrapes the simple information for a quote from Google Finance.
 * @param {string} symbol - The stock symbol.
 * @param {string} exchange - The stock exchange.
 * @param {string} currencySymbol - The currency symbol.
 * @see {@link createSimpleQuote}
 * @returns {object} The simple stock quote object with the current price.
 */
async function scrapeSimpleQuote(symbol, exchange) {
  const url = `https://www.google.com/finance/quote/${symbol}:${exchange}`;
  console.log("Probando " + url);
  const { data } = await axios.get(url);
  console.log(data);
  const $ = cheerio.load(data);

  const rawData = $(".P6K39c").text();
  const currencySymbol = rawData.charAt(0);
  const dataArray = rawData.split(currencySymbol);

  const name = $(".zzDege").text();
  const current = $(".YMlKec.fxKbKc").text().replace(currencySymbol, "").split(currencySymbol)[0];
  
  // Verificar si 'current' es un número válido
  if (isNaN(parseFloat(current))) {
    throw new Error(`El valor actual '${current}' no es un número válido.`);
  }

  const previousClose = dataArray[1];
  const change = (current - previousClose).toFixed(2);
  const percentChange = `${change >= 0 ? "+" : "-"}${((Math.abs(change) / previousClose) * 100).toFixed(2)}%`;
  
  return createSimpleQuote(
    name,
    current,
    change,
    percentChange,
    currencySymbol
  );
}

module.exports = { scrapeFullQuote, scrapeSimpleQuote };
