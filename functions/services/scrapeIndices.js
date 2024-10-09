

/* eslint-disable require-jsdoc */
const axios = require("axios");
const cheerio = require("cheerio");
const { createStockIndex } = require("../models/indexModel");
/**
 *
 * @param {string} region Either "americas", "europe-middle-east-africa", or "asia-pacific"
 * @returns {Promise<Array>}  An array of the region's indices
 */
async function scrapeIndices(region) {
  //build url
  const url = "https://www.google.com/finance/markets/indexes/" + region;
  //fetch data
  const { data } = await axios.get(url);
  //load data into cheerio
  const $ = cheerio.load(data);
  //arrays to store data
  const stockIndex = [];
  const indexNames = [];
  const scores = [];
  const changes = [];
  const percentageChanges = [];

  //scrape names
  $(".ZvmM7").each(function (i, element) {
    indexNames.push($(element).text());
  });

  //scrape scores (current index value)
  $(".xVyTdb .YMlKec ").each(function (i, element) {
    scores.push($(element).text());
  });

  //scrape changes
  $(".xVyTdb .SEGxAb .P2Luy").each(function (i, element) {
    changes.push($(element).text());
  });

  //scrape percentage changes
  $(".xVyTdb .JwB6zf").each(function (i, element) {
    percentageChanges.push($(element).text());
  });

  for (let i = 0; i < indexNames.length; i++) {
    percentageChanges[i] = changes[i].includes("-") ? "-" + percentageChanges[i] : "+" + percentageChanges[i];
    stockIndex.push(
      createStockIndex(
        indexNames[i],
        scores[i],
        changes[i],
        percentageChanges[i]
      )
    );
  }

  return stockIndex;
}

module.exports = scrapeIndices;
