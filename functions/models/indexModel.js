

/**
 * Creates json model for stock index
 * @param name Name of index
 * @param score Current value of index
 * @param change Change in value of index
 * @param percentChange Percentage change in value of index
 * @returns {Object} JSON model for stock index
 */
function createStockIndex(name, score, change, percentChange) {
  return {
    name: name,
    score: score,
    change,
    change,
    percentChange: percentChange,
  };
}

/**
 * Array of indices for the U.S. stock market
 */
const indicesUS = [
  "S&P 500",
  "Dow Jones Industrial Average",
  "Nasdaq Composite",
  "Russell 2000 Index",
  "NYSE Composite",
  "Dow Jones Transportation Average",
  "Dow Jones Utility Average",
  "Russell 1000 Index",
  "Dow Jones U.S. Total Stock Market Index",
  "Barron's 400 Index",
  "NASDAQ Composite Total Return",
  "Nasdaq-100",
  "NASDAQ-100 Total Return",
  "NASDAQ Transportation Index",
  "NASDAQ Biotechnology Index",
  "Nasdaq Financial-100",
  "Nasdaq Bank",
  "Nasdaq Insurance",
  "Nasdaq Industrial",
  "Nasdaq Computer",
  "S&P 400",
  "S&P 600",
  "Russell 2500 Index",
  "Russell 3000 Index",
  "NYSE American Composite Index",
  "Value Line Geometric Index",
  "NYSE Arca Biotechnology Index",
  "NYSE Arca Pharmaceutical Index",
  "KBW Nasdaq Bank Index",
  "Philadelphia Gold and Silver Index",
  "PHLX Oil Service Sector",
  "PHLX Semiconductor Sector"
];

module.exports = { createStockIndex, indicesUS };
