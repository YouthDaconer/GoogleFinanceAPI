// Definiciones de modelos
// Nota: En JavaScript, estas son solo para referencia y documentación

/**
 * @typedef {Object} Asset
 * @property {string} [id]
 * @property {string} name
 * @property {string} acquisitionDate
 * @property {string} company
 * @property {string} assetType
 * @property {string} portfolioAccount
 * @property {string} currency
 * @property {number} unitValue
 * @property {number} units
 * @property {number} acquisitionDollarValue
 * @property {boolean} isActive
 * @property {string} market
 * @property {number} commission
 */

/**
 * @typedef {Object} Currency
 * @property {string} [id]
 * @property {string} code
 * @property {string} name
 * @property {string} symbol
 * @property {number} exchangeRate
 * @property {boolean} isActive
 */

/**
 * @typedef {Object} CurrentPrice
 * @property {string} [id]
 * @property {string} symbol
 * @property {string} market
 * @property {number} price
 * @property {string} lastUpdated
 * @property {string} [name]
 * @property {string} [change]
 * @property {string} [percentChange]
 */

/**
 * @typedef {Object} AssetPerformance
 * @property {number} totalInvestment
 * @property {number} totalValue
 * @property {number} totalROI
 * @property {number} dailyReturn
 * @property {number} monthlyReturn
 * @property {number} annualReturn
 * @property {number} dailyChangePercentage
 * @property {number} units
 */

/**
 * @param {number} amount
 * @param {string} fromCurrency
 * @param {string} toCurrency
 * @param {Currency[]} currencies
 * @param {number} [acquisitionDollarValue]
 * @returns {number}
 */
const convertCurrency = (amount, fromCurrency, toCurrency, currencies, acquisitionDollarValue) => {
  const fromRate = currencies.find(c => c.code === fromCurrency)?.exchangeRate || 1;
  const toRate = currencies.find(c => c.code === toCurrency)?.exchangeRate || 1;

  if (fromCurrency === 'USD' && toCurrency !== 'USD' && acquisitionDollarValue) {
    return amount * acquisitionDollarValue;
  }
  return (amount * toRate) / fromRate;
};

/**
 * @param {Asset[]} assets
 * @returns {number}
 */
const calculateMaxDaysInvested = (assets) => {
  const activeAssets = assets.filter(asset => asset.isActive);
  const now = new Date();

  if (activeAssets.length === 0) return 0;

  return Math.max(...activeAssets.map(asset => {
    const acquisitionDate = new Date(asset.acquisitionDate);
    const adjustedAcquisition = new Date(acquisitionDate.getTime() - acquisitionDate.getTimezoneOffset() * 60000);
    adjustedAcquisition.setHours(0, 0, 0, 0);
    const adjustedNow = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
    adjustedNow.setHours(0, 0, 0, 0);
    return Math.floor((adjustedNow.getTime() - adjustedAcquisition.getTime()) / (1000 * 60 * 60 * 24));
  }));
};

/**
 * @param {number} totalInvestment
 * @param {number} totalValue
 * @param {number} maxDaysInvested
 * @returns {{totalROI: number, dailyReturn: number, monthlyReturn: number, annualReturn: number}}
 */
const calculateTotalROIAndReturns = (totalInvestment, totalValue, maxDaysInvested) => {
  const totalROI = totalInvestment > 0 ? ((totalValue - totalInvestment) / totalInvestment) * 100 : 0;
  const dailyReturn = totalROI > 0 ? (Math.pow(1 + totalROI / 100, 1 / maxDaysInvested) - 1) * 100 : 0;
  const monthlyReturn = totalROI > 0 && maxDaysInvested >= 30 ? (Math.pow(1 + dailyReturn / 100, 30) - 1) * 100 : 0;
  const annualReturn = totalROI > 0 && maxDaysInvested >= 365 ? (Math.pow(1 + dailyReturn / 100, 365) - 1) * 100 : 0;

  return { totalROI, dailyReturn, monthlyReturn, annualReturn };
};

/**
 * @param {number} totalValue
 * @param {number} totalValueYesterday
 * @returns {number}
 */
const calculateDailyChangePercentage = (totalValue, totalValueYesterday) => {
  if (totalValueYesterday === 0) return 0;
  return ((totalValue - totalValueYesterday) / totalValueYesterday) * 100;
};

/**
 * @param {Asset[]} assets
 * @param {CurrentPrice[]} currentPrices
 * @param {Currency[]} currencies
 * @param {Object.<string, number>} totalValueYesterday
 * @returns {Object.<string, {totalInvestment: number, totalValue: number, totalROI: number, dailyReturn: number, monthlyReturn: number, annualReturn: number, dailyChangePercentage: number, assetPerformance: Object.<string, AssetPerformance>}>}
 */
function calculateAccountPerformance(assets, currentPrices, currencies, totalValueYesterday) {
  const performanceByCurrency = {};

  for (const currency of currencies) {
    let totalInvestment = 0;
    let totalValue = 0;
    const assetPerformance = {};

    for (const asset of assets) {
      const currentPrice = currentPrices.find(cp => cp.symbol === asset.name)?.price || 0;
      const assetValueUSD = currentPrice * asset.units;

      const initialInvestmentUSD = asset.unitValue * asset.units;
      const assetInvestment = convertCurrency(initialInvestmentUSD, 'USD', currency.code, currencies, asset.acquisitionDollarValue);
      const assetValue = convertCurrency(assetValueUSD, 'USD', currency.code, currencies);

      totalInvestment += assetInvestment;
      totalValue += assetValue;

      const assetMaxDaysInvested = calculateMaxDaysInvested([asset]);
      const assetROI = calculateTotalROIAndReturns(assetInvestment, assetValue, assetMaxDaysInvested);
      const assetDailyChangePercentage = calculateDailyChangePercentage(assetValue, totalValueYesterday[currency.code] ? (totalValueYesterday[currency.code][asset.name]?.totalValue || 0) : 0);

      assetPerformance[asset.name] = {
        totalInvestment: assetInvestment,
        totalValue: assetValue,
        ...assetROI,
        dailyChangePercentage: assetDailyChangePercentage,
        units: asset.units
      };
    }

    const maxDaysInvested = calculateMaxDaysInvested(assets);
    const { totalROI, dailyReturn, monthlyReturn, annualReturn } = calculateTotalROIAndReturns(totalInvestment, totalValue, maxDaysInvested);
    const dailyChangePercentage = calculateDailyChangePercentage(totalValue, totalValueYesterday[currency.code]?.totalValue || 0);

    performanceByCurrency[currency.code] = {
      totalInvestment,
      totalValue,
      totalROI,
      dailyReturn,
      monthlyReturn,
      annualReturn,
      dailyChangePercentage,
      assetPerformance
    };
  }

  return performanceByCurrency;
}

module.exports = {
  convertCurrency,
  calculateMaxDaysInvested,
  calculateTotalROIAndReturns,
  calculateDailyChangePercentage,
  calculateAccountPerformance
};