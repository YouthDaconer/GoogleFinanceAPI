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

    if (fromCurrency === 'USD' && toCurrency === 'COP' && acquisitionDollarValue) {
        return amount * acquisitionDollarValue;
    }
    return (amount * toRate) / fromRate;
};

/**
 * @param {Asset[]} assets
 * @param {CurrentPrice[]} currentPrices
 * @param {string} selectedCurrency
 * @param {Currency[]} currencies
 * @returns {{totalInvestment: number, totalValue: number}}
 */
const calculateTotalInvestmentAndValue = (assets, currentPrices, selectedCurrency, currencies) => {
    const activeAssets = assets.filter(asset => asset.isActive);

    const totalInvestment = activeAssets.reduce((sum, asset) => {
        const convertedValue = convertCurrency(asset.unitValue * asset.units, asset.currency, selectedCurrency, currencies, asset.acquisitionDollarValue);
        return sum + convertedValue;
    }, 0);

    const totalValue = activeAssets.reduce((sum, asset) => {
        const currentPrice = currentPrices.find(cp => cp.symbol === asset.name)?.price || 0;
        const convertedValue = convertCurrency(currentPrice * asset.units, asset.currency, selectedCurrency, currencies, asset.acquisitionDollarValue);
        return sum + convertedValue;
    }, 0);

    return { totalInvestment, totalValue };
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
 * @param {Asset[]} assets
 * @param {CurrentPrice[]} currentPrices
 * @param {string} selectedCurrency
 * @param {Currency[]} currencies
 * @returns {{totalInvestment: number, totalValue: number, totalROI: number, dailyReturn: number, monthlyReturn: number, annualReturn: number}}
 */
const calculatePerformanceForPortfolioSummary = (assets, currentPrices, selectedCurrency, currencies) => {
    const activeAssets = assets.filter(asset => asset.isActive);

    const { totalInvestment, totalValue } = calculateTotalInvestmentAndValue(activeAssets, currentPrices, selectedCurrency, currencies);
    const maxDaysInvested = calculateMaxDaysInvested(activeAssets);
    const { totalROI, dailyReturn, monthlyReturn, annualReturn } = calculateTotalROIAndReturns(totalInvestment, totalValue, maxDaysInvested);

    return {
        totalInvestment,
        totalValue,
        totalROI,
        dailyReturn,
        monthlyReturn,
        annualReturn
    };
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
 * @returns {Object.<string, {totalInvestment: number, totalValue: number, totalROI: number, dailyReturn: number, monthlyReturn: number, annualReturn: number, dailyChangePercentage: number}>}
 */
const calculateAccountPerformance = (assets, currentPrices, currencies, totalValueYesterday) => {
    const performanceByCurrency = {};

    for (const currency of currencies) {
        const { totalInvestment, totalValue } = calculateTotalInvestmentAndValue(assets, currentPrices, currency.code, currencies);
        const maxDaysInvested = calculateMaxDaysInvested(assets);
        const { totalROI, dailyReturn, monthlyReturn, annualReturn } = calculateTotalROIAndReturns(totalInvestment, totalValue, maxDaysInvested);
        const dailyChangePercentage = calculateDailyChangePercentage(totalValue, totalValueYesterday[currency.code] || 0);

        performanceByCurrency[currency.code] = {
            totalInvestment,
            totalValue,
            totalROI,
            dailyReturn,
            monthlyReturn,
            annualReturn,
            dailyChangePercentage
        };
    }

    return performanceByCurrency;
};

module.exports = {
    calculatePerformanceForPortfolioSummary,
    convertCurrency,
    calculateTotalInvestmentAndValue,
    calculateMaxDaysInvested,
    calculateTotalROIAndReturns,
    calculateDailyChangePercentage,
    calculateAccountPerformance
};