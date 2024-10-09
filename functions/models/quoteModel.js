

/**
 * Creates a full stock quote object.
 * @param {string} name - The name of the stock.
 * @param {number} previousClose - The previous closing price of the stock.
 * @param {number} change - The change in value of the stock.
 * @param {number} percentChange - The percentage change in the stock price.
 * @param {number} current - The current price of the stock.
 * @param {number} aftermarketValue - The aftermarket value of the stock. Null if market is closed.
 * @param {number} high - The highest price of the stock for the day.
 * @param {number} low - The lowest price of the stock for the day.
 * @param {number} avgVolume - The average trading volume of the stock.
 * @param {number} marketCap - The market capitalization of the stock.
 * @param {number} peRatio - The price-to-earnings ratio of the stock.
 * @param {number} week52High - The highest price of the stock in the past 52 weeks.
 * @param {number} week52Low - The lowest price of the stock in the past 52 weeks.
 * @param {number} dividendYield - The dividend yield of the stock.
 * @param {number} change - The change in the stock price.
 * @param {number} percentChange - The percentage change in the stock price.
 * @param {string} about - Information about the stock.
 * @param {number} employees - The number of employees in the company.
 * @param {string} quarter - The quarter of the income statements.
 * @param {number} quarterlyRevenue - The quarterly revenue of the company.
 * @param {number} quarterlyNetIncome - The quarterly net income of the company.
 * @param {number} quarterlyEPS - The earnings per share of the company.
 * @returns {object} The full stock quote object with all the details.
 */
function createFullStockQuote(
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
    quarterlyEPS,
) {
    return {  
        name,
        previousClose,
        change,
        aftermarketValue,
        percentChange,
        current,
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
        quarterlyEPS,
    };
}
/**
 * Creates a simple stock quote object.
 * @param {string} name - The name of the stock.
 * @param {string} current - The current price of the stock.
 * @param {string} change - The change in value of the stock.
 * @param {string} percentChange - The percentage change in the stock price.
 * @returns {object} The simple stock quote object with the basic details.
 */
function createSimpleQuote(
    name,
    current,
    change,
    percentChange
) {
    return {
        name,
        current,
        change,
        percentChange
    };
}

module.exports = {createFullStockQuote, createSimpleQuote};
