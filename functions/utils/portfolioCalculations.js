// Definiciones de modelos
// Nota: En JavaScript, estas son solo para referencia y documentación

/**
 * @typedef {Object} Asset
 * @property {string} id
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
 * @property {string} defaultCurrencyForAdquisitionDollar
 */

/**
 * @typedef {Object} CurrentPrice
 * @property {string} symbol
 * @property {string} market
 * @property {number} price
 */

/**
 * @typedef {Object} Currency
 * @property {string} code
 * @property {number} exchangeRate
 */

/**
 * @typedef {Object} Transaction
 * @property {string} id
 * @property {string} assetId
 * @property {string} type
 * @property {number} amount
 * @property {number} price
 * @property {string} currency
 * @property {string} date
 * @property {string} portfolioAccountId
 * @property {number} commission
 * @property {string} assetType
 * @property {number} dollarPriceToDate
 * @property {string} defaultCurrencyForAdquisitionDollar
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
 * @param {string} defaultCurrency
 * @param {number} [acquisitionDollarValue]
 * @returns {number}
 */
const convertCurrency = (amount, fromCurrency, toCurrency, currencies, defaultCurrency, acquisitionDollarValue) => {
  const fromRate = currencies.find(c => c.code === fromCurrency)?.exchangeRate || 1;
  const toRate = currencies.find(c => c.code === toCurrency)?.exchangeRate || 1;

  if (fromCurrency === 'USD' && toCurrency === defaultCurrency && acquisitionDollarValue) {
    return amount * acquisitionDollarValue;
  }
  return (amount * toRate) / fromRate;
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
 * @param {Currency[]} currencies
 * @param {Object.<string, {totalValue: number, assetPerformance: Object.<string, {totalValue: number}>}>} totalValueYesterday
 * @param {Transaction[]} todaysTransactions
 * @param {Dividend[]} [todaysDividends=[]]
 * @returns {Object.<string, {totalInvestment: number, totalValue: number, totalROI: number, dailyReturn: number, monthlyReturn: number, annualReturn: number, dailyChangePercentage: number, adjustedDailyChangePercentage: number, assetPerformance: Object.<string, AssetPerformance>}>}
 */
const calculateAccountPerformance = (assets, currentPrices, currencies, totalValueYesterday, todaysTransactions, todaysDividends = []) => {
  const performanceByCurrency = {};

  // Group assets by name, assetType, and market
  const groupedAssets = assets.reduce((acc, asset) => {
    const key = `${asset.name}_${asset.assetType}_${asset.market}`;
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(asset);
    return acc;
  }, {});

  for (const currency of currencies) {
    let totalInvestment = 0;
    let totalValue = 0;
    let totalCashFlow = 0;
    let totalDividends = 0;
    const assetPerformance = {};
    
    // Acumular transacciones y dividendos convertidos para la moneda actual
    const convertedTransactions = todaysTransactions.map(t => ({
      amount: convertCurrency(
        t.type === 'buy' ? (-t.amount * t.price) : (t.amount * t.price),
        t.currency,
        currency.code,
        currencies,
        t.defaultCurrencyForAdquisitionDollar,
        parseFloat(t.dollarPriceToDate.toString())
      )
    }));

    const convertedDividends = todaysDividends.map(d => ({
      amount: convertCurrency(d.amount, d.currency, currency.code, currencies)
    }));

    for (const [groupKey, groupAssets] of Object.entries(groupedAssets)) {
      let groupInvestment = 0;
      let groupValue = 0;
      let groupUnits = 0;
      let groupCashFlow = 0;
      let groupDividends = 0;
      const groupReturns = {
        dailyReturns: [],
        monthlyReturns: [],
        yearlyReturns: [],
        dailyWeights: [],
        monthlyWeights: [],
        yearlyWeights: [],
        totalMonthlyInvestment: 0,
        totalYearlyInvestment: 0
      };

      // Acumular transacciones y dividendos para el grupo
      const groupTransactions = [];
      const groupDividendsList = [];

      for (const asset of groupAssets) {
        const currentPrice = currentPrices.find(cp => cp.symbol === asset.name && cp.market === asset.market)?.price || 0;
        const assetValueUSD = currentPrice * asset.units;

        const initialInvestmentUSD = asset.unitValue * asset.units;
        const assetInvestment = convertCurrency(initialInvestmentUSD, 'USD', currency.code, currencies, asset.defaultCurrencyForAdquisitionDollar, asset.acquisitionDollarValue);
        const assetValue = convertCurrency(assetValueUSD, 'USD', currency.code, currencies);

        groupInvestment += assetInvestment;
        groupValue += assetValue;
        groupUnits += parseFloat(asset.units);

        // Acumular transacciones para el grupo
        const assetTransactions = todaysTransactions.filter(t => t.assetId === asset.id);
        assetTransactions.forEach(t => {
          const convertedAmount = convertCurrency(
            t.type === 'buy' ? (-t.amount * t.price) : (t.amount * t.price),
            t.currency,
            currency.code,
            currencies,
            t.defaultCurrencyForAdquisitionDollar,
            parseFloat(t.dollarPriceToDate.toString())
          );

          groupTransactions.push({
            amount: convertedAmount
          });

          // Calcular flujo de efectivo para este activo
          groupCashFlow += convertedAmount;
        });

        // Acumular dividendos para el grupo
        if (todaysDividends.length > 0) {
          const assetDividends = todaysDividends.filter(d => d.assetId === asset.id);
          groupDividendsList.push(...assetDividends.map(d => ({
            amount: convertCurrency(d.amount, d.currency, currency.code, currencies)
          })));
          const assetDividendsTotal = assetDividends.reduce((sum, d) => {
            return sum + convertCurrency(d.amount, d.currency, currency.code, currencies);
          }, 0);
          groupDividends += assetDividendsTotal;
        }

        const daysSinceAcquisition = calculateDaysInvested(asset.acquisitionDate);
        const roi = (assetValue - assetInvestment) / assetInvestment;
        const dailyReturn = daysSinceAcquisition > 0 ? Math.pow(1 + roi, 1 / daysSinceAcquisition) - 1 : 0;
        const monthlyReturn = daysSinceAcquisition >= 30 ? Math.pow(1 + dailyReturn, 30) - 1 : 0;
        const yearlyReturn = daysSinceAcquisition >= 365 ? Math.pow(1 + dailyReturn, 365) - 1 : 0;

        groupReturns.dailyReturns.push(dailyReturn);
        groupReturns.dailyWeights.push(assetInvestment);

        if (daysSinceAcquisition >= 30 && monthlyReturn > 0) {
          groupReturns.monthlyReturns.push(monthlyReturn);
          groupReturns.monthlyWeights.push(assetInvestment);
          groupReturns.totalMonthlyInvestment += assetInvestment;
        }
        if (daysSinceAcquisition >= 365 && yearlyReturn > 0) {
          groupReturns.yearlyReturns.push(yearlyReturn);
          groupReturns.yearlyWeights.push(assetInvestment);
          groupReturns.totalYearlyInvestment += assetInvestment;
        }
      }

      totalInvestment += groupInvestment;
      totalValue += groupValue;
      totalCashFlow += groupCashFlow;
      totalDividends += groupDividends;

      const groupROI = (groupValue - groupInvestment) / groupInvestment;

      const dailyWeightedReturn = groupInvestment > 0
        ? groupReturns.dailyReturns.reduce((sum, ret, idx) => sum + ret * (groupReturns.dailyWeights[idx] / groupInvestment), 0)
        : 0;

      let monthlyWeightedReturn = 0;
      if (groupReturns.totalMonthlyInvestment > 0) {
        for (let idx = 0; idx < groupReturns.monthlyReturns.length; idx++) {
          const weight = groupReturns.monthlyWeights[idx] / groupReturns.totalMonthlyInvestment;
          if (!isNaN(weight)) {
            monthlyWeightedReturn += groupReturns.monthlyReturns[idx] * weight;
          }
        }
      }

      let yearlyWeightedReturn = 0;
      if (groupReturns.totalYearlyInvestment > 0) {
        for (let idx = 0; idx < groupReturns.yearlyReturns.length; idx) {
          const weight = groupReturns.yearlyWeights[idx] / groupReturns.totalYearlyInvestment;
          if (!isNaN(weight)) {
            yearlyWeightedReturn += groupReturns.yearlyReturns[idx] * weight;
          }
        }
      }

      const groupDailyChangePercentage = calculateDailyChangePercentage(groupValue, totalValueYesterday[currency.code]?.[groupKey]?.totalValue || 0);

      // Calcular adjusted daily change percentage usando las transacciones acumuladas
      const groupAdjustedDailyChangePercentage = calculatePureReturnWithoutCashflows(
        totalValueYesterday[currency.code]?.[groupKey]?.totalValue || 0,
        groupValue,
        groupTransactions,
        groupDividendsList
      );

      assetPerformance[groupKey] = {
        totalInvestment: groupInvestment,
        totalValue: groupValue,
        totalROI: groupROI * 100,
        dailyReturn: dailyWeightedReturn * 100,
        monthlyReturn: monthlyWeightedReturn * 100,
        annualReturn: yearlyWeightedReturn * 100,
        dailyChangePercentage: groupDailyChangePercentage,
        adjustedDailyChangePercentage: groupAdjustedDailyChangePercentage,
        totalCashFlow: groupCashFlow,
        units: groupUnits
      };
    }

    const maxDaysInvested = calculateMaxDaysInvested(assets);
    const { totalROI, dailyReturn, monthlyReturn, annualReturn } = calculateTotalROIAndReturns(totalInvestment, totalValue, maxDaysInvested);
    const dailyChangePercentage = calculateDailyChangePercentage(totalValue, totalValueYesterday[currency.code]?.totalValue || 0);
    
    // Calcular adjusted daily change percentage para toda la cartera usando el método Modified Dietz
    const adjustedDailyChangePercentage = calculatePureReturnWithoutCashflows(
      totalValueYesterday[currency.code]?.totalValue || 0,
      totalValue,
      convertedTransactions,
      convertedDividends
    );

    performanceByCurrency[currency.code] = {
      totalInvestment,
      totalValue,
      totalROI,
      dailyReturn,
      monthlyReturn,
      annualReturn,
      dailyChangePercentage,
      adjustedDailyChangePercentage,
      assetPerformance,
      totalCashFlow
    };
  }

  return performanceByCurrency;
};

const calculateDailyChangePercentage = (currentValue, previousValue) => {
  if (previousValue === 0) return 0;
  return ((currentValue - previousValue) / previousValue) * 100;
};

const calculatePureReturnWithoutCashflows = (startValue, endValue, cashFlows, dividends = []) => {
  let totalCashFlow = 0;

  cashFlows.forEach(cf => {
    totalCashFlow += cf.amount;
  });

  const totalDividends = dividends.reduce((sum, div) => sum + div.amount, 0);

  const denominator = startValue;
  if (denominator === 0) {
    return 0;
  }

  const pureReturn = (endValue - startValue + totalCashFlow + totalDividends) / denominator;
  return pureReturn * 100;
};


const calculateDaysInvested = (acquisitionDate) => {
  const approximateNewYorkTime = (date) => {
    return new Date(date.getTime() - 4 * 60 * 60 * 1000);
  };

  const setToMidnightNY = (date) => {
    const nyDate = approximateNewYorkTime(date);
    nyDate.setHours(0, 0, 0, 0);
    return nyDate;
  };

  const acquisition = setToMidnightNY(new Date(acquisitionDate));
  const today = setToMidnightNY(new Date());
  return Math.floor((today - acquisition) / (1000 * 60 * 60 * 24));
};

// Función para calcular el máximo de días invertidos (sin cambios)
const calculateMaxDaysInvested = (assets) => {
  const approximateNewYorkTime = (date) => {
    return new Date(date.getTime() - 4 * 60 * 60 * 1000);
  };

  const setToMidnightNY = (date) => {
    const nyDate = approximateNewYorkTime(date);
    nyDate.setHours(0, 0, 0, 0);
    return nyDate;
  };

  const activeAssets = assets.filter(asset => asset.isActive);
  const now = setToMidnightNY(new Date());

  if (activeAssets.length === 0) return 0;

  return Math.max(...activeAssets.map(asset => {
    const acquisitionDate = setToMidnightNY(new Date(asset.acquisitionDate));
    return Math.floor((now - acquisitionDate) / (1000 * 60 * 60 * 24));
  }));
};

module.exports = {
  convertCurrency,
  calculateMaxDaysInvested,
  calculateDaysInvested,
  calculateTotalROIAndReturns,
  calculateAccountPerformance,
  calculateDailyChangePercentage,
  calculatePureReturnWithoutCashflows
};