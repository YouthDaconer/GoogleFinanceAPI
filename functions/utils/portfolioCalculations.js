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
 * @returns {Object.<string, {totalInvestment: number, totalValue: number, totalROI: number, dailyReturn: number, monthlyReturn: number, annualReturn: number, dailyChangePercentage: number, adjustedDailyChangePercentage: number, assetPerformance: Object.<string, AssetPerformance>}>}
 */
const calculateAccountPerformance = (assets, currentPrices, currencies, totalValueYesterday, todaysTransactions) => {
  const performanceByCurrency = {};

  // Group assets by name and assetType
  const groupedAssets = assets.reduce((acc, asset) => {
    const key = `${asset.name}_${asset.assetType}`;
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(asset);
    return acc;
  }, {});

  // Separar transacciones por tipo
  const buyTransactions = todaysTransactions.filter(t => t.type === 'buy');
  const sellTransactions = todaysTransactions.filter(t => t.type === 'sell');
  const dividendTransactions = todaysTransactions.filter(t => t.type === 'dividendPay');

  for (const currency of currencies) {
    let totalInvestment = 0;
    let totalValue = 0;
    let totalCashFlow = 0;
    let totalDividends = 0;
    const assetPerformance = {};
    
    // Convertir transacciones para la moneda actual
    const convertedBuyTransactions = buyTransactions.map(t => ({
      amount: convertCurrency(
        -t.amount * t.price,
        t.currency,
        currency.code,
        currencies,
        t.defaultCurrencyForAdquisitionDollar,
        parseFloat(t.dollarPriceToDate.toString())
      )
    }));

    const convertedSellTransactions = sellTransactions.map(t => ({
      amount: convertCurrency(
        t.amount * t.price,
        t.currency,
        currency.code,
        currencies,
        t.defaultCurrencyForAdquisitionDollar,
        parseFloat(t.dollarPriceToDate.toString())
      )
    }));

    const convertedDividends = dividendTransactions.map(d => ({
      amount: convertCurrency(
        d.amount,
        d.currency,
        currency.code,
        currencies,
        d.defaultCurrencyForAdquisitionDollar,
        parseFloat(d.dollarPriceToDate.toString())
      )
    }));

    // Combinar todas las transacciones para el flujo de caja total
    const convertedTransactions = [
      ...convertedBuyTransactions,
      ...convertedSellTransactions,
      ...convertedDividends
    ];

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
        const currentPrice = currentPrices.find(cp => cp.symbol === asset.name)?.price || 0;
        const assetValueUSD = currentPrice * asset.units;

        const initialInvestmentUSD = asset.unitValue * asset.units;
        const assetInvestment = convertCurrency(initialInvestmentUSD, 'USD', currency.code, currencies, asset.defaultCurrencyForAdquisitionDollar, asset.acquisitionDollarValue);
        const assetValue = convertCurrency(assetValueUSD, 'USD', currency.code, currencies);

        groupInvestment += assetInvestment;
        groupValue += assetValue;
        groupUnits += parseFloat(asset.units);

        // Acumular transacciones para el grupo
        const assetBuyTransactions = buyTransactions.filter(t => t.assetId === asset.id);
        const assetSellTransactions = sellTransactions.filter(t => t.assetId === asset.id);
        
        assetBuyTransactions.forEach(t => {
          const convertedAmount = convertCurrency(
            -t.amount * t.price,
            t.currency,
            currency.code,
            currencies,
            t.defaultCurrencyForAdquisitionDollar,
            parseFloat(t.dollarPriceToDate.toString())
          );

          groupTransactions.push({
            amount: convertedAmount
          });
          
          groupCashFlow += convertedAmount;
        });

        assetSellTransactions.forEach(t => {
          const convertedAmount = convertCurrency(
            t.amount * t.price,
            t.currency,
            currency.code,
            currencies,
            t.defaultCurrencyForAdquisitionDollar,
            parseFloat(t.dollarPriceToDate.toString())
          );

          groupTransactions.push({
            amount: convertedAmount
          });
          
          groupCashFlow += convertedAmount;
        });

        // Acumular dividendos para el grupo (desde transacciones de tipo dividendPay)
        const assetDividends = dividendTransactions.filter(d => d.assetId === asset.id);
        assetDividends.forEach(d => {
          const convertedAmount = convertCurrency(
            d.amount,
            d.currency,
            currency.code,
            currencies,
            d.defaultCurrencyForAdquisitionDollar,
            parseFloat(d.dollarPriceToDate.toString())
          );

          groupDividendsList.push({
            amount: convertedAmount
          });
          
          groupDividends += convertedAmount;
        });

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
        for (let idx = 0; idx < groupReturns.yearlyReturns.length; idx++) {
          const weight = groupReturns.yearlyWeights[idx] / groupReturns.totalYearlyInvestment;
          if (!isNaN(weight)) {
            yearlyWeightedReturn += groupReturns.yearlyReturns[idx] * weight;
          }
        }
      }

      // Obtener datos del día anterior para este grupo
      const previousGroupData = totalValueYesterday[currency.code]?.[groupKey] || {};
      const previousGroupUnits = previousGroupData.units || 0;
      
      // Determinar si es una nueva inversión después de vender todo
      const isNewInvestment = groupUnits > 0 && previousGroupUnits === 0;
      
      const groupDailyChangePercentage = calculateDailyChangePercentage(groupValue, previousGroupData?.totalValue || 0);

      // Calcular adjusted daily change percentage usando las transacciones acumuladas y dividendos
      // Pasar isNewInvestment para resetear el cálculo cuando es una nueva inversión
      const groupAdjustedDailyChangePercentage = calculatePureReturnWithoutCashflows(
        previousGroupData?.totalValue || 0,
        groupValue,
        groupTransactions,
        groupDividendsList,
        isNewInvestment
      );
      
      // Añadir un nuevo campo para el cambio porcentual sin flujos de caja
      const groupRawDailyChangePercentage = calculateRawDailyChange(
        previousGroupData?.totalValue || 0,
        groupValue
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
        rawDailyChangePercentage: groupRawDailyChangePercentage,
        totalCashFlow: groupCashFlow,
        units: groupUnits
      };
    }

    // Verificar si hay activos en el portafolio
    const hasAnyAssets = totalValue > 0;
    
    // Determinar si el portafolio completo es una nueva inversión
    // (si tenemos activos ahora pero no teníamos ayer)
    const isPortfolioNewInvestment = hasAnyAssets && 
                                     (!totalValueYesterday[currency.code] || 
                                      totalValueYesterday[currency.code].totalValue === 0);
    
    const maxDaysInvested = calculateMaxDaysInvested(assets);
    const { totalROI, dailyReturn, monthlyReturn, annualReturn } = calculateTotalROIAndReturns(totalInvestment, totalValue, maxDaysInvested);
    const dailyChangePercentage = calculateDailyChangePercentage(totalValue, totalValueYesterday[currency.code]?.totalValue || 0);
    
    // Calcular adjusted daily change percentage para toda la cartera usando el método Modified Dietz
    // Pasar isPortfolioNewInvestment para resetear el cálculo cuando el portafolio es nuevo
    const adjustedDailyChangePercentage = calculatePureReturnWithoutCashflows(
      totalValueYesterday[currency.code]?.totalValue || 0,
      totalValue,
      convertedTransactions,
      convertedDividends,
      isPortfolioNewInvestment
    );
    
    // Añadir un nuevo campo para el cambio porcentual sin flujos de caja para toda la cartera
    const rawDailyChangePercentage = calculateRawDailyChange(
      totalValueYesterday[currency.code]?.totalValue || 0,
      totalValue
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
      rawDailyChangePercentage,
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

const calculatePureReturnWithoutCashflows = (startValue, endValue, cashFlows, dividends = [], isNewInvestment = false) => {
  // Si es una nueva inversión después de haber vendido todas las posiciones anteriores,
  // el rendimiento debe ser 0% en el primer día
  if (isNewInvestment) {
    return 0;
  }

  let totalCashFlow = 0;

  cashFlows.forEach(cf => {
    totalCashFlow += cf.amount;
  });

  const totalDividends = dividends.reduce((sum, div) => sum + div.amount, 0);

  // Si el valor inicial es 0 pero hay flujos de caja (como compras recientes)
  if (startValue === 0 && (totalCashFlow !== 0 || endValue > 0)) {
    // Si es una compra reciente (cashFlow negativo), el rendimiento sería 
    // la diferencia entre el valor final y la cantidad invertida, dividido por la cantidad invertida
    if (totalCashFlow < 0) {
      // Tomamos el valor absoluto de totalCashFlow porque es negativo para compras
      const invested = Math.abs(totalCashFlow);
      return ((endValue - invested) / invested) * 100;
    }
    // Si es una venta completa (cashFlow positivo), usar el cashflow como base
    else if (totalCashFlow > 0) {
      return 0; // Ya se vendió todo, así que no hay rendimiento para calcular
    }
    // Si no hay cashflow pero hay valor final (caso extremadamente raro)
    return 0;
  }

  const denominator = startValue;
  if (denominator === 0) {
    return 0;
  }

  const pureReturn = (endValue - startValue + totalCashFlow + totalDividends) / denominator;
  return pureReturn * 100;
};

// Nueva función para calcular el retorno sin considerar flujos de caja
const calculateRawDailyChange = (startValue, endValue) => {
  const denominator = startValue;
  if (denominator === 0) {
    return 0;
  }
  
  return ((endValue - startValue) / denominator) * 100;
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
  calculatePureReturnWithoutCashflows,
  calculateRawDailyChange
};