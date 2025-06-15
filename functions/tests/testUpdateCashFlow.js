const admin = require('../services/firebaseAdmin');
const { convertCurrency } = require('../utils/portfolioCalculations');
const { DateTime } = require('luxon');
const fetchHistoricalExchangeRate = require('../services/fetchHistoricalExchangeRate');

/**
 * Obtiene las tasas de cambio para todas las monedas en un rango de fechas
 * @param {Array} currencies - Array de objetos de monedas
 * @param {string} startDate - Fecha de inicio en formato ISO
 * @param {string} endDate - Fecha de fin en formato ISO
 * @returns {Promise<Object>} - Objeto con tasas de cambio por fecha y moneda
 */
async function getCurrencyRatesForDateRange(currencies, startDate, endDate) {
  const ratesByDate = {};
  
  // Crear rango de fechas
  let currentDate = DateTime.fromISO(startDate);
  const lastDate = DateTime.fromISO(endDate);
  
  while (currentDate <= lastDate) {
    const dateStr = currentDate.toISODate();
    ratesByDate[dateStr] = {};
    
    for (const currency of currencies) {
      try {
        // Si es USD, la tasa es 1
        if (currency.code === 'USD') {
          ratesByDate[dateStr][currency.code] = 1;
          continue;
        }
        
        // Convertir la fecha ISO a objeto Date para fetchHistoricalExchangeRate
        const dateObj = new Date(dateStr);
        const rate = await fetchHistoricalExchangeRate(currency.code, dateObj);
        
        if (rate && !isNaN(rate) && rate > 0) {
          ratesByDate[dateStr][currency.code] = rate;
          console.log(`Tasa para ${currency.code} en ${dateStr}: ${rate}`);
        } else {
          // Si no hay tasa para la fecha, usar la tasa actual de la moneda
          console.warn(`No se encontró tasa para ${currency.code} en ${dateStr}, usando tasa predeterminada: ${currency.exchangeRate}`);
          ratesByDate[dateStr][currency.code] = currency.exchangeRate;
        }
      } catch (error) {
        console.error(`Error al obtener tasa para ${currency.code} en ${dateStr}:`, error);
        // Usar tasa actual como fallback
        ratesByDate[dateStr][currency.code] = currency.exchangeRate;
      }
    }
    
    // Pasar al siguiente día
    currentDate = currentDate.plus({ days: 1 });
  }
  
  return ratesByDate;
}

/**
 * Actualiza totalCashFlow en documentos portfolioPerformance basado en transacciones desde una fecha de inicio hasta una fecha final
 * @param {string} startDate - Fecha de inicio en formato ISO (opcional, usa la fecha actual si no se proporciona)
 * @param {string} endDate - Fecha de fin en formato ISO (opcional, usa la fecha actual si no se proporciona)
 * @returns {Promise<null>}
 */
async function updateTotalCashFlow(startDate, endDate) {
  const db = admin.firestore();
  
  // Si no se proporciona fecha de inicio, usar la fecha actual
  const startDateTime = startDate 
    ? DateTime.fromISO(startDate).setZone('America/New_York') 
    : DateTime.now().setZone('America/New_York');
  
  // Si no se proporciona fecha de fin, usar la fecha actual
  const endDateTime = endDate 
    ? DateTime.fromISO(endDate).setZone('America/New_York') 
    : DateTime.now().setZone('America/New_York');
  
  const formattedStartDate = startDateTime.toISODate();
  const formattedEndDate = endDateTime.toISODate();
  
  try {
    console.log(`Iniciando actualización de totalCashFlow desde ${formattedStartDate} hasta ${formattedEndDate}`);
    
    // Obtener todos los usuarios con portfolioPerformance
    const portfolioPerformanceSnapshot = await db.collection('portfolioPerformance').get();
    const userIds = portfolioPerformanceSnapshot.docs.map(doc => doc.id);
    
    // Obtener monedas activas
    const currenciesSnapshot = await db.collection('currencies').where('isActive', '==', true).get();
    const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    
    // Obtener tasas históricas para el rango de fechas
    console.log("Obteniendo tasas de cambio históricas para el rango de fechas...");
    const historicalRates = await getCurrencyRatesForDateRange(currencies, formattedStartDate, formattedEndDate);
    
    // Para cada usuario
    for (const userId of userIds) {
      console.log(`Procesando usuario: ${userId}`);
      
      // Obtener fechas dentro del rango especificado
      const datesSnapshot = await db.collection('portfolioPerformance')
        .doc(userId)
        .collection('dates')
        .where('date', '>=', formattedStartDate)
        .where('date', '<=', formattedEndDate)
        .orderBy('date', 'asc')
        .get();
      
      // Obtener cuentas del usuario
      const accountsSnapshot = await db.collection('portfolioPerformance')
        .doc(userId)
        .collection('accounts')
        .get();
      
      const accountIds = accountsSnapshot.docs.map(doc => doc.id);
      
      // Para cada fecha
      for (const dateDoc of datesSnapshot.docs) {
        const currentDate = dateDoc.data().date;
        console.log(`Procesando fecha: ${currentDate} para usuario ${userId}`);
        
        // Obtener los datos actuales del documento de fecha para verificar las monedas existentes
        const dateDocData = dateDoc.data();
        
        // Obtener transacciones para la fecha actual
        const transactionsSnapshot = await db.collection('transactions')
          .where('date', '==', currentDate)
          .get();
        
        const todaysTransactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        
        // Filtrar transacciones por tipo
        const buyTransactions = todaysTransactions.filter(t => t.type === 'buy');
        const sellTransactions = todaysTransactions.filter(t => t.type === 'sell');
        const dividendTransactions = todaysTransactions.filter(t => t.type === 'dividendPay');
        
        // Obtener todas las transacciones de compra para los activos vendidos hoy
        const assetIdsWithSells = new Set(sellTransactions.map(t => t.assetId).filter(id => id));
        
        // Variables para almacenar las transacciones de compra históricas
        let historicalBuyTransactions = [];
        let buyTransactionsByAssetId = {};
        
        // Solo realizar la consulta si hay assetIds con ventas
        if (assetIdsWithSells.size > 0) {
          console.log(`Obteniendo transacciones de compra históricas para ${assetIdsWithSells.size} activos vendidos`);
          // Obtener todas las transacciones de compra históricas para estos activos
          const historicalBuyTransactionsSnapshot = await db.collection('transactions')
            .where('type', '==', 'buy')
            .where('assetId', 'in', [...assetIdsWithSells])
            .get();
          
          historicalBuyTransactions = historicalBuyTransactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
          
          // Agrupar transacciones de compra por assetId
          buyTransactionsByAssetId = historicalBuyTransactions.reduce((acc, t) => {
            if (!acc[t.assetId]) {
              acc[t.assetId] = [];
            }
            acc[t.assetId].push(t);
            return acc;
          }, {});
        } else {
          console.log('No hay transacciones de venta con assetId válido para hoy');
        }
        
        // Usar tasas históricas para la fecha actual
        const historicalCurrencies = currencies.map(currency => ({
          ...currency,
          exchangeRate: historicalRates[currentDate]?.[currency.code] || currency.exchangeRate
        }));
        
        // Obtener todos los activos (activos e inactivos)
        const [activeAssetsSnapshot, allAssetsSnapshot] = await Promise.all([
          db.collection('assets').where('isActive', '==', true).get(),
          db.collection('assets').get()
        ]);
        
        const activeAssets = activeAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        const allAssets = allAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        const inactiveAssets = allAssets.filter(asset => !asset.isActive);
        
        // Identificar assets inactivos que tuvieron ventas hoy
        const inactiveAssetsWithSellTransactions = new Set();
        sellTransactions.forEach(transaction => {
          if (transaction.assetId) {
            const asset = inactiveAssets.find(a => a.id === transaction.assetId);
            if (asset) {
              inactiveAssetsWithSellTransactions.add(asset.id);
            }
          }
        });
        
        // Incluir assets inactivos que tuvieron ventas hoy
        const assetsToInclude = [
          ...activeAssets,
          ...inactiveAssets.filter(asset => inactiveAssetsWithSellTransactions.has(asset.id))
        ];
        
        console.log(`Incluyendo ${inactiveAssetsWithSellTransactions.size} assets inactivos con transacciones de venta para la fecha ${currentDate}`);
        
        // Agrupar activos por nombre y tipo
        const groupedAssets = assetsToInclude.reduce((acc, asset) => {
          if (accountIds.includes(asset.portfolioAccount)) {
            const key = `${asset.name}_${asset.assetType}`;
            if (!acc[key]) {
              acc[key] = [];
            }
            acc[key].push(asset);
          }
          return acc;
        }, {});
        
        // Calcular totalCashFlow y doneProfitAndLoss para cada moneda a nivel de usuario
        const userCashFlowByCurrency = {};
        
        // Para cada moneda a nivel de usuario
        for (const currency of historicalCurrencies) {
          // Verificar si la moneda ya existe en el documento
          if (!dateDocData[currency.code]) {
            console.log(`Omitiendo moneda ${currency.code} para usuario ${userId} en fecha ${currentDate} porque no existía previamente`);
            continue;
          }
          
          // Verificar si existe el objeto assetPerformance para la moneda actual
          const assetPerformanceExists = dateDocData[currency.code]?.assetPerformance;
          
          let totalCashFlow = 0;
          let totalDoneProfitAndLoss = 0;
          const assetCashFlows = {};
          const assetDoneProfitAndLoss = {};
          
          // Procesar compras (valores negativos)
          buyTransactions.forEach(t => {
            if (accountIds.includes(t.portfolioAccountId)) {
              const convertedAmount = convertCurrency(
                -t.amount * t.price,
                t.currency,
                currency.code,
                historicalCurrencies,
                t.defaultCurrencyForAdquisitionDollar,
                parseFloat(t.dollarPriceToDate.toString())
              );
              totalCashFlow += convertedAmount;
              
              // Si hay assetId, acumular para el asset específico
              if (t.assetId && assetPerformanceExists) {
                // Encontrar el activo correspondiente
                const asset = allAssets.find(a => a.id === t.assetId);
                if (asset) {
                  const assetKey = `${asset.name}_${asset.assetType}`;
                  if (!assetCashFlows[assetKey]) {
                    assetCashFlows[assetKey] = 0;
                    assetDoneProfitAndLoss[assetKey] = 0;
                  }
                  assetCashFlows[assetKey] += convertedAmount;
                }
              }
            }
          });
          
          // Procesar ventas (valores positivos) y calcular doneProfitAndLoss
          sellTransactions.forEach(sellTx => {
            if (accountIds.includes(sellTx.portfolioAccountId)) {
              const sellAmountConverted = convertCurrency(
                sellTx.amount * sellTx.price,
                sellTx.currency,
                currency.code,
                historicalCurrencies,
                sellTx.defaultCurrencyForAdquisitionDollar,
                parseFloat(sellTx.dollarPriceToDate.toString())
              );
              totalCashFlow += sellAmountConverted;
              
              // Si hay assetId, acumular para el asset específico y calcular doneProfitAndLoss
              if (sellTx.assetId) {
                // Encontrar el activo correspondiente (activo o inactivo)
                const asset = allAssets.find(a => a.id === sellTx.assetId);
                if (asset) {
                  const assetKey = `${asset.name}_${asset.assetType}`;
                  if (!assetCashFlows[assetKey]) {
                    assetCashFlows[assetKey] = 0;
                    assetDoneProfitAndLoss[assetKey] = 0;
                  }
                  assetCashFlows[assetKey] += sellAmountConverted;
                  
                  // Calcular doneProfitAndLoss (ganancias o pérdidas realizadas)
                  const buyTxsForAsset = buyTransactionsByAssetId[sellTx.assetId] || [];
                  
                  if (buyTxsForAsset.length > 0) {
                    // Calcular el costo promedio de adquisición para esas unidades
                    let totalBuyCost = 0;
                    let totalBuyUnits = 0;
                    
                    buyTxsForAsset.forEach(buyTx => {
                      totalBuyCost += buyTx.amount * buyTx.price;
                      totalBuyUnits += buyTx.amount;
                    });
                    
                    const avgCostPerUnit = totalBuyCost / totalBuyUnits;
                    const costOfSoldUnits = sellTx.amount * avgCostPerUnit;
                    
                    // Convertir el costo de adquisición a la moneda actual
                    const costOfSoldUnitsConverted = convertCurrency(
                      costOfSoldUnits,
                      sellTx.currency,
                      currency.code,
                      historicalCurrencies,
                      sellTx.defaultCurrencyForAdquisitionDollar,
                      parseFloat(sellTx.dollarPriceToDate.toString())
                    );
                    
                    // El PnL es la diferencia entre el valor de venta y el costo de adquisición
                    const profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
                    
                    // Acumular para el asset específico
                    assetDoneProfitAndLoss[assetKey] += profitAndLoss;
                    totalDoneProfitAndLoss += profitAndLoss;
                    
                    console.log(`Calculado P&L para venta de ${sellTx.amount} unidades de ${assetKey}: ${profitAndLoss} ${currency.code}`);
                  }
                }
              }
            }
          });
          
          // Procesar dividendos (valores positivos)
          dividendTransactions.forEach(t => {
            if (accountIds.includes(t.portfolioAccountId)) {
              const convertedAmount = convertCurrency(
                t.amount,
                t.currency,
                currency.code,
                historicalCurrencies,
                t.defaultCurrencyForAdquisitionDollar,
                parseFloat(t.dollarPriceToDate.toString())
              );
              totalCashFlow += convertedAmount;
              
              // Si hay assetId, acumular para el asset específico
              if (t.assetId && assetPerformanceExists) {
                // Encontrar el activo correspondiente
                const asset = allAssets.find(a => a.id === t.assetId);
                if (asset) {
                  const assetKey = `${asset.name}_${asset.assetType}`;
                  if (!assetCashFlows[assetKey]) {
                    assetCashFlows[assetKey] = 0;
                    assetDoneProfitAndLoss[assetKey] = 0;
                  }
                  assetCashFlows[assetKey] += convertedAmount;
                }
              }
            }
          });
          
          userCashFlowByCurrency[currency.code] = { 
            totalCashFlow,
            assetCashFlows,
            doneProfitAndLoss: totalDoneProfitAndLoss,
            assetDoneProfitAndLoss
          };
        }
        
        // Actualizar totalCashFlow a nivel de usuario
        const userDateRef = db.collection('portfolioPerformance')
          .doc(userId)
          .collection('dates')
          .doc(currentDate);
        
        const batch = db.batch();
        
        // Actualizar cada moneda en el documento de fecha del usuario
        for (const [currencyCode, data] of Object.entries(userCashFlowByCurrency)) {
          // Actualizar totalCashFlow y doneProfitAndLoss a nivel de moneda
          const updateData = {
            [`${currencyCode}.totalCashFlow`]: data.totalCashFlow,
            [`${currencyCode}.doneProfitAndLoss`]: data.doneProfitAndLoss
          };
          
          batch.update(userDateRef, updateData);
          
          // Verificar si existe assetPerformance para esta moneda
          if (dateDocData[currencyCode]?.assetPerformance) {
            // En lugar de actualizar cada activo individualmente, primero copiamos la estructura existente
            const completeAssetPerformance = {};
            
            // Copiar datos existentes
            Object.entries(dateDocData[currencyCode].assetPerformance).forEach(([assetKey, assetData]) => {
              completeAssetPerformance[assetKey] = { ...assetData };
            });
            
            // Actualizar totalCashFlow y doneProfitAndLoss para activos que tienen transacciones
            for (const [assetKey, assetCashFlow] of Object.entries(data.assetCashFlows)) {
              // Si el activo ya existe en assetPerformance
              if (dateDocData[currencyCode].assetPerformance[assetKey]) {
                // Actualizar totalCashFlow y doneProfitAndLoss
                completeAssetPerformance[assetKey].totalCashFlow = assetCashFlow;
                completeAssetPerformance[assetKey].doneProfitAndLoss = data.assetDoneProfitAndLoss[assetKey] || 0;
                
                console.log(`Actualizando totalCashFlow y doneProfitAndLoss para activo ${assetKey} en moneda ${currencyCode}`);
              } 
              // Si es un activo que se vendió totalmente y no existe en assetPerformance
              else if (inactiveAssetsWithSellTransactions.size > 0) {
                // Verificar si este assetKey corresponde a un activo inactivo con ventas
                const matchesInactiveAsset = inactiveAssets
                  .filter(asset => inactiveAssetsWithSellTransactions.has(asset.id))
                  .some(asset => `${asset.name}_${asset.assetType}` === assetKey);
                
                if (matchesInactiveAsset) {
                  // Crear un nuevo registro en assetPerformance para el activo vendido
                  completeAssetPerformance[assetKey] = {
                    totalInvestment: 0,
                    totalValue: 0,
                    totalROI: 0,
                    dailyReturn: 0,
                    monthlyReturn: 0,
                    annualReturn: 0,
                    dailyChangePercentage: 0,
                    adjustedDailyChangePercentage: 0,
                    rawDailyChangePercentage: 0,
                    totalCashFlow: assetCashFlow,
                    doneProfitAndLoss: data.assetDoneProfitAndLoss[assetKey] || 0,
                    units: 0
                  };
                  
                  console.log(`Creando registro en assetPerformance para activo vendido ${assetKey} en moneda ${currencyCode}`);
                }
              }
            }
            
            // Calculamos unrealizedProfitAndLoss para todos los activos
            console.log(`Calculando unrealizedProfitAndLoss para todos los activos en ${currencyCode}`);
            
            Object.entries(dateDocData[currencyCode].assetPerformance).forEach(([assetKey, assetData]) => {
              // Solo calculamos si el activo existe en nuestro objeto (para asegurarnos)
              if (completeAssetPerformance[assetKey]) {
                const assetTotalValue = assetData.totalValue || 0;
                const assetTotalInvestment = assetData.totalInvestment || 0;
                const assetUnrealizedPnL = assetTotalValue - assetTotalInvestment;
                
                // Añadimos el campo calculado
                completeAssetPerformance[assetKey].unrealizedProfitAndLoss = assetUnrealizedPnL;
                
                console.log(`Calculado unrealizedProfitAndLoss para activo ${assetKey} en ${currencyCode}: ${assetUnrealizedPnL}`);
              }
            });
            
            // Actualizamos todo el objeto assetPerformance de una vez
            batch.update(userDateRef, {
              [`${currencyCode}.assetPerformance`]: completeAssetPerformance
            });
            
            console.log(`Actualizado totalCashFlow, doneProfitAndLoss y unrealizedProfitAndLoss para todos los activos en ${currencyCode}`);
          }
        }
        
        // Para cada cuenta del usuario
        for (const accountId of accountIds) {
          // Primero verificar que la cuenta tenga un documento para la fecha actual
          const accountDateRef = db.collection('portfolioPerformance')
            .doc(userId)
            .collection('accounts')
            .doc(accountId)
            .collection('dates')
            .doc(currentDate);
          
          const accountDateDoc = await accountDateRef.get();
          
          if (!accountDateDoc.exists) {
            console.log(`Omitiendo cuenta ${accountId} para la fecha ${currentDate} porque el documento no existe`);
            continue;
          }
          
          const accountDateData = accountDateDoc.data();
          
          // Filtrar activos para esta cuenta específica (incluyendo inactivos con ventas)
          const accountAssets = assetsToInclude.filter(asset => asset.portfolioAccount === accountId);
          
          // Agrupar activos de la cuenta por nombre y tipo
          const accountGroupedAssets = accountAssets.reduce((acc, asset) => {
            const key = `${asset.name}_${asset.assetType}`;
            if (!acc[key]) {
              acc[key] = [];
            }
            acc[key].push(asset);
            return acc;
          }, {});
          
          // Identificar assets inactivos de esta cuenta que tuvieron ventas
          const accountSellTransactions = sellTransactions.filter(t => t.portfolioAccountId === accountId);
          const inactiveAccountAssetsWithSells = inactiveAssets.filter(asset => 
            asset.portfolioAccount === accountId && 
            accountSellTransactions.some(t => t.assetId === asset.id)
          );
          
          // Calcular totalCashFlow y doneProfitAndLoss para cada moneda a nivel de cuenta
          const accountCashFlowByCurrency = {};
          
          for (const currency of historicalCurrencies) {
            // Verificar si la moneda ya existe en el documento de cuenta
            if (!accountDateData[currency.code]) {
              console.log(`Omitiendo moneda ${currency.code} para cuenta ${accountId} en fecha ${currentDate} porque no existía previamente`);
              continue;
            }
            
            // Verificar si existe el objeto assetPerformance para la moneda actual en la cuenta
            const accountAssetPerformanceExists = accountDateData[currency.code]?.assetPerformance;
            
            let accountCashFlow = 0;
            let accountDoneProfitAndLoss = 0;
            const accountAssetCashFlows = {};
            const accountAssetDoneProfitAndLoss = {};
            
            // Procesar compras (valores negativos)
            buyTransactions.forEach(t => {
              if (t.portfolioAccountId === accountId) {
                const convertedAmount = convertCurrency(
                  -t.amount * t.price,
                  t.currency,
                  currency.code,
                  historicalCurrencies,
                  t.defaultCurrencyForAdquisitionDollar,
                  parseFloat(t.dollarPriceToDate.toString())
                );
                accountCashFlow += convertedAmount;
                
                // Si hay assetId, acumular para el asset específico
                if (t.assetId && accountAssetPerformanceExists) {
                  // Encontrar el activo correspondiente
                  const asset = allAssets.find(a => a.id === t.assetId);
                  if (asset) {
                    const assetKey = `${asset.name}_${asset.assetType}`;
                    if (!accountAssetCashFlows[assetKey]) {
                      accountAssetCashFlows[assetKey] = 0;
                      accountAssetDoneProfitAndLoss[assetKey] = 0;
                    }
                    accountAssetCashFlows[assetKey] += convertedAmount;
                  }
                }
              }
            });
            
            // Procesar ventas (valores positivos) y calcular doneProfitAndLoss
            accountSellTransactions.forEach(sellTx => {
              const sellAmountConverted = convertCurrency(
                sellTx.amount * sellTx.price,
                sellTx.currency,
                currency.code,
                historicalCurrencies,
                sellTx.defaultCurrencyForAdquisitionDollar,
                parseFloat(sellTx.dollarPriceToDate.toString())
              );
              accountCashFlow += sellAmountConverted;
              
              // Si hay assetId, acumular para el asset específico y calcular doneProfitAndLoss
              if (sellTx.assetId) {
                // Encontrar el activo correspondiente (activo o inactivo)
                const asset = allAssets.find(a => a.id === sellTx.assetId);
                if (asset) {
                  const assetKey = `${asset.name}_${asset.assetType}`;
                  if (!accountAssetCashFlows[assetKey]) {
                    accountAssetCashFlows[assetKey] = 0;
                    accountAssetDoneProfitAndLoss[assetKey] = 0;
                  }
                  accountAssetCashFlows[assetKey] += sellAmountConverted;
                  
                  // Calcular doneProfitAndLoss (ganancias o pérdidas realizadas)
                  const buyTxsForAsset = buyTransactionsByAssetId[sellTx.assetId] || [];
                  
                  if (buyTxsForAsset.length > 0) {
                    // Calcular el costo promedio de adquisición para esas unidades
                    let totalBuyCost = 0;
                    let totalBuyUnits = 0;
                    
                    buyTxsForAsset.forEach(buyTx => {
                      totalBuyCost += buyTx.amount * buyTx.price;
                      totalBuyUnits += buyTx.amount;
                    });
                    
                    const avgCostPerUnit = totalBuyCost / totalBuyUnits;
                    const costOfSoldUnits = sellTx.amount * avgCostPerUnit;
                    
                    // Convertir el costo de adquisición a la moneda actual
                    const costOfSoldUnitsConverted = convertCurrency(
                      costOfSoldUnits,
                      sellTx.currency,
                      currency.code,
                      historicalCurrencies,
                      sellTx.defaultCurrencyForAdquisitionDollar,
                      parseFloat(sellTx.dollarPriceToDate.toString())
                    );
                    
                    // El PnL es la diferencia entre el valor de venta y el costo de adquisición
                    const profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
                    
                    // Acumular para el asset específico
                    accountAssetDoneProfitAndLoss[assetKey] += profitAndLoss;
                    accountDoneProfitAndLoss += profitAndLoss;
                    
                    console.log(`Calculado P&L para venta de ${sellTx.amount} unidades de ${assetKey} en cuenta ${accountId}: ${profitAndLoss} ${currency.code}`);
                  }
                }
              }
            });
            
            // Procesar dividendos (valores positivos)
            dividendTransactions.forEach(t => {
              if (t.portfolioAccountId === accountId) {
                const convertedAmount = convertCurrency(
                  t.amount,
                  t.currency,
                  currency.code,
                  historicalCurrencies,
                  t.defaultCurrencyForAdquisitionDollar,
                  parseFloat(t.dollarPriceToDate.toString())
                );
                accountCashFlow += convertedAmount;
                
                // Si hay assetId, acumular para el asset específico
                if (t.assetId && accountAssetPerformanceExists) {
                  // Encontrar el activo correspondiente
                  const asset = allAssets.find(a => a.id === t.assetId);
                  if (asset) {
                    const assetKey = `${asset.name}_${asset.assetType}`;
                    if (!accountAssetCashFlows[assetKey]) {
                      accountAssetCashFlows[assetKey] = 0;
                      accountAssetDoneProfitAndLoss[assetKey] = 0;
                    }
                    accountAssetCashFlows[assetKey] += convertedAmount;
                  }
                }
              }
            });
            
            accountCashFlowByCurrency[currency.code] = { 
              totalCashFlow: accountCashFlow,
              assetCashFlows: accountAssetCashFlows,
              doneProfitAndLoss: accountDoneProfitAndLoss,
              assetDoneProfitAndLoss: accountAssetDoneProfitAndLoss
            };
          }
          
          // Actualizar cada moneda en el documento de fecha de la cuenta
          for (const [currencyCode, data] of Object.entries(accountCashFlowByCurrency)) {
            // Actualizar totalCashFlow y doneProfitAndLoss a nivel de moneda
            const accountUpdateData = {
              [`${currencyCode}.totalCashFlow`]: data.totalCashFlow,
              [`${currencyCode}.doneProfitAndLoss`]: data.doneProfitAndLoss
            };
            
            batch.update(accountDateRef, accountUpdateData);
            
            // Verificar si existe assetPerformance para esta moneda
            if (accountDateData[currencyCode]?.assetPerformance) {
              // En lugar de actualizar cada activo individualmente, primero copiamos la estructura existente
              const completeAccountAssetPerformance = {};
              
              // Copiar datos existentes
              Object.entries(accountDateData[currencyCode].assetPerformance).forEach(([assetKey, assetData]) => {
                completeAccountAssetPerformance[assetKey] = { ...assetData };
              });
              
              // Actualizar totalCashFlow y doneProfitAndLoss para activos con transacciones
              for (const [assetKey, assetCashFlow] of Object.entries(data.assetCashFlows)) {
                // Si el activo ya existe en assetPerformance
                if (accountDateData[currencyCode].assetPerformance[assetKey]) {
                  // Actualizar totalCashFlow y doneProfitAndLoss
                  completeAccountAssetPerformance[assetKey].totalCashFlow = assetCashFlow;
                  completeAccountAssetPerformance[assetKey].doneProfitAndLoss = data.assetDoneProfitAndLoss[assetKey] || 0;
                  
                  console.log(`Actualizando totalCashFlow y doneProfitAndLoss para activo ${assetKey} en cuenta ${accountId} para moneda ${currencyCode}`);
                }
                // Si es un activo que se vendió totalmente y no existe en assetPerformance
                else if (inactiveAccountAssetsWithSells.length > 0) {
                  // Verificar si este assetKey corresponde a un activo inactivo con ventas
                  const matchesInactiveAsset = inactiveAccountAssetsWithSells
                    .some(asset => `${asset.name}_${asset.assetType}` === assetKey);
                  
                  if (matchesInactiveAsset) {
                    // Crear un nuevo registro en assetPerformance para el activo vendido
                    completeAccountAssetPerformance[assetKey] = {
                      totalInvestment: 0,
                      totalValue: 0,
                      totalROI: 0,
                      dailyReturn: 0,
                      monthlyReturn: 0,
                      annualReturn: 0,
                      dailyChangePercentage: 0,
                      adjustedDailyChangePercentage: 0,
                      rawDailyChangePercentage: 0,
                      totalCashFlow: assetCashFlow,
                      doneProfitAndLoss: data.assetDoneProfitAndLoss[assetKey] || 0,
                      units: 0
                    };
                    
                    console.log(`Creando registro en assetPerformance para activo vendido ${assetKey} en cuenta ${accountId} para moneda ${currencyCode}`);
                  }
                }
              }
              
              // Calculamos unrealizedProfitAndLoss para todos los activos
              console.log(`Calculando unrealizedProfitAndLoss para todos los activos en cuenta ${accountId}, moneda ${currencyCode}`);
              
              Object.entries(accountDateData[currencyCode].assetPerformance).forEach(([assetKey, assetData]) => {
                // Solo calculamos si el activo existe en nuestro objeto (para asegurarnos)
                if (completeAccountAssetPerformance[assetKey]) {
                  const accountAssetTotalValue = assetData.totalValue || 0;
                  const accountAssetTotalInvestment = assetData.totalInvestment || 0;
                  const accountAssetUnrealizedPnL = accountAssetTotalValue - accountAssetTotalInvestment;
                  
                  // Añadimos el campo calculado
                  completeAccountAssetPerformance[assetKey].unrealizedProfitAndLoss = accountAssetUnrealizedPnL;
                  
                  console.log(`Calculado unrealizedProfitAndLoss para activo ${assetKey} en cuenta ${accountId}, moneda ${currencyCode}: ${accountAssetUnrealizedPnL}`);
                }
              });
              
              // Actualizamos todo el objeto assetPerformance de una vez
              batch.update(accountDateRef, {
                [`${currencyCode}.assetPerformance`]: completeAccountAssetPerformance
              });
              
              console.log(`Actualizado totalCashFlow, doneProfitAndLoss y unrealizedProfitAndLoss para todos los activos en cuenta ${accountId}, moneda ${currencyCode}`);
            }
          }
        }
        
        // Comprometer todos los cambios en batch
        await batch.commit();
        console.log(`Actualizado totalCashFlow, doneProfitAndLoss y unrealizedProfitAndLoss para la fecha ${currentDate} del usuario ${userId}`);
      }
    }
    
    console.log(`Actualización de datos financieros completada desde ${formattedStartDate} hasta ${formattedEndDate}`);
    return null;
  } catch (error) {
    console.error('Error al actualizar datos financieros:', error);
    return null;
  }
}

// Exportar la función para uso externo
module.exports = { updateTotalCashFlow, getCurrencyRatesForDateRange };

// Si se ejecuta directamente, usar la fecha actual como predeterminada
/*if (require.main === module) {
  // Puedes pasar fechas ISO como argumentos: node testUpdateTotalCashFlow.js 2023-01-01 2023-01-31
  const startDate = process.argv[2];
  const endDate = process.argv[3];
  updateTotalCashFlow(startDate, endDate)
    .then(() => {
      console.log('Proceso completado');
      process.exit(0);
    })
    .catch(error => {
      console.error('Error en el proceso principal:', error);
      process.exit(1);
    });
}*/

// Comentar esta sección cuando se quiera usar con argumentos de línea de comando

const startDate = '2024-11-01';
const endDate = '2024-11-30';
updateTotalCashFlow(startDate, endDate)
  .then(() => {
    console.log('Proceso completado');
    process.exit(0);
  })
  .catch(error => {
    console.error('Error en el proceso principal:', error);
    process.exit(1);
  });
