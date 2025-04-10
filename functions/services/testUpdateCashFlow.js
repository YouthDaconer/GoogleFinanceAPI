const admin = require('./firebaseAdmin');
const { convertCurrency, calculateTimeWeightedReturn } = require('../utils/portfolioCalculations');
const { DateTime } = require('luxon');
const fetchHistoricalExchangeRate = require('./fetchHistoricalExchangeRate');

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
 * También actualiza los datos mensuales en la subcolección 'monthly'
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
        
        // Calcular totalCashFlow para cada moneda a nivel de usuario
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
          const assetCashFlows = {};
          
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
                  }
                  assetCashFlows[assetKey] += convertedAmount;
                }
              }
            }
          });
          
          // Procesar ventas (valores positivos)
          sellTransactions.forEach(t => {
            if (accountIds.includes(t.portfolioAccountId)) {
              const convertedAmount = convertCurrency(
                t.amount * t.price,
                t.currency,
                currency.code,
                historicalCurrencies,
                t.defaultCurrencyForAdquisitionDollar,
                parseFloat(t.dollarPriceToDate.toString())
              );
              totalCashFlow += convertedAmount;
              
              // Si hay assetId, acumular para el asset específico
              if (t.assetId) {
                // Encontrar el activo correspondiente (activo o inactivo)
                const asset = allAssets.find(a => a.id === t.assetId);
                if (asset) {
                  const assetKey = `${asset.name}_${asset.assetType}`;
                  if (!assetCashFlows[assetKey]) {
                    assetCashFlows[assetKey] = 0;
                  }
                  assetCashFlows[assetKey] += convertedAmount;
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
                  }
                  assetCashFlows[assetKey] += convertedAmount;
                }
              }
            }
          });
          
          userCashFlowByCurrency[currency.code] = { 
            totalCashFlow,
            assetCashFlows
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
          // Actualizar totalCashFlow a nivel de moneda
          batch.update(userDateRef, {
            [`${currencyCode}.totalCashFlow`]: data.totalCashFlow
          });
          
          // Verificar si existe assetPerformance para esta moneda
          if (dateDocData[currencyCode]?.assetPerformance) {
            // Para cada activo en assetPerformance, verificar si necesita actualización
            for (const [assetKey, assetCashFlow] of Object.entries(data.assetCashFlows)) {
              // Si el activo ya existe en assetPerformance
              if (dateDocData[currencyCode].assetPerformance[assetKey]) {
                // Actualizar totalCashFlow para el activo específico
                batch.update(userDateRef, {
                  [`${currencyCode}.assetPerformance.${assetKey}.totalCashFlow`]: assetCashFlow
                });
                console.log(`Actualizando totalCashFlow para activo ${assetKey} en moneda ${currencyCode}`);
              } 
              // Si es un activo que se vendió totalmente y no existe en assetPerformance
              else if (inactiveAssetsWithSellTransactions.size > 0) {
                // Verificar si este assetKey corresponde a un activo inactivo con ventas
                const matchesInactiveAsset = inactiveAssets
                  .filter(asset => inactiveAssetsWithSellTransactions.has(asset.id))
                  .some(asset => `${asset.name}_${asset.assetType}` === assetKey);
                
                if (matchesInactiveAsset) {
                  // Crear un nuevo registro en assetPerformance para el activo vendido
                  const newAssetPerformance = {
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
                    units: 0
                  };
                  
                  // Actualizar el documento para añadir el nuevo activo
                  batch.update(userDateRef, {
                    [`${currencyCode}.assetPerformance.${assetKey}`]: newAssetPerformance
                  });
                  console.log(`Creando registro en assetPerformance para activo vendido ${assetKey} en moneda ${currencyCode}`);
                }
              }
            }
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
          
          // Calcular totalCashFlow para cada moneda a nivel de cuenta
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
            const accountAssetCashFlows = {};
            
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
                    }
                    accountAssetCashFlows[assetKey] += convertedAmount;
                  }
                }
              }
            });
            
            // Procesar ventas (valores positivos)
            accountSellTransactions.forEach(t => {
              const convertedAmount = convertCurrency(
                t.amount * t.price,
                t.currency,
                currency.code,
                historicalCurrencies,
                t.defaultCurrencyForAdquisitionDollar,
                parseFloat(t.dollarPriceToDate.toString())
              );
              accountCashFlow += convertedAmount;
              
              // Si hay assetId, acumular para el asset específico
              if (t.assetId) {
                // Encontrar el activo correspondiente (activo o inactivo)
                const asset = allAssets.find(a => a.id === t.assetId);
                if (asset) {
                  const assetKey = `${asset.name}_${asset.assetType}`;
                  if (!accountAssetCashFlows[assetKey]) {
                    accountAssetCashFlows[assetKey] = 0;
                  }
                  accountAssetCashFlows[assetKey] += convertedAmount;
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
                    }
                    accountAssetCashFlows[assetKey] += convertedAmount;
                  }
                }
              }
            });
            
            accountCashFlowByCurrency[currency.code] = { 
              totalCashFlow: accountCashFlow,
              assetCashFlows: accountAssetCashFlows
            };
          }
          
          // Actualizar cada moneda en el documento de fecha de la cuenta
          for (const [currencyCode, data] of Object.entries(accountCashFlowByCurrency)) {
            // Actualizar totalCashFlow a nivel de moneda
            batch.update(accountDateRef, {
              [`${currencyCode}.totalCashFlow`]: data.totalCashFlow
            });
            
            // Verificar si existe assetPerformance para esta moneda
            if (accountDateData[currencyCode]?.assetPerformance) {
              // Para cada activo en assetPerformance, verificar si necesita actualización
              for (const [assetKey, assetCashFlow] of Object.entries(data.assetCashFlows)) {
                // Si el activo ya existe en assetPerformance
                if (accountDateData[currencyCode].assetPerformance[assetKey]) {
                  // Actualizar totalCashFlow para el activo específico
                  batch.update(accountDateRef, {
                    [`${currencyCode}.assetPerformance.${assetKey}.totalCashFlow`]: assetCashFlow
                  });
                  console.log(`Actualizando totalCashFlow para activo ${assetKey} en cuenta ${accountId} para moneda ${currencyCode}`);
                }
                // Si es un activo que se vendió totalmente y no existe en assetPerformance
                else if (inactiveAccountAssetsWithSells.length > 0) {
                  // Verificar si este assetKey corresponde a un activo inactivo con ventas
                  const matchesInactiveAsset = inactiveAccountAssetsWithSells
                    .some(asset => `${asset.name}_${asset.assetType}` === assetKey);
                  
                  if (matchesInactiveAsset) {
                    // Crear un nuevo registro en assetPerformance para el activo vendido
                    const newAssetPerformance = {
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
                      units: 0
                    };
                    
                    // Actualizar el documento para añadir el nuevo activo
                    batch.update(accountDateRef, {
                      [`${currencyCode}.assetPerformance.${assetKey}`]: newAssetPerformance
                    });
                    console.log(`Creando registro en assetPerformance para activo vendido ${assetKey} en cuenta ${accountId} para moneda ${currencyCode}`);
                  }
                }
              }
            }
          }
        }
        
        // Comprometer todos los cambios en batch
        await batch.commit();
        console.log(`Actualizado totalCashFlow para la fecha ${currentDate} del usuario ${userId}`);
      }
      
      // Actualizar los datos mensuales
      console.log(`Actualizando datos mensuales para usuario ${userId}`);
      
      // Identificar los meses únicos en el rango de fechas
      const monthsToProcess = new Set();
      datesSnapshot.docs.forEach(doc => {
        const date = DateTime.fromISO(doc.data().date);
        const yearMonth = date.toFormat('yyyy-MM');
        monthsToProcess.add(yearMonth);
      });
      
      // Procesar cada mes
      for (const yearMonth of monthsToProcess) {
        console.log(`Procesando mes: ${yearMonth} para usuario ${userId}`);
        
        // Calcular fechas de inicio y fin del mes
        const monthDate = DateTime.fromFormat(yearMonth, 'yyyy-MM');
        const startOfMonth = monthDate.startOf('month').toISODate();
        const endOfMonth = monthDate.endOf('month').toISODate();
        
        // Obtener las fechas dentro del mes
        const monthDatesSnapshot = await db.collection('portfolioPerformance')
          .doc(userId)
          .collection('dates')
          .where('date', '>=', startOfMonth)
          .where('date', '<=', endOfMonth)
          .orderBy('date', 'asc')
          .get();
        
        if (monthDatesSnapshot.empty) {
          console.log(`No hay datos de rendimiento diario para el usuario ${userId} en el período ${yearMonth}`);
          continue;
        }
        
        // Obtener todas las transacciones del mes para las cuentas de este usuario
        // Primero obtenemos todas las cuentas del usuario
        const userAccountsSnapshot = await db.collection('portfolioAccounts')
          .where('userId', '==', userId)
          .get();
        
        const userAccountIds = userAccountsSnapshot.docs.map(doc => doc.id);
        
        // Ahora obtenemos las transacciones para estas cuentas
        const transactionsPromises = userAccountIds.map(accountId => 
          db.collection('transactions')
            .where('portfolioAccountId', '==', accountId)
            .where('date', '>=', startOfMonth)
            .where('date', '<=', endOfMonth)
            .get()
        );
        
        const transactionsResults = await Promise.all(transactionsPromises);
        
        // Combinar todos los resultados en un solo array
        const monthTransactions = transactionsResults
          .flatMap(snapshot => snapshot.docs)
          .map(doc => ({ id: doc.id, ...doc.data() }));
        
        console.log(`Procesando ${monthTransactions.length} transacciones para el usuario ${userId} en el período ${yearMonth}`);
        
        // Obtener el primer y último día del mes con datos
        const firstDayDoc = monthDatesSnapshot.docs[0];
        const lastDayDoc = monthDatesSnapshot.docs[monthDatesSnapshot.docs.length - 1];
        
        const firstDayData = firstDayDoc.data();
        const lastDayData = lastDayDoc.data();
        
        // Obtener todos los activos de la base de datos para hacer el mapeo correcto
        const allAssetsSnapshot = await db.collection('assets').get();
        const assetsData = allAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        
        // Crear el modelo de rendimiento mensual
        const monthlyPerformance = {};
        
        // Procesar cada moneda a nivel de usuario
        for (const currency of currencies) {
          const currencyCode = currency.code;
          
          // Verificar si hay datos para esta moneda
          if (!firstDayData[currencyCode] || !lastDayData[currencyCode]) {
            continue;
          }
          
          // Valores al inicio y fin de mes
          const startValue = firstDayData[currencyCode].totalValue || 0;
          const endValue = lastDayData[currencyCode].totalValue || 0;
          
          // Filtrar y convertir transacciones para esta moneda
          const currencyTransactions = monthTransactions.map(t => {
            let amount = 0;
            
            if (t.type === 'buy') {
              // Compras son flujo negativo
              amount = convertCurrency(
                -t.amount * t.price,
                t.currency,
                currencyCode,
                currencies,
                t.defaultCurrencyForAdquisitionDollar,
                parseFloat(t.dollarPriceToDate.toString())
              );
            } else if (t.type === 'sell') {
              // Ventas son flujo positivo
              amount = convertCurrency(
                t.amount * t.price,
                t.currency,
                currencyCode,
                currencies,
                t.defaultCurrencyForAdquisitionDollar,
                parseFloat(t.dollarPriceToDate.toString())
              );
            } else if (t.type === 'dividendPay') {
              // Dividendos son flujo positivo
              amount = convertCurrency(
                t.amount,
                t.currency,
                currencyCode,
                currencies,
                t.defaultCurrencyForAdquisitionDollar,
                parseFloat(t.dollarPriceToDate.toString())
              );
            }
            
            // Convertir P&L realizada si existe
            let realizedPL = 0;
            if (t.type === 'sell' && t.realizedPL) {
              realizedPL = convertCurrency(
                t.realizedPL,
                t.currency,
                currencyCode,
                currencies,
                t.defaultCurrencyForAdquisitionDollar,
                parseFloat(t.dollarPriceToDate.toString())
              );
            }
            
            return {
              amount,
              date: t.date,
              type: t.type,
              assetId: t.assetId,
              portfolioAccountId: t.portfolioAccountId,
              symbol: t.symbol || '',
              assetName: t.assetName || '',
              assetType: t.assetType || '',
              realizedPL: realizedPL
            };
          });
          
          // Calcular el rendimiento ajustado por tiempo (TWR)
          const timeWeightedROI = calculateTimeWeightedReturn(startValue, endValue, currencyTransactions);
          
          // Calcular un ROI simple para compatibilidad
          const simpleROI = startValue > 0 ? ((endValue - startValue) / startValue) * 100 : 0;
          
          // Calcular flujo de caja total
          const totalCashFlow = currencyTransactions.reduce((sum, t) => sum + t.amount, 0);
          
          // Procesar rendimiento por activo
          const assetPerformance = {};
          
          // Primero, obtener todos los assets únicos que aparecen en los datos diarios
          const allAssets = new Set();
          
          monthDatesSnapshot.docs.forEach(doc => {
            const data = doc.data();
            if (data[currencyCode] && data[currencyCode].assetPerformance) {
              Object.keys(data[currencyCode].assetPerformance).forEach(assetKey => {
                allAssets.add(assetKey);
              });
            }
          });
          
          // Agrupar transacciones por activo
          const assetTransactions = {};
          
          // Mapear transacciones por asset utilizando assetId
          for (const t of currencyTransactions) {
            if (!t.assetId) continue;
            
            // Encontrar el activo correspondiente usando el assetId
            const asset = assetsData.find(a => a.id === t.assetId);
            if (asset) {
              const assetKey = `${asset.name}_${asset.assetType}`;
              
              // Verificar si este assetKey está en el conjunto de assets
              if (allAssets.has(assetKey)) {
                if (!assetTransactions[assetKey]) {
                  assetTransactions[assetKey] = [];
                }
                assetTransactions[assetKey].push(t);
              }
            }
          }
          
          // Procesar cada asset
          for (const assetKey of allAssets) {
            // Valores al inicio y fin de mes para este asset
            const assetStartValue = firstDayData[currencyCode]?.assetPerformance?.[assetKey]?.totalValue || 0;
            const assetEndValue = lastDayData[currencyCode]?.assetPerformance?.[assetKey]?.totalValue || 0;
            
            // Transacciones para este asset
            const assetFlows = assetTransactions[assetKey] || [];
            
            // Verificar si este asset fue completamente vendido durante el mes
            const wasFullySold = 
              assetStartValue > 0 && // Existía al inicio del mes
              assetEndValue === 0 && // No existe al final del mes
              assetFlows.some(t => t.type === 'sell'); // Hubo ventas
              
            // O no estaba al inicio pero se compró y vendió dentro del mes
            const wasBoughtAndSold = 
              assetStartValue === 0 && // No existía al inicio del mes
              assetEndValue === 0 && // No existe al final del mes
              assetFlows.some(t => t.type === 'buy') && // Hubo compras
              assetFlows.some(t => t.type === 'sell'); // Y también ventas
            
            // Calcular TWR para este asset
            const assetTimeWeightedROI = calculateTimeWeightedReturn(assetStartValue, assetEndValue, assetFlows);
            
            // Calcular ROI simple
            const assetSimpleROI = assetStartValue > 0 ? ((assetEndValue - assetStartValue) / assetStartValue) * 100 : 0;
            
            // Calcular flujo de caja para este asset
            const assetCashFlow = assetFlows.reduce((sum, t) => sum + t.amount, 0);
            
            // Calcular P&L realizado total del asset
            const realizedPL = assetFlows
              .filter(t => t.type === 'sell')
              .reduce((sum, t) => sum + t.realizedPL, 0);
            
            // Obtener las ventas del mes para este asset para análisis detallado
            const sellTransactions = assetFlows.filter(t => t.type === 'sell');
            const totalSellAmount = sellTransactions.reduce((sum, t) => sum + t.amount, 0);
            
            // Si no hay P&L realizado pero hay ventas, tratar de estimar el P&L usando el cashflow positivo
            const estimatedPL = realizedPL || (totalSellAmount > 0 ? totalSellAmount : 0);
            
            // Unidades al final del período
            const assetUnits = lastDayData[currencyCode]?.assetPerformance?.[assetKey]?.units || 0;
            
            // Calcular profit/loss neto
            let netProfit;
            
            // Nuevo enfoque para calcular profit que prioriza la P&L realizada
            if (wasFullySold || wasBoughtAndSold) {
              // Si el asset fue completamente vendido o comprado y vendido en el mismo mes,
              // el profit debe ser la suma de la P&L realizada
              netProfit = realizedPL;
              
              // Si no hay P&L realizada pero hay ventas, usar flujo de caja como estimación
              if (netProfit === 0 && sellTransactions.length > 0) {
                netProfit = estimatedPL;
              }
            } else {
              // Para activos que siguen en cartera al final del mes:
              
              // 1. Contribución de P&L realizada (ventas parciales)
              let partialProfit = realizedPL;
              
              // 2. Cambio de valor en las unidades restantes ajustado por nuevas compras
              let valueChangeContribution = 0;
              
              // Calcular compras netas durante el período
              const netPurchases = assetFlows
                .filter(t => t.type === 'buy')
                .reduce((sum, t) => sum + Math.abs(t.amount), 0);
              
              // Calcular variación de valor ajustada por las compras
              if (assetStartValue > 0 || netPurchases > 0) {
                // Valor de referencia es valor inicial + nuevas compras
                const referenceValue = assetStartValue + netPurchases;
                
                // Calcular cambio de valor ajustado
                if (referenceValue > 0) {
                  valueChangeContribution = assetEndValue - referenceValue;
                }
              }
              
              // Sumar ambas contribuciones
              netProfit = partialProfit + valueChangeContribution;
            }
            
            // Si el resultado es incorrecto (NaN o undefined), usar método de respaldo
            if (isNaN(netProfit) || netProfit === undefined) {
              netProfit = (assetEndValue - assetStartValue) + assetCashFlow;
            }
            
            // Agregar log de depuración para NVDA
            if (assetKey.includes('NVDA')) {
              console.log(`=== Debug para ${assetKey} en ${yearMonth} (testUpdateCashFlow) ===`);
              console.log(`startValue: ${assetStartValue}`);
              console.log(`endValue: ${assetEndValue}`);
              console.log(`wasFullySold: ${wasFullySold}`);
              console.log(`wasBoughtAndSold: ${wasBoughtAndSold}`);
              console.log(`Detalle de transacciones de ${assetKey}:`);
              
              // Mostrar cada transacción en detalle
              assetFlows.forEach((t, index) => {
                console.log(`  [${index + 1}] Fecha: ${t.date}, Tipo: ${t.type}, Monto: ${t.amount.toFixed(2)}, P&L realizada: ${t.realizedPL.toFixed(2)}`);
              });
              
              console.log(`Total transacciones: ${assetFlows.length}`);
              console.log(`Ventas totales: ${totalSellAmount.toFixed(2)}`);
              console.log(`cashFlow: ${assetCashFlow.toFixed(2)}`);
              console.log(`realizedPL: ${realizedPL.toFixed(2)}`);
              console.log(`estimatedPL: ${estimatedPL.toFixed(2)}`);
              console.log(`netProfit calculado: ${netProfit.toFixed(2)}`);
              console.log(`netProfit con fórmula normal: ${((assetEndValue - assetStartValue) + assetCashFlow).toFixed(2)}`);
              
              // Calcular el profit de forma más detallada
              let detailedProfit = 0;
              // Si hay P&L realizada, considerarla primero
              if (realizedPL > 0) {
                detailedProfit += realizedPL;
              }
              
              // Añadir cambio de valor en las unidades restantes
              if (assetEndValue > 0) {
                // Calcular el valor esperado al inicio del mes más las compras netas
                const netPurchases = assetFlows
                  .filter(t => t.type === 'buy')
                  .reduce((sum, t) => sum + Math.abs(t.amount), 0);
                  
                // Si hubo compras durante el mes que aún se mantienen
                if (assetEndValue > 0 && !wasFullySold && !wasBoughtAndSold) {
                  detailedProfit += (assetEndValue - assetStartValue - netPurchases);
                }
              }
              
              console.log(`Profit calculado detalladamente: ${detailedProfit.toFixed(2)}`);
              console.log(`=======================================`);
              
              // Si hay una diferencia significativa, usar el cálculo detallado
              if (Math.abs(detailedProfit - netProfit) > 1) {
                netProfit = detailedProfit;
              }
            }
            
            assetPerformance[assetKey] = {
              totalValue: assetEndValue,
              simpleROI: assetSimpleROI,
              timeWeightedROI: assetTimeWeightedROI,
              totalCashFlow: assetCashFlow,
              realizedPL: realizedPL,
              profit: netProfit,
              units: assetUnits,
              wasFullySold: wasFullySold || wasBoughtAndSold
            };
          }
          
          // Almacenar rendimiento para esta moneda
          monthlyPerformance[currencyCode] = {
            startValue,
            endValue,
            simpleROI,
            timeWeightedROI,
            totalCashFlow,
            profit: (endValue - startValue) + totalCashFlow, // Calcular profit neto a nivel de moneda
            assetPerformance
          };
        }
        
        // Guardar rendimiento mensual del usuario en Firestore
        const monthlyPerfRef = db.collection('portfolioPerformance')
          .doc(userId)
          .collection('monthly')
          .doc(yearMonth);
          
        await monthlyPerfRef.set({
          yearMonth,
          startDate: startOfMonth,
          endDate: endOfMonth,
          ...monthlyPerformance
        }, { merge: true });
        
        console.log(`Rendimiento mensual de usuario actualizado para ${userId} en período ${yearMonth}`);
        
        // Procesar el rendimiento mensual por cuenta
        for (const accountId of accountIds) {
          console.log(`Procesando cuenta: ${accountId} para usuario ${userId} en mes ${yearMonth}`);
          
          // Obtener las fechas dentro del rango del mes para esta cuenta
          const accountDatesSnapshot = await db.collection('portfolioPerformance')
            .doc(userId)
            .collection('accounts')
            .doc(accountId)
            .collection('dates')
            .where('date', '>=', startOfMonth)
            .where('date', '<=', endOfMonth)
            .orderBy('date', 'asc')
            .get();
          
          if (accountDatesSnapshot.empty) {
            console.log(`No hay datos de rendimiento diario para la cuenta ${accountId} en el período ${yearMonth}`);
            continue;
          }
          
          // Filtrar transacciones para esta cuenta específica
          const accountTransactions = monthTransactions.filter(t => t.portfolioAccountId === accountId);
          
          // Crear el modelo de rendimiento mensual para la cuenta
          const accountMonthlyPerformance = {};
          
          // Obtener el primer y último día del mes con datos para esta cuenta
          const accountFirstDayDoc = accountDatesSnapshot.docs[0];
          const accountLastDayDoc = accountDatesSnapshot.docs[accountDatesSnapshot.docs.length - 1];
          
          const accountFirstDayData = accountFirstDayDoc.data();
          const accountLastDayData = accountLastDayDoc.data();
          
          // Procesar cada moneda a nivel de cuenta
          for (const currency of currencies) {
            const currencyCode = currency.code;
            
            // Verificar si hay datos para esta moneda en la cuenta
            if (!accountFirstDayData[currencyCode] || !accountLastDayData[currencyCode]) {
              continue;
            }
            
            // Valores al inicio y fin de mes para la cuenta
            const accountStartValue = accountFirstDayData[currencyCode].totalValue || 0;
            const accountEndValue = accountLastDayData[currencyCode].totalValue || 0;
            
            // Filtrar y convertir transacciones para esta moneda y cuenta
            const accountCurrencyTransactions = accountTransactions.map(t => {
              let amount = 0;
              
              if (t.type === 'buy') {
                // Compras son flujo negativo
                amount = convertCurrency(
                  -t.amount * t.price,
                  t.currency,
                  currencyCode,
                  currencies,
                  t.defaultCurrencyForAdquisitionDollar,
                  parseFloat(t.dollarPriceToDate.toString())
                );
              } else if (t.type === 'sell') {
                // Ventas son flujo positivo
                amount = convertCurrency(
                  t.amount * t.price,
                  t.currency,
                  currencyCode,
                  currencies,
                  t.defaultCurrencyForAdquisitionDollar,
                  parseFloat(t.dollarPriceToDate.toString())
                );
              } else if (t.type === 'dividendPay') {
                // Dividendos son flujo positivo
                amount = convertCurrency(
                  t.amount,
                  t.currency,
                  currencyCode,
                  currencies,
                  t.defaultCurrencyForAdquisitionDollar,
                  parseFloat(t.dollarPriceToDate.toString())
                );
              }
              
              // Convertir P&L realizada si existe
              let realizedPL = 0;
              if (t.type === 'sell' && t.realizedPL) {
                realizedPL = convertCurrency(
                  t.realizedPL,
                  t.currency,
                  currencyCode,
                  currencies,
                  t.defaultCurrencyForAdquisitionDollar,
                  parseFloat(t.dollarPriceToDate.toString())
                );
              }
              
              return {
                amount,
                date: t.date,
                type: t.type,
                assetId: t.assetId,
                symbol: t.symbol || '',
                assetName: t.assetName || '',
                assetType: t.assetType || '',
                realizedPL: realizedPL
              };
            });
            
            // Calcular el rendimiento ajustado por tiempo (TWR) para la cuenta
            const accountTimeWeightedROI = calculateTimeWeightedReturn(
              accountStartValue, 
              accountEndValue, 
              accountCurrencyTransactions
            );
            
            // Calcular un ROI simple para compatibilidad
            const accountSimpleROI = accountStartValue > 0 
              ? ((accountEndValue - accountStartValue) / accountStartValue) * 100 
              : 0;
            
            // Calcular flujo de caja total para la cuenta
            const accountTotalCashFlow = accountCurrencyTransactions.reduce((sum, t) => sum + t.amount, 0);
            
            // Procesar rendimiento por activo para esta cuenta
            const accountAssetPerformance = {};
            
            // Primero, obtener todos los assets únicos que aparecen en los datos diarios de la cuenta
            const accountAllAssets = new Set();
            
            accountDatesSnapshot.docs.forEach(doc => {
              const data = doc.data();
              if (data[currencyCode] && data[currencyCode].assetPerformance) {
                Object.keys(data[currencyCode].assetPerformance).forEach(assetKey => {
                  accountAllAssets.add(assetKey);
                });
              }
            });
            
            // Agrupar transacciones de la cuenta por asset
            const accountAssetTransactions = {};
            
            // Mapear transacciones por asset utilizando assetId
            for (const t of accountCurrencyTransactions) {
              if (!t.assetId) continue;
              
              // Encontrar el activo correspondiente usando el assetId
              const asset = assetsData.find(a => a.id === t.assetId);
              if (asset) {
                const assetKey = `${asset.name}_${asset.assetType}`;
                
                // Verificar si este assetKey está en el conjunto de assets
                if (accountAllAssets.has(assetKey)) {
                  if (!accountAssetTransactions[assetKey]) {
                    accountAssetTransactions[assetKey] = [];
                  }
                  accountAssetTransactions[assetKey].push(t);
                }
              }
            }
            
            // Procesar cada asset de la cuenta
            for (const assetKey of accountAllAssets) {
              // Valores al inicio y fin de mes para este asset en la cuenta
              const assetAccountStartValue = accountFirstDayData[currencyCode]?.assetPerformance?.[assetKey]?.totalValue || 0;
              const assetAccountEndValue = accountLastDayData[currencyCode]?.assetPerformance?.[assetKey]?.totalValue || 0;
              
              // Transacciones para este asset en la cuenta
              const assetAccountFlows = accountAssetTransactions[assetKey] || [];
              
              // Verificar si este asset fue completamente vendido durante el mes
              const wasAccountAssetFullySold = 
                assetAccountStartValue > 0 && // Existía al inicio del mes
                assetAccountEndValue === 0 && // No existe al final del mes
                assetAccountFlows.some(t => t.type === 'sell'); // Hubo ventas
                
              // O no estaba al inicio pero se compró y vendió dentro del mes
              const wasAccountAssetBoughtAndSold = 
                assetAccountStartValue === 0 && // No existía al inicio del mes
                assetAccountEndValue === 0 && // No existe al final del mes
                assetAccountFlows.some(t => t.type === 'buy') && // Hubo compras
                assetAccountFlows.some(t => t.type === 'sell'); // Y también ventas
              
              // Calcular TWR para este asset en la cuenta
              const assetAccountTimeWeightedROI = calculateTimeWeightedReturn(
                assetAccountStartValue, 
                assetAccountEndValue, 
                assetAccountFlows
              );
              
              // Calcular ROI simple para este asset en la cuenta
              const assetAccountSimpleROI = assetAccountStartValue > 0 
                ? ((assetAccountEndValue - assetAccountStartValue) / assetAccountStartValue) * 100 
                : 0;
              
              // Calcular flujo de caja para este asset en la cuenta
              const assetAccountCashFlow = assetAccountFlows.reduce((sum, t) => sum + t.amount, 0);
              
              // Calcular P&L realizado total del asset en la cuenta
              const assetAccountRealizedPL = assetAccountFlows
                .filter(t => t.type === 'sell')
                .reduce((sum, t) => sum + t.realizedPL, 0);
              
              // Obtener las ventas del mes para este asset de la cuenta para análisis detallado
              const accountSellTransactions = assetAccountFlows.filter(t => t.type === 'sell');
              const accountTotalSellAmount = accountSellTransactions.reduce((sum, t) => sum + t.amount, 0);
              
              // Si no hay P&L realizado pero hay ventas, tratar de estimar el P&L usando el cashflow positivo
              const accountEstimatedPL = assetAccountRealizedPL || (accountTotalSellAmount > 0 ? accountTotalSellAmount : 0);
              
              // Unidades al final del período para este asset en la cuenta
              const assetAccountUnits = accountLastDayData[currencyCode]?.assetPerformance?.[assetKey]?.units || 0;
              
              // Calcular profit/loss neto para el asset en la cuenta
              let assetAccountNetProfit;
              
              // Nuevo enfoque para calcular profit que prioriza la P&L realizada
              if (wasAccountAssetFullySold || wasAccountAssetBoughtAndSold) {
                // Si el asset fue completamente vendido o comprado y vendido en el mismo mes,
                // el profit debe ser la suma de la P&L realizada
                assetAccountNetProfit = assetAccountRealizedPL;
                
                // Si no hay P&L realizada pero hay ventas, usar flujo de caja como estimación
                if (assetAccountNetProfit === 0 && accountSellTransactions.length > 0) {
                  assetAccountNetProfit = accountEstimatedPL;
                }
              } else {
                // Para activos que siguen en cartera al final del mes:
                
                // 1. Contribución de P&L realizada (ventas parciales)
                let partialAccountProfit = assetAccountRealizedPL;
                
                // 2. Cambio de valor en las unidades restantes ajustado por nuevas compras
                let valueChangeAccountContribution = 0;
                
                // Calcular compras netas durante el período
                const netAccountPurchases = assetAccountFlows
                  .filter(t => t.type === 'buy')
                  .reduce((sum, t) => sum + Math.abs(t.amount), 0);
                
                // Calcular variación de valor ajustada por las compras
                if (assetAccountStartValue > 0 || netAccountPurchases > 0) {
                  // Valor de referencia es valor inicial + nuevas compras
                  const referenceAccountValue = assetAccountStartValue + netAccountPurchases;
                  
                  // Calcular cambio de valor ajustado
                  if (referenceAccountValue > 0) {
                    valueChangeAccountContribution = assetAccountEndValue - referenceAccountValue;
                  }
                }
                
                // Sumar ambas contribuciones
                assetAccountNetProfit = partialAccountProfit + valueChangeAccountContribution;
              }
              
              // Si el resultado es incorrecto (NaN o undefined), usar método de respaldo
              if (isNaN(assetAccountNetProfit) || assetAccountNetProfit === undefined) {
                assetAccountNetProfit = (assetAccountEndValue - assetAccountStartValue) + assetAccountCashFlow;
              }
              
              // Agregar log de depuración para NVDA a nivel de cuenta
              if (assetKey.includes('NVDA')) {
                console.log(`=== Debug para ${assetKey} en cuenta ${accountId} en ${yearMonth} (testUpdateCashFlow) ===`);
                console.log(`startValue: ${assetAccountStartValue}`);
                console.log(`endValue: ${assetAccountEndValue}`);
                console.log(`wasFullySold: ${wasAccountAssetFullySold}`);
                console.log(`wasBoughtAndSold: ${wasAccountAssetBoughtAndSold}`);
                console.log(`Detalle de transacciones de ${assetKey} en cuenta ${accountId}:`);
                
                // Mostrar cada transacción en detalle
                assetAccountFlows.forEach((t, index) => {
                  console.log(`  [${index + 1}] Fecha: ${t.date}, Tipo: ${t.type}, Monto: ${t.amount.toFixed(2)}, P&L realizada: ${t.realizedPL.toFixed(2)}`);
                });
                
                console.log(`Total transacciones: ${assetAccountFlows.length}`);
                console.log(`Ventas totales: ${accountTotalSellAmount.toFixed(2)}`);
                console.log(`cashFlow: ${assetAccountCashFlow.toFixed(2)}`);
                console.log(`realizedPL: ${assetAccountRealizedPL.toFixed(2)}`);
                console.log(`estimatedPL: ${accountEstimatedPL.toFixed(2)}`);
                console.log(`netProfit calculado: ${assetAccountNetProfit.toFixed(2)}`);
                console.log(`netProfit con fórmula normal: ${((assetAccountEndValue - assetAccountStartValue) + assetAccountCashFlow).toFixed(2)}`);
                console.log(`=======================================`);
              }
              
              // Calcular el profit de forma más detallada
              let detailedAccountProfit = 0;
              // Si hay P&L realizada, considerarla primero
              if (assetAccountRealizedPL > 0) {
                detailedAccountProfit += assetAccountRealizedPL;
              }
              
              // Añadir cambio de valor en las unidades restantes
              if (assetAccountEndValue > 0) {
                // Calcular el valor esperado al inicio del mes más las compras netas
                const netAccountPurchases = assetAccountFlows
                  .filter(t => t.type === 'buy')
                  .reduce((sum, t) => sum + Math.abs(t.amount), 0);
                  
                // Si hubo compras durante el mes que aún se mantienen
                if (assetAccountEndValue > 0 && !wasAccountAssetFullySold && !wasAccountAssetBoughtAndSold) {
                  detailedAccountProfit += (assetAccountEndValue - assetAccountStartValue - netAccountPurchases);
                }
              }
              
              console.log(`Profit calculado detalladamente: ${detailedAccountProfit.toFixed(2)}`);
              console.log(`=======================================`);
              
              // Si hay una diferencia significativa, usar el cálculo detallado
              if (Math.abs(detailedAccountProfit - assetAccountNetProfit) > 1) {
                assetAccountNetProfit = detailedAccountProfit;
              }
              
              accountAssetPerformance[assetKey] = {
                totalValue: assetAccountEndValue,
                simpleROI: assetAccountSimpleROI,
                timeWeightedROI: assetAccountTimeWeightedROI,
                totalCashFlow: assetAccountCashFlow,
                realizedPL: assetAccountRealizedPL,
                profit: assetAccountNetProfit,
                units: assetAccountUnits,
                wasFullySold: wasAccountAssetFullySold || wasAccountAssetBoughtAndSold
              };
            }
            
            // Almacenar rendimiento para esta moneda en la cuenta
            accountMonthlyPerformance[currencyCode] = {
              startValue: accountStartValue,
              endValue: accountEndValue,
              simpleROI: accountSimpleROI,
              timeWeightedROI: accountTimeWeightedROI,
              totalCashFlow: accountTotalCashFlow,
              profit: (accountEndValue - accountStartValue) + accountTotalCashFlow, // Añadir profit neto
              assetPerformance: accountAssetPerformance
            };
          }
          
          // Guardar rendimiento mensual de la cuenta en Firestore
          const accountMonthlyPerfRef = db.collection('portfolioPerformance')
            .doc(userId)
            .collection('accounts')
            .doc(accountId)
            .collection('monthly')
            .doc(yearMonth);
            
          await accountMonthlyPerfRef.set({
            yearMonth,
            startDate: startOfMonth,
            endDate: endOfMonth,
            ...accountMonthlyPerformance
          }, { merge: true });
          
          console.log(`Rendimiento mensual de cuenta actualizado para ${accountId} en período ${yearMonth}`);
        }
      }
    }
    
    console.log(`Actualización de totalCashFlow y datos mensuales completada desde ${formattedStartDate} hasta ${formattedEndDate}`);
    return null;
  } catch (error) {
    console.error('Error al actualizar totalCashFlow y datos mensuales:', error);
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

const startDate = '2025-02-01';
const endDate = '2025-02-28';
updateTotalCashFlow(startDate, endDate)
  .then(() => {
    console.log('Proceso completado');
    process.exit(0);
  })
  .catch(error => {
    console.error('Error en el proceso principal:', error);
    process.exit(1);
  });
