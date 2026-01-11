const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require('firebase-admin');
const axios = require('axios');
const { calculateAccountPerformance, convertCurrency } = require('../utils/portfolioCalculations');
const { calculatePortfolioRisk } = require('./calculatePortfolioRisk');
const { invalidatePerformanceCacheBatch } = require('./historicalReturnsService');
const { DateTime } = require('luxon');

// Importar generador de logos
const { generateLogoUrl } = require('../utils/logoGenerator');

// Importar logger estructurado (SCALE-CORE-002)
const { StructuredLogger } = require('../utils/logger');

const API_BASE_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

// Flag para habilitar logs detallados (puede causar mucho ruido en producción)
const ENABLE_DETAILED_LOGS = process.env.ENABLE_DETAILED_LOGS === 'true';

// Horarios de NYSE en hora local de Nueva York (no UTC)
// Esto maneja automáticamente EST/EDT gracias a Luxon
const NYSE_OPEN_HOUR = 9;
const NYSE_OPEN_MINUTE = 30;
const NYSE_CLOSE_HOUR = 16;  // 4:00 PM

// Ventana de gracia después del cierre para capturar precios finales (en minutos)
const CLOSING_GRACE_WINDOW_MINUTES = 5;

// Logger global para este módulo (se inicializa en cada ejecución)
let logger = null;

function logDebug(...args) {
  if (logger) {
    logger.debug(args[0], args.length > 1 ? { details: args.slice(1) } : {});
  }
}

function logInfo(...args) {
  if (logger) {
    logger.info(args[0], args.length > 1 ? { details: args.slice(1) } : {});
  }
}

function logWarn(...args) {
  if (logger) {
    logger.warn(args[0], args.length > 1 ? { details: args.slice(1) } : {});
  }
}

function logError(...args) {
  if (logger) {
    const error = args.find(a => a instanceof Error);
    const message = typeof args[0] === 'string' ? args[0] : 'Error';
    logger.error(message, error, { details: args.filter(a => !(a instanceof Error) && a !== message) });
  } else {
    console.error(...args);
  }
}

/**
 * Verifica si el mercado NYSE está abierto (fallback local).
 * Usa Luxon para manejar correctamente EST/EDT automáticamente.
 * NOTA: Este es un fallback - el código principal usa markets/US.isOpen de Finnhub.
 */
function isNYSEMarketOpen() {
  const nyNow = DateTime.now().setZone('America/New_York');
  const hour = nyNow.hour;
  const minute = nyNow.minute;
  const dayOfWeek = nyNow.weekday; // 1=Monday, 7=Sunday
  
  // Fin de semana: cerrado
  if (dayOfWeek === 6 || dayOfWeek === 7) {
    return false;
  }
  
  // Convertir hora actual a minutos desde medianoche
  const currentMinutes = hour * 60 + minute;
  const openMinutes = NYSE_OPEN_HOUR * 60 + NYSE_OPEN_MINUTE; // 9:30 = 570
  const closeMinutes = NYSE_CLOSE_HOUR * 60; // 16:00 = 960
  
  return currentMinutes >= openMinutes && currentMinutes < closeMinutes;
}

/**
 * Verifica si estamos en la "ventana de cierre" del mercado.
 * Esta ventana permite una última actualización justo después del cierre
 * para capturar los precios finales del día.
 * 
 * @param {number} closeHour - Hora de cierre del mercado (ej: 16 para 4:00 PM)
 * @returns {{ inWindow: boolean, reason: string }} - Si estamos en la ventana y por qué
 */
function isInClosingWindow(closeHour = NYSE_CLOSE_HOUR) {
  const nyNow = DateTime.now().setZone('America/New_York');
  const hour = nyNow.hour;
  const minute = nyNow.minute;
  const dayOfWeek = nyNow.weekday;
  
  // Fin de semana: no hay ventana de cierre
  if (dayOfWeek === 6 || dayOfWeek === 7) {
    return { inWindow: false, reason: 'weekend' };
  }
  
  const currentMinutes = hour * 60 + minute;
  const closeMinutes = closeHour * 60;
  const graceEndMinutes = closeMinutes + CLOSING_GRACE_WINDOW_MINUTES;
  
  // Estamos en la ventana si:
  // - Es exactamente la hora de cierre (16:00), O
  // - Estamos dentro de los primeros N minutos después del cierre
  if (currentMinutes >= closeMinutes && currentMinutes <= graceEndMinutes) {
    return { 
      inWindow: true, 
      reason: `closing-window (${closeHour}:00 + ${CLOSING_GRACE_WINDOW_MINUTES}min grace)`,
      minutesAfterClose: currentMinutes - closeMinutes
    };
  }
  
  return { inWindow: false, reason: 'outside-window' };
}

/**
 * 🚀 OPTIMIZACIÓN: Función unificada que obtiene todos los datos de mercado en una sola llamada
 * Combina monedas y símbolos de activos para minimizar llamadas a la API Lambda
 */
async function getAllMarketDataBatch(currencyCodes, assetSymbols) {
  try {
    // Preparar símbolos de monedas (agregar %3DX para codificación URL)
    const currencySymbols = currencyCodes.map(code => `${code}%3DX`);
    
    // Combinar todos los símbolos en una sola consulta
    const allSymbols = [...currencySymbols, ...assetSymbols];
    
    // Dividir en lotes más grandes (100 símbolos por llamada para optimizar)
    const batchSize = 100;
    const results = {
      currencies: {},
      assets: new Map()
    };
    
    logInfo(`📡 Consultando ${allSymbols.length} símbolos en ${Math.ceil(allSymbols.length / batchSize)} lotes optimizados`);
    
    for (let i = 0; i < allSymbols.length; i += batchSize) {
      const symbolBatch = allSymbols.slice(i, i + batchSize);
      const symbolsParam = symbolBatch.join(',');
      
      const url = `${API_BASE_URL}/market-quotes?symbols=${symbolsParam}`;
      logDebug(`🔄 Lote ${Math.floor(i/batchSize) + 1}: ${symbolBatch.length} símbolos`);
      
      const { data } = await axios.get(url);
      
      if (Array.isArray(data)) {
        data.forEach(item => {
          if (item.symbol && item.regularMarketPrice) {
            // Si es una moneda (termina en =X en la respuesta)
            if (item.symbol.includes('=X')) {
              const currencyCode = item.symbol.replace('=X', '');
              if (currencyCodes.includes(currencyCode)) {
                results.currencies[currencyCode] = item.regularMarketPrice;
              }
            } 
            // Si es un activo normal
            else if (assetSymbols.includes(item.symbol)) {
              results.assets.set(item.symbol, item);
            }
          }
        });
      }
    }
    
    logInfo(`✅ Datos obtenidos: ${Object.keys(results.currencies).length} monedas, ${results.assets.size} activos`);
    return results;
  } catch (error) {
    logError(`❌ Error al obtener datos de mercado en lote:`, error.message);
    return { currencies: {}, assets: new Map() };
  }
}

/**
 * Actualiza las tasas de cambio de monedas usando datos ya obtenidos
 */
async function updateCurrencyRates(db, currencyRates) {
  logDebug('🔄 Actualizando tasas de cambio...');
  
  const currenciesRef = db.collection('currencies');
  const snapshot = await currenciesRef.where('isActive', '==', true).get();
  const batch = db.batch();
  let updatesCount = 0;
  let invalidCount = 0;

  const activeCurrencies = snapshot.docs.map(doc => ({
    code: doc.data().code,
    ref: doc.ref,
    data: doc.data()
  }));
  
  activeCurrencies.forEach(currency => {
    const { code, ref, data } = currency;
    const newRate = currencyRates[code];
    
    if (newRate && !isNaN(newRate) && newRate > 0) {
      const updatedData = {
        code: code,
        name: data.name,
        symbol: data.symbol,
        exchangeRate: newRate,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
      };

      batch.update(ref, updatedData);
      updatesCount++;
      
      // 🚀 OPTIMIZACIÓN: Solo log detallado si está habilitado
      if (ENABLE_DETAILED_LOGS) {
        logDebug(`Actualizada tasa de cambio para USD:${code} a ${newRate}`);
      }
    } else {
      invalidCount++;
      logWarn(`Valor inválido para USD:${code}: ${newRate}`);
    }
  });

  if (updatesCount > 0) {
    await batch.commit();
    logInfo(`✅ ${updatesCount} tasas de cambio actualizadas${invalidCount > 0 ? ` (${invalidCount} inválidas)` : ''}`);
  }
  
  return updatesCount;
}

/**
 * Actualiza los precios actuales de los activos usando datos ya obtenidos
 */
async function updateCurrentPrices(db, assetQuotes) {
  logDebug('🔄 Actualizando precios actuales...');
  
  const currentPricesRef = db.collection('currentPrices');
  const snapshot = await currentPricesRef.get();
  const batch = db.batch();
  let updatesCount = 0;
  let failedUpdates = 0;

  snapshot.docs.forEach(doc => {
    const docData = doc.data();
    const symbol = docData.symbol;
    const quote = assetQuotes.get(symbol);
    
    if (quote && quote.regularMarketPrice) {
      const updatedData = {
        symbol: symbol,
        price: quote.regularMarketPrice,
        lastUpdated: Date.now(),
        change: quote.regularMarketChange,
        percentChange: quote.regularMarketChangePercent,
        previousClose: quote.regularMarketPreviousClose,
        currency: quote.currency,
        marketState: quote.marketState,
        quoteType: quote.quoteType,
        exchange: quote.exchange,
        fullExchangeName: quote.fullExchangeName
      };
      
      // Mantener campos existentes
      if (docData.name) updatedData.name = docData.name;
      if (docData.isin) updatedData.isin = docData.isin;
      if (docData.type) updatedData.type = docData.type;
      if (docData.logo) updatedData.logo = docData.logo;
      if (docData.website) updatedData.website = docData.website;
      
      // Generar logo si no existe en el documento
      if (!docData.logo) {
        const generatedLogo = generateLogoUrl(symbol, { 
          website: docData.website, 
          assetType: docData.type || 'stock' 
        });
        if (generatedLogo) {
          updatedData.logo = generatedLogo;
          logDebug(`Logo generado para ${symbol}`);
        }
      }
      
      batch.update(doc.ref, updatedData);
      updatesCount++;
      
      // 🚀 OPTIMIZACIÓN: Solo log detallado si está habilitado
      if (ENABLE_DETAILED_LOGS) {
        logDebug(`Actualizado precio para ${symbol}: ${quote.regularMarketPrice} ${quote.currency}`);
      }
    } else {
      failedUpdates++;
    }
  });

  if (updatesCount > 0) {
    await batch.commit();
    logInfo(`✅ ${updatesCount} precios actualizados${failedUpdates > 0 ? ` (${failedUpdates} fallidos)` : ''}`);
  }
  
  return updatesCount;
}

/**
 * 🚀 OPTIMIZACIÓN: Sistema de caché para datos históricos
 */
class PerformanceDataCache {
  constructor() {
    this.userLastPerformance = new Map();
    this.accountLastPerformance = new Map();
    this.buyTransactionsByAsset = new Map();
  }

  async preloadHistoricalData(db, formattedDate, userPortfolios, sellTransactions) {
    logDebug('📁 Precargando datos históricos (OPTIMIZACIÓN)...');
    
    const allUserIds = Object.keys(userPortfolios);
    const allAccountIds = Object.values(userPortfolios).flat().map(acc => acc.id);
    const assetIdsWithSells = [...new Set(sellTransactions.map(t => t.assetId).filter(id => id))];

    // ✨ OPTIMIZACIÓN: Consultas paralelas masivas
    const [userPerformanceResults, accountPerformanceResults, buyTransactionsResults] = await Promise.all([
      Promise.all(allUserIds.map(async (userId) => {
        const userRef = db.collection('portfolioPerformance').doc(userId);
        const query = await userRef.collection('dates')
          .where('date', '<', formattedDate)
          .orderBy('date', 'desc')
          .limit(1)
          .get();
        return { userId, data: query.empty ? null : query.docs[0].data() };
      })),
      
      Promise.all(allAccountIds.map(async (accountId) => {
        const userId = Object.keys(userPortfolios).find(uid => 
          userPortfolios[uid].some(acc => acc.id === accountId)
        );
        const accountRef = db.collection('portfolioPerformance')
          .doc(userId)
          .collection('accounts')
          .doc(accountId);
        const query = await accountRef.collection('dates')
          .where('date', '<', formattedDate)
          .orderBy('date', 'desc')
          .limit(1)
          .get();
        return { accountId, data: query.empty ? null : query.docs[0].data() };
      })),
      
      assetIdsWithSells.length > 0 ? 
        db.collection('transactions')
          .where('type', '==', 'buy')
          .where('assetId', 'in', assetIdsWithSells)
          .get() : 
        { docs: [] }
    ]);

    // Procesar resultados
    userPerformanceResults.forEach(({ userId, data }) => {
      if (data) this.userLastPerformance.set(userId, data);
    });

    accountPerformanceResults.forEach(({ accountId, data }) => {
      if (data) this.accountLastPerformance.set(accountId, data);
    });

    if (buyTransactionsResults.docs) {
      const buyTransactions = buyTransactionsResults.docs.map(doc => ({ id: doc.id, ...doc.data() }));
      buyTransactions.forEach(tx => {
        if (!this.buyTransactionsByAsset.has(tx.assetId)) {
          this.buyTransactionsByAsset.set(tx.assetId, []);
        }
        this.buyTransactionsByAsset.get(tx.assetId).push(tx);
      });
    }

    logInfo(`✅ Caché precargado: ${this.userLastPerformance.size} usuarios, ${this.accountLastPerformance.size} cuentas`);
  }

  getUserLastPerformance(userId, currencies) {
    const data = this.userLastPerformance.get(userId);
    if (!data) {
      return currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: { totalValue: 0 } }), {});
    }
    
    return Object.entries(data).reduce((acc, [currency, currencyData]) => {
      if (currency !== 'date') {
        acc[currency] = {
          totalValue: currencyData.totalValue || 0,
          ...Object.entries(currencyData.assetPerformance || {}).reduce((assetAcc, [assetName, assetData]) => {
            assetAcc[assetName] = { 
              totalValue: assetData.totalValue || 0,
              units: assetData.units || 0
            };
            return assetAcc;
          }, {})
        };
      }
      return acc;
    }, {});
  }

  getAccountLastPerformance(accountId, currencies) {
    const data = this.accountLastPerformance.get(accountId);
    if (!data) {
      return currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: { totalValue: 0 } }), {});
    }
    
    return Object.entries(data).reduce((acc, [currency, currencyData]) => {
      if (currency !== 'date') {
        acc[currency] = {
          totalValue: currencyData.totalValue || 0,
          ...Object.entries(currencyData.assetPerformance || {}).reduce((assetAcc, [assetName, assetData]) => {
            assetAcc[assetName] = { 
              totalValue: assetData.totalValue || 0,
              units: assetData.units || 0
            };
            return assetAcc;
          }, {})
        };
      }
      return acc;
    }, {});
  }

  getBuyTransactionsForAsset(assetId) {
    return this.buyTransactionsByAsset.get(assetId) || [];
  }
}

/**
 * 🚀 OPTIMIZACIÓN: Calcula el rendimiento diario del portafolio con caché
 */
async function calculateDailyPortfolioPerformance(db) {
  logInfo('🔄 Calculando rendimiento diario del portafolio (OPTIMIZADO)...');
  
  const now = DateTime.now().setZone('America/New_York');
  const formattedDate = now.toISODate();
  let calculationsCount = 0;
  
  logDebug(`📅 Fecha de cálculo (NY): ${formattedDate}`);
  
  // ✨ OPTIMIZACIÓN: Todas las consultas iniciales en paralelo
  const [
    transactionsSnapshot,
    activeAssetsSnapshot,
    currenciesSnapshot,
    portfolioAccountsSnapshot,
    currentPricesSnapshot
  ] = await Promise.all([
    db.collection('transactions').where('date', '==', formattedDate).get(),
    db.collection('assets').where('isActive', '==', true).get(),
    db.collection('currencies').where('isActive', '==', true).get(),
    db.collection('portfolioAccounts').where('isActive', '==', true).get(),
    db.collection('currentPrices').get()
  ]);
  
  const todaysTransactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const sellTransactions = todaysTransactions.filter(t => t.type === 'sell');
  const assetIdsInSellTransactions = [...new Set(sellTransactions.map(t => t.assetId).filter(id => id))];
  
  // 🚀 OPTIMIZACIÓN: Log consolidado de transacciones
  logInfo(`📊 Transacciones para ${formattedDate}: ${todaysTransactions.length} total (${sellTransactions.length} ventas)`);
  
  const activeAssets = activeAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const currentPrices = currentPricesSnapshot.docs.map(doc => ({ symbol: doc.id.split(':')[0], ...doc.data() }));
  
  // Obtener activos inactivos involucrados en ventas
  let inactiveAssets = [];
  if (assetIdsInSellTransactions.length > 0) {
    const inactiveAssetsSnapshot = await db.collection('assets')
      .where('isActive', '==', false)
      .get();
    
    inactiveAssets = inactiveAssetsSnapshot.docs
      .map(doc => ({ id: doc.id, ...doc.data() }))
      .filter(asset => assetIdsInSellTransactions.includes(asset.id));
    
    logDebug(`Obtenidos ${inactiveAssets.length} activos inactivos involucrados en ventas`);
  }
  
  const allAssets = [...activeAssets, ...inactiveAssets];
  
  // ✨ OPTIMIZACIÓN: Las transacciones de compra ya están en el caché
  
  // Agrupar transacciones de venta por cuenta
  const sellTransactionsByAccount = sellTransactions.reduce((acc, transaction) => {
    if (!acc[transaction.portfolioAccountId]) {
      acc[transaction.portfolioAccountId] = [];
    }
    acc[transaction.portfolioAccountId].push(transaction);
    return acc;
  }, {});

  // Identificar activos inactivos con ventas
  const inactiveAssetsWithSellTransactions = new Set(
    inactiveAssets.filter(asset => 
      sellTransactions.some(tx => tx.assetId === asset.id)
    ).map(asset => asset.id)
  );
  
  const assetsToInclude = [
    ...activeAssets,
    ...inactiveAssets.filter(asset => inactiveAssetsWithSellTransactions.has(asset.id))
  ];

  const userPortfolios = portfolioAccounts.reduce((acc, account) => {
    if (!acc[account.userId]) acc[account.userId] = [];
    acc[account.userId].push(account);
    return acc;
  }, {});

  // ✨ OPTIMIZACIÓN: Sistema de caché para datos históricos
  const cache = new PerformanceDataCache();
  await cache.preloadHistoricalData(db, formattedDate, userPortfolios, sellTransactions);

  // ✨ OPTIMIZACIÓN: Batch único para todas las operaciones
  const BATCH_SIZE = 450;
  let batch = db.batch();
  let batchCount = 0;
  let totalBatchesCommitted = 0;

  // 🚀 OPTIMIZACIÓN: Log consolidado de procesamiento
  const userCount = Object.keys(userPortfolios).length;
  const totalAccounts = Object.values(userPortfolios).flat().length;
  logInfo(`👥 Procesando ${userCount} usuarios con ${totalAccounts} cuentas activas`);
  
  for (const [userId, accounts] of Object.entries(userPortfolios)) {
    logDebug(`👤 Procesando usuario ${userId} con ${accounts.length} cuentas`);
    // ✨ OPTIMIZACIÓN: Usar datos del caché en lugar de consultas individuales
    const lastOverallTotalValue = cache.getUserLastPerformance(userId, currencies);

    const allUserAssets = assetsToInclude.filter(asset => 
      accounts.some(account => account.id === asset.portfolioAccount)
    );
    
    const userTransactions = todaysTransactions.filter(t => 
      accounts.some(account => account.id === t.portfolioAccountId)
    );

    const overallPerformance = calculateAccountPerformance(
      allUserAssets,
      currentPrices,
      currencies,
      lastOverallTotalValue,
      userTransactions
    );

    // Calcular doneProfitAndLoss para cada moneda
    const userDoneProfitAndLossByCurrency = {};
    const userSellTransactions = sellTransactions.filter(t => 
      accounts.some(account => account.id === t.portfolioAccountId)
    );
    
    for (const currency of currencies) {
      let totalDoneProfitAndLoss = 0;
      const assetDoneProfitAndLoss = {};
      
      userSellTransactions.forEach(sellTx => {
        if (sellTx.assetId) {
          const asset = allAssets.find(a => a.id === sellTx.assetId);
          if (asset) {
            const assetKey = `${asset.name}_${asset.assetType}`;
            if (!assetDoneProfitAndLoss[assetKey]) {
              assetDoneProfitAndLoss[assetKey] = 0;
            }
            
            let profitAndLoss = 0;
            
            // ✨ OPTIMIZACIÓN: Usar valuePnL si está disponible
            if (sellTx.valuePnL !== undefined && sellTx.valuePnL !== null) {
              // Usar PnL precalculada y convertir a moneda objetivo
              profitAndLoss = convertCurrency(
                sellTx.valuePnL,
                sellTx.currency,
                currency.code,
                currencies,
                sellTx.defaultCurrencyForAdquisitionDollar,
                parseFloat(sellTx.dollarPriceToDate.toString())
              );
            } else {
              // Fallback: calcular PnL manualmente (método anterior)
              const sellAmountConverted = convertCurrency(
                sellTx.amount * sellTx.price,
                sellTx.currency,
                currency.code,
                currencies,
                sellTx.defaultCurrencyForAdquisitionDollar,
                parseFloat(sellTx.dollarPriceToDate.toString())
              );
              
              const buyTxsForAsset = cache.getBuyTransactionsForAsset(sellTx.assetId);
              
              if (buyTxsForAsset.length > 0) {
                let totalBuyCost = 0;
                let totalBuyUnits = 0;
                
                buyTxsForAsset.forEach(buyTx => {
                  totalBuyCost += buyTx.amount * buyTx.price;
                  totalBuyUnits += buyTx.amount;
                });
                
                const avgCostPerUnit = totalBuyCost / totalBuyUnits;
                const costOfSoldUnits = sellTx.amount * avgCostPerUnit;
                
                const costOfSoldUnitsConverted = convertCurrency(
                  costOfSoldUnits,
                  sellTx.currency,
                  currency.code,
                  currencies,
                  sellTx.defaultCurrencyForAdquisitionDollar,
                  parseFloat(sellTx.dollarPriceToDate.toString())
                );
                
                profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
              }
            }
            
            assetDoneProfitAndLoss[assetKey] += profitAndLoss;
            totalDoneProfitAndLoss += profitAndLoss;
          }
        }
      });
      
      userDoneProfitAndLossByCurrency[currency.code] = { 
        doneProfitAndLoss: totalDoneProfitAndLoss,
        assetDoneProfitAndLoss
      };
    }

    // Agregar doneProfitAndLoss a overallPerformance
    for (const [currencyCode, data] of Object.entries(userDoneProfitAndLossByCurrency)) {
      if (overallPerformance[currencyCode]) {
        overallPerformance[currencyCode].doneProfitAndLoss = data.doneProfitAndLoss;
        
        const unrealizedProfitAndLoss = overallPerformance[currencyCode].totalValue - overallPerformance[currencyCode].totalInvestment;
        overallPerformance[currencyCode].unrealizedProfitAndLoss = unrealizedProfitAndLoss;
        
        if (overallPerformance[currencyCode].assetPerformance) {
          for (const [assetKey, pnl] of Object.entries(data.assetDoneProfitAndLoss)) {
            if (overallPerformance[currencyCode].assetPerformance[assetKey]) {
              overallPerformance[currencyCode].assetPerformance[assetKey].doneProfitAndLoss = pnl;
            }
          }
          
          for (const [assetKey, assetData] of Object.entries(overallPerformance[currencyCode].assetPerformance)) {
            const assetTotalValue = assetData.totalValue || 0;
            const assetTotalInvestment = assetData.totalInvestment || 0;
            const assetUnrealizedPnL = assetTotalValue - assetTotalInvestment;
            
            overallPerformance[currencyCode].assetPerformance[assetKey].unrealizedProfitAndLoss = assetUnrealizedPnL;
          }
        }
      }
    }

    // ✨ OPTIMIZACIÓN: Asegurar documento de usuario (idempotente)
    const userPerformanceRef = db.collection('portfolioPerformance').doc(userId);
    batch.set(userPerformanceRef, { userId }, { merge: true });
    batchCount++;

    // Guardar rendimiento general del usuario
    const userOverallPerformanceRef = userPerformanceRef.collection('dates').doc(formattedDate);
    batch.set(userOverallPerformanceRef, {
      date: formattedDate,
      ...overallPerformance
    });
    batchCount++;

    // Procesar cada cuenta del usuario
    for (const account of accounts) {
      const accountSellTransactions = sellTransactionsByAccount[account.id] || [];
      const inactiveAccountAssetsWithSells = inactiveAssets.filter(asset => 
        asset.portfolioAccount === account.id && 
        accountSellTransactions.some(t => t.assetId === asset.id)
      );
      
      const accountAssets = [
        ...activeAssets.filter(asset => asset.portfolioAccount === account.id),
        ...inactiveAccountAssetsWithSells
      ];

      // ✨ OPTIMIZACIÓN: Usar datos del caché para la cuenta
      const lastAccountTotalValue = cache.getAccountLastPerformance(account.id, currencies);

      const accountTransactions = userTransactions.filter(t => t.portfolioAccountId === account.id);
      const accountPerformance = calculateAccountPerformance(
        accountAssets,
        currentPrices,
        currencies,
        lastAccountTotalValue,
        accountTransactions
      );

      // Calcular doneProfitAndLoss para la cuenta (similar al usuario)
      const accountDoneProfitAndLossByCurrency = {};
      
      for (const currency of currencies) {
        let accountDoneProfitAndLoss = 0;
        const accountAssetDoneProfitAndLoss = {};
        
        accountSellTransactions.forEach(sellTx => {
          if (sellTx.assetId) {
            const asset = allAssets.find(a => a.id === sellTx.assetId);
            if (asset) {
              const assetKey = `${asset.name}_${asset.assetType}`;
              if (!accountAssetDoneProfitAndLoss[assetKey]) {
                accountAssetDoneProfitAndLoss[assetKey] = 0;
              }
              
              let profitAndLoss = 0;
              
              // ✨ OPTIMIZACIÓN: Usar valuePnL si está disponible
              if (sellTx.valuePnL !== undefined && sellTx.valuePnL !== null) {
                // Usar PnL precalculada y convertir a moneda objetivo
                profitAndLoss = convertCurrency(
                  sellTx.valuePnL,
                  sellTx.currency,
                  currency.code,
                  currencies,
                  sellTx.defaultCurrencyForAdquisitionDollar,
                  parseFloat(sellTx.dollarPriceToDate.toString())
                );
              } else {
                // Fallback: calcular PnL manualmente (método anterior)
                const sellAmountConverted = convertCurrency(
                  sellTx.amount * sellTx.price,
                  sellTx.currency,
                  currency.code,
                  currencies,
                  sellTx.defaultCurrencyForAdquisitionDollar,
                  parseFloat(sellTx.dollarPriceToDate.toString())
                );
                
                const buyTxsForAsset = cache.getBuyTransactionsForAsset(sellTx.assetId);
                
                if (buyTxsForAsset.length > 0) {
                  let totalBuyCost = 0;
                  let totalBuyUnits = 0;
                  
                  buyTxsForAsset.forEach(buyTx => {
                    totalBuyCost += buyTx.amount * buyTx.price;
                    totalBuyUnits += buyTx.amount;
                  });
                  
                  const avgCostPerUnit = totalBuyCost / totalBuyUnits;
                  const costOfSoldUnits = sellTx.amount * avgCostPerUnit;
                  
                  const costOfSoldUnitsConverted = convertCurrency(
                    costOfSoldUnits,
                    sellTx.currency,
                    currency.code,
                    currencies,
                    sellTx.defaultCurrencyForAdquisitionDollar,
                    parseFloat(sellTx.dollarPriceToDate.toString())
                  );
                  
                  profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
                }
              }
              
              accountAssetDoneProfitAndLoss[assetKey] += profitAndLoss;
              accountDoneProfitAndLoss += profitAndLoss;
            }
          }
        });
        
        accountDoneProfitAndLossByCurrency[currency.code] = { 
          doneProfitAndLoss: accountDoneProfitAndLoss,
          assetDoneProfitAndLoss: accountAssetDoneProfitAndLoss
        };
      }

      // Agregar doneProfitAndLoss a accountPerformance
      for (const [currencyCode, data] of Object.entries(accountDoneProfitAndLossByCurrency)) {
        if (accountPerformance[currencyCode]) {
          accountPerformance[currencyCode].doneProfitAndLoss = data.doneProfitAndLoss;
          
          const accountUnrealizedPnL = accountPerformance[currencyCode].totalValue - accountPerformance[currencyCode].totalInvestment;
          accountPerformance[currencyCode].unrealizedProfitAndLoss = accountUnrealizedPnL;
          
          if (accountPerformance[currencyCode].assetPerformance) {
            for (const [assetKey, pnl] of Object.entries(data.assetDoneProfitAndLoss)) {
              if (accountPerformance[currencyCode].assetPerformance[assetKey]) {
                accountPerformance[currencyCode].assetPerformance[assetKey].doneProfitAndLoss = pnl;
              }
            }
            
            for (const [assetKey, assetData] of Object.entries(accountPerformance[currencyCode].assetPerformance)) {
              const accountAssetTotalValue = assetData.totalValue || 0;
              const accountAssetTotalInvestment = assetData.totalInvestment || 0;
              const accountAssetUnrealizedPnL = accountAssetTotalValue - accountAssetTotalInvestment;
              
              accountPerformance[currencyCode].assetPerformance[assetKey].unrealizedProfitAndLoss = accountAssetUnrealizedPnL;
            }
          }
        }
      }

      // ✨ OPTIMIZACIÓN: Asegurar documento de cuenta (idempotente)
      const accountRef = userPerformanceRef.collection('accounts').doc(account.id);
      batch.set(accountRef, { accountId: account.id }, { merge: true });
      batchCount++;

      // Guardar rendimiento de la cuenta
      const accountPerformanceRef = accountRef.collection('dates').doc(formattedDate);
      batch.set(accountPerformanceRef, {
        date: formattedDate,
        ...accountPerformance
      });
      batchCount++;

      // ✨ Commit batch si se acerca al límite
      if (batchCount >= BATCH_SIZE) {
        await batch.commit();
        totalBatchesCommitted++;
        logDebug(`📦 Batch ${totalBatchesCommitted} de ${batchCount} operaciones completado`);
        batch = db.batch();
        batchCount = 0;
      }
    }

    calculationsCount++;
  }

  // ✨ Commit final del batch
  if (batchCount > 0) {
    await batch.commit();
    totalBatchesCommitted++;
    logDebug(`📦 Batch final ${totalBatchesCommitted} de ${batchCount} operaciones completado`);
  }

  logInfo(`✅ Rendimiento calculado para ${calculationsCount} usuarios (${totalBatchesCommitted} batches)`);
  return { count: calculationsCount, userIds: Object.keys(userPortfolios) };
}

// Constante del intervalo de actualización (debe coincidir con el cron schedule)
const REFRESH_INTERVAL_MINUTES = 5;

/**
 * Función principal unificada que ejecuta todas las actualizaciones
 * 
 * COST-OPT-001: Frecuencia reducida de 2 a 5 minutos para optimizar costos
 * - Ahorro estimado: ~60% en lecturas/escrituras de Firestore
 * - Impacto UX: Precios actualizados cada 5 min en lugar de 2 min (aceptable)
 */
exports.unifiedMarketDataUpdate = onSchedule({
  schedule: `*/${REFRESH_INTERVAL_MINUTES} 9-17 * * 1-5`,  // COST-OPT-001: Cada 5 minutos (antes: */2)
  timeZone: 'America/New_York',
  retryCount: 3,
}, async (event) => {
  // Inicializar logger estructurado (SCALE-CORE-002)
  logger = StructuredLogger.forScheduled('unifiedMarketDataUpdate');
  
  const db = admin.firestore();
  
  // Verificar si estamos en la ventana de cierre del mercado
  // Esto permite una última actualización para capturar precios de cierre
  const closingWindowCheck = isInClosingWindow(NYSE_CLOSE_HOUR);
  const isInClosingGrace = closingWindowCheck.inWindow;
  
  // OPT-SYNC-001: Verificar estado del mercado desde Firestore (incluye festivos)
  // El documento markets/US es actualizado por marketStatusService que consulta Finnhub
  try {
    const marketDoc = await db.collection('markets').doc('US').get();
    if (marketDoc.exists) {
      const marketData = marketDoc.data();
      
      // Verificar si es festivo (siempre respetar festivos)
      if (marketData.holiday) {
        logger.info('Market holiday - skipping update', { 
          holiday: marketData.holiday,
          marketStatus: 'holiday'
        });
        return null;
      }
      
      // Verificar si el mercado está cerrado (usando dato de Finnhub)
      if (marketData.isOpen === false) {
        // MEJORA: Si estamos en la ventana de cierre, ejecutar una última actualización
        // para capturar los precios de cierre del día
        if (isInClosingGrace) {
          logger.info('Market just closed - executing final update to capture closing prices', { 
            session: marketData.session,
            marketStatus: 'closing-grace',
            closingWindow: closingWindowCheck,
            graceMinutes: CLOSING_GRACE_WINDOW_MINUTES
          });
          // Continuar con la ejecución (no return)
        } else {
          logger.info('Market closed (Finnhub) - skipping update', { 
            session: marketData.session,
            marketStatus: 'closed'
          });
          return null;
        }
      }
    }
  } catch (marketCheckError) {
    // Si falla la consulta, continuar con la verificación local de horario
    logger.warn('Failed to check market status from Firestore, using local check', {
      error: marketCheckError.message
    });
  }
  
  // Fallback: verificación local de horario (por si la consulta a markets/US falla)
  // También considerar la ventana de cierre
  if (!isNYSEMarketOpen() && !isInClosingGrace) {
    logger.info('Market closed (local check) - skipping update', { marketStatus: 'closed' });
    return null;
  }

  logger.info('Starting unified market data update', { marketStatus: 'open' });
  
  const startTime = Date.now();
  
  // Calcular el minuto programado (el scheduler debería haber disparado en un múltiplo de REFRESH_INTERVAL_MINUTES)
  // Esto nos da el momento exacto cuando SE PROGRAMÓ esta ejecución
  const now = DateTime.now().setZone('America/New_York');
  const scheduledMinute = Math.floor(now.minute / REFRESH_INTERVAL_MINUTES) * REFRESH_INTERVAL_MINUTES;
  const scheduledAt = now.set({ minute: scheduledMinute, second: 0, millisecond: 0 });
  const nextScheduledUpdate = scheduledAt.plus({ minutes: REFRESH_INTERVAL_MINUTES });
  const mainOp = logger.startOperation('fullUpdate');
  
      try {
    // Paso 1: Obtener códigos de monedas y símbolos de activos dinámicamente
    const dataFetchOp = logger.startOperation('fetchInitialData');
    const [currenciesSnapshot, currentPricesSnapshot] = await Promise.all([
      db.collection('currencies').where('isActive', '==', true).get(),
      db.collection('currentPrices').get()
    ]);
    
    const currencyCodes = currenciesSnapshot.docs.map(doc => doc.data().code);
    const assetSymbols = currentPricesSnapshot.docs.map(doc => doc.data().symbol);
    dataFetchOp.success({ currencyCount: currencyCodes.length, assetCount: assetSymbols.length });
    
    logger.info('Fetching market data', { currencies: currencyCodes.length, assets: assetSymbols.length });
    
    // Paso 2: Obtener TODOS los datos de mercado en llamadas optimizadas
    const marketDataOp = logger.startOperation('getAllMarketDataBatch');
    const marketData = await getAllMarketDataBatch(currencyCodes, assetSymbols);
    marketDataOp.success({ currenciesReceived: Object.keys(marketData.currencies).length, assetsReceived: marketData.assets.size });
    
    // Paso 3: Actualizar tasas de cambio con datos ya obtenidos
    const currencyOp = logger.startOperation('updateCurrencyRates');
    const currencyUpdates = await updateCurrencyRates(db, marketData.currencies);
    currencyOp.success({ updated: currencyUpdates });
    
    // Paso 4: Actualizar precios actuales con datos ya obtenidos
    const pricesOp = logger.startOperation('updateCurrentPrices');
    const priceUpdates = await updateCurrentPrices(db, marketData.assets);
    pricesOp.success({ updated: priceUpdates });
    
    // Paso 5: Calcular rendimiento del portafolio
    const perfOp = logger.startOperation('calculateDailyPortfolioPerformance');
    const portfolioResult = await calculateDailyPortfolioPerformance(db);
    perfOp.success({ portfoliosCalculated: portfolioResult.count });
    
    // Paso 6: Calcular riesgo del portafolio (usando datos actualizados)
    const riskOp = logger.startOperation('calculatePortfolioRisk');
    await calculatePortfolioRisk();
    riskOp.success();
    
    // Paso 7: Invalidar cache de rendimientos históricos (OPT-010)
    let cacheInvalidationResult = { usersProcessed: 0, cachesDeleted: 0 };
    if (portfolioResult.userIds && portfolioResult.userIds.length > 0) {
      try {
        const cacheOp = logger.startOperation('invalidatePerformanceCacheBatch');
        cacheInvalidationResult = await invalidatePerformanceCacheBatch(portfolioResult.userIds);
        cacheOp.success({ usersProcessed: cacheInvalidationResult.usersProcessed, cachesDeleted: cacheInvalidationResult.cachesDeleted });
      } catch (cacheError) {
        logger.warn('Cache invalidation failed (non-critical)', { error: cacheError.message });
      }
    }
    
    const endTime = Date.now();
    const executionTime = (endTime - startTime) / 1000;
    
    // Paso 8: Notificar al frontend que todo el pipeline completó (OPT-016)
    // Incluimos metadata para sincronización precisa del countdown
    try {
      await db.collection('systemStatus').doc('marketData').set({
        // Timestamps
        lastCompleteUpdate: admin.firestore.FieldValue.serverTimestamp(),
        lastUpdateDate: new Date().toISOString(),
        
        // Metadata de sincronización para el frontend
        refreshIntervalMinutes: REFRESH_INTERVAL_MINUTES,
        scheduledAt: scheduledAt.toISO(),           // Cuando se programó esta ejecución
        nextScheduledUpdate: nextScheduledUpdate.toISO(), // Próxima ejecución programada
        
        // Estadísticas del pipeline
        pricesUpdated: priceUpdates,
        performanceCalculated: portfolioResult.count,
        cachesInvalidated: cacheInvalidationResult.cachesDeleted,
        executionTimeMs: Math.round(executionTime * 1000),
        marketOpen: true
      }, { merge: true });
    } catch (signalError) {
      logger.warn('Frontend signal failed (non-critical)', { error: signalError.message });
    }
    
    mainOp.success({
      currencyUpdates,
      priceUpdates,
      portfoliosCalculated: portfolioResult.count,
      cachesInvalidated: cacheInvalidationResult.cachesDeleted,
      executionTimeSec: executionTime
    });
    
    return null;
  } catch (error) {
    mainOp.failure(error);
    return null;
  }
}); 