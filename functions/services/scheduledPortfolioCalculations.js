/**
 * Scheduled Portfolio Calculations
 * 
 * @deprecated OPT-DEMAND-CLEANUP: Esta función está DEPRECADA desde 2026-01-16.
 * 
 * La funcionalidad de cálculos de portfolio ahora está consolidada en:
 * - unifiedMarketDataUpdate (ejecuta 1x/día a las 17:05 ET)
 * 
 * RAZÓN: Se consolidaron todas las funciones scheduled en una sola para
 * reducir complejidad y costos de Cloud Functions.
 * 
 * Esta función se mantiene temporalmente deshabilitada para posible rollback.
 * Se eliminará completamente después de 2 semanas de estabilidad.
 * 
 * @module services/scheduledPortfolioCalculations
 * @see docs/stories/85.story.md (OPT-DEMAND-302)
 * @see docs/architecture/OPT-DEMAND-CLEANUP-phase4-closure-subplan.md
 */

const { onSchedule } = require("firebase-functions/v2/scheduler");
const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require('firebase-admin');
const { DateTime } = require('luxon');
const { calculatePortfolioRisk } = require('./calculatePortfolioRisk');
const { invalidatePerformanceCacheBatch } = require('./historicalReturnsService');
const { StructuredLogger } = require('../utils/logger');
// OPT-DEMAND-CLEANUP: Importar helper para obtener precios y currencies del API Lambda
const { getPricesFromApi, getCurrencyRatesFromApi } = require('./marketDataHelper');

// Importar la función de cálculo de performance
const { calculateAccountPerformance, convertCurrency } = require('../utils/portfolioCalculations');

// ============================================================================
// Configuration
// ============================================================================

// @deprecated Schedule deshabilitado
// DEPRECATED: Schedule original comentado
// const SCHEDULE = '30 10,17 * * 1-5';  // 10:30 AM y 5:30 PM ET
const SCHEDULE = '0 0 1 1 *';  // Nunca ejecutar (1 de enero a medianoche)

let logger = null;

// ============================================================================
// Performance Calculation (extracted from unifiedMarketDataUpdate)
// ============================================================================

/**
 * Calcula el rendimiento diario del portafolio para todos los usuarios.
 * Esta es una versión simplificada extraída de unifiedMarketDataUpdate.
 * 
 * @param {FirebaseFirestore.Firestore} db 
 * @returns {Promise<{count: number, userIds: string[]}>}
 */
async function calculateDailyPortfolioPerformance(db) {
  const now = DateTime.now().setZone('America/New_York');
  const formattedDate = now.toISODate();
  
  if (logger) {
    logger.info('Starting portfolio performance calculation', { date: formattedDate });
  }
  
  try {
    // Obtener assets primero para saber qué símbolos necesitamos
    const [
      portfolioAccountsSnapshot,
      assetsSnapshot
    ] = await Promise.all([
      db.collection('portfolioAccounts').where('isActive', '==', true).get(),
      db.collection('assets').where('isActive', '==', true).get()
    ]);
    
    const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ 
      id: doc.id, 
      ...doc.data() 
    }));
    const allAssets = assetsSnapshot.docs.map(doc => ({ 
      id: doc.id, 
      ...doc.data() 
    }));
    
    // OPT-DEMAND-CLEANUP: Obtener símbolos únicos y consultar API Lambda
    const symbols = [...new Set(allAssets.map(a => a.name).filter(Boolean))];
    
    const [currentPrices, currencies] = await Promise.all([
      getPricesFromApi(symbols),
      getCurrencyRatesFromApi()
    ]);
    
    if (logger) {
      logger.info('Market data fetched from API Lambda', {
        priceCount: currentPrices.length,
        currencyCount: currencies.length,
        source: 'api-lambda'
      });
    }
    
    // Agrupar cuentas por usuario
    const userPortfolios = {};
    portfolioAccounts.forEach(account => {
      const userId = account.userId;
      if (!userPortfolios[userId]) {
        userPortfolios[userId] = [];
      }
      userPortfolios[userId].push(account);
    });
    
    const userIds = Object.keys(userPortfolios);
    let calculatedCount = 0;
    
    if (logger) {
      logger.info('Processing portfolios', { 
        users: userIds.length, 
        accounts: portfolioAccounts.length,
        assets: allAssets.length
      });
    }
    
    // Procesar cada usuario
    for (const userId of userIds) {
      const accounts = userPortfolios[userId];
      
      try {
        // Calcular performance por cuenta y total
        const overallPerformance = {};
        
        for (const currency of currencies) {
          let totalValue = 0;
          let totalInvestment = 0;
          const assetPerformance = {};
          
          for (const account of accounts) {
            // FIX: El modelo Asset usa 'portfolioAccount' no 'portfolioAccountId'
            const accountAssets = allAssets.filter(a => a.portfolioAccount === account.id);
            
            for (const asset of accountAssets) {
              const priceData = currentPrices.find(p => 
                p.symbol === asset.name || p.id === asset.name
              );
              
              // FIX: El modelo Asset usa 'units' no 'amount'
              const units = asset.units || 0;
              if (priceData && units > 0) {
                const assetValue = units * (priceData.price || 0);
                // FIX: Cálculo de inversión igual que portfolioCalculations.js
                // unitValue está en USD. NO multiplicar por acquisitionDollarValue - 
                // ese campo es para convertCurrency cuando la moneda base es COP
                const initialInvestmentUSD = units * (asset.unitValue || 0);
                
                // Convertir valor actual a la moneda del reporte
                const valueConverted = convertCurrency(
                  assetValue,
                  priceData.currency || 'USD',
                  currency.code,
                  currencies
                );
                
                // Convertir inversión a la moneda del reporte
                // Usar los parámetros especiales para manejar acquisitionDollarValue correctamente
                const investmentConverted = convertCurrency(
                  initialInvestmentUSD,
                  'USD',
                  currency.code,
                  currencies,
                  asset.defaultCurrencyForAdquisitionDollar,
                  asset.acquisitionDollarValue
                );
                
                totalValue += valueConverted;
                totalInvestment += investmentConverted;
                
                const assetKey = `${asset.name}_${asset.assetType || 'stock'}`;
                if (!assetPerformance[assetKey]) {
                  assetPerformance[assetKey] = {
                    totalValue: 0,
                    totalInvestment: 0,
                    totalROI: 0,
                    dailyReturn: 0,
                    monthlyReturn: 0,
                    annualReturn: 0,
                    dailyChangePercentage: 0,
                    adjustedDailyChangePercentage: 0,
                    rawDailyChangePercentage: 0,
                    totalCashFlow: 0,
                    units: 0,
                    unrealizedProfitAndLoss: 0
                  };
                }
                assetPerformance[assetKey].totalValue += valueConverted;
                assetPerformance[assetKey].totalInvestment += investmentConverted;
                assetPerformance[assetKey].units += units;
                // Calcular ROI y P&L no realizada
                const assetData = assetPerformance[assetKey];
                assetData.unrealizedProfitAndLoss = assetData.totalValue - assetData.totalInvestment;
                assetData.totalROI = assetData.totalInvestment > 0 
                  ? ((assetData.totalValue - assetData.totalInvestment) / assetData.totalInvestment) * 100 
                  : 0;
              }
            }
          }
          
          overallPerformance[currency.code] = {
            totalValue,
            totalInvestment,
            profitAndLoss: totalValue - totalInvestment,
            profitAndLossPercentage: totalInvestment > 0 
              ? ((totalValue - totalInvestment) / totalInvestment) * 100 
              : 0,
            assetPerformance
          };
        }
        
        // Guardar en portfolioPerformance
        const performanceRef = db.collection('portfolioPerformance')
          .doc(userId)
          .collection('dates')
          .doc(formattedDate);
        
        await performanceRef.set({
          date: formattedDate,
          ...overallPerformance,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          source: 'scheduledPortfolioCalculations'
        }, { merge: true });
        
        calculatedCount++;
        
      } catch (userError) {
        if (logger) {
          logger.warn(`Error calculating portfolio for user ${userId}`, { 
            error: userError.message 
          });
        }
      }
    }
    
    if (logger) {
      logger.info('Portfolio performance calculation completed', { 
        calculated: calculatedCount, 
        total: userIds.length 
      });
    }
    
    return { count: calculatedCount, userIds };
    
  } catch (error) {
    if (logger) {
      logger.error('Portfolio performance calculation failed', error);
    }
    throw error;
  }
}

// ============================================================================
// Main Cloud Function - Scheduled
// ============================================================================

/**
 * @deprecated OPT-DEMAND-CLEANUP: DEPRECADA - Ver unifiedMarketDataUpdate
 * 
 * Scheduled Portfolio Calculations
 * Schedule deshabilitado para evitar ejecución accidental.
 */
const scheduledPortfolioCalculations = onSchedule(
  {
    schedule: SCHEDULE, // Configurado a '0 0 1 1 *' (nunca)
    timeZone: 'America/New_York',
    memory: '1GiB',
    timeoutSeconds: 540,
    retryCount: 0,
    labels: {
      status: 'deprecated',
      deprecated: '2026-01-16',
      replacedby: 'unified-market-data-update'
    }
  },
  async (event) => {
    console.warn('⚠️ DEPRECATED: scheduledPortfolioCalculations ejecutada pero está deprecada');
    console.warn('La funcionalidad ahora está en unifiedMarketDataUpdate (17:05 ET)');
    return null;
  }
);

// ============================================================================
// Manual Trigger
// ============================================================================

/**
 * Manual trigger para testing (solo admins)
 */
const scheduledPortfolioCalculationsManual = onCall(
  {
    memory: '1GiB',
    timeoutSeconds: 540,
    enforceAppCheck: false,
  },
  async (request) => {
    // Verificar autenticación
    if (!request.auth) {
      throw new HttpsError('unauthenticated', 'Authentication required');
    }
    
    // Verificar admin
    if (!request.auth.token.admin) {
      throw new HttpsError('permission-denied', 'Admin privileges required');
    }
    
    logger = new StructuredLogger('scheduledPortfolioCalculations', { type: 'manual' });
    logger.info('Starting manual portfolio calculations', { uid: request.auth.uid });
    
    const db = admin.firestore();
    const startTime = Date.now();
    
    try {
      const portfolioResult = await calculateDailyPortfolioPerformance(db);
      await calculatePortfolioRisk();
      
      let cacheResult = { cachesDeleted: 0 };
      if (portfolioResult?.userIds?.length > 0) {
        cacheResult = await invalidatePerformanceCacheBatch(portfolioResult.userIds);
      }
      
      const executionTimeMs = Date.now() - startTime;
      
      await db.collection('systemStatus').doc('portfolioCalculations').set({
        lastRun: admin.firestore.FieldValue.serverTimestamp(),
        runType: 'manual',
        portfoliosCalculated: portfolioResult?.count || 0,
        cachesInvalidated: cacheResult.cachesDeleted,
        executionTimeMs,
        success: true
      }, { merge: true });
      
      return {
        success: true,
        executionTimeMs,
        portfoliosCalculated: portfolioResult?.count || 0,
        cachesInvalidated: cacheResult.cachesDeleted
      };
      
    } catch (error) {
      logger.error('Manual portfolio calculations failed', error);
      return {
        success: false,
        error: error.message,
        executionTimeMs: Date.now() - startTime
      };
    }
  }
);

// ============================================================================
// Exports
// ============================================================================

module.exports = {
  scheduledPortfolioCalculations,
  scheduledPortfolioCalculationsManual,
  // Export para testing
  calculateDailyPortfolioPerformance,
};
