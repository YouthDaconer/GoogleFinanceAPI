/**
 * Scheduled Portfolio Calculations
 * 
 * Función ligera que reemplaza los cálculos de portfolio de unifiedMarketDataUpdateV2.
 * Se ejecuta 2x/día para calcular rendimiento y riesgo.
 * 
 * Esta función NO actualiza precios ni currencies - eso lo hace dailyEODSnapshot.
 * 
 * Schedule: 10:30 AM y 5:30 PM ET (después de los EOD snapshots)
 * 
 * @module services/scheduledPortfolioCalculations
 * @see docs/stories/85.story.md (OPT-DEMAND-302)
 */

const { onSchedule } = require("firebase-functions/v2/scheduler");
const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require('firebase-admin');
const { DateTime } = require('luxon');
const { calculatePortfolioRisk } = require('./calculatePortfolioRisk');
const { invalidatePerformanceCacheBatch } = require('./historicalReturnsService');
const { StructuredLogger } = require('../utils/logger');

// Importar la función de cálculo de performance
const { calculateAccountPerformance, convertCurrency } = require('../utils/portfolioCalculations');

// ============================================================================
// Configuration
// ============================================================================

// Schedule: 2x/día - después de los EOD snapshots
// 10:30 AM ET: Después del pre-market snapshot (8:30 AM) + mercado abierto
// 5:30 PM ET: Después del post-market snapshot (5:00 PM)
const SCHEDULE = '30 10,17 * * 1-5';

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
    // Obtener datos necesarios
    const [
      portfolioAccountsSnapshot,
      currentPricesSnapshot,
      currenciesSnapshot,
      assetsSnapshot
    ] = await Promise.all([
      db.collection('portfolioAccounts').where('isActive', '==', true).get(),
      db.collection('currentPrices').get(),
      db.collection('currencies').where('isActive', '==', true).get(),
      db.collection('assets').where('isActive', '==', true).get()
    ]);
    
    const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ 
      id: doc.id, 
      ...doc.data() 
    }));
    const currentPrices = currentPricesSnapshot.docs.map(doc => ({ 
      symbol: doc.id, 
      ...doc.data() 
    }));
    const currencies = currenciesSnapshot.docs.map(doc => ({ 
      id: doc.id, 
      ...doc.data() 
    }));
    const allAssets = assetsSnapshot.docs.map(doc => ({ 
      id: doc.id, 
      ...doc.data() 
    }));
    
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
            const accountAssets = allAssets.filter(a => a.portfolioAccountId === account.id);
            
            for (const asset of accountAssets) {
              const priceData = currentPrices.find(p => 
                p.symbol === asset.name || p.id === asset.name
              );
              
              if (priceData && asset.amount > 0) {
                const assetValue = asset.amount * (priceData.price || 0);
                const assetInvestment = asset.amount * (asset.purchasePrice || 0);
                
                // Convertir a la moneda del reporte
                const valueConverted = convertCurrency(
                  assetValue,
                  priceData.currency || 'USD',
                  currency.code,
                  currencies
                );
                
                const investmentConverted = convertCurrency(
                  assetInvestment,
                  asset.currency || 'USD',
                  currency.code,
                  currencies
                );
                
                totalValue += valueConverted;
                totalInvestment += investmentConverted;
                
                const assetKey = `${asset.name}_${asset.assetType || 'stock'}`;
                if (!assetPerformance[assetKey]) {
                  assetPerformance[assetKey] = {
                    totalValue: 0,
                    totalInvestment: 0,
                    units: 0
                  };
                }
                assetPerformance[assetKey].totalValue += valueConverted;
                assetPerformance[assetKey].totalInvestment += investmentConverted;
                assetPerformance[assetKey].units += asset.amount;
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
 * Scheduled Portfolio Calculations
 * 
 * Ejecuta cálculos de rendimiento y riesgo del portafolio 2 veces al día.
 * Usa los precios actualizados por dailyEODSnapshot.
 */
const scheduledPortfolioCalculations = onSchedule(
  {
    schedule: SCHEDULE, // 10:30 AM y 5:30 PM ET
    timeZone: 'America/New_York',
    memory: '1GiB', // Cálculos de portfolio pueden ser intensivos
    timeoutSeconds: 540, // 9 minutos
    retryCount: 2,
    labels: {
      deployment: 'portfolio-calculations',
      replaces: 'unified-market-data-update-v2'
    }
  },
  async (event) => {
    const startTime = Date.now();
    logger = new StructuredLogger('scheduledPortfolioCalculations');
    
    const nyNow = DateTime.now().setZone('America/New_York');
    const isPostMarket = nyNow.hour >= 17;
    const runType = isPostMarket ? 'post-market' : 'intraday';
    
    logger.info(`Starting portfolio calculations (${runType})`, {
      scheduledTime: nyNow.toISO(),
      runType
    });
    
    const db = admin.firestore();
    
    try {
      // 1. Calcular rendimiento del portafolio
      const perfOp = logger.startOperation('calculateDailyPortfolioPerformance');
      const portfolioResult = await calculateDailyPortfolioPerformance(db);
      perfOp.success({ portfoliosCalculated: portfolioResult?.count || 0 });
      
      // 2. Calcular riesgo del portafolio
      const riskOp = logger.startOperation('calculatePortfolioRisk');
      await calculatePortfolioRisk();
      riskOp.success();
      
      // 3. Invalidar cache de rendimientos históricos
      let cacheResult = { usersProcessed: 0, cachesDeleted: 0 };
      if (portfolioResult?.userIds && portfolioResult.userIds.length > 0) {
        const cacheOp = logger.startOperation('invalidatePerformanceCacheBatch');
        try {
          cacheResult = await invalidatePerformanceCacheBatch(portfolioResult.userIds);
          cacheOp.success(cacheResult);
        } catch (cacheError) {
          logger.warn('Cache invalidation failed (non-critical)', { 
            error: cacheError.message 
          });
        }
      }
      
      const executionTimeMs = Date.now() - startTime;
      
      // 4. Actualizar systemStatus para compatibilidad
      await db.collection('systemStatus').doc('portfolioCalculations').set({
        lastRun: admin.firestore.FieldValue.serverTimestamp(),
        runType,
        portfoliosCalculated: portfolioResult?.count || 0,
        cachesInvalidated: cacheResult.cachesDeleted,
        executionTimeMs,
        success: true
      }, { merge: true });
      
      logger.info('Portfolio calculations completed', {
        executionTimeMs,
        portfoliosCalculated: portfolioResult?.count || 0,
        cachesInvalidated: cacheResult.cachesDeleted,
        runType
      });
      
    } catch (error) {
      logger.error('Portfolio calculations failed', error);
      
      await db.collection('systemStatus').doc('portfolioCalculations').set({
        lastRun: admin.firestore.FieldValue.serverTimestamp(),
        runType,
        success: false,
        error: error.message,
        executionTimeMs: Date.now() - startTime
      }, { merge: true });
      
      throw error; // Re-throw para retry
    }
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
