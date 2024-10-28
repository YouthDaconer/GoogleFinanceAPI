const admin = require('./firebaseAdmin'); // Importa el archivo firebaseAdmin.js

/**
 * Calcula el Ratio de Sharpe anualizado
 * @param {number} annualizedReturn - Rendimiento anualizado del portafolio
 * @param {number} riskFreeRate - Tasa libre de riesgo anual
 * @param {number} annualizedStdDev - Desviación estándar anualizada del portafolio
 * @returns {number} Ratio de Sharpe anualizado
 */
function calculateAnnualizedSharpeRatio(annualizedReturn, riskFreeRate, annualizedStdDev) {
    return (annualizedReturn - riskFreeRate) / annualizedStdDev;
  }
  
  /**
   * Calcula el Ratio de Sortino anualizado
   * @param {number} annualizedReturn - Rendimiento anualizado del portafolio
   * @param {number} riskFreeRate - Tasa libre de riesgo anual
   * @param {number} annualizedDownsideDeviation - Desviación a la baja anualizada
   * @returns {number} Ratio de Sortino anualizado
   */
  function calculateAnnualizedSortinoRatio(annualizedReturn, riskFreeRate, annualizedDownsideDeviation) {
    return (annualizedReturn - riskFreeRate) / annualizedDownsideDeviation;
  }
  
  /**
   * Calcula el Valor en Riesgo (VaR) diario
   * @param {number[]} dailyReturns - Array de rendimientos diarios
   * @param {number} confidenceLevel - Nivel de confianza (ej. 0.95 para 95%)
   * @returns {number} VaR diario
   */
  function calculateDailyVaR(dailyReturns, confidenceLevel) {
    const sortedReturns = dailyReturns.sort((a, b) => a - b);
    const index = Math.floor((1 - confidenceLevel) * sortedReturns.length);
    return -sortedReturns[index];
  }
  
  /**
   * Calcula la correlación entre dos series de rendimientos
   * @param {number[]} returns1 - Array de rendimientos del portafolio
   * @param {number[]} returns2 - Array de rendimientos del mercado
   * @returns {number} Coeficiente de correlación
   */
  function calculateCorrelation(returns1, returns2) {
    const n = Math.min(returns1.length, returns2.length);
    let sum1 = 0, sum2 = 0, sum1Sq = 0, sum2Sq = 0, pSum = 0;
    
    for (let i = 0; i < n; i++) {
      sum1 += returns1[i];
      sum2 += returns2[i];
      sum1Sq += returns1[i] ** 2;
      sum2Sq += returns2[i] ** 2;
      pSum += returns1[i] * returns2[i];
    }
    
    const num = pSum - (sum1 * sum2 / n);
    const den = Math.sqrt((sum1Sq - sum1 ** 2 / n) * (sum2Sq - sum2 ** 2 / n));
    
    return num / den;
  }
  
  /**
   * Obtiene los datos de rendimiento diario del portafolio de Firestore
   * @param {string} userId - ID del usuario
   * @param {string} startDate - Fecha de inicio en formato 'YYYY-MM-DD'
   * @param {string} endDate - Fecha de fin en formato 'YYYY-MM-DD'
   * @returns {Promise<Object[]>} Array de datos de rendimiento diario
   */
  async function getPortfolioPerformanceData(userId, startDate, endDate) {
    const db = admin.firestore();
    const performanceRef = db.collection('portfolioPerformance').doc(userId).collection('dates');
    const snapshot = await performanceRef.where('date', '>=', startDate).where('date', '<=', endDate).get();
    
    return snapshot.docs.map(doc => ({
      date: doc.id,
      dailyReturn: doc.data().USD.dailyChangePercentage / 100 // Convertir a decimal
    }));
  }
  
  /**
   * Obtiene los datos del índice S&P 500 de Firestore
   * @param {string} startDate - Fecha de inicio en formato 'YYYY-MM-DD'
   * @param {string} endDate - Fecha de fin en formato 'YYYY-MM-DD'
   * @returns {Promise<Object[]>} Array de datos del índice
   */
  async function getMarketIndexData(startDate, endDate) {
    const db = admin.firestore();
    const indexRef = db.collection('indexHistories').doc('.INX').collection('dates');
    const snapshot = await indexRef.where('date', '>=', startDate).where('date', '<=', endDate).get();
    
    return snapshot.docs.map(doc => ({
      date: doc.id,
      dailyReturn: doc.data().percentChange / 100 // Convertir a decimal
    }));
  }
  
  /**
   * Calcula el rendimiento anualizado y la volatilidad anualizada
   * @param {number[]} dailyReturns - Array de rendimientos diarios
   * @returns {Object} Rendimiento anualizado y volatilidad anualizada
   */
  function calculateAnnualizedMetrics(dailyReturns) {
    if (dailyReturns.length < 30) {
      console.warn('Advertencia: Menos de 30 días de datos disponibles. Los resultados pueden no ser representativos.');
    }
  
    const avgDailyReturn = dailyReturns.reduce((sum, r) => sum + r, 0) / dailyReturns.length;
    const annualizedReturn = (Math.pow(1 + avgDailyReturn, 252) - 1) * 100;
  
    const variance = dailyReturns.reduce((sum, r) => sum + Math.pow(r - avgDailyReturn, 2), 0) / (dailyReturns.length - 1);
    const annualizedStdDev = Math.sqrt(variance * 252) * 100;
  
    return { 
      annualizedReturn, 
      annualizedStdDev,
      daysAnalyzed: dailyReturns.length
    };
  }
  
  /**
   * Realiza un análisis avanzado de riesgo para un portafolio
   * @param {string} userId - ID del usuario
   * @param {string} startDate - Fecha de inicio en formato 'YYYY-MM-DD'
   * @param {string} endDate - Fecha de fin en formato 'YYYY-MM-DD'
   * @param {number} riskFreeRate - Tasa libre de riesgo anual
   * @returns {Promise<Object>} Resultados del análisis de riesgo
   */
  async function performRiskAnalysis(userId, startDate, endDate, riskFreeRate) {
    // Obtener datos del portafolio y del mercado
    const [portfolioData, marketData] = await Promise.all([
      getPortfolioPerformanceData(userId, startDate, endDate),
      getMarketIndexData(startDate, endDate)
    ]);
  
    // Extraer rendimientos diarios
    const portfolioReturns = portfolioData.map(d => d.dailyReturn);
    const marketReturns = marketData.map(d => d.dailyReturn);
  
    // Calcular métricas anualizadas
    const { annualizedReturn, annualizedStdDev } = calculateAnnualizedMetrics(portfolioReturns);
  
    // Calcular desviación a la baja anualizada
    const negativeReturns = portfolioReturns.filter(r => r < 0);
    const downsideVariance = negativeReturns.reduce((sum, r) => sum + r ** 2, 0) / negativeReturns.length;
    const annualizedDownsideDeviation = Math.sqrt(downsideVariance * 252) * 100;
  
    // Calcular ratios y métricas
    const sharpeRatio = calculateAnnualizedSharpeRatio(annualizedReturn, riskFreeRate, annualizedStdDev);
    const sortinoRatio = calculateAnnualizedSortinoRatio(annualizedReturn, riskFreeRate, annualizedDownsideDeviation);
    const dailyVaR95 = calculateDailyVaR(portfolioReturns, 0.95);
    const annualizedVaR95 = dailyVaR95 * Math.sqrt(252) * 100; // Anualizar el VaR
    const correlation = calculateCorrelation(portfolioReturns, marketReturns);
  
    return {
      annualizedReturn,
      annualizedStdDev,
      sharpeRatio,
      sortinoRatio,
      annualizedVaR95,
      correlation
    };
  }
  
  // Ejemplo de uso
  async function main() {
    const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
    const startDate = '2024-01-01';
    const endDate = '2024-10-25';
    const riskFreeRate = 0.02; // 2% anual
  
    try {
      const results = await performRiskAnalysis(userId, startDate, endDate, riskFreeRate);
      console.log('Resultados del análisis de riesgo:', results);
    } catch (error) {
      console.error('Error al realizar el análisis de riesgo:', error);
    }
  }
  
  main();
  
  module.exports = {
    performRiskAnalysis,
    calculateAnnualizedSharpeRatio,
    calculateAnnualizedSortinoRatio,
    calculateDailyVaR,
    calculateCorrelation
  };