/**
 * Risk Metrics Types
 * 
 * Tipos y constantes compartidas para el sistema de métricas de riesgo del portafolio.
 * Estos tipos son la fuente de verdad para el backend.
 * 
 * @module services/riskMetrics/types
 * @see docs/stories/36.story.md
 */

/**
 * Períodos de análisis soportados
 * @typedef {'1M' | '3M' | '6M' | 'YTD' | '1Y' | '2Y' | 'ALL'} RiskMetricsPeriod
 */

/**
 * Estrategia de agregación multi-cuenta
 * @typedef {'overall' | 'single' | 'multi'} AggregationStrategy
 */

/**
 * Datos de rendimiento diario
 * @typedef {Object} DailyReturnData
 * @property {string} date - Fecha en formato ISO (YYYY-MM-DD)
 * @property {number} dailyReturn - Retorno diario como decimal (-0.02 = -2%)
 * @property {number} value - Valor del portafolio en la fecha
 * @property {number} investment - Inversión total en la fecha
 */

/**
 * Datos de rendimiento de mercado (benchmark)
 * @typedef {Object} MarketReturnData
 * @property {string} date - Fecha en formato ISO
 * @property {number} dailyReturn - Retorno diario del índice
 * @property {number} indexValue - Valor del índice
 */

/**
 * Punto de drawdown para gráfico
 * @typedef {Object} DrawdownDataPoint
 * @property {string} date - Fecha
 * @property {number} drawdown - Drawdown como decimal negativo
 * @property {number} value - Valor del portafolio
 * @property {number} peak - Valor máximo alcanzado
 */

/**
 * Resultado completo de métricas de riesgo
 * @typedef {Object} RiskMetricsResult
 * @property {boolean} success - Si el cálculo fue exitoso
 * @property {number} sharpeRatio - Ratio de Sharpe
 * @property {number} sortinoRatio - Ratio de Sortino
 * @property {number} beta - Beta vs benchmark
 * @property {number} volatility - Volatilidad anualizada
 * @property {number} maxDrawdown - Máximo drawdown como decimal
 * @property {number} valueAtRisk95 - VaR al 95% de confianza
 * @property {number} profitableWeeks - Porcentaje de semanas rentables
 * @property {DrawdownDataPoint[]} drawdownHistory - Historial de drawdowns
 * @property {RiskMetricsMetadata} metadata - Metadata del cálculo
 */

/**
 * Metadata del cálculo de métricas
 * @typedef {Object} RiskMetricsMetadata
 * @property {string} calculatedAt - Timestamp del cálculo
 * @property {string} period - Período analizado
 * @property {string} currency - Moneda utilizada
 * @property {string[]} accountsIncluded - IDs de cuentas incluidas
 * @property {AggregationStrategy} aggregationMethod - Método de agregación usado
 * @property {number} dataPointsCount - Número de puntos de datos
 * @property {string} dataQuality - Calidad de datos (excellent/good/limited/insufficient)
 */

/** Constantes de benchmark */
const DEFAULT_BENCHMARKS = {
  /** Tasa libre de riesgo anualizada (T-Bills ~5.5% en 2024) */
  RISK_FREE_RATE: 0.055,
  /** Índice de referencia para beta */
  BENCHMARK_INDEX: 'GSPC',
  /** Nombre del benchmark */
  BENCHMARK_NAME: 'S&P 500'
};

/** Días de trading por año (para anualización) */
const TRADING_DAYS_PER_YEAR = 252;

/** Mínimo de días para cálculos confiables */
const MIN_DAYS_FOR_METRICS = 30;

/** Claves de cache para benchmark data */
const CACHE_KEYS = {
  SP500_YTD: 'sp500_ytd',
  SP500_1Y: 'sp500_1y',
  SP500_2Y: 'sp500_2y',
  SP500_ALL: 'sp500_all',
  SECTOR_WEIGHTS: 'sector_weights'
};

/** TTL de cache en segundos (1 hora) */
const CACHE_TTL = 3600;

module.exports = {
  DEFAULT_BENCHMARKS,
  TRADING_DAYS_PER_YEAR,
  MIN_DAYS_FOR_METRICS,
  CACHE_KEYS,
  CACHE_TTL
};
