/**
 * VS-009: Servicio de generación de benchmark snapshots.
 *
 * Genera y persiste timelines históricos de índices de referencia (SPY, QQQ)
 * en la colección benchmarkSnapshots de Firestore. Se ejecuta como paso
 * best-effort dentro del pipeline EOD (unifiedMarketDataUpdate).
 *
 * @see docs/stories/VS-009.story.md
 */

const { DateTime } = require('luxon');
const { getPricesFromApi } = require('./marketDataHelper');
const { convertCurrency } = require('../utils/portfolioCalculations');
const {
  calculatePeriodBoundaries,
  initializePeriodFactors,
  processDailyDocument,
} = require('../utils/periodConsolidation');

const BENCHMARK_CONFIG = [
  { id: 'SPY_etf', ticker: 'SPY', name: 'S&P 500 (SPY)' },
  { id: 'QQQ_etf', ticker: 'QQQ', name: 'NASDAQ 100 (QQQ)' },
];

/**
 * Calcula returns TWR (YTD, 1M, 3M, 6M, 1Y, 2Y, 5Y) desde un timeline de puntos {d, v, c}.
 * Función pura sin side effects — testeable unitariamente.
 */
function computeReturnsFromTimeline(timeline) {
  if (!timeline || timeline.length === 0) {
    return {
      ytdReturn: 0,
      oneMonthReturn: 0,
      threeMonthReturn: 0,
      sixMonthReturn: 0,
      oneYearReturn: 0,
      twoYearReturn: null,
      fiveYearReturn: null,
    };
  }

  const now = DateTime.now().setZone('America/New_York');
  const boundaries = calculatePeriodBoundaries(now);
  const factors = initializePeriodFactors();

  for (const point of timeline) {
    processDailyDocument(factors, boundaries, {
      adjustedDailyChangePercentage: point.c,
      totalValue: point.v,
      totalCashFlow: 0,
    }, point.d);
  }

  const calcReturn = (pf) => {
    if (!pf.found || pf.startFactor === 0) return 0;
    return (pf.currentFactor / pf.startFactor - 1) * 100;
  };

  return {
    ytdReturn: calcReturn(factors.ytd),
    oneMonthReturn: calcReturn(factors.oneMonth),
    threeMonthReturn: calcReturn(factors.threeMonths),
    sixMonthReturn: calcReturn(factors.sixMonths),
    oneYearReturn: calcReturn(factors.oneYear),
    twoYearReturn: factors.twoYears.found ? calcReturn(factors.twoYears) : null,
    fiveYearReturn: factors.fiveYears.found ? calcReturn(factors.fiveYears) : null,
  };
}

/**
 * Extrae quotes de benchmarks del array de pipelineQuotes.
 * Si no se encuentran, hace fetch dedicado via API.
 */
async function resolveBenchmarkQuotes(pipelineQuotes) {
  const tickers = BENCHMARK_CONFIG.map(b => b.ticker);
  const found = (pipelineQuotes || []).filter(q => tickers.includes(q.symbol));

  if (found.length === tickers.length) {
    return found;
  }

  const missing = tickers.filter(t => !found.some(q => q.symbol === t));
  const fetched = await getPricesFromApi(missing);
  return [...found, ...fetched];
}

/**
 * Genera/actualiza benchmarkSnapshots para todos los benchmarks y monedas activas.
 *
 * @param {FirebaseFirestore.Firestore} db Instancia de Firestore
 * @param {Object[]} pipelineQuotes Array de quotes normalizados del pipeline EOD
 * @param {Object[]} currencies Array de monedas activas con exchangeRate
 * @param {import('luxon').DateTime} tradingDay Día de trading procesado
 * @param {Object} logger StructuredLogger del pipeline
 * @returns {{success: number, failed: number, skipped: number}}
 */
async function updateBenchmarkSnapshots(db, pipelineQuotes, currencies, tradingDay, logger) {
  const result = { success: 0, failed: 0, skipped: 0 };
  const dateStr = tradingDay.toISODate();

  const quotes = await resolveBenchmarkQuotes(pipelineQuotes);

  const activeCurrencies = ['USD', ...(currencies || [])
    .filter(c => c.isActive && c.code !== 'USD')
    .map(c => c.code)];

  for (const benchmark of BENCHMARK_CONFIG) {
    const quote = quotes.find(q => q.symbol === benchmark.ticker);

    if (!quote || !quote.price) {
      logger.warn(`[VS-009] Skipping ${benchmark.ticker}: no quote available`);
      result.skipped++;
      continue;
    }

    try {
      const batch = db.batch();
      const docsToWrite = [];

      for (const currency of activeCurrencies) {
        const docId = `${benchmark.id}_${currency}`;
        const docRef = db.collection('benchmarkSnapshots').doc(docId);

        const existingDoc = await docRef.get();
        const existingData = existingDoc.exists ? existingDoc.data() : {};
        const timeline = existingData.timeline ? [...existingData.timeline] : [];

        if (timeline.length > 0 && timeline[timeline.length - 1].d === dateStr) {
          result.skipped++;
          continue;
        }

        const price = currency === 'USD'
          ? quote.price
          : convertCurrency(quote.price, 'USD', currency, currencies);

        timeline.push({
          d: dateStr,
          v: Math.round(price * 100) / 100,
          c: quote.percentChange,
        });

        const returns = computeReturnsFromTimeline(timeline);

        batch.set(docRef, {
          benchmarkId: benchmark.id,
          currency,
          timeline,
          returns,
          lastUpdated: DateTime.now().toISO(),
          dataPoints: timeline.length,
          ticker: benchmark.ticker,
          name: benchmark.name,
        });

        docsToWrite.push(docId);
      }

      if (docsToWrite.length > 0) {
        await batch.commit();
        result.success += docsToWrite.length;
        logger.info(`[VS-009] ${benchmark.ticker} written: ${docsToWrite.join(', ')}`);
      }
    } catch (error) {
      logger.warn(`[VS-009] Failed to write ${benchmark.ticker}`, { error: error.message });
      result.failed++;
    }
  }

  return result;
}

module.exports = { updateBenchmarkSnapshots };

if (process.env.NODE_ENV === 'test') {
  module.exports._testExports = {
    computeReturnsFromTimeline,
    resolveBenchmarkQuotes,
    BENCHMARK_CONFIG,
  };
}
