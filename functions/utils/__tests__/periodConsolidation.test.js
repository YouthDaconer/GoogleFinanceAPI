/**
 * Tests para periodConsolidation.js
 * 
 * COST-OPT-001: Verifica que las funciones de consolidación de períodos
 * calculen correctamente los factores TWR y encadenen períodos.
 * 
 * @module __tests__/utils/periodConsolidation.test
 * @see docs/stories/62.story.md (COST-OPT-001)
 */

const { DateTime } = require('luxon');
const {
  // Consolidación
  consolidatePeriod,
  consolidateCurrencyData,
  extractCurrenciesFromDocs,
  
  // Encadenamiento
  chainFactorsForPeriods,
  calculatePeriodBoundaries,
  initializePeriodFactors,
  processConsolidatedPeriod,
  processDailyDocument,
  buildReturnsResult,
  
  // Utilidades
  isMonthClosed,
  isYearClosed,
  getNextMonth,
  getMonthsBetween,
  calculateModifiedDietzSimple,
  
  // Constantes
  CONSOLIDATED_SCHEMA_VERSION
} = require('../periodConsolidation');

// ============================================================================
// DATOS DE PRUEBA
// ============================================================================

/**
 * Genera documentos diarios de prueba para un mes
 */
function generateDailyDocs(monthKey, daysCount = 20, options = {}) {
  const { 
    currency = 'USD',
    baseValue = 10000,
    dailyChangePercent = 0.5,
    includeCashFlow = false
  } = options;
  
  const [year, month] = monthKey.split('-').map(Number);
  const docs = [];
  
  for (let day = 1; day <= daysCount; day++) {
    const date = DateTime.fromObject({ year, month, day }).toISODate();
    const dayIndex = day - 1;
    
    // Calcular valor acumulado
    const factor = Math.pow(1 + dailyChangePercent / 100, dayIndex);
    const totalValue = baseValue * factor;
    
    const doc = {
      date,
      [currency]: {
        totalValue,
        totalInvestment: baseValue,
        adjustedDailyChangePercentage: dailyChangePercent,
        dailyChangePercentage: dailyChangePercent,
        totalCashFlow: includeCashFlow && day === 10 ? -1000 : 0, // Depósito día 10
        assetPerformance: {
          'AAPL_stock': {
            totalValue: totalValue * 0.4,
            adjustedDailyChangePercentage: dailyChangePercent * 1.2
          },
          'GOOGL_stock': {
            totalValue: totalValue * 0.6,
            adjustedDailyChangePercentage: dailyChangePercent * 0.8
          }
        }
      }
    };
    
    docs.push(doc);
  }
  
  return docs;
}

/**
 * Genera un documento consolidado de mes para pruebas
 */
function generateConsolidatedMonth(periodKey, options = {}) {
  const {
    currency = 'USD',
    startFactor = 1,
    endFactor = 1.05,
    startValue = 10000,
    endValue = 10500
  } = options;
  
  return {
    periodType: 'month',
    periodKey,
    startDate: `${periodKey}-01`,
    endDate: `${periodKey}-28`,
    docsCount: 20,
    version: CONSOLIDATED_SCHEMA_VERSION,
    lastUpdated: new Date().toISOString(),
    [currency]: {
      startFactor,
      endFactor,
      periodReturn: (endFactor / startFactor - 1) * 100,
      startTotalValue: startValue,
      endTotalValue: endValue,
      totalCashFlow: 0,
      personalReturn: 5,
      assetPerformance: {
        'AAPL_stock': {
          startFactor: 1,
          endFactor: 1.06,
          periodReturn: 6,
          startTotalValue: 4000,
          endTotalValue: 4240
        }
      }
    }
  };
}

/**
 * Genera un documento consolidado de año para pruebas
 */
function generateConsolidatedYear(yearKey, options = {}) {
  const {
    currency = 'USD',
    startFactor = 1,
    endFactor = 1.15,
    startValue = 10000,
    endValue = 11500
  } = options;
  
  return {
    periodType: 'year',
    periodKey: yearKey,
    startDate: `${yearKey}-01-01`,
    endDate: `${yearKey}-12-31`,
    docsCount: 252,
    version: CONSOLIDATED_SCHEMA_VERSION,
    lastUpdated: new Date().toISOString(),
    [currency]: {
      startFactor,
      endFactor,
      periodReturn: (endFactor / startFactor - 1) * 100,
      startTotalValue: startValue,
      endTotalValue: endValue,
      totalCashFlow: 0,
      personalReturn: 15
    }
  };
}

// ============================================================================
// TESTS: CONSOLIDACIÓN
// ============================================================================

describe('periodConsolidation', () => {
  
  describe('consolidatePeriod', () => {
    
    it('should return null for empty docs array', () => {
      const result = consolidatePeriod([], '2025-12', 'month');
      expect(result).toBeNull();
    });
    
    it('should return null for null docs', () => {
      const result = consolidatePeriod(null, '2025-12', 'month');
      expect(result).toBeNull();
    });
    
    it('should consolidate daily docs into a single period document', () => {
      const dailyDocs = generateDailyDocs('2025-12', 20, { dailyChangePercent: 0.5 });
      
      const result = consolidatePeriod(dailyDocs, '2025-12', 'month');
      
      expect(result).not.toBeNull();
      expect(result.periodType).toBe('month');
      expect(result.periodKey).toBe('2025-12');
      expect(result.docsCount).toBe(20);
      expect(result.version).toBe(CONSOLIDATED_SCHEMA_VERSION);
      expect(result.startDate).toBe('2025-12-01');
      expect(result.endDate).toBe('2025-12-20');
    });
    
    it('should calculate correct TWR factor for positive returns', () => {
      // 20 días con 0.5% diario = factor de ~1.105
      const dailyDocs = generateDailyDocs('2025-12', 20, { dailyChangePercent: 0.5 });
      
      const result = consolidatePeriod(dailyDocs, '2025-12', 'month');
      
      // Factor esperado: 1.005^20 ≈ 1.1049
      const expectedFactor = Math.pow(1.005, 20);
      expect(result.USD.endFactor).toBeCloseTo(expectedFactor, 3);
      expect(result.USD.periodReturn).toBeCloseTo((expectedFactor - 1) * 100, 1);
    });
    
    it('should calculate correct TWR factor for negative returns', () => {
      const dailyDocs = generateDailyDocs('2025-12', 20, { dailyChangePercent: -0.3 });
      
      const result = consolidatePeriod(dailyDocs, '2025-12', 'month');
      
      // Factor esperado: 0.997^20 ≈ 0.9418
      const expectedFactor = Math.pow(0.997, 20);
      expect(result.USD.endFactor).toBeCloseTo(expectedFactor, 3);
      expect(result.USD.periodReturn).toBeLessThan(0);
    });
    
    it('should include asset performance data', () => {
      const dailyDocs = generateDailyDocs('2025-12', 10);
      
      const result = consolidatePeriod(dailyDocs, '2025-12', 'month');
      
      expect(result.USD.assetPerformance).toBeDefined();
      expect(result.USD.assetPerformance['AAPL_stock']).toBeDefined();
      expect(result.USD.assetPerformance['GOOGL_stock']).toBeDefined();
      expect(result.USD.assetPerformance['AAPL_stock'].endFactor).toBeGreaterThan(1);
    });
    
    it('should handle documents with cashflow', () => {
      const dailyDocs = generateDailyDocs('2025-12', 20, { includeCashFlow: true });
      
      const result = consolidatePeriod(dailyDocs, '2025-12', 'month');
      
      // Cashflow negativo significa depósito
      expect(result.USD.totalCashFlow).toBe(-1000);
      expect(result.USD.personalReturn).toBeDefined();
    });
    
    it('should handle multiple currencies', () => {
      const dailyDocs = [
        {
          date: '2025-12-01',
          USD: { totalValue: 10000, adjustedDailyChangePercentage: 1 },
          COP: { totalValue: 40000000, adjustedDailyChangePercentage: 0.5 }
        },
        {
          date: '2025-12-02',
          USD: { totalValue: 10100, adjustedDailyChangePercentage: 1 },
          COP: { totalValue: 40200000, adjustedDailyChangePercentage: 0.5 }
        }
      ];
      
      const result = consolidatePeriod(dailyDocs, '2025-12', 'month');
      
      expect(result.USD).toBeDefined();
      expect(result.COP).toBeDefined();
      expect(result.USD.endFactor).toBeCloseTo(1.0201, 3);
      expect(result.COP.endFactor).toBeCloseTo(1.01002, 3);
    });
  });
  
  describe('extractCurrenciesFromDocs', () => {
    
    it('should extract all currencies from docs', () => {
      const docs = [
        { date: '2025-01-01', USD: { totalValue: 100 }, EUR: { totalValue: 90 } },
        { date: '2025-01-02', USD: { totalValue: 101 }, COP: { totalValue: 400000 } }
      ];
      
      const currencies = extractCurrenciesFromDocs(docs);
      
      expect(currencies.size).toBe(3);
      expect(currencies.has('USD')).toBe(true);
      expect(currencies.has('EUR')).toBe(true);
      expect(currencies.has('COP')).toBe(true);
    });
    
    it('should not include date field as currency', () => {
      const docs = [{ date: '2025-01-01', USD: { totalValue: 100 } }];
      
      const currencies = extractCurrenciesFromDocs(docs);
      
      expect(currencies.has('date')).toBe(false);
    });
  });
  
  describe('calculateModifiedDietzSimple', () => {
    
    it('should return 0 when no start value and no cashflow', () => {
      const result = calculateModifiedDietzSimple(0, 0, 0, [], 20);
      expect(result).toBe(0);
    });
    
    it('should calculate simple return without cashflow', () => {
      // Inicio: 10000, Fin: 11000, Sin cashflow = 10% return
      const result = calculateModifiedDietzSimple(10000, 11000, 0, [], 20);
      expect(result).toBeCloseTo(10, 1);
    });
    
    it('should adjust for deposits (negative cashflow)', () => {
      // Inicio: 10000, Fin: 12000, Depósito: 1000
      // Ganancia real: 12000 - 10000 - 1000 = 1000
      // Base de inversión: 10000 + 500 = 10500
      // Return: 1000 / 10500 ≈ 9.52%
      const result = calculateModifiedDietzSimple(10000, 12000, -1000, [], 20);
      expect(result).toBeCloseTo(9.52, 0);
    });
    
    it('should adjust for withdrawals (positive cashflow)', () => {
      // Inicio: 10000, Fin: 9500, Retiro: 1000
      // Ganancia real: 9500 - 10000 + 1000 = 500
      // Base de inversión: 10000 - 500 = 9500
      // Return: 500 / 9500 ≈ 5.26%
      const result = calculateModifiedDietzSimple(10000, 9500, 1000, [], 20);
      expect(result).toBeCloseTo(5.26, 0);
    });
  });
});

// ============================================================================
// TESTS: ENCADENAMIENTO DE FACTORES
// ============================================================================

describe('chainFactorsForPeriods', () => {
  
  const now = DateTime.fromISO('2026-01-15').setZone('America/New_York');
  
  describe('calculatePeriodBoundaries', () => {
    
    it('should calculate correct boundaries for all periods', () => {
      const boundaries = calculatePeriodBoundaries(now);
      
      expect(boundaries.ytd).toBe('2026-01-01');
      expect(boundaries.oneMonth).toBe('2025-12-15');
      expect(boundaries.threeMonths).toBe('2025-10-15');
      expect(boundaries.sixMonths).toBe('2025-07-15');
      expect(boundaries.oneYear).toBe('2025-01-15');
      expect(boundaries.twoYears).toBe('2024-01-15');
      expect(boundaries.fiveYears).toBe('2021-01-15');
    });
  });
  
  describe('initializePeriodFactors', () => {
    
    it('should initialize all periods with factor 1', () => {
      const factors = initializePeriodFactors();
      
      expect(Object.keys(factors)).toHaveLength(7);
      expect(factors.ytd.startFactor).toBe(1);
      expect(factors.oneYear.currentFactor).toBe(1);
      expect(factors.fiveYears.found).toBe(false);
      expect(factors.oneMonth.docsCount).toBe(0);
    });
  });
  
  describe('chainFactorsForPeriods - integration', () => {
    
    it('should chain yearly and monthly factors correctly', () => {
      const yearlyDocs = [
        generateConsolidatedYear('2024', { endFactor: 1.15 }), // +15%
        generateConsolidatedYear('2025', { endFactor: 1.10 })  // +10%
      ];
      
      const monthlyDocs = []; // Año actual sin meses consolidados aún
      
      const dailyDocs = generateDailyDocs('2026-01', 15, { dailyChangePercent: 0.2 });
      
      const result = chainFactorsForPeriods(
        yearlyDocs, 
        monthlyDocs, 
        dailyDocs, 
        'USD', 
        null, 
        null, 
        now
      );
      
      expect(result.returns.hasOneYearData).toBe(true);
      expect(result.returns.hasTwoYearData).toBe(true);
      expect(result.returns.oneYearReturn).toBeGreaterThan(0);
      expect(result.consolidatedVersion).toBe(true);
    });
    
    it('should return empty result when no data', () => {
      const result = chainFactorsForPeriods([], [], [], 'USD', null, null, now);
      
      expect(result.returns.ytdReturn).toBe(0);
      expect(result.returns.hasYtdData).toBe(false);
      expect(result.totalValueData.dates).toHaveLength(0);
    });
    
    it('should handle only daily docs for current month', () => {
      const dailyDocs = generateDailyDocs('2026-01', 15, { dailyChangePercent: 0.5 });
      
      const result = chainFactorsForPeriods([], [], dailyDocs, 'USD', null, null, now);
      
      expect(result.returns.hasYtdData).toBe(true);
      expect(result.returns.ytdReturn).toBeGreaterThan(0);
      // Los datos de enero también cubren el período de 1 mes (15 días >= 15 dic)
      // Esto es correcto porque estamos en enero 15 y 1 mes atrás es dic 15
      expect(result.returns.hasOneMonthData).toBe(true);
    });
    
    it('should include validDocsCountByPeriod', () => {
      const yearlyDocs = [generateConsolidatedYear('2025')];
      const monthlyDocs = [
        generateConsolidatedMonth('2025-11'),
        generateConsolidatedMonth('2025-12')
      ];
      const dailyDocs = generateDailyDocs('2026-01', 10);
      
      const result = chainFactorsForPeriods(
        yearlyDocs, monthlyDocs, dailyDocs, 'USD', null, null, now
      );
      
      expect(result.validDocsCountByPeriod).toBeDefined();
      expect(result.validDocsCountByPeriod.ytd).toBeGreaterThan(0);
    });
    
    it('should calculate personal returns (MWR)', () => {
      const yearlyDocs = [generateConsolidatedYear('2025')];
      const dailyDocs = generateDailyDocs('2026-01', 10);
      
      const result = chainFactorsForPeriods(
        yearlyDocs, [], dailyDocs, 'USD', null, null, now
      );
      
      expect(result.returns.ytdPersonalReturn).toBeDefined();
      expect(result.returns.oneYearPersonalReturn).toBeDefined();
    });
  });
  
  describe('processConsolidatedPeriod', () => {
    
    it('should update factors for matching periods', () => {
      const factors = initializePeriodFactors();
      const boundaries = calculatePeriodBoundaries(now);
      
      const currencyData = {
        startFactor: 1,
        endFactor: 1.05,
        startTotalValue: 10000,
        endTotalValue: 10500,
        totalCashFlow: 0
      };
      
      // Usar período que claramente cae dentro del rango de oneMonth
      // now es 2026-01-15, oneMonth boundary es 2025-12-15
      // Diciembre 2025 (01-31) incluye días después de dic 15, así que SÍ aplica
      processConsolidatedPeriod(
        factors, boundaries, currencyData, 
        '2025-12-16', '2025-12-31', 10
      );
      
      // oneMonth incluye diciembre 2025 (después del día 15)
      expect(factors.oneMonth.found).toBe(true);
      expect(factors.oneMonth.currentFactor).toBeCloseTo(1.05, 2);
      expect(factors.oneMonth.docsCount).toBe(10);
    });
    
    it('should not update factors for periods outside range', () => {
      const factors = initializePeriodFactors();
      const boundaries = calculatePeriodBoundaries(now);
      
      const currencyData = {
        startFactor: 1,
        endFactor: 1.05
      };
      
      // Período muy antiguo (10 años atrás)
      processConsolidatedPeriod(
        factors, boundaries, currencyData,
        '2010-01-01', '2010-12-31', 252
      );
      
      // fiveYears no debería incluir datos de hace 10 años
      expect(factors.fiveYears.found).toBe(false);
    });
  });
  
  describe('processDailyDocument', () => {
    
    it('should update factors with daily change', () => {
      const factors = initializePeriodFactors();
      const boundaries = calculatePeriodBoundaries(now);
      
      const currencyData = {
        totalValue: 10100,
        adjustedDailyChangePercentage: 1.0,
        totalCashFlow: 0
      };
      
      processDailyDocument(factors, boundaries, currencyData, '2026-01-10');
      
      expect(factors.ytd.found).toBe(true);
      expect(factors.ytd.currentFactor).toBeCloseTo(1.01, 2);
      expect(factors.ytd.docsCount).toBe(1);
    });
  });
});

// ============================================================================
// TESTS: UTILIDADES
// ============================================================================

describe('utility functions', () => {
  
  describe('isMonthClosed', () => {
    const now = DateTime.fromISO('2026-01-15');
    
    it('should return true for past months', () => {
      expect(isMonthClosed('2025-12', now)).toBe(true);
      expect(isMonthClosed('2025-01', now)).toBe(true);
      expect(isMonthClosed('2020-06', now)).toBe(true);
    });
    
    it('should return false for current month', () => {
      expect(isMonthClosed('2026-01', now)).toBe(false);
    });
    
    it('should return false for future months', () => {
      expect(isMonthClosed('2026-02', now)).toBe(false);
      expect(isMonthClosed('2027-01', now)).toBe(false);
    });
  });
  
  describe('isYearClosed', () => {
    const now = DateTime.fromISO('2026-01-15');
    
    it('should return true for past years', () => {
      expect(isYearClosed('2025', now)).toBe(true);
      expect(isYearClosed('2020', now)).toBe(true);
    });
    
    it('should return false for current year', () => {
      expect(isYearClosed('2026', now)).toBe(false);
    });
    
    it('should return false for future years', () => {
      expect(isYearClosed('2027', now)).toBe(false);
    });
  });
  
  describe('getNextMonth', () => {
    
    it('should return next month', () => {
      expect(getNextMonth('2025-01')).toBe('2025-02');
      expect(getNextMonth('2025-06')).toBe('2025-07');
      expect(getNextMonth('2025-11')).toBe('2025-12');
    });
    
    it('should handle year boundary', () => {
      expect(getNextMonth('2025-12')).toBe('2026-01');
    });
  });
  
  describe('getMonthsBetween', () => {
    
    it('should return all months in range', () => {
      const months = getMonthsBetween('2025-10-01', '2026-01-15');
      
      expect(months).toEqual(['2025-10', '2025-11', '2025-12', '2026-01']);
    });
    
    it('should return single month for same month', () => {
      const months = getMonthsBetween('2025-06-01', '2025-06-30');
      
      expect(months).toEqual(['2025-06']);
    });
    
    it('should handle year boundary', () => {
      const months = getMonthsBetween('2024-11-01', '2025-02-28');
      
      expect(months).toEqual(['2024-11', '2024-12', '2025-01', '2025-02']);
    });
  });
});
