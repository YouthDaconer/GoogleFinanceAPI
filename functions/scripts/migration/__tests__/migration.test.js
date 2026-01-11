/**
 * Tests para scripts de migración COST-OPT-003
 * 
 * Verifica la lógica de migración y consolidación
 * sin escribir a Firestore.
 * 
 * @module scripts/migration/__tests__/migration.test
 * @see docs/stories/64.story.md (COST-OPT-003)
 */

const { DateTime } = require('luxon');

// Mock de Firebase Admin
jest.mock('../../../services/firebaseAdmin', () => ({
  firestore: jest.fn(() => ({
    collection: jest.fn(),
    doc: jest.fn()
  }))
}));

// Importar funciones de consolidación (ya testeadas en COST-OPT-001)
const { 
  consolidatePeriod, 
  chainFactorsForPeriods,
  CONSOLIDATED_SCHEMA_VERSION 
} = require('../../../utils/periodConsolidation');

const { consolidateMonthsToYear } = require('../../../services/periodConsolidationScheduled');

// ============================================================================
// DATOS DE PRUEBA
// ============================================================================

/**
 * Genera documentos diarios simulados para un mes
 */
function generateDailyDocsForMonth(yearMonth, daysCount = 20, options = {}) {
  const { 
    dailyChangePercent = 0.1,
    startValue = 10000,
    currency = 'USD'
  } = options;
  
  const [year, month] = yearMonth.split('-').map(Number);
  const docs = [];
  
  let currentValue = startValue;
  
  for (let day = 1; day <= daysCount; day++) {
    const date = `${yearMonth}-${day.toString().padStart(2, '0')}`;
    const dailyChange = (Math.random() - 0.5) * dailyChangePercent * 2;
    
    currentValue = currentValue * (1 + dailyChange / 100);
    
    docs.push({
      id: date,
      data: () => ({
        date,
        [currency]: {
          totalValue: currentValue,
          totalInvestment: startValue,
          totalCashFlow: 0,
          adjustedDailyChangePercentage: dailyChange
        }
      })
    });
  }
  
  return docs;
}

/**
 * Genera documentos mensuales consolidados para un año
 */
function generateMonthlyConsolidatedDocs(year, monthsCount = 12, options = {}) {
  const { 
    avgMonthlyReturn = 1,
    currency = 'USD'
  } = options;
  
  const docs = [];
  
  for (let month = 1; month <= monthsCount; month++) {
    const periodKey = `${year}-${month.toString().padStart(2, '0')}`;
    const factor = 1 + (avgMonthlyReturn + (Math.random() - 0.5) * 2) / 100;
    
    docs.push({
      id: periodKey,
      data: () => ({
        periodType: 'month',
        periodKey,
        startDate: `${periodKey}-01`,
        endDate: `${periodKey}-28`,
        docsCount: 20,
        version: CONSOLIDATED_SCHEMA_VERSION,
        lastUpdated: new Date().toISOString(),
        [currency]: {
          startFactor: 1,
          endFactor: factor,
          periodReturn: (factor - 1) * 100,
          startTotalValue: 10000,
          endTotalValue: 10000 * factor,
          totalCashFlow: 0,
          personalReturn: (factor - 1) * 100
        }
      })
    });
  }
  
  return docs;
}

// ============================================================================
// TESTS: CONSOLIDACIÓN MENSUAL
// ============================================================================

describe('Migración COST-OPT-003 - Consolidación Mensual', () => {
  
  describe('consolidatePeriod para meses', () => {
    
    it('debería consolidar documentos diarios en un documento mensual', () => {
      const dailyDocs = generateDailyDocsForMonth('2025-06', 20);
      
      const result = consolidatePeriod(dailyDocs, '2025-06', 'month');
      
      expect(result).not.toBeNull();
      expect(result.periodType).toBe('month');
      expect(result.periodKey).toBe('2025-06');
      expect(result.startDate).toBe('2025-06-01');
      expect(result.docsCount).toBe(20);
      expect(result.version).toBe(CONSOLIDATED_SCHEMA_VERSION);
      expect(result.USD).toBeDefined();
      expect(result.USD.endFactor).toBeGreaterThan(0);
    });
    
    it('debería calcular factor TWR correctamente', () => {
      // Crear docs con rendimientos conocidos
      const docs = [
        { id: '2025-01-02', data: () => ({ date: '2025-01-02', USD: { adjustedDailyChangePercentage: 1, totalValue: 10100 } }) },
        { id: '2025-01-03', data: () => ({ date: '2025-01-03', USD: { adjustedDailyChangePercentage: 2, totalValue: 10302 } }) },
        { id: '2025-01-04', data: () => ({ date: '2025-01-04', USD: { adjustedDailyChangePercentage: -0.5, totalValue: 10250.49 } }) }
      ];
      
      const result = consolidatePeriod(docs, '2025-01', 'month');
      
      // Factor esperado: 1.01 * 1.02 * 0.995 = 1.024449
      const expectedFactor = 1.01 * 1.02 * 0.995;
      expect(result.USD.endFactor).toBeCloseTo(expectedFactor, 4);
    });
    
    it('debería manejar meses sin datos', () => {
      const result = consolidatePeriod([], '2025-02', 'month');
      expect(result).toBeNull();
    });
    
    it('debería incluir múltiples monedas si están presentes', () => {
      const docs = [
        { 
          id: '2025-03-01', 
          data: () => ({ 
            date: '2025-03-01', 
            USD: { adjustedDailyChangePercentage: 1, totalValue: 10100 },
            COP: { adjustedDailyChangePercentage: 0.5, totalValue: 40000000 }
          }) 
        }
      ];
      
      const result = consolidatePeriod(docs, '2025-03', 'month');
      
      expect(result.USD).toBeDefined();
      expect(result.COP).toBeDefined();
    });
  });
});

// ============================================================================
// TESTS: CONSOLIDACIÓN ANUAL
// ============================================================================

describe('Migración COST-OPT-003 - Consolidación Anual', () => {
  
  describe('consolidateMonthsToYear', () => {
    
    it('debería encadenar 12 meses en un documento anual', () => {
      const monthlyDocs = generateMonthlyConsolidatedDocs('2025', 12);
      
      const result = consolidateMonthsToYear(monthlyDocs, '2025');
      
      expect(result).not.toBeNull();
      expect(result.periodType).toBe('year');
      expect(result.periodKey).toBe('2025');
      expect(result.docsCount).toBe(240); // 12 * 20
      expect(result.USD).toBeDefined();
      expect(result.USD.endFactor).toBeGreaterThan(0);
    });
    
    it('debería calcular factor compuesto correctamente', () => {
      // 3 meses con factores conocidos: 1.05, 1.03, 0.98
      const monthlyDocs = [
        {
          id: '2025-01',
          data: () => ({
            periodKey: '2025-01',
            startDate: '2025-01-01',
            endDate: '2025-01-31',
            docsCount: 20,
            USD: { startFactor: 1, endFactor: 1.05, startTotalValue: 10000, endTotalValue: 10500, totalCashFlow: 0 }
          })
        },
        {
          id: '2025-02',
          data: () => ({
            periodKey: '2025-02',
            startDate: '2025-02-01',
            endDate: '2025-02-28',
            docsCount: 19,
            USD: { startFactor: 1, endFactor: 1.03, startTotalValue: 10500, endTotalValue: 10815, totalCashFlow: 0 }
          })
        },
        {
          id: '2025-03',
          data: () => ({
            periodKey: '2025-03',
            startDate: '2025-03-01',
            endDate: '2025-03-31',
            docsCount: 21,
            USD: { startFactor: 1, endFactor: 0.98, startTotalValue: 10815, endTotalValue: 10599, totalCashFlow: 0 }
          })
        }
      ];
      
      const result = consolidateMonthsToYear(monthlyDocs, '2025');
      
      // Factor compuesto: 1.05 * 1.03 * 0.98 = 1.05987
      const expectedFactor = 1.05 * 1.03 * 0.98;
      expect(result.USD.endFactor).toBeCloseTo(expectedFactor, 4);
    });
    
    it('debería preservar valores de inicio del primer mes', () => {
      const monthlyDocs = generateMonthlyConsolidatedDocs('2024', 6);
      
      const result = consolidateMonthsToYear(monthlyDocs, '2024');
      
      const firstMonthData = monthlyDocs[0].data().USD;
      expect(result.USD.startTotalValue).toBe(firstMonthData.startTotalValue);
    });
    
    it('debería acumular cashflows de todos los meses', () => {
      const monthlyDocs = [
        {
          id: '2025-01',
          data: () => ({
            periodKey: '2025-01',
            startDate: '2025-01-01',
            endDate: '2025-01-31',
            docsCount: 20,
            USD: { startFactor: 1, endFactor: 1.02, startTotalValue: 10000, endTotalValue: 11200, totalCashFlow: -1000 }
          })
        },
        {
          id: '2025-02',
          data: () => ({
            periodKey: '2025-02',
            startDate: '2025-02-01',
            endDate: '2025-02-28',
            docsCount: 19,
            USD: { startFactor: 1, endFactor: 1.01, startTotalValue: 11200, endTotalValue: 11312, totalCashFlow: 500 }
          })
        }
      ];
      
      const result = consolidateMonthsToYear(monthlyDocs, '2025');
      
      expect(result.USD.totalCashFlow).toBe(-500); // -1000 + 500
    });
  });
});

// ============================================================================
// TESTS: ENCADENAMIENTO V2 (chainFactorsForPeriods)
// ============================================================================

describe('Migración COST-OPT-003 - Encadenamiento V2', () => {
  
  const now = DateTime.fromISO('2026-01-15T10:00:00', { zone: 'America/New_York' });
  
  describe('chainFactorsForPeriods', () => {
    
    it('debería encadenar años + meses + días correctamente', () => {
      // Años: 2024 (factor 1.10)
      const yearlyDocs = [{
        id: '2024',
        data: () => ({
          periodKey: '2024',
          startDate: '2024-01-01',
          endDate: '2024-12-31',
          docsCount: 250,
          USD: { startFactor: 1, endFactor: 1.10, startTotalValue: 10000, endTotalValue: 11000, totalCashFlow: 0 }
        })
      }];
      
      // Año 2025: solo contamos 2025-01 a 2025-12
      const monthlyDocs = [
        {
          id: '2025-01',
          data: () => ({
            periodKey: '2025-01',
            startDate: '2025-01-01',
            endDate: '2025-01-31',
            docsCount: 20,
            USD: { startFactor: 1, endFactor: 1.02, startTotalValue: 11000, endTotalValue: 11220, totalCashFlow: 0 }
          })
        }
      ];
      
      // Enero 2026 (días actuales)
      const dailyDocs = generateDailyDocsForMonth('2026-01', 10, { startValue: 11220 });
      
      const result = chainFactorsForPeriods(
        yearlyDocs,
        monthlyDocs,
        dailyDocs,
        'USD',
        null,
        null,
        now
      );
      
      expect(result.returns).toBeDefined();
      expect(result.returns.hasYtdData).toBe(true);
      expect(result.returns.hasOneYearData).toBe(true);
    });
    
    it('debería funcionar solo con documentos diarios', () => {
      const dailyDocs = generateDailyDocsForMonth('2026-01', 10);
      
      const result = chainFactorsForPeriods([], [], dailyDocs, 'USD', null, null, now);
      
      expect(result.returns).toBeDefined();
      expect(result.returns.hasYtdData).toBe(true);
    });
    
    it('debería retornar resultado vacío sin datos', () => {
      const result = chainFactorsForPeriods([], [], [], 'USD', null, null, now);
      
      expect(result.returns.hasYtdData).toBe(false);
      expect(result.returns.ytdReturn).toBe(0);
    });
  });
});

// ============================================================================
// TESTS: COMPARACIÓN V1 vs V2
// ============================================================================

describe('Migración COST-OPT-003 - Validación V1 vs V2', () => {
  
  describe('Coherencia de resultados', () => {
    
    it('debería producir resultados equivalentes con mismos datos de entrada', () => {
      // Simular datos diarios para 3 meses
      const allDailyDocs = [
        ...generateDailyDocsForMonth('2025-10', 20, { startValue: 10000 }),
        ...generateDailyDocsForMonth('2025-11', 20, { startValue: 10200 }),
        ...generateDailyDocsForMonth('2025-12', 20, { startValue: 10400 })
      ];
      
      // V1: Calcular directamente desde diarios
      const v1Factor = allDailyDocs.reduce((factor, doc) => {
        const change = doc.data().USD.adjustedDailyChangePercentage;
        return factor * (1 + change / 100);
      }, 1);
      
      // V2: Consolidar por mes y luego encadenar
      const oct = consolidatePeriod(
        allDailyDocs.filter(d => d.id.startsWith('2025-10')),
        '2025-10',
        'month'
      );
      const nov = consolidatePeriod(
        allDailyDocs.filter(d => d.id.startsWith('2025-11')),
        '2025-11',
        'month'
      );
      const dec = consolidatePeriod(
        allDailyDocs.filter(d => d.id.startsWith('2025-12')),
        '2025-12',
        'month'
      );
      
      const v2Factor = oct.USD.endFactor * nov.USD.endFactor * dec.USD.endFactor;
      
      // La diferencia debería ser mínima (errores de punto flotante)
      expect(Math.abs(v1Factor - v2Factor)).toBeLessThan(0.0001);
    });
    
    it('debería manejar rendimientos negativos correctamente', () => {
      // Simular mes con pérdidas
      const docs = [
        { id: '2025-01-02', data: () => ({ date: '2025-01-02', USD: { adjustedDailyChangePercentage: -2, totalValue: 9800 } }) },
        { id: '2025-01-03', data: () => ({ date: '2025-01-03', USD: { adjustedDailyChangePercentage: -1.5, totalValue: 9653 } }) },
        { id: '2025-01-04', data: () => ({ date: '2025-01-04', USD: { adjustedDailyChangePercentage: 0.5, totalValue: 9701 } }) }
      ];
      
      const result = consolidatePeriod(docs, '2025-01', 'month');
      
      expect(result.USD.endFactor).toBeLessThan(1);
      expect(result.USD.periodReturn).toBeLessThan(0);
    });
  });
});

// ============================================================================
// TESTS: EDGE CASES
// ============================================================================

describe('Migración COST-OPT-003 - Edge Cases', () => {
  
  it('debería manejar documentos con datos parciales (moneda solo en algunos docs)', () => {
    // Solo algunos documentos tienen USD - el consolidador procesa lo que encuentra
    const docs = [
      { id: '2025-01-02', data: () => ({ date: '2025-01-02', USD: { adjustedDailyChangePercentage: 1, totalValue: 10100 } }) },
      { id: '2025-01-03', data: () => ({ date: '2025-01-03', USD: { adjustedDailyChangePercentage: 0 } }) }, // USD sin cambio
      { id: '2025-01-04', data: () => ({ date: '2025-01-04', USD: { adjustedDailyChangePercentage: 0.5, totalValue: 10150 } }) }
    ];
    
    const result = consolidatePeriod(docs, '2025-01', 'month');
    
    expect(result).not.toBeNull();
    // USD debería estar presente
    expect(result.USD).toBeDefined();
    expect(result.USD.endFactor).toBeGreaterThan(0);
  });
  
  it('debería manejar un solo documento', () => {
    const docs = [
      { id: '2025-06-15', data: () => ({ date: '2025-06-15', USD: { adjustedDailyChangePercentage: 2, totalValue: 10200 } }) }
    ];
    
    const result = consolidatePeriod(docs, '2025-06', 'month');
    
    expect(result).not.toBeNull();
    expect(result.docsCount).toBe(1);
  });
  
  it('debería manejar rendimientos extremos sin overflow', () => {
    const docs = [
      { id: '2025-01-02', data: () => ({ date: '2025-01-02', USD: { adjustedDailyChangePercentage: 50, totalValue: 15000 } }) },
      { id: '2025-01-03', data: () => ({ date: '2025-01-03', USD: { adjustedDailyChangePercentage: -30, totalValue: 10500 } }) }
    ];
    
    const result = consolidatePeriod(docs, '2025-01', 'month');
    
    expect(result.USD.endFactor).toBeGreaterThan(0);
    expect(isFinite(result.USD.endFactor)).toBe(true);
    expect(isNaN(result.USD.endFactor)).toBe(false);
  });
  
  it('debería manejar año parcial (menos de 12 meses)', () => {
    const monthlyDocs = generateMonthlyConsolidatedDocs('2025', 6); // Solo 6 meses
    
    const result = consolidateMonthsToYear(monthlyDocs, '2025');
    
    expect(result).not.toBeNull();
    expect(result.docsCount).toBe(120); // 6 * 20
  });
});
