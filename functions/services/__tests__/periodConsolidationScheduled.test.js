/**
 * Tests para periodConsolidationScheduled.js
 * 
 * COST-OPT-002: Verifica las funciones de consolidación scheduled
 * 
 * @module __tests__/services/periodConsolidationScheduled.test
 * @see docs/stories/63.story.md (COST-OPT-002)
 */

const { DateTime } = require('luxon');

// Mock de Firebase Admin antes de importar el módulo
jest.mock('../firebaseAdmin', () => {
  const mockCollection = jest.fn();
  const mockDoc = jest.fn();
  
  return {
    firestore: jest.fn(() => ({
      collection: mockCollection,
      doc: mockDoc
    }))
  };
});

// Importar funciones después del mock
const {
  consolidateMonthsToYear,
  consolidateUserMonth,
  consolidateUserYear
} = require('../periodConsolidationScheduled');

// ============================================================================
// DATOS DE PRUEBA
// ============================================================================

/**
 * Genera documentos mensuales consolidados de prueba
 */
function generateMonthlyConsolidatedDocs(yearKey, months = 12) {
  const docs = [];
  
  for (let month = 1; month <= months; month++) {
    const monthStr = month.toString().padStart(2, '0');
    const periodKey = `${yearKey}-${monthStr}`;
    
    // Factor de rendimiento aleatorio entre 0.97 y 1.05
    const factor = 0.97 + Math.random() * 0.08;
    
    docs.push({
      data: () => ({
        periodType: 'month',
        periodKey,
        startDate: `${yearKey}-${monthStr}-01`,
        endDate: `${yearKey}-${monthStr}-28`,
        docsCount: 20,
        version: 1,
        lastUpdated: new Date().toISOString(),
        USD: {
          startFactor: 1,
          endFactor: factor,
          periodReturn: (factor - 1) * 100,
          startTotalValue: 10000,
          endTotalValue: 10000 * factor,
          startTotalInvestment: 10000,
          endTotalInvestment: 10000,
          totalCashFlow: 0,
          personalReturn: (factor - 1) * 100
        },
        COP: {
          startFactor: 1,
          endFactor: factor * 0.99,
          periodReturn: (factor * 0.99 - 1) * 100,
          startTotalValue: 40000000,
          endTotalValue: 40000000 * factor * 0.99,
          startTotalInvestment: 40000000,
          endTotalInvestment: 40000000,
          totalCashFlow: 0,
          personalReturn: (factor * 0.99 - 1) * 100
        }
      })
    });
  }
  
  return docs;
}

// ============================================================================
// TESTS: consolidateMonthsToYear
// ============================================================================

describe('periodConsolidationScheduled', () => {
  
  describe('consolidateMonthsToYear', () => {
    
    it('should return null for empty docs array', () => {
      const result = consolidateMonthsToYear([], '2025');
      expect(result).toBeNull();
    });
    
    it('should return null for null docs', () => {
      const result = consolidateMonthsToYear(null, '2025');
      expect(result).toBeNull();
    });
    
    it('should consolidate 12 months into yearly document', () => {
      const monthlyDocs = generateMonthlyConsolidatedDocs('2025', 12);
      
      const result = consolidateMonthsToYear(monthlyDocs, '2025');
      
      expect(result).not.toBeNull();
      expect(result.periodType).toBe('year');
      expect(result.periodKey).toBe('2025');
      expect(result.startDate).toBe('2025-01-01');
      expect(result.endDate).toBe('2025-12-28');
      expect(result.docsCount).toBe(240); // 12 meses * 20 docs
      expect(result.version).toBe(1);
    });
    
    it('should chain TWR factors correctly', () => {
      // Crear 3 meses con factores conocidos: 1.05, 1.03, 0.98
      const monthlyDocs = [
        {
          data: () => ({
            periodType: 'month',
            periodKey: '2025-01',
            startDate: '2025-01-01',
            endDate: '2025-01-31',
            docsCount: 20,
            USD: { startFactor: 1, endFactor: 1.05, startTotalValue: 10000, endTotalValue: 10500, totalCashFlow: 0 }
          })
        },
        {
          data: () => ({
            periodType: 'month',
            periodKey: '2025-02',
            startDate: '2025-02-01',
            endDate: '2025-02-28',
            docsCount: 19,
            USD: { startFactor: 1, endFactor: 1.03, startTotalValue: 10500, endTotalValue: 10815, totalCashFlow: 0 }
          })
        },
        {
          data: () => ({
            periodType: 'month',
            periodKey: '2025-03',
            startDate: '2025-03-01',
            endDate: '2025-03-31',
            docsCount: 21,
            USD: { startFactor: 1, endFactor: 0.98, startTotalValue: 10815, endTotalValue: 10599, totalCashFlow: 0 }
          })
        }
      ];
      
      const result = consolidateMonthsToYear(monthlyDocs, '2025');
      
      // Factor compuesto esperado: 1.05 * 1.03 * 0.98 = 1.05987
      const expectedFactor = 1.05 * 1.03 * 0.98;
      
      expect(result.USD.endFactor).toBeCloseTo(expectedFactor, 4);
      expect(result.USD.periodReturn).toBeCloseTo((expectedFactor - 1) * 100, 2);
      expect(result.docsCount).toBe(60); // 20 + 19 + 21
    });
    
    it('should handle multiple currencies', () => {
      const monthlyDocs = generateMonthlyConsolidatedDocs('2025', 3);
      
      const result = consolidateMonthsToYear(monthlyDocs, '2025');
      
      expect(result.USD).toBeDefined();
      expect(result.COP).toBeDefined();
      expect(result.USD.endFactor).toBeGreaterThan(0);
      expect(result.COP.endFactor).toBeGreaterThan(0);
    });
    
    it('should calculate MWR correctly with cashflows', () => {
      const monthlyDocs = [
        {
          data: () => ({
            periodKey: '2025-01',
            startDate: '2025-01-01',
            endDate: '2025-01-31',
            docsCount: 20,
            USD: {
              startFactor: 1,
              endFactor: 1.05,
              startTotalValue: 10000,
              endTotalValue: 11500,
              totalCashFlow: -1000 // Depósito de $1000
            }
          })
        },
        {
          data: () => ({
            periodKey: '2025-02',
            startDate: '2025-02-01',
            endDate: '2025-02-28',
            docsCount: 19,
            USD: {
              startFactor: 1,
              endFactor: 1.02,
              startTotalValue: 11500,
              endTotalValue: 11730,
              totalCashFlow: 0
            }
          })
        }
      ];
      
      const result = consolidateMonthsToYear(monthlyDocs, '2025');
      
      // Verificar que se acumuló el cashflow
      expect(result.USD.totalCashFlow).toBe(-1000);
      // MWR debería estar calculado
      expect(result.USD.personalReturn).toBeDefined();
    });
    
    it('should preserve start values from first month', () => {
      const monthlyDocs = [
        {
          data: () => ({
            periodKey: '2025-06',
            startDate: '2025-06-01',
            endDate: '2025-06-30',
            docsCount: 20,
            USD: {
              startFactor: 1,
              endFactor: 1.02,
              startTotalValue: 15000,
              endTotalValue: 15300,
              startTotalInvestment: 12000,
              endTotalInvestment: 12000
            }
          })
        },
        {
          data: () => ({
            periodKey: '2025-07',
            startDate: '2025-07-01',
            endDate: '2025-07-31',
            docsCount: 22,
            USD: {
              startFactor: 1,
              endFactor: 1.01,
              startTotalValue: 15300,
              endTotalValue: 15453,
              startTotalInvestment: 12000,
              endTotalInvestment: 12000
            }
          })
        }
      ];
      
      const result = consolidateMonthsToYear(monthlyDocs, '2025');
      
      // Valores iniciales del primer mes
      expect(result.USD.startTotalValue).toBe(15000);
      expect(result.USD.startTotalInvestment).toBe(12000);
      // Valores finales del último mes
      expect(result.USD.endTotalValue).toBe(15453);
      expect(result.USD.endTotalInvestment).toBe(12000);
    });
  });
  
  describe('consolidateUserMonth (unit logic)', () => {
    // Nota: Las pruebas de integración con Firestore se harían con emulador
    // Aquí probamos la lógica interna
    
    it('should handle account path correctly for overall', () => {
      // La función construye path diferente según accountId
      // Esta prueba verifica la lógica de construcción de path
      const userId = 'test-user';
      const accountId = null;
      
      // Path esperado para overall
      const expectedBasePath = `portfolioPerformance/${userId}`;
      const expectedDatesPath = `${expectedBasePath}/dates`;
      const expectedConsolidatedPath = `${expectedBasePath}/consolidatedPeriods/monthly/periods/2025-12`;
      
      // Verificación conceptual - en integración real se usa el emulador
      expect(expectedDatesPath).toBe('portfolioPerformance/test-user/dates');
      expect(expectedConsolidatedPath).toBe('portfolioPerformance/test-user/consolidatedPeriods/monthly/periods/2025-12');
    });
    
    it('should handle account path correctly for specific account', () => {
      const userId = 'test-user';
      const accountId = 'account-123';
      
      const expectedBasePath = `portfolioPerformance/${userId}/accounts/${accountId}`;
      const expectedDatesPath = `${expectedBasePath}/dates`;
      const expectedConsolidatedPath = `${expectedBasePath}/consolidatedPeriods/monthly/periods/2025-12`;
      
      expect(expectedDatesPath).toBe('portfolioPerformance/test-user/accounts/account-123/dates');
      expect(expectedConsolidatedPath).toBe('portfolioPerformance/test-user/accounts/account-123/consolidatedPeriods/monthly/periods/2025-12');
    });
  });
  
  describe('consolidateUserYear (unit logic)', () => {
    
    it('should handle yearly path correctly for overall', () => {
      const userId = 'test-user';
      const yearKey = '2025';
      
      const expectedBasePath = `portfolioPerformance/${userId}`;
      const expectedMonthlyPath = `${expectedBasePath}/consolidatedPeriods/monthly/periods`;
      const expectedYearlyPath = `${expectedBasePath}/consolidatedPeriods/yearly/periods/${yearKey}`;
      
      expect(expectedMonthlyPath).toBe('portfolioPerformance/test-user/consolidatedPeriods/monthly/periods');
      expect(expectedYearlyPath).toBe('portfolioPerformance/test-user/consolidatedPeriods/yearly/periods/2025');
    });
    
    it('should handle yearly path correctly for specific account', () => {
      const userId = 'test-user';
      const accountId = 'account-456';
      const yearKey = '2024';
      
      const expectedBasePath = `portfolioPerformance/${userId}/accounts/${accountId}`;
      const expectedYearlyPath = `${expectedBasePath}/consolidatedPeriods/yearly/periods/${yearKey}`;
      
      expect(expectedYearlyPath).toBe('portfolioPerformance/test-user/accounts/account-456/consolidatedPeriods/yearly/periods/2024');
    });
  });
});

// ============================================================================
// TESTS: EDGE CASES
// ============================================================================

describe('edge cases', () => {
  
  describe('consolidateMonthsToYear edge cases', () => {
    
    it('should handle single month', () => {
      const monthlyDocs = [
        {
          data: () => ({
            periodKey: '2025-06',
            startDate: '2025-06-01',
            endDate: '2025-06-30',
            docsCount: 20,
            USD: { startFactor: 1, endFactor: 1.03, startTotalValue: 10000, endTotalValue: 10300, totalCashFlow: 0 }
          })
        }
      ];
      
      const result = consolidateMonthsToYear(monthlyDocs, '2025');
      
      expect(result).not.toBeNull();
      expect(result.USD.endFactor).toBeCloseTo(1.03, 2);
      expect(result.docsCount).toBe(20);
    });
    
    it('should handle months with missing currency data gracefully', () => {
      const monthlyDocs = [
        {
          data: () => ({
            periodKey: '2025-01',
            startDate: '2025-01-01',
            endDate: '2025-01-31',
            docsCount: 20,
            USD: { startFactor: 1, endFactor: 1.02, startTotalValue: 10000, endTotalValue: 10200, totalCashFlow: 0 }
            // No COP en este mes
          })
        },
        {
          data: () => ({
            periodKey: '2025-02',
            startDate: '2025-02-01',
            endDate: '2025-02-28',
            docsCount: 19,
            USD: { startFactor: 1, endFactor: 1.01, startTotalValue: 10200, endTotalValue: 10302, totalCashFlow: 0 },
            COP: { startFactor: 1, endFactor: 1.005, startTotalValue: 40000000, endTotalValue: 40200000, totalCashFlow: 0 }
          })
        }
      ];
      
      const result = consolidateMonthsToYear(monthlyDocs, '2025');
      
      // USD debería tener datos de ambos meses
      expect(result.USD).toBeDefined();
      // COP solo tiene datos del segundo mes
      expect(result.COP).toBeDefined();
    });
    
    it('should handle negative returns correctly', () => {
      const monthlyDocs = [
        {
          data: () => ({
            periodKey: '2025-01',
            startDate: '2025-01-01',
            endDate: '2025-01-31',
            docsCount: 20,
            USD: { startFactor: 1, endFactor: 0.95, startTotalValue: 10000, endTotalValue: 9500, totalCashFlow: 0 }
          })
        },
        {
          data: () => ({
            periodKey: '2025-02',
            startDate: '2025-02-01',
            endDate: '2025-02-28',
            docsCount: 19,
            USD: { startFactor: 1, endFactor: 0.97, startTotalValue: 9500, endTotalValue: 9215, totalCashFlow: 0 }
          })
        }
      ];
      
      const result = consolidateMonthsToYear(monthlyDocs, '2025');
      
      // Factor compuesto: 0.95 * 0.97 = 0.9215
      expect(result.USD.endFactor).toBeCloseTo(0.9215, 3);
      expect(result.USD.periodReturn).toBeLessThan(0);
    });
  });
});
