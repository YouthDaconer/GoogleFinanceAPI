/**
 * Utilidades para calcular Money-Weighted Return (MWR)
 * 
 * Historia 25: Implementación de Personal Return (MWR) + TWR Dual Metrics
 * 
 * El MWR (también conocido como IRR simplificado) mide el rendimiento
 * considerando CUÁNDO el usuario invirtió su dinero.
 * 
 * Responde la pregunta: "¿Cuánto he ganado con MI dinero?"
 * 
 * Implementamos el Modified Dietz Method para balance entre precisión y rendimiento.
 * 
 * @module mwrCalculations
 * @see docs/stories/25.story.md
 */

const {
  daysBetween,
  getPeriodBoundaries,
  sortDocumentsByDate,
  extractDocumentData,
  initializePeriods,
  normalizeApiKey
} = require('./periodCalculations');

/**
 * Calcula Personal Return Simple
 * 
 * Fórmula simplificada para cuando no tenemos cashflows detallados por día
 * 
 * @param {number} startValue - Valor del portafolio al inicio del período
 * @param {number} endValue - Valor del portafolio al final del período
 * @param {number} totalCashFlow - Suma de cashflows durante el período
 *                                 (negativo = depósitos/compras, positivo = retiros/ventas)
 * @returns {number} Personal Return como porcentaje
 */
function calculateSimplePersonalReturn(startValue, endValue, totalCashFlow) {
  // Convertir cashflow a depósitos netos (cashflow negativo = depósito)
  const netDeposits = -totalCashFlow;
  
  // Caso 1: No había valor inicial (nueva inversión durante el período)
  if (startValue === 0 || startValue === null || startValue === undefined) {
    // Si no hubo depósitos, retornar 0
    if (netDeposits <= 0) return 0;
    
    // Rendimiento = (Valor Final - Lo depositado) / Lo depositado
    return ((endValue - netDeposits) / netDeposits) * 100;
  }
  
  // Caso 2: Hay valor inicial
  // Inversión base = Valor inicial + (Depósitos / 2)
  // Esto aproxima el promedio ponderado asumiendo depósitos uniformes
  const investmentBase = startValue + (netDeposits / 2);
  
  if (investmentBase <= 0) {
    // Si la base es negativa (retiros mayores que valor inicial), 
    // usar solo el valor inicial
    if (startValue <= 0) return 0;
    const gain = endValue - startValue + totalCashFlow; // +totalCashFlow porque retiros son positivos
    return (gain / startValue) * 100;
  }
  
  // Ganancia = Valor Final - Valor Inicial - Depósitos Netos
  const gain = endValue - startValue - netDeposits;
  
  return (gain / investmentBase) * 100;
}

/**
 * Calcula MWR usando Modified Dietz Method
 * 
 * Más preciso que el simple porque pondera los cashflows por el tiempo
 * que estuvieron invertidos durante el período.
 * 
 * Fórmula: R = (V_end - V_start - ΣCF) / (V_start + Σ(CF_i * W_i))
 * Donde W_i = (días restantes hasta fin) / (días totales del período)
 * 
 * NOTA: Cuando el resultado es extremo (> 100% o < -100%), el cálculo puede
 * ser matemáticamente correcto pero poco intuitivo. En estos casos, usamos
 * el método Simple como fallback más comprensible para el usuario.
 * 
 * @param {number} startValue - Valor del portafolio al inicio del período
 * @param {number} endValue - Valor del portafolio al final del período
 * @param {Array<{date: string, amount: number}>} cashFlows - Array de cashflows
 *        donde amount negativo = depósito/compra, positivo = retiro/venta
 * @param {string} startDate - Fecha de inicio del período (ISO)
 * @param {string} endDate - Fecha de fin del período (ISO)
 * @returns {number} MWR como porcentaje
 */
function calculateModifiedDietzReturn(startValue, endValue, cashFlows, startDate, endDate) {
  const totalDays = daysBetween(startDate, endDate);
  
  // Si el período es de 0 días, usar fórmula simple
  if (totalDays === 0) {
    const totalCF = cashFlows.reduce((sum, cf) => sum + cf.amount, 0);
    return calculateSimplePersonalReturn(startValue, endValue, totalCF);
  }
  
  // Calcular suma de cashflows y cashflows ponderados
  let totalCashFlow = 0;
  let weightedCashFlow = 0;
  
  cashFlows.forEach(cf => {
    // Días desde el cashflow hasta el fin del período
    const daysRemaining = daysBetween(cf.date, endDate);
    // Peso = proporción del período que el dinero estuvo invertido
    const weight = daysRemaining / totalDays;
    
    totalCashFlow += cf.amount;
    weightedCashFlow += cf.amount * weight;
  });
  
  // Convertir a depósitos netos para el cálculo del denominador
  const netDeposits = -totalCashFlow;
  const weightedNetDeposits = -weightedCashFlow;
  
  // Caso: No había valor inicial
  if (startValue === 0 || startValue === null || startValue === undefined) {
    if (netDeposits <= 0) return 0;
    
    // Usar depósitos ponderados como base
    const base = weightedNetDeposits > 0 ? weightedNetDeposits : netDeposits;
    const result = ((endValue - netDeposits) / base) * 100;
    
    // Limitar a rango razonable, usar ROI Simple como fallback si es extremo
    if (Math.abs(result) > 100) {
      return ((endValue - netDeposits) / netDeposits) * 100;
    }
    return result;
  }
  
  // Denominador: Valor inicial + Depósitos ponderados por tiempo
  const denominator = startValue + weightedNetDeposits;
  
  if (denominator <= 0) {
    // Fallback a fórmula simple si el denominador es inválido
    return calculateSimplePersonalReturn(startValue, endValue, totalCashFlow);
  }
  
  // Numerador: Ganancia neta
  const gain = endValue - startValue - netDeposits;
  
  const result = (gain / denominator) * 100;
  
  // Protección para resultados extremos:
  // Cuando el denominador es muy pequeño respecto a los flujos totales,
  // el Modified Dietz puede dar resultados extremos que no son intuitivos.
  // En estos casos, usamos el Simple Personal Return como fallback.
  if (Math.abs(result) > 100) {
    const simpleResult = calculateSimplePersonalReturn(startValue, endValue, totalCashFlow);
    // Solo usar el simple si es más razonable
    if (Math.abs(simpleResult) < Math.abs(result)) {
      return simpleResult;
    }
  }
  
  return result;
}

/**
 * Calcula Personal Returns para múltiples períodos a partir de documentos de Firestore
 * 
 * Esta función procesa los mismos documentos que calculateHistoricalReturns()
 * pero calcula MWR en lugar de TWR.
 * 
 * Refactorizado para usar utilidades compartidas de periodCalculations.js
 * cumpliendo con el principio DRY.
 * 
 * @param {Array} docs - Documentos de Firestore ordenados por fecha
 * @param {string} currency - Código de moneda (USD, COP, etc.)
 * @param {string|null} ticker - Ticker específico (opcional)
 * @param {string|null} assetType - Tipo de asset (opcional)
 * @returns {Object} Personal returns calculados para cada período
 */
function calculateAllPersonalReturns(docs, currency, ticker = null, assetType = null) {
  // Obtener fechas límite usando utilidad compartida
  const boundaries = getPeriodBoundaries();
  const { todayISO } = boundaries;
  
  // Ordenar documentos usando utilidad compartida
  const documents = sortDocumentsByDate(docs);
  
  // Inicializar períodos usando utilidad compartida (solo campos MWR)
  const periods = initializePeriods(boundaries, { includeTWR: false, includeMWR: true });

  // Procesar documentos
  documents.forEach((doc) => {
    // Extraer datos usando utilidad compartida
    const docData = extractDocumentData(doc, currency, ticker, assetType);
    
    if (!docData) return;
    
    const { date: docDate, totalValue, totalCashFlow } = docData;

    // Procesar cada período
    Object.keys(periods).forEach(periodKey => {
      const period = periods[periodKey];
      
      if (docDate >= period.startDate) {
        // Marcar valor inicial del período (primer documento dentro del rango)
        if (!period.found) {
          period.startValue = totalValue;
          period.found = true;
        }
        
        // Actualizar valor final (último documento procesado)
        period.endValue = totalValue;
        
        // Acumular cashflows
        if (totalCashFlow !== 0) {
          period.cashFlows.push({ date: docDate, amount: totalCashFlow });
        }
        period.totalCashFlow += totalCashFlow;
      }
    });
  });

  // Calcular Personal Return para cada período usando Modified Dietz
  const results = {};

  Object.keys(periods).forEach(periodKey => {
    const period = periods[periodKey];
    const returnKey = normalizeApiKey(periodKey, 'PersonalReturn');
    const hasDataKey = `has${periodKey.charAt(0).toUpperCase() + periodKey.slice(1)}PersonalData`;
    
    if (period.found && period.endValue !== null) {
      // Usar Modified Dietz si hay cashflows, sino usar Simple
      if (period.cashFlows.length > 0) {
        results[returnKey] = calculateModifiedDietzReturn(
          period.startValue,
          period.endValue,
          period.cashFlows,
          period.startDate,
          todayISO
        );
      } else {
        results[returnKey] = calculateSimplePersonalReturn(
          period.startValue,
          period.endValue,
          period.totalCashFlow
        );
      }
    } else {
      results[returnKey] = 0;
    }
    
    // Agregar flag de datos disponibles
    results[hasDataKey] = period.found;
  });

  return results;
}

module.exports = {
  // Re-exportar daysBetween desde periodCalculations para compatibilidad
  daysBetween,
  calculateSimplePersonalReturn,
  calculateModifiedDietzReturn,
  calculateAllPersonalReturns
};
