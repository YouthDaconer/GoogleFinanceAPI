/**
 * Verificar si podemos calcular MWR con los datos históricos existentes
 * 
 * Para MWR necesitamos:
 * 1. totalValue al inicio y fin de cada período
 * 2. totalCashFlow acumulado durante el período
 * 3. (Opcional) totalInvestment para validación
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const ACCOUNT_ID = 'Z3gnboYgRlTvSZNGSu8j'; // XTB

async function verifyMWRDataAvailability() {
  console.log('='.repeat(100));
  console.log('VERIFICACIÓN DE DATOS PARA CÁLCULO DE MWR');
  console.log('='.repeat(100));
  console.log();

  const accountSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/accounts/${ACCOUNT_ID}/dates`)
    .orderBy('date', 'asc')
    .get();

  const now = DateTime.now().setZone('America/New_York');
  const oneMonthAgo = now.minus({ months: 1 }).toISODate();
  const threeMonthsAgo = now.minus({ months: 3 }).toISODate();
  const sixMonthsAgo = now.minus({ months: 6 }).toISODate();
  const startOfYear = now.startOf('year').toISODate();

  console.log('📅 Períodos a analizar:');
  console.log(`   1M: ${oneMonthAgo} → ${now.toISODate()}`);
  console.log(`   3M: ${threeMonthsAgo} → ${now.toISODate()}`);
  console.log(`   6M: ${sixMonthsAgo} → ${now.toISODate()}`);
  console.log(`   YTD: ${startOfYear} → ${now.toISODate()}`);
  console.log();

  // Verificar campos disponibles
  console.log('='.repeat(100));
  console.log('CAMPOS DISPONIBLES EN DOCUMENTOS');
  console.log('='.repeat(100));
  console.log();

  let hasTotalValue = 0;
  let hasTotalInvestment = 0;
  let hasTotalCashFlow = 0;
  let hasAdjustedDailyChange = 0;
  let totalDocs = 0;

  // Datos para cálculo de MWR
  let ytdStartValue = null;
  let ytdEndValue = null;
  let ytdTotalCashFlow = 0;
  let ytdStartInvestment = null;
  let ytdEndInvestment = null;

  let oneMonthStartValue = null;
  let oneMonthEndValue = null;
  let oneMonthTotalCashFlow = 0;

  let threeMonthStartValue = null;
  let threeMonthEndValue = null;
  let threeMonthTotalCashFlow = 0;

  let sixMonthStartValue = null;
  let sixMonthEndValue = null;
  let sixMonthTotalCashFlow = 0;

  for (const doc of accountSnapshot.docs) {
    const data = doc.data();
    const currencyData = data.USD;
    
    if (!currencyData) continue;
    totalDocs++;

    // Verificar campos
    if (currencyData.totalValue !== undefined) hasTotalValue++;
    if (currencyData.totalInvestment !== undefined) hasTotalInvestment++;
    if (currencyData.totalCashFlow !== undefined) hasTotalCashFlow++;
    if (currencyData.adjustedDailyChangePercentage !== undefined) hasAdjustedDailyChange++;

    // Acumular datos para MWR por período
    
    // YTD
    if (data.date >= startOfYear) {
      if (ytdStartValue === null) {
        ytdStartValue = currencyData.totalValue || 0;
        ytdStartInvestment = currencyData.totalInvestment || 0;
      }
      ytdEndValue = currencyData.totalValue || 0;
      ytdEndInvestment = currencyData.totalInvestment || 0;
      ytdTotalCashFlow += currencyData.totalCashFlow || 0;
    }

    // 6M
    if (data.date >= sixMonthsAgo) {
      if (sixMonthStartValue === null) {
        sixMonthStartValue = currencyData.totalValue || 0;
      }
      sixMonthEndValue = currencyData.totalValue || 0;
      sixMonthTotalCashFlow += currencyData.totalCashFlow || 0;
    }

    // 3M
    if (data.date >= threeMonthsAgo) {
      if (threeMonthStartValue === null) {
        threeMonthStartValue = currencyData.totalValue || 0;
      }
      threeMonthEndValue = currencyData.totalValue || 0;
      threeMonthTotalCashFlow += currencyData.totalCashFlow || 0;
    }

    // 1M
    if (data.date >= oneMonthAgo) {
      if (oneMonthStartValue === null) {
        oneMonthStartValue = currencyData.totalValue || 0;
      }
      oneMonthEndValue = currencyData.totalValue || 0;
      oneMonthTotalCashFlow += currencyData.totalCashFlow || 0;
    }
  }

  console.log(`📊 Total documentos analizados: ${totalDocs}`);
  console.log();
  console.log('Campo                          | Disponible | % Cobertura');
  console.log('-'.repeat(70));
  console.log(`totalValue                     | ${hasTotalValue.toString().padStart(10)} | ${((hasTotalValue/totalDocs)*100).toFixed(1)}%`);
  console.log(`totalInvestment                | ${hasTotalInvestment.toString().padStart(10)} | ${((hasTotalInvestment/totalDocs)*100).toFixed(1)}%`);
  console.log(`totalCashFlow                  | ${hasTotalCashFlow.toString().padStart(10)} | ${((hasTotalCashFlow/totalDocs)*100).toFixed(1)}%`);
  console.log(`adjustedDailyChangePercentage  | ${hasAdjustedDailyChange.toString().padStart(10)} | ${((hasAdjustedDailyChange/totalDocs)*100).toFixed(1)}%`);
  console.log();

  // Calcular MWR para cada período
  console.log('='.repeat(100));
  console.log('CÁLCULO DE MWR (Modified Dietz Simplificado)');
  console.log('='.repeat(100));
  console.log();

  // Fórmula: MWR = (EndValue - StartValue - NetCashFlow) / (StartValue + NetCashFlow/2)
  // Donde NetCashFlow negativo = depósitos

  function calculateMWR(startValue, endValue, totalCashFlow, periodName) {
    console.log(`📊 ${periodName}:`);
    console.log(`   Valor Inicial: $${startValue?.toFixed(2) || 'N/A'}`);
    console.log(`   Valor Final: $${endValue?.toFixed(2) || 'N/A'}`);
    console.log(`   CashFlow Total: $${totalCashFlow?.toFixed(2) || 'N/A'}`);
    
    if (startValue === null || endValue === null) {
      console.log(`   MWR: N/A (datos insuficientes)`);
      return null;
    }

    // CashFlow negativo = depósitos, así que depósitos netos = -totalCashFlow
    const netDeposits = -totalCashFlow;
    
    // Inversión base = valor inicial + depósitos/2 (aproximación de medio período)
    const investmentBase = startValue + netDeposits / 2;
    
    if (investmentBase <= 0) {
      // Si no había valor inicial, usar depósitos como base
      if (netDeposits > 0) {
        const mwr = ((endValue - netDeposits) / netDeposits) * 100;
        console.log(`   Depósitos netos: $${netDeposits.toFixed(2)}`);
        console.log(`   MWR: ${mwr.toFixed(2)}%`);
        return mwr;
      }
      console.log(`   MWR: N/A (base de inversión <= 0)`);
      return null;
    }

    // Ganancia = valor final - valor inicial - depósitos netos
    const gain = endValue - startValue - netDeposits;
    const mwr = (gain / investmentBase) * 100;

    console.log(`   Depósitos netos: $${netDeposits.toFixed(2)}`);
    console.log(`   Base inversión: $${investmentBase.toFixed(2)}`);
    console.log(`   Ganancia: $${gain.toFixed(2)}`);
    console.log(`   MWR: ${mwr.toFixed(2)}%`);
    
    return mwr;
  }

  const ytdMWR = calculateMWR(ytdStartValue, ytdEndValue, ytdTotalCashFlow, 'YTD');
  console.log();
  const sixMonthMWR = calculateMWR(sixMonthStartValue, sixMonthEndValue, sixMonthTotalCashFlow, '6M');
  console.log();
  const threeMonthMWR = calculateMWR(threeMonthStartValue, threeMonthEndValue, threeMonthTotalCashFlow, '3M');
  console.log();
  const oneMonthMWR = calculateMWR(oneMonthStartValue, oneMonthEndValue, oneMonthTotalCashFlow, '1M');

  // Comparar con ROI Simple y TWR
  console.log();
  console.log('='.repeat(100));
  console.log('COMPARACIÓN DE MÉTRICAS');
  console.log('='.repeat(100));
  console.log();

  // ROI Simple actual (desde datos finales)
  const roiSimple = ytdEndInvestment > 0 
    ? ((ytdEndValue - ytdEndInvestment) / ytdEndInvestment) * 100 
    : 0;

  console.log('Métrica         | YTD       | 6M        | 3M        | 1M');
  console.log('-'.repeat(70));
  console.log(`TWR (actual)    | -1.26%    | 22.92%    | 8.31%     | 1.69%`);
  console.log(`MWR (calculado) | ${ytdMWR?.toFixed(2) || 'N/A'}%    | ${sixMonthMWR?.toFixed(2) || 'N/A'}%    | ${threeMonthMWR?.toFixed(2) || 'N/A'}%    | ${oneMonthMWR?.toFixed(2) || 'N/A'}%`);
  console.log(`ROI Simple      | ${roiSimple.toFixed(2)}%    | -         | -         | -`);
  console.log();

  // Verificar consistencia
  console.log('='.repeat(100));
  console.log('VALIDACIÓN');
  console.log('='.repeat(100));
  console.log();

  console.log(`📊 Inversión Final (totalInvestment): $${ytdEndInvestment?.toFixed(2)}`);
  console.log(`📊 Valor Final (totalValue): $${ytdEndValue?.toFixed(2)}`);
  console.log(`📊 Valorización calculada: $${(ytdEndValue - ytdEndInvestment)?.toFixed(2)}`);
  console.log(`📊 ROI Simple: ${roiSimple.toFixed(2)}%`);
  console.log();

  if (Math.abs(roiSimple - ytdMWR) < 1) {
    console.log('✅ MWR y ROI Simple son consistentes (diferencia < 1pp)');
  } else {
    console.log(`⚠️ Diferencia entre MWR (${ytdMWR?.toFixed(2)}%) y ROI Simple (${roiSimple.toFixed(2)}%): ${Math.abs(roiSimple - ytdMWR).toFixed(2)}pp`);
    console.log('   Esto puede ser normal si hubo muchos depósitos durante el período.');
  }

  console.log();
  console.log('='.repeat(100));
  console.log('CONCLUSIÓN');
  console.log('='.repeat(100));
  console.log();
  
  const canCalculateMWR = hasTotalValue === totalDocs && hasTotalCashFlow === totalDocs;
  
  if (canCalculateMWR) {
    console.log('✅ PODEMOS CALCULAR MWR con los datos existentes');
    console.log();
    console.log('Los campos necesarios (totalValue, totalCashFlow) están disponibles en 100% de los documentos.');
    console.log('No es necesaria una migración de datos - solo necesitamos modificar la función de cálculo.');
  } else {
    console.log('⚠️ MIGRACIÓN PARCIAL NECESARIA');
    console.log();
    console.log(`Campos faltantes:`);
    if (hasTotalValue < totalDocs) console.log(`   - totalValue: falta en ${totalDocs - hasTotalValue} docs`);
    if (hasTotalCashFlow < totalDocs) console.log(`   - totalCashFlow: falta en ${totalDocs - hasTotalCashFlow} docs`);
  }

  process.exit(0);
}

verifyMWRDataAvailability().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
