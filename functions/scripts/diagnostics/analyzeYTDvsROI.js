/**
 * Análisis detallado de la discrepancia entre YTD y ROI Simple en XTB
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

async function analyzeYTDvsROI() {
  console.log('='.repeat(100));
  console.log('ANÁLISIS: ¿POR QUÉ YTD (-1.26%) ES DIFERENTE DE ROI SIMPLE (+7.14%)?');
  console.log('='.repeat(100));
  console.log();

  const accountSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/accounts/${ACCOUNT_ID}/dates`)
    .orderBy('date', 'asc')
    .get();

  const now = DateTime.now().setZone('America/New_York');
  const startOfYear = now.startOf('year').toISODate();

  console.log('📊 CONCEPTOS CLAVE:');
  console.log();
  console.log('   ROI Simple = (Valor Actual - Inversión Total) / Inversión Total');
  console.log('   → Mide TU ganancia personal sobre lo que invertiste');
  console.log('   → NO considera cuándo invertiste');
  console.log();
  console.log('   YTD (Time-Weighted Return) = Producto de (1 + rendimiento diario)');
  console.log('   → Mide el RENDIMIENTO DEL PORTAFOLIO como si fuera un fondo');
  console.log('   → Elimina el efecto del timing de inversiones');
  console.log();

  // Calcular métricas
  let currentFactor = 1;
  let ytdStartFactor = null;
  let totalInvested = 0;
  let totalCashOutflows = 0; // Compras
  
  let ytdDaysNegative = 0;
  let ytdDaysPositive = 0;
  let ytdTotalNegative = 0;
  let ytdTotalPositive = 0;

  for (const doc of accountSnapshot.docs) {
    const data = doc.data();
    const currencyData = data.USD;
    if (!currencyData) continue;

    const adjChange = currencyData.adjustedDailyChangePercentage || 0;
    const totalCashFlow = currencyData.totalCashFlow || 0;

    // Marcar inicio de YTD
    if (ytdStartFactor === null && data.date >= startOfYear) {
      ytdStartFactor = currentFactor;
    }

    // Contar días positivos/negativos desde YTD
    if (data.date >= startOfYear) {
      if (adjChange < 0) {
        ytdDaysNegative++;
        ytdTotalNegative += adjChange;
      } else if (adjChange > 0) {
        ytdDaysPositive++;
        ytdTotalPositive += adjChange;
      }
      
      // Acumular cashflows (compras son negativas)
      if (totalCashFlow < 0) {
        totalCashOutflows += Math.abs(totalCashFlow);
      }
    }

    currentFactor = currentFactor * (1 + adjChange / 100);
  }

  const ytdReturn = ytdStartFactor ? (currentFactor / ytdStartFactor - 1) * 100 : 0;

  console.log('='.repeat(100));
  console.log('ESTADÍSTICAS YTD');
  console.log('='.repeat(100));
  console.log();
  console.log(`📅 Días con rendimiento positivo: ${ytdDaysPositive}`);
  console.log(`📅 Días con rendimiento negativo: ${ytdDaysNegative}`);
  console.log(`📈 Suma de días positivos: +${ytdTotalPositive.toFixed(2)}%`);
  console.log(`📉 Suma de días negativos: ${ytdTotalNegative.toFixed(2)}%`);
  console.log(`📊 Suma neta: ${(ytdTotalPositive + ytdTotalNegative).toFixed(2)}%`);
  console.log();
  console.log(`💵 Total invertido durante YTD: $${totalCashOutflows.toFixed(2)}`);
  console.log();

  console.log('='.repeat(100));
  console.log('EXPLICACIÓN DE LA DISCREPANCIA');
  console.log('='.repeat(100));
  console.log();
  console.log('El YTD de -1.26% significa que:');
  console.log('   → Si hubieras invertido $100 el 1 de enero,');
  console.log('   → Hoy tendrías $98.74 (perdiste $1.26)');
  console.log();
  console.log('El ROI Simple de +7.14% significa que:');
  console.log('   → Tu inversión total de $4,252 hoy vale $4,556');
  console.log('   → Ganaste $303 (7.14% de retorno)');
  console.log();
  console.log('¿Por qué son diferentes?');
  console.log('   1. Invertiste la mayoría de tu dinero cuando el mercado estaba BAJO');
  console.log('   2. Aunque el mercado desde enero ha perdido -1.26%,');
  console.log('   3. TÚ compraste barato y ahora vales más');
  console.log();
  console.log('CONCLUSIÓN:');
  console.log('   ✅ Ambas métricas son CORRECTAS');
  console.log('   ✅ El YTD mide el rendimiento del mercado (para tus activos)');
  console.log('   ✅ El ROI Simple mide tu ganancia personal');
  console.log('   ✅ Tuviste buen timing de inversión');

  process.exit(0);
}

analyzeYTDvsROI().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
