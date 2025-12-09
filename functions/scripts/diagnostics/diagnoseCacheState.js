/**
 * Script de Diagnóstico: Verificar Cache de Performance
 * 
 * Este script verifica:
 * 1. Estado del cache en userData/{userId}/performanceCache
 * 2. Diferencias entre cache y datos calculados en tiempo real
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2'
};

async function main() {
  console.log('');
  console.log('═'.repeat(80));
  console.log('  DIAGNÓSTICO DE CACHÉ DE PERFORMANCE');
  console.log('═'.repeat(80));
  console.log('');
  
  // 1. Verificar si existe caché
  console.log('🔍 Buscando caches en userData/{userId}/performanceCache...');
  
  const cacheRef = db.collection(`userData/${CONFIG.USER_ID}/performanceCache`);
  const cacheSnapshot = await cacheRef.get();
  
  console.log(`   Caches encontrados: ${cacheSnapshot.docs.length}`);
  console.log('');
  
  if (cacheSnapshot.docs.length > 0) {
    console.log('━'.repeat(80));
    console.log('  CONTENIDO DEL CACHÉ');
    console.log('━'.repeat(80));
    
    for (const doc of cacheSnapshot.docs) {
      const data = doc.data();
      console.log('');
      console.log(`📦 Cache ID: ${doc.id}`);
      console.log(`   Last Calculated: ${data.lastCalculated || 'N/A'}`);
      console.log(`   Valid Until: ${data.validUntil || 'N/A'}`);
      
      const now = new Date();
      const validUntil = data.validUntil ? new Date(data.validUntil) : null;
      const isExpired = validUntil ? validUntil < now : true;
      
      console.log(`   Expirado: ${isExpired ? '❌ SÍ' : '✅ NO'}`);
      
      if (data.data) {
        console.log('');
        console.log('   📊 Datos cacheados:');
        console.log(`      YTD Return: ${data.data.ytdReturn?.toFixed(2) || 'N/A'}%`);
        console.log(`      1M Return: ${data.data.oneMonthReturn?.toFixed(2) || 'N/A'}%`);
        console.log(`      3M Return: ${data.data.threeMonthReturn?.toFixed(2) || 'N/A'}%`);
        console.log(`      6M Return: ${data.data.sixMonthReturn?.toFixed(2) || 'N/A'}%`);
        console.log(`      1Y Return: ${data.data.oneYearReturn?.toFixed(2) || 'N/A'}%`);
        
        if (data.data.performanceByYear) {
          console.log('');
          console.log('   📅 Performance por Año:');
          for (const [year, yearData] of Object.entries(data.data.performanceByYear)) {
            console.log(`      ${year}: ${yearData.total?.toFixed(2)}% (Total)`);
            if (yearData.months) {
              const monthNames = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 
                                  'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'];
              for (const [month, value] of Object.entries(yearData.months)) {
                const monthNum = parseInt(month);
                const monthName = monthNames[monthNum - 1] || month;
                console.log(`         ${monthName}: ${value?.toFixed(2)}%`);
              }
            }
          }
        }
      }
    }
  }
  
  // 2. Verificar caché del historicalReturns
  console.log('');
  console.log('━'.repeat(80));
  console.log('  VERIFICANDO CACHE DE HISTORICAL RETURNS');
  console.log('━'.repeat(80));
  
  const hrCacheRef = db.collection(`userData/${CONFIG.USER_ID}/historicalReturnsCache`);
  const hrCacheSnapshot = await hrCacheRef.get();
  
  console.log(`   Caches de Historical Returns: ${hrCacheSnapshot.docs.length}`);
  
  if (hrCacheSnapshot.docs.length > 0) {
    for (const doc of hrCacheSnapshot.docs) {
      const data = doc.data();
      console.log('');
      console.log(`📦 Cache ID: ${doc.id}`);
      console.log(`   Last Calculated: ${data.lastCalculated || 'N/A'}`);
      console.log(`   Valid Until: ${data.validUntil || 'N/A'}`);
      
      if (data.data || data.returns) {
        const returns = data.data || data.returns || data;
        console.log('');
        console.log('   📊 Datos:');
        console.log(`      YTD: ${returns.ytdReturn?.toFixed(2) || 'N/A'}%`);
        console.log(`      1M: ${returns.oneMonthReturn?.toFixed(2) || 'N/A'}%`);
        console.log(`      3M: ${returns.threeMonthReturn?.toFixed(2) || 'N/A'}%`);
      }
    }
  }
  
  // 3. Verificar performanceByYear almacenado
  console.log('');
  console.log('━'.repeat(80));
  console.log('  DATOS MENSUALES DE 2025 EN FIRESTORE');
  console.log('━'.repeat(80));
  
  // Obtener documentos de marzo 2025 del OVERALL
  const marchDocs = await db.collection(`portfolioPerformance/${CONFIG.USER_ID}/dates`)
    .where('date', '>=', '2025-03-01')
    .where('date', '<=', '2025-03-31')
    .orderBy('date', 'asc')
    .get();
  
  console.log('');
  console.log(`   Documentos de Marzo 2025 (OVERALL): ${marchDocs.docs.length}`);
  
  // Calcular rendimiento de marzo manualmente
  let marchCompound = 1;
  marchDocs.docs.forEach(doc => {
    const data = doc.data();
    const change = data.USD?.adjustedDailyChangePercentage || 0;
    marchCompound *= (1 + change / 100);
  });
  const marchReturn = (marchCompound - 1) * 100;
  
  console.log(`   Rendimiento Marzo 2025 (calculado): ${marchReturn.toFixed(2)}%`);
  
  // Mostrar primeros y últimos días de marzo
  console.log('');
  console.log('   Primeros 3 días de marzo:');
  marchDocs.docs.slice(0, 3).forEach(doc => {
    const data = doc.data();
    console.log(`      ${data.date}: ${data.USD?.adjustedDailyChangePercentage?.toFixed(4)}% (valor: $${data.USD?.totalValue?.toFixed(2)})`);
  });
  
  console.log('');
  console.log('   Últimos 3 días de marzo:');
  marchDocs.docs.slice(-3).forEach(doc => {
    const data = doc.data();
    console.log(`      ${data.date}: ${data.USD?.adjustedDailyChangePercentage?.toFixed(4)}% (valor: $${data.USD?.totalValue?.toFixed(2)})`);
  });
  
  console.log('');
  console.log('═'.repeat(80));
  console.log('  ✅ DIAGNÓSTICO DE CACHÉ COMPLETO');
  console.log('═'.repeat(80));
  
  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
