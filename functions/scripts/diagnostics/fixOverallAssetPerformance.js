/**
 * Script para corregir los documentos OVERALL de backfill
 * 
 * Este script corrige los campos faltantes y valores de assetPerformance en el OVERALL:
 * - doneProfitAndLoss (agregar con valor 0)
 * - monthlyReturn (agregar con valor 0)
 * - annualReturn (agregar con valor 0)
 * - dailyChangePercentage, rawDailyChangePercentage, adjustedDailyChangePercentage, dailyReturn
 *   (recalcular basándose en el día anterior)
 * 
 * USO:
 *   node fixOverallAssetPerformance.js --dry-run    # Ver cambios sin aplicar
 *   node fixOverallAssetPerformance.js --fix        # Aplicar cambios a Firestore
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// Configuración
const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  START_DATE: '2025-01-01',
  END_DATE: '2025-06-01',
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
  BATCH_SIZE: 20,
};

function parseArgs() {
  const args = process.argv.slice(2);
  let mode = 'dry-run';
  
  args.forEach(arg => {
    if (arg === '--fix') mode = 'fix';
    if (arg === '--dry-run') mode = 'dry-run';
  });
  
  return { mode };
}

function log(level, message) {
  const colors = {
    INFO: '\x1b[36m',
    SUCCESS: '\x1b[32m',
    WARN: '\x1b[33m',
    ERROR: '\x1b[31m',
    PROGRESS: '\x1b[35m',
  };
  const reset = '\x1b[0m';
  const color = colors[level] || '';
  console.log(`${color}[${level}]${reset} ${message}`);
}

async function fixOverallAssetPerformance() {
  const options = parseArgs();
  
  console.log('═'.repeat(80));
  console.log('  CORRECCIÓN DE assetPerformance EN DOCUMENTOS OVERALL');
  console.log('═'.repeat(80));
  console.log('');
  log('INFO', `Modo: ${options.mode.toUpperCase()}`);
  log('INFO', `Usuario: ${CONFIG.USER_ID}`);
  log('INFO', `Rango: ${CONFIG.START_DATE} a ${CONFIG.END_DATE}`);
  console.log('');
  
  // 1. Obtener todos los documentos OVERALL en el rango
  const overallDocs = await db.collection(`portfolioPerformance/${CONFIG.USER_ID}/dates`)
    .where('date', '>=', CONFIG.START_DATE)
    .where('date', '<=', CONFIG.END_DATE)
    .orderBy('date', 'asc')
    .get();
  
  log('INFO', `Documentos OVERALL encontrados: ${overallDocs.docs.length}`);
  
  if (overallDocs.docs.length === 0) {
    log('WARN', 'No hay documentos para corregir');
    process.exit(0);
  }
  
  // 2. Construir mapa de documentos por fecha para acceso rápido
  const overallByDate = new Map();
  overallDocs.docs.forEach(doc => {
    const data = doc.data();
    overallByDate.set(data.date, { ref: doc.ref, data });
  });
  
  const allDates = [...overallByDate.keys()].sort();
  
  // 3. Analizar y corregir cada documento
  const documentsToFix = [];
  let totalIssuesFound = 0;
  
  for (let i = 0; i < allDates.length; i++) {
    const date = allDates[i];
    const { ref, data } = overallByDate.get(date);
    
    // Obtener documento del día anterior
    let previousData = null;
    if (i > 0) {
      const prevDate = allDates[i - 1];
      previousData = overallByDate.get(prevDate)?.data;
    }
    
    let needsFix = false;
    const fixes = {};
    
    // Revisar cada moneda
    CONFIG.CURRENCIES.forEach(currency => {
      const currencyData = data[currency];
      if (!currencyData || !currencyData.assetPerformance) return;
      
      const prevCurrencyData = previousData?.[currency];
      const prevAssetPerf = prevCurrencyData?.assetPerformance || {};
      
      const assetPerformanceFixes = {};
      
      Object.entries(currencyData.assetPerformance).forEach(([assetKey, assetPerf]) => {
        const assetFixes = {};
        let assetNeedsFix = false;
        
        // Verificar campos faltantes
        if (assetPerf.doneProfitAndLoss === undefined) {
          assetFixes.doneProfitAndLoss = 0;
          assetNeedsFix = true;
        }
        if (assetPerf.monthlyReturn === undefined) {
          assetFixes.monthlyReturn = 0;
          assetNeedsFix = true;
        }
        if (assetPerf.annualReturn === undefined) {
          assetFixes.annualReturn = 0;
          assetNeedsFix = true;
        }
        
        // Verificar si los cambios diarios están en 0 cuando no deberían
        const prevAsset = prevAssetPerf[assetKey];
        const prevAssetValue = prevAsset?.totalValue || 0;
        const prevAssetUnits = prevAsset?.units || 0;
        
        // Si el asset existe en ambos días y los cambios están en 0
        if (prevAssetValue > 0 && assetPerf.totalValue > 0) {
          // Recalcular cambios diarios
          const unitsDiff = assetPerf.units - prevAssetUnits;
          const hadAssetCashFlow = Math.abs(unitsDiff) > 0.0001;
          
          // Calcular cambio bruto
          const rawChange = ((assetPerf.totalValue - prevAssetValue) / prevAssetValue) * 100;
          
          // Si el valor actual es 0 pero debería ser diferente
          if (Math.abs(assetPerf.rawDailyChangePercentage || 0) < 0.0001 && Math.abs(rawChange) > 0.0001) {
            assetFixes.rawDailyChangePercentage = rawChange;
            assetFixes.dailyChangePercentage = rawChange;
            
            // Calcular cambio ajustado
            if (hadAssetCashFlow) {
              const assetCashFlow = -(assetPerf.totalInvestment - (prevAsset?.totalInvestment || 0));
              assetFixes.adjustedDailyChangePercentage = ((assetPerf.totalValue - prevAssetValue + assetCashFlow) / prevAssetValue) * 100;
            } else {
              assetFixes.adjustedDailyChangePercentage = rawChange;
            }
            
            assetFixes.dailyReturn = assetFixes.adjustedDailyChangePercentage / 100;
            assetNeedsFix = true;
          }
        }
        
        if (assetNeedsFix) {
          assetPerformanceFixes[assetKey] = { ...assetPerf, ...assetFixes };
          needsFix = true;
          totalIssuesFound++;
        }
      });
      
      if (Object.keys(assetPerformanceFixes).length > 0) {
        fixes[currency] = {
          ...currencyData,
          assetPerformance: {
            ...currencyData.assetPerformance,
            ...assetPerformanceFixes
          }
        };
      }
    });
    
    if (needsFix) {
      documentsToFix.push({
        ref,
        date,
        fixes
      });
    }
  }
  
  log('INFO', `Documentos que necesitan corrección: ${documentsToFix.length}`);
  log('INFO', `Total de issues encontrados: ${totalIssuesFound}`);
  console.log('');
  
  if (documentsToFix.length === 0) {
    log('SUCCESS', 'No hay nada que corregir');
    process.exit(0);
  }
  
  // Mostrar muestra de correcciones
  log('PROGRESS', 'Muestra de correcciones:');
  documentsToFix.slice(0, 3).forEach(doc => {
    console.log(`  ${doc.date}:`);
    Object.entries(doc.fixes).forEach(([currency, data]) => {
      const assetCount = Object.keys(data.assetPerformance).length;
      console.log(`    ${currency}: ${assetCount} assets corregidos`);
    });
  });
  console.log('');
  
  // 4. Aplicar correcciones
  if (options.mode === 'fix') {
    log('PROGRESS', `Aplicando correcciones a ${documentsToFix.length} documentos...`);
    
    for (let i = 0; i < documentsToFix.length; i += CONFIG.BATCH_SIZE) {
      const batch = db.batch();
      const chunk = documentsToFix.slice(i, i + CONFIG.BATCH_SIZE);
      
      chunk.forEach(doc => {
        batch.update(doc.ref, doc.fixes);
      });
      
      await batch.commit();
      log('SUCCESS', `Batch ${Math.floor(i / CONFIG.BATCH_SIZE) + 1} completado (${chunk.length} docs)`);
    }
    
    log('SUCCESS', `✅ Corrección completada: ${documentsToFix.length} documentos actualizados`);
  } else {
    log('WARN', 'Modo dry-run: No se aplicaron cambios');
    log('INFO', 'Ejecuta con --fix para aplicar las correcciones');
  }
  
  process.exit(0);
}

fixOverallAssetPerformance().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
