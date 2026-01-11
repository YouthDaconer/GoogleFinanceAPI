/**
 * Script para recalcular los cambios diarios en todos los documentos
 * después de corregir los tipos de cambio
 * 
 * USO:
 *   node recalculateDailyChanges.js --dry-run    # Ver cambios sin aplicar
 *   node recalculateDailyChanges.js --fix        # Aplicar cambios
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
  BATCH_SIZE: 10,
};

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    mode: args.includes('--fix') ? 'fix' : 'dry-run'
  };
}

function log(level, message) {
  const colors = {
    INFO: '\x1b[36m',
    SUCCESS: '\x1b[32m',
    WARN: '\x1b[33m',
    PROGRESS: '\x1b[35m',
  };
  const reset = '\x1b[0m';
  console.log(`${colors[level] || ''}[${level}]${reset} ${message}`);
}

async function recalculateDailyChanges() {
  const options = parseArgs();
  
  console.log('═'.repeat(80));
  console.log('  RECÁLCULO DE CAMBIOS DIARIOS');
  console.log('═'.repeat(80));
  console.log('');
  log('INFO', `Modo: ${options.mode.toUpperCase()}`);
  
  // Obtener todos los documentos ordenados por fecha
  const allDocs = await db.collection(`portfolioPerformance/${CONFIG.USER_ID}/dates`)
    .orderBy('date', 'asc')
    .get();
  
  log('INFO', `Documentos encontrados: ${allDocs.docs.length}`);
  
  // Construir mapa de documentos
  const docsByDate = new Map();
  allDocs.docs.forEach(doc => {
    const data = doc.data();
    docsByDate.set(data.date, { ref: doc.ref, data });
  });
  
  const allDates = [...docsByDate.keys()].sort();
  
  // Recalcular cambios diarios para cada documento
  const documentsToFix = [];
  let totalChanges = 0;
  
  for (let i = 0; i < allDates.length; i++) {
    const date = allDates[i];
    const { ref, data } = docsByDate.get(date);
    
    // Obtener día anterior
    let previousData = null;
    if (i > 0) {
      previousData = docsByDate.get(allDates[i - 1])?.data;
    }
    
    const fixes = {};
    let hasChanges = false;
    
    CONFIG.CURRENCIES.forEach(currency => {
      const currencyData = data[currency];
      if (!currencyData || !currencyData.assetPerformance) return;
      
      const prevCurrencyData = previousData?.[currency];
      const prevAssetPerf = prevCurrencyData?.assetPerformance || {};
      
      const assetFixes = {};
      
      Object.entries(currencyData.assetPerformance).forEach(([assetKey, asset]) => {
        const prevAsset = prevAssetPerf[assetKey];
        const prevValue = prevAsset?.totalValue || 0;
        const currentValue = asset.totalValue || 0;
        
        // Calcular cambios diarios correctos
        let newRaw = 0;
        let newAdj = 0;
        
        if (prevValue > 0 && currentValue > 0) {
          newRaw = ((currentValue - prevValue) / prevValue) * 100;
          
          // Para adjusted, considerar si hubo cashflow
          const unitsDiff = (asset.units || 0) - (prevAsset?.units || 0);
          const hadCashFlow = Math.abs(unitsDiff) > 0.0001;
          
          if (hadCashFlow) {
            const assetCashFlow = -((asset.totalInvestment || 0) - (prevAsset?.totalInvestment || 0));
            newAdj = ((currentValue - prevValue + assetCashFlow) / prevValue) * 100;
          } else {
            newAdj = newRaw;
          }
        } else if (prevValue === 0 && currentValue > 0) {
          // Nueva inversión - 0%
          newRaw = 0;
          newAdj = 0;
        }
        
        // Verificar si necesita corrección (diferencia > 0.01%)
        const currentRaw = asset.rawDailyChangePercentage || 0;
        const currentAdj = asset.adjustedDailyChangePercentage || 0;
        
        if (Math.abs(currentRaw - newRaw) > 0.01 || Math.abs(currentAdj - newAdj) > 0.01) {
          assetFixes[assetKey] = {
            ...asset,
            rawDailyChangePercentage: newRaw,
            dailyChangePercentage: newRaw,
            adjustedDailyChangePercentage: newAdj,
            dailyReturn: newAdj / 100,
          };
          hasChanges = true;
          totalChanges++;
        }
      });
      
      if (Object.keys(assetFixes).length > 0) {
        fixes[currency] = {
          ...currencyData,
          assetPerformance: {
            ...currencyData.assetPerformance,
            ...assetFixes
          }
        };
      }
    });
    
    if (hasChanges) {
      documentsToFix.push({ ref, date, fixes });
    }
  }
  
  log('INFO', `Documentos a corregir: ${documentsToFix.length}`);
  log('INFO', `Total de cambios: ${totalChanges}`);
  
  if (documentsToFix.length === 0) {
    log('SUCCESS', 'No hay nada que corregir');
    process.exit(0);
  }
  
  // Mostrar muestra
  if (documentsToFix.length > 0) {
    const sample = documentsToFix.find(d => d.date === '2025-02-18') || documentsToFix[0];
    log('PROGRESS', `Muestra (${sample.date}):`);
    Object.entries(sample.fixes).slice(0, 2).forEach(([currency, data]) => {
      const asset = Object.keys(data.assetPerformance)[0];
      if (asset) {
        console.log(`  ${currency} ${asset}: adj=${data.assetPerformance[asset].adjustedDailyChangePercentage?.toFixed(4)}%`);
      }
    });
  }
  
  // Aplicar correcciones
  if (options.mode === 'fix') {
    log('PROGRESS', 'Aplicando correcciones...');
    
    for (let i = 0; i < documentsToFix.length; i += CONFIG.BATCH_SIZE) {
      const batch = db.batch();
      const chunk = documentsToFix.slice(i, i + CONFIG.BATCH_SIZE);
      
      chunk.forEach(({ ref, fixes }) => {
        batch.update(ref, fixes);
      });
      
      await batch.commit();
      log('SUCCESS', `Batch ${Math.floor(i / CONFIG.BATCH_SIZE) + 1} completado (${chunk.length} docs)`);
    }
    
    log('SUCCESS', `✅ Corrección completada: ${documentsToFix.length} documentos`);
  } else {
    log('WARN', 'Modo dry-run: No se aplicaron cambios');
  }
  
  process.exit(0);
}

recalculateDailyChanges().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
