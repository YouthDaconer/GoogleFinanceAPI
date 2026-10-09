/**
 * Script de reparación para la corrupción de datos del 2026-01-16 al 2026-01-20
 * 
 * PROBLEMA IDENTIFICADO:
 * 1. 2026-01-16: scheduledPortfolioCalculations solo escribió overall, NO las cuentas
 * 2. 2026-01-17: No hay datos (viernes, día de trading válido)
 * 3. 2026-01-20: Datos corruptos por inconsistencia de caché
 * 
 * SOLUCIÓN:
 * 1. Eliminar documentos corruptos del 2026-01-20
 * 2. Propagar datos de cuentas del 2026-01-15 al 2026-01-16 (como base)
 * 3. Regenerar 2026-01-17 usando precios históricos o del 2026-01-16
 * 
 * @author Arquitecto Ceiba
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const accounts = [
  { id: 'BZHvXz4QT2yqqqlFP22X', name: 'IBKR' },
  { id: 'Z3gnboYgRlTvSZNGSu8j', name: 'XTB' },
  { id: 'zHZCvwpQeA2HoYMxDtPF', name: 'Binance Cryptos' },
  { id: 'ggM52GimbLL7jwvegc9o', name: 'Trii' },
  { id: '7yOZyIh2YBRN26WyOpb7', name: 'Other' }
];

// MODO: 'dry-run' o 'execute'
const MODE = process.argv[2] || 'dry-run';

async function log(message) {
  console.log(`[${MODE}] ${message}`);
}

async function deleteDocument(path) {
  if (MODE === 'execute') {
    await db.doc(path).delete();
    console.log(`  🗑️ DELETED: ${path}`);
  } else {
    console.log(`  🔍 Would delete: ${path}`);
  }
}

async function writeDocument(path, data) {
  if (MODE === 'execute') {
    await db.doc(path).set(data);
    console.log(`  ✅ WRITTEN: ${path}`);
  } else {
    console.log(`  📝 Would write to: ${path}`);
    console.log(`     Data preview: date=${data.date}, USD.totalValue=${data.USD?.totalValue?.toFixed(2)}`);
  }
}

async function repair() {
  console.log('='.repeat(90));
  console.log(`REPARACIÓN DE DATOS - MODO: ${MODE.toUpperCase()}`);
  console.log('='.repeat(90));
  
  if (MODE === 'dry-run') {
    console.log('⚠️  MODO DRY-RUN: No se realizarán cambios. Use "node repairCorruption.js execute" para aplicar.');
  }
  console.log('');
  
  // =========================================================================
  // PASO 1: Eliminar datos corruptos del 2026-01-20
  // =========================================================================
  console.log('─'.repeat(90));
  console.log('PASO 1: Eliminar datos corruptos del 2026-01-20');
  console.log('─'.repeat(90));
  
  // Eliminar overall
  await deleteDocument(`portfolioPerformance/${userId}/dates/2026-01-20`);
  
  // Eliminar todas las cuentas
  for (const account of accounts) {
    const docRef = db.doc(`portfolioPerformance/${userId}/accounts/${account.id}/dates/2026-01-20`);
    const doc = await docRef.get();
    if (doc.exists) {
      await deleteDocument(docRef.path);
    }
  }
  console.log('');
  
  // =========================================================================
  // PASO 2: Propagar datos de cuentas del 2026-01-15 al 2026-01-16
  // =========================================================================
  console.log('─'.repeat(90));
  console.log('PASO 2: Propagar datos de cuentas del 2026-01-15 al 2026-01-16');
  console.log('─'.repeat(90));
  
  // Obtener datos de 2026-01-15 para cada cuenta
  for (const account of accounts) {
    const doc15 = await db.doc(`portfolioPerformance/${userId}/accounts/${account.id}/dates/2026-01-15`).get();
    const doc16 = await db.doc(`portfolioPerformance/${userId}/accounts/${account.id}/dates/2026-01-16`).get();
    
    if (doc16.exists) {
      console.log(`  ℹ️ ${account.name}: Ya existe 2026-01-16, saltando`);
      continue;
    }
    
    if (!doc15.exists) {
      console.log(`  ⚠️ ${account.name}: No existe 2026-01-15, saltando`);
      continue;
    }
    
    const data15 = doc15.data();
    
    // Crear datos para 2026-01-16 basados en 2026-01-15
    // Como el 16 fue un día de trading, los valores deberían haber cambiado
    // Pero para reparar, usaremos los mismos valores como aproximación
    const data16 = {
      ...data15,
      date: '2026-01-16',
      // Resetear el cambio diario ya que estamos interpolando
      USD: {
        ...data15.USD,
        dailyChangePercentage: 0,
        adjustedDailyChangePercentage: 0,
        totalCashFlow: 0
      },
      EUR: data15.EUR ? { ...data15.EUR, dailyChangePercentage: 0, adjustedDailyChangePercentage: 0, totalCashFlow: 0 } : undefined,
      COP: data15.COP ? { ...data15.COP, dailyChangePercentage: 0, adjustedDailyChangePercentage: 0, totalCashFlow: 0 } : undefined,
      MXN: data15.MXN ? { ...data15.MXN, dailyChangePercentage: 0, adjustedDailyChangePercentage: 0, totalCashFlow: 0 } : undefined,
      GBP: data15.GBP ? { ...data15.GBP, dailyChangePercentage: 0, adjustedDailyChangePercentage: 0, totalCashFlow: 0 } : undefined,
      CAD: data15.CAD ? { ...data15.CAD, dailyChangePercentage: 0, adjustedDailyChangePercentage: 0, totalCashFlow: 0 } : undefined,
      BRL: data15.BRL ? { ...data15.BRL, dailyChangePercentage: 0, adjustedDailyChangePercentage: 0, totalCashFlow: 0 } : undefined
    };
    
    // Limpiar campos undefined
    Object.keys(data16).forEach(key => {
      if (data16[key] === undefined) delete data16[key];
    });
    
    await writeDocument(`portfolioPerformance/${userId}/accounts/${account.id}/dates/2026-01-16`, data16);
  }
  console.log('');
  
  // =========================================================================
  // PASO 3: Crear datos para 2026-01-17 (viernes)
  // =========================================================================
  console.log('─'.repeat(90));
  console.log('PASO 3: Crear datos para 2026-01-17 (día faltante)');
  console.log('─'.repeat(90));
  
  // Verificar si ya existe
  const overall17 = await db.doc(`portfolioPerformance/${userId}/dates/2026-01-17`).get();
  
  if (overall17.exists) {
    console.log('  ℹ️ 2026-01-17 overall ya existe, verificando cuentas...');
  } else {
    // Usar datos de 2026-01-16 como base para el overall
    const overall16 = await db.doc(`portfolioPerformance/${userId}/dates/2026-01-16`).get();
    if (overall16.exists) {
      const data16 = overall16.data();
      const data17 = {
        ...data16,
        date: '2026-01-17',
        source: 'repair-script-backfill',
        repairedAt: new Date().toISOString()
      };
      
      // Resetear cambios diarios
      for (const currency of ['USD', 'EUR', 'COP', 'MXN', 'GBP', 'CAD', 'BRL']) {
        if (data17[currency]) {
          data17[currency] = {
            ...data17[currency],
            dailyChangePercentage: 0,
            adjustedDailyChangePercentage: 0,
            totalCashFlow: 0
          };
        }
      }
      
      await writeDocument(`portfolioPerformance/${userId}/dates/2026-01-17`, data17);
    }
  }
  
  // Crear datos de cuentas para 2026-01-17
  for (const account of accounts) {
    const doc17 = await db.doc(`portfolioPerformance/${userId}/accounts/${account.id}/dates/2026-01-17`).get();
    
    if (doc17.exists) {
      console.log(`  ℹ️ ${account.name}: Ya existe 2026-01-17`);
      continue;
    }
    
    // Usar datos de 2026-01-16 como base
    let doc16 = await db.doc(`portfolioPerformance/${userId}/accounts/${account.id}/dates/2026-01-16`).get();
    
    // Si no existe 2026-01-16, usar 2026-01-15
    if (!doc16.exists) {
      doc16 = await db.doc(`portfolioPerformance/${userId}/accounts/${account.id}/dates/2026-01-15`).get();
    }
    
    if (!doc16.exists) {
      console.log(`  ⚠️ ${account.name}: No hay datos base, saltando`);
      continue;
    }
    
    const dataBase = doc16.data();
    const data17 = {
      ...dataBase,
      date: '2026-01-17',
      source: 'repair-script-backfill',
      repairedAt: new Date().toISOString()
    };
    
    // Resetear cambios diarios
    for (const currency of ['USD', 'EUR', 'COP', 'MXN', 'GBP', 'CAD', 'BRL']) {
      if (data17[currency]) {
        data17[currency] = {
          ...data17[currency],
          dailyChangePercentage: 0,
          adjustedDailyChangePercentage: 0,
          totalCashFlow: 0
        };
      }
    }
    
    await writeDocument(`portfolioPerformance/${userId}/accounts/${account.id}/dates/2026-01-17`, data17);
  }
  console.log('');
  
  // =========================================================================
  // RESUMEN
  // =========================================================================
  console.log('='.repeat(90));
  console.log('RESUMEN DE REPARACIÓN');
  console.log('='.repeat(90));
  
  if (MODE === 'dry-run') {
    console.log(`
⚠️  MODO DRY-RUN COMPLETADO

Para aplicar los cambios, ejecute:
  node repairCorruption.js execute

Esto realizará las siguientes acciones:
1. Eliminar datos corruptos del 2026-01-20 (overall y cuentas)
2. Crear datos de cuentas para 2026-01-16 basados en 2026-01-15
3. Crear datos de overall y cuentas para 2026-01-17

IMPORTANTE: Después de ejecutar, debe:
1. Invalidar el cache de performance
2. Verificar que el frontend muestre datos correctos
    `);
  } else {
    console.log(`
✅ REPARACIÓN COMPLETADA

Próximos pasos:
1. Invalidar cache de performance para el usuario
2. Verificar en el frontend que los datos son correctos
3. Ejecutar unifiedMarketDataUpdate manualmente si es necesario para 2026-01-20
    `);
  }
}

repair().catch(console.error).finally(() => process.exit(0));
