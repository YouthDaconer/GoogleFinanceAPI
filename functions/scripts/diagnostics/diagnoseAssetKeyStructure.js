/**
 * Diagnóstico de estructura de assetPerformance en datos históricos
 * 
 * Problema potencial: datos guardados como:
 *   assetPerformance.VUAA.L (incorrecto - sin _stock)
 * en lugar de:
 *   assetPerformance["VUAA.L_stock"] (correcto - con tipo de asset)
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

async function diagnoseAssetPerformanceStructure() {
  console.log('='.repeat(100));
  console.log('DIAGNÓSTICO DE ESTRUCTURA DE assetPerformance');
  console.log('='.repeat(100));
  console.log();

  // 1. Revisar datos a nivel OVERALL
  console.log('📂 NIVEL OVERALL');
  console.log('-'.repeat(80));
  
  const overallSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/dates`)
    .orderBy('date', 'asc')
    .get();

  const assetKeysFound = new Map(); // key -> { correctFormat: count, incorrectFormat: count, dates: [] }
  const problematicDates = [];

  for (const doc of overallSnapshot.docs) {
    const data = doc.data();
    const currencyData = data.USD;
    
    if (!currencyData || !currencyData.assetPerformance) continue;

    const assetPerf = currencyData.assetPerformance;
    const assetKeys = Object.keys(assetPerf);

    for (const key of assetKeys) {
      // Verificar si el key tiene el formato correcto (contiene _)
      const hasUnderscore = key.includes('_');
      
      if (!assetKeysFound.has(key)) {
        assetKeysFound.set(key, { 
          correct: hasUnderscore, 
          count: 0, 
          firstDate: data.date,
          lastDate: data.date,
          sampleValue: assetPerf[key]
        });
      }
      
      const info = assetKeysFound.get(key);
      info.count++;
      info.lastDate = data.date;
    }

    // Detectar fechas con keys incorrectos
    const incorrectKeys = assetKeys.filter(k => !k.includes('_'));
    if (incorrectKeys.length > 0) {
      problematicDates.push({
        date: data.date,
        incorrectKeys,
        allKeys: assetKeys
      });
    }
  }

  console.log();
  console.log('📊 ASSET KEYS ENCONTRADOS:');
  console.log();
  
  // Separar correctos e incorrectos
  const correctKeys = [];
  const incorrectKeys = [];
  
  for (const [key, info] of assetKeysFound) {
    if (info.correct) {
      correctKeys.push({ key, ...info });
    } else {
      incorrectKeys.push({ key, ...info });
    }
  }

  console.log('✅ KEYS CON FORMATO CORRECTO (contienen _):');
  for (const item of correctKeys.sort((a, b) => a.key.localeCompare(b.key))) {
    console.log(`   ${item.key}: ${item.count} docs (${item.firstDate} → ${item.lastDate})`);
  }
  console.log();

  if (incorrectKeys.length > 0) {
    console.log('❌ KEYS CON FORMATO INCORRECTO (sin _):');
    for (const item of incorrectKeys.sort((a, b) => a.key.localeCompare(b.key))) {
      console.log(`   ${item.key}: ${item.count} docs (${item.firstDate} → ${item.lastDate})`);
      console.log(`      Sample value:`, JSON.stringify(item.sampleValue).substring(0, 100));
    }
    console.log();
  } else {
    console.log('✅ No se encontraron keys con formato incorrecto a nivel OVERALL');
    console.log();
  }

  // 2. Revisar datos a nivel de CUENTA XTB
  console.log('📂 NIVEL CUENTA XTB');
  console.log('-'.repeat(80));
  
  const accountSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/accounts/${ACCOUNT_ID}/dates`)
    .orderBy('date', 'asc')
    .get();

  const accountAssetKeysFound = new Map();

  for (const doc of accountSnapshot.docs) {
    const data = doc.data();
    const currencyData = data.USD;
    
    if (!currencyData || !currencyData.assetPerformance) continue;

    const assetPerf = currencyData.assetPerformance;
    const assetKeys = Object.keys(assetPerf);

    for (const key of assetKeys) {
      const hasUnderscore = key.includes('_');
      
      if (!accountAssetKeysFound.has(key)) {
        accountAssetKeysFound.set(key, { 
          correct: hasUnderscore, 
          count: 0, 
          firstDate: data.date,
          lastDate: data.date
        });
      }
      
      const info = accountAssetKeysFound.get(key);
      info.count++;
      info.lastDate = data.date;
    }
  }

  const accountCorrectKeys = [];
  const accountIncorrectKeys = [];
  
  for (const [key, info] of accountAssetKeysFound) {
    if (info.correct) {
      accountCorrectKeys.push({ key, ...info });
    } else {
      accountIncorrectKeys.push({ key, ...info });
    }
  }

  console.log();
  console.log('✅ KEYS CON FORMATO CORRECTO:');
  for (const item of accountCorrectKeys.sort((a, b) => a.key.localeCompare(b.key))) {
    console.log(`   ${item.key}: ${item.count} docs (${item.firstDate} → ${item.lastDate})`);
  }
  console.log();

  if (accountIncorrectKeys.length > 0) {
    console.log('❌ KEYS CON FORMATO INCORRECTO:');
    for (const item of accountIncorrectKeys.sort((a, b) => a.key.localeCompare(b.key))) {
      console.log(`   ${item.key}: ${item.count} docs (${item.firstDate} → ${item.lastDate})`);
    }
    console.log();
  } else {
    console.log('✅ No se encontraron keys con formato incorrecto a nivel CUENTA');
    console.log();
  }

  // 3. Análisis de impacto
  console.log('='.repeat(100));
  console.log('ANÁLISIS DE IMPACTO');
  console.log('='.repeat(100));
  console.log();

  if (incorrectKeys.length > 0 || accountIncorrectKeys.length > 0) {
    console.log('⚠️ SE DETECTARON PROBLEMAS DE ESTRUCTURA');
    console.log();
    console.log('El problema es que los datos históricos tienen keys sin el sufijo _assetType');
    console.log('Esto puede causar:');
    console.log('  1. El cálculo de rendimientos no encuentra los datos del día anterior');
    console.log('  2. El adjustedDailyChangePercentage se calcula incorrectamente');
    console.log('  3. Los rendimientos históricos (YTD, 6M, etc.) están afectados');
    console.log();
    
    // Mostrar fechas problemáticas
    if (problematicDates.length > 0) {
      console.log(`📅 Fechas con datos en formato incorrecto: ${problematicDates.length}`);
      console.log(`   Rango: ${problematicDates[0].date} → ${problematicDates[problematicDates.length - 1].date}`);
    }
  } else {
    console.log('✅ No se detectaron problemas de estructura');
  }

  // 4. Revisar un documento específico para ver la estructura real
  console.log();
  console.log('='.repeat(100));
  console.log('MUESTRA DE DOCUMENTOS');
  console.log('='.repeat(100));
  console.log();

  // Primer documento
  const firstDoc = overallSnapshot.docs[0];
  if (firstDoc) {
    console.log(`📄 Primer documento (${firstDoc.data().date}):`);
    const usdData = firstDoc.data().USD;
    if (usdData?.assetPerformance) {
      console.log('   Keys en assetPerformance:', Object.keys(usdData.assetPerformance));
    }
  }

  // Documento de mitad de año
  const midYearDoc = overallSnapshot.docs.find(d => d.data().date === '2025-06-05');
  if (midYearDoc) {
    console.log(`📄 Documento 2025-06-05:`);
    const usdData = midYearDoc.data().USD;
    if (usdData?.assetPerformance) {
      console.log('   Keys en assetPerformance:', Object.keys(usdData.assetPerformance));
    }
  }

  // Documento de junio 6 (después de la corrección?)
  const june6Doc = overallSnapshot.docs.find(d => d.data().date === '2025-06-06');
  if (june6Doc) {
    console.log(`📄 Documento 2025-06-06:`);
    const usdData = june6Doc.data().USD;
    if (usdData?.assetPerformance) {
      console.log('   Keys en assetPerformance:', Object.keys(usdData.assetPerformance));
    }
  }

  // Último documento
  const lastDoc = overallSnapshot.docs[overallSnapshot.docs.length - 1];
  if (lastDoc) {
    console.log(`📄 Último documento (${lastDoc.data().date}):`);
    const usdData = lastDoc.data().USD;
    if (usdData?.assetPerformance) {
      console.log('   Keys en assetPerformance:', Object.keys(usdData.assetPerformance));
    }
  }

  process.exit(0);
}

diagnoseAssetPerformanceStructure().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
