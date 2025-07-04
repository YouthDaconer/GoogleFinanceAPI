const admin = require('../services/firebaseAdmin');
const { DateTime } = require('luxon');
const { scrapeDividendsInfoFromStockEvents } = require('../services/scrapeDividendsInfoFromStock');

async function testProcessDividendPayments(testDate) {
  const db = admin.firestore();
  const now = DateTime.now().setZone('America/New_York');
  const formattedDate = now.toISODate();

  console.log(`Verificando dividendos para la fecha ${formattedDate}`);

  try {
    // Primero actualizar información de dividendos para ETFs y acciones sin datos
    console.log('Actualizando información de dividendos de activos...');
    await scrapeDividendsInfoFromStockEvents();
    console.log('Actualización de información de dividendos completada');

    // Obtener los precios actuales
    const currentPricesSnapshot = await db.collection('currentPrices').get();
    const currentPrices = currentPricesSnapshot.docs.map(doc => ({
      id: doc.id,
      symbol: doc.id.split(':')[0],
      ...doc.data()
    }));

    // Filtrar activos con fecha de dividendo hoy y que tengan la información de dividendo necesaria
    const todaysDividends = currentPrices.filter(price => {
      // Verificar que tenga toda la información necesaria de dividendos
      if (!price.dividendDate || !price.dividend || parseFloat(price.dividend) <= 0) {
        return false;
      }

      // Convertir la fecha de dividendo a formato ISO
      try {
        const dividendDate = DateTime.fromFormat(price.dividendDate, 'MMM d, yyyy').toISODate();
        return dividendDate === formattedDate;
      } catch (error) {
        console.log(`Error al procesar fecha de dividendo para ${price.symbol}: ${error.message}`);
        return false;
      }
    });

    if (todaysDividends.length === 0) {
      console.log('No hay dividendos programados para hoy');
      return null;
    }

    console.log(`Encontrados ${todaysDividends.length} activos con dividendos para hoy`);

    // Obtener todos los activos activos
    const assetsSnapshot = await db.collection('assets').where('isActive', '==', true).get();
    const assets = assetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

    // Obtener todas las cuentas de cartera activas
    const portfolioAccountsSnapshot = await db.collection('portfolioAccounts').where('isActive', '==', true).get();
    const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

    // Obtener las monedas activas para la conversión
    const currenciesSnapshot = await db.collection('currencies').where('isActive', '==', true).get();
    const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

    // Obtener las transacciones de dividendos de hoy para evitar duplicados
    const todaysDividendTransactionsSnapshot = await db.collection('transactions')
      .where('date', '==', formattedDate)
      .where('type', '==', 'dividendPay')
      .get();
    const todaysDividendTransactions = todaysDividendTransactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

    // Agrupar activos por cuenta de portafolio y símbolo
    const portfolioSymbolAssets = {};

    for (const asset of assets) {
      const portfolioAccount = portfolioAccounts.find(acc => acc.id === asset.portfolioAccount);
      if (!portfolioAccount) continue;

      // Verificar si es un símbolo con dividendo hoy
      const matchingDividend = todaysDividends.find(div => div.symbol === asset.name);
      if (!matchingDividend) continue;

      // Verificar si el asset tiene fecha de adquisición y si la fecha de exDividend está disponible
      if (!matchingDividend.exDividend || !asset.acquisitionDate) {
        console.log(`Activo ${asset.id} (${asset.name}) excluido del pago de dividendos: ${!matchingDividend.exDividend ? 'falta fecha exDividend' : 'falta fecha de adquisición'}`);
        continue;
      }
      
      try {
        // Convertir la fecha de exDividend a formato ISO
        const exDividendDate = DateTime.fromFormat(matchingDividend.exDividend, 'MMM d, yyyy').toISODate();
        
        // Verificar si la fecha de adquisición es después de la fecha exDividend
        if (asset.acquisitionDate > exDividendDate) {
          console.log(`Activo ${asset.id} (${asset.name}) excluido del pago de dividendos: fue adquirido el ${asset.acquisitionDate} después de la fecha exDividend ${exDividendDate}`);
          continue;
        }
      } catch (error) {
        console.log(`Error al procesar fecha exDividend para ${matchingDividend.symbol}: ${error.message}, activo excluido`);
        continue;
      }

      const key = `${asset.portfolioAccount}_${asset.name}`;

      if (!portfolioSymbolAssets[key]) {
        portfolioSymbolAssets[key] = {
          portfolioAccountId: asset.portfolioAccount,
          userId: portfolioAccount.userId,
          symbol: asset.name,
          assetType: asset.assetType,
          currency: asset.currency,
          defaultCurrencyForAdquisitionDollar: asset.defaultCurrencyForAdquisitionDollar,
          dividend: matchingDividend,
          units: 0,
          relatedAssets: []
        };
      }

      portfolioSymbolAssets[key].units += parseFloat(asset.units || 0);
      portfolioSymbolAssets[key].relatedAssets.push(asset.id);
    }

    const batch = db.batch();
    let transactionsCreated = 0;
    
    // Mapa para acumular los dividendos por cuenta y moneda
    const portfolioAccountUpdates = {};

    // Crear transacciones por cuenta y símbolo
    for (const key of Object.keys(portfolioSymbolAssets)) {
      const portfolioSymbolData = portfolioSymbolAssets[key];

      // Verificar si ya existe una transacción para esta cuenta y símbolo hoy
      const existingTransaction = todaysDividendTransactions.find(
        t => t.portfolioAccountId === portfolioSymbolData.portfolioAccountId &&
          t.symbol === portfolioSymbolData.symbol &&
          t.type === 'dividendPay'
      );

      if (existingTransaction) {
        console.log(`Ya existe una transacción de dividendo para la cuenta ${portfolioSymbolData.portfolioAccountId} y símbolo ${portfolioSymbolData.symbol}`);
        continue;
      }

      // Calcular el monto del dividendo
      const annualDividend = parseFloat(portfolioSymbolData.dividend.dividend || 0);
      const totalUnits = portfolioSymbolData.units;

      if (annualDividend <= 0 || totalUnits <= 0) {
        console.log(`Monto de dividendo calculado es cero o negativo para ${portfolioSymbolData.symbol} en cuenta ${portfolioSymbolData.portfolioAccountId}`);
        continue;
      }

      // Crear una única transacción de dividendo para esta cuenta y símbolo
      const transactionRef = db.collection('transactions').doc();
      const transaction = {
        id: transactionRef.id,
        symbol: portfolioSymbolData.symbol,
        type: 'dividendPay',
        amount: totalUnits,
        price: annualDividend / 4,
        currency: portfolioSymbolData.currency || 'USD',
        date: formattedDate,
        portfolioAccountId: portfolioSymbolData.portfolioAccountId,
        commission: 0,
        assetType: portfolioSymbolData.assetType,
        dollarPriceToDate: currencies.find(c => c.code === 'USD')?.exchangeRate || 1,
        defaultCurrencyForAdquisitionDollar: portfolioSymbolData.defaultCurrencyForAdquisitionDollar || 'USD',
        description: `Pago de dividendo anual de ${portfolioSymbolData.dividend.name || portfolioSymbolData.symbol} (${totalUnits} unidades)`,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        userId: portfolioSymbolData.userId,
        relatedAssets: portfolioSymbolData.relatedAssets
      };

      batch.set(transactionRef, transaction);
      transactionsCreated++;

      const amount = (annualDividend / 4) * totalUnits;
      console.log(`Creada transacción de dividendo para ${portfolioSymbolData.symbol} en cuenta ${portfolioSymbolData.portfolioAccountId}, unidades totales: ${totalUnits}, monto: ${amount} ${transaction.currency}`);
      
      // Acumular los montos de dividendos por cuenta y moneda
      const accountKey = portfolioSymbolData.portfolioAccountId;
      const currency = transaction.currency;
      
      if (!portfolioAccountUpdates[accountKey]) {
        portfolioAccountUpdates[accountKey] = {};
      }
      
      if (!portfolioAccountUpdates[accountKey][currency]) {
        portfolioAccountUpdates[accountKey][currency] = 0;
      }
      
      portfolioAccountUpdates[accountKey][currency] += amount;
    }
    
    // Actualizar los balances de las cuentas después de acumular todos los dividendos
    for (const [accountId, currencyAmounts] of Object.entries(portfolioAccountUpdates)) {
      const portfolioAccountRef = db.collection('portfolioAccounts').doc(accountId);
      const portfolioAccountDoc = await portfolioAccountRef.get();
      const portfolioAccountData = portfolioAccountDoc.data();
      
      // Inicializar balances si no existe
      if (!portfolioAccountData.balances) {
        portfolioAccountData.balances = {};
      }
      
      // Actualizar el balance para cada moneda
      for (const [currency, amount] of Object.entries(currencyAmounts)) {
        const currentCurrencyBalance = portfolioAccountData.balances[currency] || 0;
        portfolioAccountData.balances[currency] = currentCurrencyBalance + amount;
        console.log(`Acumulado dividendo de ${amount} ${currency} para la cuenta ${accountId}. Nuevo balance: ${portfolioAccountData.balances[currency]}`);
      }
      
      // Actualizar la cuenta del portafolio en el batch
      batch.update(portfolioAccountRef, {
        balances: portfolioAccountData.balances
      });
      console.log(`Preparada actualización de balances para la cuenta ${accountId}`);
    }

    if (transactionsCreated > 0) {
      await batch.commit();
      console.log(`${transactionsCreated} transacciones de dividendos procesadas exitosamente y balances actualizados`);
    } else {
      console.log('No se crearon transacciones de dividendos');
    }

    return null;
  } catch (error) {
    console.error('Error al procesar dividendos:', error);
    return null;
  }
}

// Llamar a la función de prueba con una fecha específica opcional
// testProcessDividendPayments("2023-10-17"); // Descomentar para probar con fecha específica
testProcessDividendPayments("2025-03-31");
