const functions = require('firebase-functions');
const admin = require('firebase-admin');
const { DateTime } = require('luxon');

exports.processDividendPayments = functions.pubsub
  .schedule('0 7,18 * * *')  // Ejecutar a las 7:00 AM y 6:00 PM todos los días
  .timeZone('America/New_York')
  .onRun(async (context) => {
    const db = admin.firestore();
    const now = DateTime.now().setZone('America/New_York');
    const formattedDate = now.toISODate();

    console.log(`Verificando dividendos para la fecha ${formattedDate}`);

    try {
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
        const amount = annualDividend * totalUnits;

        if (amount <= 0 || totalUnits <= 0) {
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
          price: annualDividend,
          currency: portfolioSymbolData.currency || 'USD',
          date: formattedDate,
          portfolioAccountId: portfolioSymbolData.portfolioAccountId,
          commission: 0,
          assetType: portfolioSymbolData.assetType,
          dollarPriceToDate: currencies.find(c => c.code === 'USD')?.exchangeRate || 1,
          defaultCurrencyForAdquisitionDollar: portfolioSymbolData.defaultCurrencyForAdquisitionDollar || 'USD',
          description: `Pago de dividendo trimestral de ${portfolioSymbolData.dividend.name || portfolioSymbolData.symbol} (${totalUnits} unidades)`,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          userId: portfolioSymbolData.userId,
          relatedAssets: portfolioSymbolData.relatedAssets  // Guardamos referencia a todos los assets relacionados
        };

        batch.set(transactionRef, transaction);
        transactionsCreated++;

        console.log(`Creada transacción de dividendo para ${portfolioSymbolData.symbol} en cuenta ${portfolioSymbolData.portfolioAccountId}, unidades totales: ${totalUnits}, monto: ${amount} ${transaction.currency}`);
      }

      if (transactionsCreated > 0) {
        await batch.commit();
        console.log(`${transactionsCreated} transacciones de dividendos procesadas exitosamente`);
      } else {
        console.log('No se crearon transacciones de dividendos');
      }

      return null;
    } catch (error) {
      console.error('Error al procesar dividendos:', error);
      return null;
    }
  }); 