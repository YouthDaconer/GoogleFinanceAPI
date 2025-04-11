const admin = require('firebase-admin');
const { DateTime } = require('luxon');

/**
 * Calcula el porcentaje de semanas rentables basado en los datos de rendimiento diario
 * @param {string} startDate - Fecha de inicio en formato ISO (opcional, por defecto últimas 52 semanas)
 * @param {string} endDate - Fecha de fin en formato ISO (opcional, por defecto fecha actual)
 * @returns {Promise<null>}
 */
async function calculateProfitableWeeks(startDate, endDate) {
    const db = admin.firestore();

    // Si no se proporciona fecha de fin, usar la fecha actual
    const endDateTime = endDate
        ? DateTime.fromISO(endDate).setZone('America/New_York')
        : DateTime.now().setZone('America/New_York');

    // Si no se proporciona fecha de inicio, usar 52 semanas antes de la fecha de fin
    const startDateTime = startDate
        ? DateTime.fromISO(startDate).setZone('America/New_York')
        : endDateTime.minus({ weeks: 52 });

    const formattedStartDate = startDateTime.toISODate();
    const formattedEndDate = endDateTime.toISODate();
    const calculationDate = endDateTime.toISODate(); // Fecha de cálculo (fecha actual)

    console.log(`Calculando porcentaje de semanas rentables desde ${formattedStartDate} hasta ${formattedEndDate}`);

    try {
        // Obtener todos los usuarios con portfolioPerformance
        const portfolioPerformanceSnapshot = await db.collection('portfolioPerformance').get();
        const userIds = portfolioPerformanceSnapshot.docs.map(doc => doc.id);

        // Obtener monedas activas
        const currenciesSnapshot = await db.collection('currencies').where('isActive', '==', true).get();
        const currencies = currenciesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

        // Para cada usuario
        for (const userId of userIds) {
            console.log(`Procesando usuario: ${userId}`);

            // Obtener todas las fechas dentro del rango especificado
            const datesSnapshot = await db.collection('portfolioPerformance')
                .doc(userId)
                .collection('dates')
                .where('date', '>=', formattedStartDate)
                .where('date', '<=', formattedEndDate)
                .orderBy('date', 'asc')
                .get();

            if (datesSnapshot.empty) {
                console.log(`No hay datos de rendimiento para el usuario ${userId} en el rango especificado`);
                continue;
            }

            // Agrupar días por semana
            const dailyDataByWeek = {};
            const dateDataByWeek = {};

            datesSnapshot.docs.forEach(doc => {
                const date = doc.data().date;
                const dateTime = DateTime.fromISO(date);
                const weekNumber = dateTime.weekNumber;
                const year = dateTime.year;
                const weekKey = `${year}-W${weekNumber}`;

                if (!dailyDataByWeek[weekKey]) {
                    dailyDataByWeek[weekKey] = [];
                    dateDataByWeek[weekKey] = [];
                }

                dateDataByWeek[weekKey].push(doc.data());
                dailyDataByWeek[weekKey].push(date);
            });

            // Calcular si cada semana fue rentable para cada moneda
            const profitableWeeksByCurrency = {};

            currencies.forEach(currency => {
                profitableWeeksByCurrency[currency.code] = {
                    totalWeeks: 0,
                    profitableWeeks: 0,
                    profitableWeeksPercentage: 0,
                    latestWeek: null
                };
            });

            // Para cada semana
            for (const [weekKey, dates] of Object.entries(dailyDataByWeek)) {
                // Ordenar fechas por orden cronológico
                dates.sort();

                // Obtener primera y última fecha de la semana
                const firstDate = dates[0];
                const lastDate = dates[dates.length - 1];

                const firstDateData = dateDataByWeek[weekKey].find(d => d.date === firstDate);
                const lastDateData = dateDataByWeek[weekKey].find(d => d.date === lastDate);

                if (!firstDateData || !lastDateData) continue;

                // Para cada moneda
                currencies.forEach(currency => {
                    const currencyCode = currency.code;

                    // Verificar si hay datos para esta moneda
                    if (firstDateData[currencyCode] && lastDateData[currencyCode]) {
                        const firstTotalValue = firstDateData[currencyCode].totalValue || 0;
                        const lastTotalValue = lastDateData[currencyCode].totalValue || 0;

                        // Considerar semanas parciales (si tienen al menos 3 días)
                        if (dates.length >= 3) {
                            profitableWeeksByCurrency[currencyCode].totalWeeks++;

                            // Calcular si la semana fue rentable (valor final > valor inicial)
                            const weeklyPerformance = lastTotalValue > firstTotalValue;

                            if (weeklyPerformance) {
                                profitableWeeksByCurrency[currencyCode].profitableWeeks++;
                            }

                            // Guardar datos solo de la semana más reciente para referencia
                            const weeklyReturn = firstTotalValue > 0
                                ? ((lastTotalValue - firstTotalValue) / firstTotalValue) * 100
                                : 0;

                            // Almacenar solo la semana más reciente
                            // Determinar si esta es la semana más reciente que hemos procesado
                            const weekDateTime = DateTime.fromISO(lastDate);
                            const currentLatestWeek = profitableWeeksByCurrency[currencyCode].latestWeek;

                            if (!currentLatestWeek ||
                                weekDateTime > DateTime.fromISO(currentLatestWeek.endDate)) {
                                profitableWeeksByCurrency[currencyCode].latestWeek = {
                                    weekKey,
                                    profitable: weeklyPerformance,
                                    startDate: firstDate,
                                    endDate: lastDate,
                                    startValue: firstTotalValue,
                                    endValue: lastTotalValue,
                                    weeklyReturn
                                };
                            }
                        }
                    }
                });
            }

            // Calcular porcentajes
            currencies.forEach(currency => {
                const currencyData = profitableWeeksByCurrency[currency.code];

                if (currencyData.totalWeeks > 0) {
                    currencyData.profitableWeeksPercentage =
                        (currencyData.profitableWeeks / currencyData.totalWeeks) * 100;
                }
            });

            // Crear batch para realizar todas las escrituras
            const batch = db.batch();

            // Guardar resultados en la nueva colección portfolioMetrics
            const userMetricsRef = db.collection('portfolioMetrics').doc(userId);

            // Crear documento de weeklyMetrics - usando 'latest' como ID fijo
            const weeklyMetricsRef = userMetricsRef.collection('weeklyMetrics').doc('latest');

            // Preparar datos para escritura
            const weeklyMetricsData = {
                calculationDate: calculationDate,
                periodStart: formattedStartDate,
                periodEnd: formattedEndDate,
                metrics: profitableWeeksByCurrency
            };

            // Agregar a batch
            batch.set(weeklyMetricsRef, weeklyMetricsData);

            // Actualizar cuentas individuales
            const accountsSnapshot = await db.collection('portfolioPerformance')
                .doc(userId)
                .collection('accounts')
                .get();

            for (const accountDoc of accountsSnapshot.docs) {
                const accountId = accountDoc.id;

                // Obtener fechas para esta cuenta
                const accountDatesSnapshot = await db.collection('portfolioPerformance')
                    .doc(userId)
                    .collection('accounts')
                    .doc(accountId)
                    .collection('dates')
                    .where('date', '>=', formattedStartDate)
                    .where('date', '<=', formattedEndDate)
                    .orderBy('date', 'asc')
                    .get();

                if (accountDatesSnapshot.empty) {
                    console.log(`No hay datos de rendimiento para la cuenta ${accountId} en el rango especificado`);
                    continue;
                }

                // Agrupar días por semana para la cuenta
                const accountDailyDataByWeek = {};
                const accountDateDataByWeek = {};

                accountDatesSnapshot.docs.forEach(doc => {
                    const date = doc.data().date;
                    const dateTime = DateTime.fromISO(date);
                    const weekNumber = dateTime.weekNumber;
                    const year = dateTime.year;
                    const weekKey = `${year}-W${weekNumber}`;

                    if (!accountDailyDataByWeek[weekKey]) {
                        accountDailyDataByWeek[weekKey] = [];
                        accountDateDataByWeek[weekKey] = [];
                    }

                    accountDateDataByWeek[weekKey].push(doc.data());
                    accountDailyDataByWeek[weekKey].push(date);
                });

                // Calcular si cada semana fue rentable para cada moneda
                const accountProfitableWeeksByCurrency = {};

                currencies.forEach(currency => {
                    accountProfitableWeeksByCurrency[currency.code] = {
                        totalWeeks: 0,
                        profitableWeeks: 0,
                        profitableWeeksPercentage: 0,
                        latestWeek: null
                    };
                });

                // Para cada semana
                for (const [weekKey, dates] of Object.entries(accountDailyDataByWeek)) {
                    // Ordenar fechas por orden cronológico
                    dates.sort();

                    // Obtener primera y última fecha de la semana
                    const firstDate = dates[0];
                    const lastDate = dates[dates.length - 1];

                    const firstDateData = accountDateDataByWeek[weekKey].find(d => d.date === firstDate);
                    const lastDateData = accountDateDataByWeek[weekKey].find(d => d.date === lastDate);

                    if (!firstDateData || !lastDateData) continue;

                    // Para cada moneda
                    currencies.forEach(currency => {
                        const currencyCode = currency.code;

                        // Verificar si hay datos para esta moneda
                        if (firstDateData[currencyCode] && lastDateData[currencyCode]) {
                            const firstTotalValue = firstDateData[currencyCode].totalValue || 0;
                            const lastTotalValue = lastDateData[currencyCode].totalValue || 0;

                            // Considerar semanas parciales (si tienen al menos 3 días)
                            if (dates.length >= 3) {
                                accountProfitableWeeksByCurrency[currencyCode].totalWeeks++;

                                // Calcular si la semana fue rentable (valor final > valor inicial)
                                const weeklyPerformance = lastTotalValue > firstTotalValue;

                                if (weeklyPerformance) {
                                    accountProfitableWeeksByCurrency[currencyCode].profitableWeeks++;
                                }

                                // Determinar si esta es la semana más reciente que hemos procesado
                                const weekDateTime = DateTime.fromISO(lastDate);
                                const currentLatestWeek = accountProfitableWeeksByCurrency[currencyCode].latestWeek;

                                // Guardar datos solo de la semana más reciente
                                const weeklyReturn = firstTotalValue > 0
                                    ? ((lastTotalValue - firstTotalValue) / firstTotalValue) * 100
                                    : 0;

                                if (!currentLatestWeek ||
                                    weekDateTime > DateTime.fromISO(currentLatestWeek.endDate)) {
                                    accountProfitableWeeksByCurrency[currencyCode].latestWeek = {
                                        weekKey,
                                        profitable: weeklyPerformance,
                                        startDate: firstDate,
                                        endDate: lastDate,
                                        startValue: firstTotalValue,
                                        endValue: lastTotalValue,
                                        weeklyReturn
                                    };
                                }
                            }
                        }
                    });
                }

                // Calcular porcentajes
                currencies.forEach(currency => {
                    const currencyData = accountProfitableWeeksByCurrency[currency.code];

                    if (currencyData.totalWeeks > 0) {
                        currencyData.profitableWeeksPercentage =
                            (currencyData.profitableWeeks / currencyData.totalWeeks) * 100;
                    }
                });

                // Guardar resultados para la cuenta en la nueva estructura con ID 'latest'
                const accountWeeklyMetricsRef = userMetricsRef
                    .collection('accounts')
                    .doc(accountId)
                    .collection('weeklyMetrics')
                    .doc('latest');

                // Preparar datos para escritura
                const accountWeeklyMetricsData = {
                    calculationDate: calculationDate,
                    periodStart: formattedStartDate,
                    periodEnd: formattedEndDate,
                    metrics: accountProfitableWeeksByCurrency
                };

                // Agregar a batch
                batch.set(accountWeeklyMetricsRef, accountWeeklyMetricsData);
            }

            // Guardar todos los cambios
            await batch.commit();
            console.log(`Datos de semanas rentables calculados y actualizados para el usuario ${userId}`);
        }

        console.log('Cálculo de semanas rentables completado');
        return null;
    } catch (error) {
        console.error('Error al calcular semanas rentables:', error);
        return null;
    }
}

// Exportar para uso desde Cloud Functions
module.exports = { calculateProfitableWeeks };

// Si se ejecuta directamente, usar el período predeterminado (últimas 52 semanas)
if (require.main === module) {
    // Si Firebase Admin no está inicializado, inicializarlo
    if (!admin.apps.length) {
        admin.initializeApp();
    }

    // Se pueden pasar fechas como argumentos: node calculateProfitableWeeks.js 2023-01-01 2023-12-31
    const startDate = process.argv[2];
    const endDate = process.argv[3];

    calculateProfitableWeeks(startDate, endDate)
        .then(() => {
            console.log('Proceso completado');
            process.exit(0);
        })
        .catch(error => {
            console.error('Error en el proceso principal:', error);
            process.exit(1);
        });
}