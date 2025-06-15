# 🧪 Carpeta de Pruebas (Tests)

Esta carpeta contiene todos los archivos de prueba y testing del proyecto GoogleFinanceAPI.

## 📁 Estructura de Archivos

### 🔧 Pruebas de Servicios Principales
- `testUnifiedMarketDataUpdate.js` - Pruebas de la función unificada de actualización de datos de mercado
- `testPortfolioPerformanceOnly.js` - Pruebas específicas de cálculo de rendimiento de portafolio
- `testUpdatePortfolioPerformance.js` - Pruebas de actualización de rendimiento de portafolio
- `testUpdateCurrentPrices.js` - Pruebas de actualización de precios actuales
- `testUpdateCurrencyRates.js` - Pruebas de actualización de tasas de cambio

### 💰 Pruebas de Gestión Financiera
- `testUpdateCashFlow.js` - Pruebas de flujo de efectivo
- `testCalculateAccountPerformance.js` - Pruebas de cálculo de rendimiento de cuentas
- `testProcessDividendPayments.js` - Pruebas de procesamiento de pagos de dividendos

### 🌍 Pruebas de Datos Internacionales
- `testUpdateCurrencyRatesFromExistingCountriesInYahoo.js` - Pruebas de tasas de cambio desde Yahoo Finance
- `testUpdateCurrencyFlags.js` - Pruebas de actualización de banderas de países
- `testUpdateCountrySecondName.js` - Pruebas de nombres secundarios de países
- `testGetInformationFromCountries.js` - Pruebas de información de países

### 📊 Pruebas de Datos de Mercado
- `testSaveAllIndicesAndSectorsHistoryData.js` - Pruebas de guardado de datos históricos de índices
- `testMarketStatusUpdate.js` - Pruebas de actualización de estado del mercado

### 🔧 Pruebas de Utilidades
- `testReplaceTickerNameFromAssetPerfomance.js` - Pruebas de reemplazo de nombres de tickers
- `testDeleteKeys.js` - Pruebas de eliminación de claves

## 🚀 Cómo Ejecutar las Pruebas

Para ejecutar cualquier archivo de prueba desde la carpeta `functions`:

```bash
# Ejemplo: ejecutar pruebas de la función unificada
node tests/testUnifiedMarketDataUpdate.js

# Ejemplo: ejecutar pruebas de rendimiento de portafolio
node tests/testPortfolioPerformanceOnly.js
```

## 📝 Notas Importantes

1. **Rutas de Importación**: Todos los archivos de prueba han sido actualizados para usar rutas relativas correctas hacia la carpeta `services`.

2. **Dependencias**: Los archivos de prueba requieren acceso a:
   - Firebase Admin SDK
   - Servicios de la carpeta `../services/`
   - Utilidades de la carpeta `../utils/`

3. **Configuración**: Asegúrate de tener configuradas las variables de entorno necesarias antes de ejecutar las pruebas.

## 🔄 Migración Realizada

Los archivos fueron movidos desde `functions/services/` a `functions/tests/` para mejorar la organización del proyecto y separar claramente el código de producción del código de pruebas. 