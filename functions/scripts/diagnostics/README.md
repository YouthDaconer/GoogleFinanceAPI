# Scripts de Diagnóstico de Portafolio

Esta carpeta contiene scripts para diagnosticar, verificar y corregir datos de rendimiento del portafolio.

## Estructura

```
diagnostics/
├── README.md                           # Este archivo
├── diagnoseAsset.js                    # Diagnóstico genérico de cualquier asset
├── diagnoseAndFixAllAssets.js          # Detecta y corrige cashflows implícitos en todos los assets
├── fixAccountLevelAdjustedChange.js    # Recalcula adjustedDailyChangePercentage a nivel cuenta/overall
├── verifyAccountLevelConsistency.js    # Verifica consistencia entre niveles
├── verifyReturnsVsPnL.js               # Compara rendimientos calculados vs P&L
├── invalidatePerformanceCache.js       # Limpia cache de rendimientos
├── diagnoseNike.js                     # Diagnóstico específico de NKE
├── diagnoseVisa.js                     # Diagnóstico específico de V
└── tests/
    ├── testCashflowImplicitFix.js      # Test: compra fuera de horario
    ├── testNormalCase.js               # Test: compra en horario normal
    └── testNoPriceChange.js            # Test: solo variación de precio
```

## Uso

### Diagnóstico Rápido de un Asset

```bash
cd functions
node scripts/diagnostics/diagnoseAsset.js BTC-USD_crypto
node scripts/diagnostics/diagnoseAsset.js V_stock
node scripts/diagnostics/diagnoseAsset.js SPYG_etf
```

### Flujo Completo de Corrección

```bash
cd functions

# 1. Diagnosticar y corregir assets individuales
node scripts/diagnostics/diagnoseAndFixAllAssets.js

# 2. Corregir nivel cuenta/overall
node scripts/diagnostics/fixAccountLevelAdjustedChange.js

# 3. Invalidar cache
node scripts/diagnostics/invalidatePerformanceCache.js

# 4. Verificar consistencia
node scripts/diagnostics/verifyAccountLevelConsistency.js

# 5. Verificar contra P&L
node scripts/diagnostics/verifyReturnsVsPnL.js
```

### Ejecutar Tests

```bash
cd functions
node scripts/diagnostics/tests/testCashflowImplicitFix.js
node scripts/diagnostics/tests/testNormalCase.js
node scripts/diagnostics/tests/testNoPriceChange.js
```

## Descripción de Scripts

### `diagnoseAsset.js`

Muestra información detallada de un asset específico:
- Assets en Firestore (activos e inactivos)
- Precio actual y valorización
- Historial día por día con factor acumulativo
- Detección de problemas (cambio de units sin cashflow)
- Cálculo de rendimientos (1M, 3M, 6M, YTD)

### `diagnoseAndFixAllAssets.js`

Busca **todos** los assets del usuario y detecta inconsistencias donde:
- Las unidades cambiaron de un día al siguiente
- No hay cashflow registrado
- Las unidades anteriores eran > 0

Aplica correcciones automáticamente calculando el cashflow implícito.

### `fixAccountLevelAdjustedChange.js`

Después de corregir los assets individuales, este script recalcula el `adjustedDailyChangePercentage` a nivel de cuenta y overall usando la suma de cashflows de los assets (que ya están corregidos).

### `verifyAccountLevelConsistency.js`

Verifica que el `adjustedDailyChangePercentage` a nivel de cuenta sea consistente con la suma ponderada de los cambios de cada asset. Reporta discrepancias.

### `verifyReturnsVsPnL.js`

Compara los rendimientos históricos calculados con el P&L mostrado en la UI:
- Inversión Total
- Valor Actual
- P&L Realizada / No Realizada
- Rendimientos (1M, 3M, 6M, YTD)

### `invalidatePerformanceCache.js`

Elimina el cache de rendimientos históricos para forzar un recálculo en la siguiente consulta.

## Tests Unitarios

### `testCashflowImplicitFix.js`

**Escenario**: Compra fuera del horario del job (no hay transacción del día, pero las unidades aumentaron)

**Esperado**: 
- `adjustedDailyChangePercentage` ≈ 0%
- `totalCashFlow` = valor de la compra implícita (negativo)

### `testNormalCase.js`

**Escenario**: Compra durante el horario del job (hay transacción del día)

**Esperado**:
- `adjustedDailyChangePercentage` ≈ 0%
- `totalCashFlow` = valor de la compra (negativo)

### `testNoPriceChange.js`

**Escenario**: Sin transacciones, solo variación de precio

**Esperado**:
- `adjustedDailyChangePercentage` = variación del precio
- `totalCashFlow` = 0

## Notas Importantes

1. **USER_ID**: Todos los scripts usan un USER_ID hardcodeado. Modifícalo según sea necesario.

2. **key.json**: Los scripts requieren acceso a Firestore mediante `../../key.json`.

3. **Orden de ejecución**: Siempre ejecuta `diagnoseAndFixAllAssets.js` antes de `fixAccountLevelAdjustedChange.js`.

4. **Cache**: Después de cualquier corrección, ejecuta `invalidatePerformanceCache.js`.

## Referencia

Para más detalles sobre el problema y la solución, ver:
- `docs/architecture/OPT-017-implicit-cashflow-fix-analysis.md`
