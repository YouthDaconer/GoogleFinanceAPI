# Scripts de Migración - COST-OPT-003

Scripts para migrar datos históricos de rendimiento a la estructura de períodos consolidados.

## Estructura

```
scripts/migration/
├── migrateHistoricalPeriods.js   # Script principal de migración
├── validateMigration.js           # Validación V1 vs V2
├── diagnoseMigration.js           # Diagnóstico post-migración
├── __tests__/
│   └── migration.test.js          # Tests unitarios
└── README.md                      # Este archivo
```

## Pre-requisitos

1. **Firebase Admin SDK configurado**
   ```bash
   # Verificar que existe key.json en functions/
   ls -la ../../key.json
   ```

2. **Dependencias instaladas**
   ```bash
   cd src/GoogleFinanceAPI/functions
   npm install commander
   ```

3. **BACKUP de producción** (MUY RECOMENDADO)
   - Firebase Console > Firestore > Export

## Flujo de Ejecución Recomendado

### Paso 1: Dry-Run (Simulación)

```bash
# Ver qué se va a hacer sin escribir datos
node migrateHistoricalPeriods.js --dry-run

# Para un usuario específico
node migrateHistoricalPeriods.js --dry-run --user-id=DDeR8P5hYgfuN8gcU4RsQfdTJqx2

# Con logs detallados
node migrateHistoricalPeriods.js --dry-run --verbose
```

### Paso 2: Migrar Usuario de Prueba

```bash
# Migrar solo un usuario para validar
node migrateHistoricalPeriods.js --execute --user-id=TEST_USER_ID

# Verificar la migración
node validateMigration.js --user-id=TEST_USER_ID --verbose

# Diagnóstico detallado
node diagnoseMigration.js --user-id=TEST_USER_ID --detailed
```

### Paso 3: Validar Resultados

```bash
# Comparar V1 vs V2 con tolerancia estricta
node validateMigration.js --user-id=TEST_USER_ID --tolerance=0.01 --strict

# Si falla, revisar diagnóstico
node diagnoseMigration.js --user-id=TEST_USER_ID --detailed
```

### Paso 4: Migración Completa

```bash
# Migrar todos los usuarios
node migrateHistoricalPeriods.js --execute

# O con rango de años específico
node migrateHistoricalPeriods.js --execute --start-year=2022 --end-year=2026
```

### Paso 5: Validación Final

```bash
# Validar muestra de usuarios
node validateMigration.js --sample-size=20 --export-report

# Diagnóstico general
node diagnoseMigration.js --all --export
```

## Opciones de Comandos

### migrateHistoricalPeriods.js

| Opción | Descripción | Default |
| ------ | ----------- | ------- |
| `--dry-run` | Simula sin escribir datos | `true` |
| `--execute` | Ejecuta migración real | `false` |
| `--user-id <id>` | Solo un usuario | todos |
| `--start-year <year>` | Año de inicio | 2020 |
| `--end-year <year>` | Año de fin | actual |
| `--skip-monthly` | Omitir meses | `false` |
| `--skip-yearly` | Omitir años | `false` |
| `--verbose` | Logs detallados | `false` |
| `--throttle <ms>` | Delay entre usuarios | 100 |

### validateMigration.js

| Opción | Descripción | Default |
| ------ | ----------- | ------- |
| `--user-id <id>` | Validar un usuario | - |
| `--sample-size <n>` | Usuarios aleatorios | 5 |
| `--all` | Todos los usuarios | `false` |
| `--tolerance <pct>` | Tolerancia de diferencia | 0.01 |
| `--currency <code>` | Moneda a validar | USD |
| `--verbose` | Mostrar comparaciones | `false` |
| `--strict` | Fallar si diff > tolerancia | `false` |
| `--export-report` | Exportar JSON | `false` |

### diagnoseMigration.js

| Opción | Descripción | Default |
| ------ | ----------- | ------- |
| `--user-id <id>` | Diagnosticar un usuario | - |
| `--sample-size <n>` | Usuarios aleatorios | 10 |
| `--all` | Todos los usuarios | `false` |
| `--fix` | Intentar reparar | `false` |
| `--detailed` | Info detallada | `false` |
| `--export` | Exportar JSON | `false` |
| `--currency <code>` | Moneda a verificar | USD |

## Interpretación de Resultados

### Validación

```
✅ PASSED - Max diff: 0.0012% | V1: 450ms, V2: 120ms (3.8x)
```
- La diferencia está dentro de tolerancia
- V2 es 3.8x más rápido que V1

```
❌ FAILED - Max diff: 0.0523% (tolerancia: 0.01%)
```
- La diferencia excede la tolerancia
- Revisar con `diagnoseMigration.js --detailed`

### Diagnóstico

| Severidad | Acción |
| --------- | ------ |
| ❌ ERROR | Debe repararse - re-ejecutar migración |
| ⚠️ WARNING | Revisar manualmente |
| ℹ️ INFO | Informativo, no requiere acción |

## Troubleshooting

### Error: "Usuario X no encontrado"

```bash
# Verificar que el usuario existe
firebase firestore:get portfolioPerformance/USER_ID
```

### Error: "Sin datos consolidados"

```bash
# El usuario no fue migrado
node migrateHistoricalPeriods.js --execute --user-id=USER_ID
```

### Diferencia > tolerancia

```bash
# 1. Diagnóstico detallado
node diagnoseMigration.js --user-id=USER_ID --detailed

# 2. Si hay errores estructurales, re-migrar
node migrateHistoricalPeriods.js --execute --user-id=USER_ID

# 3. Si persiste, aumentar tolerancia (revisar manualmente)
node validateMigration.js --user-id=USER_ID --tolerance=0.1
```

### Rate Limits de Firestore

```bash
# Aumentar throttle entre usuarios
node migrateHistoricalPeriods.js --execute --throttle=500
```

## Métricas de Éxito

| Métrica | Objetivo |
| ------- | -------- |
| Tasa de éxito validación | > 99% |
| Diferencia máxima TWR | < 0.01% |
| Speedup V2/V1 | > 10x |
| Reducción de docs leídos | > 90% |

## Rollback

Los documentos originales en `dates/` NO se modifican. Para hacer rollback:

```bash
# Eliminar documentos consolidados de un usuario
firebase firestore:delete portfolioPerformance/USER_ID/consolidatedPeriods --recursive --force
```

## Reportes Generados

Los scripts generan reportes JSON en el directorio actual:

- `migration-report-{timestamp}.json` - Reporte de migración
- `validation-report-{timestamp}.json` - Reporte de validación
- `diagnostic-report-{timestamp}.json` - Reporte de diagnóstico

## Changelog

| Versión | Fecha | Cambios |
| ------- | ----- | ------- |
| 1.0.0 | 2026-01-04 | Versión inicial COST-OPT-003 |
