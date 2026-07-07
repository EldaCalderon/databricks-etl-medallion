# ETL Medallion con Databricks

Pipeline de datos end-to-end usando **arquitectura Medallion** (Bronze → Silver → Gold) sobre Azure Databricks con autenticación por Managed Identity y CI/CD en GitHub Actions.

## Datasets
| Dataset | Fuente | Descripción |
|---------|--------|-------------|
| Movies | [Kaggle](https://www.kaggle.com/datasets/hassanelfattmi/which-movie-should-i-watch-today) | Películas con ratings, géneros y directores |
| Automobiles | [Kaggle](https://www.kaggle.com/datasets/sumaya23abdul/automobile-database) | Base de datos de autos con precios y especificaciones |

## Arquitectura
```
ADLS Gen2 (raw/)
    │
    ▼ Managed Identity
[BRONZE] Ingesta cruda + metadata de auditoría
    │
    ▼ PySpark
[SILVER] Limpieza, tipado, estandarización
    │
    ▼ PySpark
[GOLD]   Agregaciones y métricas de negocio
    │
    ▼
Dashboard (Power BI / Databricks SQL)
```

## Estructura del repositorio
```
├── .github/workflows/cicd.yml     ← Pipeline CI/CD
├── proceso/
│   ├── 01_bronze_ingestion.py     ← Ingesta desde ADLS Gen2
│   ├── 02_silver_transformation.py← Limpieza y transformación
│   └── 03_gold_analytics.py       ← Métricas de negocio
├── src/utils/helpers.py           ← Funciones reutilizables
├── config/settings.py             ← Configuración central
├── databricks.yml                 ← Definición del bundle y jobs
└── README.md
```

## Setup paso a paso

### 1. Azure — Crear recursos
1. Crear un **Resource Group** en Azure Portal
2. Crear **Azure Data Lake Storage Gen2** (ADLS Gen2)
   - Habilitar "Hierarchical namespace"
   - Crear contenedor llamado `raw`
3. Crear **Azure Databricks Workspace**
4. Asignar rol **Storage Blob Data Contributor** al Managed Identity del workspace sobre el storage

### 2. Subir datasets a ADLS Gen2
Descargar los CSVs de Kaggle y subirlos a:
- `raw/movies/` → archivo CSV de películas
- `raw/automobiles/` → archivo CSV de autos

### 3. Databricks — Configurar
1. Crear un cluster (este será el único cluster activo)
2. Anotar el **Cluster ID** (está en la URL del cluster)
3. Anotar el **Workspace URL** (ej: `https://adb-xxxxx.azuredatabricks.net`)
4. Generar un **Personal Access Token** en User Settings → Developer

### 4. GitHub — Crear repositorio y secrets
1. Crear repositorio público en GitHub
2. Subir todos los archivos del proyecto
3. Ir a Settings → Secrets and variables → Actions y agregar:

| Secret | Valor |
|--------|-------|
| `DATABRICKS_HOST` | URL de tu workspace (ej: `https://adb-xxxxx.azuredatabricks.net`) |
| `DATABRICKS_TOKEN` | Personal Access Token generado en Databricks |
| `DATABRICKS_CLUSTER_ID` | ID del cluster de producción |
| `DATABRICKS_USER` | Tu email de usuario en Databricks |

### 5. Ajustar config/settings.py
Cambiar `STORAGE_ACCOUNT` por el nombre real de tu cuenta de storage.

### 6. Hacer push a main
El pipeline de CI/CD se activa automáticamente al hacer push a `main`:
- Valida el bundle
- Despliega los notebooks en producción
- Ejecuta el workflow en el cluster de producción

## Tablas Gold generadas
| Tabla | Descripción |
|-------|-------------|
| `main.gold.genre_analytics` | Métricas por género de película |
| `main.gold.auto_brand_analytics` | Precios y eficiencia por marca de auto |
| `main.gold.market_summary` | Resumen ejecutivo multi-fuente |
