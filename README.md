# Pipeline ETL Medallion — Azure Databricks

Pipeline de datos end-to-end usando **arquitectura Medallion** (Bronze → Silver → Gold)
sobre Azure Databricks con PySpark, ADLS Gen2 y CI/CD automatizado con GitHub Actions.

---

## Recursos Azure aprovisionados

| Recurso | Nombre | Descripción |
|---------|--------|-------------|
| Azure Databricks Workspace | `databricks-etl-medallion` | Workspace principal del pipeline |
| Storage Account (ADLS Gen2) | `sadatabricksetl26` | Almacenamiento de datos por capa |
| Contenedor raw | `raw` | CSVs originales (movies, automobiles) |
| Cluster | `0707-153444-7bzqblwy` | Cluster de ejecución del pipeline |

---

## Datasets

| Dataset | Fuente | Columnas clave |
|---------|--------|----------------|
| **Movies** | [Kaggle](https://www.kaggle.com/datasets/hassanelfattmi/which-movie-should-i-watch-today) | id, director, top_billed, budget_usd, revenue_usd |
| **Automobiles** | [Kaggle](https://www.kaggle.com/datasets/sumaya23abdul/automobile-database) | make, fuel_type, price, horsepower, city_mpg, highway_mpg |

Los archivos CSV se almacenan en ADLS Gen2:
- `abfss://raw@sadatabricksetl26.dfs.core.windows.net/movies/`
- `abfss://raw@sadatabricksetl26.dfs.core.windows.net/automobiles/`

---

## Arquitectura Medallion

```
┌─────────────────────────────────────────────────────────────┐
│                    ADLS Gen2 — raw/                         │
│         movies/*.csv          automobiles/*.csv             │
└──────────────────────┬──────────────────────────────────────┘
                       │  (autenticación: storage account key)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  BRONZE — bronze.movies / bronze.automobiles                │
│  • Ingesta directa del CSV sin modificaciones               │
│  • Agrega metadatos: _source, _ingestion_date, _year        │
│  • Elimina duplicados exactos                               │
│  • Formato: Delta Lake                                      │
└──────────────────────┬──────────────────────────────────────┘
                       │  PySpark transformations
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER — silver.movies / silver.automobiles                │
│  • Casting de tipos (DoubleType, IntegerType)               │
│  • Relleno de nulos con "Unknown"                           │
│  • Columnas derivadas:                                      │
│    Movies: profit_usd, profit_margin_pct, performance_category│
│    Autos:  avg_mpg, price_segment                           │
│  • Estandarización de nombres (snake_case, UPPER)           │
└──────────────────────┬──────────────────────────────────────┘
                       │  PySpark aggregations
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD — 3 tablas de consumo                                 │
│  • gold.genre_analytics     → Analítica de directores       │
│  • gold.auto_brand_analytics → Marcas y eficiencia          │
│  • gold.market_summary      → Resumen ejecutivo combinado   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
              Dashboard / Power BI
```

---

## Estructura del repositorio

```
databricks-etl-medallion/
│
├── .github/
│   └── workflows/
│       └── cicd.yml                  ← Pipeline CI/CD (validate → deploy → run)
│
├── proceso/                          ← Notebooks ETL + preparación
│   ├── 00_prepare_environment.py     ← Crea schemas en Hive Metastore
│   ├── 01_bronze_ingestion.py        ← Ingesta CSV desde ADLS Gen2
│   ├── 02_silver_transformation.py   ← Limpieza y transformación
│   ├── 03_gold_analytics.py          ← Agregaciones y métricas
│   └── 04_security_grants.py         ← Aplicación de grants
│
├── PrepAmb/
│   └── 01_prepare_environment.sql    ← CREATE DATABASE para Bronze/Silver/Gold
│
├── seguridad/
│   └── grants.sql                    ← GRANT para usuarios y grupos
│
├── reversion/
│   └── cleanup.sql                   ← DROP TABLE / DROP DATABASE (rollback)
│
├── dashboard/
│   ├── queries_gold.sql              ← Queries SQL para visualizaciones
│   └── dashboard_link.txt            ← Enlace al dashboard publicado
│
├── datasets/
│   └── datasets_info.txt             ← Descripción y rutas de los datasets
│
├── certificaciones/                  ← Capturas de certificaciones
├── evidencias/                       ← Capturas de ejecuciones exitosas
│
├── config/
│   └── settings.py                   ← Configuración central (paths, tabla names)
│
├── src/
│   └── utils/
│       └── helpers.py                ← Funciones PySpark reutilizables
│
└── databricks.yml                    ← Databricks Asset Bundle (dev/prod targets)
```

---

## Tablas Gold generadas

| Tabla | Descripción | Columnas clave |
|-------|-------------|----------------|
| `gold.genre_analytics` | Métricas de directores por rentabilidad | director, ganancia_promedio_usd, roi_promedio, categoria_rendimiento |
| `gold.auto_brand_analytics` | Eficiencia y precio por marca y combustible | make, fuel_type, precio_promedio, eficiencia_promedio_mpg |
| `gold.market_summary` | Resumen ejecutivo multi-fuente | categoria, segmento, total_items |

---

## CI/CD — GitHub Actions

El pipeline se activa automáticamente en cada push a `main`:

```
push a main
    │
    ▼
[1] databricks bundle validate   ← Valida la estructura del bundle
    │
    ▼
[2] databricks bundle deploy     ← Despliega notebooks al workspace
    │
    ▼
[3] databricks bundle run etl_pipeline  ← Ejecuta Bronze → Silver → Gold
```

### Secrets requeridos en GitHub

| Secret | Descripción |
|--------|-------------|
| `DATABRICKS_HOST` | URL del workspace (`https://adb-7405610893212127.7.azuredatabricks.net`) |
| `DATABRICKS_TOKEN` | Personal Access Token de Databricks |
| `DATABRICKS_CLUSTER_ID` | ID del cluster de producción |
| `DATABRICKS_USER` | Email del usuario Databricks |

---

## Cómo ejecutar

### Opción 1: CI/CD automático
Hacer push a `main` — GitHub Actions ejecuta el pipeline completo.

### Opción 2: Manual en Databricks
1. Ir a Databricks → Workflows → `etl_pipeline`
2. Clic en **Run now**
3. El orden de ejecución es: `00_prepare_environment` → `01_bronze` → `02_silver` → `03_gold`

### Opción 3: Databricks CLI
```bash
databricks bundle run etl_pipeline \
  --var="cluster_id=<CLUSTER_ID>" \
  --var="databricks_user=<EMAIL>"
```

---

## Seguridad

Los permisos sobre las tablas se gestionan por capas:

| Rol | Acceso |
|-----|--------|
| `etl_admin` | Lectura y escritura en Bronze + Silver + Gold |
| `ingenieros_datos` | Lectura en Silver + Gold |
| `analistas` | Lectura solo en Gold |

Ver scripts completos en [seguridad/grants.sql](seguridad/grants.sql).

---

## Reversión

Para eliminar completamente el pipeline (tablas y schemas):

```sql
-- Ejecutar en Databricks SQL Editor
-- Ver reversion/cleanup.sql
DROP TABLE IF EXISTS gold.genre_analytics;
DROP TABLE IF EXISTS gold.auto_brand_analytics;
DROP TABLE IF EXISTS gold.market_summary;
DROP TABLE IF EXISTS silver.movies;
DROP TABLE IF EXISTS silver.automobiles;
DROP TABLE IF EXISTS bronze.movies;
DROP TABLE IF EXISTS bronze.automobiles;
DROP DATABASE IF EXISTS gold   CASCADE;
DROP DATABASE IF EXISTS silver CASCADE;
DROP DATABASE IF EXISTS bronze CASCADE;
```
