# ─────────────────────────────────────────────
# Configuración global del proyecto
# Ajusta estos valores según tu ambiente Azure
# ─────────────────────────────────────────────

# Azure Storage
STORAGE_ACCOUNT  = "sadatabricksetl26"    # Nombre de tu ADLS Gen2
CONTAINER_RAW    = "raw"                  # Contenedor con los CSVs originales

# Rutas en ADLS Gen2
RAW_MOVIES_PATH  = f"abfss://{CONTAINER_RAW}@{STORAGE_ACCOUNT}.dfs.core.windows.net/movies/"
RAW_AUTOS_PATH   = f"abfss://{CONTAINER_RAW}@{STORAGE_ACCOUNT}.dfs.core.windows.net/automobiles/"

# Unity Catalog
CATALOG          = "main"
SCHEMA_BRONZE    = "bronze"
SCHEMA_SILVER    = "silver"
SCHEMA_GOLD      = "gold"

# Tablas
TBL_BRONZE_MOVIES    = f"{CATALOG}.{SCHEMA_BRONZE}.movies"
TBL_BRONZE_AUTOS     = f"{CATALOG}.{SCHEMA_BRONZE}.automobiles"
TBL_SILVER_MOVIES    = f"{CATALOG}.{SCHEMA_SILVER}.movies"
TBL_SILVER_AUTOS     = f"{CATALOG}.{SCHEMA_SILVER}.automobiles"
TBL_GOLD_GENRE       = f"{CATALOG}.{SCHEMA_GOLD}.genre_analytics"
TBL_GOLD_AUTOS_BRAND = f"{CATALOG}.{SCHEMA_GOLD}.auto_brand_analytics"
TBL_GOLD_COMBINED    = f"{CATALOG}.{SCHEMA_GOLD}.market_summary"
