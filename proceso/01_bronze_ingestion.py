# ═══════════════════════════════════════════════════════
# CAPA BRONZE — Ingesta desde ADLS Gen2
# Lee los CSVs crudos y los persiste como tablas Delta
# Autenticación exclusivamente por Managed Identity
# ═══════════════════════════════════════════════════════

import sys
sys.path.insert(0, "/Workspace/Shared/proyecto")

from config.settings import (
    STORAGE_ACCOUNT, RAW_MOVIES_PATH, RAW_AUTOS_PATH,
    CATALOG, SCHEMA_BRONZE,
    TBL_BRONZE_MOVIES, TBL_BRONZE_AUTOS
)
from src.utils.helpers import (
    configure_managed_identity,
    add_ingestion_metadata,
    drop_full_duplicates,
    write_delta,
    log_counts,
    create_schema_if_not_exists
)

print("=" * 60)
print("INICIANDO CAPA BRONZE")
print("=" * 60)

# ── 1. Managed Identity ──────────────────────────────────
configure_managed_identity(spark, STORAGE_ACCOUNT)

# ── 2. Crear schemas si no existen ──────────────────────
create_schema_if_not_exists(spark, CATALOG, SCHEMA_BRONZE)

# ── 3. Ingestar Movies ──────────────────────────────────
print("\n[MOVIES] Leyendo desde ADLS Gen2...")

df_movies_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("escape", '"')
    .csv(RAW_MOVIES_PATH)
)

log_counts(df_movies_raw, "raw", "movies")

df_movies_bronze = add_ingestion_metadata(df_movies_raw, "kaggle_movies")
df_movies_bronze = drop_full_duplicates(df_movies_bronze)

write_delta(df_movies_bronze, TBL_BRONZE_MOVIES)
log_counts(df_movies_bronze, "bronze", TBL_BRONZE_MOVIES)

# ── 4. Ingestar Automobiles ─────────────────────────────
print("\n[AUTOMOBILES] Leyendo desde ADLS Gen2...")

df_autos_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("nullValue", "?")        # El dataset usa "?" para nulos
    .csv(RAW_AUTOS_PATH)
)

log_counts(df_autos_raw, "raw", "automobiles")

df_autos_bronze = add_ingestion_metadata(df_autos_raw, "kaggle_automobiles")
df_autos_bronze = drop_full_duplicates(df_autos_bronze)

write_delta(df_autos_bronze, TBL_BRONZE_AUTOS)
log_counts(df_autos_bronze, "bronze", TBL_BRONZE_AUTOS)

print("\n[OK] Capa Bronze completada exitosamente.")
