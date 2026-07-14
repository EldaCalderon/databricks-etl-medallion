# Databricks notebook source
# ═══════════════════════════════════════════════════════
# CAPA SILVER — Limpieza, validación y estandarización
# Lee desde Bronze y aplica transformaciones de calidad
# ═══════════════════════════════════════════════════════

import sys

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_notebook_path = _ctx.notebookPath().get()
_project_root = "/Workspace" + "/".join(_notebook_path.split("/")[:-2])
sys.path.insert(0, _project_root)

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from config.settings import (
    SCHEMA_SILVER,
    TBL_BRONZE_MOVIES, TBL_BRONZE_AUTOS,
    TBL_SILVER_MOVIES, TBL_SILVER_AUTOS
)
from src.utils.helpers import (
    cast_column_safe,
    fill_nulls_with_unknown,
    drop_full_duplicates,
    write_delta,
    log_counts,
    create_schema_if_not_exists
)

print("=" * 60)
print("INICIANDO CAPA SILVER")
print("=" * 60)

create_schema_if_not_exists(spark, None, SCHEMA_SILVER)

# ══════════════════════════════════════
# MOVIES — Transformaciones Silver
# Columnas: id, director, top_billed, budget_usd, revenue_usd
# ══════════════════════════════════════
print("\n[MOVIES] Procesando capa Silver...")

df_movies = spark.read.table(TBL_BRONZE_MOVIES)
log_counts(df_movies, "bronze_read", "movies")

df_movies_silver = (
    df_movies
    .toDF(*[c.lower().replace(" ", "_").replace("-", "_") for c in df_movies.columns])
    # Castear columnas numéricas
    .transform(lambda df: cast_column_safe(df, "budget_usd",  DoubleType()))
    .transform(lambda df: cast_column_safe(df, "revenue_usd", DoubleType()))
    # Limpiar nulos en texto
    .transform(lambda df: fill_nulls_with_unknown(df, ["director", "top_billed"]))
    # Filtrar registros sin datos financieros válidos
    .filter(F.col("budget_usd").isNotNull()  & (F.col("budget_usd")  > 0))
    .filter(F.col("revenue_usd").isNotNull() & (F.col("revenue_usd") > 0))
    # Estandarizar director
    .withColumn("director", F.initcap(F.trim(F.col("director"))))
    # Columnas derivadas financieras
    .withColumn("profit_usd",
        F.col("revenue_usd") - F.col("budget_usd"))
    .withColumn("profit_margin_pct",
        F.round((F.col("profit_usd") / F.col("budget_usd")) * 100, 2))
    .withColumn("roi",
        F.round(F.col("revenue_usd") / F.col("budget_usd"), 4))
    # Clasificar por rentabilidad
    .withColumn(
        "performance_category",
        F.when(F.col("profit_usd") > 100_000_000, "Blockbuster")
         .when(F.col("profit_usd") > 0,           "Rentable")
         .otherwise("Pérdida")
    )
    .drop("_source", "_ingestion_date", "_year", "_month")
    .withColumn("_source",         F.lit("silver_movies"))
    .withColumn("_ingestion_date", F.current_timestamp())
)

df_movies_silver = drop_full_duplicates(df_movies_silver)
write_delta(df_movies_silver, TBL_SILVER_MOVIES)
log_counts(df_movies_silver, "silver", TBL_SILVER_MOVIES)

# ══════════════════════════════════════
# AUTOMOBILES — Transformaciones Silver
# ══════════════════════════════════════
print("\n[AUTOMOBILES] Procesando capa Silver...")

df_autos = spark.read.table(TBL_BRONZE_AUTOS)
log_counts(df_autos, "bronze_read", "automobiles")

df_autos_silver = (
    df_autos
    .toDF(*[c.lower().replace(" ", "_").replace("-", "_") for c in df_autos.columns])
    .transform(lambda df: cast_column_safe(df, "price",       DoubleType()))
    .transform(lambda df: cast_column_safe(df, "horsepower",  DoubleType()))
    .transform(lambda df: cast_column_safe(df, "city_mpg",    IntegerType()))
    .transform(lambda df: cast_column_safe(df, "highway_mpg", IntegerType()))
    .transform(lambda df: cast_column_safe(df, "engine_size", IntegerType()))
    .transform(lambda df: cast_column_safe(df, "curb_weight", IntegerType()))
    .transform(lambda df: fill_nulls_with_unknown(df, ["make", "fuel_type", "body_style", "drive_wheels"]))
    .filter(F.col("price").isNotNull() & (F.col("price") > 0))
    .withColumn("make", F.upper(F.trim(F.col("make"))))
    .withColumn("avg_mpg",
        F.round((F.col("city_mpg") + F.col("highway_mpg")) / 2, 1))
    .withColumn(
        "price_segment",
        F.when(F.col("price") < 10000, "Económico")
         .when(F.col("price") < 20000, "Medio")
         .when(F.col("price") < 35000, "Premium")
         .otherwise("Lujo")
    )
    .drop("_source", "_ingestion_date", "_year", "_month")
    .withColumn("_source",         F.lit("silver_automobiles"))
    .withColumn("_ingestion_date", F.current_timestamp())
)

df_autos_silver = drop_full_duplicates(df_autos_silver)
write_delta(df_autos_silver, TBL_SILVER_AUTOS)
log_counts(df_autos_silver, "silver", TBL_SILVER_AUTOS)

print("\n[OK] Capa Silver completada exitosamente.")
