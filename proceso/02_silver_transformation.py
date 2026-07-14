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
# ══════════════════════════════════════
print("\n[MOVIES] Procesando capa Silver...")

df_movies = spark.read.table(TBL_BRONZE_MOVIES)
log_counts(df_movies, "bronze_read", "movies")

df_movies_silver = (
    df_movies
    # Estandarizar nombres de columnas a snake_case
    .toDF(*[c.lower().replace(" ", "_").replace("-", "_") for c in df_movies.columns])
    # Castear tipos
    .transform(lambda df: cast_column_safe(df, "rating", DoubleType()))
    .transform(lambda df: cast_column_safe(df, "votes", IntegerType()))
    .transform(lambda df: cast_column_safe(df, "year", IntegerType()))
    # Limpiar nulos en campos clave
    .transform(lambda df: fill_nulls_with_unknown(df, ["genre", "director", "language", "country"]))
    # Filtrar registros sin título o rating
    .filter(F.col("title").isNotNull())
    .filter(F.col("rating").isNotNull() & (F.col("rating") > 0))
    # Estandarizar género: tomar solo el primer género listado
    .withColumn("primary_genre", F.trim(F.split(F.col("genre"), ",")[0]))
    # Clasificar por rating
    .withColumn(
        "rating_category",
        F.when(F.col("rating") >= 8.0, "Excelente")
         .when(F.col("rating") >= 6.5, "Buena")
         .when(F.col("rating") >= 5.0, "Regular")
         .otherwise("Baja")
    )
    # Eliminar columnas de auditoría bronze para re-agregar limpias
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
    # Castear columnas numéricas
    .transform(lambda df: cast_column_safe(df, "price",            DoubleType()))
    .transform(lambda df: cast_column_safe(df, "horsepower",       DoubleType()))
    .transform(lambda df: cast_column_safe(df, "city_mpg",         IntegerType()))
    .transform(lambda df: cast_column_safe(df, "highway_mpg",      IntegerType()))
    .transform(lambda df: cast_column_safe(df, "engine_size",      IntegerType()))
    .transform(lambda df: cast_column_safe(df, "curb_weight",      IntegerType()))
    # Limpiar nulos en campos clave
    .transform(lambda df: fill_nulls_with_unknown(df, ["make", "fuel_type", "body_style", "drive_wheels"]))
    # Filtrar registros sin precio válido
    .filter(F.col("price").isNotNull() & (F.col("price") > 0))
    # Estandarizar make a mayúsculas
    .withColumn("make", F.upper(F.trim(F.col("make"))))
    # Promedio MPG
    .withColumn(
        "avg_mpg",
        F.round((F.col("city_mpg") + F.col("highway_mpg")) / 2, 1)
    )
    # Clasificar por precio
    .withColumn(
        "price_segment",
        F.when(F.col("price") < 10000,  "Económico")
         .when(F.col("price") < 20000,  "Medio")
         .when(F.col("price") < 35000,  "Premium")
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
