# Databricks notebook source
# ═══════════════════════════════════════════════════════
# CAPA GOLD — Agregaciones y métricas de negocio
# Lee desde Silver y genera tablas listas para consumo
# en dashboards y reportes ejecutivos
# ═══════════════════════════════════════════════════════

import sys

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_notebook_path = _ctx.notebookPath().get()
_project_root = "/Workspace" + "/".join(_notebook_path.split("/")[:-2])
sys.path.insert(0, _project_root)

from pyspark.sql import functions as F

from config.settings import (
    SCHEMA_GOLD,
    TBL_SILVER_MOVIES, TBL_SILVER_AUTOS,
    TBL_GOLD_GENRE, TBL_GOLD_AUTOS_BRAND, TBL_GOLD_COMBINED
)
from src.utils.helpers import (
    write_delta,
    log_counts,
    create_schema_if_not_exists
)

print("=" * 60)
print("INICIANDO CAPA GOLD")
print("=" * 60)

create_schema_if_not_exists(spark, None, SCHEMA_GOLD)

df_movies = spark.read.table(TBL_SILVER_MOVIES)
df_autos  = spark.read.table(TBL_SILVER_AUTOS)

# ══════════════════════════════════════
# GOLD 1 — Analítica de Directores (Movies)
# Columnas: director, budget_usd, revenue_usd, profit_usd
# ══════════════════════════════════════
print("\n[GOLD] Generando analítica de directores...")

df_director = (
    df_movies
    .groupBy("director")
    .agg(
        F.count("*")                                    .alias("total_peliculas"),
        F.round(F.avg("budget_usd"),        2)          .alias("presupuesto_promedio_usd"),
        F.round(F.avg("revenue_usd"),       2)          .alias("recaudacion_promedio_usd"),
        F.round(F.avg("profit_usd"),        2)          .alias("ganancia_promedio_usd"),
        F.round(F.avg("profit_margin_pct"), 1)          .alias("margen_promedio_pct"),
        F.round(F.avg("roi"),               4)          .alias("roi_promedio"),
        F.max("profit_usd")                             .alias("mejor_pelicula_profit_usd"),
    )
    .withColumn(
        "categoria_rendimiento",
        F.when(F.col("ganancia_promedio_usd") > 50_000_000, "Exitoso")
         .when(F.col("ganancia_promedio_usd") > 0,          "Rentable")
         .otherwise("Deficitario")
    )
    .orderBy(F.desc("ganancia_promedio_usd"))
    .withColumn("_ingestion_date", F.current_timestamp())
)

write_delta(df_director, TBL_GOLD_GENRE)
log_counts(df_director, "gold", TBL_GOLD_GENRE)

# ══════════════════════════════════════
# GOLD 2 — Analítica por Marca (Autos)
# ══════════════════════════════════════
print("\n[GOLD] Generando analítica por marca de autos...")

df_brand = (
    df_autos
    .groupBy("make", "fuel_type")
    .agg(
        F.count("*")                          .alias("total_modelos"),
        F.round(F.avg("price"),         2)    .alias("precio_promedio"),
        F.min("price")                        .alias("precio_minimo"),
        F.max("price")                        .alias("precio_maximo"),
        F.round(F.avg("horsepower"),    1)    .alias("hp_promedio"),
        F.round(F.avg("avg_mpg"),       1)    .alias("eficiencia_promedio_mpg"),
        F.round(F.avg("engine_size"),   1)    .alias("motor_promedio_cc"),
    )
    .orderBy(F.desc("precio_promedio"))
    .withColumn("_ingestion_date", F.current_timestamp())
)

write_delta(df_brand, TBL_GOLD_AUTOS_BRAND)
log_counts(df_brand, "gold", TBL_GOLD_AUTOS_BRAND)

# ══════════════════════════════════════
# GOLD 3 — Resumen de Mercado Combinado
# Movies por performance_category + Autos por price_segment
# ══════════════════════════════════════
print("\n[GOLD] Generando resumen de mercado combinado...")

df_autos_summary = (
    df_autos
    .groupBy("price_segment")
    .agg(
        F.count("*")                    .alias("total_items"),
        F.round(F.avg("price"),   2)    .alias("precio_promedio"),
        F.round(F.avg("avg_mpg"), 1)    .alias("eficiencia_promedio"),
    )
    .withColumn("categoria",    F.lit("Automóvil"))
    .withColumnRenamed("price_segment", "segmento")
    .select("categoria", "segmento", "total_items", "precio_promedio", "eficiencia_promedio")
)

df_movies_summary = (
    df_movies
    .groupBy("performance_category")
    .agg(
        F.count("*")                               .alias("total_items"),
        F.round(F.avg("profit_usd"),         2)    .alias("ganancia_promedio_usd"),
        F.round(F.avg("profit_margin_pct"),  1)    .alias("margen_promedio_pct"),
    )
    .withColumn("categoria", F.lit("Película"))
    .withColumnRenamed("performance_category", "segmento")
    .select("categoria", "segmento", "total_items", "ganancia_promedio_usd", "margen_promedio_pct")
)

df_combined = (
    df_autos_summary
    .unionByName(df_movies_summary, allowMissingColumns=True)
    .withColumn("_ingestion_date", F.current_timestamp())
)

write_delta(df_combined, TBL_GOLD_COMBINED)
log_counts(df_combined, "gold", TBL_GOLD_COMBINED)

print("\n[OK] Capa Gold completada exitosamente.")
print("\nTablas disponibles para visualización:")
print(f"  - {TBL_GOLD_GENRE}")
print(f"  - {TBL_GOLD_AUTOS_BRAND}")
print(f"  - {TBL_GOLD_COMBINED}")
