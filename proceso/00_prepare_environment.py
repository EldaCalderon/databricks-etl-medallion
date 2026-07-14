# Databricks notebook source
# ═══════════════════════════════════════════════════════
# PREPARACIÓN DE AMBIENTE — Creación de schemas
# Ejecutar una sola vez antes del primer run del pipeline
# ═══════════════════════════════════════════════════════

print("=" * 60)
print("PREPARACIÓN DE AMBIENTE")
print("=" * 60)

# ── Crear schemas en Hive Metastore ─────────────────────
schemas = ["bronze", "silver", "gold"]

for schema in schemas:
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {schema}
        COMMENT 'Capa {schema.capitalize()} del pipeline ETL Medallion'
    """)
    print(f"[OK] Schema '{schema}' verificado/creado")

# ── Verificar ────────────────────────────────────────────
print("\n[INFO] Schemas disponibles:")
spark.sql("SHOW DATABASES").show()

print("\n[OK] Ambiente listo para ejecutar el pipeline ETL.")
