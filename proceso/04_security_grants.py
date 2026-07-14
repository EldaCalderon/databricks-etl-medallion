# Databricks notebook source
# ═══════════════════════════════════════════════════════
# SEGURIDAD — Aplicación de Grants sobre tablas ETL
# Ejecutar después del pipeline para asignar permisos
# ═══════════════════════════════════════════════════════

print("=" * 60)
print("APLICANDO GRANTS DE SEGURIDAD")
print("=" * 60)

# ── Grants a nivel de base de datos ─────────────────────
grants_db = [
    ("USAGE",          "DATABASE bronze", "`etl_admin`"),
    ("USAGE",          "DATABASE silver", "`etl_admin`"),
    ("USAGE",          "DATABASE gold",   "`etl_admin`"),
    ("USAGE",          "DATABASE gold",   "`analistas`"),
    ("USAGE",          "DATABASE silver", "`ingenieros_datos`"),
    ("USAGE",          "DATABASE gold",   "`ingenieros_datos`"),
]

for privilege, obj, principal in grants_db:
    try:
        spark.sql(f"GRANT {privilege} ON {obj} TO {principal}")
        print(f"[OK] GRANT {privilege} ON {obj} TO {principal}")
    except Exception as e:
        print(f"[WARN] {privilege} ON {obj} TO {principal}: {e}")

# ── Grants a nivel de tabla (Gold — consumo final) ───────
tables_gold = [
    "gold.genre_analytics",
    "gold.auto_brand_analytics",
    "gold.market_summary",
]

principals_gold = ["`analistas`", "`ingenieros_datos`", "`ecalderon@maxwarehouse.com`"]

for table in tables_gold:
    for principal in principals_gold:
        try:
            spark.sql(f"GRANT SELECT ON TABLE {table} TO {principal}")
            print(f"[OK] GRANT SELECT ON {table} TO {principal}")
        except Exception as e:
            print(f"[WARN] SELECT ON {table} TO {principal}: {e}")

# ── Grants Silver — ingenieros_datos ────────────────────
tables_silver = ["silver.movies", "silver.automobiles"]
for table in tables_silver:
    try:
        spark.sql(f"GRANT SELECT ON TABLE {table} TO `ingenieros_datos`")
        print(f"[OK] GRANT SELECT ON {table} TO ingenieros_datos")
    except Exception as e:
        print(f"[WARN] SELECT ON {table}: {e}")

print("\n[OK] Grants de seguridad aplicados.")
