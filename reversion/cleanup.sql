-- ═══════════════════════════════════════════════════════════
-- REVERSION — Limpieza completa del pipeline ETL Medallion
-- Elimina tablas lógicas (Hive Metastore) y datos físicos (Delta)
-- ADVERTENCIA: Ejecutar solo si se desea borrar todo el pipeline
-- ═══════════════════════════════════════════════════════════

-- ──────────────────────────────────────────
-- CAPA GOLD — Eliminar tablas
-- ──────────────────────────────────────────
DROP TABLE IF EXISTS gold.genre_analytics;
DROP TABLE IF EXISTS gold.auto_brand_analytics;
DROP TABLE IF EXISTS gold.market_summary;

-- ──────────────────────────────────────────
-- CAPA SILVER — Eliminar tablas
-- ──────────────────────────────────────────
DROP TABLE IF EXISTS silver.movies;
DROP TABLE IF EXISTS silver.automobiles;

-- ──────────────────────────────────────────
-- CAPA BRONZE — Eliminar tablas
-- ──────────────────────────────────────────
DROP TABLE IF EXISTS bronze.movies;
DROP TABLE IF EXISTS bronze.automobiles;

-- ──────────────────────────────────────────
-- ELIMINAR SCHEMAS (bases de datos)
-- ──────────────────────────────────────────
DROP DATABASE IF EXISTS gold    CASCADE;
DROP DATABASE IF EXISTS silver  CASCADE;
DROP DATABASE IF EXISTS bronze  CASCADE;

-- ══════════════════════════════════════════
-- Verificar que todo fue eliminado
-- ══════════════════════════════════════════
SHOW DATABASES;
