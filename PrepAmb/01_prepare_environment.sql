-- ═══════════════════════════════════════════════════════════
-- PREPARACIÓN DE AMBIENTE — Pipeline ETL Medallion
-- Crea los schemas (bases de datos) en Hive Metastore
-- Almacenamiento físico: ADLS Gen2 - sadatabricksetl26
-- ═══════════════════════════════════════════════════════════

-- ──────────────────────────────────────────
-- 1. CAPA BRONZE — Datos crudos desde ADLS
-- ──────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS bronze
COMMENT 'Capa Bronze: datos crudos ingestados desde ADLS Gen2 sin transformación';

-- ──────────────────────────────────────────
-- 2. CAPA SILVER — Datos limpios y validados
-- ──────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS silver
COMMENT 'Capa Silver: datos limpios, tipados y estandarizados';

-- ──────────────────────────────────────────
-- 3. CAPA GOLD — Agregaciones para consumo
-- ──────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS gold
COMMENT 'Capa Gold: tablas agregadas y métricas listas para dashboards';

-- ══════════════════════════════════════════
-- Verificar schemas creados
-- ══════════════════════════════════════════
SHOW DATABASES;

-- ──────────────────────────────────────────
-- 4. Verificar estructura de tablas (post ETL)
-- ──────────────────────────────────────────
-- SHOW TABLES IN bronze;
-- SHOW TABLES IN silver;
-- SHOW TABLES IN gold;
