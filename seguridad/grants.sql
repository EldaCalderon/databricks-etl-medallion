-- ═══════════════════════════════════════════════════════════
-- SEGURIDAD — Grants sobre tablas del pipeline ETL Medallion
-- Otorga permisos a usuarios y grupos según capa de acceso
-- ═══════════════════════════════════════════════════════════

-- ──────────────────────────────────────────
-- GRUPO: analistas (acceso solo a Gold)
-- Solo lectura sobre tablas de consumo final
-- ──────────────────────────────────────────
GRANT USAGE  ON DATABASE gold                 TO `analistas`;
GRANT SELECT ON TABLE gold.genre_analytics    TO `analistas`;
GRANT SELECT ON TABLE gold.auto_brand_analytics TO `analistas`;
GRANT SELECT ON TABLE gold.market_summary     TO `analistas`;

-- ──────────────────────────────────────────
-- GRUPO: ingenieros_datos (acceso Silver + Gold)
-- Pueden leer datos transformados
-- ──────────────────────────────────────────
GRANT USAGE  ON DATABASE silver               TO `ingenieros_datos`;
GRANT SELECT ON TABLE silver.movies           TO `ingenieros_datos`;
GRANT SELECT ON TABLE silver.automobiles      TO `ingenieros_datos`;

GRANT USAGE  ON DATABASE gold                 TO `ingenieros_datos`;
GRANT SELECT ON TABLE gold.genre_analytics    TO `ingenieros_datos`;
GRANT SELECT ON TABLE gold.auto_brand_analytics TO `ingenieros_datos`;
GRANT SELECT ON TABLE gold.market_summary     TO `ingenieros_datos`;

-- ──────────────────────────────────────────
-- GRUPO: etl_admin (acceso completo Bronze + Silver + Gold)
-- Administradores del pipeline con permisos de escritura
-- ──────────────────────────────────────────
GRANT ALL PRIVILEGES ON DATABASE bronze       TO `etl_admin`;
GRANT ALL PRIVILEGES ON DATABASE silver       TO `etl_admin`;
GRANT ALL PRIVILEGES ON DATABASE gold         TO `etl_admin`;

-- ──────────────────────────────────────────
-- USUARIO ESPECÍFICO: acceso a Gold para reportes
-- ──────────────────────────────────────────
GRANT USAGE  ON DATABASE gold                         TO `ecalderon@maxwarehouse.com`;
GRANT SELECT ON TABLE gold.genre_analytics            TO `ecalderon@maxwarehouse.com`;
GRANT SELECT ON TABLE gold.auto_brand_analytics       TO `ecalderon@maxwarehouse.com`;
GRANT SELECT ON TABLE gold.market_summary             TO `ecalderon@maxwarehouse.com`;

-- ══════════════════════════════════════════
-- Verificar grants aplicados
-- ══════════════════════════════════════════
SHOW GRANT ON DATABASE bronze;
SHOW GRANT ON DATABASE silver;
SHOW GRANT ON DATABASE gold;
