-- ═══════════════════════════════════════════════════════════
-- DASHBOARD — Consultas SQL para visualización (Capa Gold)
-- Usar estas queries en Databricks SQL o Power BI
-- ═══════════════════════════════════════════════════════════

-- ──────────────────────────────────────────
-- 1. Analítica de Directores — Movies
--    Tabla: gold.genre_analytics
-- ──────────────────────────────────────────

-- Top directores por rentabilidad promedio
SELECT
    director,
    total_peliculas,
    ROUND(ganancia_promedio_usd / 1000000, 2) AS ganancia_MM_usd,
    ROUND(roi_promedio * 100, 1)              AS roi_pct,
    categoria_rendimiento
FROM gold.genre_analytics
ORDER BY ganancia_promedio_usd DESC
LIMIT 20;

-- Distribución de películas por categoría de rendimiento
SELECT
    categoria_rendimiento,
    COUNT(*)                                  AS total_directores,
    ROUND(AVG(ganancia_promedio_usd) / 1000000, 2) AS ganancia_prom_MM
FROM gold.genre_analytics
GROUP BY categoria_rendimiento
ORDER BY ganancia_prom_MM DESC;

-- ──────────────────────────────────────────
-- 2. Analítica por Marca — Automobiles
--    Tabla: gold.auto_brand_analytics
-- ──────────────────────────────────────────

-- Marcas por precio promedio y eficiencia
SELECT
    make,
    fuel_type,
    total_modelos,
    precio_promedio,
    eficiencia_promedio_mpg,
    hp_promedio
FROM gold.auto_brand_analytics
ORDER BY precio_promedio DESC
LIMIT 20;

-- Comparativa de eficiencia por tipo de combustible
SELECT
    fuel_type,
    COUNT(DISTINCT make)                AS total_marcas,
    ROUND(AVG(eficiencia_promedio_mpg), 1) AS mpg_promedio,
    ROUND(AVG(precio_promedio), 0)      AS precio_prom
FROM gold.auto_brand_analytics
GROUP BY fuel_type
ORDER BY mpg_promedio DESC;

-- ──────────────────────────────────────────
-- 3. Resumen de Mercado Combinado
--    Tabla: gold.market_summary
-- ──────────────────────────────────────────

-- Vista ejecutiva: distribución por segmento
SELECT
    categoria,
    segmento,
    total_items,
    precio_promedio,
    rating_promedio,
    eficiencia_promedio,
    votos_promedio
FROM gold.market_summary
ORDER BY categoria, total_items DESC;

-- Totales por categoría
SELECT
    categoria,
    SUM(total_items) AS total_registros
FROM gold.market_summary
GROUP BY categoria;
