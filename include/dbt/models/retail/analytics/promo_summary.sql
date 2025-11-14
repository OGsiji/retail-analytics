{{
  config(
    materialized='table',
    schema='retail_analytics'
  )
}}

-- Summary metrics for promotional activity with uplift analysis

WITH promo_data AS (
    SELECT * FROM {{ ref('promo_detection') }}
    WHERE is_on_promo = 1
),

-- Promo coverage by supplier
promo_coverage AS (
    SELECT
        supplier,
        is_bidco,
        COUNT(DISTINCT store_name) as stores_with_promo,
        COUNT(DISTINCT item_code) as skus_on_promo,
        ROUND(AVG(promo_uplift_pct), 2) as avg_uplift_pct,
        ROUND(AVG(promo_discount_depth_pct), 2) as avg_discount_depth_pct,
        SUM(promo_units_sold) as total_promo_units,
        SUM(promo_sales_value) as total_promo_sales,
        -- Discount tier distribution
        COUNT(CASE WHEN discount_tier = 'Deep Discount (30%+)' THEN 1 END) as deep_discount_count,
        COUNT(CASE WHEN discount_tier = 'Heavy Discount (20-30%)' THEN 1 END) as heavy_discount_count,
        COUNT(CASE WHEN discount_tier = 'Moderate Discount (10-20%)' THEN 1 END) as moderate_discount_count
    FROM promo_data
    GROUP BY supplier, is_bidco
),

-- Top performing SKUs by uplift
top_skus_by_uplift_ranked AS (
    SELECT
        'By Promo Uplift' as ranking_criteria,
        supplier,
        is_bidco,
        item_code,
        product_description,
        store_name,
        promo_uplift_pct,
        promo_discount_depth_pct,
        promo_units_sold,
        promo_sales_value,
        ROW_NUMBER() OVER (PARTITION BY is_bidco ORDER BY promo_uplift_pct DESC NULLS LAST) as rank
    FROM promo_data
    WHERE promo_uplift_pct IS NOT NULL
),

top_skus_by_uplift AS (
    SELECT * FROM top_skus_by_uplift_ranked WHERE rank <= 10
),

-- Top performing SKUs by volume
top_skus_by_volume_ranked AS (
    SELECT
        'By Promo Volume' as ranking_criteria,
        supplier,
        is_bidco,
        item_code,
        product_description,
        store_name,
        promo_uplift_pct,
        promo_discount_depth_pct,
        promo_units_sold,
        promo_sales_value,
        ROW_NUMBER() OVER (PARTITION BY is_bidco ORDER BY promo_units_sold DESC) as rank
    FROM promo_data
),

top_skus_by_volume AS (
    SELECT * FROM top_skus_by_volume_ranked WHERE rank <= 10
),

-- Store-level promo performance
store_promo_performance AS (
    SELECT
        store_name,
        COUNT(DISTINCT CASE WHEN is_bidco = 1 THEN item_code END) as bidco_skus_on_promo,
        COUNT(DISTINCT CASE WHEN is_bidco = 0 THEN item_code END) as competitor_skus_on_promo,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN promo_uplift_pct END), 2) as bidco_avg_uplift,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN promo_uplift_pct END), 2) as competitor_avg_uplift,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN promo_discount_depth_pct END), 2) as bidco_avg_discount,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN promo_discount_depth_pct END), 2) as competitor_avg_discount,
        SUM(CASE WHEN is_bidco = 1 THEN promo_sales_value ELSE 0 END) as bidco_promo_sales,
        SUM(CASE WHEN is_bidco = 0 THEN promo_sales_value ELSE 0 END) as competitor_promo_sales
    FROM promo_data
    GROUP BY store_name
),

-- Category/Section level analysis
category_promo_analysis AS (
    SELECT
        sub_department,
        section,
        COUNT(DISTINCT CASE WHEN is_bidco = 1 THEN item_code END) as bidco_skus,
        COUNT(DISTINCT CASE WHEN is_bidco = 0 THEN item_code END) as competitor_skus,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN promo_discount_depth_pct END), 2) as bidco_avg_discount,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN promo_discount_depth_pct END), 2) as competitor_avg_discount,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN promo_uplift_pct END), 2) as bidco_avg_uplift,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN promo_uplift_pct END), 2) as competitor_avg_uplift,
        SUM(CASE WHEN is_bidco = 1 THEN promo_sales_value ELSE 0 END) as bidco_promo_sales,
        SUM(CASE WHEN is_bidco = 0 THEN promo_sales_value ELSE 0 END) as competitor_promo_sales
    FROM promo_data
    GROUP BY sub_department, section
)

-- Export summary for API
SELECT
    'promo_coverage' as metric_type,
    supplier,
    is_bidco,
    NULL as store_name,
    NULL as sub_department,
    NULL as section,
    stores_with_promo as metric_value_1,
    skus_on_promo as metric_value_2,
    avg_uplift_pct as metric_value_3,
    avg_discount_depth_pct as metric_value_4,
    CURRENT_TIMESTAMP as generated_at
FROM promo_coverage

UNION ALL

SELECT
    'store_performance' as metric_type,
    NULL as supplier,
    NULL as is_bidco,
    store_name,
    NULL as sub_department,
    NULL as section,
    bidco_skus_on_promo as metric_value_1,
    competitor_skus_on_promo as metric_value_2,
    bidco_avg_uplift as metric_value_3,
    competitor_avg_uplift as metric_value_4,
    CURRENT_TIMESTAMP as generated_at
FROM store_promo_performance

UNION ALL

SELECT
    'category_analysis' as metric_type,
    NULL as supplier,
    NULL as is_bidco,
    NULL as store_name,
    sub_department,
    section,
    bidco_skus as metric_value_1,
    competitor_skus as metric_value_2,
    bidco_avg_uplift as metric_value_3,
    competitor_avg_uplift as metric_value_4,
    CURRENT_TIMESTAMP as generated_at
FROM category_promo_analysis

ORDER BY metric_type, is_bidco DESC NULLS LAST
