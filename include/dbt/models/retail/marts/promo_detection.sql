{{
  config(
    materialized='table',
    schema='retail_marts'
  )
}}

WITH base_sales AS (
    SELECT *
    FROM {{ ref('stg_retail_sales') }}
    WHERE is_low_quality_record = 0  -- Only use high-quality records
),

-- Calculate SKU-level daily pricing
sku_daily_prices AS (
    SELECT
        store_name,
        item_code,
        product_description,
        supplier,
        sub_department,
        section,
        sale_date,
        is_bidco,
        AVG(realized_unit_price) as avg_daily_price,
        AVG(rrp) as avg_daily_rrp,
        SUM(quantity) as total_daily_quantity,
        SUM(total_sales) as total_daily_sales,
        COUNT(*) as transaction_count,
        MAX(is_potential_promo) as has_promo_indicator
    FROM base_sales
    WHERE realized_unit_price IS NOT NULL
      AND rrp IS NOT NULL
      AND rrp > 0
    GROUP BY
        store_name,
        item_code,
        product_description,
        supplier,
        sub_department,
        section,
        sale_date,
        is_bidco
),

-- Calculate baseline (non-promo) price per SKU
sku_baseline AS (
    SELECT
        store_name,
        item_code,
        AVG(avg_daily_rrp) as baseline_rrp,
        -- Baseline price is median of non-discounted days
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_daily_price) as baseline_price,
        AVG(avg_daily_price) as mean_price
    FROM sku_daily_prices
    WHERE has_promo_indicator = 0  -- Days without significant discount
    GROUP BY store_name, item_code
    HAVING COUNT(DISTINCT sale_date) >= 2  -- Need at least 2 days of non-promo data
),

-- Detect promotional periods
promo_periods AS (
    SELECT
        sdp.*,
        sb.baseline_price,
        sb.baseline_rrp,
        -- Calculate discount depth vs baseline
        ROUND(((COALESCE(sb.baseline_price, sdp.avg_daily_rrp) - sdp.avg_daily_price) /
               NULLIF(COALESCE(sb.baseline_price, sdp.avg_daily_rrp), 0) * 100), 2) as discount_depth_pct,
        -- Flag as promo if >= 10% discount from baseline
        CASE
            WHEN sdp.avg_daily_price <= (COALESCE(sb.baseline_price, sdp.avg_daily_rrp) * (1 - {{ var('promo_discount_threshold') }}))
            THEN 1
            ELSE 0
        END as is_promo_day
    FROM sku_daily_prices sdp
    LEFT JOIN sku_baseline sb
        ON sdp.store_name = sb.store_name
        AND sdp.item_code = sb.item_code
),

-- Count consecutive promo days per SKU
promo_day_count AS (
    SELECT
        store_name,
        item_code,
        product_description,
        supplier,
        sub_department,
        section,
        is_bidco,
        SUM(is_promo_day) as promo_days,
        COUNT(DISTINCT sale_date) as total_days,
        MIN(sale_date) as week_start_date,
        MAX(sale_date) as week_end_date
    FROM promo_periods
    GROUP BY
        store_name,
        item_code,
        product_description,
        supplier,
        sub_department,
        section,
        is_bidco
),

-- Final promo classification
promo_classification AS (
    SELECT
        pdc.*,
        -- Classify as promo if >= 2 days at discounted price
        CASE
            WHEN pdc.promo_days >= {{ var('promo_min_days') }}
            THEN 1
            ELSE 0
        END as is_on_promo,
        ROUND(pdc.promo_days * 100.0 / pdc.total_days, 2) as promo_days_pct
    FROM promo_day_count pdc
),

-- Calculate promo metrics
promo_metrics AS (
    SELECT
        pc.*,
        pp.baseline_price,
        pp.baseline_rrp,
        -- Promo period metrics
        AVG(CASE WHEN pp.is_promo_day = 1 THEN pp.avg_daily_price END) as avg_promo_price,
        AVG(CASE WHEN pp.is_promo_day = 1 THEN pp.discount_depth_pct END) as avg_promo_discount_pct,
        SUM(CASE WHEN pp.is_promo_day = 1 THEN pp.total_daily_quantity END) as promo_units_sold,
        SUM(CASE WHEN pp.is_promo_day = 1 THEN pp.total_daily_sales END) as promo_sales_value,
        -- Non-promo period metrics
        AVG(CASE WHEN pp.is_promo_day = 0 THEN pp.avg_daily_price END) as avg_non_promo_price,
        SUM(CASE WHEN pp.is_promo_day = 0 THEN pp.total_daily_quantity END) as non_promo_units_sold,
        SUM(CASE WHEN pp.is_promo_day = 0 THEN pp.total_daily_sales END) as non_promo_sales_value
    FROM promo_classification pc
    LEFT JOIN promo_periods pp
        ON pc.store_name = pp.store_name
        AND pc.item_code = pp.item_code
    GROUP BY
        pc.store_name,
        pc.item_code,
        pc.product_description,
        pc.supplier,
        pc.sub_department,
        pc.section,
        pc.is_bidco,
        pc.promo_days,
        pc.total_days,
        pc.week_start_date,
        pc.week_end_date,
        pc.is_on_promo,
        pc.promo_days_pct,
        pp.baseline_price,
        pp.baseline_rrp
),

-- Calculate uplift
with_uplift AS (
    SELECT
        *,
        -- Calculate average daily units
        ROUND(COALESCE(promo_units_sold, 0) / NULLIF(promo_days, 0), 2) as avg_daily_promo_units,
        ROUND(COALESCE(non_promo_units_sold, 0) / NULLIF((total_days - promo_days), 0), 2) as avg_daily_non_promo_units,
        -- Promo uplift %
        ROUND(
            ((COALESCE(promo_units_sold, 0) / NULLIF(promo_days, 0)) -
             (COALESCE(non_promo_units_sold, 0) / NULLIF((total_days - promo_days), 0))) /
            NULLIF((COALESCE(non_promo_units_sold, 0) / NULLIF((total_days - promo_days), 0)), 0) * 100,
        2) as promo_uplift_pct
    FROM promo_metrics
)

SELECT
    store_name,
    item_code,
    product_description,
    supplier,
    is_bidco,
    sub_department,
    section,
    week_start_date,
    week_end_date,
    total_days,
    promo_days,
    promo_days_pct,
    is_on_promo,
    baseline_price,
    baseline_rrp,
    avg_non_promo_price,
    avg_promo_price,
    avg_promo_discount_pct as promo_discount_depth_pct,
    non_promo_units_sold,
    promo_units_sold,
    avg_daily_non_promo_units as baseline_daily_units,
    avg_daily_promo_units as promo_daily_units,
    promo_uplift_pct,
    non_promo_sales_value,
    promo_sales_value,
    COALESCE(promo_sales_value, 0) + COALESCE(non_promo_sales_value, 0) as total_sales_value,
    CURRENT_TIMESTAMP as generated_at
FROM with_uplift
ORDER BY
    is_bidco DESC,
    promo_uplift_pct DESC NULLS LAST,
    total_sales_value DESC
