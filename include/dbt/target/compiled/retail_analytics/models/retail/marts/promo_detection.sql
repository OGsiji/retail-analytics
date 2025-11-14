

-- Cross-store, time-based promo detection and uplift calculation
-- Approach: Compare same SKU across different stores and dates to detect promos and measure uplift

WITH base_sales AS (
    SELECT *
    FROM "postgres"."retail_staging"."stg_retail_sales"
    WHERE is_low_quality_record = 0
      AND realized_unit_price IS NOT NULL
      AND rrp IS NOT NULL
      AND rrp > 0
),

-- Daily store-SKU level aggregation
daily_sku_sales AS (
    SELECT
        store_name,
        item_code,
        product_description,
        supplier,
        sub_department,
        section,
        sale_date,
        is_bidco,
        AVG(realized_unit_price) as avg_realized_price,
        AVG(rrp) as avg_rrp,
        SUM(quantity) as daily_quantity,
        SUM(total_sales) as daily_sales,
        COUNT(*) as transaction_count,
        -- Calculate discount from RRP
        ROUND(CAST(
            (AVG(rrp) - AVG(realized_unit_price)) / NULLIF(AVG(rrp), 0) * 100
            AS NUMERIC
        ), 2) as discount_from_rrp_pct
    FROM base_sales
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

-- Calculate SKU-level baselines across ALL stores and dates
sku_baselines AS (
    SELECT
        item_code,
        is_bidco,
        -- Baseline RRP and price (median across all observations)
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_rrp) as baseline_rrp,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_realized_price) as baseline_price,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY discount_from_rrp_pct) as p25_discount,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY discount_from_rrp_pct) as median_discount,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY discount_from_rrp_pct) as p75_discount,
        -- Baseline quantity (median daily quantity across all stores)
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY daily_quantity) as baseline_daily_quantity,
        AVG(daily_quantity) as avg_daily_quantity,
        STDDEV(daily_quantity) as stddev_daily_quantity
    FROM daily_sku_sales
    GROUP BY item_code, is_bidco
),

-- Classify each observation as promo or non-promo
promo_classification AS (
    SELECT
        dss.*,
        sb.baseline_rrp,
        sb.baseline_price,
        sb.median_discount,
        sb.p75_discount,
        sb.baseline_daily_quantity,
        sb.avg_daily_quantity,
        -- Flag as promo if discount >= threshold (10%)
        CASE
            WHEN dss.discount_from_rrp_pct >= (0.1 * 100)
            THEN 1
            ELSE 0
        END as is_on_promo,
        -- Also flag as "strong promo" if discount > 75th percentile for this SKU
        CASE
            WHEN dss.discount_from_rrp_pct > sb.p75_discount
            THEN 1
            ELSE 0
        END as is_strong_promo,
        -- Calculate uplift: (actual quantity - baseline) / baseline * 100
        CASE
            WHEN sb.baseline_daily_quantity > 0
            THEN ROUND(CAST(
                (dss.daily_quantity - sb.baseline_daily_quantity) / sb.baseline_daily_quantity * 100
                AS NUMERIC
            ), 2)
            ELSE NULL
        END as quantity_uplift_pct,
        -- Z-score for quantity (how many std devs above/below average)
        CASE
            WHEN sb.stddev_daily_quantity > 0
            THEN ROUND(CAST(
                (dss.daily_quantity - sb.avg_daily_quantity) / sb.stddev_daily_quantity
                AS NUMERIC
            ), 2)
            ELSE 0
        END as quantity_zscore
    FROM daily_sku_sales dss
    LEFT JOIN sku_baselines sb
        ON dss.item_code = sb.item_code
        AND dss.is_bidco = sb.is_bidco
),

-- Aggregate to store-SKU level with promo metrics
store_sku_aggregated AS (
    SELECT
        store_name,
        item_code,
        product_description,
        supplier,
        sub_department,
        section,
        is_bidco,
        -- Date range
        MIN(sale_date) as first_sale_date,
        MAX(sale_date) as last_sale_date,
        -- Promo metrics
        SUM(is_on_promo) as promo_days,
        COUNT(*) as total_days,
        ROUND(CAST(SUM(is_on_promo) * 100.0 / COUNT(*) AS NUMERIC), 2) as promo_coverage_pct,
        MAX(is_on_promo) as is_on_promo,  -- 1 if ANY day was promo
        -- Pricing
        AVG(avg_rrp) as avg_rrp,
        AVG(avg_realized_price) as avg_realized_price,
        AVG(baseline_price) as baseline_price,
        AVG(discount_from_rrp_pct) as avg_discount_from_rrp_pct,
        MAX(discount_from_rrp_pct) as max_discount_from_rrp_pct,
        -- Promo vs non-promo comparison
        AVG(CASE WHEN is_on_promo = 1 THEN avg_realized_price END) as avg_promo_price,
        AVG(CASE WHEN is_on_promo = 0 THEN avg_realized_price END) as avg_non_promo_price,
        AVG(CASE WHEN is_on_promo = 1 THEN discount_from_rrp_pct END) as promo_discount_depth_pct,
        -- Volume metrics
        SUM(daily_quantity) as total_units_sold,
        SUM(daily_sales) as total_sales_value,
        AVG(daily_quantity) as avg_daily_units,
        AVG(baseline_daily_quantity) as baseline_daily_units,
        -- Promo vs baseline volume
        SUM(CASE WHEN is_on_promo = 1 THEN daily_quantity ELSE 0 END) as promo_units_sold,
        SUM(CASE WHEN is_on_promo = 0 THEN daily_quantity ELSE 0 END) as non_promo_units_sold,
        SUM(CASE WHEN is_on_promo = 1 THEN daily_sales ELSE 0 END) as promo_sales_value,
        SUM(CASE WHEN is_on_promo = 0 THEN daily_sales ELSE 0 END) as non_promo_sales_value,
        -- Uplift metrics
        AVG(quantity_uplift_pct) as avg_quantity_uplift_pct,
        AVG(CASE WHEN is_on_promo = 1 THEN quantity_uplift_pct END) as promo_quantity_uplift_pct,
        MAX(quantity_zscore) as max_quantity_zscore
    FROM promo_classification
    GROUP BY
        store_name,
        item_code,
        product_description,
        supplier,
        sub_department,
        section,
        is_bidco
),

-- Calculate final uplift (promo days vs non-promo days)
with_uplift AS (
    SELECT
        *,
        -- Calculate uplift: (avg promo daily units - avg non-promo daily units) / avg non-promo daily units
        CASE
            WHEN non_promo_units_sold > 0 AND promo_days > 0 AND (total_days - promo_days) > 0
            THEN ROUND(CAST(
                ((promo_units_sold / promo_days) - (non_promo_units_sold / (total_days - promo_days))) /
                (non_promo_units_sold / (total_days - promo_days)) * 100
                AS NUMERIC
            ), 2)
            -- If no non-promo days, use baseline from cross-store comparison
            WHEN baseline_daily_units > 0 AND promo_days > 0
            THEN ROUND(CAST(
                ((promo_units_sold / promo_days) - baseline_daily_units) /
                baseline_daily_units * 100
                AS NUMERIC
            ), 2)
            ELSE NULL
        END as promo_uplift_pct,
        -- Categorize discount tier
        CASE
            WHEN avg_discount_from_rrp_pct IS NULL THEN 'No RRP Data'
            WHEN avg_discount_from_rrp_pct < 5 THEN 'Minimal Discount (<5%)'
            WHEN avg_discount_from_rrp_pct < 10 THEN 'Light Discount (5-10%)'
            WHEN avg_discount_from_rrp_pct < 20 THEN 'Moderate Discount (10-20%)'
            WHEN avg_discount_from_rrp_pct < 30 THEN 'Heavy Discount (20-30%)'
            ELSE 'Deep Discount (30%+)'
        END as discount_tier
    FROM store_sku_aggregated
)

SELECT
    -- Identifiers
    store_name,
    item_code,
    product_description,
    supplier,
    is_bidco,
    sub_department,
    section,
    -- Date range
    first_sale_date,
    last_sale_date,
    total_days,
    promo_days,
    promo_coverage_pct,
    -- Promo classification
    is_on_promo,
    discount_tier,
    -- Pricing metrics
    avg_rrp,
    baseline_price,
    avg_realized_price,
    avg_non_promo_price,
    avg_promo_price,
    avg_discount_from_rrp_pct,
    max_discount_from_rrp_pct,
    promo_discount_depth_pct,
    -- Volume metrics
    total_units_sold,
    total_sales_value,
    avg_daily_units,
    baseline_daily_units,
    promo_units_sold,
    non_promo_units_sold,
    promo_sales_value,
    non_promo_sales_value,
    -- Uplift metrics
    promo_uplift_pct,
    promo_quantity_uplift_pct,
    max_quantity_zscore,
    -- Metadata
    CURRENT_TIMESTAMP as generated_at
FROM with_uplift
ORDER BY
    is_bidco DESC,
    promo_uplift_pct DESC NULLS LAST,
    total_sales_value DESC