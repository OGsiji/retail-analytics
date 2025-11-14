

WITH base_sales AS (
    SELECT *
    FROM "postgres"."retail_staging"."stg_retail_sales"
    WHERE is_low_quality_record = 0
      AND realized_unit_price IS NOT NULL
      AND realized_unit_price > 0
),

-- Calculate average prices by SKU, Store, Sub-Department, Section
sku_store_prices AS (
    SELECT
        store_name,
        item_code,
        product_description,
        supplier,
        is_bidco,
        sub_department,
        section,
        AVG(realized_unit_price) as avg_realized_price,
        AVG(rrp) as avg_rrp,
        SUM(quantity) as total_units_sold,
        SUM(total_sales) as total_sales_value,
        COUNT(*) as transaction_count
    FROM base_sales
    GROUP BY
        store_name,
        item_code,
        product_description,
        supplier,
        is_bidco,
        sub_department,
        section
),

-- Calculate competitor average prices within same Section per store
section_store_prices AS (
    SELECT
        store_name,
        sub_department,
        section,
        is_bidco,
        AVG(avg_realized_price) as section_avg_price,
        AVG(avg_rrp) as section_avg_rrp,
        COUNT(DISTINCT item_code) as skus_in_section,
        SUM(total_units_sold) as section_total_units,
        SUM(total_sales_value) as section_total_sales
    FROM sku_store_prices
    GROUP BY
        store_name,
        sub_department,
        section,
        is_bidco
),

-- Calculate overall Section prices (all competitors) per store
overall_section_prices AS (
    SELECT
        store_name,
        sub_department,
        section,
        AVG(avg_realized_price) as overall_section_avg_price,
        AVG(avg_rrp) as overall_section_avg_rrp
    FROM sku_store_prices
    WHERE is_bidco = 0  -- Competitors only
    GROUP BY
        store_name,
        sub_department,
        section
),

-- Join SKU prices with section benchmarks
sku_with_benchmarks AS (
    SELECT
        ssp.*,
        osp.overall_section_avg_price as competitor_avg_price_in_section,
        osp.overall_section_avg_rrp as competitor_avg_rrp_in_section,
        -- Calculate price index (Bidco vs Competitors)
        
    CASE
        WHEN osp.overall_section_avg_price > 0
        THEN ROUND((ssp.avg_realized_price / osp.overall_section_avg_price * 100), 2)
        ELSE NULL
    END
 as price_index_vs_competitors,
        -- Calculate RRP index
        
    CASE
        WHEN osp.overall_section_avg_rrp > 0
        THEN ROUND((ssp.avg_rrp / osp.overall_section_avg_rrp * 100), 2)
        ELSE NULL
    END
 as rrp_index_vs_competitors,
        -- Discount from own RRP
        ROUND((ssp.avg_rrp - ssp.avg_realized_price) / NULLIF(ssp.avg_rrp, 0) * 100, 2) as own_discount_pct
    FROM sku_store_prices ssp
    LEFT JOIN overall_section_prices osp
        ON ssp.store_name = osp.store_name
        AND ssp.sub_department = osp.sub_department
        AND ssp.section = osp.section
),

-- Add price positioning categories
with_positioning AS (
    SELECT
        *,
        
    CASE
        WHEN price_index_vs_competitors IS NULL THEN 'unknown'
        WHEN price_index_vs_competitors < 90 THEN 'discount'
        WHEN price_index_vs_competitors BETWEEN 90 AND 110 THEN 'at_market'
        WHEN price_index_vs_competitors > 110 THEN 'premium'
        ELSE 'unknown'
    END
 as price_positioning,
        CASE
            WHEN price_index_vs_competitors IS NULL THEN 'No Competition Data'
            WHEN price_index_vs_competitors < 80 THEN 'Significant Discount'
            WHEN price_index_vs_competitors < 90 THEN 'Moderate Discount'
            WHEN price_index_vs_competitors <= 110 THEN 'Competitive'
            WHEN price_index_vs_competitors <= 120 THEN 'Moderate Premium'
            ELSE 'Significant Premium'
        END as price_tier
    FROM sku_with_benchmarks
)

SELECT
    store_name,
    item_code,
    product_description,
    supplier,
    is_bidco,
    sub_department,
    section,
    avg_realized_price,
    avg_rrp,
    own_discount_pct,
    competitor_avg_price_in_section,
    competitor_avg_rrp_in_section,
    price_index_vs_competitors,
    rrp_index_vs_competitors,
    price_positioning,
    price_tier,
    total_units_sold,
    total_sales_value,
    transaction_count,
    CURRENT_TIMESTAMP as generated_at
FROM with_positioning
ORDER BY
    is_bidco DESC,
    store_name,
    sub_department,
    section,
    item_code