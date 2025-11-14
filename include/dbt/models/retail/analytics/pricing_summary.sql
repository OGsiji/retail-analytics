{{
  config(
    materialized='table',
    schema='retail_analytics',
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_pricing_summary_view ON {{ this }}(view_level)",
      "CREATE INDEX IF NOT EXISTS idx_pricing_summary_store ON {{ this }}(store_name)"
    ]
  )
}}

WITH price_data AS (
    SELECT * FROM {{ ref('price_index') }}
),

-- Overall Bidco positioning summary
bidco_overall_position AS (
    SELECT
        'Overall' as view_level,
        NULL as store_name,
        NULL as sub_department,
        NULL as section,
        COUNT(DISTINCT CASE WHEN is_bidco = 1 THEN item_code END) as bidco_sku_count,
        COUNT(DISTINCT CASE WHEN is_bidco = 0 THEN item_code END) as competitor_sku_count,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN price_index_vs_competitors END), 2) as bidco_avg_price_index,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN price_index_vs_competitors END), 2) as competitor_avg_price_index,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN own_discount_pct END), 2) as bidco_avg_discount_pct,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN own_discount_pct END), 2) as competitor_avg_discount_pct,
        -- Positioning distribution for Bidco
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'discount' THEN 1 END) as bidco_discount_count,
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'at_market' THEN 1 END) as bidco_at_market_count,
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'premium' THEN 1 END) as bidco_premium_count
    FROM price_data
),

-- Store-level positioning
store_level_position AS (
    SELECT
        'Store' as view_level,
        store_name,
        NULL as sub_department,
        NULL as section,
        COUNT(DISTINCT CASE WHEN is_bidco = 1 THEN item_code END) as bidco_sku_count,
        COUNT(DISTINCT CASE WHEN is_bidco = 0 THEN item_code END) as competitor_sku_count,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN price_index_vs_competitors END), 2) as bidco_avg_price_index,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN price_index_vs_competitors END), 2) as competitor_avg_price_index,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN own_discount_pct END), 2) as bidco_avg_discount_pct,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN own_discount_pct END), 2) as competitor_avg_discount_pct,
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'discount' THEN 1 END) as bidco_discount_count,
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'at_market' THEN 1 END) as bidco_at_market_count,
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'premium' THEN 1 END) as bidco_premium_count
    FROM price_data
    GROUP BY store_name
),

-- Section-level positioning
section_level_position AS (
    SELECT
        'Section' as view_level,
        NULL as store_name,
        sub_department,
        section,
        COUNT(DISTINCT CASE WHEN is_bidco = 1 THEN item_code END) as bidco_sku_count,
        COUNT(DISTINCT CASE WHEN is_bidco = 0 THEN item_code END) as competitor_sku_count,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN price_index_vs_competitors END), 2) as bidco_avg_price_index,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN price_index_vs_competitors END), 2) as competitor_avg_price_index,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN own_discount_pct END), 2) as bidco_avg_discount_pct,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN own_discount_pct END), 2) as competitor_avg_discount_pct,
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'discount' THEN 1 END) as bidco_discount_count,
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'at_market' THEN 1 END) as bidco_at_market_count,
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'premium' THEN 1 END) as bidco_premium_count
    FROM price_data
    GROUP BY sub_department, section
),

-- Store + Section level (most granular)
store_section_position AS (
    SELECT
        'Store + Section' as view_level,
        store_name,
        sub_department,
        section,
        COUNT(DISTINCT CASE WHEN is_bidco = 1 THEN item_code END) as bidco_sku_count,
        COUNT(DISTINCT CASE WHEN is_bidco = 0 THEN item_code END) as competitor_sku_count,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN price_index_vs_competitors END), 2) as bidco_avg_price_index,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN price_index_vs_competitors END), 2) as competitor_avg_price_index,
        ROUND(AVG(CASE WHEN is_bidco = 1 THEN own_discount_pct END), 2) as bidco_avg_discount_pct,
        ROUND(AVG(CASE WHEN is_bidco = 0 THEN own_discount_pct END), 2) as competitor_avg_discount_pct,
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'discount' THEN 1 END) as bidco_discount_count,
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'at_market' THEN 1 END) as bidco_at_market_count,
        COUNT(CASE WHEN is_bidco = 1 AND price_positioning = 'premium' THEN 1 END) as bidco_premium_count
    FROM price_data
    GROUP BY store_name, sub_department, section
),

-- Combine all views
combined AS (
    SELECT * FROM bidco_overall_position
    UNION ALL
    SELECT * FROM store_level_position
    UNION ALL
    SELECT * FROM section_level_position
    UNION ALL
    SELECT * FROM store_section_position
),

-- Add interpretations
with_insights AS (
    SELECT
        *,
        -- Determine dominant positioning
        CASE
            WHEN bidco_premium_count > bidco_at_market_count AND bidco_premium_count > bidco_discount_count
            THEN 'Premium Positioning'
            WHEN bidco_discount_count > bidco_at_market_count AND bidco_discount_count > bidco_premium_count
            THEN 'Discount Positioning'
            WHEN bidco_at_market_count > bidco_discount_count AND bidco_at_market_count > bidco_premium_count
            THEN 'Market Rate Positioning'
            ELSE 'Mixed Positioning'
        END as dominant_positioning,
        -- Calculate positioning percentages
        ROUND(bidco_discount_count * 100.0 / NULLIF(bidco_sku_count, 0), 2) as discount_pct,
        ROUND(bidco_at_market_count * 100.0 / NULLIF(bidco_sku_count, 0), 2) as at_market_pct,
        ROUND(bidco_premium_count * 100.0 / NULLIF(bidco_sku_count, 0), 2) as premium_pct
    FROM combined
)

SELECT
    view_level,
    store_name,
    sub_department,
    section,
    bidco_sku_count,
    competitor_sku_count,
    bidco_avg_price_index,
    competitor_avg_price_index,
    bidco_avg_discount_pct,
    competitor_avg_discount_pct,
    bidco_discount_count,
    bidco_at_market_count,
    bidco_premium_count,
    discount_pct,
    at_market_pct,
    premium_pct,
    dominant_positioning,
    CURRENT_TIMESTAMP as generated_at
FROM with_insights
ORDER BY
    CASE view_level
        WHEN 'Overall' THEN 1
        WHEN 'Store' THEN 2
        WHEN 'Section' THEN 3
        WHEN 'Store + Section' THEN 4
    END,
    store_name NULLS FIRST,
    sub_department NULLS FIRST,
    section NULLS FIRST
