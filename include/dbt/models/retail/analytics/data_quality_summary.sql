{{
  config(
    materialized='table',
    schema='retail_analytics'
  )
}}

WITH base_data AS (
    SELECT * FROM {{ ref('stg_retail_sales') }}
),

-- Overall data quality metrics
overall_quality AS (
    SELECT
        'Overall Dataset' as dimension,
        'All' as dimension_value,
        COUNT(*) as total_records,
        SUM(is_low_quality_record) as low_quality_records,
        COUNT(*) - SUM(is_low_quality_record) as high_quality_records,
        ROUND(AVG(record_quality_score), 2) as avg_quality_score,
        SUM(has_invalid_quantity) as invalid_quantity_count,
        SUM(has_invalid_sales) as invalid_sales_count,
        SUM(has_missing_rrp) as missing_rrp_count,
        SUM(has_missing_barcode) as missing_barcode_count,
        ROUND(SUM(is_low_quality_record) * 100.0 / COUNT(*), 2) as low_quality_pct
    FROM base_data
),

-- Quality by store
store_quality AS (
    SELECT
        'Store' as dimension,
        store_name as dimension_value,
        COUNT(*) as total_records,
        SUM(is_low_quality_record) as low_quality_records,
        COUNT(*) - SUM(is_low_quality_record) as high_quality_records,
        ROUND(AVG(record_quality_score), 2) as avg_quality_score,
        SUM(has_invalid_quantity) as invalid_quantity_count,
        SUM(has_invalid_sales) as invalid_sales_count,
        SUM(has_missing_rrp) as missing_rrp_count,
        SUM(has_missing_barcode) as missing_barcode_count,
        ROUND(SUM(is_low_quality_record) * 100.0 / COUNT(*), 2) as low_quality_pct
    FROM base_data
    GROUP BY store_name
),

-- Quality by supplier
supplier_quality AS (
    SELECT
        'Supplier' as dimension,
        supplier as dimension_value,
        COUNT(*) as total_records,
        SUM(is_low_quality_record) as low_quality_records,
        COUNT(*) - SUM(is_low_quality_record) as high_quality_records,
        ROUND(AVG(record_quality_score), 2) as avg_quality_score,
        SUM(has_invalid_quantity) as invalid_quantity_count,
        SUM(has_invalid_sales) as invalid_sales_count,
        SUM(has_missing_rrp) as missing_rrp_count,
        SUM(has_missing_barcode) as missing_barcode_count,
        ROUND(SUM(is_low_quality_record) * 100.0 / COUNT(*), 2) as low_quality_pct
    FROM base_data
    GROUP BY supplier
),

-- Quality by category
category_quality AS (
    SELECT
        'Category' as dimension,
        category as dimension_value,
        COUNT(*) as total_records,
        SUM(is_low_quality_record) as low_quality_records,
        COUNT(*) - SUM(is_low_quality_record) as high_quality_records,
        ROUND(AVG(record_quality_score), 2) as avg_quality_score,
        SUM(has_invalid_quantity) as invalid_quantity_count,
        SUM(has_invalid_sales) as invalid_sales_count,
        SUM(has_missing_rrp) as missing_rrp_count,
        SUM(has_missing_barcode) as missing_barcode_count,
        ROUND(SUM(is_low_quality_record) * 100.0 / COUNT(*), 2) as low_quality_pct
    FROM base_data
    GROUP BY category
),

-- Combine all quality metrics
combined AS (
    SELECT * FROM overall_quality
    UNION ALL
    SELECT * FROM store_quality
    UNION ALL
    SELECT * FROM supplier_quality
    UNION ALL
    SELECT * FROM category_quality
),

-- Add health score rating
with_rating AS (
    SELECT
        *,
        CASE
            WHEN avg_quality_score >= 90 THEN 'Excellent'
            WHEN avg_quality_score >= 80 THEN 'Good'
            WHEN avg_quality_score >= 70 THEN 'Fair'
            WHEN avg_quality_score >= 60 THEN 'Poor'
            ELSE 'Critical'
        END as health_rating,
        CASE
            WHEN low_quality_pct <= 5 THEN 'Reliable'
            WHEN low_quality_pct <= 15 THEN 'Moderate Issues'
            WHEN low_quality_pct <= 30 THEN 'Significant Issues'
            ELSE 'Unreliable'
        END as reliability_status
    FROM combined
)

SELECT
    dimension,
    dimension_value,
    total_records,
    high_quality_records,
    low_quality_records,
    low_quality_pct,
    avg_quality_score,
    health_rating,
    reliability_status,
    invalid_quantity_count,
    invalid_sales_count,
    missing_rrp_count,
    missing_barcode_count,
    CURRENT_TIMESTAMP as generated_at
FROM with_rating
ORDER BY
    CASE dimension
        WHEN 'Overall Dataset' THEN 1
        WHEN 'Store' THEN 2
        WHEN 'Supplier' THEN 3
        WHEN 'Category' THEN 4
    END,
    low_quality_pct DESC
