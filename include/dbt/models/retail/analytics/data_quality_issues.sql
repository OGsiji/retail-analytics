{{
  config(
    materialized='table',
    schema='retail_analytics'
  )
}}

WITH base_data AS (
    SELECT * FROM {{ ref('stg_retail_sales') }}
),

-- Identify duplicate records
duplicates AS (
    SELECT
        store_name,
        item_code,
        sale_date,
        COUNT(*) as duplicate_count,
        ARRAY_AGG(sales_record_id) as duplicate_ids,
        SUM(total_sales) as total_duplicate_sales
    FROM base_data
    GROUP BY store_name, item_code, sale_date
    HAVING COUNT(*) > 1
),

-- Calculate statistical thresholds for outlier detection
stats AS (
    SELECT
        AVG(quantity) as avg_quantity,
        STDDEV(quantity) as stddev_quantity,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY quantity) as q99_quantity,
        AVG(realized_unit_price) as avg_price,
        STDDEV(realized_unit_price) as stddev_price,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY realized_unit_price) as q99_price
    FROM base_data
    WHERE quantity > 0 AND realized_unit_price IS NOT NULL
),

-- Identify quantity outliers
quantity_outliers AS (
    SELECT
        b.sales_record_id,
        b.store_name,
        b.supplier,
        b.item_code,
        b.product_description,
        b.quantity,
        b.total_sales,
        b.sale_date,
        'Quantity Outlier' as issue_type,
        CASE
            WHEN b.quantity < 0 THEN 'Negative Quantity'
            WHEN b.quantity > (s.avg_quantity + 3 * s.stddev_quantity) THEN 'Extremely High Quantity'
            WHEN b.quantity > s.q99_quantity THEN 'High Quantity (>99th percentile)'
        END as issue_severity,
        ROUND(b.quantity / NULLIF(s.avg_quantity, 0), 2) as deviation_from_avg
    FROM base_data b
    CROSS JOIN stats s
    WHERE b.quantity < 0
       OR b.quantity > (s.avg_quantity + 3 * s.stddev_quantity)
),

-- Identify price outliers
price_outliers AS (
    SELECT
        b.sales_record_id,
        b.store_name,
        b.supplier,
        b.item_code,
        b.product_description,
        b.quantity,
        b.total_sales,
        b.sale_date,
        'Price Outlier' as issue_type,
        CASE
            WHEN b.total_sales < 0 THEN 'Negative Sales Value'
            WHEN b.realized_unit_price > (s.avg_price + 3 * s.stddev_price) THEN 'Extremely High Price'
            WHEN b.realized_unit_price > s.q99_price THEN 'High Price (>99th percentile)'
            WHEN b.realized_unit_price < 1 THEN 'Suspiciously Low Price'
        END as issue_severity,
        ROUND(b.realized_unit_price / NULLIF(s.avg_price, 0), 2) as deviation_from_avg
    FROM base_data b
    CROSS JOIN stats s
    WHERE b.total_sales < 0
       OR b.realized_unit_price < 1
       OR (b.realized_unit_price > (s.avg_price + 3 * s.stddev_price))
),

-- Identify missing critical fields
missing_fields AS (
    SELECT
        sales_record_id,
        store_name,
        supplier,
        item_code,
        product_description,
        quantity,
        total_sales,
        sale_date,
        'Missing Critical Data' as issue_type,
        CASE
            WHEN has_missing_rrp = 1 AND has_missing_barcode = 1 THEN 'Missing RRP and Barcode'
            WHEN has_missing_rrp = 1 THEN 'Missing RRP'
            WHEN has_missing_barcode = 1 THEN 'Missing Barcode'
        END as issue_severity,
        NULL::NUMERIC as deviation_from_avg
    FROM base_data
    WHERE has_missing_rrp = 1 OR has_missing_barcode = 1
),

-- Combine all issues
all_issues AS (
    SELECT * FROM quantity_outliers
    UNION ALL
    SELECT * FROM price_outliers
    UNION ALL
    SELECT * FROM missing_fields
),

-- Add priority ranking
prioritized_issues AS (
    SELECT
        *,
        CASE
            WHEN issue_severity LIKE 'Negative%' THEN 1
            WHEN issue_severity LIKE 'Extremely%' THEN 2
            WHEN issue_severity LIKE 'High%' THEN 3
            WHEN issue_severity LIKE 'Missing RRP and Barcode%' THEN 2
            ELSE 4
        END as priority_rank,
        CURRENT_TIMESTAMP as identified_at
    FROM all_issues
)

SELECT
    sales_record_id,
    store_name,
    supplier,
    item_code,
    product_description,
    quantity,
    total_sales,
    sale_date,
    issue_type,
    issue_severity,
    priority_rank,
    deviation_from_avg,
    identified_at,
    -- Flag if this is a Bidco product with issues
    CASE WHEN UPPER(supplier) LIKE '%BIDCO%' THEN 1 ELSE 0 END as affects_bidco
FROM prioritized_issues
ORDER BY priority_rank, affects_bidco DESC, sale_date DESC
