{{
  config(
    materialized='view',
    schema='retail_staging'
  )
}}

WITH source_data AS (
    SELECT
        "Store_Name",
        "Item_Code",
        "Item_Barcode",
        "Description",
        "Category",
        "Department",
        "Sub_Department",
        "Section",
        "Quantity",
        "Total_Sales",
        "RRP",
        "Supplier",
        "Date_Of_Sale"
    FROM {{ source('retail_raw', 'retail_sales') }}
),

cleaned_data AS (
    SELECT
        -- Generate unique row ID for tracking
        {{ dbt_utils.generate_surrogate_key(['"Store_Name"', '"Item_Code"', '"Date_Of_Sale"', '"Quantity"', '"Total_Sales"']) }} as sales_record_id,

        -- Standardized dimensions
        {{ standardize_store_name('"Store_Name"') }} as store_name,
        TRIM("Item_Code") as item_code,
        TRIM("Item_Barcode") as item_barcode,
        TRIM("Description") as product_description,
        UPPER(TRIM("Category")) as category,
        UPPER(TRIM("Department")) as department,
        UPPER(TRIM("Sub_Department")) as sub_department,
        UPPER(TRIM("Section")) as section,
        {{ standardize_supplier_name('"Supplier"') }} as supplier,

        -- Identify Bidco products
        {{ is_bidco_product('"Supplier"') }} as is_bidco,

        -- Metrics
        "Quantity" as quantity,
        "Total_Sales" as total_sales,
        "RRP" as rrp,

        -- Calculate realized unit price
        {{ calculate_unit_price('"Total_Sales"', '"Quantity"') }} as realized_unit_price,

        -- Calculate discount depth
        {{ calculate_discount_depth('"Total_Sales" / NULLIF("Quantity", 0)', '"RRP"') }} as discount_depth_pct,

        -- Detect promotional pricing
        {{ is_promo_price('"Total_Sales" / NULLIF("Quantity", 0)', '"RRP"', var('promo_discount_threshold')) }} as is_potential_promo,

        -- Date information
        "Date_Of_Sale" as sale_date,
        EXTRACT(YEAR FROM "Date_Of_Sale") as sale_year,
        EXTRACT(MONTH FROM "Date_Of_Sale") as sale_month,
        EXTRACT(DAY FROM "Date_Of_Sale") as sale_day,
        EXTRACT(DOW FROM "Date_Of_Sale") as day_of_week,
        TO_CHAR("Date_Of_Sale", 'Day') as day_name,

        -- Data quality flags
        CASE
            WHEN "Quantity" IS NULL OR "Quantity" <= 0 THEN 1
            ELSE 0
        END as has_invalid_quantity,

        CASE
            WHEN "Total_Sales" IS NULL OR "Total_Sales" < 0 THEN 1
            ELSE 0
        END as has_invalid_sales,

        CASE
            WHEN "RRP" IS NULL OR "RRP" <= 0 THEN 1
            ELSE 0
        END as has_missing_rrp,

        CASE
            WHEN "Item_Barcode" IS NULL OR TRIM("Item_Barcode") = '' THEN 1
            ELSE 0
        END as has_missing_barcode,

        -- Record quality score
        {{ calculate_record_quality_score() }} as record_quality_score,

        -- Metadata
        CURRENT_TIMESTAMP as processed_at

    FROM source_data
),

with_validation AS (
    SELECT
        *,
        -- Overall data quality flag
        CASE
            WHEN has_invalid_quantity = 1
                OR has_invalid_sales = 1
                OR record_quality_score < 70
            THEN 1
            ELSE 0
        END as is_low_quality_record

    FROM cleaned_data
)

SELECT * FROM with_validation
