

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
    FROM "postgres"."public"."retail_sales"
),

cleaned_data AS (
    SELECT
        -- Generate unique row ID for tracking
        md5(cast(coalesce(cast("Store_Name" as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast("Item_Code" as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast("Date_Of_Sale" as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast("Quantity" as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast("Total_Sales" as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as sales_record_id,

        -- Standardized dimensions
        
    LOWER(TRIM(REGEXP_REPLACE("Store_Name", '\s+', ' ', 'g')))
 as store_name,
        TRIM("Item_Code") as item_code,
        TRIM("Item_Barcode") as item_barcode,
        TRIM("Description") as product_description,
        UPPER(TRIM("Category")) as category,
        UPPER(TRIM("Department")) as department,
        UPPER(TRIM("Sub_Department")) as sub_department,
        UPPER(TRIM("Section")) as section,
        
    UPPER(TRIM(REGEXP_REPLACE("Supplier", '\s+', ' ', 'g')))
 as supplier,

        -- Identify Bidco products
        
    CASE
        WHEN UPPER("Supplier") LIKE '%BIDCO%' THEN 1
        ELSE 0
    END
 as is_bidco,

        -- Metrics
        "Quantity" as quantity,
        "Total_Sales" as total_sales,
        "RRP" as rrp,

        -- Calculate realized unit price
        
    CASE
        WHEN "Quantity" > 0 THEN "Total_Sales" / NULLIF("Quantity", 0)
        ELSE NULL
    END
 as realized_unit_price,

        -- Calculate discount depth
        
    CASE
        WHEN "RRP" IS NOT NULL AND "RRP" > 0 AND "Total_Sales" / NULLIF("Quantity", 0) IS NOT NULL
        THEN ROUND((("RRP" - "Total_Sales" / NULLIF("Quantity", 0)) / "RRP" * 100), 2)
        ELSE 0
    END
 as discount_depth_pct,

        -- Detect promotional pricing
        
    CASE
        WHEN "RRP" IS NOT NULL
            AND "RRP" > 0
            AND "Total_Sales" / NULLIF("Quantity", 0) IS NOT NULL
            AND "Total_Sales" / NULLIF("Quantity", 0) <= ("RRP" * (1 - 0.1))
        THEN 1
        ELSE 0
    END
 as is_potential_promo,

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
        
    (
        CASE WHEN "Store_Name" IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN "Item_Code" IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN "Item_Barcode" IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN "Description" IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN "Quantity" IS NOT NULL AND "Quantity" > 0 THEN 1 ELSE 0 END +
        CASE WHEN "Total_Sales" IS NOT NULL AND "Total_Sales" >= 0 THEN 1 ELSE 0 END +
        CASE WHEN "RRP" IS NOT NULL AND "RRP" > 0 THEN 1 ELSE 0 END +
        CASE WHEN "Supplier" IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN "Date_Of_Sale" IS NOT NULL THEN 1 ELSE 0 END
    ) / 9.0 * 100
 as record_quality_score,

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