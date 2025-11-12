-- Utility macros for retail analytics

-- Calculate realized unit price from total sales and quantity
{% macro calculate_unit_price(total_sales, quantity) %}
    CASE
        WHEN {{ quantity }} > 0 THEN {{ total_sales }} / NULLIF({{ quantity }}, 0)
        ELSE NULL
    END
{% endmacro %}

-- Detect if a price indicates a promotion (discount >= threshold from RRP)
{% macro is_promo_price(realized_price, rrp, threshold=0.10) %}
    CASE
        WHEN {{ rrp }} IS NOT NULL
            AND {{ rrp }} > 0
            AND {{ realized_price }} IS NOT NULL
            AND {{ realized_price }} <= ({{ rrp }} * (1 - {{ threshold }}))
        THEN 1
        ELSE 0
    END
{% endmacro %}

-- Calculate discount depth percentage
{% macro calculate_discount_depth(realized_price, rrp) %}
    CASE
        WHEN {{ rrp }} IS NOT NULL AND {{ rrp }} > 0 AND {{ realized_price }} IS NOT NULL
        THEN ROUND((({{ rrp }} - {{ realized_price }}) / {{ rrp }} * 100), 2)
        ELSE 0
    END
{% endmacro %}

-- Standardize store names (remove extra spaces, lowercase)
{% macro standardize_store_name(store_column) %}
    LOWER(TRIM(REGEXP_REPLACE({{ store_column }}, '\s+', ' ', 'g')))
{% endmacro %}

-- Standardize supplier names
{% macro standardize_supplier_name(supplier_column) %}
    UPPER(TRIM(REGEXP_REPLACE({{ supplier_column }}, '\s+', ' ', 'g')))
{% endmacro %}

-- Check if supplier is Bidco
{% macro is_bidco_product(supplier_column) %}
    CASE
        WHEN UPPER({{ supplier_column }}) LIKE '%BIDCO%' THEN 1
        ELSE 0
    END
{% endmacro %}

-- Calculate data quality score for a record
{% macro calculate_record_quality_score() %}
    (
        CASE WHEN Store_Name IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN Item_Code IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN Item_Barcode IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN Description IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN Quantity IS NOT NULL AND Quantity > 0 THEN 1 ELSE 0 END +
        CASE WHEN Total_Sales IS NOT NULL AND Total_Sales >= 0 THEN 1 ELSE 0 END +
        CASE WHEN RRP IS NOT NULL AND RRP > 0 THEN 1 ELSE 0 END +
        CASE WHEN Supplier IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN Date_Of_Sale IS NOT NULL THEN 1 ELSE 0 END
    ) / 9.0 * 100
{% endmacro %}

-- Detect suspicious outliers in quantity
{% macro is_quantity_outlier(quantity, std_devs=3) %}
    CASE
        WHEN {{ quantity }} IS NULL THEN 1
        WHEN {{ quantity }} < 0 THEN 1
        WHEN {{ quantity }} > (
            SELECT AVG(Quantity) + ({{ std_devs }} * STDDEV(Quantity))
            FROM {{ source('retail_raw', 'retail_sales') }}
        ) THEN 1
        ELSE 0
    END
{% endmacro %}

-- Detect suspicious outliers in price
{% macro is_price_outlier(price, column_name='Total_Sales', std_devs=3) %}
    CASE
        WHEN {{ price }} IS NULL THEN 1
        WHEN {{ price }} < 0 THEN 1
        WHEN {{ price }} > (
            SELECT AVG({{ column_name }}) + ({{ std_devs }} * STDDEV({{ column_name }}))
            FROM {{ source('retail_raw', 'retail_sales') }}
        ) THEN 1
        ELSE 0
    END
{% endmacro %}

-- Calculate price index (product vs category average)
{% macro calculate_price_index(product_price, category_avg_price) %}
    CASE
        WHEN {{ category_avg_price }} > 0
        THEN ROUND(({{ product_price }} / {{ category_avg_price }} * 100), 2)
        ELSE NULL
    END
{% endmacro %}

-- Categorize price positioning
{% macro categorize_price_position(price_index) %}
    CASE
        WHEN {{ price_index }} IS NULL THEN 'unknown'
        WHEN {{ price_index }} < 90 THEN 'discount'
        WHEN {{ price_index }} BETWEEN 90 AND 110 THEN 'at_market'
        WHEN {{ price_index }} > 110 THEN 'premium'
        ELSE 'unknown'
    END
{% endmacro %}

-- Generate a composite key for deduplication
{% macro generate_composite_key(columns) %}
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}
