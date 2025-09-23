

WITH source_transactions AS (
    SELECT 
        transaction_id,
        user_id,
        amount,
        currency,
        status,
        created_at
    FROM "postgres"."public"."transactions"
),

cleaned_transactions AS (
    SELECT 
        transaction_id,
        user_id,
        amount,
        UPPER(TRIM(currency)) AS currency,
        LOWER(TRIM(status)) AS status,
        created_at AS transaction_timestamp,
        -- Convert amount to standard currency (assume NGN base)
        CASE 
            WHEN UPPER(TRIM(currency)) = 'NGN' THEN amount
            WHEN UPPER(TRIM(currency)) = 'USD' THEN amount * 800  -- Approximate conversion
            WHEN UPPER(TRIM(currency)) = 'EUR' THEN amount * 900
            ELSE amount
        END AS amount_ngn,
        -- Categorize transaction amounts
        CASE 
            WHEN amount < 1000 THEN 'micro'
            WHEN amount BETWEEN 1000 AND 10000 THEN 'small'
            WHEN amount BETWEEN 10000 AND 100000 THEN 'medium'
            WHEN amount >= 100000 THEN 'large'
            ELSE 'unknown'
        END AS transaction_category,
        -- Extract date parts for time-based analysis
        DATE(created_at) AS transaction_date,
        EXTRACT(HOUR FROM created_at) AS transaction_hour,
        EXTRACT(DOW FROM created_at) AS transaction_day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM created_at) IN (0, 6) THEN 'weekend'
            ELSE 'weekday'
        END AS transaction_day_type
    FROM source_transactions
    WHERE user_id IS NOT NULL
    AND amount > 0
    AND status IN ('success', 'failed', 'refunded')
)

SELECT * FROM cleaned_transactions