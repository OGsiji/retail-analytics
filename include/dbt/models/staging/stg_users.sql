-- include/dbt/models/staging/stg_users.sql
-- Staging model for users data - clean and standardize

{{
  config(
    materialized='view',
    schema='churn_staging'
  )
}}

WITH source_users AS (
    SELECT 
        user_id,
        email,
        region,
        signup_channel,
        created_at
    FROM {{ source('public', 'users') }}
),

cleaned_users AS (
    SELECT 
        user_id,
        email,
        created_at::timestamp AS signup_date,
        TRIM(LOWER(region)) AS region,
        TRIM(LOWER(signup_channel)) AS channel,
        created_at,
        -- Calculate user tenure in days
        CURRENT_DATE - created_at::date AS user_tenure_days,
        -- Standardize region names
        CASE 
            WHEN TRIM(LOWER(region)) IN ('lagos', 'lag') THEN 'lagos'
            WHEN TRIM(LOWER(region)) IN ('abuja', 'fct', 'abj') THEN 'abuja'
            WHEN TRIM(LOWER(region)) IN ('kano', 'kan') THEN 'kano'
            WHEN TRIM(LOWER(region)) IN ('port harcourt', 'ph', 'rivers') THEN 'port_harcourt'
            WHEN TRIM(LOWER(region)) IN ('oyo', 'ogun', 'osun') THEN 'southwest'
            WHEN TRIM(LOWER(region)) IN ('anambra', 'enugu', 'imo', 'abia', 'ebonyi') THEN 'southeast'
            WHEN TRIM(LOWER(region)) IN ('edo', 'delta', 'bayelsa', 'cross river', 'akwa ibom') THEN 'southsouth'
            ELSE TRIM(LOWER(region))
        END AS standardized_region,
        -- Standardize channel names
        CASE 
            WHEN TRIM(LOWER(signup_channel)) = 'web' THEN 'web'
            WHEN TRIM(LOWER(signup_channel)) IN ('android', 'ios') THEN 'mobile_app'
            WHEN TRIM(LOWER(signup_channel)) IN ('social', 'facebook', 'instagram', 'twitter') THEN 'social_media'
            WHEN TRIM(LOWER(signup_channel)) IN ('referral', 'ref') THEN 'referral'
            WHEN TRIM(LOWER(signup_channel)) IN ('email', 'newsletter') THEN 'email_marketing'
            ELSE TRIM(LOWER(signup_channel))
        END AS standardized_channel
    FROM source_users
    WHERE user_id IS NOT NULL
    AND created_at IS NOT NULL
)

SELECT * FROM cleaned_users

---

-- include/dbt/models/staging/stg_transactions.sql
-- Staging model for transactions data

{{
  config(
    materialized='view',
    schema='churn_staging'
  )
}}

WITH source_transactions AS (
    SELECT 
        transaction_id,
        user_id,
        amount,
        currency,
        status,
        created_at
    FROM {{ source('public', 'transactions') }}
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

---

-- include/dbt/models/staging/stg_activities.sql
-- Staging model for user activities data

{{
  config(
    materialized='view',
    schema='churn_staging'
  )
}}

WITH source_activities AS (
    SELECT 
        user_id,
        session_id,
        event_name,
        event_timestamp,
        device,
        app_version,
        created_at
    FROM {{ source('churn_analytics', 'raw_user_activities') }}
),

cleaned_activities AS (
    SELECT 
        user_id,
        session_id,
        LOWER(TRIM(event_name)) AS event_name,
        event_timestamp,
        LOWER(TRIM(device)) AS device,
        app_version,
        created_at,
        -- Standardize event names
        CASE 
            WHEN LOWER(TRIM(event_name)) IN ('session_start', 'login', 'app_open') THEN 'session_start'
            WHEN LOWER(TRIM(event_name)) IN ('session_end', 'logout', 'app_close') THEN 'session_end'
            WHEN LOWER(TRIM(event_name)) IN ('page_view', 'screen_view', 'view') THEN 'page_view'
            WHEN LOWER(TRIM(event_name)) IN ('add_to_cart', 'cart_add', 'add_cart') THEN 'add_to_cart'
            WHEN LOWER(TRIM(event_name)) IN ('purchase', 'buy', 'transaction') THEN 'purchase'
            ELSE LOWER(TRIM(event_name))
        END AS standardized_event,
        -- Categorize devices
        CASE 
            WHEN LOWER(TRIM(device)) IN ('phone', 'mobile', 'smartphone') THEN 'mobile'
            WHEN LOWER(TRIM(device)) IN ('tablet', 'ipad') THEN 'tablet'
            WHEN LOWER(TRIM(device)) IN ('desktop', 'computer', 'laptop') THEN 'desktop'
            ELSE 'other'
        END AS device_category,
        -- Extract time components
        DATE(event_timestamp) AS activity_date,
        EXTRACT(HOUR FROM event_timestamp) AS activity_hour,
        EXTRACT(DOW FROM event_timestamp) AS activity_day_of_week,
        -- Parse app version for version analysis
        SPLIT_PART(app_version, '.', 1)::integer AS major_version,
        SPLIT_PART(app_version, '.', 2)::integer AS minor_version
    FROM source_activities
    WHERE user_id IS NOT NULL
    AND session_id IS NOT NULL
    AND event_timestamp IS NOT NULL
),

-- Add session-level calculations
activities_with_session_data AS (
    SELECT 
        *,
        -- Calculate session duration (minutes between session_start and session_end)
        CASE 
            WHEN standardized_event = 'session_end' THEN
                LAG(event_timestamp) OVER (
                    PARTITION BY session_id 
                    ORDER BY event_timestamp
                ) 
        END AS session_start_time,
        -- Count events per session
        COUNT(*) OVER (PARTITION BY session_id) AS events_per_session,
        -- Add row number for deduplication
        ROW_NUMBER() OVER (
            PARTITION BY user_id, session_id, standardized_event, event_timestamp 
            ORDER BY created_at DESC
        ) AS row_num
    FROM cleaned_activities
)

SELECT 
    user_id,
    session_id,
    event_name,
    standardized_event,
    event_timestamp,
    device,
    device_category,
    app_version,
    major_version,
    minor_version,
    activity_date,
    activity_hour,
    activity_day_of_week,
    events_per_session,
    CASE 
        WHEN session_start_time IS NOT NULL THEN
            EXTRACT(EPOCH FROM (event_timestamp - session_start_time)) / 60
        ELSE NULL
    END AS session_duration_minutes,
    created_at
FROM activities_with_session_data
WHERE row_num = 1  -- Remove duplicates