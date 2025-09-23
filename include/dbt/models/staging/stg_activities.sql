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