
  create view "postgres"."public_churn_staging"."stg_users__dbt_tmp"
    
    
  as (
    -- include/dbt/models/staging/stg_users.sql
-- Staging model for users data - clean and standardize



WITH source_users AS (
    SELECT 
        user_id,
        email,
        region,
        signup_channel,
        created_at
    FROM "postgres"."public"."users"
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
  );