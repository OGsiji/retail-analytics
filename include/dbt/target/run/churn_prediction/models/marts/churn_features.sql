
  
    

  create  table "postgres"."public_churn_marts"."churn_features__dbt_tmp"
  
  
    as
  
  (
    -- include/dbt/models/marts/churn_features.sql
-- Final mart model for churn prediction features



WITH users_base AS (
    SELECT * FROM "postgres"."public_churn_staging"."stg_users"
),

transactions_agg AS (
    SELECT 
        user_id,
        COUNT(*) AS total_transactions,
        COUNT(CASE WHEN status = 'success' THEN 1 END) AS successful_transactions,
        COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failed_transactions,
        COUNT(CASE WHEN status = 'refunded' THEN 1 END) AS refunded_transactions,
        SUM(CASE WHEN status = 'success' THEN amount_ngn ELSE 0 END) AS total_spend_ngn,
        AVG(CASE WHEN status = 'success' THEN amount_ngn END) AS avg_transaction_amount,
        MIN(CASE WHEN status = 'success' THEN transaction_timestamp END) AS first_transaction_date,
        MAX(CASE WHEN status = 'success' THEN transaction_timestamp END) AS last_transaction_date,
        -- Transaction frequency metrics
        CASE 
            WHEN COUNT(CASE WHEN status = 'success' THEN 1 END) = 0 THEN 0
            ELSE EXTRACT(DAYS FROM MAX(transaction_timestamp) - MIN(transaction_timestamp))::float / 
                 NULLIF(COUNT(CASE WHEN status = 'success' THEN 1 END) - 1, 0)
        END AS avg_days_between_transactions,
        -- Transaction patterns
        COUNT(CASE WHEN transaction_day_type = 'weekend' THEN 1 END) AS weekend_transactions,
        COUNT(CASE WHEN transaction_hour BETWEEN 9 AND 17 THEN 1 END) AS business_hours_transactions,
        -- Success rate
        CASE 
            WHEN COUNT(*) = 0 THEN 0
            ELSE COUNT(CASE WHEN status = 'success' THEN 1 END)::float / COUNT(*)
        END AS transaction_success_rate
    FROM "postgres"."public_churn_staging"."stg_transactions"
    GROUP BY user_id
),

activities_agg AS (
    SELECT 
        user_id,
        COUNT(*) AS total_activity_events,
        COUNT(DISTINCT session_id) AS unique_sessions,
        COUNT(DISTINCT activity_date) AS active_days,
        COUNT(CASE WHEN standardized_event = 'page_view' THEN 1 END) AS page_views,
        COUNT(CASE WHEN standardized_event = 'add_to_cart' THEN 1 END) AS cart_additions,
        COUNT(CASE WHEN standardized_event = 'purchase' THEN 1 END) AS purchase_events,
        COUNT(CASE WHEN standardized_event = 'session_start' THEN 1 END) AS session_starts,
        COUNT(CASE WHEN standardized_event = 'session_end' THEN 1 END) AS session_ends,
        MIN(event_timestamp) AS first_activity_date,
        MAX(event_timestamp) AS last_activity_date,
        -- Device preferences
        COUNT(CASE WHEN device_category = 'mobile' THEN 1 END) AS mobile_activities,
        COUNT(CASE WHEN device_category = 'desktop' THEN 1 END) AS desktop_activities,
        COUNT(CASE WHEN device_category = 'tablet' THEN 1 END) AS tablet_activities,
        -- Session metrics
        AVG(CASE WHEN session_duration_minutes > 0 THEN session_duration_minutes END) AS avg_session_duration_minutes,
        AVG(events_per_session) AS avg_events_per_session,
        -- App version usage
        MAX(major_version) AS latest_major_version_used,
        COUNT(DISTINCT app_version) AS versions_used,
        -- Activity patterns
        COUNT(CASE WHEN activity_hour BETWEEN 6 AND 12 THEN 1 END) AS morning_activities,
        COUNT(CASE WHEN activity_hour BETWEEN 12 AND 18 THEN 1 END) AS afternoon_activities,
        COUNT(CASE WHEN activity_hour BETWEEN 18 AND 24 THEN 1 END) AS evening_activities,
        COUNT(CASE WHEN activity_day_of_week IN (0, 6) THEN 1 END) AS weekend_activities
    FROM "postgres"."public_churn_staging"."stg_activities"
    GROUP BY user_id
),

-- Calculate churn indicators and recency metrics
churn_indicators AS (
    SELECT 
        u.user_id,
        u.email,
        u.signup_date,
        u.standardized_region AS region,
        u.standardized_channel AS channel,
        u.user_tenure_days,
        
        -- Transaction features
        COALESCE(t.total_transactions, 0) AS total_transactions,
        COALESCE(t.successful_transactions, 0) AS successful_transactions,
        COALESCE(t.failed_transactions, 0) AS failed_transactions,
        COALESCE(t.total_spend_ngn, 0) AS total_spend_ngn,
        COALESCE(t.avg_transaction_amount, 0) AS avg_transaction_amount,
        t.first_transaction_date,
        t.last_transaction_date,
        COALESCE(t.avg_days_between_transactions, 0) AS avg_days_between_transactions,
        COALESCE(t.transaction_success_rate, 0) AS transaction_success_rate,
        
        -- Activity features
        COALESCE(a.total_activity_events, 0) AS total_activity_events,
        COALESCE(a.unique_sessions, 0) AS unique_sessions,
        COALESCE(a.active_days, 0) AS active_days,
        COALESCE(a.page_views, 0) AS page_views,
        COALESCE(a.cart_additions, 0) AS cart_additions,
        COALESCE(a.purchase_events, 0) AS purchase_events,
        a.first_activity_date,
        a.last_activity_date,
        COALESCE(a.avg_session_duration_minutes, 0) AS avg_session_duration_minutes,
        COALESCE(a.avg_events_per_session, 0) AS avg_events_per_session,
        
        -- Device preferences (percentages)
        CASE 
            WHEN COALESCE(a.total_activity_events, 0) = 0 THEN 0
            ELSE COALESCE(a.mobile_activities, 0)::float / a.total_activity_events
        END AS mobile_activity_ratio,
        
        CASE 
            WHEN COALESCE(a.total_activity_events, 0) = 0 THEN 0
            ELSE COALESCE(a.desktop_activities, 0)::float / a.total_activity_events
        END AS desktop_activity_ratio,
        
        -- Engagement metrics
        CASE 
            WHEN COALESCE(a.page_views, 0) = 0 THEN 0
            ELSE COALESCE(a.cart_additions, 0)::float / a.page_views
        END AS cart_conversion_rate,
        
        CASE 
            WHEN COALESCE(a.cart_additions, 0) = 0 THEN 0
            ELSE COALESCE(a.purchase_events, 0)::float / a.cart_additions
        END AS purchase_conversion_rate,
        
        -- Recency calculations (most important for churn prediction)
        CURRENT_DATE - COALESCE(a.last_activity_date::date, u.signup_date::date) AS days_since_last_activity,
        CURRENT_DATE - COALESCE(t.last_transaction_date::date, u.signup_date::date) AS days_since_last_transaction,
        
        -- Frequency metrics
        CASE 
            WHEN u.user_tenure_days = 0 THEN 0
            ELSE COALESCE(a.active_days, 0)::float / GREATEST(u.user_tenure_days, 1)
        END AS activity_frequency_rate,
        
        CASE 
            WHEN u.user_tenure_days = 0 THEN 0
            ELSE COALESCE(t.successful_transactions, 0)::float / GREATEST(u.user_tenure_days, 1) * 30
        END AS monthly_transaction_rate,
        
        -- Monetary value
        COALESCE(t.total_spend_ngn, 0) AS monetary_value
        
    FROM users_base u
    LEFT JOIN transactions_agg t ON u.user_id = t.user_id
    LEFT JOIN activities_agg a ON u.user_id = a.user_id
),

-- Final feature engineering and churn flag creation
final_features AS (
    SELECT 
        *,
        
        -- RFM Score Components
        CASE 
            WHEN days_since_last_activity <= 7 THEN 5
            WHEN days_since_last_activity <= 14 THEN 4
            WHEN days_since_last_activity <= 30 THEN 3
            WHEN days_since_last_activity <= 60 THEN 2
            ELSE 1
        END AS recency_score,
        
        CASE 
            WHEN monthly_transaction_rate >= 2 THEN 5
            WHEN monthly_transaction_rate >= 1 THEN 4
            WHEN monthly_transaction_rate >= 0.5 THEN 3
            WHEN monthly_transaction_rate > 0 THEN 2
            ELSE 1
        END AS frequency_score,
        
        CASE 
            WHEN monetary_value >= 100000 THEN 5
            WHEN monetary_value >= 50000 THEN 4
            WHEN monetary_value >= 10000 THEN 3
            WHEN monetary_value >= 1000 THEN 2
            WHEN monetary_value > 0 THEN 1
            ELSE 0
        END AS monetary_score,
        
        -- User lifecycle stage
        CASE 
            WHEN user_tenure_days <= 7 THEN 'new'
            WHEN user_tenure_days <= 30 THEN 'activated'
            WHEN user_tenure_days <= 90 AND total_spend_ngn > 0 THEN 'engaged'
            WHEN total_spend_ngn > 10000 THEN 'loyal'
            WHEN days_since_last_activity <= 30 THEN 'active'
            ELSE 'inactive'
        END AS user_lifecycle_stage,
        
        -- Churn prediction flag
        CASE 
            WHEN days_since_last_activity > 30 AND days_since_last_transaction > 60 THEN 1
            WHEN days_since_last_activity > 60 THEN 1
            WHEN user_tenure_days > 90 AND total_activity_events < 5 THEN 1
            WHEN user_tenure_days > 30 AND total_spend_ngn = 0 AND unique_sessions < 3 THEN 1
            ELSE 0
        END AS churn_flag
    FROM churn_indicators
)

SELECT 
    user_id,
    email,
    signup_date,
    region,
    channel,
    user_tenure_days,
    
    -- Transaction features
    total_transactions,
    successful_transactions,
    total_spend_ngn,
    avg_transaction_amount,
    first_transaction_date,
    last_transaction_date,
    days_since_last_transaction,
    transaction_success_rate,
    monthly_transaction_rate,
    
    -- Activity features
    total_activity_events,
    unique_sessions,
    active_days,
    page_views,
    cart_additions,
    purchase_events,
    first_activity_date,
    last_activity_date,
    days_since_last_activity,
    avg_session_duration_minutes,
    avg_events_per_session,
    activity_frequency_rate,
    
    -- Engagement metrics
    mobile_activity_ratio,
    desktop_activity_ratio,
    cart_conversion_rate,
    purchase_conversion_rate,
    
    -- RFM scores
    recency_score,
    frequency_score,
    monetary_score,
    (recency_score + frequency_score + monetary_score) AS rfm_total_score,
    
    -- Segmentation
    user_lifecycle_stage,
    
    -- Target variable
    churn_flag,
    
    -- Metadata
    CURRENT_TIMESTAMP AS feature_created_at
    
FROM final_features
  );
  