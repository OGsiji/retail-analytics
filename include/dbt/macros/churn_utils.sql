-- Utility macros for churn prediction

{% macro generate_churn_flag(days_since_last_activity, threshold=30) %}
    CASE 
        WHEN {{ days_since_last_activity }} > {{ threshold }} OR {{ days_since_last_activity }} IS NULL
        THEN 1 
        ELSE 0 
    END
{% endmacro %}

{% macro calculate_rfm_score(value, breakpoints) %}
    CASE 
        {% for i in range(breakpoints|length) %}
        WHEN {{ value }} >= {{ breakpoints[i] }} THEN {{ breakpoints|length - i }}
        {% endfor %}
        ELSE 1
    END
{% endmacro %}

{% macro standardize_region(region_column) %}
    CASE 
        WHEN LOWER(TRIM({{ region_column }})) IN ('lagos', 'lag') THEN 'lagos'
        WHEN LOWER(TRIM({{ region_column }})) IN ('abuja', 'fct', 'abj') THEN 'abuja'
        WHEN LOWER(TRIM({{ region_column }})) IN ('kano', 'kan') THEN 'kano'
        WHEN LOWER(TRIM({{ region_column }})) IN ('port harcourt', 'ph', 'rivers') THEN 'port_harcourt'
        ELSE LOWER(TRIM({{ region_column }}))
    END
{% endmacro %}