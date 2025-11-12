"""
Retail Analytics Pipeline DAG

This DAG orchestrates the retail analytics pipeline for Bidco Africa analysis:
1. Load CSV data into PostgreSQL
2. Run dbt models to transform data
3. Generate data quality reports
4. Calculate promotion metrics
5. Build pricing index
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

import pandas as pd


# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def load_csv_to_postgres(**context):
    """
    Load retail sales CSV data into PostgreSQL.
    Expects CSV file at /usr/local/airflow/include/datasets/retail_sales.csv
    """
    csv_path = Path('/usr/local/airflow/include/datasets/retail_sales.csv')

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found at {csv_path}")

    # Read CSV
    df = pd.read_csv(csv_path)

    # Clean column names to match expected format
    df.columns = [
        'Store_Name', 'Item_Code', 'Item_Barcode', 'Description',
        'Category', 'Department', 'Sub_Department', 'Section',
        'Quantity', 'Total_Sales', 'RRP', 'Supplier', 'Date_Of_Sale'
    ]

    # Convert date column
    df['Date_Of_Sale'] = pd.to_datetime(df['Date_Of_Sale'])

    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()

    # Load data (replace existing)
    df.to_sql(
        'retail_sales',
        engine,
        schema='public',
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )

    row_count = len(df)
    print(f"Successfully loaded {row_count} rows into retail_sales table")

    return row_count


def validate_data_load(**context):
    """
    Validate that data was loaded correctly
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # Check row count
    row_count = pg_hook.get_first(
        "SELECT COUNT(*) FROM public.retail_sales"
    )[0]

    if row_count == 0:
        raise ValueError("No data found in retail_sales table")

    # Check for required columns
    columns_check = pg_hook.get_first("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = 'retail_sales'
        AND column_name IN (
            'Store_Name', 'Item_Code', 'Quantity',
            'Total_Sales', 'Supplier', 'Date_Of_Sale'
        )
    """)[0]

    if columns_check < 6:
        raise ValueError("Missing required columns in retail_sales table")

    print(f"Data validation passed: {row_count} rows, all required columns present")

    return {'row_count': row_count, 'validation': 'passed'}


def generate_insights_summary(**context):
    """
    Generate summary of key insights for stakeholders
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # Get data quality summary
    dq_summary = pg_hook.get_pandas_df("""
        SELECT dimension, dimension_value, health_rating, reliability_status
        FROM retail_analytics.data_quality_summary
        WHERE dimension = 'Overall Dataset'
    """)

    # Get top promo performers for Bidco
    top_promos = pg_hook.get_pandas_df("""
        SELECT product_description, store_name, promo_uplift_pct, promo_discount_depth_pct
        FROM retail_marts.promo_detection
        WHERE is_bidco = 1 AND is_on_promo = 1
        ORDER BY promo_uplift_pct DESC
        LIMIT 5
    """)

    # Get Bidco pricing position
    price_position = pg_hook.get_pandas_df("""
        SELECT view_level, dominant_positioning, bidco_avg_price_index
        FROM retail_analytics.pricing_summary
        WHERE view_level = 'Overall'
    """)

    insights = {
        'data_quality': dq_summary.to_dict('records'),
        'top_promo_performers': top_promos.to_dict('records'),
        'pricing_position': price_position.to_dict('records')
    }

    print("=" * 80)
    print("RETAIL ANALYTICS INSIGHTS SUMMARY")
    print("=" * 80)
    print(f"\nData Quality: {dq_summary.iloc[0]['health_rating']}")
    print(f"\nTop 5 Bidco Promo Performers:")
    for idx, row in top_promos.iterrows():
        print(f"  {idx+1}. {row['product_description']} @ {row['store_name']}: +{row['promo_uplift_pct']}% uplift")
    print(f"\nBidco Price Positioning: {price_position.iloc[0]['dominant_positioning']}")
    print(f"Average Price Index: {price_position.iloc[0]['bidco_avg_price_index']}")
    print("=" * 80)

    return insights


# DAG definition
with DAG(
    'retail_analytics_pipeline',
    default_args=default_args,
    description='End-to-end retail analytics pipeline for Bidco Africa analysis',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['retail', 'analytics', 'bidco'],
) as dag:

    # Start task
    start = EmptyOperator(task_id='start')

    # Load CSV data
    load_data = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres,
    )

    # Validate data load
    validate_data = PythonOperator(
        task_id='validate_data_load',
        python_callable=validate_data_load,
    )

    # dbt transformation tasks
    dbt_profile_config = ProfileConfig(
        profile_name='retail_analytics',
        target_name='dev',
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id='postgres_default',
            profile_args={
                'schema': 'public',
            },
        ),
    )

    dbt_project_config = ProjectConfig(
        dbt_project_path='/usr/local/airflow/include/dbt',
    )

    dbt_execution_config = ExecutionConfig(
        dbt_executable_path='/usr/local/bin/dbt',
    )

    # dbt task group for retail models
    retail_dbt_models = DbtTaskGroup(
        group_id='retail_dbt_transform',
        project_config=dbt_project_config,
        profile_config=dbt_profile_config,
        execution_config=dbt_execution_config,
        operator_args={
            'install_deps': True,
        },
        select=['path:models/retail'],  # Only run retail models
    )

    # Generate insights summary
    insights = PythonOperator(
        task_id='generate_insights_summary',
        python_callable=generate_insights_summary,
    )

    # End task
    end = EmptyOperator(task_id='end')

    # Define task dependencies
    start >> load_data >> validate_data >> retail_dbt_models >> insights >> end
