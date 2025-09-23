# dags/churn_pipeline.py

from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import chain
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType
import pandas as pd
import json
import logging

# dbt integration
try:
    from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
    from cosmos.airflow.task_group import DbtTaskGroup
    from cosmos.constants import LoadMode
    from cosmos.config import RenderConfig
    DBT_AVAILABLE = True
except ImportError:
    DBT_AVAILABLE = False
    logging.warning("dbt/Cosmos not available. Transformation tasks will use plain SQL.")


@dag(
    dag_id='churn_prediction_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['churn', 'ml', 'etl'],
    default_args={
        'owner': 'data-team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='ETL pipeline for customer churn prediction features',
    max_active_runs=1,
)
def churn_prediction_pipeline():
    """
    ## Churn Prediction Data Pipeline
    
    This pipeline:
    1. Creates database schema and tables
    2. Loads raw data from CSV files and SQL dumps
    3. Transforms data into ML-ready features using dbt
    4. Performs data quality checks
    5. Prepares final dataset for API serving
    
    **Data Sources:**
    - users.csv: User profiles and signup information
    - user_activities.csv: User activity events and sessions
    - postgres_transactions_dump.sql: Transaction history
    
    **Output:**
    - churn_features table: ML-ready dataset with churn indicators
    """
    
    # Task 1: Initialize Database Schema
    create_schema = PostgresOperator(
        task_id='create_database_schema',
        postgres_conn_id='postgres_default',
        sql="""
        -- Create schema if not exists
        CREATE SCHEMA IF NOT EXISTS churn_analytics;
        
        -- Create raw data tables
        CREATE TABLE IF NOT EXISTS churn_analytics.raw_users (
            user_id INTEGER,
            signup_date TIMESTAMP,
            region VARCHAR(50),
            channel VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS churn_analytics.raw_user_activities (
            user_id INTEGER,
            session_id VARCHAR(100),
            event_name VARCHAR(50),
            event_timestamp TIMESTAMP,
            device VARCHAR(50),
            app_version VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create staging tables for dbt
        CREATE SCHEMA IF NOT EXISTS churn_staging;
        CREATE SCHEMA IF NOT EXISTS churn_marts;
        """,
    )
    
    # Task 2: Load Transaction Data from SQL Dump
    @task
    def load_transaction_dump():
        """Load transaction data from PostgreSQL dump file"""
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Read and execute the SQL dump file
        try:
            with open('/usr/local/airflow/include/datasets/postgres_transactions_dump.sql', 'r') as file:
                sql_content = file.read()
            
            # Execute the SQL dump
            postgres_hook.run(sql_content)
            logging.info("Successfully loaded transaction data from dump file")
            
            # Get row count for validation
            row_count = postgres_hook.get_first(
                "SELECT COUNT(*) FROM transactions;"
            )[0]
            logging.info(f"Loaded {row_count} transaction records")
            
            return {"transactions_loaded": row_count}
            
        except Exception as e:
            logging.error(f"Failed to load transaction dump: {str(e)}")
            raise
    
    # Task 3: Validate SQL Dump Execution
    @task
    def validate_sql_dump_data():
        """Validate that the SQL dump was executed correctly"""
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        try:
            # Check users table
            users_count = postgres_hook.get_first(
                "SELECT COUNT(*) FROM users;"
            )[0]
            
            # Check transactions table  
            transactions_count = postgres_hook.get_first(
                "SELECT COUNT(*) FROM transactions;"
            )[0]
            
            # Validate data integrity
            if users_count == 0:
                raise ValueError("No users loaded from SQL dump")
            
            if transactions_count == 0:
                raise ValueError("No transactions loaded from SQL dump")
            
            # Check for orphaned transactions
            orphaned_transactions = postgres_hook.get_first("""
                SELECT COUNT(*) FROM transactions t 
                LEFT JOIN users u ON t.user_id = u.user_id 
                WHERE u.user_id IS NULL;
            """)[0]
            
            if orphaned_transactions > 0:
                logging.warning(f"Found {orphaned_transactions} orphaned transactions")
            
            logging.info(f"SQL dump validation passed:")
            logging.info(f"  - Users: {users_count}")
            logging.info(f"  - Transactions: {transactions_count}")
            logging.info(f"  - Orphaned transactions: {orphaned_transactions}")
            
            return {
                "users_loaded": users_count,
                "transactions_loaded": transactions_count,
                "orphaned_transactions": orphaned_transactions
            }
            
        except Exception as e:
            logging.error(f"SQL dump validation failed: {str(e)}")
            raise
    
    # Task 4: Load User Activities CSV Data  
    @task
    def load_activities_csv():
        """Load user activities data from CSV file into PostgreSQL"""
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        try:
            # Read CSV file
            df = pd.read_csv('/usr/local/airflow/include/datasets/user_activities.csv')
            
            # Clean and validate data
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
            df = df.dropna()
            
            # Clear existing data
            postgres_hook.run("TRUNCATE TABLE churn_analytics.raw_user_activities;")
            
            # Insert data in batches
            records = df.to_dict('records')
            insert_sql = """
                INSERT INTO churn_analytics.raw_user_activities 
                (user_id, session_id, event_name, event_timestamp, device, app_version) 
                VALUES %s;
            """
            
            # Insert in batches of 1000
            for i in range(0, len(records), 1000):
                batch = records[i:i+1000]
                values = [(r['user_id'], r['session_id'], r['event_name'], 
                          r['event_timestamp'], r['device'], r['app_version']) 
                         for r in batch]
                postgres_hook.run(insert_sql, parameters=values)
            
            logging.info(f"Successfully loaded {len(df)} activity records")
            return {"activities_loaded": len(df)}
            
        except Exception as e:
            logging.error(f"Failed to load activities CSV: {str(e)}")
            raise
    
    # Task 5: Data Quality Checks
    @task
    def validate_raw_data(activities_result, sql_dump_result):
        """Perform data quality checks on loaded data"""
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        validations = []
        
        try:
            # Check users data (from SQL dump)
            users_count = postgres_hook.get_first(
                "SELECT COUNT(*) FROM users;"
            )[0]
            validations.append(f"Users: {users_count} records")
            
            # Check activities data  
            activities_count = postgres_hook.get_first(
                "SELECT COUNT(*) FROM churn_analytics.raw_user_activities;"
            )[0]
            validations.append(f"Activities: {activities_count} records")
            
            # Check transactions data (from SQL dump)
            transactions_count = postgres_hook.get_first(
                "SELECT COUNT(*) FROM transactions;"
            )[0]
            validations.append(f"Transactions: {transactions_count} records")
            
            # Check for data integrity
            null_users = postgres_hook.get_first(
                "SELECT COUNT(*) FROM users WHERE user_id IS NULL;"
            )[0]
            
            if null_users > 0:
                raise ValueError(f"Found {null_users} users with NULL user_id")
            
            # Check date ranges for users
            date_check = postgres_hook.get_first("""
                SELECT 
                    MIN(created_at) as min_signup,
                    MAX(created_at) as max_signup
                FROM users;
            """)
            
            validations.append(f"User date range: {date_check[0]} to {date_check[1]}")
            
            # Check activity-user alignment
            unmatched_activities = postgres_hook.get_first("""
                SELECT COUNT(*) FROM churn_analytics.raw_user_activities a
                LEFT JOIN users u ON a.user_id = u.user_id
                WHERE u.user_id IS NULL;
            """)[0]
            
            if unmatched_activities > 0:
                logging.warning(f"Found {unmatched_activities} activities for non-existent users")
                validations.append(f"Unmatched activities: {unmatched_activities}")
            
            logging.info("Data validation passed:")
            for validation in validations:
                logging.info(f"  - {validation}")
            
            return {
                "validation_status": "passed",
                "users_count": users_count,
                "activities_count": activities_count,
                "transactions_count": transactions_count,
                "unmatched_activities": unmatched_activities,
                "validations": validations
            }
            
        except Exception as e:
            logging.error(f"Data validation failed: {str(e)}")
            raise
    
    # Task Group: Data Transformations
    @task_group
    def data_transformations():
        """Transform raw data into ML-ready features"""
        
        if DBT_AVAILABLE:
            # Use dbt for transformations
            transform_staging = DbtTaskGroup(
                group_id='staging',
                project_config=DBT_PROJECT_CONFIG,
                profile_config=DBT_CONFIG,
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_LS,
                    select=['path:models/staging']
                )
            )
            
            transform_marts = DbtTaskGroup(
                group_id='marts',
                project_config=DBT_PROJECT_CONFIG,
                profile_config=DBT_CONFIG,
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_LS,
                    select=['path:models/marts']
                )
            )
            
            transform_staging >> transform_marts
        else:
            # Fallback to plain SQL transformations
            create_features_sql = PostgresOperator(
                task_id='create_churn_features',
                postgres_conn_id='postgres_default',
                sql="""
                -- Create churn features table
                DROP TABLE IF EXISTS churn_marts.churn_features;
                CREATE TABLE churn_marts.churn_features AS
                WITH user_stats AS (
                    SELECT 
                        u.user_id,
                        u.signup_date,
                        u.region,
                        u.channel,
                        COALESCE(SUM(CASE WHEN t.status = 'success' THEN t.amount END), 0) as total_spend,
                        COUNT(CASE WHEN t.status = 'success' THEN 1 END) as transaction_count,
                        MAX(t.created_at) as last_transaction_date,
                        MAX(a.event_timestamp) as last_active_date,
                        COUNT(DISTINCT a.session_id) as session_count,
                        CURRENT_DATE - MAX(a.event_timestamp)::date as days_since_last_activity
                    FROM churn_analytics.raw_users u
                    LEFT JOIN transactions t ON u.user_id = t.user_id
                    LEFT JOIN churn_analytics.raw_user_activities a ON u.user_id = a.user_id
                    GROUP BY u.user_id, u.signup_date, u.region, u.channel
                )
                SELECT 
                    *,
                    CASE 
                        WHEN days_since_last_activity > 30 OR days_since_last_activity IS NULL 
                        THEN 1 
                        ELSE 0 
                    END as churn_flag
                FROM user_stats;
                """,
            )
    
    # Task 6: Final Data Quality Check
    @task
    def validate_final_features():
        """Validate the final churn features dataset"""
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        try:
            # Get feature statistics
            stats = postgres_hook.get_first("""
                SELECT 
                    COUNT(*) as total_users,
                    SUM(churn_flag) as churned_users,
                    ROUND(AVG(total_spend), 2) as avg_total_spend,
                    ROUND(AVG(session_count), 2) as avg_session_count
                FROM churn_marts.churn_features;
            """)
            
            total_users, churned_users, avg_spend, avg_sessions = stats
            churn_rate = (churned_users / total_users * 100) if total_users > 0 else 0
            
            logging.info(f"Final dataset statistics:")
            logging.info(f"  - Total users: {total_users}")
            logging.info(f"  - Churned users: {churned_users}")
            logging.info(f"  - Churn rate: {churn_rate:.1f}%")
            logging.info(f"  - Average total spend: ₦{avg_spend}")
            logging.info(f"  - Average session count: {avg_sessions}")
            
            # Validate minimum requirements
            if total_users < 100:
                raise ValueError(f"Insufficient data: only {total_users} users")
            
            if churn_rate > 90:
                logging.warning(f"High churn rate detected: {churn_rate:.1f}%")
            
            return {
                "total_users": total_users,
                "churned_users": int(churned_users),
                "churn_rate": round(churn_rate, 1),
                "avg_total_spend": float(avg_spend) if avg_spend else 0,
                "avg_session_count": float(avg_sessions) if avg_sessions else 0
            }
            
        except Exception as e:
            logging.error(f"Final validation failed: {str(e)}")
            raise
    
    # Task 7: Update API Cache/Refresh
    @task
    def refresh_api_cache():
        """Signal API to refresh its cache of the churn features"""
        import requests
        
        try:
            # Send refresh signal to API
            response = requests.post(
                'http://churn-api:8000/refresh-cache',
                timeout=30
            )
            
            if response.status_code == 200:
                logging.info("API cache refreshed successfully")
                return {"cache_refresh": "success"}
            else:
                logging.warning(f"API cache refresh returned status: {response.status_code}")
                return {"cache_refresh": "warning", "status_code": response.status_code}
                
        except requests.exceptions.RequestException as e:
            logging.warning(f"Could not reach API service: {str(e)}")
            return {"cache_refresh": "api_unavailable"}
    
    # Define task dependencies
    load_transactions = load_transaction_dump()
    validate_dump = validate_sql_dump_data()
    load_activities = load_activities_csv()
    
    validation = validate_raw_data(load_activities, validate_dump)
    transforms = data_transformations()
    final_validation = validate_final_features()
    refresh_cache = refresh_api_cache()
    
    # Set up the pipeline flow
    chain(
        create_schema,
        load_transactions,
        validate_dump,
        load_activities,
        validation,
        transforms,
        final_validation,
        refresh_cache
    )


# Instantiate the DAG
churn_prediction_pipeline()