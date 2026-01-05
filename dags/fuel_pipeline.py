"""
Fuel Price ETL Pipeline DAG
Bronze → Silver → Gold architecture with MLflow tracking
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Add include path for imports
sys.path.insert(0, "/usr/local/airflow/include")

# =====================
# DAG Configuration
# =====================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# =====================
# Task Functions
# =====================

def extract_to_bronze(**context):
    """
    BRONZE LAYER: Extract raw data from API
    Stores complete API response as JSONB
    """
    from utils.api_client import get_fuel_data_for_cities, DEFAULT_CITIES
    from utils.db_utils import insert_bronze_data, log_etl_run
    
    api_key = os.getenv("RAPIDAPI_KEY")
    run_id = context["run_id"]
    
    print(f"Fetching fuel data for {len(DEFAULT_CITIES)} cities...")
    
    try:
        # Fetch data from API
        data = get_fuel_data_for_cities(api_key, DEFAULT_CITIES)
        
        # Insert into bronze table
        records_count = insert_bronze_data(data)
        
        print(f"Inserted {records_count} records into bronze layer")
        
        # Log success
        log_etl_run(
            run_id=run_id,
            dag_id="fuel_pipeline",
            task_id="extract_to_bronze",
            layer="bronze",
            records_processed=records_count
        )
        
        return records_count
        
    except Exception as e:
        log_etl_run(
            run_id=run_id,
            dag_id="fuel_pipeline",
            task_id="extract_to_bronze",
            layer="bronze",
            records_processed=0,
            status="failed",
            error_message=str(e)
        )
        raise


def transform_to_silver(**context):
    """
    SILVER LAYER: Clean and normalize data
    Flattens nested JSON, creates one row per fuel type
    """
    from utils.db_utils import transform_bronze_to_silver, log_etl_run
    
    run_id = context["run_id"]
    
    print("Transforming bronze data to silver layer...")
    
    try:
        records_count = transform_bronze_to_silver()
        
        print(f"Transformed {records_count} records to silver layer")
        
        log_etl_run(
            run_id=run_id,
            dag_id="fuel_pipeline",
            task_id="transform_to_silver",
            layer="silver",
            records_processed=records_count
        )
        
        return records_count
        
    except Exception as e:
        log_etl_run(
            run_id=run_id,
            dag_id="fuel_pipeline",
            task_id="transform_to_silver",
            layer="silver",
            records_processed=0,
            status="failed",
            error_message=str(e)
        )
        raise


def aggregate_to_gold(**context):
    """
    GOLD LAYER: Create analytics-ready aggregations
    State-level stats and price trends for ML
    """
    from utils.db_utils import (
        aggregate_to_gold_state_analytics,
        compute_price_trends,
        log_etl_run
    )
    
    run_id = context["run_id"]
    
    print("Aggregating silver data to gold layer...")
    
    try:
        # State analytics
        state_records = aggregate_to_gold_state_analytics()
        print(f"Created {state_records} state analytics records")
        
        # Price trends for ML
        trend_records = compute_price_trends()
        print(f"Computed {trend_records} price trend records")
        
        total_records = state_records + trend_records
        
        log_etl_run(
            run_id=run_id,
            dag_id="fuel_pipeline",
            task_id="aggregate_to_gold",
            layer="gold",
            records_processed=total_records
        )
        
        return total_records
        
    except Exception as e:
        log_etl_run(
            run_id=run_id,
            dag_id="fuel_pipeline",
            task_id="aggregate_to_gold",
            layer="gold",
            records_processed=0,
            status="failed",
            error_message=str(e)
        )
        raise


def log_to_mlflow(**context):
    """
    Log pipeline metrics to MLflow (DagsHub)
    Tracks fuel prices and data quality
    """
    from utils.mlflow_utils import setup_mlflow, log_etl_metrics
    from utils.db_utils import get_analytics_summary
    
    run_id = context["run_id"]
    
    print("Logging metrics to MLflow...")
    
    try:
        # Setup MLflow with DagsHub
        setup_mlflow()
        
        # Get latest analytics
        metrics = get_analytics_summary()
        
        # Log to MLflow
        log_etl_metrics(metrics, run_name=f"fuel_etl_{run_id}")
        
        print(f"Logged metrics for {len(metrics)} fuel types")
        
    except Exception as e:
        print(f"MLflow logging failed (non-critical): {e}")
        # Don't fail the DAG for MLflow issues


# =====================
# DAG Definition
# =====================

with DAG(
    dag_id="fuel_price_etl",
    default_args=default_args,
    description="Fuel Price ETL: Bronze → Silver → Gold with MLflow",
    schedule="0 6 * * *",  # Daily at 6 AM (API updates at 6 AM IST)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "fuel", "mlflow"],
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id="start")
    
    # Bronze: Raw data extraction
    bronze_task = PythonOperator(
        task_id="extract_to_bronze",
        python_callable=extract_to_bronze,
    )
    
    # Silver: Data transformation
    silver_task = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_to_silver,
    )
    
    # Gold: Analytics aggregation
    gold_task = PythonOperator(
        task_id="aggregate_to_gold",
        python_callable=aggregate_to_gold,
    )
    
    # MLflow: Metric logging
    mlflow_task = PythonOperator(
        task_id="log_to_mlflow",
        python_callable=log_to_mlflow,
    )
    
    # End marker
    end = EmptyOperator(task_id="end")
    
    # Define task dependencies
    # start → bronze → silver → gold → mlflow → end
    start >> bronze_task >> silver_task >> gold_task >> mlflow_task >> end
