"""
Database Utilities for Fuel Price ETL
PostgreSQL connection and query helpers
"""

import os
import json
import psycopg2
from psycopg2.extras import execute_values, Json
from typing import List, Dict, Any, Optional
from contextlib import contextmanager


def get_db_connection_string() -> str:
    """Get PostgreSQL connection string from environment"""
    return os.getenv(
        "ETL_DATABASE_URL",
        "postgresql://postgres:postgres@host.docker.internal:5433/fuel_prices"
    )


@contextmanager
def get_connection():
    """Context manager for database connections"""
    conn = psycopg2.connect(get_db_connection_string())
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


# =====================
# BRONZE LAYER OPERATIONS
# =====================

def insert_bronze_data(data_list: List[Dict[str, Any]]) -> int:
    """Number of records inserted"""
    query = """
        INSERT INTO bronze_fuel_prices 
        (city_id, city_name, state_id, state_name, applicable_on, raw_data)
        VALUES %s
        ON CONFLICT (city_id, applicable_on) DO UPDATE SET
            raw_data = EXCLUDED.raw_data,
            ingestion_timestamp = NOW()
    """
    
    records = []
    for data in data_list:
        city_id = data.get("cityId")
        city_name = data.get("cityName")
        state_id = data.get("stateId")
        state_name = data.get("stateName")
        
        for history_item in data.get("history", []):
            records.append((
                city_id,
                city_name,
                state_id,
                state_name,
                history_item.get("applicableOn"),
                Json(history_item)
            ))
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, records)
    
    return len(records)


# =====================
# SILVER LAYER OPERATIONS
# =====================

def transform_bronze_to_silver() -> int:
    """Number of records processed"""
    query = """
        INSERT INTO silver_fuel_prices 
        (city_id, city_name, state_id, state_name, applicable_on, 
         fuel_type, retail_price, price_change, change_interval, retail_unit, currency)
        SELECT 
            city_id,
            city_name,
            state_id,
            state_name,
            applicable_on,
            fuel.key as fuel_type,
            (fuel.value->>'retailPrice')::decimal as retail_price,
            (fuel.value->>'retailPriceChange')::decimal as price_change,
            fuel.value->>'retailPriceChangeInterval' as change_interval,
            fuel.value->>'retailUnit' as retail_unit,
            fuel.value->>'currency' as currency
        FROM bronze_fuel_prices,
        LATERAL jsonb_each(raw_data->'fuel') as fuel
        WHERE (raw_data->'fuel') IS NOT NULL
        ON CONFLICT (city_id, applicable_on, fuel_type) DO UPDATE SET
            retail_price = EXCLUDED.retail_price,
            price_change = EXCLUDED.price_change,
            processed_timestamp = NOW()
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.rowcount

def aggregate_to_gold_state_analytics() -> int:
    """Number of records created"""
    query = """
        INSERT INTO gold_state_analytics
        (state_id, state_name, report_date, fuel_type, 
         avg_price, min_price, max_price, price_std_dev, city_count)
        SELECT 
            state_id,
            MAX(state_name) as state_name,
            applicable_on as report_date,
            fuel_type,
            ROUND(AVG(retail_price), 2) as avg_price,
            MIN(retail_price) as min_price,
            MAX(retail_price) as max_price,
            ROUND(STDDEV(retail_price)::numeric, 4) as price_std_dev,
            COUNT(DISTINCT city_id) as city_count
        FROM silver_fuel_prices
        WHERE retail_price IS NOT NULL
        GROUP BY state_id, applicable_on, fuel_type
        ON CONFLICT (state_id, report_date, fuel_type) DO UPDATE SET
            avg_price = EXCLUDED.avg_price,
            min_price = EXCLUDED.min_price,
            max_price = EXCLUDED.max_price,
            price_std_dev = EXCLUDED.price_std_dev,
            city_count = EXCLUDED.city_count,
            computed_timestamp = NOW()
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.rowcount


def compute_price_trends() -> int:
    """Number of trend records created"""
    query = """
        INSERT INTO gold_price_trends
        (city_id, city_name, state_id, fuel_type, 
         trend_start_date, trend_end_date, days_count,
         price_trend_slope, avg_daily_change, total_change,
         start_price, end_price)
        WITH city_trends AS (
            SELECT 
                city_id,
                MAX(city_name) as city_name,
                MAX(state_id) as state_id,
                fuel_type,
                MIN(applicable_on) as trend_start_date,
                MAX(applicable_on) as trend_end_date,
                COUNT(*) as days_count,
                ROUND(REGR_SLOPE(retail_price, EXTRACT(EPOCH FROM applicable_on))::numeric, 6) as price_trend_slope,
                ROUND(AVG(price_change), 4) as avg_daily_change,
                MAX(retail_price) - MIN(retail_price) as total_change,
                (ARRAY_AGG(retail_price ORDER BY applicable_on ASC))[1] as start_price,
                (ARRAY_AGG(retail_price ORDER BY applicable_on DESC))[1] as end_price
            FROM silver_fuel_prices
            WHERE retail_price IS NOT NULL
              AND applicable_on >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY city_id, fuel_type
            HAVING COUNT(*) >= 5  -- Need at least 5 data points
        )
        SELECT * FROM city_trends
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE gold_price_trends")  # Replace trends daily
            cur.execute(query)
            return cur.rowcount


def log_etl_run(run_id: str, dag_id: str, task_id: str, layer: str,
                records_processed: int, records_failed: int = 0,
                status: str = "success", error_message: str = None):
    """Log ETL run metadata"""
    query = """
        INSERT INTO etl_run_log
        (run_id, dag_id, task_id, layer, records_processed, 
         records_failed, start_time, end_time, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), %s, %s)
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (
                run_id, dag_id, task_id, layer,
                records_processed, records_failed, status, error_message
            ))


def get_analytics_summary() -> Dict[str, Any]:
    """Get summary metrics for MLflow logging"""
    query = """
        SELECT 
            fuel_type,
            ROUND(AVG(avg_price), 2) as national_avg,
            MIN(min_price) as national_min,
            MAX(max_price) as national_max,
            SUM(city_count) as total_cities
        FROM gold_state_analytics
        WHERE report_date = (SELECT MAX(report_date) FROM gold_state_analytics)
        GROUP BY fuel_type
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            
    return {
        row[0]: {
            "national_avg": float(row[1]) if row[1] else 0,
            "national_min": float(row[2]) if row[2] else 0,
            "national_max": float(row[3]) if row[3] else 0,
            "total_cities": int(row[4]) if row[4] else 0
        }
        for row in rows
    }
