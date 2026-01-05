-- =====================
-- BRONZE LAYER (Raw Data)
-- =====================
CREATE TABLE IF NOT EXISTS bronze_fuel_prices (
    id SERIAL PRIMARY KEY,
    city_id VARCHAR(100) NOT NULL,
    city_name VARCHAR(100),
    state_id VARCHAR(100) NOT NULL,
    state_name VARCHAR(100),
    country_id VARCHAR(50) DEFAULT 'india',
    applicable_on DATE NOT NULL,
    raw_data JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(city_id, applicable_on)
);

CREATE INDEX IF NOT EXISTS idx_bronze_city_date ON bronze_fuel_prices(city_id, applicable_on);
CREATE INDEX IF NOT EXISTS idx_bronze_state ON bronze_fuel_prices(state_id);

-- =====================
-- SILVER LAYER (Cleaned & Normalized)
-- =====================
CREATE TABLE IF NOT EXISTS silver_fuel_prices (
    id SERIAL PRIMARY KEY,
    city_id VARCHAR(100) NOT NULL,
    city_name VARCHAR(100),
    state_id VARCHAR(100) NOT NULL,
    state_name VARCHAR(100),
    applicable_on DATE NOT NULL,
    fuel_type VARCHAR(20) NOT NULL,  -- petrol, diesel, lpg, cng
    retail_price DECIMAL(10,2),
    price_change DECIMAL(10,2),
    change_interval VARCHAR(20),
    retail_unit VARCHAR(20),
    currency VARCHAR(10) DEFAULT 'INR',
    processed_timestamp TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(city_id, applicable_on, fuel_type)
);

CREATE INDEX IF NOT EXISTS idx_silver_city_fuel ON silver_fuel_prices(city_id, fuel_type);
CREATE INDEX IF NOT EXISTS idx_silver_date ON silver_fuel_prices(applicable_on);
CREATE INDEX IF NOT EXISTS idx_silver_state ON silver_fuel_prices(state_id);

-- =====================
-- GOLD LAYER (Analytics Ready)
-- =====================

-- State-level aggregations
CREATE TABLE IF NOT EXISTS gold_state_analytics (
    id SERIAL PRIMARY KEY,
    state_id VARCHAR(100) NOT NULL,
    state_name VARCHAR(100),
    report_date DATE NOT NULL,
    fuel_type VARCHAR(20) NOT NULL,
    avg_price DECIMAL(10,2),
    min_price DECIMAL(10,2),
    max_price DECIMAL(10,2),
    price_std_dev DECIMAL(10,4),
    city_count INT,
    computed_timestamp TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(state_id, report_date, fuel_type)
);

-- Price trends for ML/forecasting
CREATE TABLE IF NOT EXISTS gold_price_trends (
    id SERIAL PRIMARY KEY,
    city_id VARCHAR(100) NOT NULL,
    city_name VARCHAR(100),
    state_id VARCHAR(100),
    fuel_type VARCHAR(20) NOT NULL,
    trend_start_date DATE,
    trend_end_date DATE,
    days_count INT,
    price_trend_slope DECIMAL(10,4),
    avg_daily_change DECIMAL(10,4),
    total_change DECIMAL(10,2),
    start_price DECIMAL(10,2),
    end_price DECIMAL(10,2),
    computed_timestamp TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_trends_city ON gold_price_trends(city_id, fuel_type);

-- =====================
-- DATA QUALITY TABLE
-- =====================
CREATE TABLE IF NOT EXISTS etl_run_log (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100),
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    layer VARCHAR(20),  -- bronze, silver, gold
    records_processed INT,
    records_failed INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    error_message TEXT
);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
