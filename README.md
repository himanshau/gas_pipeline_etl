# 🛢️ Fuel Price ETL Pipeline

A production-ready ETL pipeline that fetches daily fuel prices across India and stores them in PostgreSQL using the **Bronze-Silver-Gold** medallion architecture.
---

## 📌 Project Overview

This pipeline **extracts real-time fuel prices** (Petrol, Diesel, LPG, CNG) for 700+ cities in India from RapidAPI, transforms the data through Bronze → Silver → Gold layers, and stores it in PostgreSQL for analytics and ML forecasting.

### Data Source
- **API**: [Daily Fuel Prices India - RapidAPI](https://rapidapi.com/search/fuel%20prices%20india)
- **Update Frequency**: Daily at 6:00 AM IST
- **Coverage**: 700+ cities across all Indian states
- **Fuel Types**: Petrol, Diesel, LPG, CNG

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     ETL PIPELINE FLOW                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   RapidAPI  →  BRONZE (Raw)  →  SILVER (Clean)  →  GOLD (Agg)   │
│                                                                  │
│   Nested        JSONB           Normalized         State-level  │
│   JSON          Storage         Rows               Analytics    │
│                                                                  │
│   1 API call    1 row           4 rows             Aggregates   │
│   per city      per city/day    per city/day       per state    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Layers

| Layer | Table | Records | Description |
|-------|-------|---------|-------------|
| **Bronze** | `bronze_fuel_prices` | 150 | Raw API response stored as JSONB |
| **Silver** | `silver_fuel_prices` | 600 | Flattened - one row per fuel type |
| **Gold** | `gold_state_analytics` | 600 | State-level MIN, MAX, AVG prices |

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop
- Python 3.10+
- Astronomer CLI (`winget install Astronomer.Astro`)

## Project Structure

```
gas_pipeline_etl/
├── dags/
│   └── fuel_pipeline.py          # Main ETL DAG (Bronze→Silver→Gold)
├── include/
│   ├── sql/
│   │   └── schema.sql            # PostgreSQL schema
│   └── utils/
│       ├── api_client.py         # RapidAPI client
│       ├── db_utils.py           # Database operations
│       └── mlflow_utils.py       # MLflow tracking
├── notebooks/
│   └── etl_data_showcase.ipynb   # Data exploration notebook
├── docker-compose.yaml           # PostgreSQL container
├── requirements.txt              # Python dependencies
└── Dockerfile                    # Astro Airflow image
```

---

## 📊 Sample Data

### Bronze Layer (Raw JSONB)
```sql
SELECT city_id, applicable_on, raw_data->'fuel'->'petrol'->>'retailPrice' 
FROM bronze_fuel_prices LIMIT 3;
```
| city_id | applicable_on | petrol_price |
|---------|---------------|--------------|
| mumbai | 2025-08-13 | 103.49 |
| delhi | 2025-08-13 | 94.27 |
| bengaluru | 2025-08-13 | 102.90 |

### Silver Layer (Normalized)
| city_id | fuel_type | retail_price | price_change |
|---------|-----------|--------------|--------------|
| mumbai | petrol | 103.49 | 0.00 |
| mumbai | diesel | 89.88 | 0.00 |
| mumbai | lpg | 772.50 | 0.00 |
| mumbai | cng | 75.00 | 0.00 |

### Gold Layer (Analytics)
| state_name | fuel_type | avg_price | min_price | max_price |
|------------|-----------|-----------|-----------|-----------|
| Delhi | petrol | 94.27 | 94.27 | 94.27 |
| Maharashtra | petrol | 103.49 | 103.49 | 103.49 |
| Karnataka | petrol | 102.90 | 102.90 | 102.90 |

---

## 🛠️ Technologies

- **Apache Airflow** (Astronomer) - Workflow orchestration
- **PostgreSQL 13** - Data warehouse with JSONB support
- **Python 3.10** - ETL logic
- **Docker** - Containerization
- **MLflow** - Experiment tracking (local)

---

## 📈 Future Enhancements

- [ ] Add more cities (currently tracking 5 major metros)
- [ ] ML model for price forecasting
- [ ] Grafana dashboard for visualization
- [ ] Deploy to Astronomer Cloud

---

## 📄 License

MIT License
