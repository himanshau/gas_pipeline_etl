# 🛢️ Fuel Price ETL Pipeline + ML Forecasting

ETL pipeline that fetches daily fuel prices across India using **Bronze-Silver-Gold** architecture, with ML models for price prediction.

---

## 📌 Overview

| Component | Description |
|-----------|-------------|
| **Data Source** | RapidAPI - Daily fuel prices for 700+ Indian cities |
| **ETL** | Airflow DAG: Bronze → Silver → Gold layers |
| **ML Models** | Random Forest & LightGBM with hyperparameter tuning |
| **Storage** | PostgreSQL with JSONB support |

---

## 🏗️ Architecture

```
RapidAPI → Bronze (JSONB) → Silver (Normalized) → Gold (Analytics) → ML Model
```

## 🤖 ML Model

**Notebook:** `models/fuel_price_prediction.ipynb`

### Models Trained:
| Model | MAE | RMSE | R² Score |
|-------|-----|------|----------|
| Random Forest | 2.45 | 8.12 | 0.9992 |
| LightGBM | 1.89 | 6.34 | 0.9995 |

### Hyperparameter Tuning (GridSearchCV):
- **Random Forest**: n_estimators, max_depth, min_samples_split
- **LightGBM**: n_estimators, max_depth, learning_rate

### Predictions:
| City | Fuel | Current | Predicted (2026) | Change |
|------|------|---------|------------------|--------|
| Mumbai | Petrol | ₹103.49 | ₹105.23 | +1.68% |
| Delhi | Petrol | ₹94.27 | ₹95.89 | +1.72% |

---

## 📁 Project Structure

```
gas_pipeline_etl/
├── dags/fuel_pipeline.py          # Airflow ETL DAG
├── include/
│   ├── sql/schema.sql             # PostgreSQL schema
│   └── utils/                     # API client, DB utils
├── models/
│   └── fuel_price_prediction.ipynb  # ML training notebook
├── notebooks/
│   └── etl_data_showcase.ipynb    # Data exploration
└── docker-compose.yaml            # PostgreSQL
```

---

## 🚀 Quick Start

### 1. Get RapidAPI Key
1. Go to [RapidAPI - Daily Fuel Prices India](https://rapidapi.com/jainmayank131993/api/daily-petrol-diesel-lpg-cng-fuel-prices-in-india)
2. Sign up for free account
3. Subscribe to the API (free tier available)
4. Copy your API key

### 2. Setup Environment
Create `.env` file in project root:
```bash
RAPIDAPI_KEY=your_api_key_here
```

### 3. Run the Pipeline
```bash
# Start PostgreSQL
docker-compose up -d

# Start Airflow
astro dev start

# Open Airflow UI: http://localhost:8080
# Trigger: fuel_price_etl DAG

# Run ML notebook
jupyter notebook models/fuel_price_prediction.ipynb
```

---

## 🛠️ Tech Stack

| Category | Tools |
|----------|-------|
| Orchestration | Apache Airflow (Astronomer) |
| Database | PostgreSQL 13 |
| ML | scikit-learn, LightGBM |
| Visualization | Plotly |
| Tracking | MLflow |

---

## 📄 License

MIT License
