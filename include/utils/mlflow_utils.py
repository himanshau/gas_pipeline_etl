import os
from typing import Dict, Any, Optional
import mlflow


def setup_mlflow(use_local: bool = True):

    # Always use local by default to avoid costs
    if use_local or not os.getenv("DAGSHUB_TOKEN"):
        # Store in project folder for easy access
        local_path = os.path.join(os.path.dirname(__file__), "..", "mlruns")
        os.makedirs(local_path, exist_ok=True)
        
        tracking_uri = f"file:///{local_path.replace(os.sep, '/')}"
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("fuel_price_etl")
        print(f"MLflow LOCAL mode: {tracking_uri}")
        print("View experiments: mlflow ui --backend-store-uri ./include/mlruns")
        return
    
    # Optional: DagsHub remote (only if token is set)
    try:
        import dagshub
        owner = os.getenv("DAGSHUB_USER")
        repo = os.getenv("DAGSHUB_REPO", "gas_pipeline_etl")
        dagshub.init(repo_owner=owner, repo_name=repo, mlflow=True)
        print(f"MLflow REMOTE mode: https://dagshub.com/{owner}/{repo}.mlflow")
    except Exception as e:
        print(f"DagsHub failed, using local: {e}")


def log_etl_metrics(metrics: Dict[str, Any], run_name: str = "fuel_etl_run"):
    # Log ETL pipeline metrics to MLflow
    import mlflow
    
    with mlflow.start_run(run_name=run_name):
        # Log fuel-specific metrics
        for fuel_type, fuel_metrics in metrics.items():
            if isinstance(fuel_metrics, dict):
                for metric_name, value in fuel_metrics.items():
                    if isinstance(value, (int, float)):
                        mlflow.log_metric(f"{fuel_type}_{metric_name}", value)
        
        # Log tags
        mlflow.set_tag("pipeline", "fuel_price_etl")
        mlflow.set_tag("data_source", "rapidapi")


def log_data_quality_metrics(
    bronze_count: int,
    silver_count: int,
    gold_count: int,
    run_id: str
):
    """Log data quality metrics for pipeline monitoring"""
    import mlflow
    
    with mlflow.start_run(run_name=f"etl_quality_{run_id}"):
        mlflow.log_metrics({
            "bronze_records": bronze_count,
            "silver_records": silver_count,
            "gold_records": gold_count,
            "bronze_to_silver_ratio": silver_count / max(bronze_count, 1),
        })
        mlflow.set_tag("run_id", run_id)
