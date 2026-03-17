# Pipeline runner script

import argparse
import time
import os
from datetime import datetime
from src.utils.spark_session import get_spark_session
from src.layers.bronze_layer import run_bronze_layer
from src.layers.silver_layer import run_silver_layer
from src.layers.gold_layer import run_gold_layer
from src.metadata.pipeline_metadata import log_pipeline_run
from src.monitoring.pipeline_metrics import compute_pipeline_metrics

DATA_DIR = "data"
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")
UPLOADED_DIR = os.path.join(DATA_DIR, "uploaded")
GENERATED_DIR = os.path.join(DATA_DIR, "generated")
QUARANTINE_PATH = os.path.join(DATA_DIR, "quarantine", "quarantine.parquet")
METADATA_PATH = os.path.join(GOLD_DIR, "pipeline_metadata.parquet")

DEV_ROWS = 50000
FULL_ROWS = 300000


def ensure_directories() -> None:
    for path in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, UPLOADED_DIR, GENERATED_DIR, os.path.dirname(QUARANTINE_PATH)]:
        os.makedirs(path, exist_ok=True)

def main(mode: str):
    ensure_directories()
    spark = get_spark_session()
    start_epoch = time.time()
    start_time = datetime.now()
    print(f"[DEBUG] Pipeline started in {mode.upper()} mode.")

    # 1. Bronze Layer: ingestion
    num_rows = DEV_ROWS if mode == "dev" else FULL_ROWS
    print(f"[DEBUG] Bronze layer ingestion: num_rows={num_rows}")
    bronze_path = run_bronze_layer(
        spark=spark,
        uploaded_dir=UPLOADED_DIR,
        bronze_dir=BRONZE_DIR,
        min_rows=num_rows,
    )
    print(f"[DEBUG] Bronze path: {bronze_path}")

    # 2. Silver Layer: cleaning and validation
    silver_path = os.path.join(SILVER_DIR, "transactions_silver.parquet")
    quarantine_path = QUARANTINE_PATH
    print(f"[DEBUG] Silver layer transform: bronze_path={bronze_path}, silver_path={silver_path}, quarantine_path={quarantine_path}")
    run_silver_layer(
        spark=spark,
        bronze_path=bronze_path,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )
    print(f"[DEBUG] Silver and quarantine written.")

    # 3. Gold Layer: features, risk scoring, alerts
    gold_features_path = os.path.join(GOLD_DIR, "features_gold.parquet")
    gold_alerts_path = os.path.join(GOLD_DIR, "fraud_alerts.parquet")
    print(f"[DEBUG] Gold layer build: silver_path={silver_path}, gold_features_path={gold_features_path}")
    _, _, scored_df, _ = run_gold_layer(
        spark=spark,
        silver_path=silver_path,
        gold_features_path=gold_features_path,
        gold_alerts_path=gold_alerts_path,
    )
    print(f"[DEBUG] Features, scored data, and alerts written to gold.")

    # 4. Metadata logging
    end_epoch = time.time()
    end_time = datetime.now()
    records_processed = scored_df.count()
    pipeline_status = "SUCCESS"
    data_source = "uploaded" if any(f.endswith(".csv") for f in os.listdir(UPLOADED_DIR)) else "synthetic"
    print(f"[DEBUG] Logging pipeline metadata.")
    log_pipeline_run(
        spark,
        METADATA_PATH,
        records_processed,
        pipeline_status,
        data_source,
        start_time=start_time,
        end_time=end_time,
    )
    print(f"[DEBUG] Metadata logged.")

    # 5. Monitoring metrics
    print(f"[DEBUG] Computing pipeline metrics.")
    metrics = compute_pipeline_metrics(
        spark,
        silver_path,
        gold_alerts_path,
        quarantine_path,
        start_epoch,
        end_epoch,
    )
    print("Pipeline metrics:")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
    print("Pipeline completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the fraud detection pipeline.")
    parser.add_argument("--mode", choices=["dev", "full"], default="dev", help="Pipeline mode: dev or full")
    args = parser.parse_args()
    main(args.mode)
