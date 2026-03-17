import time
from pyspark.sql import SparkSession

def compute_pipeline_metrics(spark: SparkSession, silver_path: str, gold_alerts_path: str, quarantine_path: str, start_time: float, end_time: float) -> dict:
    """
    Compute monitoring metrics for a pipeline run.
    Returns a dictionary with metrics.
    """
    # Total transactions processed
    silver_df = spark.read.parquet(silver_path)
    total_transactions_processed = silver_df.count()

    # Fraud alert count
    alerts_df = spark.read.parquet(gold_alerts_path)
    fraud_alert_count = alerts_df.count()

    # Fraud rate
    fraud_rate = fraud_alert_count / total_transactions_processed if total_transactions_processed > 0 else 0.0

    # Invalid records
    quarantine_df = spark.read.parquet(quarantine_path)
    invalid_records = quarantine_df.count()

    # Pipeline runtime in seconds
    pipeline_runtime_seconds = round(end_time - start_time, 2)

    metrics = {
        "total_transactions_processed": total_transactions_processed,
        "fraud_alert_count": fraud_alert_count,
        "fraud_rate": fraud_rate,
        "invalid_records": invalid_records,
        "pipeline_runtime_seconds": pipeline_runtime_seconds
    }
    return metrics
