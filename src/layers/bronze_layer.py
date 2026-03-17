import os
from pyspark.sql import SparkSession

from src.ingestion.upload_handler import ingest_transactions


def run_bronze_layer(
    spark: SparkSession,
    uploaded_dir: str,
    bronze_dir: str,
    min_rows: int,
) -> str:
    """Ingest latest transaction input into Bronze parquet."""
    os.makedirs(uploaded_dir, exist_ok=True)
    os.makedirs(bronze_dir, exist_ok=True)
    return ingest_transactions(
        spark=spark,
        uploaded_dir=uploaded_dir,
        bronze_dir=bronze_dir,
        min_rows=min_rows,
    )
