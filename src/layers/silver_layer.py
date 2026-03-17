import os
from pyspark.sql import SparkSession

from src.transformations.clean_transactions import clean_transactions


def run_silver_layer(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
) -> tuple[str, str]:
    """Apply schema and quality checks, then write Silver and Quarantine outputs."""
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    os.makedirs(os.path.dirname(quarantine_path), exist_ok=True)
    return clean_transactions(
        spark=spark,
        bronze_path=bronze_path,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )
