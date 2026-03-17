import os
import pandas as pd
from pyspark.sql import SparkSession
from src.ingestion.data_generator import generate_synthetic_transactions

def ingest_transactions(spark: SparkSession, uploaded_dir: str, bronze_dir: str, min_rows: int = 50000):
    """
    Ingest transactions from uploaded CSV or generate synthetic data if none found.
    Store raw transactions in the Bronze layer as Parquet files.
    """
    print(f"[DEBUG] Checking for uploaded files in: {uploaded_dir}")
    uploaded_files = [f for f in os.listdir(uploaded_dir) if f.endswith('.csv')]
    print(f"[DEBUG] Uploaded files found: {uploaded_files}")
    if uploaded_files:
        uploaded_files.sort(key=lambda f: os.path.getmtime(os.path.join(uploaded_dir, f)), reverse=True)
        csv_path = os.path.join(uploaded_dir, uploaded_files[0])
        print(f"[DEBUG] Using uploaded dataset: {csv_path}")
        df = pd.read_csv(csv_path)
        print(f"[DEBUG] Uploaded dataset shape: {df.shape}")
    else:
        print("[DEBUG] No uploaded dataset found. Generating synthetic transactions...")
        df = generate_synthetic_transactions(num_rows=min_rows)
        print(f"[DEBUG] Synthetic dataset shape: {df.shape}")

    sdf = spark.createDataFrame(df)
    bronze_path = os.path.join(bronze_dir, "transactions_bronze.parquet")
    print(f"[DEBUG] Writing to Bronze layer: {bronze_path}")
    sdf.write.mode("overwrite").parquet(bronze_path)
    print(f"[DEBUG] Raw transactions written to Bronze layer: {bronze_path}")
    return bronze_path
