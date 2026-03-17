from pyspark.sql import DataFrame, functions as F, types as T
import os

def validate_schema(df: DataFrame) -> bool:
    required_fields = [
        ("transaction_id", T.StringType()),
        ("user_id", T.StringType()),
        ("merchant_id", T.StringType()),
        ("amount", T.DoubleType()),
        ("location", T.StringType()),
        ("device_id", T.StringType()),
        ("payment_method", T.StringType()),
        ("timestamp", T.TimestampType()),
        ("transaction_status", T.StringType()),
    ]
    for name, dtype in required_fields:
        if name not in df.columns:
            print(f"[DEBUG] Missing column: {name}")
            return False
    return True

def clean_transactions(spark, bronze_path: str, silver_path: str, quarantine_path: str):
    print(f"[DEBUG] Reading bronze data from: {bronze_path}")
    df = spark.read.parquet(bronze_path)
    print(f"[DEBUG] Bronze schema: {df.printSchema()}")
    print(f"[DEBUG] Bronze row count: {df.count()}")

    # Remove duplicates
    df = df.dropDuplicates(["transaction_id"])
    print(f"[DEBUG] After deduplication: {df.count()}")

    # Timestamp normalization
    df = df.withColumn(
        "timestamp",
        F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")
    )

    # Null value handling: quarantine rows with any nulls in required fields
    required_cols = [
        "transaction_id", "user_id", "merchant_id", "amount", "location",
        "device_id", "payment_method", "timestamp", "transaction_status"
    ]
    valid_df = df.dropna(subset=required_cols)
    quarantine_df = df.subtract(valid_df)
    print(f"[DEBUG] Valid rows after null drop: {valid_df.count()}")
    print(f"[DEBUG] Quarantine rows after null drop: {quarantine_df.count()}")

    # Schema validation (basic)
    if not validate_schema(valid_df):
        print("[ERROR] Schema validation failed for valid records.")
        raise ValueError("Schema validation failed for valid records.")

    # Transaction amount validation: quarantine negative or zero amounts
    valid_df = valid_df.filter(F.col("amount") > 0)
    quarantine_df = quarantine_df.unionByName(df.filter(F.col("amount") <= 0))
    print(f"[DEBUG] Valid rows after amount check: {valid_df.count()}")
    print(f"[DEBUG] Quarantine rows after amount check: {quarantine_df.count()}")

    # Write valid records to Silver layer
    print(f"[DEBUG] Writing valid records to Silver: {silver_path}")
    valid_df.write.mode("overwrite").parquet(silver_path)
    # Write invalid records to quarantine
    print(f"[DEBUG] Writing invalid records to Quarantine: {quarantine_path}")
    quarantine_df.write.mode("overwrite").parquet(quarantine_path)
    print(f"[DEBUG] Cleaned transactions written to Silver layer: {silver_path}")
    print(f"[DEBUG] Invalid records written to quarantine: {quarantine_path}")
    return silver_path, quarantine_path
