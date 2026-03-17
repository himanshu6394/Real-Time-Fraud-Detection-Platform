from pyspark.sql import DataFrame, functions as F, Window
import os

def generate_fraud_features(spark, silver_path: str, gold_features_path: str):
    """
    Generate fraud detection features and write to Gold layer.
    Features: transaction_velocity, average_transaction_amount, device_switch_frequency,
    location_change_rate, daily_transaction_count
    """
    df = spark.read.parquet(silver_path)

    # Window for user and time
    user_window = Window.partitionBy("user_id").orderBy(F.col("timestamp").cast("long")).rangeBetween(-3600, 0)  # 1 hour window
    day_window = Window.partitionBy("user_id", F.to_date("timestamp")).orderBy("timestamp")

    # Transaction velocity: count of transactions in last 1 hour
    df = df.withColumn(
        "transaction_velocity",
        F.count("transaction_id").over(user_window)
    )

    # Average transaction amount (per user, per day)
    df = df.withColumn(
        "average_transaction_amount",
        F.avg("amount").over(day_window)
    )

    # Device switch frequency (per user, per day) - PySpark workaround
    device_switch = F.size(F.collect_set("device_id").over(day_window))
    df = df.withColumn("device_switch_frequency", device_switch)

    # Location change rate (per user, per day) - PySpark workaround
    location_change = F.size(F.collect_set("location").over(day_window))
    df = df.withColumn("location_change_rate", location_change)

    # Daily transaction count (per user, per day)
    daily_txn_count = (
        F.count("transaction_id").over(day_window)
    )
    df = df.withColumn("daily_transaction_count", daily_txn_count)

    # Write features to Gold layer
    df.write.mode("overwrite").parquet(gold_features_path)
    print(f"Fraud features written to Gold layer: {gold_features_path}")
    return gold_features_path
