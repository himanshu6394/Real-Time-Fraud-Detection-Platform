from pyspark.sql import DataFrame, Window, functions as F


def detect_fraud_rules(spark, scored_df: DataFrame, gold_alerts_path: str) -> DataFrame:
    """
    Build fraud alerts in Gold layer using:
    - Base risk threshold alerts (MEDIUM/HIGH)
    - Velocity anomaly: >10 txns in 10 minutes
    - Large transaction from first-seen device for user
    - Location anomaly: multiple locations in 1 hour
    """
    window_10min = (
        Window.partitionBy("user_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-600, 0)
    )
    window_1hr = (
        Window.partitionBy("user_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-3600, 0)
    )

    enriched = (
        scored_df
        .withColumn("txn_count_10min", F.count("transaction_id").over(window_10min))
        .withColumn("loc_count_1hr", F.size(F.collect_set("location").over(window_1hr)))
    )

    first_seen_device = enriched.groupBy("user_id", "device_id").agg(
        F.min("timestamp").alias("first_seen_device_ts")
    )
    enriched = enriched.join(first_seen_device, ["user_id", "device_id"], "left")

    base_cols = [
        "transaction_id",
        "user_id",
        "merchant_id",
        "amount",
        "location",
        "device_id",
        "payment_method",
        "timestamp",
        "transaction_status",
        "transaction_velocity",
        "average_transaction_amount",
        "device_switch_frequency",
        "location_change_rate",
        "daily_transaction_count",
        "tv_norm",
        "ds_norm",
        "lr_norm",
        "risk_score",
        "risk_level",
    ]

    medium_high_alerts = (
        enriched
        .filter(F.col("risk_level").isin("MEDIUM", "HIGH"))
        .withColumn("alert_type", F.lit("Risk threshold alert"))
        .select(*base_cols, "alert_type")
    )

    rule1_alerts = (
        enriched
        .filter(F.col("txn_count_10min") > 10)
        .withColumn("alert_type", F.lit("High transaction velocity (10min)"))
        .select(*base_cols, "alert_type")
    )

    rule2_alerts = (
        enriched
        .filter((F.col("amount") > 2000) & (F.col("timestamp") == F.col("first_seen_device_ts")))
        .withColumn("alert_type", F.lit("Large transaction from new device"))
        .select(*base_cols, "alert_type")
    )

    rule3_alerts = (
        enriched
        .filter(F.col("loc_count_1hr") > 1)
        .withColumn("alert_type", F.lit("Multiple locations in 1 hour"))
        .select(*base_cols, "alert_type")
    )

    alerts_df = (
        medium_high_alerts
        .unionByName(rule1_alerts)
        .unionByName(rule2_alerts)
        .unionByName(rule3_alerts)
        .dropDuplicates(["transaction_id", "alert_type"])
    )

    alerts_df.write.mode("overwrite").parquet(gold_alerts_path)
    print(f"Fraud alerts written to Gold layer: {gold_alerts_path}")
    return alerts_df
