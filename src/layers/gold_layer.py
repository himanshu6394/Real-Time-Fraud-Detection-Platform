import os
from pyspark.sql import DataFrame, SparkSession

from src.feature_engineering.fraud_features import generate_fraud_features
from src.fraud_rules.risk_scoring import compute_risk_score
from src.fraud_rules.rule_engine import detect_fraud_rules


def run_gold_layer(
    spark: SparkSession,
    silver_path: str,
    gold_features_path: str,
    gold_alerts_path: str,
) -> tuple[str, str, DataFrame, DataFrame]:
    """Build Gold features, compute risk scores, and write fraud alerts."""
    os.makedirs(os.path.dirname(gold_features_path), exist_ok=True)
    os.makedirs(os.path.dirname(gold_alerts_path), exist_ok=True)

    generate_fraud_features(spark, silver_path, gold_features_path)
    features_df = spark.read.parquet(gold_features_path)
    scored_df = compute_risk_score(features_df)

    alerts_df = detect_fraud_rules(spark, scored_df, gold_alerts_path)
    scored_df.write.mode("overwrite").parquet(
        os.path.join(os.path.dirname(gold_features_path), "scored_gold.parquet")
    )
    return gold_features_path, gold_alerts_path, scored_df, alerts_df
