# Pytest tests for the pipeline

import pytest
from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark_session
from src.ingestion.data_generator import generate_synthetic_transactions
from src.ingestion.upload_handler import ingest_transactions
from src.transformations.clean_transactions import clean_transactions
from src.feature_engineering.fraud_features import generate_fraud_features
from src.fraud_rules.risk_scoring import compute_risk_score
from src.fraud_rules.rule_engine import detect_fraud_rules
import os
import shutil

@pytest.fixture(scope="session")
def spark():
    return get_spark_session("TestFraudPipeline")

def test_data_ingestion(spark, tmp_path):
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    uploaded_dir = tmp_path / "uploaded"
    uploaded_dir.mkdir()
    bronze_path = ingest_transactions(spark, str(uploaded_dir), str(bronze_dir), min_rows=1000)
    assert os.path.exists(bronze_path)
    df = spark.read.parquet(bronze_path)
    assert df.count() == 1000

def test_cleaning_transformations(spark, tmp_path):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    quarantine_dir = tmp_path / "quarantine"
    bronze_dir.mkdir()
    silver_dir.mkdir()
    quarantine_dir.mkdir()
    # Generate and write synthetic data
    df = generate_synthetic_transactions(1000)
    bronze_path = str(bronze_dir / "bronze.parquet")
    df.to_parquet(bronze_path)
    silver_path = str(silver_dir / "silver.parquet")
    quarantine_path = str(quarantine_dir / "quarantine.parquet")
    clean_transactions(spark, bronze_path, silver_path, quarantine_path)
    silver_df = spark.read.parquet(silver_path)
    assert silver_df.count() > 0
    assert "transaction_id" in silver_df.columns

def test_feature_generation(spark, tmp_path):
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    silver_dir.mkdir()
    gold_dir.mkdir()
    # Generate and write synthetic data
    df = generate_synthetic_transactions(1000)
    silver_path = str(silver_dir / "silver.parquet")
    df.to_parquet(silver_path)
    gold_features_path = str(gold_dir / "features_gold.parquet")
    generate_fraud_features(spark, silver_path, gold_features_path)
    features_df = spark.read.parquet(gold_features_path)
    assert "transaction_velocity" in features_df.columns
    assert features_df.count() > 0

def test_risk_scoring_logic(spark, tmp_path):
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    # Generate and write synthetic features
    df = generate_synthetic_transactions(1000)
    df["transaction_velocity"] = 1
    df["device_switch_frequency"] = 1
    df["location_change_rate"] = 1
    features_path = str(gold_dir / "features_gold.parquet")
    df.to_parquet(features_path)
    features_df = spark.read.parquet(features_path)
    scored_df = compute_risk_score(features_df)
    assert "risk_score" in scored_df.columns
    assert "risk_level" in scored_df.columns


def test_fraud_rule_engine(spark, tmp_path):
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    # Generate and write synthetic scored data
    df = generate_synthetic_transactions(1000)
    df["transaction_velocity"] = 11  # trigger rule 1
    df["device_switch_frequency"] = 1
    df["location_change_rate"] = 2  # trigger rule 3
    df["amount"] = 3000  # trigger rule 2
    df["timestamp"] = "2024-01-01 00:00:00"
    scored_path = str(gold_dir / "scored_gold.parquet")
    df.to_parquet(scored_path)
    scored_df = spark.read.parquet(scored_path)
    gold_alerts_path = str(gold_dir / "fraud_alerts.parquet")
    alerts_df = detect_fraud_rules(spark, scored_df, gold_alerts_path)
    assert alerts_df.count() > 0
    assert "alert_type" in alerts_df.columns

def test_data_quality_checks(spark, tmp_path):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    quarantine_dir = tmp_path / "quarantine"
    bronze_dir.mkdir()
    silver_dir.mkdir()
    quarantine_dir.mkdir()
    # Generate and write synthetic data with potential issues
    df = generate_synthetic_transactions(1000)
    df["amount"] = df["amount"] * -1  # introduce negative amounts
    bronze_path = str(bronze_dir / "bronze.parquet")
    df.to_parquet(bronze_path)
    silver_path = str(silver_dir / "silver.parquet")
    quarantine_path = str(quarantine_dir / "quarantine.parquet")
    clean_transactions(spark, bronze_path, silver_path, quarantine_path)
    # Check that records with negative amounts are sent to quarantine
    quarantine_df = spark.read.parquet(quarantine_path)
    assert quarantine_df.count() > 0
    assert "transaction_id" in quarantine_df.columns

def test_schema_enforcement(spark, tmp_path):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    quarantine_dir = tmp_path / "quarantine"
    bronze_dir.mkdir()
    silver_dir.mkdir()
    quarantine_dir.mkdir()
    # Generate and write synthetic data
    df = generate_synthetic_transactions(1000)
    bronze_path = str(bronze_dir / "bronze.parquet")
    df.to_parquet(bronze_path)
    silver_path = str(silver_dir / "silver.parquet")
    quarantine_path = str(quarantine_dir / "quarantine.parquet")
    # Intentionally alter schema: remove "transaction_id" column
    df_invalid = df.drop("transaction_id", axis=1)
    df_invalid.to_parquet(silver_path)
    # Run cleaning and check that it handles schema evolution
    clean_transactions(spark, bronze_path, silver_path, quarantine_path)
    silver_df = spark.read.parquet(silver_path)
    assert silver_df.count() > 0
    assert "transaction_id" in silver_df.columns

def test_idempotent_ingestion(spark, tmp_path):
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    uploaded_dir = tmp_path / "uploaded"
    uploaded_dir.mkdir()
    # First ingestion
    bronze_path_1 = ingest_transactions(spark, str(uploaded_dir), str(bronze_dir), min_rows=1000)
    df_1 = spark.read.parquet(bronze_path_1)
    initial_count = df_1.count()
    # Duplicate ingestion (idempotent)
    bronze_path_2 = ingest_transactions(spark, str(uploaded_dir), str(bronze_dir), min_rows=1000)
    df_2 = spark.read.parquet(bronze_path_2)
    assert df_1.count() == df_2.count() == initial_count

def test_fraud_rule_engine_multiple_rules(spark, tmp_path):
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    # Generate and write synthetic scored data
    df = generate_synthetic_transactions(1000)
    df["transaction_velocity"] = 11  # trigger rule 1
    df["device_switch_frequency"] = 5  # trigger rule 4
    df["location_change_rate"] = 2  # trigger rule 3
    df["amount"] = 3000  # trigger rule 2
    df["timestamp"] = "2024-01-01 00:00:00"
    scored_path = str(gold_dir / "scored_gold.parquet")
    df.to_parquet(scored_path)
    scored_df = spark.read.parquet(scored_path)
    gold_alerts_path = str(gold_dir / "fraud_alerts.parquet")
    alerts_df = detect_fraud_rules(spark, scored_df, gold_alerts_path)
    assert alerts_df.count() > 0
    assert "alert_type" in alerts_df.columns
    # Check for multiple alerts for the same transaction
    transaction_ids = [row.transaction_id for row in alerts_df.select("transaction_id").distinct().collect()]
    assert len(transaction_ids) > 1

def test_end_to_end_fraud_pipeline(spark, tmp_path):
    uploaded_dir = tmp_path / "uploaded"
    uploaded_dir.mkdir()
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    silver_dir = tmp_path / "silver"
    silver_dir.mkdir()
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    # Ingest data
    bronze_path = ingest_transactions(spark, str(uploaded_dir), str(bronze_dir), min_rows=1000)
    # Clean and transform data
    silver_path = str(silver_dir / "silver.parquet")
    quarantine_path = str(tmp_path / "quarantine" / "quarantine.parquet")
    clean_transactions(spark, bronze_path, silver_path, quarantine_path)
    # Generate features
    gold_features_path = str(gold_dir / "features_gold.parquet")
    generate_fraud_features(spark, silver_path, gold_features_path)
    # Risk scoring
    features_df = spark.read.parquet(gold_features_path)
    scored_df = compute_risk_score(features_df)
    # Fraud detection
    gold_alerts_path = str(gold_dir / "fraud_alerts.parquet")
    alerts_df = detect_fraud_rules(spark, scored_df, gold_alerts_path)
    # Assertions
    assert os.path.exists(gold_alerts_path)
    assert alerts_df.count() > 0
    assert "alert_type" in alerts_df.columns
