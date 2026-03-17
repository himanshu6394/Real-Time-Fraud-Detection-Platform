Gold Layer

Purpose:

- Stores business-ready fraud intelligence outputs.
- Includes engineered features, risk scores, fraud alerts, and pipeline metadata.

Produced by:

- src/layers/gold_layer.py
- src/feature_engineering/fraud_features.py
- src/fraud_rules/risk_scoring.py
- src/fraud_rules/rule_engine.py
- src/metadata/pipeline_metadata.py

Typical artifacts:

- features_gold.parquet
- scored_gold.parquet
- fraud_alerts.parquet
- pipeline_metadata.parquet
