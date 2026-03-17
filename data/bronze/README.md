Bronze Layer

Purpose:

- Stores raw ingested transaction data exactly as received from uploaded or generated sources.
- No business transformations are applied in this layer.

Produced by:

- src/layers/bronze_layer.py
- src/ingestion/upload_handler.py

Typical artifact:

- transactions_bronze.parquet
