import uuid
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, Row
from pyspark.sql.utils import AnalysisException
import os

def log_pipeline_run(
    spark: SparkSession,
    metadata_path: str,
    records_processed: int,
    pipeline_status: str,
    data_source: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> str:
    """
    Log metadata for a pipeline run.
    Stores: pipeline_run_id, start_time, end_time, records_processed, pipeline_status, data_source
    """
    pipeline_run_id = str(uuid.uuid4())
    if start_time is None:
        start_time = datetime.now()
    if end_time is None:
        end_time = datetime.now()
    metadata = [
        Row(
            pipeline_run_id=pipeline_run_id,
            start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"),
            end_time=end_time.strftime("%Y-%m-%d %H:%M:%S"),
            records_processed=records_processed,
            pipeline_status=pipeline_status,
            data_source=data_source
        )
    ]
    df = spark.createDataFrame(metadata)
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
    if os.path.exists(metadata_path):
        try:
            existing = spark.read.parquet(metadata_path)
            df = existing.unionByName(df)
        except AnalysisException:
            # Existing metadata path can be empty/corrupt on first local runs.
            pass
    df.write.mode("overwrite").parquet(metadata_path)
    print(f"Pipeline metadata logged to: {metadata_path}")
    return pipeline_run_id
