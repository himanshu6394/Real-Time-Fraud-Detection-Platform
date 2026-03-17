from pyspark.sql import SparkSession
import os
import sys

def get_spark_session(app_name: str = "FraudDetectionPipeline") -> SparkSession:
    """
    Create and return a SparkSession configured for local laptop execution.
    Optimized for 8GB RAM, PySpark 3.5.1, and medallion architecture pipeline.
    """
    python_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .getOrCreate()
    )
    return spark
