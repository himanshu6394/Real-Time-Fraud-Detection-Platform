from pyspark.sql import DataFrame, functions as F

def compute_risk_score(features_df: DataFrame) -> DataFrame:
    """
    Compute fraud risk score and classification for each transaction.
    risk_score = 0.4 * transaction_velocity + 0.3 * device_switch_frequency + 0.3 * location_change_rate
    """
    # Normalize features to [0, 1] range for scoring
    window = None  # No window needed for min/max
    stats = features_df.agg(
        F.min("transaction_velocity").alias("min_tv"), F.max("transaction_velocity").alias("max_tv"),
        F.min("device_switch_frequency").alias("min_ds"), F.max("device_switch_frequency").alias("max_ds"),
        F.min("location_change_rate").alias("min_lr"), F.max("location_change_rate").alias("max_lr")
    ).collect()[0]

    def normalize(col, min_val, max_val):
        return (F.col(col) - F.lit(min_val)) / (F.lit(max_val - min_val) + F.lit(1e-6))

    norm_df = features_df \
        .withColumn("tv_norm", normalize("transaction_velocity", stats['min_tv'], stats['max_tv'])) \
        .withColumn("ds_norm", normalize("device_switch_frequency", stats['min_ds'], stats['max_ds'])) \
        .withColumn("lr_norm", normalize("location_change_rate", stats['min_lr'], stats['max_lr']))

    # Weighted risk score
    norm_df = norm_df.withColumn(
        "risk_score",
        0.4 * F.col("tv_norm") + 0.3 * F.col("ds_norm") + 0.3 * F.col("lr_norm")
    )

    # Risk classification
    norm_df = norm_df.withColumn(
        "risk_level",
        F.when(F.col("risk_score") < 0.3, "LOW")
         .when(F.col("risk_score") < 0.7, "MEDIUM")
         .otherwise("HIGH")
    )

    return norm_df
