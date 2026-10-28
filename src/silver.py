"""Silver layer: clean, validate and enrich bronze data.

Rows that fail a rule are NOT silently dropped: they go to a quarantine Delta table
with a reject_reason, so data loss is always visible and auditable.
"""
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config


def clean(bronze: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Return (valid_trips, rejected_trips). Pure function so it is easy to unit test."""
    df = (
        bronze.withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_ts")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_ts")
        .withColumnRenamed("PULocationID", "pickup_zone_id")
        .withColumnRenamed("DOLocationID", "dropoff_zone_id")
    )

    duration_min = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0
    df = df.withColumn("trip_duration_min", F.round(duration_min, 2))
    df = df.withColumn(
        "avg_speed_mph",
        F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60.0), 2),
    )

    lo, hi = config.PASSENGER_RANGE
    reason = (
        F.when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), "null_timestamp")
        .when(
            (F.col("pickup_ts") < F.lit(config.MIN_PICKUP_DATE).cast("timestamp"))
            | (F.col("pickup_ts") >= F.lit(config.MAX_PICKUP_DATE).cast("timestamp")),
            "pickup_out_of_range",
        )
        .when(F.col("trip_duration_min") < config.MIN_DURATION_MIN, "duration_too_short")
        .when(F.col("trip_duration_min") > config.MAX_DURATION_MIN, "duration_too_long")
        .when(
            F.col("trip_distance").isNull()
            | (F.col("trip_distance") <= 0)
            | (F.col("trip_distance") > config.MAX_DISTANCE_MI),
            "bad_distance",
        )
        .when(F.col("avg_speed_mph") > config.MAX_SPEED_MPH, "implausible_speed")
        .when(F.col("fare_amount").isNull() | (F.col("fare_amount") <= 0), "bad_fare")
        .when(F.col("total_amount").isNull() | (F.col("total_amount") <= 0), "bad_total")
        .when(
            F.col("passenger_count").isNull()
            | ~(F.col("passenger_count").between(lo, hi)),
            "bad_passenger_count",
        )
        .when(
            F.col("pickup_zone_id").isNull() | F.col("dropoff_zone_id").isNull(),
            "null_zone",
        )
    )

    flagged = df.withColumn("reject_reason", reason)
    rejected = flagged.filter(F.col("reject_reason").isNotNull())

    valid = (
        flagged.filter(F.col("reject_reason").isNull())
        .drop("reject_reason")
        .dropDuplicates(config.DEDUPE_KEYS)
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
        .withColumn(
            "tip_pct", F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2)
        )
    )
    return valid, rejected


def run(spark: SparkSession) -> None:
    bronze = spark.read.format("delta").load(config.BRONZE_PATH)
    valid, rejected = clean(bronze)

    (
        valid.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("pickup_month")
        .save(config.SILVER_PATH)
    )
    (
        rejected.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(config.QUARANTINE_PATH)
    )

    bronze_rows = bronze.count()
    silver_rows = spark.read.format("delta").load(config.SILVER_PATH).count()
    rejected_df = spark.read.format("delta").load(config.QUARANTINE_PATH)
    rejected_rows = rejected_df.count()
    duplicates = bronze_rows - silver_rows - rejected_rows

    print(f"[silver] bronze={bronze_rows:,} | valid={silver_rows:,} | "
          f"quarantined={rejected_rows:,} | duplicates removed={duplicates:,}")
    print("[silver] rejection reasons:")
    rejected_df.groupBy("reject_reason").count().orderBy(F.desc("count")).show(truncate=False)
