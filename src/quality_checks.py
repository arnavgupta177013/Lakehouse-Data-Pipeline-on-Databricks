"""Automated data-quality checks on the silver and gold layers.

Every run appends its results to a Delta quality_log table and raises an error if
any check fails, so a bad run can't pass unnoticed.
"""
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, StringType, StructField,
                               StructType)

from src import config

NOT_NULL_COLUMNS = [
    "pickup_ts", "dropoff_ts", "pickup_zone_id", "dropoff_zone_id",
    "trip_distance", "fare_amount", "total_amount", "passenger_count",
]

LOG_SCHEMA = StructType([
    StructField("run_ts", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("passed", BooleanType(), False),
    StructField("observed", DoubleType(), True),
    StructField("expected", StringType(), True),
])


def run(spark: SparkSession, fail_on_error: bool = True) -> None:
    silver = spark.read.format("delta").load(config.SILVER_PATH)
    gold_hourly = spark.read.format("delta").load(config.GOLD_HOURLY_PATH)

    aggs = [F.count("*").alias("row_count")]
    aggs += [F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in NOT_NULL_COLUMNS]
    aggs += [
        F.sum((F.col("trip_distance") <= 0).cast("int")).alias("nonpositive_distance"),
        F.sum((F.col("fare_amount") <= 0).cast("int")).alias("nonpositive_fare"),
        F.sum((F.col("dropoff_ts") <= F.col("pickup_ts")).cast("int")).alias("bad_time_order"),
        F.sum((F.col("avg_speed_mph") > config.MAX_SPEED_MPH).cast("int")).alias("implausible_speed"),
    ]
    stats = {k: (v or 0) for k, v in silver.agg(*aggs).first().asDict().items()}

    row_count = stats["row_count"]
    duplicates = row_count - silver.dropDuplicates(config.DEDUPE_KEYS).count()
    gold_trip_total = gold_hourly.agg(F.sum("trips")).first()[0] or 0

    checks = [("silver_not_empty", row_count > 0, float(row_count), "> 0")]
    for col in NOT_NULL_COLUMNS:
        value = stats[f"null_{col}"]
        checks.append((f"silver_no_nulls_{col}", value == 0, float(value), "== 0"))
    for name in ["nonpositive_distance", "nonpositive_fare", "bad_time_order", "implausible_speed"]:
        checks.append((f"silver_{name}_is_zero", stats[name] == 0, float(stats[name]), "== 0"))
    checks.append(("silver_no_duplicates", duplicates == 0, float(duplicates), "== 0"))
    checks.append((
        "gold_reconciles_with_silver",
        gold_trip_total == row_count,
        float(gold_trip_total - row_count),
        "== 0 (gold trips - silver rows)",
    ))

    run_ts = datetime.utcnow().isoformat(timespec="seconds")
    rows = [(run_ts, name, bool(passed), observed, expected)
            for name, passed, observed, expected in checks]
    log_df = spark.createDataFrame(rows, schema=LOG_SCHEMA)
    log_df.write.format("delta").mode("append").save(config.QUALITY_LOG_PATH)

    failed = [name for name, passed, *_ in checks if not passed]
    print(f"[quality] {len(checks) - len(failed)}/{len(checks)} checks passed")
    for name in failed:
        print(f"[quality] FAILED: {name}")
    if failed and fail_on_error:
        raise RuntimeError(f"Data-quality checks failed: {failed}")
