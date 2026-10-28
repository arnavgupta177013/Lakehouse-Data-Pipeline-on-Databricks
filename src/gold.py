"""Gold layer: business-ready aggregates built from silver."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config


def run(spark: SparkSession) -> None:
    silver = spark.read.format("delta").load(config.SILVER_PATH)

    hourly = silver.groupBy("pickup_date", "pickup_hour").agg(
        F.count("*").alias("trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
    )

    zone_daily = silver.groupBy("pickup_zone_id", "pickup_date").agg(
        F.count("*").alias("trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    )

    for df, path, name in [
        (hourly, config.GOLD_HOURLY_PATH, "gold_hourly_demand"),
        (zone_daily, config.GOLD_ZONE_DAILY_PATH, "gold_zone_daily"),
    ]:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        print(f"[gold] wrote {name}: {spark.read.format('delta').load(path).count():,} rows")
