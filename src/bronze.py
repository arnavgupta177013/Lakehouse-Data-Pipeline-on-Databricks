"""Bronze layer: land raw trip files in Delta with lineage columns.

Only light standardisation happens here (column selection and type casts, so files
from different months with slightly different schemas can be unioned). No rows are
dropped; all business rules live in the silver layer.
"""
import glob
import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config

BRONZE_COLUMNS = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "tip_amount": "double",
    "total_amount": "double",
}


def read_raw_file(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.parquet(path)
    cols = [F.col(name).cast(dtype).alias(name) for name, dtype in BRONZE_COLUMNS.items()]
    return df.select(*cols).withColumn("source_file", F.lit(os.path.basename(path)))


def run(spark: SparkSession) -> None:
    files = sorted(glob.glob(os.path.join(config.RAW_PATH, "*.parquet")))
    if not files:
        raise FileNotFoundError(
            f"No .parquet files found in {config.RAW_PATH}. See the README for download steps."
        )

    frames = [read_raw_file(spark, f) for f in files]
    bronze = reduce(DataFrame.unionByName, frames).withColumn(
        "ingested_at", F.current_timestamp()
    )

    (
        bronze.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(config.BRONZE_PATH)
    )

    rows = spark.read.format("delta").load(config.BRONZE_PATH).count()
    print(f"[bronze] ingested {len(files)} file(s) -> {rows:,} rows")
