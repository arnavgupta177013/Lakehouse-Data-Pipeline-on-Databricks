"""Create a SparkSession that works both locally and on Databricks."""
import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "lakehouse-pipeline") -> SparkSession:
    # On Databricks the runtime already provides Delta Lake and a configured session.
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return SparkSession.builder.getOrCreate()

    # Local mode: wire in Delta Lake via the delta-spark pip package.
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
