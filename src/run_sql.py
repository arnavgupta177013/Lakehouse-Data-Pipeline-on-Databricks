"""Execute sql/queries.sql against the gold layer and print the results."""
import os

from src import config
from src.spark_session import get_spark

SQL_FILE = os.path.join(os.path.dirname(__file__), "..", "sql", "queries.sql")


def main() -> None:
    spark = get_spark("lakehouse-sql")
    spark.sparkContext.setLogLevel("WARN")

    with open(SQL_FILE, encoding="utf-8") as f:
        text = f.read().format(
            gold_hourly=config.GOLD_HOURLY_PATH,
            gold_zone_daily=config.GOLD_ZONE_DAILY_PATH,
            quarantine=config.QUARANTINE_PATH,
            quality_log=config.QUALITY_LOG_PATH,
        )

    for statement in text.split(";"):
        lines = [ln for ln in statement.splitlines() if not ln.strip().startswith("--")]
        query = "\n".join(lines).strip()
        if query:
            print(f"\n>>> {query.splitlines()[0]} ...")
            spark.sql(query).show(truncate=False)


if __name__ == "__main__":
    main()
