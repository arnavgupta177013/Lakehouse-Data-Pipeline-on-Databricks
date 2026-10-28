"""Run the full pipeline: python -m src.run_pipeline [--steps bronze silver gold quality]"""
import argparse

from src import bronze, gold, quality_checks, silver
from src.spark_session import get_spark

STEPS = {
    "bronze": bronze,
    "silver": silver,
    "gold": gold,
    "quality": quality_checks,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Lakehouse pipeline runner")
    parser.add_argument("--steps", nargs="+", choices=STEPS.keys(), default=list(STEPS))
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    for name in args.steps:
        print(f"\n=== {name.upper()} ===")
        STEPS[name].run(spark)
    print("\nPipeline finished.")


if __name__ == "__main__":
    main()
