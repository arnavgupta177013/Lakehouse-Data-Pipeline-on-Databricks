from datetime import datetime

import pytest
from pyspark.sql import Row

from src import silver
from src.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests")
    yield session
    session.stop()


def make_row(**overrides):
    base = dict(
        VendorID=1,
        tpep_pickup_datetime=datetime(2024, 1, 1, 8, 0),
        tpep_dropoff_datetime=datetime(2024, 1, 1, 8, 20),
        passenger_count=1,
        trip_distance=3.0,
        PULocationID=10,
        DOLocationID=20,
        payment_type=1,
        fare_amount=15.0,
        tip_amount=3.0,
        total_amount=20.0,
    )
    base.update(overrides)
    return Row(**base)


def test_valid_trip_is_kept_and_enriched(spark):
    valid, rejected = silver.clean(spark.createDataFrame([make_row()]))
    assert rejected.count() == 0
    row = valid.first()
    assert row["trip_duration_min"] == 20.0
    assert row["pickup_hour"] == 8
    assert row["tip_pct"] == 20.0


def test_bad_rows_are_quarantined_with_reason(spark):
    rows = [
        make_row(tpep_dropoff_datetime=datetime(2024, 1, 1, 7, 0)),  # ends before it starts
        make_row(fare_amount=0.0),
        make_row(passenger_count=9),
        make_row(trip_distance=0.0),
    ]
    valid, rejected = silver.clean(spark.createDataFrame(rows))
    reasons = {r["reject_reason"] for r in rejected.collect()}
    assert valid.count() == 0
    assert reasons == {
        "duration_too_short", "bad_fare", "bad_passenger_count", "bad_distance"
    }


def test_exact_duplicates_are_removed(spark):
    valid, _ = silver.clean(spark.createDataFrame([make_row(), make_row()]))
    assert valid.count() == 1
