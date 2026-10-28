"""Central configuration: paths and data-quality thresholds.

Override the base location with the PIPELINE_BASE_PATH environment variable
(e.g. a Unity Catalog volume path when running on Databricks).
"""
import os

BASE_PATH = os.path.abspath(os.environ.get("PIPELINE_BASE_PATH", "data"))

RAW_PATH = os.path.join(BASE_PATH, "raw")
LAKEHOUSE_PATH = os.path.join(BASE_PATH, "lakehouse")

BRONZE_PATH = os.path.join(LAKEHOUSE_PATH, "bronze_trips")
SILVER_PATH = os.path.join(LAKEHOUSE_PATH, "silver_trips")
QUARANTINE_PATH = os.path.join(LAKEHOUSE_PATH, "silver_rejected_trips")
GOLD_HOURLY_PATH = os.path.join(LAKEHOUSE_PATH, "gold_hourly_demand")
GOLD_ZONE_DAILY_PATH = os.path.join(LAKEHOUSE_PATH, "gold_zone_daily")
QUALITY_LOG_PATH = os.path.join(LAKEHOUSE_PATH, "quality_log")

# --- Silver-layer validation rules ---
MIN_PICKUP_DATE = "2015-01-01"   # stray timestamps (e.g. 2002, 2098) exist in raw data
MAX_PICKUP_DATE = "2030-01-01"
MIN_DURATION_MIN = 1.0
MAX_DURATION_MIN = 180.0
MAX_DISTANCE_MI = 100.0
MAX_SPEED_MPH = 80.0
PASSENGER_RANGE = (1, 6)

# Columns that identify a duplicate trip
DEDUPE_KEYS = [
    "vendor_id", "pickup_ts", "dropoff_ts",
    "pickup_zone_id", "dropoff_zone_id", "total_amount",
]
