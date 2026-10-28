-- Analytics queries over the gold layer.
-- Run locally with:  python -m src.run_sql
-- On Databricks, replace the delta.`...` paths with your table names.

-- Busiest hours of the day
SELECT pickup_hour, SUM(trips) AS trips
FROM delta.`{gold_hourly}`
GROUP BY pickup_hour
ORDER BY trips DESC
LIMIT 10;

-- Daily revenue and trip trend
SELECT pickup_date, SUM(trips) AS trips, ROUND(SUM(total_revenue), 2) AS revenue
FROM delta.`{gold_hourly}`
GROUP BY pickup_date
ORDER BY pickup_date;

-- Top pickup zones by trip volume
SELECT pickup_zone_id, SUM(trips) AS trips, ROUND(SUM(total_revenue), 2) AS revenue
FROM delta.`{gold_zone_daily}`
GROUP BY pickup_zone_id
ORDER BY trips DESC
LIMIT 10;

-- Why were rows quarantined?
SELECT reject_reason, COUNT(*) AS rows_rejected
FROM delta.`{quarantine}`
GROUP BY reject_reason
ORDER BY rows_rejected DESC;

-- Latest data-quality results
SELECT run_ts, check_name, passed, observed
FROM delta.`{quality_log}`
ORDER BY run_ts DESC
LIMIT 20;
