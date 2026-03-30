/* @bruin

name: san_cycles_mrt.bike_stats
type: bq.sql

depends:
  - san_cycles_mrt.fct_journeys

materialization:
  type: table
  strategy: create+replace

connection: san_cycles

description: |
  Monthly bike-level activity. One row per bike per month.
  Supports the bicycle table KPI and bicycle of the year stats.

columns:
  - name: journey_month
    description: First day of the calendar month
    checks:
      - name: not_null
  - name: bike_id
    description: Bike identifier
    checks:
      - name: not_null

@bruin */


SELECT
    DATE_TRUNC(start_date_utc, MONTH) AS journey_month,
    bike_id,
    COUNT(*)                       AS total_rides,
    SUM(duration_seconds)          AS total_duration_seconds,
    ROUND(AVG(duration_seconds))   AS avg_duration_seconds
FROM san_cycles_mrt.fct_journeys
WHERE bike_id IS NOT NULL
GROUP BY 1, 2