-- models/marts/fct_journeys.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Fact table: one row per Santander Cycle journey.
--
-- PARTITIONING: By `start_date` (DAY)
--   Rationale: Upstream dashboard queries are almost always date-range filtered
--   (e.g. "last 30 days", "2023 vs 2024"). Day-level partitioning means BQ
--   only scans the relevant partitions — on a 50M+ row table this reduces
--   query cost from ~$X to ~$0.01 for typical date-bounded queries.
--
-- CLUSTERING: By `start_station_id`, `end_station_id`
--   Rationale: The two most common filter patterns in our dashboards are:
--     1. "Journeys from/to station X" → cluster on start_station_id
--     2. "Flow between station X and Y" → cluster on both IDs
--   BigQuery co-locates rows with the same station IDs within each partition,
--   turning full scans into targeted block reads.
--
-- MATERIALIZATION: Incremental (append-only)
--   New weekly files add rows but never modify historical ones, so we only
--   scan new data on each run using the start_date watermark.
-- ─────────────────────────────────────────────────────────────────────────────

{{
  config(
    materialized           = 'incremental',
    unique_key             = 'journey_id',
    incremental_strategy   = 'merge',
    partition_by = {
      "field": "start_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by             = ['start_station_id', 'end_station_id'],
    description            = 'Core fact table: one row per Santander Cycle journey',
    labels                 = {'layer': 'marts', 'domain': 'cycling'}
  )
}}

WITH journeys AS (
  SELECT * FROM {{ ref('stg_journeys') }}

  {% if is_incremental() %}
    -- On incremental runs, only process data newer than the current max
    -- with a small lookback window to catch late-arriving files
    WHERE start_date > (
      SELECT DATE_SUB(MAX(start_date), INTERVAL {{ var('lookback_days', 7) }} DAY)
      FROM {{ this }}
    )
  {% endif %}
),

stations AS (
  SELECT * FROM {{ ref('dim_stations') }}
),

enriched AS (
  SELECT
    j.journey_id,
    j.duration_seconds,
    j.duration_minutes,
    j.bike_id,
    j.bike_model,

    -- Temporal dimensions
    j.start_datetime,
    j.end_datetime,
    j.start_date,
    j.start_year,
    j.start_month,
    j.start_day_of_week,
    j.start_hour,
    j.is_weekend,

    -- Categorise time of day for dashboards
    CASE
      WHEN j.start_hour BETWEEN 6  AND 9  THEN 'Morning Rush'
      WHEN j.start_hour BETWEEN 10 AND 11 THEN 'Mid Morning'
      WHEN j.start_hour BETWEEN 12 AND 14 THEN 'Lunch'
      WHEN j.start_hour BETWEEN 15 AND 17 THEN 'Afternoon'
      WHEN j.start_hour BETWEEN 17 AND 19 THEN 'Evening Rush'
      WHEN j.start_hour BETWEEN 20 AND 22 THEN 'Evening'
      ELSE 'Night'
    END                                     AS time_of_day_bucket,

    -- Season (Northern Hemisphere)
    CASE
      WHEN j.start_month IN (12, 1, 2)  THEN 'Winter'
      WHEN j.start_month IN (3, 4, 5)   THEN 'Spring'
      WHEN j.start_month IN (6, 7, 8)   THEN 'Summer'
      WHEN j.start_month IN (9, 10, 11) THEN 'Autumn'
    END                                     AS season,

    -- Station references
    j.start_station_id,
    j.start_station_name,
    j.end_station_id,
    j.end_station_name,

    -- Self-loop flag (bike hired and returned to same station)
    (j.start_station_id = j.end_station_id) AS is_round_trip,

    -- Duration bands for histogram / bucketed analyses
    CASE
      WHEN j.duration_minutes < 5   THEN '< 5 min'
      WHEN j.duration_minutes < 10  THEN '5–10 min'
      WHEN j.duration_minutes < 20  THEN '10–20 min'
      WHEN j.duration_minutes < 30  THEN '20–30 min'
      WHEN j.duration_minutes < 60  THEN '30–60 min'
      ELSE '60+ min'
    END                                     AS duration_bucket,

    -- Lineage
    j.source_file,
    CURRENT_TIMESTAMP()                     AS dbt_updated_at

  FROM journeys j
)

SELECT * FROM enriched
