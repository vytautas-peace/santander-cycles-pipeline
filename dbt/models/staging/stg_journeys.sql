-- models/staging/stg_journeys.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Staging model: cleans and types the raw journey data from GCS Parquet.
--
-- Source: BigQuery external table over GCS Parquet files
--   (partitioned by start_year / start_month)
--
-- This model:
--   • Casts all columns to correct types
--   • Deduplicates on journey_id (TfL files occasionally overlap week boundaries)
--   • Filters out records with null start timestamps or impossible durations
--   • Standardises station names (trim, title-case)
-- ─────────────────────────────────────────────────────────────────────────────

{{
  config(
    materialized = 'view',
    description  = 'Cleaned and typed journey records from TfL raw data'
  )
}}

WITH raw AS (
  SELECT
    CAST(journey_id       AS INT64)     AS journey_id,
    CAST(duration_seconds AS INT64)     AS duration_seconds,
    CAST(bike_id          AS INT64)     AS bike_id,
    CAST(start_datetime   AS TIMESTAMP) AS start_datetime,
    CAST(end_datetime     AS TIMESTAMP) AS end_datetime,
    CAST(start_station_id AS INT64)     AS start_station_id,
    CAST(end_station_id   AS INT64)     AS end_station_id,
    INITCAP(TRIM(start_station_name))   AS start_station_name,
    INITCAP(TRIM(end_station_name))     AS end_station_name,
    NULLIF(TRIM(bike_model), '')        AS bike_model,
    source_file,
    start_year,
    start_month
  FROM {{ source('raw', 'raw_journeys') }}
  WHERE
    start_datetime IS NOT NULL
    AND start_year  IS NOT NULL
    AND start_month IS NOT NULL
),

deduped AS (
  -- TfL weekly files overlap at boundaries — keep the first occurrence
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY journey_id
      ORDER BY start_datetime
    ) AS rn
  FROM raw
  WHERE journey_id IS NOT NULL
),

validated AS (
  SELECT * FROM deduped
  WHERE
    rn              = 1
    AND duration_seconds  > 0
    AND duration_seconds  < 86400   -- cap at 24 hours (extreme outliers)
    AND start_station_id IS NOT NULL
    AND end_station_id   IS NOT NULL
)

SELECT
  journey_id,
  duration_seconds,
  ROUND(duration_seconds / 60.0, 2)              AS duration_minutes,
  bike_id,
  start_datetime,
  end_datetime,
  DATE(start_datetime)                            AS start_date,
  EXTRACT(YEAR  FROM start_datetime)              AS start_year,
  EXTRACT(MONTH FROM start_datetime)              AS start_month,
  EXTRACT(DAYOFWEEK FROM start_datetime)          AS start_day_of_week,  -- 1=Sun, 7=Sat
  EXTRACT(HOUR  FROM start_datetime)              AS start_hour,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM start_datetime) IN (1, 7) THEN TRUE
    ELSE FALSE
  END                                             AS is_weekend,
  start_station_id,
  end_station_id,
  start_station_name,
  end_station_name,
  bike_model,
  source_file
FROM validated
