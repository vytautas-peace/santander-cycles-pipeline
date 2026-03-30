/* @bruin

name: san_cycles_stg.journeys_stg
type: bq.sql

depends:
  - san_cycles_raw.journeys_raw

materialization:
  type: table
  strategy: create+replace

connection: san_cycles

description: |
  Staging model for Santander Cycles journeys.
  Reads from raw, applies type casting, derived fields,
  and quality checks. Fails if data quality thresholds are breached.

columns:
  - name: rental_id
    description: Unique journey identifier
    checks:
      - name: not_null
      - name: unique

  - name: start_date_utc
    description: Journey start timestamp (UTC)
    checks:
      - name: not_null

  - name: duration_seconds
    description: Journey duration in seconds
    checks:
      - name: not_null
      - name: positive

  - name: start_station_id
    description: Departure docking station ID
    checks:
      - name: not_null

  - name: end_station_id
    description: Arrival docking station ID
    checks:
      - name: not_null

quality:
  - name: duration_matches_timestamps
    description: "Duration must match end_date - start_date within 300 seconds"
    query: |
      SELECT COUNT(*) as failing_rows
      FROM san_cycles_stg.stg_journeys
      WHERE ABS(
        TIMESTAMP_DIFF(end_date_utc, start_date_utc, SECOND) - duration_seconds
      ) > 300
    threshold: 0

@bruin */



WITH source AS (
    SELECT * FROM san_cycles_raw.journeys_raw
),

cleaned AS (
    SELECT
        -- Keys
        rental_id,
        bike_id,

        -- Timestamps
        TIMESTAMP_TRUNC(start_date, SECOND)     AS start_date_utc,
        TIMESTAMP_TRUNC(end_date, SECOND)       AS end_date_utc,

        -- Duration
        duration                                AS duration_seconds,

        -- Station IDs and names
        start_station_id,
        start_station_name,
        end_station_id,
        end_station_name,

        -- Derived fields
        DATE(start_date)                        AS ride_date,
        DATE_TRUNC(DATE(start_date), MONTH)     AS ride_month,
        EXTRACT(HOUR FROM start_date)           AS start_hour,
        EXTRACT(DAYOFWEEK FROM start_date)      AS start_day_of_week,

        -- Round trip flag
        start_station_id = end_station_id       AS is_round_trip,

        -- Metadata
        _source_file,
        _ingested_at

    FROM source

    WHERE
        start_date IS NOT NULL
        AND end_date IS NOT NULL
        AND duration IS NOT NULL
        AND duration > 0
),

-- Deduplicate: keep most recently ingested version of each rental_id
deduped AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY rental_id
        ORDER BY _ingested_at DESC
    ) = 1
)

SELECT * FROM deduped