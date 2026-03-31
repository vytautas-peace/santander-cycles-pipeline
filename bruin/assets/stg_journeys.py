/* @bruin

name: san_cycles_stg.journeys
type: bq.sql

depends:
  - san_cycles_ing.journeys

materialization:
  type: table
  strategy: create+replace

connection: san_cycles

description: |
  Staging model for Santander Cycles journeys.
  Reads from raw, applies type casting, derived fields,
  and quality checks. Fails if data quality thresholds are breached.
  Missing end_station_id values are backfilled via a name→id lookup
  built from all rows where both fields are present.

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
    description: Arrival docking station ID (backfilled from name lookup where missing)
    checks:
      - name: not_null

quality:
  - name: duration_matches_timestamps
    description: "Duration must match end_date - start_date within 300 seconds"
    query: |
      SELECT COUNT(*) as failing_rows
      FROM san_cycles_stg.journeys
      WHERE ABS(
        TIMESTAMP_DIFF(end_date_utc, start_date_utc, SECOND) - duration_seconds
      ) > 300
    threshold: 0

@bruin */



WITH source AS (
    SELECT * FROM san_cycles_ing.journeys
),

-- Build name→id lookups from all rows where both fields are present.
-- Resolves missing station IDs for files where the column was absent.
-- start_station_id: 229,646 nulls in 21JourneyDataExtract31Aug2016-06Sep2016.csv
--                   (start_station_name is populated — lookup recovers the IDs)
-- end_station_id:   resolved for Sep 2022 schema transition file;
--                   16 rows with both end_station_id AND end_station_name null
--                   are dropped below (incomplete journeys, duration=0)
start_station_id_lookup AS (
    SELECT
        start_station_name,
        MIN(start_station_id) AS start_station_id
    FROM source
    WHERE start_station_id IS NOT NULL
        AND start_station_name IS NOT NULL
    GROUP BY start_station_name
),

end_station_id_lookup AS (
    SELECT
        end_station_name,
        MIN(end_station_id) AS end_station_id
    FROM source
    WHERE end_station_id IS NOT NULL
        AND end_station_name IS NOT NULL
    GROUP BY end_station_name
),

cleaned AS (
    SELECT
        -- Keys
        s.rental_id,
        s.bike_id,

        -- Timestamps
        TIMESTAMP_TRUNC(s.start_date, SECOND)                                        AS start_date_utc,
        TIMESTAMP_TRUNC(s.end_date, SECOND)                                          AS end_date_utc,

        -- Duration
        s.duration                                                                   AS duration_seconds,

        -- Station IDs and names (backfilled from name→id lookup where missing)
        COALESCE(s.start_station_id, slkp.start_station_id)                         AS start_station_id,
        s.start_station_name,
        COALESCE(s.end_station_id, elkp.end_station_id)                             AS end_station_id,
        s.end_station_name,

        -- Derived fields
        DATE(s.start_date)                                                           AS ride_date,
        DATE_TRUNC(DATE(s.start_date), MONTH)                                        AS journey_month,
        EXTRACT(HOUR FROM s.start_date)                                              AS start_hour,
        EXTRACT(DAYOFWEEK FROM s.start_date)                                         AS start_day_of_week,

        -- Round trip flag (uses coalesced IDs)
        COALESCE(s.start_station_id, slkp.start_station_id)
            = COALESCE(s.end_station_id, elkp.end_station_id)                       AS is_round_trip,

        -- Metadata
        s._source_file,
        s._ingested_at

    FROM source s
    LEFT JOIN start_station_id_lookup slkp ON s.start_station_name = slkp.start_station_name
    LEFT JOIN end_station_id_lookup   elkp ON s.end_station_name   = elkp.end_station_name

    WHERE
        s.start_date IS NOT NULL
        AND s.end_date IS NOT NULL
        AND s.duration IS NOT NULL
        AND s.duration > 0
        -- Drop 1,173 journeys with erroneous pre-2012 timestamps (1901–1902 dates
        -- present in source data, likely default values from legacy systems)
        AND EXTRACT(YEAR FROM s.start_date) >= 2012
        -- Drop 16 incomplete journeys with no end location data at all
        -- (end_station_id and end_station_name both null — unrecoverable)
        AND NOT (s.end_station_id IS NULL AND s.end_station_name IS NULL)
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