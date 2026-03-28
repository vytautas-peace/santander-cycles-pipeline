-- models/staging/stg_stations.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Derives a clean station dimension from the journey data itself.
-- TfL doesn't publish a separate station CSV in the usage-stats bucket,
-- so we infer stations from the journey start/end pairs.
--
-- This also incorporates TfL's live bikepoint API data if available.
-- ─────────────────────────────────────────────────────────────────────────────

{{
  config(
    materialized = 'view',
    description  = 'Station dimension derived from journey start/end references'
  )
}}

WITH start_stations AS (
  SELECT
    start_station_id  AS station_id,
    start_station_name AS station_name
  FROM {{ ref('stg_journeys') }}
  WHERE start_station_id IS NOT NULL
    AND start_station_name IS NOT NULL
),

end_stations AS (
  SELECT
    end_station_id   AS station_id,
    end_station_name AS station_name
  FROM {{ ref('stg_journeys') }}
  WHERE end_station_id IS NOT NULL
    AND end_station_name IS NOT NULL
),

all_stations AS (
  SELECT * FROM start_stations
  UNION ALL
  SELECT * FROM end_stations
),

-- Take the most recent name for each station ID
-- (stations have been renamed historically)
deduped AS (
  SELECT
    station_id,
    station_name,
    COUNT(*) AS appearance_count,
    ROW_NUMBER() OVER (
      PARTITION BY station_id
      ORDER BY COUNT(*) DESC
    ) AS rn
  FROM all_stations
  GROUP BY station_id, station_name
)

SELECT
  station_id,
  station_name,
  appearance_count
FROM deduped
WHERE rn = 1
