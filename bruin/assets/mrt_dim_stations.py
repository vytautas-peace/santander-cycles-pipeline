/* @bruin

name: san_cycles_mrt.dim_stations
type: bq.sql

depends:
  - san_cycles_stg.journeys

materialization:
  type: table
  strategy: create+replace

connection: san_cycles

description: |
  Dimension table of all known docking stations derived from journey history.
  Most frequently used name per station_id is picked to handle minor name
  changes over the years.

columns:
  - name: station_id
    description: Unique docking station identifier
    checks:
      - name: not_null
      - name: unique
  - name: station_name
    description: Most common name for this station across all journeys

@bruin */


WITH all_stations AS (
    SELECT start_station_id AS station_id, start_station_name AS station_name
    FROM san_cycles_stg.journeys
    WHERE start_station_id IS NOT NULL AND start_station_name IS NOT NULL

    UNION ALL

    SELECT end_station_id AS station_id, end_station_name AS station_name
    FROM san_cycles_stg.journeys
    WHERE end_station_id IS NOT NULL AND end_station_name IS NOT NULL
),

name_counts AS (
    SELECT
        station_id,
        station_name,
        COUNT(*) AS n
    FROM all_stations
    GROUP BY station_id, station_name
),

-- Most frequently used name per station_id
stations AS (
    SELECT
        station_id,
        station_name
    FROM name_counts
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY station_id
        ORDER BY n DESC
    ) = 1
)

SELECT
    station_id,
    station_name
FROM stations
