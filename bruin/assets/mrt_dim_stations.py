/* @bruin

name: san_cycles_mrt.dim_stations
type: bq.sql

depends:
  - san_cycles_stg.journeys
  - san_cycles_stg.bikepoints

materialization:
  type: table
  strategy: create+replace

connection: san_cycles

description: |
  Dimension table of all known docking stations derived from journey history.
  Most frequently used name per station_id is picked to handle minor name
  changes over the years.
  Borough is joined from stg.bikepoints (TfL BikePoint API + reverse geocoding).
  Note: station IDs have changed over the years — some historical IDs will not
  match the current TfL API and will have a NULL borough.

columns:
  - name: station_id
    description: Unique docking station identifier
    checks:
      - name: not_null
      - name: unique
  - name: station_name
    description: Most common name for this station across all journeys
  - name: borough
    description: London borough (NULL for historical IDs not in current TfL API)

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
    s.station_id,
    s.station_name,
    b.borough
FROM stations s
LEFT JOIN san_cycles_stg.bikepoints b ON s.station_id = b.station_id
