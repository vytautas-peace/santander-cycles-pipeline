/* @bruin

name: san_cycles_mrt.station_stats
type: bq.sql

depends:
  - san_cycles_mrt.fct_journeys

materialization:
  type: table
  strategy: create+replace

connection: san_cycles

description: |
  Monthly station-level activity. One row per station per month.
  Pickups = journeys starting at the station.
  Dropoffs = journeys ending at the station.
  Total visits = pickups + dropoffs.

columns:
  - name: journey_month
    description: First day of the calendar month
    checks:
      - name: not_null
  - name: station_id
    description: Docking station identifier
    checks:
      - name: not_null
  - name: station_name
    description: Station name at time of journey

@bruin */


WITH pickups AS (
    SELECT
        DATE_TRUNC(start_date_utc, MONTH) AS journey_month,
        start_station_id  AS station_id,
        start_station_name AS station_name,
        COUNT(*)           AS pickups
    FROM san_cycles_mrt.fct_journeys
    WHERE start_station_id IS NOT NULL
    GROUP BY 1, 2, 3
),

dropoffs AS (
    SELECT
        DATE_TRUNC(start_date_utc, MONTH) AS journey_month,
        end_station_id   AS station_id,
        end_station_name AS station_name,
        COUNT(*)         AS dropoffs
    FROM san_cycles_mrt.fct_journeys
    WHERE end_station_id IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    COALESCE(p.journey_month,    d.journey_month)    AS journey_month,
    COALESCE(p.station_id,    d.station_id)    AS station_id,
    COALESCE(p.station_name,  d.station_name)  AS station_name,
    COALESCE(p.pickups,   0)                   AS pickups,
    COALESCE(d.dropoffs,  0)                   AS dropoffs,
    COALESCE(p.pickups, 0) + COALESCE(d.dropoffs, 0) AS total_visits

FROM pickups p
FULL OUTER JOIN dropoffs d
    ON  p.journey_month  = d.journey_month
    AND p.station_id  = d.station_id