/* @bruin

name: san_cycles_mrt.kpis_monthly
type: bq.sql

depends:
  - san_cycles_mrt.fct_journeys
  - san_cycles_mrt.station_stats

materialization:
  type: table
  strategy: create+replace

connection: san_cycles

description: |
  Monthly KPI summary. One row per calendar month.

  Includes:
    - Total rides and ride hours
    - Longest single ride (rental_id + duration)
    - Top pickup station (most journeys started)
    - Top dropoff station (most journeys ended)
    - Top bike (most rides)

columns:
  - name: journey_month
    description: First day of the calendar month
    checks:
      - name: not_null
      - name: unique

@bruin */


WITH base AS (
    SELECT
        DATE_TRUNC(start_date_utc, MONTH)         AS journey_month,
        COUNT(*)                                  AS total_journeys,
        ROUND(SUM(duration_seconds) / 3600.0, 1) AS total_journey_hours
    FROM san_cycles_mrt.fct_journeys
    GROUP BY journey_month
),

longest_journey AS (
    SELECT
        DATE_TRUNC(start_date_utc, MONTH) AS journey_month,
        rental_id        AS longest_journey_rental_id,
        duration_seconds AS longest_journey_seconds
    FROM san_cycles_mrt.fct_journeys
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY journey_month
        ORDER BY duration_seconds DESC
    ) = 1
),

top_pickup AS (
    SELECT
        DATE_TRUNC(start_date_utc, MONTH) AS journey_month,
        start_station_id   AS top_pickup_station_id,
        start_station_name AS top_pickup_station_name,
        COUNT(*)           AS top_pickup_count
    FROM san_cycles_mrt.fct_journeys
    WHERE start_station_id IS NOT NULL
    GROUP BY 1, 2, 3
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY journey_month
        ORDER BY COUNT(*) DESC
    ) = 1
),

top_dropoff AS (
    SELECT
        DATE_TRUNC(start_date_utc, MONTH) AS journey_month,
        end_station_id   AS top_dropoff_station_id,
        end_station_name AS top_dropoff_station_name,
        COUNT(*)         AS top_dropoff_count
    FROM san_cycles_mrt.fct_journeys
    WHERE end_station_id IS NOT NULL
    GROUP BY 1, 2, 3
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY journey_month
        ORDER BY COUNT(*) DESC
    ) = 1
),

top_station AS (
    SELECT
        journey_month,
        station_id          AS top_station_id,
        station_name        AS top_station_name,
        SUM(pickups)        AS top_station_pickups,
        SUM(dropoffs)       AS top_station_dropoffs,
        SUM(total_visits)   AS top_station_total_visits
    FROM san_cycles_mrt.station_stats
    GROUP BY 1, 2, 3
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY journey_month
        ORDER BY SUM(total_visits) DESC
    ) = 1
),

top_bike AS (
    SELECT
        DATE_TRUNC(start_date_utc, MONTH) AS journey_month,
        bike_id          AS top_bike_id,
        COUNT(*)         AS top_bike_journeys,
        SUM(duration_seconds) AS top_bike_total_duration_seconds
    FROM san_cycles_mrt.fct_journeys
    WHERE bike_id IS NOT NULL
    GROUP BY 1, 2
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY journey_month
        ORDER BY COUNT(*) DESC
    ) = 1
)

SELECT
    b.journey_month,
    b.total_journeys,
    b.total_journey_hours,
    lr.longest_journey_seconds,
    lr.longest_journey_rental_id,
    tp.top_pickup_station_id,
    tp.top_pickup_station_name,
    tp.top_pickup_count,
    td.top_dropoff_station_id,
    td.top_dropoff_station_name,
    td.top_dropoff_count,
    ts.top_station_id,
    ts.top_station_name,
    ts.top_station_pickups,
    ts.top_station_dropoffs,
    ts.top_station_total_visits,
    tb.top_bike_id,
    tb.top_bike_journeys,
    tb.top_bike_total_duration_seconds

FROM base b
LEFT JOIN longest_journey lr ON b.journey_month = lr.journey_month
LEFT JOIN top_pickup    tp ON b.journey_month = tp.journey_month
LEFT JOIN top_dropoff   td ON b.journey_month = td.journey_month
LEFT JOIN top_station   ts ON b.journey_month = ts.journey_month
LEFT JOIN top_bike      tb ON b.journey_month = tb.journey_month