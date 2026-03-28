-- mart_station_stats.sql
-- Per-station aggregations for dashboard Tile 2 (top stations map/bar chart).
-- Clustered by borough for fast geographic filtering.

{{
  config(
    materialized = 'table',
    cluster_by   = ['borough'],
    labels       = {'layer': 'mart', 'project': 'santander-cycles'}
  )
}}

with rides as (
    select * from {{ ref('fct_rides') }}
),

stations as (
    select * from {{ ref('dim_stations') }}
),

departures as (
    select
        start_station_id      as station_id,
        count(*)              as total_departures,
        round(avg(duration_minutes), 2) as avg_departure_duration_min
    from rides
    group by 1
),

arrivals as (
    select
        end_station_id        as station_id,
        count(*)              as total_arrivals
    from rides
    group by 1
),

final as (
    select
        s.station_id,
        s.station_name,
        s.borough,
        coalesce(d.total_departures, 0)          as total_departures,
        coalesce(a.total_arrivals, 0)            as total_arrivals,
        coalesce(d.total_departures, 0)
          + coalesce(a.total_arrivals, 0)        as total_activity,
        coalesce(d.avg_departure_duration_min, 0) as avg_departure_duration_min,
        -- Net flow: positive = more departures (net source), negative = net sink
        coalesce(d.total_departures, 0)
          - coalesce(a.total_arrivals, 0)        as net_flow
    from stations s
    left join departures d using (station_id)
    left join arrivals   a using (station_id)
)

select * from final
order by total_activity desc
