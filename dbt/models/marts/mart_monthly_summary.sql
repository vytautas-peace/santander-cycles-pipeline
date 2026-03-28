-- mart_monthly_summary.sql
-- Pre-aggregated monthly metrics for dashboard Tile 1 (rides over time).
-- Partitioned by ride_month so the dashboard query reads only the months
-- actually requested, not the full fact table (~100M+ rows).

{{
  config(
    materialized = 'table',
    partition_by = {
      'field': 'ride_month',
      'data_type': 'date',
      'granularity': 'month'
    },
    labels = {'layer': 'mart', 'project': 'santander-cycles'}
  )
}}

with rides as (
    select * from {{ ref('fct_rides') }}
),

monthly as (

    select
        ride_month,
        count(*)                              as total_rides,
        count(distinct bike_id)               as unique_bikes_used,
        count(distinct start_station_id)      as active_departure_stations,
        count(distinct end_station_id)        as active_arrival_stations,
        round(avg(duration_minutes), 2)       as avg_duration_minutes,
        round(median(duration_minutes), 2)    as median_duration_minutes,
        sum(case when is_round_trip then 1 else 0 end)
                                              as round_trips,
        round(
            safe_divide(
                sum(case when is_round_trip then 1 else 0 end),
                count(*)
            ) * 100, 2
        )                                     as round_trip_pct

    from rides
    group by ride_month

)

select * from monthly
order by ride_month
