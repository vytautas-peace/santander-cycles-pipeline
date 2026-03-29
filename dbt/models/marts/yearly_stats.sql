{{
  config(
    materialized = 'table',
    labels       = {'layer': 'mart', 'project': 'santander-cycles'}
  )
}}

with rides as (
    select * from {{ ref('fct_rides') }}
)

select
    extract(year from start_date_utc)           as year,
    count(*)                                    as total_rides,
    count(distinct bike_id)                     as unique_bikes,
    count(distinct start_station_id)            as active_stations,
    round(sum(duration_seconds) / 3600.0, 2)   as total_hours,
    round(avg(duration_minutes), 2)             as avg_duration_minutes,
    max(duration_seconds)                       as longest_ride_seconds,
    -- ride of the year
    array_agg(
        struct(rental_id, bike_id, start_station_name, end_station_name, duration_seconds)
        order by duration_seconds desc limit 1
    )[offset(0)]                                as longest_ride
from rides
where start_date_utc is not null
group by year