{{
  config(
    materialized = 'table',
    cluster_by   = ['bike_id'],
    labels       = {'layer': 'mart', 'project': 'santander-cycles'}
  )
}}

with rides as (
    select * from {{ ref('fct_rides') }}
)

select
    bike_id,
    count(*)                                    as total_rides,
    round(sum(duration_seconds) / 3600.0, 2)   as total_hours,
    round(avg(duration_minutes), 2)             as avg_duration_minutes,
    max(duration_seconds)                       as longest_ride_seconds,
    min(start_date_utc)                         as first_seen,
    max(start_date_utc)                         as last_seen
from rides
where bike_id is not null
group by bike_id