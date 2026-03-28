-- dim_stations.sql
-- Station dimension derived from ride data (no external lookup needed).
-- Uses the most-recent name observed for each station ID.
-- Clustered by borough (extracted from name suffix).
-- WHY clustering by borough? Dashboards filter/group by borough heavily;
-- clustering eliminates full scans for geographic slices.

{{
  config(
    materialized = 'table',
    cluster_by   = ['borough'],
    labels       = {'layer': 'mart', 'project': 'santander-cycles'}
  )
}}

with starts as (
    select
        start_station_id  as station_id,
        start_station_name as station_name,
        start_date_utc    as observed_at
    from {{ ref('stg_rides') }}
    where start_station_id is not null
),

ends as (
    select
        end_station_id    as station_id,
        end_station_name  as station_name,
        end_date_utc      as observed_at
    from {{ ref('stg_rides') }}
    where end_station_id is not null
),

all_stations as (
    select * from starts
    union all
    select * from ends
),

-- Keep the most-recently-observed name per station_id
ranked as (
    select
        station_id,
        station_name,
        row_number() over (partition by station_id order by observed_at desc) as rn
    from all_stations
    where station_name is not null
      and station_name not in ('nan', 'None', '')
),

deduped as (
    select station_id, station_name
    from ranked
    where rn = 1
),

-- Extract borough from station name pattern "Name, Borough"
with_borough as (
    select
        station_id,
        station_name,
        case
            when station_name like '%, %'
                then trim(split(station_name, ', ')[safe_offset(
                    array_length(split(station_name, ', ')) - 1
                )])
            else 'Unknown'
        end as borough
    from deduped
),

-- Ride counts for station ranking
ride_counts as (
    select
        start_station_id as station_id,
        count(*)         as total_departures
    from {{ ref('stg_rides') }}
    group by 1
),

final as (
    select
        s.station_id,
        s.station_name,
        s.borough,
        coalesce(r.total_departures, 0) as total_departures
    from with_borough s
    left join ride_counts r using (station_id)
)

select * from final
order by total_departures desc
