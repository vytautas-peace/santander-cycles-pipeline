-- dim_stations.sql
-- Station dimension derived from ride data (no external lookup needed).
-- Uses the most-recent name observed for each station ID.
-- borough: manual overrides for edge IDs + name heuristics; never null (dashboard colour).

{{
  config(
    materialized = 'table',
    cluster_by   = ['station_id'],
    labels       = {'layer': 'mart', 'project': 'santander-cycles'}
  )
}}

-- TfL IDs that do not resolve cleanly from name alone (depots, renames, etc.)
with overrides as (

    select * from unnest([
        struct('434' as station_id, 'Lambeth' as borough),
        struct('346' as station_id, 'Islington' as borough),
        struct('465' as station_id, 'Hackney' as borough),
        struct('571' as station_id, 'Hammersmith and Fulham' as borough),
        struct('780' as station_id, 'Hammersmith and Fulham' as borough)
    ])

),

starts as (
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

with_borough as (
    select
        d.station_id,
        d.station_name,
        coalesce(
            o.borough,
            case
                when regexp_contains(lower(d.station_name), r'westfield|shepherd|imperial wharf') then 'Hammersmith and Fulham'
                when regexp_contains(lower(d.station_name), r'pitfield|hoxton') then 'Hackney'
                when regexp_contains(lower(d.station_name), r'mechanical workshop') and regexp_contains(lower(d.station_name), r'clapham') then 'Lambeth'
                when regexp_contains(lower(d.station_name), r'mechanical workshop') and regexp_contains(lower(d.station_name), r'penton') then 'Islington'
            end,
            'Other London'
        ) as borough
    from deduped d
    left join overrides o using (station_id)
)

select * from with_borough
