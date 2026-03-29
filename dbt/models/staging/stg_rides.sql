-- stg_rides.sql
-- Cleans and types the raw TfL rides table.
-- Materialised as a VIEW to avoid data duplication at this layer.

with source as (

    select * from {{ source('raw', 'raw_rides') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (partition by rental_id order by _ingested_at desc) = 1
),

cleaned as (

    select
        -- Keys
        cast(rental_id as string)                                   as rental_id,
        cast(bike_id   as string)                                   as bike_id,

        -- Timestamps
        timestamp_trunc(start_date, second)                         as start_date_utc,
        timestamp_trunc(end_date,   second)                         as end_date_utc,

        -- Duration (sanitise: keep only 1–86400 seconds)
        case
            when cast(duration as int64) between 1 and 86400
                then cast(duration as int64)
            else null
        end                                                         as duration_seconds,

        -- Station IDs and names
        {{ normalize_station_id('start_station_id') }}              as start_station_id,
        trim(start_station_name)                                    as start_station_name,
        {{ normalize_station_id('end_station_id') }}                as end_station_id,
        trim(end_station_name)                                      as end_station_name,

        -- Derived date fields (for partitioning + analysis)
        date(start_date)                                            as ride_date,
        date_trunc(date(start_date), month)                         as ride_month,
        extract(hour  from start_date)                              as start_hour,
        extract(dayofweek from start_date)                          as start_day_of_week,  -- 1=Sun, 7=Sat in BQ

        -- Round-trip flag
        case
            when start_station_id = end_station_id then true
            else false
        end                                                         as is_round_trip,

        -- Metadata
        _source_file,
        _ingested_at

    from deduplicated

    where
        -- Remove rows with no usable start timestamp
        start_date is not null
        -- Same quality bar as fct_rides: duration must parse to 1–86400 s (cannot reference alias here)
        and safe_cast(duration as int64) between 1 and 86400
        -- Remove test/unknown stations
        and start_station_id not in ('0', 'nan', 'None')
        and end_station_id   not in ('0', 'nan', 'None')

)

select * from cleaned
