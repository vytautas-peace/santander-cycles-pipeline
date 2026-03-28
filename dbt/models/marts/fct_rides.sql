-- fct_rides.sql
-- Central fact table. One row per completed journey.
-- Partitioned by ride_month (DATE), clustered by start/end station.
-- Supports: time-series analysis, OD (origin-destination) matrices,
--           station utilisation, duration histograms.

{{
  config(
    materialized  = 'table',
    partition_by  = {
      'field': 'ride_month',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by    = ['start_station_id', 'end_station_id'],
    labels        = {'layer': 'mart', 'project': 'santander-cycles'}
  )
}}

with rides as (
    select * from {{ ref('stg_rides') }}
),

-- Surrogate key using dbt_utils
final as (

    select
        -- Surrogate key (deterministic, reproducible)
        {{ dbt_utils.generate_surrogate_key(['rental_id']) }}      as ride_sk,

        rental_id,
        bike_id,

        -- Timestamps
        start_date_utc,
        end_date_utc,
        ride_date,
        ride_month,        -- partition key
        start_hour,
        start_day_of_week,

        -- Duration
        duration_seconds,
        round(duration_seconds / 60.0, 2)                         as duration_minutes,

        -- Station FKs
        start_station_id,
        start_station_name,
        end_station_id,
        end_station_name,

        -- Flags
        is_round_trip,

        -- Metadata
        _source_file,
        _ingested_at

    from rides

    -- Final quality gate: must have both stations and a valid duration
    where
        start_station_id is not null
        and end_station_id   is not null
        and duration_seconds is not null
        and duration_seconds > 0

)

select * from final
