/* @bruin

name: san_cycles_mrt.fct_journeys
type: bq.sql

depends:
  - san_cycles_stg.journeys

materialization:
  type: table
  strategy: create+replace

connection: san_cycles

description: |
  Core fact table — one row per journey. Passes through all staging
  fields with year extracted as a convenience column for aggregations.
  Metadata (_source_file, _ingested_at) retained for lineage.

columns:
  - name: rental_id
    description: Unique journey identifier
    checks:
      - name: not_null
      - name: unique

@bruin */


SELECT
    -- Keys
    rental_id,
    bike_id,

    -- Timestamps
    start_date_utc,
    end_date_utc,

    -- Duration
    duration_seconds,

    -- Station IDs and names
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,

    -- Lineage
    _source_file,
    _ingested_at

FROM san_cycles_stg.journeys