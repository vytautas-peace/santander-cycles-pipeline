/* @bruin

name: san_cycles_stg.bikepoints
type: bq.sql

depends:
  - san_cycles_raw.bikepoints

materialization:
  type: table
  strategy: create+replace

connection: san_cycles

description: |
  Cleaned and typed staging model for TfL docking station data.
  One row per station as currently active on the TfL BikePoint API.
  Borough is derived from reverse geocoding station coordinates.

columns:
  - name: station_id
    description: Numeric station identifier (TfL TerminalName)
    checks:
      - name: not_null
      - name: unique
  - name: borough
    description: London borough from reverse geocoding station coordinates

@bruin */


SELECT
    CAST(station_id   AS INTEGER)   AS station_id,
    TRIM(station_name)              AS station_name,
    CAST(lat          AS FLOAT64)   AS lat,
    CAST(lon          AS FLOAT64)   AS lon,
    TRIM(borough)                   AS borough
FROM san_cycles_raw.bikepoints
WHERE station_id IS NOT NULL
