{# TfL IDs are integers; raw data sometimes surfaces as floats ("571.0"). #}
{% macro normalize_station_id(column) %}
cast(
  cast(
    safe_cast(nullif(trim(cast({{ column }} as string)), '') as float64) as int64
  ) as string
)
{% endmacro %}
