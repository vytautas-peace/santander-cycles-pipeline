-- Custom test: duration must be strictly positive
select *
from {{ ref('fct_rides') }}
where duration_seconds <= 0
   or duration_seconds is null
