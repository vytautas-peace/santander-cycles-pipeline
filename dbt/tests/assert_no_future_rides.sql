-- Custom test: no ride should have a start date in the future
select *
from {{ ref('fct_rides') }}
where start_date_utc > current_timestamp()
