-- Every station should have a resolved borough for Looker / maps (no Unknown / null).
select *
from {{ ref('dim_stations') }}
where borough is null
   or trim(borough) = ''
   or lower(trim(borough)) in ('unknown', 'null')
