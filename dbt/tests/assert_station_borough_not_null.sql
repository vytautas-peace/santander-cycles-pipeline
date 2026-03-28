-- Custom test: every station must have a resolved borough
select *
from {{ ref('dim_stations') }}
where borough is null or borough = 'Unknown'
limit 1  -- presence of any row = test failure
