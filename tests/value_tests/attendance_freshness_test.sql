{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
select *
from {{ ref('attendance_freshness') }}
where (current_date() - max_date) > 7
-- try to avoid warnings when school is out
and monthname(current_date()) not in ('Jul', 'Aug')