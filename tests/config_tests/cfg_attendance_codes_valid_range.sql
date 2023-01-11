{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
select distinct is_absent
from {{ ref('xwalk_attendance_events') }}
where is_absent not between 0 and 1