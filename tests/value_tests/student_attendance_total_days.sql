{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
-- find students with total days enrolled larger than typical
with cumulative_attendance as (
    select * from {{ ref('msr_student_cumulative_attendance') }}
)
select *
from cumulative_attendance
where days_enrolled > 185