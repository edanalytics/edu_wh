with att_events as (
    select * from {{ ref(var("edu:attendance:daily_attendance_source", 'fct_student_school_attendance_event')) }}
),
dim_calendar as (
    select * from {{ ref('dim_calendar_date') }}
)
select
    att_events.tenant_code,
    max(calendar_date) as max_date
from att_events
join dim_calendar
    on att_events.k_calendar_date = dim_calendar.k_calendar_date
group by 1