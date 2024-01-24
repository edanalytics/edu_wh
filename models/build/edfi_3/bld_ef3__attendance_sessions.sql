-- when filling positive attendance, we need to link to sessions 
-- to produce k_session. we can't fill by date range, because schools
-- have sessions with overlapping date ranges
-- (specifically, nested sessions: full year, semester 1, quarter 1)
-- to accurately fill the appropriate sessions, we need to know what level
-- of sessions is linked to attendance events in each school

-- add test to assure that these sessions are non-overlapping
with dim_session as (
    select * from {{ ref('dim_session') }}
),
fct_student_sch_attend as (
    select * from {{ ref(var("edu:attendance:daily_attendance_source", 'fct_student_school_attendance_event')) }}
),
joined as (
    select 
        distinct 
        fct_student_sch_attend.k_school,
        dim_session.k_session,
        fct_student_sch_attend.tenant_code,
        dim_session.session_begin_date,
        dim_session.session_end_date,
        dim_session.total_instructional_days
    from fct_student_sch_attend
    join dim_session 
        on fct_student_sch_attend.k_session = dim_session.k_session
)
select * from joined
