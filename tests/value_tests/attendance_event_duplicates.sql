/*
**What is this test?**
This test finds records where there are multiple attendance event records per student-school-calendar date.
These duplicates are handled in fct_student_daily_attendance, but they may point to data quality issues that 
could be addressed in the source system or ODS.

**When is this important to resolve?**
When the duplicates are signs of source system errors, e.g. Absent is corrected with Tardy, then Absent record should
actually be deleted from ODS. The warehouse does its best to handle these cases, but correcting in ODS is always preferred.

**How to resolve?**
Depends on the case, but once issue is diagnosed, you might resolve that issue and also clean up ODS by deleting the "corrected"
records.
*/
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with fct_student_school_attendance_event as (
    select * from {{ ref('fct_student_school_attendance_event') }}
),
dim_calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),
dim_school_calendar as (
    select * from {{ ref('dim_school_calendar') }}
),
dim_session as (
    select * from {{ ref('dim_session') }}
),
d_aec_counts as (
    select k_student, k_school, k_calendar_date,
        count(distinct attendance_event_category) as n_unique_attendance_event_categories
    from fct_student_school_attendance_event
    group by k_student, k_school, k_calendar_date
),
d_isAbsent_counts as (
    select k_student, k_school, k_calendar_date,
        count(distinct is_absent) as n_unique_is_absent_values
    from fct_student_school_attendance_event
    group by k_student, k_school, k_calendar_date
),
d_session_counts as (
    select k_student, k_school, k_calendar_date,
        count(distinct k_session) as n_unique_sessions
    from fct_student_school_attendance_event
    group by k_student, k_school, k_calendar_date
)
select 
  fct.k_student, 
  fct.k_school, 
  dim_calendar_date.calendar_date, 
  dim_school_calendar.calendar_code,
  fct.attendance_event_category, 
  fct.is_absent,
  fct.attendance_event_category = min(fct.attendance_event_category) over(partition by fct.k_student, fct.k_school, dim_calendar_date.calendar_date) as is_preferred_category_by_dedupe,
  fct.k_session,
  count(*) over(partition by fct.k_student, fct.k_school, dim_calendar_date.calendar_date) as n_duplicates,
  d_aec_c.n_unique_attendance_event_categories,
  d_ia_c.n_unique_is_absent_values,
  d_s_c.n_unique_sessions
from fct_student_school_attendance_event fct
join dim_calendar_date
  on fct.k_calendar_date = dim_calendar_date.k_calendar_date
join dim_school_calendar
  on dim_calendar_date.k_school_calendar = dim_school_calendar.k_school_calendar
join d_aec_counts d_aec_c
  on fct.k_student = d_aec_c.k_student
  and fct.k_school = d_aec_c.k_school
  and fct.k_calendar_date = d_aec_c.k_calendar_date
join d_isAbsent_counts d_ia_c
  on fct.k_student = d_ia_c.k_student
  and fct.k_school = d_ia_c.k_school
  and fct.k_calendar_date = d_ia_c.k_calendar_date
join d_session_counts d_s_c
  on fct.k_student = d_s_c.k_student
  and fct.k_school = d_s_c.k_school
  and fct.k_calendar_date = d_s_c.k_calendar_date
qualify 1 < count(*) over(partition by fct.k_student, fct.k_school, dim_calendar_date.calendar_date)
order by fct.k_student, fct.k_school, calendar_date, attendance_event_category