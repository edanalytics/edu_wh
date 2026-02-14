/*
Find records in student school associations where we can't
associate the record with a school calendar, and the school has >1 calendar.
These records are dropped from attendance calculation, so we should notify
the tenant, in case they can resolve the missing calendar reference.
*/
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with
fct_stu_school_assoc as (
  select * from {{ ref('fct_student_school_association') }}
),
dim_calendar as (
  select * from {{ ref('dim_school_calendar') }}
),
dim_school as (
  select * from {{ ref('dim_school') }}
),
first_school_day as (
  select * from {{ ref('bld_ef3__school_calendar_windows') }}
),
school_calendar_counts as (
  select
    k_school,
    school_year,
    count(*) as n_calendars_at_school
  from dim_calendar
  group by 1,2
)
select 
    fct_stu_school_assoc.k_school,
    dim_school.school_id,
    fct_stu_school_assoc.school_year,
    count_if(dim_calendar.k_school_calendar is null) as n_students_missing_calendar,
    count(*) as n_total,
    round(100*n_students_missing_calendar/n_total, 2) as pct_missing,
    any_value(school_calendar_counts.n_calendars_at_school) as n_calendars_at_school,
    n_students_missing_calendar || ' records (' || pct_missing || ' %) of student enrollments have ambiguous calendars: the enrollment has no calendar association, and the school has multiple calendars. Calendars are necessary to calculate attendance.' as audit_message
from fct_stu_school_assoc
join dim_school
  on fct_stu_school_assoc.k_school = dim_school.k_school
join school_calendar_counts
  on dim_school.k_school = school_calendar_counts.k_school
  and fct_stu_school_assoc.school_year = school_calendar_counts.school_year
left join dim_calendar
    on fct_stu_school_assoc.k_school_calendar = dim_calendar.k_school_calendar
join first_school_day
  on dim_school.k_school = first_school_day.k_school
  and fct_stu_school_assoc.school_year = first_school_day.school_year
  -- subset to school overall first date
  and first_school_day.k_school_calendar is null
where true 
-- exclude students who exited before first day of school
and {{ date_within_end_date('first_school_day.first_school_day', 'fct_stu_school_assoc.exit_withdraw_date', var('edu:enroll:exit_withdraw_date_inclusive', True)) }}
group by 1,2,3
having n_students_missing_calendar != 0