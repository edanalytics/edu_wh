/*
Find records where attendance is recorded for a student, but we can't 
associate the record with a day in the school's calendar.
This could be because the student does not have a valid association with a 
school calendar, or because the school's calendar is not complete in its 
coverage of days.
*/
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with stg_stu_sch_attend as (
    select * from {{ ref('stg_ef3__student_school_attendance_events') }}
),
fct_stu_school_assoc as (
    select * from {{ ref('fct_student_school_association') }}
    where is_latest_annual_entry
),
dim_calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
joined as (
    select 
        stg_stu_sch_attend.tenant_code,
        stg_stu_sch_attend.api_year,
        stg_stu_sch_attend.k_student,
        stg_stu_sch_attend.k_school,
        fct_stu_school_assoc.k_school_calendar,
        stg_stu_sch_attend.attendance_event_date
    from stg_stu_sch_attend
    join dim_student
        on stg_stu_sch_attend.k_student = dim_student.k_student
    left join fct_stu_school_assoc
        on stg_stu_sch_attend.k_student = fct_stu_school_assoc.k_student
        and stg_stu_sch_attend.k_school = fct_stu_school_assoc.k_school
    left join dim_calendar_date
        on fct_stu_school_assoc.k_school_calendar = dim_calendar_date.k_school_calendar
        and stg_stu_sch_attend.attendance_event_date = dim_calendar_date.calendar_date
    where dim_calendar_date.k_calendar_date is null
)
select count(*) as failed_row_count, tenant_code, api_year from joined
group by all
having count(*) > 1
