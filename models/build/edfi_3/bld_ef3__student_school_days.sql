with student_school_enr as (
    select * from {{ ref('fct_student_school_association') }}
),
student_section_enr as (
    select * from {{ ref('fct_student_section_association') }}
),
course_section as (
    select * from {{ ref('dim_course_section') }}
),
section_class_periods as (
    select * from {{ ref('stg_ef3__sections__class_periods') }}
),
class_period as (
    select * from {{ ref('dim_class_period') }}
),
bell_class_period as (
    select * from {{ ref('stg_ef3__bell_schedules__class_periods') }}
),
bell as (
    select * from {{ ref('stg_ef3__bell_schedules') }}
),
bell_dates as (
    select * from {{ ref('stg_ef3__bell_schedules__dates') }}
),
school_calendar as (
    select * from {{ ref('dim_school_calendar') }}
),
calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),
-- Find all class dates for a student based on their bell schedules
student_bell_schedule_dates as (
    select distinct
        student_school_enr.k_student,
        student_school_enr.k_student_xyear,
        student_school_enr.k_school,
        student_school_enr.k_school_calendar,
        student_school_enr.entry_date,
        student_school_enr.exit_withdraw_date,
        bell_dates.calendar_date
    from student_school_enr
    inner join student_section_enr
        on student_school_enr.k_student = student_section_enr.k_student
        and student_school_enr.k_school = student_section_enr.k_school
    inner join course_section
        on student_section_enr.k_course_section = course_section.k_course_section
    inner join section_class_periods
        on student_section_enr.k_course_section = section_class_periods.k_course_section
    inner join class_period
        on section_class_periods.k_class_period = class_period.k_class_period
    inner join bell_class_period
        on class_period.k_class_period = bell_class_period.k_class_period
    inner join bell
        on bell_class_period.k_bell_schedule = bell.k_bell_schedule
    inner join bell_dates
        on bell.k_bell_schedule = bell_dates.k_bell_schedule
    -- limit to the duration of the section enrollment
    where bell_dates.calendar_date between student_section_enr.begin_date and student_section_enr.end_date
    -- limit to the duration of the school enrollment
        and bell_dates.calendar_date >= student_school_enr.entry_date
        and (bell_dates.calendar_date <= student_school_enr.exit_withdraw_date
            or student_school_enr.exit_withdraw_date is null)
),
-- Remove any non-instructional days
student_class_dates as (
    select 
        student_bell_schedule_dates.k_student,
        student_bell_schedule_dates.k_student_xyear,
        student_bell_schedule_dates.k_school,
        calendar_date.k_calendar_date,
        school_calendar.tenant_code,
        school_calendar.school_year,
        student_bell_schedule_dates.entry_date,
        student_bell_schedule_dates.exit_withdraw_date,
        calendar_date.calendar_date
    from student_bell_schedule_dates
    inner join school_calendar
        on student_bell_schedule_dates.k_school_calendar = school_calendar.k_school_calendar
    inner join calendar_date
        on school_calendar.k_school_calendar = calendar_date.k_school_calendar
        and student_bell_schedule_dates.calendar_date = calendar_date.calendar_date
    where calendar_date.is_school_day
)
select *
from student_class_dates