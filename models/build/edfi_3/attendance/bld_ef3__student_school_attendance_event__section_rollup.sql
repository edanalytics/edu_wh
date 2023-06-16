with school as (
    select * from {{ ref('dim_school') }}
),
student as (
    select * from {{ ref('dim_student') }}
),
course_section as (
    select * from {{ ref('dim_course_section') }}
),
class_period as (
    select * from {{ ref('dim_class_period') }}
),
calendar_dates as (
    select * from {{ ref('dim_calendar_date') }}
),
student_section_attendance_event as (
    select * from {{ ref('fct_student_section_attendance_event') }}
),
student_section_association as (
    select * from {{ ref('fct_student_section_association') }}
),
student_school_association as (
    select * from {{ ref('fct_student_school_association') }}
),
sections__class_periods as (
    select * from {{ ref('stg_ef3__sections__class_periods') }}
),
bell_schedules__class_periods as (
    select * from {{ ref('stg_ef3__bell_schedules__class_periods') }}
),
bell_schedules as (
    select * from {{ ref('stg_ef3__bell_schedules') }}
),
bell_schedules__dates as (
    select * from {{ ref('stg_ef3__bell_schedules__dates') }}
),
course_section_attendance_flag as (
    select * from {{ ref('stg_ic__course_sections_attendance') }}
),
class_period_attendance_flag as (
    select * from {{ ref('stg_ic__class_periods_attendance') }}
),

section_attendance as (
    select
        student_school_association.k_school,
        student_section_association.k_student,
        student_section_association.k_student_xyear,
        course_section.k_session,
        student_school_association.k_school_calendar,
        course_section.k_course_section,
        class_period.k_class_period,
        bell_schedules__dates.calendar_date,
        student_section_attendance_event.attendance_event_category,
        student_section_attendance_event.attendance_event_reason,
        student_section_attendance_event.event_duration,
        student_section_attendance_event.section_attendance_duration,
        student_section_attendance_event.arrival_time,
        student_section_attendance_event.departure_time,
        student_section_attendance_event.educational_environment,
        coalesce(student_section_attendance_event.is_absent, 0) as is_absent
        {# course_section_attendance_flag.attendance as course_section_attendance_flag,
        class_period_attendance_flag.attendance as class_period_attendance_flag #}
    from student_section_association
    inner join student_school_association
        on student_section_association.k_student = student_school_association.k_student
    inner join course_section
        on student_section_association.k_course_section = course_section.k_course_section
    inner join sections__class_periods
        on student_section_association.k_course_section = sections__class_periods.k_course_section
    inner join class_period
        on sections__class_periods.k_class_period = class_period.k_class_period
    inner join bell_schedules__class_periods
        on class_period.k_class_period = bell_schedules__class_periods.k_class_period
    inner join bell_schedules
        on bell_schedules__class_periods.k_bell_schedule = bell_schedules.k_bell_schedule
    inner join bell_schedules__dates
        on bell_schedules.k_bell_schedule = bell_schedules__dates.k_bell_schedule
    left join student_section_attendance_event -- left join to pull in absence records but keep all other sections
        on student_section_association.k_student = student_section_attendance_event.k_student
        and student_section_association.k_course_section = student_section_attendance_event.k_course_section
        and bell_schedules__dates.calendar_date = student_section_attendance_event.attendance_event_date
    {# TODO make theses dynamic from dbt variable
    inner join course_section_attendance_flag
        on course_section.k_course_section = course_section_attendance_flag.k_course_section
    inner join class_period_attendance_flag
        on class_period.k_class_period = class_period_attendance_flag.k_class_period #}
    where true
        and bell_schedules__dates.calendar_date between student_section_association.begin_date and student_section_association.end_date
        and (bell_schedules__dates.calendar_date >= student_school_association.entry_date
            and (bell_schedules__dates.calendar_date <= exit_withdraw_date or exit_withdraw_date is null))
        and bell_schedules__dates.calendar_date <= current_date()
    order by 1,2,8
),

daily_attendance as (
    select
        section_attendance.k_student,
        section_attendance.k_student_xyear,
        section_attendance.k_school,
        calendar_dates.k_calendar_date,
        section_attendance.k_session,
        section_attendance.calendar_date,
        max(attendance_event_category) as attendance_event_category, -- max may not work for all cases
        max(attendance_event_reason) as attendance_event_reason, -- max may not work for all cases
        ifnull(sum(section_attendance.is_absent), 0) as periods_absent, 
        count(k_class_period) as periods_total,
        iff(periods_absent / periods_total > 0.5, 1, 0) as is_absent,
        {# TODO check on the logic for aggregating the below fields
        they're null for jeffco so unclear how to handle these fields #}
        sum(event_duration) as event_duration,
        sum(section_attendance_duration) as school_attendance_duration,
        max(arrival_time) as arrival_time,
        min(departure_time) as departure_time,
        max(educational_environment) as educational_environment
    from section_attendance
    inner join calendar_dates
        on section_attendance.k_school_calendar = calendar_dates.k_school_calendar
        and section_attendance.calendar_date = calendar_dates.calendar_date
    {# TODO add back later
    where section_attendance.course_section_attendance_flag = 1
        and class_period_attendance_flag = 1
        and calendar_dates.is_school_day = 1 #}
    group by 1,2,3,4,5,6
    order by 3,1,6
),

/*columns needed for final bld_ef3__student_school_attendance_event__section_rollup*/
final as (
    select
        k_student,
        k_student_xyear,
        k_school,
        k_calendar_date,
        k_session,
        attendance_event_category,
        attendance_event_reason,
        is_absent,
        event_duration,
        school_attendance_duration,
        arrival_time,
        departure_time,
        educational_environment
    from daily_attendance
    where is_absent = 1
)

select * from final