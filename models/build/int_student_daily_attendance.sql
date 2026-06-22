{{
  config(
    materialized='incremental',
    unique_key=['k_student', 'k_school', 'calendar_date'],
  )
}}

with
{% if is_incremental() %}
changed_enrollments as (
    select
        k_student,
        k_school,
        k_school_calendar
    from {{ ref('fct_student_school_association') }}
    where last_modified_timestamp > (select max(enr_last_modified_timestamp) from {{ this }})
),
{% endif %}
fct_student_school_att as (
    select * from {{ ref(var("edu:attendance:daily_attendance_source", 'fct_student_school_attendance_event')) }}
    {% if is_incremental() %}
    where last_modified_timestamp > (select max(att_last_modified_timestamp) from {{ this }})
        or (k_student, k_school) in (select k_student, k_school from changed_enrollments)
    {% endif %}
),
dim_calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
    {% if is_incremental() %}
    where k_calendar_date in (select distinct k_calendar_date from fct_student_school_att)
        or k_school_calendar in (select distinct k_school_calendar from changed_enrollments)
    {% endif %}
),
dim_session as (
    select * from {{ ref('dim_session') }}
),
fct_student_school_assoc as (
    select * from {{ ref('fct_student_school_association') }}
    {% if is_incremental() %}
    where (k_student, k_school) in (
        select k_student, k_school from fct_student_school_att
        union
        select k_student, k_school from changed_enrollments
    )
    {% endif %}
),
bld_attendance_sessions as (
    select * from {{ ref('bld_ef3__attendance_sessions') }}
),
school_max_submitted as (
    select
        fct_student_school_att.k_school,
        dim_calendar_date.school_year,
        max(dim_calendar_date.calendar_date) as max_date_by_school
    from fct_student_school_att
    join dim_calendar_date
        on fct_student_school_att.k_calendar_date = dim_calendar_date.k_calendar_date
    group by 1, 2
),
max_calendar as (
    select
        dim_calendar_date.k_school,
        dim_calendar_date.school_year,
        max(dim_calendar_date.calendar_date) as max_calendar_date
    from dim_calendar_date
    group by 1, 2
),
attendance_calendar as (
    select
        dim_calendar_date.k_school,
        dim_calendar_date.k_school_calendar,
        dim_calendar_date.k_calendar_date,
        dim_calendar_date.school_year,
        dim_calendar_date.calendar_date,
        school_max_submitted.max_date_by_school,
        max_calendar.max_calendar_date
    from dim_calendar_date
    join school_max_submitted
        on dim_calendar_date.k_school = school_max_submitted.k_school
        and dim_calendar_date.school_year = school_max_submitted.school_year
    join max_calendar
        on dim_calendar_date.k_school = max_calendar.k_school
        and dim_calendar_date.school_year = max_calendar.school_year
    where dim_calendar_date.is_school_day
    and (
        (
            dim_calendar_date.calendar_date <= current_date()
            and dim_calendar_date.calendar_date <= school_max_submitted.max_date_by_school
        )
        or max_calendar.max_calendar_date <= current_date()
    )
),
stu_enr_att_cal as (
    select
        enr.k_student,
        enr.k_student_xyear,
        enr.k_school,
        enr.tenant_code,
        enr.school_year,
        enr.entry_date,
        attendance_calendar.k_calendar_date,
        attendance_calendar.k_school_calendar,
        attendance_calendar.calendar_date,
        enr.exit_withdraw_date,
        enr.last_modified_timestamp as enr_last_modified_timestamp
    from fct_student_school_assoc as enr
    join attendance_calendar
        on enr.k_school_calendar = attendance_calendar.k_school_calendar
    where attendance_calendar.calendar_date >= enr.entry_date
),
fill_positive_attendance as (
    select
        stu_enr_att_cal.k_student,
        stu_enr_att_cal.k_student_xyear,
        stu_enr_att_cal.k_school,
        stu_enr_att_cal.k_calendar_date,
        coalesce(
            fct_student_school_att.k_session,
            bld_attendance_sessions.k_session
        ) as k_session,
        {# set total_instructional_days using dim_session for records that come from fct_student_school_att.
           If left blank, positively-filled records sort above negative attendance records in the dedupe below. #}
        coalesce(
            dim_session.total_instructional_days,
            bld_attendance_sessions.total_instructional_days
        ) as total_instructional_days,
        stu_enr_att_cal.tenant_code,
        stu_enr_att_cal.school_year,
        stu_enr_att_cal.calendar_date,
        fct_student_school_att.attendance_event_reason,
        case
            when {{ date_within_end_date('stu_enr_att_cal.calendar_date', 'stu_enr_att_cal.exit_withdraw_date', var('edu:enroll:exit_withdraw_date_inclusive', True)) }}
            then 1.0
            else 0.0
        end as is_enrolled,
        case
            when is_enrolled = 0 then 'Not Enrolled'
            else coalesce(
                    fct_student_school_att.attendance_event_category,
                    '{{ var("edu:attendance:in_attendance_code") }}')
        end as attendance_event_category,
        case
            when is_enrolled = 0 then 'Not Enrolled'
            when coalesce(fct_student_school_att.is_absent, 0.0) = 0 then 'Not Absent'
            else coalesce(fct_student_school_att.attendance_excusal_status, 'Unknown Excusal Status')
        end as attendance_excusal_status,
        coalesce(
            case
                when is_enrolled = 1 then fct_student_school_att.is_absent
                else 0.0
            end, 0.0) as is_absent,
        1.0 - coalesce(
            case
                when is_enrolled = 1 then fct_student_school_att.is_absent
                else 1.0
            end, 0.0) as is_present,
        fct_student_school_att.event_duration,
        fct_student_school_att.school_attendance_duration,
        fct_student_school_att.last_modified_timestamp as att_last_modified_timestamp,
        stu_enr_att_cal.enr_last_modified_timestamp
    from stu_enr_att_cal
    left join fct_student_school_att
        on stu_enr_att_cal.k_student = fct_student_school_att.k_student
        and stu_enr_att_cal.k_school = fct_student_school_att.k_school
        and stu_enr_att_cal.k_calendar_date = fct_student_school_att.k_calendar_date
    left join dim_session
        on fct_student_school_att.k_session = dim_session.k_session
    left join bld_attendance_sessions
        on fct_student_school_att.k_session is null
        and stu_enr_att_cal.k_school = bld_attendance_sessions.k_school
        and stu_enr_att_cal.calendar_date between
            bld_attendance_sessions.session_begin_date and
            bld_attendance_sessions.session_end_date
)
select * from (
    {{
        dbt_utils.deduplicate(
            relation='fill_positive_attendance',
            partition_by='k_student, k_school, calendar_date',
            order_by='is_enrolled desc, total_instructional_days, attendance_event_category, k_session'
        )
    }}
)
