{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_school, k_calendar_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_calendar_date foreign key (k_calendar_date) references {{ ref('dim_calendar_date') }}",
    ]
  )
}}

with fct_student_school_att as (
    select * from {{ ref('fct_student_school_attendance_event') }}
),
dim_calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),
fct_student_school_assoc as (
    select * from {{ ref('fct_student_school_association') }}
),
metric_absentee_categories as (
    select * from {{ ref('absentee_categories') }}
),
bld_attendance_sessions as (
    select * from {{ ref('bld_ef3__attendance_sessions') }}
),
school_max_submitted as (
    -- find the most recently submitted attendance date by school
    select 
        fct_student_school_att.k_school,
        max(dim_calendar_date.calendar_date) as max_date_by_school
    from fct_student_school_att 
    join dim_calendar_date 
        on fct_student_school_att.k_calendar_date = dim_calendar_date.k_calendar_date
    group by 1
),
attendance_calendar as (
    -- a dataset of all possible days on which school attendance could be recorded
    select 
        --todo: detecting the maximum attendance submitted thus far?

        dim_calendar_date.k_school,
        dim_calendar_date.k_school_calendar,
        dim_calendar_date.k_calendar_date,
        dim_calendar_date.school_year,
        dim_calendar_date.calendar_date
    from dim_calendar_date
    join school_max_submitted
        on dim_calendar_date.k_school = school_max_submitted.k_school
    -- only include instructional days in the attendance calendar
    where dim_calendar_date.is_school_day
    -- don't include dates in the future, as of run-time
    and dim_calendar_date.calendar_date <= current_date()
    -- don't include dates beyond the max submitted attendance event by school
    and dim_calendar_date.calendar_date <= school_max_submitted.max_date_by_school
),
stu_enr_att_cal as (
    -- create an attendance calendar by student, conditional on enrollment
    select 
        enr.k_student,
        enr.k_school,
        enr.tenant_code,
        enr.entry_date,
        attendance_calendar.k_calendar_date,
        attendance_calendar.calendar_date,
        sum(1) over(
            partition by enr.k_student, enr.k_school
        ) as total_days_enrolled
    from fct_student_school_assoc as enr
    join attendance_calendar
        on enr.k_school_calendar = attendance_calendar.k_school_calendar
    where (attendance_calendar.calendar_date <= enr.exit_withdraw_date
          or enr.exit_withdraw_date is null)
    and attendance_calendar.calendar_date >= enr.entry_date
),
fill_positive_attendance as (
    select 
        stu_enr_att_cal.k_student,
        stu_enr_att_cal.k_school,
        stu_enr_att_cal.k_calendar_date,
        coalesce(
            fct_student_school_att.k_session, 
            bld_attendance_sessions.k_session
        ) as k_session,
        bld_attendance_sessions.total_instructional_days,
        stu_enr_att_cal.tenant_code,
        stu_enr_att_cal.calendar_date,
        coalesce(
            fct_student_school_att.attendance_event_category,
            '{{ var("edu:attendance:in_attendance_code") }}' 
        ) as attendance_event_category,
        fct_student_school_att.attendance_event_reason,
        coalesce(fct_student_school_att.is_absent, FALSE) as is_absent,
        not coalesce(fct_student_school_att.is_absent, FALSE) as is_present,
        true as is_enrolled,
        total_days_enrolled,
        fct_student_school_att.event_duration,
        fct_student_school_att.school_attendance_duration
    from stu_enr_att_cal
    left join fct_student_school_att
        on stu_enr_att_cal.k_student = fct_student_school_att.k_student
        and stu_enr_att_cal.k_school = fct_student_school_att.k_school
        and stu_enr_att_cal.k_calendar_date = fct_student_school_att.k_calendar_date
    left join bld_attendance_sessions 
        on fct_student_school_att.k_session is null
        and stu_enr_att_cal.k_school = bld_attendance_sessions.k_school
        and stu_enr_att_cal.calendar_date between 
            bld_attendance_sessions.session_begin_date and 
            bld_attendance_sessions.session_end_date
),
positive_attendance_deduped as (
    -- account for multiple overlapping enrollments at the same school by ensuring
    -- we only count each day once.
    -- also handle cases where the sessions attributed to attendance days
    -- are not all in the same level of the hierarchy, for instance:
    -- Fall attendance events have the 'Fall Semester' session, Spring attendance
    -- events have the 'Year Round' session. In these cases, all Fall semester
    -- events will be doubled because they occur within the bounds of two sessions.
    -- We sort by session.total_instructional_days to prefer the smallest available
    -- session in such cases.
    {{ 
        dbt_utils.deduplicate(
            relation='fill_positive_attendance',
            partition_by='k_student, k_school, calendar_date',
            order_by='total_instructional_days'
        )
    }}
),
cumulatives as (
    select 
        positive_attendance_deduped.k_student,
        positive_attendance_deduped.k_school,
        positive_attendance_deduped.k_calendar_date,
        positive_attendance_deduped.k_session,
        positive_attendance_deduped.tenant_code,
        positive_attendance_deduped.attendance_event_category,
        positive_attendance_deduped.attendance_event_reason,
        positive_attendance_deduped.is_absent,
        positive_attendance_deduped.is_present,
        positive_attendance_deduped.is_enrolled,
        positive_attendance_deduped.total_days_enrolled,
        sum(is_absent::integer) over(
            partition by k_student, k_school 
            order by calendar_date) as cumulative_days_absent,
        sum(is_present::integer) over(
            partition by k_student, k_school 
            order by calendar_date) as cumulative_days_attended,
        sum(1) over(
            partition by k_student, k_school
            order by calendar_date) as cumulative_days_enrolled,
        round(100 * cumulative_days_attended / nullif(cumulative_days_enrolled, 0), 2) as cumulative_attendance_rate,
        cumulative_days_enrolled >= {{ var('edu:attendance:chronic_absence_min_days') }} as meets_enrollment_threshold,
        {{ msr_chronic_absentee('cumulative_attendance_rate', 'cumulative_days_enrolled') }} as is_chronic_absentee,
        positive_attendance_deduped.event_duration,
        positive_attendance_deduped.school_attendance_duration
    from positive_attendance_deduped
),
metric_labels as (
    select 
        cumulatives.*,
        case 
            when meets_enrollment_threshold then metric_absentee_categories.level_numeric 
            else null 
        end as absentee_category_rank,
        case 
            when meets_enrollment_threshold then metric_absentee_categories.level_label 
            else null
        end as absentee_category_label
    from cumulatives
    left join metric_absentee_categories
        on cumulative_attendance_rate >= metric_absentee_categories.threshold_lower
        and cumulative_attendance_rate <= metric_absentee_categories.threshold_upper
)
select * from metric_labels
order by tenant_code, k_school, k_student, cumulative_days_enrolled
