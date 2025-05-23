    {# pre_hook=["{{edu_wh.incremental_pre_hook_by_year()}}"], #}
{{
  config(
    materialized='incremental',
    unique_key=['k_student', 'k_school', 'calendar_date'],
    post_hook=[
        "{% if not is_incremental() %} alter table {{ this }} add primary key (k_student, k_school, calendar_date) {% endif %}",
        "{% if not is_incremental() %} alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }} {% endif %}",
        "{% if not is_incremental() %} alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }} {% endif %}",
        "{% if not is_incremental() %} alter table {{ this }} add constraint fk_{{ this.name }}_calendar_date foreign key (k_calendar_date) references {{ ref('dim_calendar_date') }} {% endif %}",
    ]
  )
}}

with fct_student_school_att as (
    select * from {{ ref(var("edu:attendance:daily_attendance_source", 'fct_student_school_attendance_event')) }}
    {% if is_incremental() %}
    -- Only get from latest year (school_year + 1) to avoid reprocessing all data. +1 because in pre-hook we delete latest year
    where last_modified_timestamp > (select max(att_last_modified_timestamp) from {{ this }})
    {% endif %}
),
dim_calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
    {% if is_incremental() %}
    where k_calendar_date in (select distinct k_calendar_date from fct_student_school_att)
    {% endif %}
),
dim_session as (
    select * from {{ ref('dim_session') }}
    {# {% if is_incremental() %}
    where school_year = (select max(school_year) + 1 from {{ this }})
    {% endif %} #}
),
fct_student_school_assoc as (
    select * from {{ ref('fct_student_school_association') }}
    {# {% if is_incremental() %}
    where school_year = (select max(school_year) + 1 from {{ this }})
    {% endif %} #}
),
metric_absentee_categories as (
    select * from {{ ref('absentee_categories') }}
),
bld_attendance_sessions as (
    select * from {{ ref('bld_ef3__attendance_sessions') }}
    {# add this if you can add school year upstream and think that will help with performance #}
    {# {% if is_incremental() %}
    where school_year = (select max(school_year) + 1 from {{ this }})
    {% endif %} #}
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
        enr.k_student_xyear,
        enr.k_school,
        enr.tenant_code,
        enr.school_year,
        enr.entry_date,
        attendance_calendar.k_calendar_date,
        attendance_calendar.calendar_date,
        enr.exit_withdraw_date
    from fct_student_school_assoc as enr
    join attendance_calendar
        on enr.k_school_calendar = attendance_calendar.k_school_calendar
    -- keep days from enrollment to current-date/end of year to assist with rolling
    -- absenteeism metrics forward post-enrollment
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
        {# REVIEW #}
        {# set total_instructional_days using dim_session for records that come from fct_student_school_att.
           If left blank (as was previously), positively-filled records sort above negative attendance records in the dedupe below
        #}
        coalesce(
            dim_session.total_instructional_days,
            bld_attendance_sessions.total_instructional_days
        ) as total_instructional_days,
        stu_enr_att_cal.tenant_code,
        stu_enr_att_cal.school_year,
        stu_enr_att_cal.calendar_date,
        fct_student_school_att.attendance_event_reason,
        -- set enrollment flag: 1 during enrollment, 0 after, no row prior
        case 
            when stu_enr_att_cal.calendar_date <= stu_enr_att_cal.exit_withdraw_date
                or stu_enr_att_cal.exit_withdraw_date is null
            then 1.0
            else 0.0
        end is_enrolled,
        case 
            when is_enrolled = 0 then 'Not Enrolled'
            else coalesce(
                    fct_student_school_att.attendance_event_category,
                    '{{ var("edu:attendance:in_attendance_code") }}') 
        end as attendance_event_category,
        coalesce(
            case 
                when is_enrolled = 1 then fct_student_school_att.is_absent
                else 0.0
            end, 0.0) as is_absent,
        -- invert is_absent: 1->0, 0->1, 0.25->0.75
        1.0 - coalesce(
            case 
                when is_enrolled = 1 then fct_student_school_att.is_absent
                else 1.0
            end, 0.0) as is_present,
        fct_student_school_att.event_duration,
        fct_student_school_att.school_attendance_duration,
        fct_student_school_att.last_modified_timestamp as att_last_modified_timestamp
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
            order_by='is_enrolled desc, total_instructional_days, attendance_event_category, k_session'
        )
    }}
),
cumulatives as (
    select 
        positive_attendance_deduped.k_student,
        positive_attendance_deduped.k_student_xyear,
        positive_attendance_deduped.k_school,
        positive_attendance_deduped.k_calendar_date,
        positive_attendance_deduped.calendar_date,
        positive_attendance_deduped.k_session,
        positive_attendance_deduped.tenant_code,
        positive_attendance_deduped.school_year,
        positive_attendance_deduped.attendance_event_category,
        positive_attendance_deduped.attendance_event_reason,
        positive_attendance_deduped.is_absent,
        positive_attendance_deduped.is_present,
        positive_attendance_deduped.is_enrolled,
        sum(is_enrolled) over(
            partition by k_student, k_school) as total_days_enrolled,
        sum(is_absent) over(
            partition by k_student, k_school 
            order by calendar_date) as cumulative_days_absent,
        sum(is_present) over(
            partition by k_student, k_school 
            order by calendar_date) as cumulative_days_attended,
        sum(is_enrolled) over(
            partition by k_student, k_school
            order by calendar_date) as cumulative_days_enrolled,
        round(100 * cumulative_days_attended / nullif(cumulative_days_enrolled, 0), 2) as cumulative_attendance_rate,
        cumulative_days_enrolled >= {{ var('edu:attendance:chronic_absence_min_days') }} as meets_enrollment_threshold,
        {{ msr_chronic_absentee('cumulative_attendance_rate', 'cumulative_days_enrolled') }} as is_chronic_absentee,
        positive_attendance_deduped.event_duration,
        positive_attendance_deduped.school_attendance_duration,
        positive_attendance_deduped.att_last_modified_timestamp
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
        on cumulative_attendance_rate > metric_absentee_categories.threshold_lower
        and cumulative_attendance_rate <= metric_absentee_categories.threshold_upper
)
select * from metric_labels
order by tenant_code, k_school, k_student, cumulative_days_enrolled
