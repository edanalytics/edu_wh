{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column calendar_date set not null",
        "alter table {{ this }} add primary key (k_student, k_school, calendar_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_calendar_date foreign key (k_calendar_date) references {{ ref('dim_calendar_date') }}",
    ]
  )
}}

{{ cds_depends_on('edu:student_daily_attendance:custom_data_sources') }}
{% set custom_data_sources = var('edu:student_daily_attendance:custom_data_sources', []) %}

with fct_student_school_att as (
    select * from {{ ref(var("edu:attendance:daily_attendance_source", 'fct_student_school_attendance_event')) }}
),
dim_calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),
dim_session as (
    select * from {{ ref('dim_session') }}
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
        dim_calendar_date.school_year,
        max(dim_calendar_date.calendar_date) as max_date_by_school
    from fct_student_school_att 
    join dim_calendar_date 
        on fct_student_school_att.k_calendar_date = dim_calendar_date.k_calendar_date
    group by 1, 2
),
max_calendar as (
    -- find the last day of the school calendar
    select 
        dim_calendar_date.k_school,
        dim_calendar_date.school_year,
        max(dim_calendar_date.calendar_date) as max_calendar_date
    from dim_calendar_date
    group by 1,2
),
attendance_calendar as (
    -- a dataset of all possible days on which school attendance could be recorded
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
    -- only include instructional days in the attendance calendar
    where dim_calendar_date.is_school_day
    and 
    (
        -- The first part of this OR statement handles dates in the current school year.
        -- The second part handles dates in past school years.
        (   
            -- don't include dates in the future, as of run-time
            dim_calendar_date.calendar_date <= current_date()
            -- don't include dates beyond the max submitted attendance event by school
            and dim_calendar_date.calendar_date <= school_max_submitted.max_date_by_school
        )
        -- or the max_calendar_date is before the current date
        -- ensures days from the last attendance event until the end of the school calendar are counted
        or max_calendar_date <= current_date()
    )
),
stu_enr_att_cal as (
    -- create an attendance calendar by student, conditional on enrollment
    select 
        enr.k_student,
        enr.k_student_xyear,
        enr.k_school,
        enr.tenant_code,
        enr.entry_date,
        attendance_calendar.k_calendar_date,
        attendance_calendar.school_year,
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
        stu_enr_att_cal.school_year,
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
        case
            when is_enrolled = 0  then 'Not Enrolled'
            when coalesce(fct_student_school_att.is_absent, 0.0) = 0 then 'Not Absent'
            else coalesce(fct_student_school_att.attendance_excusal_status, 'Unknown Excusal Status')
        end as attendance_excusal_status,
        fct_student_school_att.event_duration,
        fct_student_school_att.school_attendance_duration
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
excusal_status_streaks as (
    select 
        *,
        dense_rank() over (partition by k_student, k_school, excusal_status_streak_id, attendance_excusal_status order by calendar_date) as consecutive_days_by_excusal_status
    from 
        (select 
            *,
            dense_rank() over ( partition by k_student, k_school order by calendar_date ) 
            - dense_rank() over ( partition by k_student,k_school, attendance_excusal_status order by calendar_date) 
        as excusal_status_streak_id 
        from positive_attendance_deduped
        )
),
cumulatives as (
    select 
        excusal_status_streaks.k_student,
        excusal_status_streaks.k_student_xyear,
        excusal_status_streaks.k_school,
        excusal_status_streaks.k_calendar_date,
        excusal_status_streaks.school_year,
        excusal_status_streaks.calendar_date,
        excusal_status_streaks.k_session,
        excusal_status_streaks.tenant_code,
        excusal_status_streaks.attendance_event_category,
        excusal_status_streaks.attendance_event_reason,
        excusal_status_streaks.is_absent,
        excusal_status_streaks.is_present,
        excusal_status_streaks.is_enrolled,
        excusal_status_streaks.attendance_excusal_status,
        excusal_status_streaks.consecutive_days_by_excusal_status,
        sum(excusal_status_streaks.is_enrolled) over(
            partition by excusal_status_streaks.k_student, excusal_status_streaks.k_school) as total_days_enrolled,
        sum(excusal_status_streaks.is_absent) over(
            partition by excusal_status_streaks.k_student, excusal_status_streaks.k_school 
            order by excusal_status_streaks.calendar_date) as cumulative_days_absent,
        sum(excusal_status_streaks.is_present) over(
            partition by excusal_status_streaks.k_student, excusal_status_streaks.k_school 
            order by excusal_status_streaks.calendar_date) as cumulative_days_attended,
        sum(excusal_status_streaks.is_enrolled) over(
            partition by excusal_status_streaks.k_student, excusal_status_streaks.k_school
            order by excusal_status_streaks.calendar_date) as cumulative_days_enrolled,
        round(100 * cumulative_days_attended / nullif(cumulative_days_enrolled, 0), 2) as cumulative_attendance_rate,
        cumulative_days_enrolled >= {{ var('edu:attendance:chronic_absence_min_days') }} as meets_enrollment_threshold,
        {{ msr_chronic_absentee('cumulative_attendance_rate', 'cumulative_days_enrolled') }} as is_chronic_absentee,
        excusal_status_streaks.event_duration,
        excusal_status_streaks.school_attendance_duration
    from excusal_status_streaks
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
        
        -- custom data sources columns
        {{ add_cds_columns(custom_data_sources=custom_data_sources) }}
    from cumulatives
    left join metric_absentee_categories
        on cumulative_attendance_rate > metric_absentee_categories.threshold_lower
        and cumulative_attendance_rate <= metric_absentee_categories.threshold_upper
        
    -- custom data sources
    {{ add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='cumulatives', join_cols=['k_student', 'k_school', 'calendar_date']) }}
    {{ add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)

select * from metric_labels 



