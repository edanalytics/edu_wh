{{
  config(
    materialized='incremental',
    unique_key=['k_student', 'k_school', 'calendar_date'],
    post_hook=[
        "{% if not is_incremental() %} alter table {{ this }} alter column k_student set not null {% endif %}",
        "{% if not is_incremental() %} alter table {{ this }} alter column k_school set not null {% endif %}",
        "{% if not is_incremental() %} alter table {{ this }} alter column calendar_date set not null {% endif %}",
        "{% if not is_incremental() %} alter table {{ this }} add primary key (k_student, k_school, calendar_date) {% endif %}",
        "{% if not is_incremental() %} alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }} {% endif %}",
        "{% if not is_incremental() %} alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }} {% endif %}",
        "{% if not is_incremental() %} alter table {{ this }} add constraint fk_{{ this.name }}_calendar_date foreign key (k_calendar_date) references {{ ref('dim_calendar_date') }} {% endif %}",
    ]
  )
}}

with
{% if is_incremental() %}
affected_students as (
    select distinct k_student, k_school
    from {{ ref('int_student_daily_attendance') }}
    where att_last_modified_timestamp > (select max(att_last_modified_timestamp) from {{ this }})
       or enr_last_modified_timestamp > (select max(enr_last_modified_timestamp) from {{ this }})
),
{% endif %}
base as (
    {% if is_incremental() %}
    -- pull full history for affected students so window functions see complete data
    select b.*
    from {{ ref('int_student_daily_attendance') }} b
    inner join affected_students a
        on b.k_student = a.k_student and b.k_school = a.k_school
    {% else %}
    select * from {{ ref('int_student_daily_attendance') }}
    {% endif %}
),
metric_absentee_categories as (
    select * from {{ ref('absentee_categories') }}
),
cumulatives as (
    select
        base.k_student,
        base.k_student_xyear,
        base.k_school,
        base.k_calendar_date,
        base.calendar_date,
        base.k_session,
        base.tenant_code,
        base.school_year,
        base.attendance_event_category,
        base.attendance_event_reason,
        base.attendance_excusal_status,
        base.is_absent,
        base.is_present,
        base.is_enrolled,
        base.event_duration,
        base.school_attendance_duration,
        sum(is_enrolled) over(
            partition by base.k_student, base.k_school) as total_days_enrolled,
        sum(is_absent) over(
            partition by base.k_student, base.k_school
            order by base.calendar_date) as cumulative_days_absent,
        sum(is_present) over(
            partition by base.k_student, base.k_school
            order by base.calendar_date) as cumulative_days_attended,
        sum(is_enrolled) over(
            partition by base.k_student, base.k_school
            order by base.calendar_date) as cumulative_days_enrolled,
        round(100 * cumulative_days_attended / nullif(cumulative_days_enrolled, 0), 2) as cumulative_attendance_rate,
        cumulative_days_enrolled >= {{ var('edu:attendance:chronic_absence_min_days') }} as meets_enrollment_threshold,
        {{ msr_chronic_absentee('cumulative_attendance_rate', 'cumulative_days_enrolled') }} as is_chronic_absentee,
        base.att_last_modified_timestamp,
        base.enr_last_modified_timestamp,
        greatest(
            max(base.att_last_modified_timestamp) over (partition by base.k_student, base.k_school),
            max(base.enr_last_modified_timestamp) over (partition by base.k_student, base.k_school)
        ) as student_max_modified_timestamp
    from base
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
