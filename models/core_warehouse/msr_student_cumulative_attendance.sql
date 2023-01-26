{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_school, school_year)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

with stu_daily_attendance as (
    select * from {{ ref('fct_student_daily_attendance') }}
),
dim_calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),
metric_absentee_categories as (
    select * from {{ ref('absentee_categories') }}
),
aggregated as (
    select 
        stu_daily_attendance.k_student,
        stu_daily_attendance.k_school,
        dim_calendar_date.school_year,
        any_value(stu_daily_attendance.tenant_code) as tenant_code,
        sum(is_absent) as days_absent,
        sum(is_present) as days_attended,
        sum(is_enrolled) as days_enrolled,
        case 
          when days_enrolled = 0 then null
          else round(100 * days_attended / days_enrolled, 2)
        end as  attendance_rate,
        days_enrolled >= {{ var('edu:attendance:chronic_absence_min_days') }} as meets_enrollment_threshold,
        {{ msr_chronic_absentee('attendance_rate', 'days_enrolled') }} as is_chronic_absentee
    from stu_daily_attendance
    join dim_calendar_date
        on stu_daily_attendance.k_calendar_date = dim_calendar_date.k_calendar_date
    group by 1,2,3
),
metric_labels as (
    select 
        aggregated.*,
        case 
            when meets_enrollment_threshold then metric_absentee_categories.level_numeric 
            else null 
        end as absentee_category_rank,
        case 
            when meets_enrollment_threshold then metric_absentee_categories.level_numeric 
            else null
        end as absentee_category_label
    from aggregated
    left join metric_absentee_categories
        on attendance_rate >= metric_absentee_categories.threshold_lower
        and attendance_rate < metric_absentee_categories.threshold_upper
)
select * from metric_labels
