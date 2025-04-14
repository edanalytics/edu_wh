{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_school_calendar)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

with stg_calendar as (
    select * from {{ ref('stg_ef3__calendars') }}
),
calendar_grades as (
    select * from {{ ref('stg_ef3__calendars__grade_levels') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
calendar_event_summary as (
    select * from {{ ref('bld_ef3__calendar_event_summary') }}
),
stg_calendar_dates as (
    select* from {{ ref('stg_ef3__calendar_dates') }}
),
calendar_descriptives as (
    select 
        calendar_event_summary.k_school_calendar,
        min(stg_calendar_dates.calendar_date) as min_calendar_date,
        max(stg_calendar_dates.calendar_date) as max_calendar_date,
        count(*) as total_school_days
    from calendar_event_summary
    join stg_calendar_dates
        on calendar_event_summary.k_calendar_date = stg_calendar_dates.k_calendar_date
    where is_school_day
    group by all
)
aggregated_grades as (
    select 
        k_school_calendar,
        array_agg(grade_level) as applicable_grade_levels_array
    from calendar_grades
    group by 1
),
formatted as (
    select 
        stg_calendar.k_school_calendar,
        stg_calendar.k_school,
        stg_calendar.tenant_code,
        stg_calendar.school_year,
        stg_calendar.calendar_code,
        stg_calendar.calendar_type,
        calendar_descriptives.min_calendar_date,
        calendar_descriptives.max_calendar_date,
        calendar_descriptives.total_school_days
        calendar_descriptives.total_school_days < {{ var('edu:calendar:nonstandard_threshold', 60) }} as is_nonstandard_calendar,
        aggregated_grades.applicable_grade_levels_array
    from stg_calendar
    join dim_school
        on stg_calendar.k_school = dim_school.k_school
    left join aggregated_grades
        on stg_calendar.k_school_calendar = aggregated_grades.k_school_calendar
    left join calendar_descriptives 
        on stg_calendar.k_school_calendar = calendar_descriptives.k_school_calendar
)
select * from formatted
order by tenant_code, k_school, k_school_calendar
