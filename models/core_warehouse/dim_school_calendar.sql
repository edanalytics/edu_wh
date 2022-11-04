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
        aggregated_grades.applicable_grade_levels_array
    from stg_calendar
    join dim_school
        on stg_calendar.k_school = dim_school.k_school
    left join aggregated_grades
        on stg_calendar.k_school_calendar = aggregated_grades.k_school_calendar
)
select * from formatted
order by tenant_code, k_school, k_school_calendar