{{
  config(
    materialized = 'ephemeral',
    )
}}
with calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),
school_calendar_windows as (
    select 
        tenant_code,
        k_school,
        school_year,
        k_school_calendar,
        min(calendar_date) as first_school_day,
        max(calendar_date) as last_school_day
    from calendar_date
    where is_school_day
    group by grouping sets (
        -- school level
        (tenant_code, k_school, school_year),
        (tenant_code, k_school, school_year, k_school_calendar)
    )
)
select * from school_calendar_windows