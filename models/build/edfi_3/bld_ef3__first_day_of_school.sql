{{
  config(
    materialized = 'ephemeral',
    )
}}
with calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),
first_school_day as (
    -- note: not including k_school_calendar or k_calendar_date
    -- because we need to use this in cases where we don't know 
    -- a student's calendar association
    select 
        tenant_code,
        k_school,
        school_year,
        min(calendar_date) as first_date
    from calendar_date
    where is_school_day
    group by 1,2,3
)
select * from first_school_day