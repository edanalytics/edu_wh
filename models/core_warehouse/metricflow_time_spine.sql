{{
  config(
    materialized = 'table',
  )
}}

/*
  MetricFlow Time Spine
  
  This model generates a date dimension table required by the dbt semantic layer.
  It creates one row per day from 2015-01-01 through 10 years in the future.
  
  Reference: https://docs.getdbt.com/docs/build/metricflow-time-spine
*/

with days as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="dateadd(year, 10, current_date())"
    ) }}
),

final as (
    select
        cast(date_day as date) as date_day
    from days
)

select * from final
