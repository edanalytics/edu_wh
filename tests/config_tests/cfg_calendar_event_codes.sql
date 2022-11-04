{#- 
Ensure that the cross-walking of calendar events completely covers data in Ed-Fi.
 todo: 
 - add test configs
 - warn severity, optionally keep in audit table
 -#}
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with stg_calendar_events as (
    select * from {{ ref('stg_ef3__calendar_dates__calendar_events') }}
),
xwalk_calendar_events as (
    select * from {{ ref('xwalk_calendar_events') }}
),
joined as (
    select distinct
        stg_calendar_events.tenant_code,
        stg_calendar_events.api_year,
        stg_calendar_events.calendar_event
    from stg_calendar_events
    left join xwalk_calendar_events
        on stg_calendar_events.calendar_event = xwalk_calendar_events.calendar_event_descriptor
    where xwalk_calendar_events.is_school_day is null
)
select * from joined