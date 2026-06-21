with stg_calendar_events as (
    select * from {{ ref('stg_ef3__calendar_dates__calendar_events')}}
),
xwalk_calendar_events as (
    select * from {{ ref('xwalk_calendar_events') }}
),
summarize_calendar_events as (
    select 
        stg_calendar_events.tenant_code,
        stg_calendar_events.k_calendar_date,
        -- if there are multiple events on a day, having at least one 
        -- that counts as a school day applies to the whole day
        sum(xwalk_calendar_events.is_school_day::integer) >= {{ var("edu:attendance:num_school_day_calendar_events", 1) }} as is_school_day,
        array_agg(stg_calendar_events.calendar_event) as calendar_events_array
    from stg_calendar_events
    join xwalk_calendar_events
        on stg_calendar_events.calendar_event = xwalk_calendar_events.calendar_event_descriptor
    group by all
)
select * from summarize_calendar_events