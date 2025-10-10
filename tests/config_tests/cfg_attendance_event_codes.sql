{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with stg_attendance_events as (
    select * from {{ ref('stg_ef3__student_school_attendance_events') }}
),
xwalk_attendance_events as (
    select * from {{ ref('xwalk_attendance_events') }}
),
joined as (
    select distinct
        stg_attendance_events.tenant_code,
        stg_attendance_events.api_year,
        stg_attendance_events.attendance_event_category
    from stg_attendance_events
    left join xwalk_attendance_events
        on stg_attendance_events.attendance_event_category = xwalk_attendance_events.attendance_event_descriptor
    where xwalk_attendance_events.is_absent is null
)
select count(*) as failed_row_count, tenant_code, api_year from joined
group by all
having count(*) > 1