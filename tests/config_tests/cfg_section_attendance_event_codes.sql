{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with stg_section_attendance_events as (
    select * from {{ ref('stg_ef3__student_section_attendance_events') }}
),
xwalk_attendance_events as (
    select * from {{ ref('xwalk_attendance_events') }}
),
joined as (
    select distinct
        stg_section_attendance_events.tenant_code,
        stg_section_attendance_events.api_year,
        stg_section_attendance_events.attendance_event_category
    from stg_section_attendance_events
    left join xwalk_attendance_events
        on stg_section_attendance_events.attendance_event_category = xwalk_attendance_events.attendance_event_descriptor
    where xwalk_attendance_events.is_absent is null
)
select * from joined