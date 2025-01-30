
{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_school, calendar_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}"    ]
  )
}}

{%- set xwalk_att_events_cols = dbt_utils.get_filtered_columns_in_relation(from = ref('xwalk_attendance_events')) -%}

with fct_student_daily_att as (
    select * from {{ ref('fct_student_daily_attendance') }}
),
xwalk_att_events as (
    select * from {{ ref('xwalk_attendance_events') }}
),
joined_student_daily_att as (
    select 
      fct_student_daily_att.*
    {%- if 'IS_UNEXCUSED' in xwalk_att_events_cols %}
      , xwalk_att_events.is_unexcused
    {%- endif %}
    from fct_student_daily_att
    join xwalk_att_events
    on fct_student_daily_att.attendance_event_category = xwalk_att_events.attendance_event_descriptor
),
consecutive as (
    select 
      *,
      lag(calendar_date) over (
        partition by k_student, k_school, attendance_event_category order by calendar_date) as previous_date,
      -- a column to indicate whether or not there has been a change in `attendance_event_category` for a student ordered by `calendar_date`.
      -- if there is no school day on a weekend and no change of `attendance_event_category` from Friday to Monday,
      -- it still counts as consecutive.
      case 
        when previous_date is null then 1
        when datediff(day, previous_date, calendar_date) = 1 
          or dayofweek(previous_date) = 5 and dayofweek(calendar_date) = 1 and datediff(day, previous_date, calendar_date) = 3 then 0
        else 1
        end as consecutive
    from joined_student_daily_att 
    order by k_student, k_school, calendar_date
),
consecutive_grouping as (
    select 
      *,
      -- all consecutive records of a student's `attendance_event_category` has the same `consecutive_group`.
      sum(consecutive) over (partition by k_student, k_school, attendance_event_category order by calendar_date) as consecutive_group
    from consecutive
),
consecutive_absences as (
    select 
      k_student,
      k_student_xyear,
      k_school,
      k_calendar_date,
      calendar_date,
      k_session,
      tenant_code,
      is_absent,
      is_present,
      is_enrolled,
      total_days_enrolled,
      cumulative_days_absent,
      cumulative_days_attended,
      cumulative_days_enrolled,
      cumulative_attendance_rate,
      meets_enrollment_threshold,
      is_chronic_absentee,
      event_duration,
      school_attendance_duration,
      absentee_category_rank,
      absentee_category_label,
  {%- if 'IS_UNEXCUSED' in xwalk_att_events_cols %}
      is_unexcused,
  {%- endif %}
      attendance_event_category,
      -- the consecutive count of attendance per student per school 
      row_number() over ( partition by k_student, k_school, attendance_event_category, consecutive_group order by calendar_date) as consecutive_day_number,
      
    from consecutive_grouping
)

select * from consecutive_absences 
order by k_student, k_school, calendar_date, consecutive_day_number
