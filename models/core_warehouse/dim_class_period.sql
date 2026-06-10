{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_class_period set not null",
        "alter table {{ this }} add primary key (k_class_period)",
    ]
  )
}}

with class_periods as (
    select * from {{ ref('stg_ef3__class_periods') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
formatted as (
    select 
        class_periods.k_class_period,
        dim_school.k_school,
        class_periods.tenant_code,
        class_periods.school_year,
        class_periods.class_period_name,
        class_periods.is_official_attendance_period,
        {{ edu_edfi_source.json_get_first('v_meeting_times', 'variant') }} as meeting_time,
        -- if there is only one meeting time, extract it, else leave null
        case when {{ edu_edfi_source.json_array_size('v_meeting_times') }} = 1
        then
            -- convert to military time for time math, if it isn't
            -- (assume class periods will not be scheduled between 1 and 6 AM)
            case 
                when left(meeting_time:startTime::string, 2)::int between 1 and 6
                then concat(left(meeting_time:startTime, 2)::int + 12, right(meeting_time:startTime::string, 6))
                else meeting_time:startTime::string
            end
        end as start_time,
        case when {{ edu_edfi_source.json_array_size('v_meeting_times') }} = 1
        then 
            case 
                when left(meeting_time:endTime::string, 2)::int between 1 and 6
                then concat(left(meeting_time:endTime::string, 2)::int + 12, right(meeting_time:endTime::string, 6))
                else meeting_time:endTime::string
            end
        end as end_time,
        case 
          when start_time <= end_time then timediff(MINUTE, concat('2020-01-01 ', start_time)::timestamp, concat('2020-01-01 ', end_time)::timestamp) 
          else timediff(MINUTE, concat('2020-01-01 ', start_time)::timestamp, dateadd(hour, 12 , concat('2020-01-01 ', end_time)::timestamp))   
        end as period_duration

    from class_periods
    join dim_school
        on class_periods.k_school = dim_school.k_school

),

add_custom_data_source as (
    {{ add_custom_data_source() }}
)

select {{ edu_edfi_source.star('add_custom_data_source', except=['meeting_time']) }}
from add_custom_data_source
order by tenant_code, k_school, k_class_period