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
        -- if there is only one start time, extract it, else leave null
        case when array_size(v_meeting_times) = 1
        then
            -- convert to military time for time math, if it isn't
            -- (assume class periods will not be scheduled between 1 and 6 AM)
            case 
                when date_part(hour, v_meeting_times[0]['startTime']::time) between 1 and 6
                then timeadd(hours, 12, v_meeting_times[0]['startTime']::time)
                else v_meeting_times[0]['startTime']::time
            end
        end as start_time,
        case when array_size(v_meeting_times) = 1
        then 
            case 
                when date_part(hour, v_meeting_times[0]['endTime']::time) between 1 and 6
                then timeadd(hours, 12, v_meeting_times[0]['endTime']::time)
                else v_meeting_times[0]['endTime']::time
            end
        end as end_time,
        timediff(minutes, start_time, end_time) as period_duration

    from class_periods
    join dim_school
        on class_periods.k_school = dim_school.k_school
)
select * from formatted
order by tenant_code, k_school, k_class_period