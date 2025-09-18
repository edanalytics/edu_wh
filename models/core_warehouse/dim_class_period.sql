{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_class_period set not null",
        "alter table {{ this }} add primary key (k_class_period)",
    ]
  )
}}

{% set custom_data_sources_name = "edu:class_period:custom_data_sources" %}
{% set attempt_military_whatever = var("edu:class_period:attempt_military_whatever", true) %}

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
        case when size(try_cast(v_meeting_times as array<string>)) = 1
        then
        {% if attempt_military_whatever == true -%}
            -- convert to military time for time math, if it isn't
            -- (assume class periods will not be scheduled between 1 and 6 AM)
            case 
                when date_part('HOUR', v_meeting_times:[0].startTime::timestamp) between 1 and 6
                    then dateadd(HOUR, 12, v_meeting_times:[0].startTime::timestamp)
                else v_meeting_times:[0].startTime::timestamp
            end
        {% else %}
            v_meeting_times:[0].startTime::timestamp
        {%- endif %}
        end as start_time,
        case when size(try_cast(v_meeting_times as array<string>)) = 1
        then 
        {% if attempt_military_whatever == true -%}
            case 
                when date_part('HOUR', v_meeting_times:[0].endTime::timestamp) between 1 and 6
                    then dateadd(HOUR, 12, v_meeting_times:[0].endTime::timestamp)
                else v_meeting_times:[0].endTime::timestamp
            end
        {% else %}
            v_meeting_times:[0].endTime::timestamp
        {%- endif %}
        end as end_time,
        timediff(MINUTE, start_time, end_time) as period_duration

        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from class_periods
    join dim_school
        on class_periods.k_school = dim_school.k_school

    -- custom data sources
    {{ add_cds_joins_v1(cds_model_config=custom_data_sources_name, driving_alias='class_periods', join_cols=['k_class_period']) }}
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select * from formatted
order by tenant_code, k_school, k_class_period