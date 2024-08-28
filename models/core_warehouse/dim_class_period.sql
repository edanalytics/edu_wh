{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_class_period set not null",
        "alter table {{ this }} add primary key (k_class_period)",
    ]
  )
}}
{# Load custom data sources from var #}
{% set custom_data_sources = var("edu:class_period:custom_data_sources", []) %}

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
            -- convert to military time for time math, if it isn't
            -- (assume class periods will not be scheduled between 1 and 6 AM)
            case 
                when date_part('HOUR', v_meeting_times:[0].startTime::timestamp) between 1 and 6
                then dateadd(HOUR, 12, v_meeting_times:[0].startTime::timestamp)
                else v_meeting_times:[0].startTime::timestamp
            end
        end as start_time,
        case when size(try_cast(v_meeting_times as array<string>)) = 1
        then 
            case 
                when date_part('HOUR', v_meeting_times:[0].endTime::timestamp) between 1 and 6
                then dateadd(HOUR, 12, v_meeting_times:[0].endTime::timestamp)
                else v_meeting_times:[0].endTime::timestamp
            end
        end as end_time,
        timediff(MINUTE, start_time, end_time) as period_duration

        -- custom indicators
        {% if custom_data_sources is not none and custom_data_sources | length -%}
          {%- for source in custom_data_sources -%}
            {%- for indicator in custom_data_sources[source] -%}
              , {{ custom_data_sources[source][indicator]['where'] }} as {{ indicator }}
            {%- endfor -%}
          {%- endfor -%}
        {%- endif %}
    from class_periods
    join dim_school
        on class_periods.k_school = dim_school.k_school
    -- custom data sources
    {% if custom_data_sources is not none and custom_data_sources | length -%}
      {%- for source in custom_data_sources -%}
        left join {{ ref(source) }}
          on class_periods.k_class_period = {{ source }}.k_class_period
      {% endfor %}
    {%- endif %}
)
select * from formatted
order by tenant_code, k_school, k_class_period