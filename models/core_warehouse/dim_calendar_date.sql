{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_calendar_date set not null",
        "alter table {{ this }} add primary key (k_calendar_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school_calendar foreign key (k_school_calendar) references {{ ref('dim_school_calendar') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}
{# Load custom data sources from var #}
{% set custom_data_sources = var("edu:calendar_date:custom_data_sources", []) %}

with stg_calendar_date as (
    select * from {{ ref('stg_ef3__calendar_dates') }}
),

dim_school_calendar as (
    select * from {{ ref('dim_school_calendar') }}
),
summarize_calendar_events as (
    select * from {{ ref('bld_ef3__calendar_event_summary') }}
),
formatted as (
    select 
        stg_calendar_date.k_calendar_date,
        stg_calendar_date.k_school_calendar,
        dim_school_calendar.k_school,
        stg_calendar_date.tenant_code,
        stg_calendar_date.school_year,
        stg_calendar_date.calendar_date,
        case 
            when array_size(summarize_calendar_events.calendar_events_array) = 1
                then summarize_calendar_events.calendar_events_array[0]
            when array_size(summarize_calendar_events.calendar_events_array) > 1
                then 'Multiple'
            else null
        end as calendar_event,
        summarize_calendar_events.is_school_day,
        dim_school_calendar.calendar_code,
        dim_school_calendar.calendar_type,
        summarize_calendar_events.calendar_events_array
    from stg_calendar_date
    join dim_school_calendar
        on stg_calendar_date.k_school_calendar = dim_school_calendar.k_school_calendar
    join summarize_calendar_events
        on stg_calendar_date.k_calendar_date = summarize_calendar_events.k_calendar_date
),
-- augment calendar with useful summary tools
augmented as (
    select 
        formatted.*,
        -- incrementing count of school days within the year
        row_number() over(partition by k_school_calendar, is_school_day
                          order by calendar_date) as day_of_school_year,
        dayname(calendar_date) as week_day,
        weekofyear(calendar_date) as week_of_calendar_year
    from formatted
),
-- find the week of the first day of school, for calculating week of school year
week_offset as (
    select 
        k_school_calendar,
        week_of_calendar_year - 1 as start_week_offset
    from augmented
    where day_of_school_year = 1
    and is_school_day
),
week_calculation as (
    select 
        augmented.k_calendar_date,
        augmented.k_school_calendar,
        k_school,
        tenant_code,
        calendar_code,
        school_year,
        calendar_date,
        calendar_event,
        calendar_events_array,
        is_school_day,
        case when is_school_day then day_of_school_year
            else null
        end as day_of_school_year,
        week_day,
        week_of_calendar_year,
        -- math to determine week of the school year, accounting for spanning across calendar years
        case 
            when not is_school_day then null
            when week_of_calendar_year >= start_week_offset
                then week_of_calendar_year - start_week_offset
            else week_of_calendar_year + 52 - start_week_offset
        end as week_of_school_year

        -- custom indicators
        {% if custom_data_sources is not none and custom_data_sources | length -%}
          {%- for source in custom_data_sources -%}
            {%- for indicator in custom_data_sources[source] -%}
              , {{ custom_data_sources[source][indicator]['where'] }} as {{ indicator }}
            {%- endfor -%}
          {%- endfor -%}
        {%- endif %}
    from augmented
    join week_offset
        on augmented.k_school_calendar = week_offset.k_school_calendar
    -- custom data sources
    {% if custom_data_sources is not none and custom_data_sources | length -%}
      {%- for source in custom_data_sources -%}
        left join {{ ref(source) }}
          on augmented.k_calendar_date = {{ source }}.k_calendar_date
      {% endfor %}
    {%- endif %}
)
select * from week_calculation
order by tenant_code, k_school, calendar_date desc
