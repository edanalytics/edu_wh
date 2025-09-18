{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_school_calendar set not null",
        "alter table {{ this }} add primary key (k_school_calendar)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

{% set custom_data_sources_name = "edu:school_calendar:custom_data_sources" %}

with stg_calendar as (
    select * from {{ ref('stg_ef3__calendars') }}
),
calendar_grades as (
    select * from {{ ref('stg_ef3__calendars__grade_levels') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
aggregated_grades as (
    select 
        k_school_calendar,
        array_agg(grade_level) as applicable_grade_levels_array
    from calendar_grades
    group by 1
),
formatted as (
    select 
        stg_calendar.k_school_calendar,
        stg_calendar.k_school,
        stg_calendar.tenant_code,
        stg_calendar.school_year,
        stg_calendar.calendar_code,
        stg_calendar.calendar_type,
        aggregated_grades.applicable_grade_levels_array

        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stg_calendar
    join dim_school
        on stg_calendar.k_school = dim_school.k_school
    left join aggregated_grades
        on stg_calendar.k_school_calendar = aggregated_grades.k_school_calendar

    -- custom data sources
    {{ add_cds_joins_v1(cds_model_config=custom_data_sources_name, driving_alias='stg_calendar', join_cols=['k_school_calendar']) }}
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select * from formatted
order by tenant_code, k_school, k_school_calendar