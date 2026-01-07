{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_grading_period set not null",
        "alter table {{ this }} add primary key (k_grading_period)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

{{ cds_depends_on('edu:grading_period:custom_data_sources') }}
{% set custom_data_sources = var('edu:grading_period:custom_data_sources', []) %}

with stg_grading_periods as (
    select * from {{ ref('stg_ef3__grading_periods') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
formatted as (
    select 
        stg_grading_periods.k_grading_period,
        stg_grading_periods.k_school,
        stg_grading_periods.tenant_code,
        stg_grading_periods.grading_period,
        stg_grading_periods.period_sequence,
        stg_grading_periods.school_year,
        stg_grading_periods.begin_date,
        stg_grading_periods.end_date,
        stg_grading_periods.total_instructional_days

        -- custom data sources columns
        {{ add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_grading_periods
    join dim_school
        on stg_grading_periods.k_school = dim_school.k_school

    -- custom data sources
    {{ add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='stg_grading_periods', join_cols=['k_grading_period']) }}
    {{ add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from formatted
order by tenant_code, k_school, k_grading_period