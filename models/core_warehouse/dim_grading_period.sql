{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_grading_period set not null",
        "alter table {{ this }} add primary key (k_grading_period)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}


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
    from stg_grading_periods
    join dim_school
        on stg_grading_periods.k_school = dim_school.k_school
)
select * from formatted
order by tenant_code, k_school, k_grading_period