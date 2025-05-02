{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_cohort set not null",
        "alter table {{ this }} add primary key (k_cohort)",
    ]
  )
}}

with stg_cohorts as (
    select * from {{ ref('stg_ef3__cohorts') }}
),
formatted as (
    select 
        stg_cohorts.k_cohort,
        stg_cohorts.k_lea,
        stg_cohorts.k_school,
        stg_cohorts.tenant_code,
        stg_cohorts.school_year,
        stg_cohorts.ed_org_id,
        stg_cohorts.ed_org_type,
        stg_cohorts.cohort_id,
        stg_cohorts.cohort_description,
        stg_cohorts.cohort_scope,
        stg_cohorts.cohort_type
    from stg_cohorts
)
select * from formatted
order by tenant_code, school_year desc, k_cohort