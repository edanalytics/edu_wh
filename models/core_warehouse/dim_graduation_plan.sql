{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_graduation_plan set not null",
        "alter table {{ this }} add primary key (k_graduation_plan)"
    ]
  )
}}

{% set custom_data_sources_name = "edu:graduation_plan:custom_data_sources" %}

with stg_graduation_plans as (
    select * from {{ ref('stg_ef3__graduation_plans') }}
),
formatted as (
    select 
        stg_graduation_plans.k_graduation_plan,
        stg_graduation_plans.tenant_code,
        stg_graduation_plans.school_year,
        stg_graduation_plans.k_lea,
        stg_graduation_plans.k_school,
        stg_graduation_plans.ed_org_id,
        stg_graduation_plans.ed_org_type,
        stg_graduation_plans.graduation_school_year,
        stg_graduation_plans.graduation_plan_type,
        stg_graduation_plans.total_required_credit_type,
        stg_graduation_plans.total_required_credit_conversion,
        stg_graduation_plans.total_required_credits,
        stg_graduation_plans.is_individual_plan,
        stg_graduation_plans.v_credits_by_credit_categories,
        stg_graduation_plans.v_credits_by_courses,
        stg_graduation_plans.v_credits_by_subjects,
        stg_graduation_plans.v_required_assessments

        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__graduation_plans', flatten=False) }}

        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stg_graduation_plans

    -- custom data sources
    {{ add_cds_joins_v1(cds_model_config=custom_data_sources_name, driving_alias='stg_graduation_plans', join_cols=['k_graduation_plan']) }}
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select * from formatted
order by tenant_code, k_graduation_plan