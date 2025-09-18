{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_objective_assessment set not null",
        "alter table {{ this }} add primary key (k_objective_assessment)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_assessment foreign key (k_assessment) references {{ ref('dim_assessment') }}"
    ]
  )
}}

{% set custom_data_sources_name = "edu:objective_assessment:custom_data_sources" %}

with stg_obj_assessments as (
    select * from {{ ref('stg_ef3__objective_assessments') }}
),
obj_assessment_scores as (
    select * from {{ ref('bld_ef3__objective_assessment_scores') }}
),
obj_assessment_pls as (
    select * from {{ ref('bld_ef3__objective_assessment_performance_levels') }}
),
formatted as (
    select
        stg_obj_assessments.k_objective_assessment,
        stg_obj_assessments.k_assessment,
        stg_obj_assessments.tenant_code,
        stg_obj_assessments.api_year as school_year,
        stg_obj_assessments.assessment_identifier,
        stg_obj_assessments.namespace,
        stg_obj_assessments.objective_assessment_description,
        stg_obj_assessments.objective_assessment_identification_code,
        stg_obj_assessments.max_raw_score,
        stg_obj_assessments.nomenclature,
        stg_obj_assessments.percent_of_assessment,
        stg_obj_assessments.academic_subject,
        obj_assessment_scores.scores_array,
        obj_assessment_pls.performance_levels_array

        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stg_obj_assessments
    -- making all of these left joins because none of these are actually required
    left join obj_assessment_scores 
        on stg_obj_assessments.k_objective_assessment = obj_assessment_scores.k_objective_assessment
    left join obj_assessment_pls
        on stg_obj_assessments.k_objective_assessment = obj_assessment_pls.k_objective_assessment

    -- custom data sources
    {{ add_cds_joins_v1(cds_model_config=custom_data_sources_name, driving_alias='stg_obj_assessments', join_cols=['k_objective_assessment']) }}
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select * from formatted
order by tenant_code, k_objective_assessment