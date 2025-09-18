{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_assessment set not null",
        "alter table {{ this }} add primary key (k_assessment)"
    ]
  )
}}

{% set custom_data_sources_name = "edu:assessment:custom_data_sources" %}

with stg_assessments as (
    select * from {{ ref('stg_ef3__assessments') }}
),
assessment_scores as (
    select * from {{ ref('bld_ef3__assessment_scores') }}
),
assessment_pls as (
    select * from {{ ref('bld_ef3__assessment_performance_levels') }}
),
assessment_grades as (
    select * from {{ ref('bld_ef3__assessment_grade_levels') }}
),
formatted as (
    select
        stg_assessments.k_assessment,
        stg_assessments.tenant_code,
        stg_assessments.api_year as school_year,
        stg_assessments.assessment_identifier,
        stg_assessments.namespace,
        stg_assessments.assessment_title,
        stg_assessments.academic_subject,
        stg_assessments.is_adaptive_assessment,
        stg_assessments.assessment_family,
        stg_assessments.assessment_form,
        stg_assessments.assessment_version,
        stg_assessments.max_raw_score,
        stg_assessments.nomenclature,
        stg_assessments.revision_date,
        stg_assessments.assessment_category,
        stg_assessments.assessment_period_begin_date,
        stg_assessments.assessment_period_end_date,
        stg_assessments.content_standard,
        assessment_scores.scores_array,
        assessment_pls.performance_levels_array,
        assessment_grades.grades_array

        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stg_assessments
    -- making all of these left joins because none of these are actually required
    left join assessment_scores 
        on stg_assessments.k_assessment = assessment_scores.k_assessment
    left join assessment_pls
        on stg_assessments.k_assessment = assessment_pls.k_assessment
    left join assessment_grades
        on stg_assessments.k_assessment = assessment_grades.k_assessment

    -- custom data sources
    {{ add_cds_joins_v1(cds_model_config=custom_data_sources_name, driving_alias='stg_assessments', join_cols=['k_assessment']) }}
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select * from formatted
order by tenant_code, k_assessment