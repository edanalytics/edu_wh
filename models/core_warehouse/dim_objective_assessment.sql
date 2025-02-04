  -- depends_on: {{ ref('stg_ef3__objective_assessments') }}
{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_objective_assessment)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_assessment foreign key (k_assessment) references {{ ref('dim_assessment') }}"
    ]
  )
}}

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
        stg_obj_assessments.k_parent_objective_assessment,
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
    from stg_obj_assessments
    -- making all of these left joins because none of these are actually required
    left join obj_assessment_scores 
        on stg_obj_assessments.k_objective_assessment = obj_assessment_scores.k_objective_assessment
    left join obj_assessment_pls
        on stg_obj_assessments.k_objective_assessment = obj_assessment_pls.k_objective_assessment
)
select * from formatted
order by tenant_code, k_objective_assessment
