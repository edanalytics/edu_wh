{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_objective_assessment set not null",
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
student_assessment_cross_tenant as (
    select * from {{ ref('bld_ef3__student_assessment_cross_tenant') }}
),
-- we need to use the student level data to determine which tenant/assessment combos we need records for
-- some of those actual tenant/assessment combos might exist already, but some might not
-- we need to create records when they don't exist, using the metadata from the original tenant
dedupe_cross_tenant_assessments as (
    {{
        dbt_utils.deduplicate(
            relation='student_assessment_cross_tenant',
            partition_by='k_assessment',
            order_by='tenant_code,school_year'
        )
    }}
),
obj_assessment_surrogate_keys as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['dedupe_cross_tenant_assessments.tenant_code',
            'dedupe_cross_tenant_assessments.api_year',
            'lower(stg_obj_assessments.assess_academic_subject)',
            'lower(stg_obj_assessments.academic_subject)',
            'lower(stg_obj_assessments.assessment_identifier)',
            'lower(stg_obj_assessments.namespace)',
            'lower(stg_obj_assessments.objective_assessment_identification_code)'
            ]
        ) }} as k_objective_assessment,
        stg_obj_assessments.k_objective_assessment as k_objective_assessment__original,
        dedupe_cross_tenant_assessments.k_assessment,
        dedupe_cross_tenant_assessments.k_assessment__original,
        dedupe_cross_tenant_assessments.tenant_code,
        dedupe_cross_tenant_assessments.school_year
    from stg_obj_assessments
    join dedupe_cross_tenant_assessments
        on stg_obj_assessments.k_assessment = k_assessment__original
),
formatted as (
    select
        coalesce(obj_assessment_surrogate_keys.k_objective_assessment, stg_obj_assessments.k_objective_assessment) as k_objective_assessment,
        coalesce(obj_assessment_surrogate_keys.k_assessment, stg_obj_assessments.k_assessment) as k_assessment,
        coalesce(obj_assessment_surrogate_keys.tenant_code, stg_obj_assessments.tenant_code) as tenant_code,
        coalesce(obj_assessment_surrogate_keys.api_year, stg_obj_assessments.api_year) as school_year,
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
     -- this could result in dupes if the assessment already exists, will dedupe below
    left join obj_assessment_surrogate_keys
        on stg_obj_assessments.k_objective_assessment = k_objective_assessment__original
),
dedupe_objective_assessments as (
    {{
        dbt_utils.deduplicate(
            relation='formatted',
            partition_by='k_assessment,k_objective_assessment',
            order_by='tenant_code,school_year'
        )
    }}
)
select * from dedupe_objective_assessments
order by tenant_code, k_objective_assessment