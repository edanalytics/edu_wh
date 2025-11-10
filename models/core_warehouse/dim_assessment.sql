{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_assessment set not null",
        "alter table {{ this }} add primary key (k_assessment)"
    ]
  )
}}

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
formatted as (
    select
        coalesce(dedupe_cross_tenant_assessments.k_assessment, stg_assessments.k_assessment) as k_assessment,
        coalesce(dedupe_cross_tenant_assessments.tenant_code, stg_assessments.tenant_code) as tenant_code,
        coalesce(dedupe_cross_tenant_assessments.api_year, stg_assessments.api_year) as school_year,
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
    from stg_assessments
    -- making all of these left joins because none of these are actually required
    left join assessment_scores 
        on stg_assessments.k_assessment = assessment_scores.k_assessment
    left join assessment_pls
        on stg_assessments.k_assessment = assessment_pls.k_assessment
    left join assessment_grades
        on stg_assessments.k_assessment = assessment_grades.k_assessment
    -- this could result in dupes if the assessment already exists, will dedupe below
    left join dedupe_cross_tenant_assessments
        on stg_assessments.k_assessment = k_assessment__original
),
dedupe_assessments as (
    {{
        dbt_utils.deduplicate(
            relation='formatted',
            partition_by='k_assessment',
            order_by='tenant_code,school_year'
        )
    }}
)
select * from dedupe_assessments
order by tenant_code, k_assessment