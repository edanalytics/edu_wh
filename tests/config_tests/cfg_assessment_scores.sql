{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}

with fct_student_assessment  as (
    select * from {{ ref('fct_student_assessment') }}
),

dim_assessment as (
    select * from {{ ref('dim_assessment') }}
),

xwalk_assessment_scores as (
    select * from {{ ref('xwalk_assessment_scores') }}
),

joined as (
    select distinct
        fct_student_assessment.tenant_code, 
        fct_student_assessment.school_year,
        dim_assessment.assessment_identifier,
        dim_assessment.assessment_family
    from fct_student_assessment
    join dim_assessment 
        on fct_student_assessment.k_assessment = 
            dim_assessment.k_assessment
    left join xwalk_assessment_scores
        -- Join on assessment_identifier and/or assessment_family if present in xwalk_assessment_scores
        on ifnull(xwalk_assessment_scores.assessment_identifier, '1') = iff(xwalk_assessment_scores.assessment_identifier is null, '1', dim_assessment.assessment_identifier)
        and ifnull(xwalk_assessment_scores.assessment_family, '1') = iff(xwalk_assessment_scores.assessment_family is null, '1', dim_assessment.assessment_family)

    where 
        xwalk_assessment_scores.assessment_identifier is null and xwalk_assessment_scores.assessment_family is null
    order by assessment_identifier
)

select * from joined