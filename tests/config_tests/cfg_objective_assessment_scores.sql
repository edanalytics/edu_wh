
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with fct_student_objective_assessment  as (
    select * from {{ ref('fct_student_objective_assessment') }}
),
dim_objective_assessment as (
    select * from {{ ref('dim_objective_assessment') }}
),
xwalk_objective_assessment_scores as (
    select * from {{ ref('xwalk_objective_assessment_scores') }}
),
joined as (
    select distinct
        dim_objective_assessment.assessment_identifier,
        dim_objective_assessment.objective_assessment_identification_code,
        fct_student_objective_assessment.tenant_code, 
        fct_student_objective_assessment.school_year
    from fct_student_objective_assessment
    join dim_objective_assessment 
        on fct_student_objective_assessment.k_objective_assessment = 
            dim_objective_assessment.k_objective_assessment
    left join xwalk_objective_assessment_scores
        on (dim_objective_assessment.assessment_identifier = 
            xwalk_objective_assessment_scores.assessment_identifier
        and dim_objective_assessment.objective_assessment_identification_code = 
            xwalk_objective_assessment_scores.objective_assessment_identification_code)

    where xwalk_objective_assessment_scores.objective_assessment_identification_code is null
)
select * from joined
