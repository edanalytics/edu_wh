with fct_student_assessment  as (
    select * from analytics.prod_wh.fct_student_assessment
),
dim_assessment as (
    select * from analytics.prod_wh.dim_assessment
),
xwalk_assessment_scores as (
    select * from analytics.prod_seed.xwalk_assessment_scores
),
joined as (
    select distinct
        dim_assessment.assessment_identifier,
        fct_student_assessment.tenant_code, 
        fct_student_assessment.school_year
    from fct_student_assessment
    join dim_assessment 
        on fct_student_assessment.k_assessment = 
            dim_assessment.k_assessment
    left join xwalk_assessment_scores
        on dim_assessment.assessment_identifier = 
            xwalk_assessment_scores.assessment_identifier

    where xwalk_assessment_scores.assessment_identifier is null
    order by assessment_identifier
)
select * from joined