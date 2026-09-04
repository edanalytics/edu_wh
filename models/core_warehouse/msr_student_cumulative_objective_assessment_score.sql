with fct_student_objective_assessment as (
    select * from {{ ref('fct_student_objective_assessment') }}
),

dim_assessment as (
    select * from {{ ref('dim_assessment') }}
),

dim_objective_assessment as (
    select * from {{ ref('dim_objective_assessment') }}
),

annual_scores as (

    select
        fct_student_objective_assessment.k_student_xyear,
        fct_student_objective_assessment.tenant_code,
        fct_student_objective_assessment.school_year,
        dim_assessment.assessment_identifier,
        dim_objective_assessment.objective_assessment_identification_code,
        max_by(dim_objective_assessment.k_objective_assessment, fct_student_objective_assessment.school_year) as k_objective_assessment__latest_year,
        max(fct_student_objective_assessment.scale_score) as max_scale_score,
        max(fct_student_objective_assessment.performance_level) as max_performance_level
        {# TODO dunno if we need this -- could technically differ from max_performance_level. #}
        {# max_by(fct_student_objective_assessment.performance_level, fct_student_objective_assessment.scale_score) as implied_max_performance_level #}

    from fct_student_objective_assessment

    join dim_assessment
        on fct_student_objective_assessment.k_assessment = dim_assessment.k_assessment

    join dim_objective_assessment
        on fct_student_objective_assessment.k_objective_assessment = dim_objective_assessment.k_objective_assessment

    group by 1, 2, 3, 4, 5

),

cumulative_scores as (

    select
        annual_scores.k_student_xyear,
        annual_scores.tenant_code,
        annual_scores.school_year,
        annual_scores.assessment_identifier,
        annual_scores.objective_assessment_identification_code,
        max_by(annual_scores.k_objective_assessment__latest_year, annual_scores.max_scale_score) over (
            partition by annual_scores.k_student_xyear, annual_scores.assessment_identifier, annual_scores.objective_assessment_identification_code
            order by annual_scores.school_year
            rows between unbounded preceding and current row
        ) as k_objective_assessment__best_year, {# TODO what if max (pl) is from a different k_objective_assessment? uh oh #}
        max(annual_scores.max_scale_score) over (
            partition by annual_scores.k_student_xyear, annual_scores.assessment_identifier, annual_scores.objective_assessment_identification_code
            order by annual_scores.school_year
            rows between unbounded preceding and current row
        ) as max_scale_score,
        {# TODO dunno if we need this #}
        {# max_by(annual_scores.implied_max_performance_level, annual_scores.max_scale_score) over (
            partition by annual_scores.k_student_xyear, annual_scores.assessment_identifier, annual_scores.objective_assessment_identification_code
            order by annual_scores.school_year
            rows between unbounded preceding and current row
        ) as implied_max_performance_level, #}
        max(annual_scores.max_performance_level) over (
            partition by annual_scores.k_student_xyear, annual_scores.assessment_identifier, annual_scores.objective_assessment_identification_code
            order by annual_scores.school_year
            rows between unbounded preceding and current row
        ) as max_performance_level

    from annual_scores

)

select * from cumulative_scores
