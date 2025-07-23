with stg_performance_evaluations as (
    select * from {{ ref('stg_tpdm__performance_evaluations') }}
),

stg_performance_evaluation_ratings as (
    select * from {{ ref('stg_tpdm__performance_evaluation_ratings') }}
),

formatted as (
    select
        stg_performance_evaluation_ratings.k_performance_evaluation,
        stg_performance_evaluations.ed_org_id,
        stg_performance_evaluations.performance_evaluation_title,
        stg_performance_evaluations.performance_evaluation_type,
        stg_performance_evaluations.school_year,
        stg_performance_evaluations.academic_term,
        stg_performance_evaluations.performance_evaluation_description,
        stg_performance_evaluations.academic_subject,
        stg_performance_evaluations.v_grade_levels,
        stg_performance_evaluations.v_rating_levels
    from stg_performance_evaluation_ratings
    join stg_performance_evaluations
        on stg_performance_evaluation_ratings.k_performance_evaluation = stg_performance_evaluations.k_performance_evaluation
)
select * from formatted