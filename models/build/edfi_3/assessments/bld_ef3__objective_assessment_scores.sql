with stg_obj_assessment_scores as (
    select * from {{ ref('stg_ef3__objective_assessments__scores') }}
),
build_object as (
    select 
        k_objective_assessment,
        array_agg(score_name) as scores_array
    from stg_obj_assessment_scores
    group by 1
)
select * from build_object