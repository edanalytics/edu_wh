with stg_assessment_scores as (
    select * from {{ ref('stg_ef3__assessments__scores') }}
),
build_object as (
    select 
        tenant_code,
        api_year,
        k_assessment,
        array_agg(score_name) as scores_array
    from stg_assessment_scores
    group by 1,2,3
)
select * from build_object