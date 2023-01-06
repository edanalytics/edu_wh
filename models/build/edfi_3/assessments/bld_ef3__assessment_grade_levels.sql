with stg_assessment_grades as (
    select * from {{ ref('stg_ef3__assessments__grade_levels') }}
),
build_object as (
    select 
        tenant_code,
        api_year,
        k_assessment,
        array_agg(grade_level) as grades_array
    from stg_assessment_grades
    group by 1,2,3
)
select * from build_object