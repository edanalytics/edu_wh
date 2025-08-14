with stg_assessment_perf_levels as (
    select * from {{ ref('stg_ef3__assessments__performance_levels') }}
),
build_object as (
    select 
        tenant_code,
        api_year,
        k_assessment, 
        ARRAY_AGG(NAMED_STRUCT(
            'performance_level_name', performance_level_name,
            'performance_level_value', performance_level_value
            )) AS performance_levels_array
    from stg_assessment_perf_levels
    group by 1,2,3
)
select * from build_object