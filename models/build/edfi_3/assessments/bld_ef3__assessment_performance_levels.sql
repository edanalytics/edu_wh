with stg_assessment_perf_levels as (
    select * from {{ ref('stg_ef3__assessments__performance_levels') }}
),
build_object as (
    select 
        k_assessment,
        array_agg(object_construct('performance_level_name', performance_level_name,
                                   'performance_level_value', performance_level_value)) as performance_levels_array
    from stg_assessment_perf_levels
    group by 1
)
select * from build_object