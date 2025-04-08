with stg_assessment_perf_levels as (
    select * from {{ ref('stg_ef3__assessments__performance_levels') }}
),
build_object as (
    select 
        tenant_code,
        api_year,
        k_assessment,
        {{
            json_array_agg(
                json_object_construct(
                    [['performance_level_name', 'performance_level_name'],
                     ['performance_level_value', 'performance_level_value']]
                ),
            is_terminal=True)
        }} as performance_levels_array
    from stg_assessment_perf_levels
    group by 1,2,3
)
select * from build_object