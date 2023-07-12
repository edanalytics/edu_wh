with score_results as (
    select * from {{ ref('stg_ef3__student_objective_assessments__score_results') }}
),
xwalk_scores as (
    select * from {{ ref('xwalk_objective_assessment_scores') }}
),
xwalk_score_values as (
    select * from {{ ref('xwalk_assessment_score_values') }}
),
xwalk_score_value_thresholds as (
    select * from {{ ref('xwalk_assessment_score_value_thresholds') }}
),
performance_levels as (
    select
        tenant_code,
        api_year,
        k_student_objective_assessment,
        assessment_identifier,
        namespace,
        objective_assessment_identification_code,
        -- normalize column names to stack with scores
        performance_level_name as score_name,
        performance_level_result as score_result
    from {{ ref('stg_ef3__student_objective_assessments__performance_levels') }}
),
stack_results as (
    select * from score_results
    union all 
    select * from performance_levels
),
-- we have seen examples of the say key existing in both score results and PLs
-- this will choose the highest score result
dedupe_results as (
    {{
        dbt_utils.deduplicate(
            relation='stack_results',
            partition_by='k_student_objective_assessment, score_name',
            order_by='score_result desc'
        )
    }}
),
merged_xwalk as (
    select
        tenant_code,
        api_year,
        k_student_objective_assessment,
        score_name as original_score_name,
        coalesce(xwalk_scores.normalized_score_name, 'other') as normalized_score_name,
        score_result,
        coalesce(xwalk_score_value_thresholds.normalized_score_result::varchar,
                 xwalk_score_values.normalized_score_result::varchar,
                 score_result::varchar
                ) as normalized_score_result
    from dedupe_results
    left join xwalk_scores
        on dedupe_results.assessment_identifier = xwalk_scores.assessment_identifier
        and dedupe_results.namespace = xwalk_scores.namespace
        and dedupe_results.objective_assessment_identification_code = xwalk_scores.objective_assessment_identification_code
        and dedupe_results.score_name = xwalk_scores.original_score_name
    left join xwalk_score_values
        on dedupe_results.assessment_identifier = xwalk_score_values.assessment_identifier
        and dedupe_results.namespace = xwalk_score_values.namespace
        and xwalk_scores.normalized_score_name = xwalk_score_values.normalized_score_name
        and dedupe_results.score_result = xwalk_score_values.original_score_result
    left join xwalk_score_value_thresholds
        on dedupe_results.assessment_identifier = xwalk_score_value_thresholds.assessment_identifier
        and dedupe_results.namespace = xwalk_score_value_thresholds.namespace
        and xwalk_scores.normalized_score_name = xwalk_score_value_thresholds.normalized_score_name
        -- todo check these comparators -- what if there's a value between the upper and next lower? eg value is 20.4 and the cutoffs are 20 and 21
        -- todo review my use of try_to_numeric here -- the idea is to allow numeric values to merge, otherwise don't merge without error
        and try_to_numeric(dedupe_results.score_result) >= xwalk_score_value_thresholds.lower_bound
        and try_to_numeric(dedupe_results.score_result) <= xwalk_score_value_thresholds.upper_bound
        -- todo in future, may need to include subject & grade level in this join (with options to join across subjects)
)
select * from merged_xwalk