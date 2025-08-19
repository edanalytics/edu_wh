with assessment_family_lookup as (
        select distinct
            assessment_identifier,
            namespace,
            assessment_family
        from {{ ref('dim_assessment') }}
        where assessment_family is not null
),
score_results as (
    select * from {{ ref('stg_ef3__student_assessments__score_results') }}
),
xwalk_scores as (
    select * from {{ ref('xwalk_assessment_scores') }}
),
performance_levels as (
    select
        tenant_code,
        api_year,
        k_student_assessment,
        assessment_identifier,
        namespace,
        -- normalize column names to stack with scores
        performance_level_name as score_name,
        performance_level_result as score_result
    from {{ ref('stg_ef3__student_assessments__performance_levels') }}
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
            partition_by='k_student_assessment, score_name',
            order_by='score_result desc'
        )
    }}
),
merged_xwalk as (
    select
        tenant_code,
        api_year,
        k_student_assessment,
        score_name as original_score_name,
        coalesce(normalized_score_name, 'other') as normalized_score_name,
        score_result
    from dedupe_results
    left join assessment_family_lookup
        on dedupe_results.assessment_identifier = assessment_family_lookup.assessment_identifier
        and dedupe_results.namespace = assessment_family_lookup.namespace
    left join xwalk_scores
        on dedupe_results.namespace = xwalk_scores.namespace
        and dedupe_results.score_name = xwalk_scores.original_score_name
        -- join on assessment_family and/or assessment_identifier if the fields have been entered in the xwalk.
        and ifnull(xwalk_scores.assessment_family, '1') = iff(xwalk_scores.assessment_family is null, '1', assessment_family_lookup.assessment_family)
        and ifnull(xwalk_scores.assessment_identifier, '1') = iff(xwalk_scores.assessment_identifier is null, '1', dedupe_results.assessment_identifier)
) 
select * from merged_xwalk