with stg_perf_eval_rating as (
    select * from {{ ref('stg_tpdm__performance_evaluation_ratings') }}
),

formatted as (
    select
        stg_perf_eval_rating.k_person,
        stg_perf_eval_rating.k_performance_evaluation,
        stg_perf_eval_rating.tenant_code,
        stg_perf_eval_rating.school_year,
        stg_perf_eval_rating.ed_org_id,
        stg_perf_eval_rating.evaluation_period,
        stg_perf_eval_rating.performance_evaluation_title,
        stg_perf_eval_rating.performance_evaluation_type,
        stg_perf_eval_rating.person_id,
        stg_perf_eval_rating.source_system,
        stg_perf_eval_rating.academic_term,
        stg_perf_eval_rating.actual_time,
        stg_perf_eval_rating.schedule_date,
        stg_perf_eval_rating.actual_date,
        stg_perf_eval_rating.comments,
        stg_perf_eval_rating.actual_duration,
        stg_perf_eval_rating.is_announced,
        stg_perf_eval_rating.performance_evaluation_rating_level,
        stg_perf_eval_rating.coteaching_style_observed,
        stg_perf_eval_rating.reviewers,
        stg_perf_eval_rating.results
    from stg_perf_eval_rating
)
select * from formatted
