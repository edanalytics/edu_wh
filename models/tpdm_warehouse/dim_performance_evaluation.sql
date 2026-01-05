{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_performance_evaluation set not null",
        "alter table {{ this }} add primary key (k_performance_evaluation)",
    ]
  )
}}

with stg_perf_eval as (
    select * from {{ ref('stg_tpdm__performance_evaluations') }}
),

formatted as (
    select
        stg_perf_eval.k_performance_evaluation,
        stg_perf_eval.tenant_code,
        stg_perf_eaval.school_year,
        stg_perf_eval.ed_org_id,
        stg_perf_eval.evaluation_period,
        stg_perf_eval.performance_evaluation_title,
        stg_perf_eval.performance_evaluation_type,
        stg_perf_eval.academic_term,
        stg_perf_eval.academic_subject,
        stg_perf_eval.v_grade_levels,
        stg_perf_eval.v_rating_levels
        {{ edu_edfi_source.extract_extension(model_name='stg_tpdm__performance_evaluations', flatten=False) }}
    from stg_perf_eval
)
select * from formatted
