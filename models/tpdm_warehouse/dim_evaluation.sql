{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_evaluation set not null",
        "alter table {{ this }} add primary key (k_evaluation)",
    ]
  )
}}

with stg_eval as (
    select * from {{ ref('stg_tpdm__evaluations') }}
),

formatted as (
    select
        stg_eval.k_evaluation,
        stg_eval.tenant_code,
        stg_eval.school_year,
        stg_eval.ed_org_id,
        stg_eval.evaluation_period,
        stg_eval.performance_evaluation_title,
        stg_eval.performance_evaluation_type,
        stg_eval.school_year,
        stg_eval.academic_term,
        stg_eval.academic_subject,
        stg_eval.evaluation_description,
        stg_eval.min_rating,
        stg_eval.max_rating,
        stg_eval.inter_rater_reliability_score,
        stg_eval.evaluation_type,
        stg_eval.v_rating_levels,
        stg_eval.performance_evaluation_reference
    from stg_eval
)
select * from formatted