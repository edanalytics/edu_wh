{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_certification_exam set not null",
        "alter table {{ this }} add primary key (k_certification_exam)",
    ]
  )
}}

with stg_exam as (
    select * from {{ ref('stg_tpdm__certification_exams') }}
),

formatted as (
    select
        stg_exam.k_certification_exam,
        stg_exam.tenant_code,
        stg_exam.school_year,
        stg_exam.certification_exam_id,
        stg_exam.namespace,
        stg_exam.certification_exam_title,
        stg_exam.certification_exam_type,
        stg_exam.effective_date,
        stg_exam.end_date
    from stg_exam
)
select * from formatted