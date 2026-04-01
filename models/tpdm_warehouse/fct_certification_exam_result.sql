{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_certification_exam_result set not null",
    ]
  )
}}

with stage as (
    select * from {{ ref('stg_tpdm__certification_exam_results') }}
),

formatted as (
    select
        stage.k_certification_exam_result,
        stage.k_person,
        stage.k_certification_exam,
        stage.tenant_code,
        stage.school_year,
        stage.certification_exam_date,
        stage.attempt_number,
        stage.certification_exam_pass_indicator,
        stage.certification_exam_score,
        stage.certification_exam_status_descriptor,
        stage.person_source_system_descriptor
    from stage
)
select * from formatted
