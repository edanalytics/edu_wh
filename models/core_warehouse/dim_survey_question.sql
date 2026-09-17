-- TODO: make indentation more consistent
{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_survey_question set not null",
        "alter table {{ this }} add primary key (k_survey_question)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_survey foreign key (k_survey) references {{ ref('dim_survey') }}",
    ]
  )
}}

with stg_survey_questions as (
    select * from {{ ref('stg_ef3__survey_questions') }}
),
formatted as (
    select
        stg_survey_questions.k_survey_question,
        stg_survey_questions.k_survey,
        stg_survey_questions.tenant_code,
        stg_survey_questions.question_code,
        stg_survey_questions.question_text,
        stg_survey_questions.question_form
    from stg_survey_questions
)
select * from formatted
