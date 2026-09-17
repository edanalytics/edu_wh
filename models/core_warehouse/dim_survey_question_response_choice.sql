{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_survey_question set not null",
        "alter table {{ this }} alter column sort_order set not null",
        "alter table {{ this }} add primary key (k_survey_question, sort_order)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_survey_question foreign key (k_survey_question) references {{ ref('dim_survey_question') }}",
    ]
  )
}}

with stg_survey_question_response_choices as (
    select * from {{ ref('stg_ef3__survey_questions__response_choices') }}
),
formatted as (
    select
        stg_survey_question_response_choices.k_survey_question,
        stg_survey_question_response_choices.tenant_code,
        stg_survey_question_response_choices.sort_order,
        stg_survey_question_response_choices.numeric_value,
        stg_survey_question_response_choices.text_value
    from stg_survey_question_response_choices
)
select * from formatted
