{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_survey_response set not null",
        "alter table {{ this }} alter column k_survey_question set not null",
        "alter table {{ this }} alter column question_response_value_id set not null",
        "alter table {{ this }} add primary key (k_survey_response, k_survey_question, question_response_value_id)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_survey_question_response foreign key (k_survey_response, k_survey_question) references {{ ref('fct_survey_question_response') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_survey foreign key (k_survey) references {{ ref('dim_survey') }}",
    ]
  )
}}

with stg_survey_question_response_values as (
    select * from {{ ref('stg_ef3__survey_question_responses__values') }}
),
fct_survey_question_response as (
    select * from {{ ref('fct_survey_question_response') }}
),
formatted as (
    select
        stg_survey_question_response_values.k_survey_response,
        stg_survey_question_response_values.k_survey_question,
        stg_survey_question_response_values.tenant_code,
        stg_survey_question_response_values.question_response_value_id,
        fct_survey_question_response.k_survey,
        fct_survey_question_response.k_student,
        fct_survey_question_response.k_staff,
        fct_survey_question_response.k_parent,
        fct_survey_question_response.respondent_type,
        stg_survey_question_response_values.numeric_response,
        stg_survey_question_response_values.text_response,
        fct_survey_question_response.location,
        fct_survey_question_response.comment,
        fct_survey_question_response.no_response,
        fct_survey_question_response.response_date,
        1 as question_response_value_count
    from stg_survey_question_response_values
    left join fct_survey_question_response
        on stg_survey_question_response_values.k_survey_response = fct_survey_question_response.k_survey_response
        and stg_survey_question_response_values.k_survey_question = fct_survey_question_response.k_survey_question
)
select * from formatted
