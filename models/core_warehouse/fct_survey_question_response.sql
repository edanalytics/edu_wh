{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_survey_response set not null",
        "alter table {{ this }} alter column k_survey_question set not null",
        "alter table {{ this }} add primary key (k_survey_response, k_survey_question)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_survey_response foreign key (k_survey_response) references {{ ref('fct_survey_response') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_survey_question foreign key (k_survey_question) references {{ ref('dim_survey_question') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_survey foreign key (k_survey) references {{ ref('dim_survey') }}",
    ]
  )
}}

with stg_survey_question_responses as (
    select * from {{ ref('stg_ef3__survey_question_responses') }}
),
fct_survey_response as (
    select * from {{ ref('fct_survey_response') }}
),
formatted as (
    select
        stg_survey_question_responses.k_survey_response,
        stg_survey_question_responses.k_survey_question,
        fct_survey_response.k_survey,
        fct_survey_response.k_student,
        fct_survey_response.k_staff,
        fct_survey_response.k_parent,
        stg_survey_question_responses.tenant_code,
        fct_survey_response.respondent_type,
        fct_survey_response.location,
        stg_survey_question_responses.comment,
        stg_survey_question_responses.no_response,
        fct_survey_response.response_date,
        1 as question_response_count
    from stg_survey_question_responses
    left join fct_survey_response
        on stg_survey_question_responses.k_survey_response = fct_survey_response.k_survey_response
)
select * from formatted
