{{
    config(
        post_hook=[
            "alter table {{ this }} alter column k_survey_response set not null",
            "alter table {{ this }} alter column k_survey_question set not null",
            "alter table {{ this }} alter column question_response_value_id set not null",
            "alter table {{ this }} add primary key (k_survey_response, k_survey_question, question_response_value_id)",
            "alter table {{ this }} add constraint fk_{{ this.name }}_survey foreign key (k_survey) references {{ ref('dim_survey') }}",
            "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
            "alter table {{ this }} add constraint fk_{{ this.name }}_staff foreign key (k_staff) references {{ ref('dim_staff') }}",
            "alter table {{ this }} add constraint fk_{{ this.name }}_parent foreign key (k_parent) references {{ ref('dim_parent') }}",
        ]
    )
}}

with stg_survey_question_response_values as (
    select * from {{ ref('stg_ef3__survey_question_responses__values') }}
),

stg_survey_question_responses as (
    select * from {{ ref('stg_ef3__survey_question_responses') }}
),

formatted as (
    select
        stg_survey_question_response_values.k_survey_response,
        stg_survey_question_response_values.k_survey_question,
        stg_survey_question_response_values.tenant_code,
        stg_survey_question_response_values.question_response_value_id,
        stg_survey_question_responses.k_survey,
        stg_survey_question_responses.k_student,
        stg_survey_question_responses.k_staff,
        stg_survey_question_responses.k_parent,
        stg_survey_question_responses.respondent_type,
        stg_survey_question_response_values.numeric_response,
        stg_survey_question_response_values.text_response,
        stg_survey_question_responses.location,
        stg_survey_question_responses.comment,
        stg_survey_question_responses.no_response,
        stg_survey_question_responses.response_date

    from stg_survey_question_response_values
    
    inner join stg_survey_question_responses
        on stg_survey_question_response_values.k_survey_response = stg_survey_question_responses.k_survey_response
        and stg_survey_question_response_values.k_survey_question = stg_survey_question_responses.k_survey_question
)
select * from formatted
