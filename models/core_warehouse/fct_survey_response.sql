{{
    config(
        post_hook=[
            "alter table {{ this }} alter column k_survey_response set not null",
            "alter table {{ this }} add primary key (k_survey_response)",
            "alter table {{ this }} add constraint fk_{{ this.name }}_survey foreign key (k_survey) references {{ ref('dim_survey') }}",
            "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
            "alter table {{ this }} add constraint fk_{{ this.name }}_staff foreign key (k_staff) references {{ ref('dim_staff') }}",
            "alter table {{ this }} add constraint fk_{{ this.name }}_parent foreign key (k_parent) references {{ ref('dim_parent') }}",
        ]
    )
}}

with stg_survey_responses as (
    select * from {{ ref('stg_ef3__survey_responses') }}
),

formatted as (
    select
        stg_survey_responses.k_survey_response,
        stg_survey_responses.k_survey,
        stg_survey_responses.k_student,
        stg_survey_responses.k_staff,
        stg_survey_responses.k_contact as k_parent,
        stg_survey_responses.tenant_code,
        stg_survey_responses.survey_response_id,
        case
            when stg_survey_responses.k_student is not null then 'student'
            when stg_survey_responses.k_staff is not null then 'staff'
            when stg_survey_responses.k_contact is not null then 'parent'
            else 'anonymous'
        end as respondent_type,
        stg_survey_responses.electronic_mail_address,
        stg_survey_responses.full_name,
        stg_survey_responses.location,
        stg_survey_responses.response_date,
        stg_survey_responses.completion_time_seconds

    from stg_survey_responses
)

select * from formatted
