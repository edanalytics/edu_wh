{{
    config(
        post_hook=[
            "alter table {{ this }} alter column k_survey_response set not null",
            "alter table {{ this }} alter column k_survey_section set not null",
            "alter table {{ this }} add primary key (k_survey_response, k_survey_section)",
            "alter table {{ this }} add constraint fk_{{ this.name }}_survey_response foreign key (k_survey_response) references {{ ref('fct_survey_response') }}",
            "alter table {{ this }} add constraint fk_{{ this.name }}_survey_section foreign key (k_survey_section) references {{ ref('dim_survey_section') }}",
            "alter table {{ this }} add constraint fk_{{ this.name }}_survey foreign key (k_survey) references {{ ref('dim_survey') }}",
        ]
    )
}}

with stg_survey_section_responses as (
    select * from {{ ref('stg_ef3__survey_section_responses') }}
),

fct_survey_response as (
    select * from {{ ref('fct_survey_response') }}
),

formatted as (
    select
        stg_survey_section_responses.k_survey_response,
        stg_survey_section_responses.k_survey_section,
        fct_survey_response.k_survey,
        fct_survey_response.k_student,
        fct_survey_response.k_staff,
        fct_survey_response.k_parent,
        stg_survey_section_responses.tenant_code,
        fct_survey_response.respondent_type,
        fct_survey_response.location,
        fct_survey_response.response_date,
        stg_survey_section_responses.section_rating,
        1 as section_response_count

    from stg_survey_section_responses
    
    left join fct_survey_response
        on stg_survey_section_responses.k_survey_response = fct_survey_response.k_survey_response
)
select * from formatted
