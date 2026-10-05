{{
    config(
        post_hook=[
            "alter table {{ this }} alter column k_survey_section set not null",
            "alter table {{ this }} add primary key (k_survey_section)",
        ]
    )
}}

with stg_survey_sections as (
    select * from {{ ref('stg_ef3__survey_sections') }}
),

formatted as (
    select
        stg_survey_sections.k_survey_section,
        stg_survey_sections.k_survey,
        stg_survey_sections.tenant_code,
        stg_survey_sections.survey_section_title

    from stg_survey_sections
)

select * from formatted
