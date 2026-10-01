{{
    config(
        post_hook=[
            "alter table {{ this }} alter column k_survey set not null",
            "alter table {{ this }} add primary key (k_survey)",
            "alter table {{ this }} add constraint fk_{{ this.name }}_lea foreign key (k_lea) references {{ ref('dim_lea') }}",
            "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
            "alter table {{ this }} add constraint fk_{{ this.name }}_session foreign key (k_session) references {{ ref('dim_session') }}",
        ]
    )
}}

with stg_surveys as (
    select * from {{ ref('stg_ef3__surveys') }}
),

-- Intermediate CTE w/ prefixed field references to make adding complexity later easier
formatted as (
    select
        stg_surveys.k_survey,
        stg_surveys.k_lea,
        stg_surveys.k_school,
        stg_surveys.k_session,
        stg_surveys.tenant_code,
        stg_surveys.school_year,
        stg_surveys.ed_org_id,
        stg_surveys.ed_org_type,
        stg_surveys.survey_id,
        stg_surveys.survey_title,
        stg_surveys.survey_category,    
        stg_surveys.number_administered

    from stg_surveys
)

select * from formatted
