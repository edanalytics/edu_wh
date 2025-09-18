{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_course_section set not null",
        "alter table {{ this }} add primary key (k_course_section)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course foreign key (k_course) references {{ ref('dim_course') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_session foreign key (k_session) references {{ ref('dim_session') }}",
    ]
  )
}}

{% set custom_data_sources_name = "edu:course_section:custom_data_sources" %}
  
with offering as (
    select * from {{ ref('stg_ef3__course_offerings') }}
),
stg_ef3__sections as (
    select * from {{ ref('stg_ef3__sections') }}
),
dim_course as (
    select * from {{ ref('dim_course') }}
),
section_chars as (
    select * from {{ ref('bld_ef3__course_char__combined_wide') }}
),
joined as (
    select 
        stg_ef3__sections.k_course_section,
        dim_course.k_course,
        offering.k_school,
        offering.k_session,
        stg_ef3__sections.k_location as k_classroom,
        stg_ef3__sections.tenant_code,
        stg_ef3__sections.section_id,
        stg_ef3__sections.section_name,
        offering.local_course_code,
        offering.local_course_title,
        dim_course.course_code,
        dim_course.course_title,
        offering.school_year,
        offering.session_name,
        dim_course.academic_subject,
        dim_course.career_pathway,
        offering.instructional_time_planned,
        stg_ef3__sections.is_official_attendance_period,
        stg_ef3__sections.sequence_of_course,

        -- section characteristics
        {{ accordion_columns(
            source_table='bld_ef3__course_char__combined_wide',
            exclude_columns=['tenant_code', 'api_year', 'k_course', 'k_course_offering', 'k_course_section'],
            source_alias='section_chars',
            coalesce_value = 'FALSE'
        ) }}

        stg_ef3__sections.educational_environment_type,
        stg_ef3__sections.instruction_language,
        stg_ef3__sections.medium_of_instruction,
        stg_ef3__sections.population_served,
        stg_ef3__sections.section_type,
        stg_ef3__sections.available_credits,
        stg_ef3__sections.available_credit_type,
        stg_ef3__sections.available_credit_conversion
        
        -- todo: add characteristic indicators
        -- custom data sources_columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stg_ef3__sections
    join offering
        on stg_ef3__sections.k_course_offering = offering.k_course_offering
    join dim_course 
        on offering.k_course = dim_course.k_course
    left join section_chars 
        on stg_ef3__sections.k_course_section = section_chars.k_course_section

    -- custom data sources
    {{ add_cds_joins_v1(cds_model_config=custom_data_sources_name, driving_alias='stg_ef3__sections', join_cols=['k_course_section']) }}
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select * from joined
order by tenant_code, k_school, k_course_section
