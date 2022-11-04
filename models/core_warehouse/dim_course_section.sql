{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_course_section)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course foreign key (k_course) references {{ ref('dim_course') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_session foreign key (k_session) references {{ ref('dim_session') }}",
    ]
  )
}}

with offering as (
    select * from {{ ref('stg_ef3__course_offerings') }}
),
section as (
    select * from {{ ref('stg_ef3__sections') }}
),
dim_course as (
    select * from {{ ref('dim_course') }}
),
-- todo: pivot characteristics wide
joined as (
    select 
        section.k_course_section,
        dim_course.k_course,
        offering.k_school,
        offering.k_session,
        section.k_location as k_classroom,
        section.tenant_code,
        section.section_id,
        section.section_name,
        offering.local_course_code,
        offering.local_course_title,
        dim_course.course_code,
        dim_course.course_title,
        offering.school_year,
        offering.session_name,
        dim_course.academic_subject,
        dim_course.career_pathway,
        offering.instructional_time_planned,
        section.sequence_of_course,
        section.educational_environment_type,
        section.instruction_language,
        section.medium_of_instruction,
        section.population_served,
        section.available_credits,
        section.available_credit_conversion,
        section.available_credit_type,
        section.is_official_attendance_period
        -- todo: add characteristic indicators
    from section
    join offering
        on section.k_course_offering = offering.k_course_offering
    join dim_course 
        on offering.k_course = dim_course.k_course
)
select * from joined
order by tenant_code, k_school, k_course_section