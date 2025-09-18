{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_course_section set not null",
        "alter table {{ this }} alter column begin_date set not null",
        "alter table {{ this }} add primary key (k_student, k_course_section, begin_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course_section foreign key (k_course_section) references {{ ref('dim_course_section') }}",
    ]
  )
}}

{% set custom_data_sources_name = "edu:student_section_association:custom_data_sources" %}

with stg_stu_section as (
    select * from {{ ref('stg_ef3__student_section_associations') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_course_section as (
    select * from {{ ref('dim_course_section') }}
),
formatted as (
    select 
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_course_section.k_school,
        dim_course_section.k_course_section,
        stg_stu_section.tenant_code,
        stg_stu_section.school_year,
        stg_stu_section.begin_date,
        stg_stu_section.end_date,
        stg_stu_section.is_homeroom,
        -- create indicator for active enrollment
        iff(
            -- is highest school year observed by tenant
            stg_stu_section.school_year = max(stg_stu_section.school_year) 
                over(partition by stg_stu_section.tenant_code)
            -- not yet exited
            and (end_date is null
                or end_date >= current_date())
            -- enrollment has begun
            and begin_date <= current_date(),
            true, false
        ) as is_active_enrollment,
        stg_stu_section.teacher_student_data_link_exclusion,
        stg_stu_section.attempt_status,
        stg_stu_section.repeat_identifier
        {# add any extension columns configured from stg_ef3__student_section_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_section_associations', flatten=False) }}

        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stg_stu_section
    join dim_student 
        on stg_stu_section.k_student = dim_student.k_student
    join dim_course_section
        on stg_stu_section.k_course_section = dim_course_section.k_course_section
        
    -- custom data sources
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select * from formatted