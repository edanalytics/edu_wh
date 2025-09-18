{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_student_xyear set not null",
        "alter table {{ this }} alter column k_program set not null",
        "alter table {{ this }} alter column program_enroll_begin_date set not null",
        "alter table {{ this }} add primary key (k_student, k_student_xyear, k_program, program_enroll_begin_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
    ]
  )
}}

{% set custom_data_sources_name = "edu:student_special_education_program_association:custom_data_sources" %}

with stage as (
    select * from {{ ref('stg_ef3__student_special_education_program_associations') }}
),

dim_student as (
    select * from {{ ref('dim_student') }}
),

dim_program as (
    select * from {{ ref('dim_program') }}
),

bld_program_services as (
    select * From {{ ref ('bld_ef3__student_program__special_education__program_services')}}
),

bld_primary_disability as (
    select * from {{ ref('bld_ef3__student_program__special_education__primary_disability') }}
),

formatted as (
    select
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_program.k_program,
        dim_program.k_lea,
        dim_program.k_school,
        stage.tenant_code,
        dim_program.school_year,
        stage.program_enroll_begin_date,
        stage.program_enroll_end_date,
        stage.is_idea_eligible,
        stage.iep_begin_date,
        stage.iep_end_date,
        stage.iep_review_date,
        stage.last_evaluation_date,
        stage.is_medically_fragile,
        stage.is_multiply_disabled,
        stage.school_hours_per_week,
        stage.spec_ed_hours_per_week,
        stage.is_served_outside_regular_session,
        stage.participation_status_designated_by,
        stage.participation_status_begin_date,
        stage.participation_status_end_date,
        stage.participation_status,
        stage.reason_exited,
        stage.special_education_setting,
        bld_program_services.program_services as special_education_program_services,
        bld_primary_disability.disability_type as primary_disability_type
        {# add any extension columns configured from stg_ef3__student_special_education_program_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_special_education_program_associations', flatten=False) }}

        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stage
    
        inner join dim_student
            on stage.k_student = dim_student.k_student
            
        inner join dim_program
            on stage.k_program = dim_program.k_program
            
        -- left join because not all special ed programs include services (and they're optional in EdFi)
        left join bld_program_services 
            on stage.k_student = bld_program_services.k_student
            and stage.k_program = bld_program_services.k_program
            and stage.program_enroll_begin_date = bld_program_services.program_enroll_begin_date

        left join bld_primary_disability
            on stage.k_student = bld_primary_disability.k_student
            and stage.k_program = bld_primary_disability.k_program
            and stage.program_enroll_begin_date = bld_primary_disability.program_enroll_begin_date
        
        -- custom data sources
        {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)

select * from formatted
