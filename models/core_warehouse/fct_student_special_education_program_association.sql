{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_student_xyear, k_program, k_school, program_enroll_begin_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
    ]
  )
}}


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
        bld_program_services.program_services as special_education_program_services
        
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_special_education_program_associations', flatten=False) }}
        
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
)

select * from formatted
