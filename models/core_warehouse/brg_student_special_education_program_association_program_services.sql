{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_stu_sped_program_assoc set not null",
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_student_xyear set not null",
        "alter table {{ this }} alter column k_program set not null",
        "alter table {{ this }} alter column program_enroll_begin_date set not null",
        "alter table {{ this }} alter column program_service set not null",
        "alter table {{ this }} add primary key (k_stu_sped_program_assoc, program_service)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_stu_sped_program_assoc foreign key (k_student, k_student_xyear, k_program, program_enroll_begin_date) references {{ ref('fct_student_special_education_program_association') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
    ]
  )
}}

with stage as (
    select * from {{ ref('stg_ef3__stu_spec_ed__program_services') }}
),
formatted as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'stage.tenant_code',
                'stage.api_year', 
                'stage.k_student',
                'stage.k_program', 
                'stage.k_lea',
                'stage.k_school',
                'stage.program_enroll_begin_date'
            ]
        )}} as k_stu_sped_program_assoc,
        stage.k_student,
        stage.k_student_xyear,
        stage.k_program,
        stage.k_lea,
        stage.k_school,
        stage.tenant_code,
        stage.api_year as school_year,
        stage.program_enroll_begin_date,
        stage.program_enroll_end_date,
        stage.program_service,
        stage.primary_indicator as program_service_is_primary,
        stage.service_begin_date as program_service_begin_date,
        stage.service_end_date as program_service_end_date,
        stage.v_providers as v_program_service_providers
    from stage
)
select * from formatted