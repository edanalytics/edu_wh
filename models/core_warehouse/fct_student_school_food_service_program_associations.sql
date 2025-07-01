{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_student_xyear, k_program, program_enroll_begin_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
    ]
  )
}}


with stage as (
    select * from {{ ref('stg_ef3__student_school_food_service_program_association') }}
),

dim_student as (
    select * from {{ ref('dim_student') }}
),

dim_program as (
    select * from {{ ref('dim_program') }}
),

stu_program_services as (
    select * from {{ ref('stg_ef3__stu_school_food_service__program_services') }}
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

        stage.direct_certification,
        stage.served_outside_of_regular_session,
        stu_program_services.program_service as school_food_service_program_service,
        {# add any extension columns configured from stg_ef3__student_school_food_service_program_association #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_school_food_service_program_association', flatten=False) }}
    from stage

    inner join dim_student
        on stage.k_student = dim_student.k_student
    
    inner join dim_program
        on stage.k_program = dim_program.k_program
    
    inner join stu_program_services
        on stage.k_student = stu_program_service.k_student
        and stage.k_program = stu_program_service.k_program
        and stage.program_enroll_begin_date = stu_program_service.program_enroll_begin_date
)

select * from formatted