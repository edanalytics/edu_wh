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
    select * from {{ ref('stg_ef3__student_language_instruction_program_associations') }}
),

dim_student as (
    select * from {{ ref('dim_student') }}
),

dim_program as (
    select * from {{ ref('dim_program') }}
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

        stage.has_english_learner_participation,
        stage.intervention_duration_min,

        stage.is_served_outside_regular_session,
        stage.participation_status,
        stage.participation_status_designated_by,
        stage.participation_status_begin_date,
        stage.participation_status_end_date,
        stage.reason_exited
        {# add any extension columns configured from stg_ef3__student_language_instruction_program_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_language_instruction_program_associations', flatten=False) }}
    from stage

        inner join dim_student
            on stage.k_student = dim_student.k_student

        inner join dim_program
            on stage.k_program = dim_program.k_program
)

select * from formatted
