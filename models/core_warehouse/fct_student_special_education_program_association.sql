{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_student_xyear, k_program, k_school, spec_ed_program_begin_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
    ]
  )
}}

with stg_stu_spec_ed as (
    select * from {{ ref('stg_ef3__student_special_education_program_associations') }}
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
        stg_stu_spec_ed.tenant_code,
        dim_program.school_year,
        stg_stu_spec_ed.spec_ed_program_begin_date,
        stg_stu_spec_ed.spec_ed_program_end_date,
        stg_stu_spec_ed.is_idea_eligible,
        stg_stu_spec_ed.iep_begin_date,
        stg_stu_spec_ed.iep_end_date,
        stg_stu_spec_ed.iep_review_date,
        stg_stu_spec_ed.last_evaluation_date,
        stg_stu_spec_ed.is_medically_fragile,
        stg_stu_spec_ed.is_multiply_disabled,
        stg_stu_spec_ed.school_hours_per_week,
        stg_stu_spec_ed.spec_ed_hours_per_week,
        stg_stu_spec_ed.is_served_outside_regular_session,
        stg_stu_spec_ed.participation_status_designated_by,
        stg_stu_spec_ed.participation_status_begin_date,
        stg_stu_spec_ed.participation_status_end_date,
        stg_stu_spec_ed.participation_status,
        stg_stu_spec_ed.reason_exited,
        stg_stu_spec_ed.special_education_setting
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_special_education_program_associations', flatten=False) }}
    from stg_stu_spec_ed
    inner join dim_student
        on stg_stu_spec_ed.k_student = dim_student.k_student
    inner join dim_program
        on stg_stu_spec_ed.k_program = dim_program.k_program
)

select * from formatted