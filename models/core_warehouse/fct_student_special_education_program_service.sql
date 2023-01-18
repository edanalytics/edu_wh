{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_student_xyear, k_program, spec_ed_program_begin_date, special_education_program_service)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
    ]
  )
}}

with stg_stu_spec_ed_services as (
    select * from {{ ref('stg_ef3__stu_spec_ed__program_services') }}
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
      stg_stu_spec_ed_services.tenant_code,
      dim_program.school_year,
      stg_stu_spec_ed_services.spec_ed_program_begin_date,
      stg_stu_spec_ed_services.special_education_program_service,
      stg_stu_spec_ed_services.primary_indicator,
      stg_stu_spec_ed_services.v_providers,
      stg_stu_spec_ed_services.service_begin_date,
      stg_stu_spec_ed_services.service_end_date
      {{ edu_edfi_source.extract_extension(model_name='stg_ef3__stu_spec_ed__program_services', flatten=False) }}
    from stg_stu_spec_ed_services
    join dim_student
        on stg_stu_spec_ed_services.k_student = dim_student.k_student
    join dim_program
        on stg_stu_spec_ed_services.k_program = dim_program.k_program
)

select * from formatted