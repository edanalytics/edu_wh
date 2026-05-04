{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student_iep set not null",
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_student_xyear set not null",
        "alter table {{ this }} add primary key (k_student_iep)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
    ]
  )
}}

with stage as (
    select * from {{ ref('stg_sedm__student_ieps') }}
),

dim_student as (
    select * from {{ ref('dim_student') }}
),

formatted as (
    select
        stage.k_student_iep,
        dim_student.k_student,
        dim_student.k_student_xyear,
        stage.tenant_code,
        stage.school_year,
        stage.student_iep_association_id,
        stage.ed_org_id,
        stage.ed_org_type,
        stage.iep_finalized_date,
        stage.iep_begin_date,
        stage.iep_end_date,
        stage.iep_amended_date,
        stage.iep_status,
        stage.reason_exited,
        stage.special_education_setting,
        stage.is_medically_fragile,
        stage.is_multiply_disabled,
        stage.school_hours_per_week,
        stage.special_education_hours_per_week
        {{ edu_edfi_source.extract_extension(model_name='stg_sedm__student_ieps', flatten=False) }}
    from stage
        inner join dim_student
            on stage.k_student = dim_student.k_student
)

select * from formatted
